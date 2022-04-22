package redis

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frain-dev/disq"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack"
)

type Broker struct {
	Redis disq.Redis

	zset                string
	stream              string
	streamGroup         string
	streamConsumer      string
	opts                *RedisConfig
	buffer              chan *disq.Message
	processed           uint32
	retries             uint32
	fails               uint32
	wg                  sync.WaitGroup
	quit                chan bool
	SchedulerLockPrefix string
}

func New(cfg *RedisConfig) disq.Broker {

	err := cfg.Init()
	if err != nil {
		log.Errorf("Error:", err)
	}
	broker := &Broker{
		Redis:          cfg.Redis,
		zset:           cfg.Name + ":zset",
		stream:         cfg.Name + ":stream",
		streamGroup:    cfg.StreamGroup,
		streamConsumer: ConsumerName(),
		opts:           cfg,
		buffer:         make(chan *disq.Message, cfg.BufferSize),
	}
	return broker
}

func (b *Broker) Consume(ctx context.Context) {
	for id := 0; id < int(b.opts.Concurency); id++ {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			for {
				select {
				case msg := <-b.buffer:
					b.Process(msg)
				case <-b.quit:
					return
				}
			}
		}()
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		timer := time.NewTimer(time.Minute)
		timer.Stop()
		for {
			timeout, err := b.fetchMessages(ctx, timer, time.Minute*10)
			const backoff = time.Second
			if err != nil {
				time.Sleep(backoff)
				continue
			}
			if timeout {
				return
			}
		}
	}()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		disq.Scheduler("delayed", b.Redis.(*redis.Client), b.scheduleDelayed)
	}()
}

func (b *Broker) Process(msg *disq.Message) error {
	tasks := &disq.Tasks
	task, err := tasks.LoadTask(msg.TaskName)
	if err != nil {
		msg.Err = err
		disq.Logger.Printf("Error loading task: %s", err)
		return err
	}

	// retry exeeded
	if msg.RetryCount >= task.RetryLimit() {
		atomic.AddUint32(&b.fails, 1) //count as fail
		err := b.Delete(msg)          //delete from queue
		if err != nil {
			disq.Logger.Printf("delete failed: %s", err)
		}
		return nil
	}

	msgErr := task.HandleMessage(msg)

	if msgErr != nil {
		//retry
		msg.Delay = disq.Delay(msg, msgErr)
		atomic.AddUint32(&b.retries, 1)
		msg.Err = msgErr
		err := b.Requeue(msg)
		if err != nil {
			disq.Logger.Printf("requeue failed: %s", err)
		}
		return msgErr
	}

	atomic.AddUint32(&b.processed, 1)
	err = b.Delete(msg)
	if err != nil {
		disq.Logger.Printf("delete failed: %s", err)
	}
	return msg.Err
}

func (b *Broker) fetchMessages(
	ctx context.Context, timer *time.Timer, timeout time.Duration,
) (bool, error) {
	size := b.opts.ReservationSize

	msgs, err := b.FetchN(ctx, size, b.opts.WaitTimeout)
	if err != nil {
		return false, err
	}

	if len(msgs) == 0 {
		return false, nil
	}

	timer.Reset(timeout)
	for i := range msgs {
		msg := &msgs[i]
		select {
		case b.buffer <- msg:
		case <-timer.C:
			for i := range msgs[i:] {
				_ = b.Requeue(&msgs[i])
			}
			return true, nil
		}
	}

	if !timer.Stop() {
		<-timer.C
	}

	return false, nil
}

func (b *Broker) Publish(msg *disq.Message) error {
	body, err := msgpack.Marshal((*disq.MessageRaw)(msg))
	if err != nil {
		return err
	}

	//add to Zset
	if msg.Delay > 0 {
		tm := time.Now().Add(msg.Delay)
		return b.Redis.ZAdd(msg.Ctx, b.zset, &redis.Z{
			Score:  float64(unixMs(tm)),
			Member: body,
		}).Err()
	}
	//add to stream
	return b.Redis.XAdd(msg.Ctx, &redis.XAddArgs{
		Stream: b.stream,
		Values: map[string]interface{}{
			"body": body,
		},
	}).Err()
}

//Fetch N messages from the stream.
func (b *Broker) FetchN(
	ctx context.Context, n int, waitTimeout time.Duration,
) ([]disq.Message, error) {
	streams, err := b.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{b.stream, ">"},
		Group:    b.streamGroup,
		Consumer: b.streamConsumer,
		Count:    int64(n),
		Block:    waitTimeout,
	}).Result()
	if err != nil {
		if err == redis.Nil { // timeout
			return nil, nil
		}
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			b.createStreamGroup(ctx)
			return b.FetchN(ctx, n, waitTimeout)
		}
		return nil, err
	}

	stream := &streams[0]
	msgs := make([]disq.Message, len(stream.Messages))
	for i := range stream.Messages {
		xmsg := &stream.Messages[i]
		msg := &msgs[i]
		msg.Ctx = ctx
		err = unmarshalMessage(msg, xmsg)
		if err != nil {
			msg.Err = err
		}
	}

	return msgs, nil
}

//Ack and delete a message then add it back to the queue
func (b *Broker) Requeue(msg *disq.Message) error {
	if err := b.Redis.XAck(msg.Ctx, b.stream, b.streamGroup, msg.ID).Err(); err != nil {
		return err
	}

	err := b.Redis.XDel(msg.Ctx, b.stream, msg.ID).Err()
	if err != nil {
		return err
	}
	//Requeue
	msg.RetryCount++ //to know how many times it has been retried.
	err = b.Publish(msg)
	if err != nil {
		return err
	}
	return err
}

// Delete deletes the message from the queue.
func (b *Broker) Delete(msg *disq.Message) error {
	if err := b.Redis.XAck(context.Background(), b.stream, b.streamGroup, msg.ID).Err(); err != nil {
		return err
	}
	return b.Redis.XDel(context.Background(), b.stream, msg.ID).Err()
}

func (b *Broker) Stop() error {
	go func() {
		b.quit <- true
	}()
	return nil
}

// Purge deletes all messages from the queue.
func (b *Broker) Purge() error {
	ctx := context.TODO()
	_ = b.Redis.Del(ctx, b.zset).Err()
	_ = b.Redis.XTrim(ctx, b.stream, 0).Err()
	return nil
}

func (b *Broker) Len() (int, error) {
	n, err := b.Redis.XLen(context.TODO(), b.stream).Result()
	return int(n), err
}

func (b *Broker) createStreamGroup(ctx context.Context) {
	_ = b.Redis.XGroupCreateMkStream(ctx, b.stream, b.streamGroup, "0").Err()
}

func (b *Broker) Stats() *disq.Stats {
	return &disq.Stats{
		Name:      b.streamConsumer,
		Processed: atomic.LoadUint32(&b.processed),
		Retries:   atomic.LoadUint32(&b.retries),
		Fails:     atomic.LoadUint32(&b.fails),
	}
}

func unixMs(tm time.Time) int64 {
	return tm.UnixNano() / int64(time.Millisecond)
}

func unmarshalMessage(msg *disq.Message, xmsg *redis.XMessage) error {
	body := xmsg.Values["body"].(string)
	if err := msgpack.Unmarshal([]byte(body), (*disq.MessageRaw)(msg)); err != nil {
		return err
	}
	msg.ID = xmsg.ID
	return nil
}

func ConsumerName() string {
	s, _ := os.Hostname()
	s += ":pid:" + strconv.Itoa(os.Getpid())
	s += ":" + strconv.Itoa(rand.Int())
	return s
}

func (b *Broker) scheduleDelayed(ctx context.Context) (int, error) {
	tm := time.Now()
	max := strconv.FormatInt(unixMs(tm), 10)
	bodies, err := b.Redis.ZRangeByScore(ctx, b.zset, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   max,
		Count: 100,
	}).Result()
	if err != nil {
		return 0, err
	}

	pipe := b.Redis.TxPipeline()
	for _, body := range bodies {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: b.stream,
			Values: map[string]interface{}{
				"body": body,
			},
		})
		pipe.ZRem(ctx, b.zset, body)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return len(bodies), nil
}
