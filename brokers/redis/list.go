package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frain-dev/disq"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// Broker based on redis LIST only.
// Implements a FIFO queue with no support for delays, supports retries.
type List struct {
	Redis disq.Redis

	list         string
	opts         *RedisConfig
	buffer       chan *disq.Message
	consumerName string
	processed    uint32
	retries      uint32
	fails        uint32
	isConsuming  bool
	wg           sync.WaitGroup
	quit         chan bool
}

func NewList(cfg *RedisConfig) disq.Broker {

	err := cfg.Init()
	if err != nil {
		log.Errorf("Error: %v", err)
	}
	broker := &List{
		Redis:        cfg.Redis,
		list:         "disq:" + "{" + cfg.Name + ":list",
		opts:         cfg,
		consumerName: disq.ConsumerName(),
		buffer:       make(chan *disq.Message, cfg.BufferSize),
	}
	return broker
}

func (b *List) Consume(ctx context.Context) {
	for id := 0; id < int(b.opts.Concurency); id++ {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			for {
				select {
				case msg := <-b.buffer:
					_ = b.Process(msg)
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
			timeout, err := b.fetchMessages(ctx, timer, b.opts.ReservationTimeout)
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
	b.isConsuming = true
}

func (b *List) Process(msg *disq.Message) error {
	tasks := &disq.Tasks
	task, err := tasks.LoadTask(msg.TaskName)
	if err != nil {
		msg.Err = err
		disq.Logger.Printf("Error loading task: %s", err)
		return err
	}

	if msg.RetryCount >= task.RetryLimit() {
		atomic.AddUint32(&b.fails, 1)
		err := b.Delete(msg)
		if err != nil {
			disq.Logger.Printf("delete failed: %s", err)
			return err
		}
		return nil
	}

	msgErr := task.HandleMessage(msg)

	if msgErr != nil {
		_ = disq.ErrorHandler(msg, msgErr, &b.retries)
		disq.Logger.Println(disq.FormatHandlerError(msg, task.RetryLimit()))
		msg.Err = msgErr
		err := b.Requeue(msg)
		if err != nil {
			disq.Logger.Printf("requeue failed: %s", err)
		}
		return err
	}

	atomic.AddUint32(&b.processed, 1)
	err = b.Delete(msg)
	if err != nil {
		disq.Logger.Printf("delete failed: %s", err)
	}
	return err
}

//delete a message then add it back to the queue
func (b *List) Requeue(msg *disq.Message) error {
	err := b.Delete(msg)
	if err != nil {
		return err
	}
	err = b.Publish(msg)
	if err != nil {
		return err
	}
	return err
}

func (b *List) fetchMessages(
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

func (b *List) Publish(msg *disq.Message) error {
	if msg.ID == "" {
		msg.ID = uuid.NewString()
	}

	body, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	//add to List
	return b.Redis.LPush(msg.Ctx, b.list, body).Err()
}

//Fetch N messages from the List.
func (b *List) FetchN(
	ctx context.Context, n int, waitTimeout time.Duration,
) ([]disq.Message, error) {
	List, err := b.Redis.LPopCount(ctx, b.list, n).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	msgs := make([]disq.Message, len(List))
	for i := range List {
		lmsg := List[i]
		msg := &msgs[i]
		msg.Ctx = ctx
		err = ListUnmarshalMessage(msg, lmsg)
		if err != nil {
			msg.Err = err
		}
	}
	return msgs, nil
}

func (b *List) Delete(msg *disq.Message) error {
	body, err := msg.MarshalBinary()
	if err != nil {
		return err
	}
	if err := b.Redis.LRem(context.Background(), b.list, 0, body).Err(); err != nil {
		return err
	}
	return nil
}

func (b *List) Stop() error {
	go func() {
		b.quit <- true
	}()
	b.isConsuming = false
	return nil
}

func (b *List) Purge() error {
	ctx := context.TODO()
	_ = b.Redis.Del(ctx, b.list).Err()
	_ = b.Redis.LTrim(ctx, b.list, 0, -1).Err()
	return nil
}

func (b *List) Len() (int, error) {
	n, err := b.Redis.LLen(context.TODO(), b.list).Result()
	return int(n), err
}

func (b *List) Stats() *disq.Stats {
	return &disq.Stats{
		Name:      b.consumerName,
		Processed: atomic.LoadUint32(&b.processed),
		Retries:   atomic.LoadUint32(&b.retries),
		Fails:     atomic.LoadUint32(&b.fails),
	}
}

func (b *List) Name() string {
	return b.opts.Name
}

func (b *List) Config() disq.Config {
	return b.opts
}

func (b *List) Status() bool {
	return b.isConsuming
}

func ListUnmarshalMessage(msg *disq.Message, body string) error {
	err := msg.UnmarshalBinary(disq.StringToBytes(body))
	if err != nil {
		return err
	}
	return nil
}
