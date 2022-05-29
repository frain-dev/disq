package redis

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frain-dev/disq"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const count = math.MaxInt64

// Broker based on redis STREAM and ZSET.
// Implements a delayed queue with support for retries.
type Stream struct {
	Redis disq.Redis

	zset                string
	stream              string
	streamGroup         string
	consumerName        string
	opts                *RedisConfig
	buffer              chan *disq.Message
	processed           uint32
	retries             uint32
	fails               uint32
	wg                  sync.WaitGroup
	isConsuming         bool
	quit                chan bool
	SchedulerLockPrefix string
}

func NewStream(cfg *RedisConfig) disq.Broker {

	err := cfg.Init()
	if err != nil {
		log.Errorf("Error: %v", err)
	}
	broker := &Stream{
		Redis:        cfg.Redis,
		zset:         "disq:" + "{" + cfg.Name + "}:zset",
		stream:       "disq:" + "{" + cfg.Name + "}:stream",
		streamGroup:  cfg.StreamGroup,
		consumerName: disq.ConsumerName(),
		opts:         cfg,
		buffer:       make(chan *disq.Message, cfg.BufferSize),
	}
	return broker
}

func (b *Stream) Consume(ctx context.Context) {
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

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		disq.Scheduler("delayed", b.Redis.(*redis.Client), b.scheduleDelayed)
	}()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		disq.Scheduler("pending", b.Redis.(*redis.Client), b.schedulePending)
	}()
	b.isConsuming = true
}

func (b *Stream) Process(msg *disq.Message) error {
	tasks := &disq.Tasks
	task, err := tasks.LoadTask(msg.TaskName)
	if err != nil {
		msg.Err = err
		disq.Logger.Printf("Error loading task: %s", err)
		return err
	}

	// retry exeeded
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
		//retry
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
	return msg.Err
}

func (b *Stream) fetchMessages(
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

func (b *Stream) Publish(msg *disq.Message) error {

	if msg.ID == "" {
		msg.ID = uuid.NewString()
	}

	body, err := msg.MarshalBinary()
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
func (b *Stream) FetchN(
	ctx context.Context, n int, waitTimeout time.Duration,
) ([]disq.Message, error) {
	streams, err := b.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{b.stream, ">"},
		Group:    b.streamGroup,
		Consumer: b.consumerName,
		Count:    int64(n),
		Block:    waitTimeout,
	}).Result()
	if err != nil {
		if err == redis.Nil {
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
		err = StreamUnmarshalMessage(msg, xmsg)
		if err != nil {
			msg.Err = err
		}
	}

	return msgs, nil
}

//Ack and delete a message then add it back to the queue
func (b *Stream) Requeue(msg *disq.Message) error {
	if err := b.Redis.XAck(msg.Ctx, b.stream, b.streamGroup, msg.ID).Err(); err != nil {
		return err
	}

	err := b.Redis.XDel(msg.Ctx, b.stream, msg.ID).Err()
	if err != nil {
		return err
	}
	//Requeue
	// msg.RetryCount++ //to know how many times it has been retried.
	err = b.Publish(msg)
	if err != nil {
		return err
	}
	return err
}

//deletes the message from the queue.
func (b *Stream) Delete(msg *disq.Message) error {
	if err := b.Redis.XAck(context.Background(), b.stream, b.streamGroup, msg.ID).Err(); err != nil {
		return err
	}
	return b.Redis.XDel(context.Background(), b.stream, msg.ID).Err()
}

func (b *Stream) Stop() error {
	go func() {
		b.quit <- true
	}()
	err := b.Redis.XGroupDelConsumer(
		context.TODO(), b.stream, b.streamGroup, b.consumerName).Err()
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			b.isConsuming = false
			return nil
		}
	}
	b.isConsuming = false
	return err
}

// Purge deletes all messages from the queue.
func (b *Stream) Purge() error {
	ctx := context.TODO()
	_ = b.Redis.Del(ctx, b.zset).Err()
	_ = b.Redis.XTrim(ctx, b.stream, 0).Err()
	return nil
}

func (b *Stream) Len() (int, error) {
	n, err := b.Redis.XLen(context.TODO(), b.stream).Result()
	return int(n), err
}

func (b *Stream) createStreamGroup(ctx context.Context) {
	_ = b.Redis.XGroupCreateMkStream(ctx, b.stream, b.streamGroup, "0").Err()
}

func (b *Stream) Stats() *disq.Stats {
	return &disq.Stats{
		Name:      b.consumerName,
		Processed: atomic.LoadUint32(&b.processed),
		Retries:   atomic.LoadUint32(&b.retries),
		Fails:     atomic.LoadUint32(&b.fails),
	}
}

func (b *Stream) Name() string {
	return b.opts.Name
}

func (b *Stream) Status() bool {
	return b.isConsuming
}

func (b *Stream) Config() disq.Config {
	return b.opts
}

func unixMs(tm time.Time) int64 {
	return tm.UnixNano() / int64(time.Millisecond)
}

func StreamUnmarshalMessage(msg *disq.Message, xmsg *redis.XMessage) error {
	body := xmsg.Values["body"].(string)
	err := msg.UnmarshalBinary(disq.StringToBytes(body))
	if err != nil {
		return err
	}
	msg.ID = xmsg.ID
	return nil
}

func (b *Stream) scheduleDelayed(ctx context.Context) (int, error) {
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

func (b *Stream) schedulePending(ctx context.Context) (int, error) {
	tm := time.Now().Add(-b.opts.WaitTimeout)
	end := strconv.FormatInt(unixMs(tm), 10)

	pending, err := b.Redis.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: b.stream,
		Group:  b.streamGroup,
		Start:  "-",
		End:    end,
		Count:  100,
	}).Result()
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			b.createStreamGroup(ctx)

			return 0, nil
		}
		return 0, err
	}

	for i := range pending {
		xmsgInfo := &pending[i]
		id := xmsgInfo.ID

		xmsgs, err := b.Redis.XRangeN(ctx, b.stream, id, id, 1).Result()
		if err != nil {
			return 0, err
		}

		if len(xmsgs) != 1 {
			err := fmt.Errorf("disq: can't find pending message id=%q in stream=%q",
				id, b.stream)
			return 0, err
		}

		xmsg := &xmsgs[0]
		msg := new(disq.Message)
		msg.Ctx = ctx
		err = StreamUnmarshalMessage(msg, xmsg)
		if err != nil {
			return 0, err
		}

		err = b.Requeue(msg)
		if err != nil {
			return 0, err
		}
	}

	return len(pending), nil
}

func (q *Stream) ZRangebyScore(ctx context.Context, min string, max string) ([]string, error) {
	bodies, err := q.opts.Redis.ZRangeByScore(ctx, q.zset, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	if err != nil {
		return nil, err
	}
	return bodies, nil
}

func (q *Stream) XPendingExt(ctx context.Context, start string, end string) ([]redis.XPendingExt, error) {
	pending, err := q.opts.Redis.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: q.stream,
		Group:  q.streamGroup,
		Start:  start,
		End:    end,
		Count:  count,
	}).Result()
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			_ = q.Redis.XGroupCreateMkStream(ctx, q.stream, q.streamGroup, "0").Err()
		}
		return nil, err
	}
	return pending, nil
}

func (q *Stream) XRange(ctx context.Context, start string, end string) *redis.XMessageSliceCmd {
	xrange := q.Redis.XRange(ctx, q.stream, start, end)
	return xrange
}

func (q *Stream) XRangeN(ctx context.Context, start string, end string, count int64) *redis.XMessageSliceCmd {
	xrange := q.Redis.XRangeN(ctx, q.stream, start, end, count)
	return xrange
}

func (q *Stream) XPending(ctx context.Context) (*redis.XPending, error) {
	pending, err := q.Redis.XPending(ctx, q.stream, q.streamGroup).Result()
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			_ = q.opts.Redis.XGroupCreateMkStream(ctx, q.stream, q.streamGroup, "0").Err()
		}
	}
	return pending, err
}

func (q *Stream) ZRem(ctx context.Context, body string) *redis.IntCmd {
	result := q.Redis.ZRem(ctx, q.zset, body)
	return result
}

func (q *Stream) XDel(ctx context.Context, id string) *redis.IntCmd {
	result := q.Redis.XDel(ctx, q.stream, id)
	return result
}

func (q *Stream) XAck(ctx context.Context, id string) *redis.IntCmd {
	result := q.Redis.XAck(ctx, q.stream, id)
	return result
}

func (q *Stream) XInfoConsumers(ctx context.Context) *redis.XInfoConsumersCmd {
	consumersInfo := q.Redis.XInfoConsumers(ctx, q.stream, q.streamGroup)
	return consumersInfo
}

func (q *Stream) XInfoStream(ctx context.Context) *redis.XInfoStreamCmd {
	infoStream := q.Redis.XInfoStream(ctx, q.stream)
	return infoStream
}

func (q *Stream) ExportMessagesfromStream(ctx context.Context) ([]disq.Message, error) {
	xmsgs, err := q.XRange(ctx, "-", "+").Result()
	if err != nil {
		return nil, err
	}

	msgs := make([]disq.Message, len(xmsgs))
	for i := range xmsgs {
		xmsg := &xmsgs[i]
		msg := &msgs[i]

		err = StreamUnmarshalMessage(msg, xmsg)

		if err != nil {
			return nil, err
		}
	}
	return msgs, nil
}

func (q *Stream) ExportMessagesfromStreamXACK(ctx context.Context) ([]disq.Message, error) {
	xmsgs, err := q.XRange(ctx, "-", "+").Result()
	if err != nil {
		return nil, err
	}

	msgs := make([]disq.Message, len(xmsgs))
	for i := range xmsgs {
		xmsg := &xmsgs[i]
		msg := &msgs[i]

		err = StreamUnmarshalMessage(msg, xmsg)

		if err != nil {
			return nil, err
		}
		if err := q.opts.Redis.XAck(ctx, q.stream, q.streamGroup, xmsg.ID).Err(); err != nil {
			return nil, err
		}

		err = q.opts.Redis.XDel(ctx, q.stream, xmsg.ID).Err()
		if err != nil {
			return nil, err
		}
	}
	return msgs, nil
}
