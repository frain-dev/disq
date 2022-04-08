package disq

import (
	"context"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v5"
)

type RedisStreamClient interface {
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	TxPipeline() redis.Pipeliner

	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	XDel(ctx context.Context, stream string, ids ...string) *redis.IntCmd
	XLen(ctx context.Context, stream string) *redis.IntCmd
	XRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
	XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd
	XTrim(ctx context.Context, key string, maxLen int64) *redis.IntCmd
	XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd

	ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd
	ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	XInfoConsumers(ctx context.Context, key string, group string) *redis.XInfoConsumersCmd
}

type Queue struct {
	Redis RedisStreamClient

	Zset                string
	Stream              string
	StreamGroup         string
	StreamConsumer      string
	SchedulerLockPrefix string
}

func (q *Queue) Add(msg *Message) error {
	body, err := msgpack.Marshal((*messageRaw)(msg))
	if err != nil {
		return err
	}

	//add to Zset
	if msg.Delay > 0 {
		tm := time.Now().Add(msg.Delay)
		return q.Redis.ZAdd(msg.Ctx, q.Zset, &redis.Z{
			Score:  float64(unixMs(tm)),
			Member: body,
		}).Err()
	}
	//add to stream
	return q.Redis.XAdd(msg.Ctx, &redis.XAddArgs{
		Stream: q.Stream,
		Values: map[string]interface{}{
			"body": body,
		},
	}).Err()
}

//Reserve N messages from the stream.
func (q *Queue) ReserveN(
	ctx context.Context, n int, waitTimeout time.Duration,
) ([]Message, error) {
	streams, err := q.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Streams:  []string{q.Stream, ">"},
		Group:    q.StreamGroup,
		Consumer: q.StreamConsumer,
		Count:    int64(n),
		Block:    waitTimeout,
	}).Result()
	if err != nil {
		if err == redis.Nil { // timeout
			return nil, nil
		}
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			q.createStreamGroup(ctx)
			return q.ReserveN(ctx, n, waitTimeout)
		}
		return nil, err
	}

	stream := &streams[0]
	msgs := make([]Message, len(stream.Messages))
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
func (q *Queue) Requeue(msg *Message) error {
	if err := q.Redis.XAck(msg.Ctx, q.Stream, q.StreamGroup, msg.ID).Err(); err != nil {
		return err
	}

	err := q.Redis.XDel(msg.Ctx, q.Stream, msg.ID).Err()
	if err != nil {
		return err
	}
	//Requeue
	msg.RetryCount++ //to know how many times it has been retried.
	err = q.Add(msg)
	if err != nil {
		return err
	}
	return err
}

// Delete deletes the message from the queue.
func (q *Queue) Delete(msg *Message) error {
	if err := q.Redis.XAck(context.Background(), q.Stream, q.StreamGroup, msg.ID).Err(); err != nil {
		return err
	}
	return q.Redis.XDel(context.Background(), q.Stream, msg.ID).Err()
}

// Purge deletes all messages from the queue.
func (q *Queue) Purge() error {
	ctx := context.TODO()
	_ = q.Redis.Del(ctx, q.Zset).Err()
	_ = q.Redis.XTrim(ctx, q.Stream, 0).Err()
	return nil
}

func (q *Queue) Len() (int, error) {
	n, err := q.Redis.XLen(context.TODO(), q.Stream).Result()
	return int(n), err
}

func (q *Queue) createStreamGroup(ctx context.Context) {
	_ = q.Redis.XGroupCreateMkStream(ctx, q.Stream, q.StreamGroup, "0").Err()
}

func unixMs(tm time.Time) int64 {
	return tm.UnixNano() / int64(time.Millisecond)
}

func unmarshalMessage(msg *Message, xmsg *redis.XMessage) error {
	body := xmsg.Values["body"].(string)
	if err := msgpack.Unmarshal([]byte(body), (*messageRaw)(msg)); err != nil {
		return err
	}
	msg.ID = xmsg.ID
	return nil
}
