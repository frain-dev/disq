package disq

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
)

func Scheduler(name string, q *Queue, c *redis.Client,
	fn func(ctx context.Context, q *Queue) (int, error)) {
	for {
		ctx := context.TODO()

		var n int
		err := WithRedisLock(ctx, "disq-sheduler"+name, c, func(ctx context.Context) error {
			var err error
			n, err = fn(ctx, q)
			return err
		})
		if err != nil && err != redislock.ErrNotObtained {
			Logger.Printf("redisq: %s failed: %s", name, err)
		}
		if err != nil || n == 0 {
			time.Sleep(schedulerBackoff())
		}
	}
}

func schedulerBackoff() time.Duration {
	n := 250 + rand.Intn(250)
	return time.Duration(n) * time.Millisecond
}

func ScheduleDelayed(ctx context.Context, q *Queue) (int, error) {
	tm := time.Now()
	max := strconv.FormatInt(unixMs(tm), 10)
	bodies, err := q.Redis.ZRangeByScore(ctx, q.Zset, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   max,
		Count: 100,
	}).Result()
	if err != nil {
		return 0, err
	}

	pipe := q.Redis.TxPipeline()
	for _, body := range bodies {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: q.Stream,
			Values: map[string]interface{}{
				"body": body,
			},
		})
		pipe.ZRem(ctx, q.Zset, body)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return len(bodies), nil
}

func WithRedisLock(
	ctx context.Context, name string, redis Redis, fn func(ctx context.Context) error,
) error {
	lock, err := redislock.Obtain(ctx, redis, name, time.Minute, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err := lock.Release(ctx); err != nil {
			Logger.Printf("redislock.Release failed: %s", err)
		}
	}()

	return fn(ctx)
}
