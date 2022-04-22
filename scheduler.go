package disq

import (
	"context"
	"math/rand"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
)

func Scheduler(name string, c *redis.Client,
	fn func(ctx context.Context) (int, error)) {
	for {
		ctx := context.TODO()

		var n int
		err := WithRedisLock(ctx, name, c, func(ctx context.Context) error {
			var err error
			n, err = fn(ctx)
			return err
		})
		if err != nil && err != redislock.ErrNotObtained {
			Logger.Printf("redis: %s failed: %s", name, err)
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
