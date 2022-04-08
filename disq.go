package disq

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
)

var Logger *log.Logger

func init() {
	SetLogger(log.New(os.Stderr, "disq: ", log.LstdFlags|log.Lshortfile))
}

func SetLogger(logger *log.Logger) {
	Logger = logger
}

type Redis interface {
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Pipelined(ctx context.Context, fn func(pipe redis.Pipeliner) error) ([]redis.Cmder, error)

	// Eval Required by redislock
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}
