package disq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type Config interface {
	Init() error
}

type Broker interface {
	Consume(context.Context)
	Publish(*Message) error
	Process(*Message) error
	FetchN(context.Context, int, time.Duration) ([]Message, error)
	Delete(*Message) error
	Stats() *Stats
	Status() bool
	Len() (int, error)
	Stop() error
}

type Stats struct {
	Name      string
	Processed uint32
	Retries   uint32
	Fails     uint32
}

type Redis interface {
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Pipelined(ctx context.Context, fn func(pipe redis.Pipeliner) error) ([]redis.Cmder, error)

	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd

	//Stream and ZSET methods
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

	//List methods
	LIndex(ctx context.Context, key string, index int64) *redis.StringCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
	LPop(ctx context.Context, key string) *redis.StringCmd
	LPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	LRem(ctx context.Context, key string, count int64, value interface{}) *redis.IntCmd
	LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd
}
