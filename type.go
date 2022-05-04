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
	BLPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd
	BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd
	BRPopLPush(ctx context.Context, source, destination string, timeout time.Duration) *redis.StringCmd
	LIndex(ctx context.Context, key string, index int64) *redis.StringCmd
	LInsert(ctx context.Context, key, op string, pivot, value interface{}) *redis.IntCmd
	LInsertBefore(ctx context.Context, key string, pivot, value interface{}) *redis.IntCmd
	LInsertAfter(ctx context.Context, key string, pivot, value interface{}) *redis.IntCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
	LPop(ctx context.Context, key string) *redis.StringCmd
	LPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd
	LPos(ctx context.Context, key string, value string, args redis.LPosArgs) *redis.IntCmd
	LPosCount(ctx context.Context, key string, value string, count int64, args redis.LPosArgs) *redis.IntSliceCmd
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	LPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	LRem(ctx context.Context, key string, count int64, value interface{}) *redis.IntCmd
	LSet(ctx context.Context, key string, index int64, value interface{}) *redis.StatusCmd
	LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd
	RPop(ctx context.Context, key string) *redis.StringCmd
	RPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd
	RPopLPush(ctx context.Context, source, destination string) *redis.StringCmd
	RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	RPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	LMove(ctx context.Context, source, destination, srcpos, destpos string) *redis.StringCmd
	BLMove(ctx context.Context, source, destination, srcpos, destpos string, timeout time.Duration) *redis.StringCmd
}
