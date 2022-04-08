package test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/frain-dev/disq"
	"github.com/go-redis/redis/v8"
)

type RedisQueue struct {
	Name  string
	Queue *disq.Queue
	inner *redis.Client
}

type RedisWorker struct {
	Worker *disq.Worker
	inner  *redis.Client
}

func NewClient() (*redis.Client, error) {
	dsn := "redis://localhost:6379"

	opts, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, err
	}

	Redis := redis.NewClient(opts)
	if err := Redis.
		Ping(context.Background()).
		Err(); err != nil {
		fmt.Print(err)
		return nil, err
	}

	return Redis, nil
}

func NewQueue(c *redis.Client, name string) *RedisQueue {

	q := &disq.Queue{
		Redis: c,

		Zset:        name + ":zset",
		Stream:      name + ":stream",
		StreamGroup: "disq",
	}

	return &RedisQueue{
		Name:  name,
		inner: c,
		Queue: q,
	}
}

func NewRedisWorker(c *redis.Client, q *RedisQueue, handler *disq.Handler) *RedisWorker {
	con := disq.NewWorker(&disq.Options{
		Queue:           q.Queue,
		ReservationSize: 10,
		Handler:         *handler,
		RetryLimit:      4,
	}, 1)

	return &RedisWorker{
		inner:  c,
		Worker: con,
	}
}

type EndpointError struct {
	delay time.Duration
	Err   error
}

func (e *EndpointError) Error() string {
	return e.Err.Error()
}

func (e *EndpointError) Delay() time.Duration {
	return e.delay
}

var CountHandler = disq.NewHandler(&disq.HandlerOptions{
	Name: "printer",
	Handler: func(name string) error {
		//time.Sleep(time.Duration(10) * time.Second)
		fmt.Println("Hello", name)
		// return errors.New("error")
		// return &EndpointError{Err: errors.New("Error"), delay: time.Minute * 1}
		return nil
	},
})

var (
	Redis, _ = NewClient()
)

var RQueue = NewQueue(Redis, "disq7")

var RWorker = NewRedisWorker(Redis, RQueue, &CountHandler)

var counter int32

func GetLocalCounter() int32 {
	return atomic.LoadInt32(&counter)
}

func IncrLocalCounter() {
	atomic.AddInt32(&counter, 1)
}

func LogStats() {
	var prev int32
	for range time.Tick(3 * time.Second) {
		n := GetLocalCounter()
		log.Printf("processed %d tasks (%d/s)", n, (n-prev)/3)
		prev = n
	}
}

func WaitSignal() os.Signal {
	ch := make(chan os.Signal, 2)
	signal.Notify(
		ch,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGTERM,
	)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM:
			return sig
		}
	}
}
