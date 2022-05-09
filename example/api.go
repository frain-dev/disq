package example

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/frain-dev/disq"
	redisBroker "github.com/frain-dev/disq/brokers/redis"
	"github.com/go-redis/redis/v8"
)

type RedisBroker struct {
	Name   string
	Broker disq.Broker
	inner  *redis.Client
}

type Worker struct {
	Worker *disq.Worker
	inner  *redis.Client
}

//Create Redis client
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

//Create new broker
func NewBroker(c *redis.Client, concurency int, name string) *RedisBroker {

	cfg := redisBroker.RedisConfig{
		Redis:       c,
		Name:        name,
		Concurency:  int32(concurency),
		StreamGroup: "disq:",
	}
	b := redisBroker.NewStream(&cfg)

	return &RedisBroker{
		Name:   name,
		inner:  c,
		Broker: b,
	}
}

//create new worker
func NewWorker(c *redis.Client, brokers []disq.Broker) *Worker {
	w := disq.NewWorker(brokers, disq.WorkerConfig{})

	return &Worker{
		inner:  c,
		Worker: w,
	}
}

//create task
var CountHandler, _ = disq.RegisterTask(&disq.TaskOptions{
	Name: "CountHandler",
	Handler: func(name string) error {
		//time.Sleep(time.Duration(10) * time.Second)
		// fmt.Println("Hello", name)
		// return errors.New("error")
		// return &EndpointError{Err: errors.New("Error"), delay: time.Second * 60}
		return nil
	},
	RetryLimit: 3,
})

var (
	Redis, _ = NewClient()
)

var RBroker1 = NewBroker(Redis, 1000, "disq10")
var RBroker2 = NewBroker(Redis, 100, "disq9")
var RBroker3 = NewBroker(Redis, 10, "disq8")

var RWorker = NewWorker(Redis, []disq.Broker{RBroker1.Broker, RBroker2.Broker, RBroker3.Broker})

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
