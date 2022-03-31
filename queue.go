package disq

import (
	"context"
	"fmt"
	"time"
)

type Queue interface {
	fmt.Stringer
	Name() string
	Options() *QueueOptions
	Len() (int, error)
	Add(msg *Message) error
	Release(msg *Message) error
	ReserveN(ctx context.Context, n int, waitTimeout time.Duration) ([]Message, error)
	Delete(msg *Message) error
	Purge() error
	Close() error
	CloseTimeout(timeout time.Duration) error
}

type Worker interface {
	Queue() Queue
	Options() *QueueOptions
	Start(ctx context.Context) error
	Process(msg *Message) error
	Stats() *WorkerStats
	Stop() error
	StopTimeout(timeout time.Duration) error
}

type WorkerStats struct {
	Processed uint32
	Retries   uint32
	Fails     uint32
}

type QueueOptions struct {
	// Queue name.
	Name string

	//Redis Options
	// Redis client
	Redis Redis
	// Number of messages reserved by redis Worker in one request.
	// Default is 1000 messages.
	ReservationSize int
	// Time after which the reserved message is returned to the queue.
	// Default is 5 minutes.
	ReservationTimeout time.Duration
	// Time that a long polling receive call waits for a message to become
	// available before returning an empty response.
	// Default is 10 seconds.
	WaitTimeout time.Duration
	// Size of the buffer where reserved messages are stored.
	// Default is the same as ReservationSize.
	BufferSize int

	Handler Handler

	RetryLimit int

	//General Options
	// WorkerIdleTimeout Time after which the Worker need to be deleted.
	// Default is 6 hour
	WorkerIdleTimeout time.Duration

	init bool
}

func (opt *QueueOptions) Init() {
	if opt.init {
		return
	}
	opt.init = true

	if opt.Name == "" {
		panic("QueueOptions.Name is required")
	}

	if opt.ReservationSize == 0 {
		opt.ReservationSize = 1000
	}
	if opt.ReservationTimeout == 0 {
		opt.ReservationTimeout = 5 * time.Minute
	}
	if opt.BufferSize == 0 {
		opt.BufferSize = opt.ReservationSize
	}
	if opt.WaitTimeout == 0 {
		opt.WaitTimeout = 10 * time.Second
	}

	if opt.WorkerIdleTimeout == 0 {
		opt.WorkerIdleTimeout = 6 * time.Hour
	}
	if opt.Handler == nil {
		panic("Handler is required.")
	}
}
