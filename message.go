package disq

import (
	"context"
	"time"
)

// Message is used as a uniform structure for publishing and consuming messages from a queue.
type Message struct {
	Ctx context.Context

	ID string

	TaskName string

	// Delay specifies the duration the worker must wait
	// before executing the message.
	Delay time.Duration

	// Args passed to the handler.
	Args []interface{}

	// The number of times the message has been reserved or released.
	RetryCount int

	//Execution time need for localstorage delays
	ExecutionTime time.Time

	Err error
}

func NewMessage(ctx context.Context, args ...interface{}) *Message {
	return &Message{
		Ctx:  ctx,
		Args: args,
	}
}

func (m *Message) SetDelay(delay time.Duration) {
	m.Delay = delay
}

type MessageRaw Message
