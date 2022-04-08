package disq

import (
	"context"
	"time"
)

// Message is used to create and retrieve messages from a queue.
type Message struct {
	Ctx context.Context

	ID string

	// Delay specifies the duration the queue must wait
	// before executing the message.
	Delay time.Duration

	// Args passed to the handler.
	Args []interface{}

	// The number of times the message has been reserved or released.
	RetryCount int

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

type messageRaw Message
