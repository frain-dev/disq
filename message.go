package disq

import (
	"context"
	"time"

	"github.com/vmihailenco/msgpack"
)

// Message is used as a uniform object for publishing and consuming messages from a queue.
type Message struct {
	Ctx context.Context

	ID string

	TaskName string

	// Delay specifies the duration the worker must wait
	// before executing the message.
	Delay time.Duration

	// Args passed to the handler.
	Args    []interface{}
	ArgsBin []byte
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

func (m *Message) MarshalBinary() ([]byte, error) {
	bArgs, err := msgpack.Marshal(m.Args)
	if err != nil {
		return nil, err
	}
	m.ArgsBin = bArgs

	bMsg, err := msgpack.Marshal((*MessageRaw)(m))
	if err != nil {
		return nil, err
	}
	return bMsg, nil
}

func (m *Message) UnmarshalBinary(b []byte) error {
	if err := msgpack.Unmarshal(b, (*MessageRaw)(m)); err != nil {
		return err
	}
	return nil
}

func (m *Message) SetDelay(delay time.Duration) {
	m.Delay = delay
}

type MessageRaw Message
