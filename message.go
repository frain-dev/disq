package disq

import (
	"context"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Message is used as a uniform object for publishing and consuming messages from a queue.
type Message struct {
	Ctx context.Context `msgpack:"-"`

	ID string `msgpack:"ID"`

	Name string `msgpack:"Name"`

	// Delay specifies the duration the queue must wait
	// before executing the message.
	Delay time.Duration `msgpack:"Delay"`

	Args []interface{} `msgpack:"Args"`

	ArgsBin []byte `msgpack:"ArgsBin"`

	TaskName string `msgpack:"TaskName"`

	RetryCount int `msgpack:"RetryCount"`

	//Execution time need for localstorage delays
	ExecutionTime time.Time `msgpack:"ExecutionTime"`

	Err error `msgpack:"Err"`
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

func (m *Message) MarshalArgs() ([]byte, error) {
	b, err := msgpack.Marshal(m.Args)
	if err != nil {
		return nil, err
	}
	m.ArgsBin = b

	return b, nil
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
