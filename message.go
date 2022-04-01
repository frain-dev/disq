package disq

import (
	"context"
	"encoding"
	"errors"
	"fmt"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
)

// ErrDuplicate is returned when adding duplicate message to the queue.
var ErrDuplicate = errors.New("disq: message with such name already exists")

// Message is used to create and retrieve messages from a queue.
type Message struct {
	Ctx context.Context `msgpack:"-"`

	// SQS/IronMQ message id.
	ID string `msgpack:"1,omitempty,alias:ID"`

	// Optional name for the message. Messages with the same name
	// are processed only once.
	Name string `msgpack:"-"`

	// Delay specifies the duration the queue must wait
	// before executing the message.
	Delay time.Duration `msgpack:"-"`

	// Args passed to the handler.
	Args []interface{} `msgpack:"-"`

	// Binary representation of the args.
	ArgsCompression string `msgpack:"2,omitempty,alias:ArgsCompression"`
	ArgsBin         []byte `msgpack:"3,alias:ArgsBin"`

	// The number of times the message has been reserved or released.
	ReservedCount int `msgpack:"4,omitempty,alias:ReservedCount"`

	Err error `msgpack:"-"`

	marshalBinaryCache []byte
}

func NewMessage(ctx context.Context, args ...interface{}) *Message {
	return &Message{
		Ctx:  ctx,
		Args: args,
	}
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<ID=%q Name=%q ReservedCount=%d>",
		m.ID, m.Name, m.ReservedCount)
}

// SetDelay sets the message delay.
func (m *Message) SetDelay(delay time.Duration) {
	m.Delay = delay
}

func (m *Message) MarshalArgs() ([]byte, error) {
	if m.ArgsBin != nil {
		if m.ArgsCompression == "" {
			return m.ArgsBin, nil
		}
		if m.Args == nil {
			return decompress(nil, m.ArgsBin, m.ArgsCompression)
		}
	}

	b, err := msgpack.Marshal(m.Args)
	if err != nil {
		return nil, err
	}
	m.ArgsBin = b

	return b, nil
}

type messageRaw Message

var _ encoding.BinaryMarshaler = (*Message)(nil)

func (m *Message) MarshalBinary() ([]byte, error) {

	if m.marshalBinaryCache != nil {
		return m.marshalBinaryCache, nil
	}

	_, err := m.MarshalArgs()
	if err != nil {
		return nil, err
	}

	if m.ArgsCompression == "" && len(m.ArgsBin) >= 512 {
		compressed := s2.Encode(nil, m.ArgsBin)
		if len(compressed) < len(m.ArgsBin) {
			m.ArgsCompression = "s2"
			m.ArgsBin = compressed
		}
	}

	b, err := msgpack.Marshal((*messageRaw)(m))
	if err != nil {
		return nil, err
	}

	m.marshalBinaryCache = b
	return b, nil
}

var _ encoding.BinaryUnmarshaler = (*Message)(nil)

func (m *Message) UnmarshalBinary(b []byte) error {
	if err := msgpack.Unmarshal(b, (*messageRaw)(m)); err != nil {
		return err
	}

	b, err := decompress(nil, m.ArgsBin, m.ArgsCompression)
	if err != nil {
		return err
	}

	m.ArgsCompression = ""
	m.ArgsBin = b

	return nil
}

var zdec, _ = zstd.NewReader(nil)

func decompress(dst, src []byte, compression string) ([]byte, error) {
	switch compression {
	case "":
		return src, nil
	case "zstd":
		return zdec.DecodeAll(dst, src)
	case "s2":
		return s2.Decode(dst, src)
	default:
		return nil, fmt.Errorf("disq: unsupported compression=%s", compression)
	}
}
