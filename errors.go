package disq

import "time"

type Delayer interface {
	Delay() time.Duration
}

func Delay(msg *Message, msgErr error) time.Duration {
	if delayer, ok := msgErr.(Delayer); ok {
		return delayer.Delay()
	}
	return msg.Delay
}
