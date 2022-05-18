package disq

import (
	"sync/atomic"
	"time"
)

type DisqError interface {
	GetDelay() time.Duration
	GetRateLimit() bool
}

type Error struct {
	RateLimit bool
	Delay     time.Duration
	Err       error
}

func (e *Error) Error() string {
	return e.Err.Error()
}

func (e *Error) GetDelay() time.Duration {
	return e.Delay
}

func (e *Error) GetRateLimit() bool {
	return e.RateLimit
}

func ErrorHandler(msg *Message, msgErr error, retries *uint32) error {
	if disqError, ok := msgErr.(DisqError); ok {
		if disqError.GetRateLimit() {
			msg.Delay = disqError.GetDelay()
			return nil
		}
		msg.RetryCount++
		atomic.AddUint32(retries, 1)
		msg.Delay = disqError.GetDelay()
		return nil
	}
	msg.RetryCount++
	atomic.AddUint32(retries, 1)
	return nil
}
