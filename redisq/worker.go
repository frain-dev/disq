package redisq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frain-dev/disq"
	"github.com/frain-dev/disq/internal"
)

type Worker struct {
	ID        int
	queue     disq.Queue
	opt       *disq.QueueOptions
	buffer    chan *disq.Message
	processed uint32
	retries   uint32
	fails     uint32

	wg   sync.WaitGroup
	quit chan bool
}

func NewWorker(opt *disq.QueueOptions, id int) *Worker {
	w := &Worker{
		ID:     id,
		queue:  NewQueue(opt),
		opt:    opt,
		buffer: make(chan *disq.Message, opt.BufferSize),
		quit:   make(chan bool),
	}

	return w
}

func (w *Worker) Queue() disq.Queue {
	return w.queue
}

func (w *Worker) Options() *disq.QueueOptions {
	return nil
}

func (w *Worker) Start(ctx context.Context) error {
	go func() {
		timer := time.NewTimer(time.Minute)
		timer.Stop()
		for {
			timeout, err := w.fetchMessages(ctx, timer, w.opt.ReservationTimeout)
			const backoff = time.Second
			if err != nil {
				time.Sleep(backoff)
				continue
			}
			if timeout {
				return
			}
		}
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case msg := <-w.buffer:
				w.Process(msg)
			case <-w.quit:
				return
			}
		}
	}()
	return nil
}

func (w *Worker) Stats() *disq.WorkerStats {
	return &disq.WorkerStats{
		Processed: atomic.LoadUint32(&w.processed),
		Retries:   atomic.LoadUint32(&w.retries),
		Fails:     atomic.LoadUint32(&w.fails),
	}
}

func (w *Worker) Process(msg *disq.Message) error {
	// retry exeeded
	if msg.ReservedCount >= w.opt.RetryLimit {
		atomic.AddUint32(&w.fails, 1)
		err := w.queue.Delete(msg)
		if err != nil {
			internal.Logger.Printf("task=%q Delete failed: %s", msg.Name, err)
		}
		return nil
	}

	msgErr := w.opt.Handler.HandleMessage(msg)
	if msgErr != nil {
		atomic.AddUint32(&w.retries, 1)
		msg.Err = msgErr
		err := w.queue.Release(msg)
		if err != nil {
			internal.Logger.Printf("task=%q release failed: %s", msg.Name, err)
		}
		return msgErr
	}

	atomic.AddUint32(&w.processed, 1)
	err := w.queue.Delete(msg)
	if err != nil {
		internal.Logger.Printf("task=%q delete failed: %s", msg.Name, err)
	}
	return msg.Err
}

func (w *Worker) fetchMessages(
	ctx context.Context, timer *time.Timer, timeout time.Duration,
) (bool, error) {
	size := w.opt.ReservationSize

	msgs, err := w.queue.ReserveN(ctx, size, w.opt.WaitTimeout)
	if err != nil {
		return false, err
	}

	if len(msgs) == 0 {
		return false, nil
	}

	timer.Reset(timeout)
	for i := range msgs {
		msg := &msgs[i]
		select {
		case w.buffer <- msg:
		case <-timer.C:
			for i := range msgs[i:] {
				_ = w.queue.Release(&msgs[i])
			}
			return true, nil
		}
	}

	if !timer.Stop() {
		<-timer.C
	}

	return false, nil
}

func (w *Worker) Stop() error {
	go func() {
		w.quit <- true
	}()
	return nil
}

func (w *Worker) StopTimeout(timeout time.Duration) error {
	go func() {
		w.quit <- true
	}()
	return nil
}
