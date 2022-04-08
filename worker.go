package disq

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

type Worker struct {
	ID        int
	Opt       *Options
	buffer    chan *Message
	processed uint32
	retries   uint32
	fails     uint32

	wg   sync.WaitGroup
	quit chan bool
}

type WorkerStats struct {
	Processed uint32
	Retries   uint32
	Fails     uint32
}

type Options struct {
	Queue           *Queue
	Handler         Handler
	ReservationSize int
	WaitTimeout     time.Duration
	RetryLimit      int
}

func NewWorker(opt *Options, id int) *Worker {
	w := &Worker{
		ID:     id,
		Opt:    opt,
		buffer: make(chan *Message, 100),
		quit:   make(chan bool),
	}
	opt.Queue.StreamConsumer = w.WorkerName()
	return w
}

func (w *Worker) Queue() *Queue {
	return w.Opt.Queue
}

func (w *Worker) Start(ctx context.Context) error {
	go func() {
		timer := time.NewTimer(time.Minute)
		timer.Stop()
		for {
			timeout, err := w.fetchMessages(ctx, timer, time.Minute*10)
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

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		Scheduler("delayed", w.Queue(), w.Queue().Redis.(*redis.Client), ScheduleDelayed)
	}()

	return nil
}

func (w *Worker) Stats() *WorkerStats {
	return &WorkerStats{
		Processed: atomic.LoadUint32(&w.processed),
		Retries:   atomic.LoadUint32(&w.retries),
		Fails:     atomic.LoadUint32(&w.fails),
	}
}

func (w *Worker) Process(msg *Message) error {
	// retry exeeded
	if msg.RetryCount >= w.Opt.RetryLimit {
		atomic.AddUint32(&w.fails, 1)  //count as fail
		err := w.Opt.Queue.Delete(msg) //delete from queue
		if err != nil {
			Logger.Printf("delete failed: %s", err)
		}
		return nil
	}

	msgErr := w.Opt.Handler.HandleMessage(msg)
	//retry
	if msgErr != nil {
		msg.Delay = Delay(msg, msgErr)
		atomic.AddUint32(&w.retries, 1)
		msg.Err = msgErr
		err := w.Opt.Queue.Requeue(msg)
		if err != nil {
			Logger.Printf("release failed: %s", err)
		}
		return msgErr
	}

	atomic.AddUint32(&w.processed, 1)
	err := w.Opt.Queue.Delete(msg)
	if err != nil {
		Logger.Printf("delete failed: %s", err)
	}
	return msg.Err
}

func (w *Worker) fetchMessages(
	ctx context.Context, timer *time.Timer, timeout time.Duration,
) (bool, error) {
	size := w.Opt.ReservationSize

	msgs, err := w.Opt.Queue.ReserveN(ctx, size, w.Opt.WaitTimeout)
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
				_ = w.Opt.Queue.Requeue(&msgs[i])
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

func (w *Worker) WorkerName() string {
	s, _ := os.Hostname()
	s += ":pid:" + strconv.Itoa(os.Getpid())
	s += ":" + strconv.Itoa(rand.Int())
	return s
}

func Delay(msg *Message, msgErr error) time.Duration {
	if delayer, ok := msgErr.(Delayer); ok {
		fmt.Println("I'm inside here")
		return delayer.Delay()
	}
	return msg.Delay
}

func (w *Worker) StopTimeout(timeout time.Duration) error {
	go func() {
		w.quit <- true
	}()
	return nil
}
