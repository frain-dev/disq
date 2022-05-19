package localstorage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frain-dev/disq"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// Broker based on in-memory buffer only.
// Implements a FIFO queue with support for delays and retries.
type LocalStorage struct {
	opts        *LocalStorageConfig
	buffer      chan *disq.Message
	consumer    string
	processed   uint32
	retries     uint32
	fails       uint32
	isConsuming bool
	wg          sync.WaitGroup
	quit        chan bool
}

func New(cfg *LocalStorageConfig) disq.Broker {

	err := cfg.Init()
	if err != nil {
		log.Errorf("Error: %v", err)
	}

	broker := &LocalStorage{
		opts:     cfg,
		consumer: disq.ConsumerName(),
		buffer:   make(chan *disq.Message, cfg.BufferSize),
	}
	return broker
}

func (b *LocalStorage) Consume(ctx context.Context) {
	for id := 0; id < int(b.opts.Concurency); id++ {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			for {
				select {
				case msg := <-b.buffer:
					_ = b.Process(msg)
				case <-b.quit:
					return
				}
			}
		}()
	}
	b.isConsuming = true
}

func (b *LocalStorage) Process(msg *disq.Message) error {
	if time.Now().Before(msg.ExecutionTime) {
		err := b.Publish(msg)
		if err != nil {
			return err
		}
		return nil
	}
	tasks := &disq.Tasks
	task, err := tasks.LoadTask(msg.TaskName)
	if err != nil {
		msg.Err = err
		disq.Logger.Printf("Error loading task: %s", err)
		return err
	}

	// retry exeeded
	if msg.RetryCount >= task.RetryLimit() {
		atomic.AddUint32(&b.fails, 1) //count as fail
		return nil
	}

	msgErr := task.HandleMessage(msg)

	if msgErr != nil {
		//retry
		_ = disq.ErrorHandler(msg, msgErr, &b.retries)
		disq.Logger.Println(disq.FormatHandlerError(msg, task.RetryLimit()))
		msg.Err = msgErr
		err := b.Requeue(msg)
		if err != nil {
			disq.Logger.Printf("requeue failed: %s", err)
		}
		return err
	}

	atomic.AddUint32(&b.processed, 1)
	return err
}

func (b *LocalStorage) Requeue(msg *disq.Message) error {
	//Requeue
	err := b.Publish(msg)
	if err != nil {
		return err
	}
	return err
}

func (b *LocalStorage) Publish(msg *disq.Message) error {
	if msg.ID == "" {
		msg.ID = uuid.NewString()
	}
	if msg.Delay > 0 {
		if time.Now().After(msg.ExecutionTime) {
			tm := time.Now().Add(msg.Delay)
			msg.ExecutionTime = tm
		}
	}
	b.buffer <- msg
	return nil
}

func (b *LocalStorage) FetchN(
	ctx context.Context, n int, waitTimeout time.Duration,
) ([]disq.Message, error) {
	msgs := make([]disq.Message, n)
	for i := 0; i < n; i++ {
		msg := <-b.buffer
		msgs[i] = *msg
	}
	return msgs, nil
}

func (b *LocalStorage) Delete(msg *disq.Message) error {
	return errors.New("not implemented")
}

func (b *LocalStorage) Stop() error {
	go func() {
		b.quit <- true
	}()
	b.isConsuming = false
	return nil
}

func (b *LocalStorage) Purge() error {
	return errors.New("not implemented")
}

func (b *LocalStorage) Len() (int, error) {
	n := len(b.buffer)
	return int(n), nil
}

func (b *LocalStorage) Stats() *disq.Stats {
	return &disq.Stats{
		Name:      b.consumer,
		Processed: atomic.LoadUint32(&b.processed),
		Retries:   atomic.LoadUint32(&b.retries),
		Fails:     atomic.LoadUint32(&b.fails),
	}
}

func (b *LocalStorage) Status() bool {
	return b.isConsuming
}
