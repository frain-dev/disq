package localstorage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/frain-dev/disq"
	"github.com/google/uuid"
)

func TestConsume(t *testing.T) {
	TestHandler, _ := disq.RegisterTask(&disq.TaskOptions{
		Name: uuid.NewString(),
		Handler: func(name string) error {
			return nil
		},
		RetryLimit: 3,
	})
	value := fmt.Sprint("message_", uuid.NewString())

	msg := &disq.Message{
		Ctx:      context.Background(),
		TaskName: TestHandler.Name(),
		Args:     []interface{}{value},
	}
	tests := []struct {
		name      string
		queueName string
		config    *LocalStorageConfig
		tFN       func(context.Context, *LocalStorageConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "consume message from queue",
			queueName: uuid.NewString(),
			config: &LocalStorageConfig{
				Name: uuid.NewString(),
			},
			tFN: func(ctx context.Context, config *LocalStorageConfig, msg *disq.Message) (string, error) {

				b := New(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				b.Consume(ctx)
				time.Sleep(time.Duration(1) * time.Second)

				length, err := b.Len()
				if err != nil {
					return "", err
				}
				_ = b.Stop()
				return fmt.Sprint(length), nil
			},
			expect: fmt.Sprint(0),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFile := tc.config
			result, err := tc.tFN(context.Background(), configFile, msg)
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if result != tc.expect {
				t.Fatalf("Expected %v, got %v", tc.expect, result)
			}
		})
	}

}

func TestPublish(t *testing.T) {
	TestHandler, _ := disq.RegisterTask(&disq.TaskOptions{
		Name: uuid.NewString(),
		Handler: func(name string) error {
			return nil
		},
		RetryLimit: 3,
	})
	value := fmt.Sprint("message_", uuid.NewString())

	msg := &disq.Message{
		Ctx:      context.Background(),
		TaskName: TestHandler.Name(),
		Args:     []interface{}{value},
	}
	tests := []struct {
		name      string
		queueName string
		config    *LocalStorageConfig
		tFN       func(context.Context, *LocalStorageConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "stream publish a message.",
			queueName: uuid.NewString(),
			config: &LocalStorageConfig{
				Name: uuid.NewString(),
			},
			tFN: func(ctx context.Context, config *LocalStorageConfig, msg *disq.Message) (string, error) {

				b := New(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				length, err := b.Len()
				if err != nil {
					return "", err
				}
				_ = b.Stop()
				return fmt.Sprint(length), nil
			},
			expect: fmt.Sprint(1),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFile := tc.config
			result, err := tc.tFN(context.Background(), configFile, msg)
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if result != tc.expect {
				t.Fatalf("Expected %v, got %v", tc.expect, result)
			}
		})
	}

}

func TestFetchNandProcess(t *testing.T) {
	TestHandler, _ := disq.RegisterTask(&disq.TaskOptions{
		Name: uuid.NewString(),
		Handler: func(name string) error {
			return nil
		},
		RetryLimit: 3,
	})
	value := fmt.Sprint("message_", uuid.NewString())

	msg := &disq.Message{
		Ctx:      context.Background(),
		TaskName: TestHandler.Name(),
		Args:     []interface{}{value},
	}
	tests := []struct {
		name      string
		queueName string
		config    *LocalStorageConfig
		tFN       func(context.Context, *LocalStorageConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "stream process one message.",
			queueName: uuid.NewString(),
			config: &LocalStorageConfig{
				Name: uuid.NewString(),
			},
			tFN: func(ctx context.Context, config *LocalStorageConfig, msg *disq.Message) (string, error) {

				b := New(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				msgs, err := b.FetchN(ctx, 1, time.Duration(1))
				if err != nil {
					return "", err
				}
				err = b.Process(&msgs[0])
				if err != nil {
					return "", err
				}
				length, err := b.Len()
				if err != nil {
					return "", err
				}
				_ = b.Stop()
				return fmt.Sprint(length), nil
			},
			expect: fmt.Sprint(0),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFile := tc.config
			result, err := tc.tFN(context.Background(), configFile, msg)
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if result != tc.expect {
				t.Fatalf("Expected %v, got %v", tc.expect, result)
			}
		})
	}

}

func TestRequeue(t *testing.T) {
	TestHandler, _ := disq.RegisterTask(&disq.TaskOptions{
		Name: uuid.NewString(),
		Handler: func(name string) error {
			return errors.New("error")
		},
		RetryLimit: 1,
	})
	value := fmt.Sprint("message_", uuid.NewString())

	msg := &disq.Message{
		Ctx:      context.Background(),
		TaskName: TestHandler.Name(),
		Args:     []interface{}{value},
	}
	tests := []struct {
		name      string
		queueName string
		config    *LocalStorageConfig
		tFN       func(context.Context, *LocalStorageConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "requeue message",
			queueName: uuid.NewString(),
			config: &LocalStorageConfig{
				Name: uuid.NewString(),
			},
			tFN: func(ctx context.Context, config *LocalStorageConfig, msg *disq.Message) (string, error) {

				b := New(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				msgs, err := b.FetchN(ctx, 1, time.Duration(0))
				if err != nil {
					return "", err
				}
				err = b.Process(&msgs[0])
				if err != nil {
					return "", err
				}
				time.Sleep(time.Duration(3) * time.Second)

				msgs, err = b.FetchN(ctx, 1, time.Duration(1))
				if err != nil {
					return "", nil
				}

				count := msgs[0].RetryCount
				_ = b.Stop()
				return fmt.Sprint(count), nil
			},
			expect: fmt.Sprint(1),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFile := tc.config
			result, err := tc.tFN(context.Background(), configFile, msg)
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if result != tc.expect {
				t.Fatalf("Expected %v, got %v", tc.expect, result)
			}
		})
	}

}

func TestDelay(t *testing.T) {
	handlerCh := make(chan time.Time, 10)
	TestHandler, _ := disq.RegisterTask(&disq.TaskOptions{
		Name: uuid.NewString(),
		Handler: func(name string) error {
			handlerCh <- time.Now()
			return nil
		},
		RetryLimit: 1,
	})
	value := fmt.Sprint("message_", uuid.NewString())
	delay := time.Second * 5
	msg := &disq.Message{
		Ctx:      context.Background(),
		TaskName: TestHandler.Name(),
		Args:     []interface{}{value},
		Delay:    delay,
	}
	tests := []struct {
		name      string
		queueName string
		config    *LocalStorageConfig
		tFN       func(context.Context, *LocalStorageConfig, *disq.Message) (bool, error)
		expect    bool
	}{
		{
			name:      "stream message with delay",
			queueName: uuid.NewString(),
			config: &LocalStorageConfig{
				Name: uuid.NewString(),
			},
			tFN: func(ctx context.Context, config *LocalStorageConfig, msg *disq.Message) (bool, error) {

				b := New(config)
				err := b.Publish(msg)
				if err != nil {
					return false, err
				}
				start := time.Now()
				b.Consume(ctx)

				tm := <-handlerCh
				sub := tm.Sub(start)
				_ = b.Stop()
				return disq.DurEqual(msg.Delay, sub, 3), nil
			},
			expect: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFile := tc.config
			result, err := tc.tFN(context.Background(), configFile, msg)
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if result != tc.expect {
				t.Fatalf("Expected %v, got %v", tc.expect, result)
			}
		})
	}

}
