package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/frain-dev/disq"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

func TestConsume(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("Error creating redis client: %s", err)
	}
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
		config    *RedisConfig
		tFN       func(context.Context, *RedisConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "stream consume message from queue",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewStream(config)
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
				return fmt.Sprint(length), nil
			},
			expect: fmt.Sprint(0),
		},
		{
			name:      "list consume message from queue",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewList(config)
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

func TestFetchNandProcess(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("Error creating redis client: %s", err)
	}
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
		config    *RedisConfig
		tFN       func(context.Context, *RedisConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "stream process one message.",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewStream(config)
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
				return fmt.Sprint(length), nil
			},
			expect: fmt.Sprint(0),
		},
		{
			name:      "list process one message.",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewList(config)
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
	client, err := NewClient()
	if err != nil {
		t.Fatalf("Error creating redis client: %s", err)
	}
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
		config    *RedisConfig
		tFN       func(context.Context, *RedisConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "stream publish a message.",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewStream(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				length, err := b.Len()
				if err != nil {
					return "", err
				}
				return fmt.Sprint(length), nil
			},
			expect: fmt.Sprint(1),
		},
		{
			name:      "list publish a message.",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewList(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				length, err := b.Len()
				if err != nil {
					return "", err
				}
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

func TestRequeue(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("Error creating redis client: %s", err)
	}
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
		config    *RedisConfig
		tFN       func(context.Context, *RedisConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "stream requeue message",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewStream(config)
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
				time.Sleep(time.Duration(3) * time.Second)

				msgs, err = b.FetchN(ctx, 1, time.Duration(1))
				if err != nil {
					return "", nil
				}

				count := msgs[0].RetryCount
				return fmt.Sprint(count), nil
			},
			expect: fmt.Sprint(1),
		},
		{
			name:      "list requeue message",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewList(config)
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
				time.Sleep(time.Duration(3) * time.Second)

				msgs, err = b.FetchN(ctx, 1, time.Duration(1))
				if err != nil {
					return "", nil
				}
				count := msgs[0].RetryCount
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
	client, err := NewClient()
	if err != nil {
		t.Fatalf("Error creating redis client: %s", err)
	}
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
		config    *RedisConfig
		tFN       func(context.Context, *RedisConfig, *disq.Message) (bool, error)
		expect    bool
	}{
		{
			name:      "stream message with delay",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (bool, error) {

				b := NewStream(config)
				err := b.Publish(msg)
				if err != nil {
					return false, err
				}
				start := time.Now()
				b.Consume(ctx)

				tm := <-handlerCh
				sub := tm.Sub(start)
				return disq.DurEqual(msg.Delay, sub, 1), nil
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

func TestDelete(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("Error creating redis client: %s", err)
	}
	//create task
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
		config    *RedisConfig
		tFN       func(context.Context, *RedisConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "stream delete a message.",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewStream(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				msgs, err := b.FetchN(ctx, 1, time.Duration(1))
				if err != nil {
					return "", nil
				}
				err = b.Delete(&msgs[0])
				if err != nil {
					return "", err
				}
				length, err := b.Len()
				if err != nil {
					return "", err
				}
				return fmt.Sprint(length), nil
			},
			expect: fmt.Sprint(0),
		},
		{
			name:      "list delete a message.",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewList(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				err = b.Delete(msg)
				if err != nil {
					return "", err
				}
				length, err := b.Len()
				if err != nil {
					return "", err
				}
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

func TestPurge(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("Error creating redis client: %s", err)
	}
	//create task
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
		config    *RedisConfig
		tFN       func(context.Context, *RedisConfig, *disq.Message) (string, error)
		expect    string
	}{
		{
			name:      "stream purge queue.",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewStream(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				err = b.(*Stream).Purge()
				if err != nil {
					return "", err
				}
				length, err := b.Len()
				if err != nil {
					return "", err
				}
				return fmt.Sprint(length), nil
			},
			expect: fmt.Sprint(0),
		},
		{
			name:      "list purge queue.",
			queueName: uuid.NewString(),
			config: &RedisConfig{
				Name:  uuid.NewString(),
				Redis: client,
			},
			tFN: func(ctx context.Context, config *RedisConfig, msg *disq.Message) (string, error) {

				b := NewList(config)
				err := b.Publish(msg)
				if err != nil {
					return "", err
				}
				err = b.(*List).Purge()
				if err != nil {
					return "", err
				}
				length, err := b.Len()
				if err != nil {
					return "", err
				}
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

//Create Redis client
func NewClient() (*redis.Client, error) {
	dsn := "redis://localhost:6379"

	opts, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, err
	}

	Redis := redis.NewClient(opts)
	if err := Redis.
		Ping(context.Background()).
		Err(); err != nil {
		fmt.Print(err)
		return nil, err
	}

	return Redis, nil
}
