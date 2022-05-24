package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/frain-dev/disq"
	localstorage "github.com/frain-dev/disq/brokers/localstorage"
	redisBroker "github.com/frain-dev/disq/brokers/redis"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStart(t *testing.T) {

	tests := []struct {
		name   string
		tFN    func(context.Context) []int
		expect []int
	}{
		{
			name: "start all",
			tFN: func(ctx context.Context) []int {

				client, msg, err := GetTestConst()
				if err != nil {
					t.Fatalf("error intializing test: %s", err)
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})

				w := NewWorker([]disq.Broker{b1, b2, b3})
				_ = b1.Publish(msg)
				_ = b2.Publish(msg)
				_ = b3.Publish(msg)

				err = w.StartAll(context.Background())
				if err != nil {
					t.Fatalf("error starting brokers %s", err)
				}

				time.Sleep(time.Duration(1) * time.Second)

				l1, _ := b1.Len()
				l2, _ := b2.Len()
				l3, _ := b3.Len()
				_ = w.StopAll()

				return []int{l1, l2, l3}
			},
			expect: []int{0, 0, 0},
		},
		{
			name: "start one",
			tFN: func(ctx context.Context) []int {

				client, msg, err := GetTestConst()
				if err != nil {
					t.Fatalf("error intializing test: %s", err)
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})

				w := NewWorker([]disq.Broker{b1, b2, b3})
				_ = b1.Publish(msg)
				_ = b2.Publish(msg)
				_ = b3.Publish(msg)

				err = w.StartOne(context.Background(), b1.Name())
				if err != nil {
					t.Fatalf("error starting broker %s", err)
				}

				time.Sleep(time.Duration(1) * time.Second)

				l1, _ := b1.Len()
				l2, _ := b2.Len()
				l3, _ := b3.Len()
				_ = w.StopAll()

				return []int{l1, l2, l3}
			},
			expect: []int{0, 1, 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val := tc.tFN(context.Background())
			require.Equal(t, tc.expect, val)
		})
	}

}

func TestStop(t *testing.T) {

	tests := []struct {
		name   string
		tFN    func(context.Context) []bool
		expect []bool
	}{
		{
			name: "stop all",
			tFN: func(ctx context.Context) []bool {

				client, msg, err := GetTestConst()
				if err != nil {
					t.Fatalf("error intializing test: %s", err)
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})

				w := NewWorker([]disq.Broker{b1, b2, b3})
				_ = b1.Publish(msg)
				_ = b2.Publish(msg)
				_ = b3.Publish(msg)

				err = w.StartAll(context.Background())
				if err != nil {
					t.Fatalf("error starting broker %s", err)
				}
				err = w.StopAll()
				if err != nil {
					t.Fatalf("error stopping broker %s", err)
				}
				l1 := w.GetAllBrokers()[b1.Name()].Status()
				l2 := w.GetAllBrokers()[b2.Name()].Status()
				l3 := w.GetAllBrokers()[b3.Name()].Status()

				return []bool{l1, l2, l3}
			},
			expect: []bool{false, false, false},
		},
		{
			name: "stop one",
			tFN: func(ctx context.Context) []bool {

				client, msg, err := GetTestConst()
				if err != nil {
					t.Fatalf("error intializing test: %s", err)
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})

				w := NewWorker([]disq.Broker{b1, b2, b3})
				_ = b1.Publish(msg)
				_ = b2.Publish(msg)
				_ = b3.Publish(msg)

				err = w.StartAll(context.Background())
				if err != nil {
					t.Fatalf("error starting broker %s", err)
				}
				err = w.StopOne(b1.Name())
				if err != nil {
					t.Fatalf("error stopping broker %s", err)
				}
				l1 := w.GetAllBrokers()[b1.Name()].Status()
				l2 := w.GetAllBrokers()[b2.Name()].Status()
				l3 := w.GetAllBrokers()[b3.Name()].Status()

				return []bool{l1, l2, l3}
			},
			expect: []bool{false, true, true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val := tc.tFN(context.Background())
			require.Equal(t, tc.expect, val)
		})
	}

}

func TestAdd(t *testing.T) {

	tests := []struct {
		name   string
		tFN    func(context.Context) error
		expect error
	}{
		{
			name: "add a new broker",
			tFN: func(ctx context.Context) error {

				client, _, err := GetTestConst()
				if err != nil {
					t.Fatalf("error intializing test: %s", err)
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})
				w := NewWorker([]disq.Broker{b1, b3})

				err = w.AddOne(ctx, b2)

				return err
			},
			expect: nil,
		},
		{
			name: "add a broker that exists",
			tFN: func(ctx context.Context) error {

				client, _, err := GetTestConst()
				if err != nil {
					t.Fatalf("error intializing test: %s", err)
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  "disqTest",
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:  "disqTest",
					Redis: client,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})
				w := NewWorker([]disq.Broker{b1, b3})

				err = w.AddOne(ctx, b2)

				return err
			},
			expect: fmt.Errorf("broker with name=%q already exists", "disqTest"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.tFN(context.Background())
			require.Equal(t, tc.expect, err)
		})
	}

}

func TestUpdate(t *testing.T) {

	tests := []struct {
		name   string
		tFN    func(context.Context) (int, error)
		expect int
	}{
		{
			name: "update one that exists",
			tFN: func(ctx context.Context) (int, error) {

				client, _, err := GetTestConst()
				if err != nil {
					return 0, err
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:       "disqTest",
					Redis:      client,
					Concurency: 10,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})
				w := NewWorker([]disq.Broker{b1, b2, b3})
				_ = w.StartAll(ctx)

				b_new := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:       "disqTest",
					Redis:      client,
					Concurency: 1000,
				})
				err = w.UpdateOne(ctx, b_new)
				if err != nil {
					return 0, err
				}
				concurrency := w.GetAllBrokers()["disqTest"].Config().(*redisBroker.RedisConfig).Concurency
				_ = w.StopAll()
				return int(concurrency), err
			},
			expect: 1000,
		},
		{
			name: "update one thaat is new",
			tFN: func(ctx context.Context) (int, error) {

				client, _, err := GetTestConst()
				if err != nil {
					return 0, err
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:       "disqTest",
					Redis:      client,
					Concurency: 10,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})
				w := NewWorker([]disq.Broker{b1, b3})
				_ = w.StartAll(ctx)

				err = w.UpdateOne(ctx, b2)
				if err != nil {
					return 0, nil
				}
				_ = w.StopAll()
				concurrency := w.GetAllBrokers()["disqTest"].Config().(*redisBroker.RedisConfig).Concurency
				return int(concurrency), err
			},
			expect: 10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, err := tc.tFN(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, tc.expect, val)
		})
	}

}

func TestDelete(t *testing.T) {

	tests := []struct {
		name   string
		tFN    func(context.Context) (bool, error)
		expect bool
	}{
		{
			name: "delete one",
			tFN: func(ctx context.Context) (bool, error) {

				client, _, err := GetTestConst()
				if err != nil {
					return false, err
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})
				w := NewWorker([]disq.Broker{b1, b2, b3})
				_ = w.StartAll(ctx)

				err = w.DeleteOne(b3.Name())
				if err != nil {
					return false, err
				}
				val := w.Contains(b3.Name())
				_ = w.StopAll()
				return val, nil
			},
			expect: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, err := tc.tFN(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, tc.expect, val)
		})
	}

}

func TestStats(t *testing.T) {

	tests := []struct {
		name   string
		tFN    func(context.Context) []uint32
		expect []uint32
	}{
		{
			name: "get stats",
			tFN: func(ctx context.Context) []uint32 {

				client, msg, err := GetTestConst()
				if err != nil {
					t.Fatalf(err.Error())
				}
				b1 := redisBroker.NewStream(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b2 := redisBroker.NewList(&redisBroker.RedisConfig{
					Name:  uuid.NewString(),
					Redis: client,
				})
				b3 := localstorage.New(&localstorage.LocalStorageConfig{
					Name: uuid.NewString(),
				})
				_ = b1.Publish(msg)
				_ = b2.Publish(msg)
				_ = b3.Publish(msg)

				w := NewWorker([]disq.Broker{b1, b2, b3})
				_ = w.StartAll(ctx)
				time.Sleep(time.Duration(1) * time.Second)

				stats := w.GetAllStats()
				s1 := stats[b1.Name()].Processed
				s2 := stats[b2.Name()].Processed
				s3 := stats[b3.Name()].Processed
				_ = w.StopAll()
				return []uint32{s1, s2, s3}
			},
			expect: []uint32{1, 1, 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val := tc.tFN(context.Background())
			require.Equal(t, tc.expect, val)
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

func GetTestConst() (*redis.Client, *disq.Message, error) {
	client, err := NewClient()
	if err != nil {
		return nil, nil, fmt.Errorf("Error creating redis client: %s", err)
	}
	testHandler, _ := disq.RegisterTask(&disq.TaskOptions{
		Name: uuid.NewString(),
		Handler: func(name string) error {
			return nil
		},
		RetryLimit: 3,
	})
	value := fmt.Sprint("message_", uuid.NewString())

	msg := &disq.Message{
		Ctx:      context.Background(),
		TaskName: testHandler.Name(),
		Args:     []interface{}{value},
	}
	return client, msg, nil
}
