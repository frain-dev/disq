package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/frain-dev/disq"
)

type Worker struct {
	name string
	m    sync.Map
}

func NewWorker(brokers []disq.Broker) *Worker {
	w := &Worker{
		name: disq.ConsumerName(),
	}

	for _, b := range brokers {
		_, loaded := w.m.LoadOrStore(b.Name(), b)
		if loaded {
			disq.Logger.Printf("broker with name=%q already exists, skipping.", b.Name())
		} else {
			disq.Logger.Printf("succesfully added broker=%s", b.Name())
		}
	}
	return w
}

func (w *Worker) GetAllBrokers() map[string]disq.Broker {
	brokers := map[string]disq.Broker{}

	w.m.Range(func(key, value interface{}) bool {
		b := value.(disq.Broker)
		brokers[fmt.Sprint(key)] = b
		return true
	})
	return brokers
}

func (w *Worker) GetOneBroker(name string) (disq.Broker, error) {
	if v, ok := w.m.Load(name); ok {
		b := v.(disq.Broker)
		return b, nil
	}
	return nil, fmt.Errorf("broker with name=%q not found", name)
}

func (w *Worker) Contains(name string) bool {
	if _, ok := w.m.Load(name); ok {
		return ok
	}
	return false
}

func (w *Worker) StartAll(ctx context.Context) error {
	w.m.Range(func(key, value interface{}) bool {
		b := value.(disq.Broker)
		if !b.Status() {
			b.Consume(ctx)
		}
		return true
	})
	return nil
}

func (w *Worker) StartOne(ctx context.Context, name string) error {
	if v, ok := w.m.Load(name); ok {
		b := v.(disq.Broker)
		if !b.Status() {
			b.Consume(ctx)
		}
		disq.Logger.Printf("succesfully started broker=%s", name)
		return nil
	}

	return fmt.Errorf("broker with name=%q not found", name)
}

func (w *Worker) AddOne(ctx context.Context, broker disq.Broker) error {
	_, loaded := w.m.LoadOrStore(broker.Name(), broker)
	if loaded {
		err := fmt.Errorf("broker with name=%q already exists", broker.Name())
		return err
	}
	disq.Logger.Printf("succesfully added broker=%s", broker.Name())
	return nil
}

func (w *Worker) AddMany(ctx context.Context, brokers []disq.Broker) error {
	for _, b := range brokers {
		err := w.AddOne(ctx, b)
		if err != nil {
			disq.Logger.Printf(err.Error())
		}
	}
	return nil
}

func (w *Worker) UpdateOne(ctx context.Context, broker disq.Broker) error {
	name := broker.Name()
	if v, ok := w.m.LoadAndDelete(name); ok {
		b := v.(disq.Broker)
		err := b.Stop()
		if err != nil {
			return err
		}
		w.m.Store(name, broker)
		disq.Logger.Printf("succesfully updated broker=%s", name)
		err = w.StartOne(ctx, name)
		if err != nil {
			return err
		}
	} else {
		disq.Logger.Printf("broker with name=%s not found, adding instead.", name)
		err := w.AddOne(ctx, broker)
		if err != nil {
			return err
		}
		err = w.StartOne(ctx, name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) UpdateMany(ctx context.Context, brokers []disq.Broker) error {
	for _, brk := range brokers {
		err := w.UpdateOne(ctx, brk)
		if err != nil {
			disq.Logger.Printf(err.Error())
		}
	}
	return nil
}

func (w *Worker) DeleteOne(name string) error {
	w.GetAllBrokers()[name].Stop()
	_, loaded := w.m.LoadAndDelete(name)
	if loaded {
		disq.Logger.Printf("broker with name=%q deleted", name)
		return nil
	}
	return fmt.Errorf("broker with name=%s not found", name)
}

func (w *Worker) DeleteMany(names []string) error {
	for _, name := range names {
		err := w.DeleteOne(name)
		if err != nil {
			disq.Logger.Printf(err.Error())
		}
	}
	return nil
}

func (w *Worker) GetAllStats() map[string]*disq.Stats {
	stats := map[string]*disq.Stats{}

	w.m.Range(func(key, value interface{}) bool {
		b := value.(disq.Broker)
		stats[fmt.Sprint(key)] = b.Stats()
		return true
	})
	return stats
}

func (w *Worker) GetOneStat(name string) (*disq.Stats, error) {
	if v, ok := w.m.Load(name); ok {
		b := v.(disq.Broker)
		return b.Stats(), nil
	}

	return nil, fmt.Errorf("broker with name=%q not found", name)
}

func (w *Worker) StopAll() error {
	w.m.Range(func(key, value interface{}) bool {
		b := value.(disq.Broker)
		if b.Status() {
			b.Stop()
			disq.Logger.Printf("succesfully stopped broker=%s", key)
		}
		return true
	})
	return nil
}

func (w *Worker) StopOne(name string) error {
	if v, ok := w.m.Load(name); ok {
		b := v.(disq.Broker)
		if b.Status() {
			b.Stop()
			disq.Logger.Printf("succesfully stopped broker=%s", name)
		}
		return nil
	}

	return fmt.Errorf("broker with name=%q not found", name)
}

func (w *Worker) Name() string {
	return w.name
}

func (w *Worker) SetName(name string) {
	w.name = name
}
