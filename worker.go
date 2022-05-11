package disq

import (
	"context"
)

type Worker struct {
	brokers []Broker
}

func NewWorker(brokers []Broker) *Worker {
	w := &Worker{
		brokers: brokers,
	}
	return w
}

func (w *Worker) Brokers() []Broker {
	return w.brokers
}

func (w *Worker) Start(ctx context.Context) error {
	for _, b := range w.brokers {
		brk := b
		go func() {
			if !brk.Status() {
				brk.Consume(ctx)
			}
		}()
	}
	return nil
}

func (w *Worker) AddBroker(ctx context.Context, broker Broker) {
	if !broker.Status() {
		broker.Consume(ctx)
	}
	w.brokers = append(w.brokers, broker)
}

func (w *Worker) Stats() []*Stats {
	stats := make([]*Stats, len(w.brokers))
	for i, b := range w.brokers {
		stat := b.Stats()
		stats[i] = stat
	}
	return stats
}

func (w *Worker) Stop() error {
	for _, b := range w.brokers {
		brk := b
		go func() {
			_ = brk.Stop()
		}()
	}
	return nil
}
