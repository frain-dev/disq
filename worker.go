package disq

import (
	"context"
)

type Worker struct {
	brokers []Broker
}

type WorkerConfig struct {
}

func NewWorker(brokers []Broker, cfg WorkerConfig) *Worker {
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
			brk.Consume(ctx)
		}()
	}
	return nil
}

func (w *Worker) AddBroker(broker Broker) {
	w.brokers = append(w.brokers, broker)
	//ToDo: Start newly added broker
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
			brk.Stop()
		}()
	}
	return nil
}
