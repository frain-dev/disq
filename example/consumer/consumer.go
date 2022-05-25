package main

import (
	"context"
	"flag"
	"log"
	"time"

	example "github.com/frain-dev/disq/example"
)

func main() {
	ctx := context.Background()
	flag.Parse()

	w := example.RWorker.Worker

	// b := example.RQueue.Queue
	// w.Brokers()[0].(*redisBroker.Broker).Purge()

	w.StartAll(ctx)
	// b.Consume(ctx)

	ticker := time.NewTicker(200 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			// len, _ := b.Len()
			for n, b := range w.LoadAll() {
				len, _ := b.Len()
				log.Printf("Broker_%s Queue Size: %+v", n, len)
				log.Printf("Broker_%s Stats: %+v\n\n", n, b.Stats())
			}
		case <-ctx.Done():
			log.Println("Worker quiting")
			_ = w.StopAll()
			return
		}
	}
}
