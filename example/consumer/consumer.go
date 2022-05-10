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

	err := w.Start(ctx)
	// b.Consume(ctx)

	if err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(200 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			// len, _ := b.Len()
			for i, b := range w.Brokers() {
				len, _ := b.Len()
				log.Printf("Broker_%d Queue Size: %+v", i, len)
				log.Printf("Broker_%d Stats: %+v\n\n", i, b.Stats())
			}
		case <-ctx.Done():
			log.Println("Worker quiting")
			w.Stop()
			return
		}
	}
}
