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

	err := w.StartAll(ctx)
	// b.Consume(ctx)

	if err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(200 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			// len, _ := b.Len()
			for n, b := range w.GetAllBrokers() {
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
