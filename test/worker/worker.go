package main

import (
	"context"
	"flag"
	"log"
	"time"

	redisBroker "github.com/frain-dev/disq/brokers/redis"
	test "github.com/frain-dev/disq/test"
)

func main() {
	ctx := context.Background()
	flag.Parse()

	w := test.RWorker.Worker
	// b := test_task.RQueue.Queue
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
				len, _ := b.(*redisBroker.Broker).Len()
				log.Printf("Broker_%d Queue Size: %+v", i, len)
				log.Printf("Broker_%d Stats: %+v\n\n", i, b.Stats())
			}
		case <-ctx.Done():
			log.Println("Worker quiting")
			return
		}
	}
}
