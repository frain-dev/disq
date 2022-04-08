package main

import (
	"context"
	"flag"
	"log"
	"time"

	test_task "github.com/frain-dev/disq/test"
)

func main() {
	ctx := context.Background()
	flag.Parse()

	w := test_task.RWorker.Worker
	// w.Queue().Purge()

	err := w.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(200 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			len, _ := w.Queue().Len()
			log.Printf("Queuesize: %+v\n\n", len)
			log.Printf("Worker Stats: %+v\n\n", w.Stats())
		case <-ctx.Done():
			log.Println("Worker quiting")
			return
		}
	}
}
