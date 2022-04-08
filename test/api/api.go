package main

import (
	"context"
	"fmt"
	"log"

	"github.com/frain-dev/disq"
	test_task "github.com/frain-dev/disq/test"
	"github.com/google/uuid"
)

func main() {

	//go test_task.LogStats()
	go func() {
		for i := 0; i < 500000000; i++ {
			value := fmt.Sprint("message_", uuid.NewString())
			ctx := context.Background()
			// delay := time.Minute * 1
			msg := &disq.Message{
				Ctx:  ctx,
				Args: []interface{}{value},
				// Delay: delay,
			}
			err := test_task.RWorker.Worker.Queue().Add(msg)
			if err != nil {
				log.Fatal(err)
			}
			// time.Sleep(time.Duration(3) * time.Second)
		}
	}()

	sig := test_task.WaitSignal()
	log.Println(sig.String())
}
