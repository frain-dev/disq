package main

import (
	"context"
	"fmt"
	"log"

	"github.com/frain-dev/disq"
	test "github.com/frain-dev/disq/test"
	"github.com/google/uuid"
)

func main() {

	count := 5000000
	go func() {
		for i := 0; i < count; i++ {
			value := fmt.Sprint("message_", uuid.NewString())
			ctx := context.Background()
			// delay := time.Second * 10
			msg := &disq.Message{
				Ctx:      ctx,
				TaskName: test.CountHandler.Name(),
				Args:     []interface{}{value},
				// Delay:    delay,
			}
			err := test.RWorker.Worker.Brokers()[0].Publish(msg)
			if err != nil {
				log.Fatal(err)
			}
			// time.Sleep(time.Duration(3) * time.Second)
		}
	}()

	go func() {
		if len(test.RWorker.Worker.Brokers()) > 1 {
			for i := 0; i < count; i++ {
				value := fmt.Sprint("message_", uuid.NewString())
				ctx := context.Background()
				// delay := time.Second * 10
				msg := &disq.Message{
					Ctx:      ctx,
					TaskName: test.CountHandler.Name(),
					Args:     []interface{}{value},
					// Delay:    delay,
				}
				err := test.RWorker.Worker.Brokers()[1].Publish(msg)
				if err != nil {
					log.Fatal(err)
				}
				// time.Sleep(time.Duration(3) * time.Second)
			}
		}

	}()

	go func() {
		if len(test.RWorker.Worker.Brokers()) > 2 {

			for i := 0; i < count; i++ {
				value := fmt.Sprint("message_", uuid.NewString())
				ctx := context.Background()
				// delay := time.Second * 10
				msg := &disq.Message{
					Ctx:      ctx,
					TaskName: test.CountHandler.Name(),
					Args:     []interface{}{value},
					// Delay:    delay,
				}
				err := test.RWorker.Worker.Brokers()[2].Publish(msg)
				if err != nil {
					log.Fatal(err)
				}
				// time.Sleep(time.Duration(3) * time.Second)
			}
		}
	}()

	sig := test.WaitSignal()
	log.Println(sig.String())
}
