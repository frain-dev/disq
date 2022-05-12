package main

import (
	"context"
	"fmt"
	"log"

	"github.com/frain-dev/disq"
	example "github.com/frain-dev/disq/example"
	"github.com/google/uuid"
)

func main() {
	count := 50
	go func() {
		for i := 0; i < count; i++ {
			value := fmt.Sprint("message_", uuid.NewString())
			ctx := context.Background()
			// delay := time.Second * 10
			msg := &disq.Message{
				Ctx:      ctx,
				TaskName: example.CountHandler.Name(),
				Args: []interface{}{&example.MsgValue{
					Name:  "test",
					Value: value,
				}},
				// Delay:    delay,
			}
			err := example.RWorker.Worker.Brokers()[0].Publish(msg)
			if err != nil {
				log.Fatal(err)
			}
			// time.Sleep(time.Duration(3) * time.Second)
		}
	}()

	go func() {
		if len(example.RWorker.Worker.Brokers()) > 1 {
			for i := 0; i < count; i++ {
				value := fmt.Sprint("message_", uuid.NewString())
				ctx := context.Background()
				// delay := time.Second * 10
				msg := &disq.Message{
					Ctx:      ctx,
					TaskName: example.CountHandler.Name(),
					Args: []interface{}{&example.MsgValue{
						Name:  "test",
						Value: value,
					}},
					// Delay:    delay,
				}
				err := example.RWorker.Worker.Brokers()[1].Publish(msg)
				if err != nil {
					log.Fatal(err)
				}
				// time.Sleep(time.Duration(3) * time.Second)
			}
		}

	}()

	go func() {
		if len(example.RWorker.Worker.Brokers()) > 2 {

			for i := 0; i < count; i++ {
				value := fmt.Sprint("message_", uuid.NewString())
				ctx := context.Background()
				// delay := time.Second * 10
				msg := &disq.Message{
					Ctx:      ctx,
					TaskName: example.CountHandler.Name(),
					Args: []interface{}{&example.MsgValue{
						Name:  "test",
						Value: value,
					}},
					// Delay:    delay,
				}
				err := example.RWorker.Worker.Brokers()[2].Publish(msg)
				if err != nil {
					log.Fatal(err)
				}
				// time.Sleep(time.Duration(3) * time.Second)
			}
		}
	}()

	sig := example.WaitSignal()
	log.Println(sig.String())
}
