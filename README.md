# disq
A proof-of-concept implementaion of disq, a custom job-queue library for Convoy. This POC makes use of the redis backend only.

# Usage

## Create a broker
You can create a single broker for publishing and consuming messages from a queue. You'll need to first create a task and a message though.  

```go
import (
    "github.com/frain-dev/disq"
    redisBroker "github.com/frain-dev/disq/brokers/redis"
) 

//Create a Task
var CountHandler, _ = disq.RegisterTask(&disq.TaskOptions{
	Name: "CountHandler",
	Handler: func(name string) error {
		time.Sleep(time.Duration(10) * time.Second)
		fmt.Println("Hello", name)
		return nil
	},
	RetryLimit: 3,
})

//Create a Message
var value = fmt.Sprint("message_", uuid.NewString())
var ctx = context.Background()

var msg := &disq.Message{
    Ctx:      ctx,
    TaskName: CountHandler.Name(),
    Args:     []interface{}{value},
}

// Create a Broker
cfg := redisBroker.RedisConfig{
		Redis:       c, //redis client
		Name:        name, //name of queue
		Concurency:  int32(concurency),
		StreamGroup: "disq:",
	}

var broker = redisBroker.New(&cfg)

broker.publish(msg)
broker.Consume(ctx)
broker.Stats()
```

## Create multiple brokers and assign them to a worker  
You can create multiple brokers, create a worker and manage those brokers with the worker. 

```go
import (
    "github.com/frain-dev/disq"
    redisBroker "github.com/frain-dev/disq/brokers/redis"
) 
//Create a worker
var brokers = []disq.Broker{broker1, broker2, broker3}
var w = disq.NewWorker(brokers, disq.WorkerConfig{})

//start processing messages
var err = w.Start(ctx)
if err != nil {
    log.Fatal(err)
}

//Get stats from all brokers
for i, b := range w.Brokers() {
    var len, _ = b.(*redisBroker.Broker).Len()
    log.Printf("Broker_%d Queue Size: %+v", i, len)
    log.Printf("Broker_%d Stats: %+v\n\n", i, b.Stats())
}
```

## Full example
There is a full working example in [test](./test/). To run it; 
```bash
go run test/api/api.go 
go run test/worker/worker.go
```