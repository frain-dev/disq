# disq
A custom job-queue library for [convoy](https://github.com/frain-dev/convoy). Provides in-memory (localstorage) and redis (stream and list) backends only for now.

# Features  
- Redis (Stream and List), and in-memory backends.
- Message Delay
- Automatic retries

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

// Create a (redis stream) Broker
cfg := redisBroker.RedisConfig{
		Redis:       c, //redis client
		Name:        name, //name of queue
		Concurency:  int32(concurency),
		StreamGroup: "disq:",
	}

var broker = redisBroker.NewStream(&cfg)

// Publish and Consume with the broker
broker.publish(msg)
broker.Consume(ctx)
broker.Stats() //View consumer stats
```

## Create multiple brokers and assign them to a worker  
You can create multiple brokers, create a worker and manage those brokers with the worker. 

```go
import (
    "github.com/frain-dev/disq"
) 
//Create a worker
var brokers = []disq.Broker{broker1, broker2, broker3}
var w = disq.NewWorker(brokers)

//start processing messages
var err = w.StartAll(ctx)
if err != nil {
    log.Fatal(err)
}

//Get stats from all brokers
for name, broker := range w.GetAllBrokers() {
    var len, _ = broker.Len()
    log.Printf("Broker_%d Queue Size: %+v", name, len)
    log.Printf("Broker_%d Stats: %+v\n\n", i, broker.Stats())
}
```

## Full example
For a full working example see the [example](./example/) folder. To run it; 
```bash
go run example/publisher/publisher.go 
go run example/consumer/consumer.go
```

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) for details.

## Credits

- [Frain](https://github.com/frain-dev)
- [taskq](https://github.com/vmihailenco/taskq)

## License

The MIT License (MIT). Please see [License File](LICENSE) for more information.
