# go-redis-reservation
Golang library for resource reservation using Redis

## Usage
```
  import {
    "github.com/Clever/go-redis-reservation/reservation"
  }

  manager, err := reservation.NewManager("my-redis-host.com:6379", "myWorkerName")
  if err != nil {
    // Error connecting to redis
  }

  reservation, err := manager.Lock(jobName)
  if err != nil {
    // Reservation is already held for jobName
  }

  myWorker.DoWork()

  err = reservation.Release()

```

## Testing
The tests depend on a local redis instance. Run
```
  redis-server &
  REDIS_TEST_URL=127.0.0.1:6379 go test ./reservation
```
