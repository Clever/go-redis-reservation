# go-redis-reservation
Golang library for resource reservation using Redis

## Testing
The tests depend on a local redis instance. Run
```
  redis-server &
  REDIS_TEST_URL=127.0.0.1:6379 go test ./reservation
```
