package reservation

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

// func dropRedisKey(t *testing.T, key string) {
// 	conn, err := redis.Dial("tcp", redisURL)
// 	defer conn.Close()
// 	assert.Nil(t, err)
// 	_, err = conn.Do("DEL", key)
// 	assert.Nil(t, err)
// }

// func assertRedisKey(t *testing.T, key string) {
// 	conn, err := redis.Dial("tcp", redisURL)
// 	defer conn.Close()
// 	assert.Nil(t, err)
// 	_, err = conn.Do("DEL", key)
// 	assert.Nil(t, err)
// }
var redisTestURL = os.Getenv("REDIS_TEST_URL")

func TestNewManager(t *testing.T) {
	manager, err := NewManager(redisTestURL, "test-worker")
	assert.Nil(t, err)
	// If manager is not nil, NewManager was able to succesfully ping local redis
	assert.NotNil(t, manager)
}

func setUp(t *testing.T) (*ReservationManager, string, redis.Conn) {
	conn, err := redis.DialTimeout("tcp", redisTestURL, 15*time.Second, 10*time.Second, 10*time.Second)
	assert.Nil(t, err)
	conn.Do("FLUSHALL")
	manager, err := NewManager(redisTestURL, "test-worker")
	assert.Nil(t, err)
	resourceId := "12345"
	return manager, resourceId, conn
}

func TestManagerLockCreate(t *testing.T) {
	manager, resourceId, conn := setUp(t)
	defer conn.Close()

	// Make a new reservation
	reservation, err := manager.Lock(resourceId)
	assert.Nil(t, err)

	// Assert reservation created with correct field values
	assert.NotNil(t, reservation)

	// Make the same reservation again and ensure error returned
	dupeReservation, err := manager.Lock(resourceId)
	assert.Nil(t, dupeReservation)
	assert.EqualError(t, err, fmt.Sprintf("Reservation already exists for resource %s", resourceId))

	// Release the first reservation and ensure we can make a second reservation
	err = reservation.Release()
	assert.Nil(t, err)
	reservation, err = manager.Lock(resourceId)
	assert.NotNil(t, reservation)
}

func TestManagerLockConcurrentRequests(t *testing.T) {
	// Test to ensure no race conditions
	manager, resourceId, conn := setUp(t)
	defer conn.Close()

	var wg sync.WaitGroup

	// Make 100 requests for locks
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Make the same reservation again and ensure error returned
			_, _ = manager.Lock(resourceId)
		}()
	}
	wg.Wait()

	numKeys, err := conn.Do("DBSIZE")
	assert.Nil(t, err)
	assert.Equal(t, numKeys, 1, "Expected only one reservation to be made")
}

func TestReservationTTL(t *testing.T) {
	manager, resourceId, conn := setUp(t)
	defer conn.Close()

	// This setup would never happen in production, but simulates orphaned reservations
	// being left in redis if (for example) the process dies unexpectedly without releasing
	// the reservation
	manager.Heartbeat = 100 * time.Second
	manager.TTL = 1 * time.Second

	// Create a reservation
	reservation, err := manager.Lock(resourceId)
	assert.Nil(t, err)

	// Assert we can make a new reservation after TTL expires
	time.Sleep(2 * time.Second)
	reservation, err = manager.Lock(resourceId)
	assert.NotNil(t, reservation)
	assert.Nil(t, err)
}

func TestReservationExtend(t *testing.T) {
	manager, resourceId, conn := setUp(t)
	defer conn.Close()

	manager.Heartbeat = 500 * time.Millisecond
	manager.TTL = 1 * time.Second

	// Create a reservation
	reservation, err := manager.Lock(resourceId)
	assert.Nil(t, err)

	// Assert we cannot make a new reservation because TTL has been extended
	time.Sleep(2 * time.Second)
	reservation, err = manager.Lock(resourceId)
	assert.Nil(t, reservation)
	assert.EqualError(t, err, fmt.Sprintf("Reservation already exists for resource %s", resourceId))
}
