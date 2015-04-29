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

func setUp(t *testing.T) (*ReservationManager, string) {
	conn, err := redis.DialTimeout("tcp", redisTestURL, 15*time.Second, 10*time.Second, 10*time.Second)
	defer conn.Close()
	assert.Nil(t, err)
	conn.Do("FLUSHALL")
	manager, err := NewManager(redisTestURL, "test-worker")
	assert.Nil(t, err)
	resourceId := "12345"
	return manager, resourceId
}

func TestManagerLockCreate(t *testing.T) {
	manager, resourceId := setUp(t)

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
	// Test to ensure no race conditions when making many concurrent lock requests
	manager, resourceId := setUp(t)

	var wg sync.WaitGroup

	// Make 100 simultaneous requests for locks
	numErrors := 0
	numReservations := 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reservation, err := manager.Lock(resourceId)
			if err != nil {
				numErrors++
			}
			if reservation != nil {
				numReservations++
			}
		}()
	}
	wg.Wait()

	// Assert only one entry in redis and the rest errors
	assert.Equal(t, numReservations, 1, "Expected only one reservation to be made")
	assert.Equal(t, numErrors, 99, "Expected 99 errors")
}

func TestReservationTTL(t *testing.T) {
	manager, resourceId := setUp(t)

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
	manager, resourceId := setUp(t)

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
