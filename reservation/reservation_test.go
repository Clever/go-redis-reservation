package reservation

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var redisTestURL = os.Getenv("REDIS_TEST_URL")

func TestNewManager(t *testing.T) {
	manager, err := NewManager(redisTestURL, "test-worker")
	assert.Nil(t, err)
	// If manager is not nil, NewManager was able to succesfully ping local redis
	assert.NotNil(t, manager)
}

func setUp(t *testing.T) (*Manager, string) {
	conn, err := redis.DialTimeout("tcp", redisTestURL, 15*time.Second, 10*time.Second, 10*time.Second)
	message := fmt.Sprintf("Could not connect to redis at %s. Ensure redis is running at this location (maybe locally)?", redisTestURL)
	assert.Nil(t, err, message)
	defer conn.Close()
	conn.Do("FLUSHALL")
	manager, err := NewManager(redisTestURL, "test-worker")
	assert.Nil(t, err)
	resourceID := "12345"
	return manager, resourceID
}

func TestSourceExposed(t *testing.T) {
	manager, resourceID := setUp(t)

	// Create a reservation
	reservation, err := manager.Lock(resourceID)
	assert.Nil(t, err)

	hostname, err := os.Hostname()
	assert.Nil(t, err)

	expectedKeySubstr := fmt.Sprintf("%s-%s", hostname, manager.owner)
	// Assert we can access the reservation value
	assert.Contains(t, reservation.Source, expectedKeySubstr)
}

func TestManagerLockCreate(t *testing.T) {
	manager, resourceID := setUp(t)

	// Make a new reservation
	reservation, err := manager.Lock(resourceID)
	assert.Nil(t, err)

	// Assert reservation created with correct field values
	assert.NotNil(t, reservation)

	// Make the same reservation again and ensure error returned
	dupeReservation, err := manager.Lock(resourceID)
	assert.Nil(t, dupeReservation)
	assert.EqualError(t, err, fmt.Sprintf("Reservation already exists for resource %s", resourceID))

	// Release the first reservation and ensure we can make a second reservation
	err = reservation.Release()
	assert.Nil(t, err)
	reservation, err = manager.Lock(resourceID)
	assert.NotNil(t, reservation)
}

func TestManagerLockConcurrentRequests(t *testing.T) {
	// Test to ensure no race conditions when making many concurrent lock requests
	manager, resourceID := setUp(t)

	var wg sync.WaitGroup

	hold := make(chan struct{})

	// Make 100 simultaneous requests for locks
	var (
		numReservationExistsErrors int32
		numOtherErrors             int32
		numReservations            int32
	)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-hold // try to read from channel to block the goroutine
			reservation, err := manager.Lock(resourceID)
			expectedErr := fmt.Sprintf("Reservation already exists for resource %s", resourceID)
			if fmt.Sprintf("%s", err) == expectedErr {
				atomic.AddInt32(&numReservationExistsErrors, 1)
			} else if err != nil {
				fmt.Printf("Other error: %s", err)
				atomic.AddInt32(&numOtherErrors, 1)
			}
			if reservation != nil {
				atomic.AddInt32(&numReservations, 1)
			}
		}()
	}
	close(hold) // close channel so all goroutines make requests at once
	wg.Wait()

	assert.Equal(t, numReservations, int32(1), "Expected only one reservation to be made")
	assert.Equal(t, numOtherErrors+numReservationExistsErrors, int32(99), "Expected 99 errors")
}

func TestReservationTTL(t *testing.T) {
	manager, resourceID := setUp(t)

	// This setup would never happen in production, but simulates orphaned reservations
	// being left in redis if (for example) the process dies unexpectedly without releasing
	// the reservation
	manager.Heartbeat = 100 * time.Second
	manager.TTL = 1 * time.Second

	// Create a reservation
	reservation, err := manager.Lock(resourceID)
	assert.Nil(t, err)

	// Assert we can make a new reservation after TTL expires
	time.Sleep(2 * time.Second)
	reservation, err = manager.Lock(resourceID)
	assert.NotNil(t, reservation)
	assert.Nil(t, err)
}

func TestReservationWaitUntilLock(t *testing.T) {
	manager, resourceID := setUp(t)
	c := make(chan interface{})

	// Create a reservation and hold it for a little while
	go func() {
		reservation, err := manager.Lock(resourceID)
		assert.Nil(t, err)
		close(c)
		time.Sleep(5 * time.Second)
		reservation.Release()
	}()

	// Assert that the reservation is currently held, and we can't get it
	<-c
	_, err := manager.Lock(resourceID)
	assert.EqualError(t, err, fmt.Sprintf("Reservation already exists for resource %s", resourceID))

	// Now wait for the reservation, and assert that we get it eventually
	waitingReservation, err := manager.WaitUntilLock(resourceID)
	assert.NoError(t, err)
	assert.NotNil(t, waitingReservation)
}

func TestReservationExtend(t *testing.T) {
	manager, resourceID := setUp(t)

	manager.Heartbeat = 500 * time.Millisecond
	manager.TTL = 1 * time.Second

	// Create a reservation
	reservation, err := manager.Lock(resourceID)
	assert.Nil(t, err)

	// Assert we cannot make a new reservation because TTL has been extended
	time.Sleep(2 * time.Second)
	reservation, err = manager.Lock(resourceID)
	assert.Nil(t, reservation)
	assert.EqualError(t, err, fmt.Sprintf("Reservation already exists for resource %s", resourceID))
}
