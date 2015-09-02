package reservation

import (
	"fmt"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Reservation is a type that represents a lock on a resource. At most one reservation
// can exist for an individual resource at any time.
type Reservation struct {
	stopped     bool
	key, Source string
	getConn     func() redis.Conn
	ttl         time.Duration
}

// Manager is responsible for creating and extending reservations. When a Reservation
// is created using Manager, the manager will automatically extend that Reservation
// every `Manager.Heartbeat` time units by setting the Reservation to expire
// after `Manager.TTL` time elapses.
type Manager struct {
	owner          string
	pool           *redis.Pool
	Heartbeat, TTL time.Duration
}

// NewManager returns a new Manager, or an error if a connection to the supplied
// Redis server cannot be made.
func NewManager(redisURL, owner string) (*Manager, error) {
	// Open redis pool
	redisPool := redis.NewPool(func() (redis.Conn, error) {
		return redis.DialTimeout("tcp", redisURL, 15*time.Second, 10*time.Second, 10*time.Second)
	}, 5)

	// Get a conn and ping so we fail immediately if the URL is wrong
	conn := redisPool.Get()
	defer conn.Close()
	if _, err := conn.Do("PING"); err != nil {
		return nil, fmt.Errorf("Error connecting to redis: %s", err)
	}

	return &Manager{
		Heartbeat: 15 * time.Minute,
		TTL:       4 * time.Hour,
		owner:     owner,
		pool:      redisPool,
	}, nil
}

// Lock creates a Reservation for `resource`, or returns an error if there already exists a
// Reservation for that resource.
func (manager *Manager) Lock(resource string) (*Reservation, error) {
	// Get hostname
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("reservation-%s", resource)
	val := fmt.Sprintf("%s-%s-%d", hostname, manager.owner, os.Getpid())

	// Get connection
	conn := manager.pool.Get()
	defer conn.Close()

	// Try to set the reservation
	success, err := conn.Do(
		"SET", key, val,
		"EX", manager.TTL.Seconds(),
		"NX")
	if err != nil {
		return nil, fmt.Errorf("Error with SET command: %s", err.Error())
	}
	if success == nil {
		return nil, fmt.Errorf("Reservation already exists for resource %s", resource)
	}

	// Make new reservation
	res := &Reservation{
		key:     key,
		Source:  val,
		getConn: manager.pool.Get,
		ttl:     manager.TTL,
	}

	// Set up heartbeat in background
	go func() {
		for _ = range time.Tick(manager.Heartbeat) {
			if res.stopped {
				break
			}
			// Panic if err; no way to handle the error gracefully when this runs in the background
			success, err := res.heartbeat()
			if err != nil {
				panic(err)
			}
			if success != 1 {
				panic(fmt.Errorf("Got code %d when attempting to extend reservation", success))
			}
		}
	}()

	return res, nil
}

// WaitUntilLock creates a Reservation for `resource`, or waits until it can do so.
func (manager *Manager) WaitUntilLock(resource string) (*Reservation, error) {
	res, err := manager.Lock(resource)
	for fmt.Sprintf("%s", err) == fmt.Sprintf("Reservation already exists for resource %s", resource) {
		fmt.Printf("RESERVE: Attempting to reserve %s\n", resource)
		time.Sleep(time.Second)
		res, err = manager.Lock(resource)
	}
	fmt.Printf("RESERVE: Done waiting for resource. reserve_state: %t\n", err == nil)
	return res, err
}

// Release ends a lock on a resource. Release returns `nil` if release was successful or
// an `error` if not. In the event of an error, the reservation will be removed from Redis after
// `Reservation.ttl` expires.
func (res *Reservation) Release() error {
	conn := res.getConn()
	defer conn.Close()

	_, err := redis.Int(conn.Do("DEL", res.key))
	// Always release lock so reservation will expire after TTL if delete fails
	res.stopped = true

	if err != nil {
		return fmt.Errorf("Error deleting reservation key for %s: %s", res.key, err.Error())
	}
	return nil
}

func (res *Reservation) heartbeat() (int, error) {
	// Get connection
	conn := res.getConn()
	defer conn.Close()

	// Check that the reservation still exists and error if we don't have it
	source, err := redis.String(conn.Do("GET", res.key))
	if err != nil {
		return -1, fmt.Errorf("Could not fetch owner of reservation %s: ERR %s", res.key, err.Error())
	}
	if source != res.Source {
		return -1, fmt.Errorf("Reservation for %s has unknown owner %s", res.key, source)
	}

	// Extend reservation
	success, err := redis.Int(conn.Do("EXPIRE", res.key, res.ttl.Seconds()))
	if err != nil {
		return -1, fmt.Errorf("Could not extend reservation %s: ERR %s", res.key, err.Error())
	}
	return success, nil
}
