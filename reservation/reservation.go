package reservation

import (
	"fmt"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Reservation struct {
	stopped     bool
	key, source string
	pool        *redis.Pool
	ttl         time.Duration
}

type ReservationManager struct {
	owner          string
	pool           *redis.Pool
	Heartbeat, TTL time.Duration
}

func NewManager(redisURL, owner string) (*ReservationManager, error) {
	// Open redis pool
	redisPool := redis.NewPool(func() (redis.Conn, error) {
		return redis.DialTimeout("tcp", redisURL, 15*time.Second, 10*time.Second, 10*time.Second)
	}, 5)

	// Get a conn and ping so we fail immediately if the URL is wrong
	conn := redisPool.Get()
	defer conn.Close()
	if _, err := conn.Do("PING"); err != nil {
		return nil, err
	}

	return &ReservationManager{
		Heartbeat: 15 * time.Minute,
		TTL:       4 * time.Hour,
		owner:     owner,
		pool:      redisPool,
	}, nil
}

func (manager *ReservationManager) Lock(resource string) (*Reservation, error) {
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
		key:    key,
		source: val,
		pool:   manager.pool,
		ttl:    manager.TTL,
	}

	// Set up heartbeat in background
	go func() {
		for _ = range time.Tick(manager.Heartbeat) {
			if res.stopped {
				break
			}
			// Panic if err; no way to handle the error gracefully when this runs in the background
			if _, err := res.heartbeat(); err != nil {
				panic(err)
			}
		}
	}()

	return res, nil
}

func (res *Reservation) Release() error {
	conn := res.pool.Get()
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
	conn := res.pool.Get()
	defer conn.Close()

	// Check that the reservation still exists and error if we don't have it
	source, err := redis.String(conn.Do("GET", res.key))
	if err != nil {
		return -1, fmt.Errorf("Could not fetch owner of reservation %s: ERR %s", res.key, err.Error())
	}
	if source != res.source {
		return -1, fmt.Errorf("Reservation for %s has unknown owner %s", res.key, source)
	}

	// Extend reservation
	success, err := redis.Int(conn.Do("EXPIRE", res.key, res.ttl.Seconds()))
	if err != nil || success != 1 {
		return -1, fmt.Errorf("Could not extend reservation %s: ERR %s", res.key, err.Error())
	}
	return success, nil
}
