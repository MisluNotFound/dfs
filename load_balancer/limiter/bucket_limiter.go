package limiter

import (
	"log"
	"sync"
	"time"
)

type Limiter struct {
	lock     sync.Mutex
	capacity int64
	tokens   int64
	rate     time.Duration
	lastTime time.Time
}

func NewLimiter(rate time.Duration, capacity int64) *Limiter {
	if capacity < 1 {
		log.Fatal("limiter capacity must be greater than zero")
	}
	return &Limiter{
		lock:     sync.Mutex{},
		rate:     rate,
		capacity: capacity,
		tokens:   0,
		lastTime: time.Time{},
	}
}

func (l *Limiter) GetToken() bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	now := time.Now()
	if now.Sub(l.lastTime) > l.rate {
		l.tokens += int64(now.Sub(l.lastTime) / l.rate)
		if l.tokens > l.capacity {
			l.tokens = l.capacity
		}
		l.lastTime = now
	}

	if l.tokens > 0 {
		l.tokens -= 1
		return true
	}
	return false
}
