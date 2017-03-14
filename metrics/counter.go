package metrics

import (
	"sync/atomic"
)

func NewCounter() *Counter {
	return &Counter{}
}

// Simple incrementing and decrementing 64-bit integer:
type Counter struct {
	v int64
}

// Increments the counter.
func (c *Counter) Inc() {
	atomic.AddInt64(&c.v, 1)
}

// Decrements the counter.
func (c *Counter) Dec() {
	atomic.AddInt64(&c.v, -1)
}

func (c *Counter) Get() int64 {
	return atomic.LoadInt64(&c.v)
}
