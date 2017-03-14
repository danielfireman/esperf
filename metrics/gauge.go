package metrics

import (
	"sync"
	"sync/atomic"
)

func NewIntGauge() *IntGauge {
	return &IntGauge{}
}

func NewFloatGauge() *FloatGauge {
	return &FloatGauge{}
}

type IntGauge struct {
	v int64
}

func (c *IntGauge) Set(v int64) *IntGauge {
	atomic.StoreInt64(&c.v, v)
	return c
}

func (c *IntGauge) Get() int64 {
	return atomic.LoadInt64(&c.v)
}

type FloatGauge struct {
	sync.Mutex
	v float64
}

func (c *FloatGauge) Set(v float64) *FloatGauge {
	c.Lock()
	defer c.Unlock()
	c.v = v
	return c
}

func (c *FloatGauge) Get() float64 {
	return c.v
}
