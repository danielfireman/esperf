package metrics

import (
	"sync"
	"sync/atomic"
)

func NewIntGaugeSet(header ...string) *IntGaugeSet {
	return &IntGaugeSet{Header: header, v: make([]int64, len(header))}
}

type IntGaugeSet struct {
	sync.Mutex
	v      []int64
	Header []string
}

func (g *IntGaugeSet) Set(v ...int64) {
	g.Lock()
	defer g.Unlock()
	copy(g.v, v)
}

func (g *IntGaugeSet) Get() []int64 {
	g.Lock()
	defer g.Unlock()
	ret := make([]int64, len(g.v), len(g.v))
	copy(ret, g.v)
	return ret
}

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
