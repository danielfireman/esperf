package main

import (
	"sync"

	"github.com/spenczar/tdigest"
)

func newSnapshot(values []int64) *Snapshot {
	td := tdigest.New()
	for _, v := range values {
		td.Add(float64(v), 1)
	}
	return &Snapshot{td, len(values)}
}

type Snapshot struct {
	estimator tdigest.TDigest
	count int
}

// Estimate the qth quantile value of the snapshot. The input value of
// q should be in the range [0.0, 1.0]; if  it is outside that range, it will
// be clipped into it automatically.
func (s *Snapshot) Quantile(quantiles ...float64) []float64 {
	ret := make([]float64, len(quantiles), len(quantiles))
	for i, q := range quantiles {
		ret[i] = s.estimator.Quantile(q)
	}
	return ret
}

func (s *Snapshot) Count() int {
	return s.count
}

func NewHistogram() *Histogram {
	return &Histogram{}
}

type Histogram struct {
	sync.Mutex
	td    tdigest.TDigest
	buff  []int64
}

func (s *Histogram) Record(v int64) {
	s.Lock()
	defer s.Unlock()
	s.buff = append(s.buff, v)
}

func (s *Histogram) Snapshot() *Snapshot{
	s.Lock()
	// Snapshotting buffers. Need to keep this race region as
	// small as possible.
	vSnapshot := make([]int64, len(s.buff), len(s.buff))
	copy(vSnapshot, s.buff)
	s.buff = nil
	s.Unlock()
	return newSnapshot(vSnapshot)
}
