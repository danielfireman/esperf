package main

import (
	"sync"

	"github.com/spenczar/tdigest"
)

func newSnapshot(values map[int][]int64) *Snapshot {
	ret := make(map[int]tdigest.TDigest)
	for code, values := range values {
		td := tdigest.New()
		for _, v := range values {
			td.Add(float64(v), 1)
		}
		ret[code] = td
	}
	return &Snapshot{ret}
}

type Snapshot struct {
	estimators map[int]tdigest.TDigest
}

// Estimate the qth quantile value of the snapshot. The input value of
// q should be in the range [0.0, 1.0]; if  it is outside that range, it will
// be clipped into it automatically.
func (s *Snapshot) Quantile(quantiles ...float64) map[int][]float64 {
	ret := make(map[int][]float64)
	for code, e := range s.estimators {
		for _, q := range quantiles {
			ret[code] = append(ret[code], e.Quantile(q))
		}
	}
	return ret
}

func NewResponseTimeStats() *ResponseTimeStats {
	return &ResponseTimeStats{buff: make(map[int][]int64), count: make(map[int]int64)}
}

type ResponseTimeStats struct {
	sync.Mutex
	count map[int]int64
	td    tdigest.TDigest
	buff  map[int][]int64
}

func (s *ResponseTimeStats) Record(code int, v int64) {
	s.Lock()
	defer s.Unlock()
	s.buff[code] = append(s.buff[code], v)
	s.count[code]++
}

func (s *ResponseTimeStats) Snapshot() (*Snapshot, map[int]int64) {
	s.Lock()
	// Snapshotting buffers. Need to keep this race region as
	// small as possible.
	vSnapshot := make(map[int][]int64)
	for code, buff := range s.buff {
		b := make([]int64, len(buff))
		copy(b, buff)
		vSnapshot[code] = b
	}
	s.buff = make(map[int][]int64)
	cSnapshot := make(map[int]int64)
	for code, c := range s.count {
		cSnapshot[code] = c
	}
	s.count = make(map[int]int64)
	s.Unlock()
	return newSnapshot(vSnapshot), cSnapshot
}
