package main

import (
	"sync"

	"github.com/spenczar/tdigest"
)

type Snapshot struct {
	td tdigest.TDigest
}

func (s *Snapshot) Quantile(q float64) float64 {
	return s.td.Quantile(q)
}

type ResponseTimeStats struct {
	sync.Mutex
	count int64
	td    tdigest.TDigest
	buff  []int64
}

func (s *ResponseTimeStats) Record(v int64) {
	s.Lock()
	defer s.Unlock()
	s.buff = append(s.buff, v)
	s.count++
}

func (s *ResponseTimeStats) Snapshot() (*Snapshot, int64) {
	s.Lock()
	auxBuff := make([]int64, len(s.buff))
	copy(auxBuff, s.buff)
	s.buff = nil
	count := s.count
	s.Unlock()

	td := tdigest.New()
	for _, v := range auxBuff {
		td.Add(float64(v), 1)
	}

	return &Snapshot{td}, count
}
