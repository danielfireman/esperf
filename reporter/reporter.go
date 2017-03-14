package reporter

import (
	"context"
	"log"
	"sync"
	"time"
)

type Collector interface {
	Name() string
	Collect(context.Context) error
}

type Reporter struct {
	endChan    chan struct{}
	waiter     sync.WaitGroup
	interval   time.Duration
	timeout    time.Duration
	stores     []Store
	collectors []Collector
}

type ReportOption func(*Reporter) error

func New(interval time.Duration, timeout time.Duration, opts ...ReportOption) (*Reporter, error) {
	r := Reporter{
		endChan:  make(chan struct{}),
		waiter:   sync.WaitGroup{},
		interval: interval,
		timeout:  timeout,
	}
	for _, opt := range opts {
		if err := opt(&r); err != nil {
			return nil, err
		}
	}
	return &r, nil
}

func AddCollector(c Collector) ReportOption {
	return func(r *Reporter) error {
		r.collectors = append(r.collectors, c)
		return nil
	}
}

func MetricToCSV(metric interface{}, path string) ReportOption {
	return func(r *Reporter) error {
		s, err := CSVStore(metric, path)
		if err != nil {
			return err
		}
		r.stores = append(r.stores, s)
		return nil
	}
}

func (r *Reporter) dumpToStores() {
	now := time.Now().Unix()
	for _, c := range r.collectors {
		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		if err := c.Collect(ctx); err != nil {
			log.Printf("error collecting %s: %q", c.Name(), err)
			return
		}
		cancel()
	}
	for _, s := range r.stores {
		if err := s.Write(now); err != nil {
			log.Printf("error writing to store: %q", err)
		}
	}
}

func (r *Reporter) Start() {
	r.waiter.Add(1)
	go func() {
		defer r.waiter.Done()
		fire := time.Tick(r.interval)
		for {
			select {
			case <-fire:
				r.dumpToStores()

			case <-r.endChan:
				r.dumpToStores()
				return
			}
		}
	}()
}

func (r *Reporter) Finish() {
	r.endChan <- struct{}{}
	r.waiter.Wait()
	close(r.endChan)
}
