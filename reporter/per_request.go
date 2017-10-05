package reporter

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
)

// PerRequestReport that tracks and keeps metrics for each request across the whole
// load test.
type PerRequestReport struct {
	f *os.File
	w *csv.Writer
	c chan []string
}

func NewPerRequestReport(path string) (*PerRequestReport, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(bufio.NewWriter(f))
	w.Write([]string{"ts", "code", "took_in_millis", "id"})
	if err := w.Error(); err != nil {
		return nil, w.Error()
	}
	return &PerRequestReport{f, w, make(chan []string, 10000)}, nil
}

func (p *PerRequestReport) RequestProcessed(ts int64, code int, tookInMillis, latency int64, id int) {
	p.c <- []string{
		fmt.Sprintf("%d", ts),
		fmt.Sprintf("%d", code),
		fmt.Sprintf("%d", tookInMillis),
		fmt.Sprintf("%d", latency),
		fmt.Sprintf("%d", id),
	}
}

func (p *PerRequestReport) Start() {
	go func() {
		for t := range p.c {
			p.w.Write(t)
		}
	}()
}

func (p *PerRequestReport) Finish() {
	close(p.c)
	p.w.Flush()
	p.f.Close()
}
