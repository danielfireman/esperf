package reporter

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/danielfireman/esperf/metrics"
)

type Store interface {
	io.Closer
	Write(now int64) error
}

func CSVStore(i interface{}, path string) (Store, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(bufio.NewWriter(f))
	switch t := i.(type) {
	default:
		return nil, fmt.Errorf("unexpected metric: %T\n", t)
	case *metrics.Histogram:
		w.Write([]string{"ts", "count", "p50", "p90", "p99", "p999"})
		return &CSVHistogram{f: f, w: w, v: i.(*metrics.Histogram)}, nil
	case *metrics.Counter:
		w.Write([]string{"ts", "value"})
		return &CSVCounter{f: f, w: w, v: i.(*metrics.Counter)}, nil
	case *metrics.IntGauge:
		w.Write([]string{"ts", "value"})
		return &CSVIntGauge{f: f, w: w, v: i.(*metrics.IntGauge)}, nil
	}
	return nil, nil
}

type CSVHistogram struct {
	f *os.File
	w *csv.Writer
	v *metrics.Histogram
}

func (csv *CSVHistogram) Close() error {
	return csv.f.Close()
}

func (csv *CSVHistogram) Write(now int64) error {
	s := csv.v.Snapshot()
	q := s.Quantile(0.5, 0.9, 0.99, 0.999)
	csv.w.Write([]string{
		strconv.FormatInt(now, 10),
		strconv.FormatInt(s.Count(), 10),
		fmt.Sprintf("%.2f", float64(q[0])),
		fmt.Sprintf("%.2f", float64(q[1])),
		fmt.Sprintf("%.2f", float64(q[2])),
		fmt.Sprintf("%.2f", float64(q[3]))})
	csv.w.Flush()
	if err := csv.w.Error(); err != nil {
		return csv.w.Error()
	}
	return nil
}

type CSVCounter struct {
	f *os.File
	w *csv.Writer
	v *metrics.Counter
}

func (csv *CSVCounter) Close() error {
	return csv.f.Close()
}

func (csv *CSVCounter) Write(now int64) error {
	csv.w.Write([]string{
		strconv.FormatInt(now, 10),
		strconv.FormatInt(csv.v.Get(), 10)})
	csv.w.Flush()
	if err := csv.w.Error(); err != nil {
		return csv.w.Error()
	}
	return nil
}

type CSVIntGauge struct {
	f *os.File
	w *csv.Writer
	v *metrics.IntGauge
}

func (csv *CSVIntGauge) Close() error {
	return csv.f.Close()
}

func (csv *CSVIntGauge) Write(now int64) error {
	csv.w.Write([]string{
		strconv.FormatInt(now, 10),
		strconv.FormatInt(csv.v.Get(), 10)})
	csv.w.Flush()
	if err := csv.w.Error(); err != nil {
		return csv.w.Error()
	}
	return nil
}
