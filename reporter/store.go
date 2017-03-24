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
		return &CSVHistogram{fileAndWriter{f, w}, i.(*metrics.Histogram)}, nil
	case *metrics.Counter:
		w.Write([]string{"ts", "value"})
		return &CSVCounter{fileAndWriter{f, w}, i.(*metrics.Counter)}, nil
	case *metrics.IntGauge:
		w.Write([]string{"ts", "value"})
		return &CSVIntGauge{fileAndWriter{f, w}, i.(*metrics.IntGauge)}, nil
	case *metrics.IntGaugeSet:
		igs := i.(*metrics.IntGaugeSet)
		w.Write(append([]string{"ts"}, igs.Header...))
		return &CSVIntGaugeSet{fileAndWriter{f, w}, igs}, nil
	}
	return nil, nil
}

type fileAndWriter struct {
	f *os.File
	w *csv.Writer
}

func (fw *fileAndWriter) Close() error {
	return fw.f.Close()
}

type CSVIntGaugeSet struct {
	fileAndWriter
	igs *metrics.IntGaugeSet
}

func (csv *CSVIntGaugeSet) Write(now int64) error {
	values := csv.igs.Get()
	strValues := make([]string, len(values)+1)
	strValues[0] = strconv.FormatInt(now, 10)
	for i, v := range csv.igs.Get() {
		strValues[i+1] = strconv.FormatInt(v, 10)
	}
	csv.w.Write(strValues)
	csv.w.Flush()
	if err := csv.w.Error(); err != nil {
		return csv.w.Error()
	}
	return nil
}

type CSVHistogram struct {
	fileAndWriter
	v *metrics.Histogram
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
	fileAndWriter
	v *metrics.Counter
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
	fileAndWriter
	v *metrics.IntGauge
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
