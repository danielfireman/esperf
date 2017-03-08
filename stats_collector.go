package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"gopkg.in/olivere/elastic.v5"
)

func statsCollector(client *elastic.Client, end chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	mF := newFile("mem.pools")
	defer mF.Close()
	memPools := csv.NewWriter(bufio.NewWriter(mF))
	writeMemHeader(memPools)

	gcF := newFile("gc")
	defer gcF.Close()
	gc := csv.NewWriter(bufio.NewWriter(gcF))
	writeGCHeader(gc)

	cpuF := newFile("cpu")
	defer cpuF.Close()
	cpu := csv.NewWriter(bufio.NewWriter(cpuF))
	writeCPUHeader(cpu)

	pauseF := newFile("pause")
	defer pauseF.Close()
	pause := csv.NewWriter(bufio.NewWriter(pauseF))
	writePauseHeader(pause)

	tpFs := make(map[int]*os.File)
	tpWriters := make(map[int]*csv.Writer)
	defer func() {
		for _, f := range tpFs {
			f.Close()
		}
	}()

	latencyFs := make(map[int]*os.File)
	latencyWriters := make(map[int]*csv.Writer)
	defer func() {
		for _, f := range latencyFs {
			f.Close()
		}
	}()

	collect := func() {
		nss := client.NodesStats().Metric("jvm", "process")
		resp, err := nss.Do(context.Background())
		if err != nil {
			logger.Printf("%q\n", err)
			return
		}
		ts := time.Now().UnixNano() / 1000000

		var ns *elastic.NodesStatsNode
		for _, ns = range resp.Nodes {
		}
		writeMem(ns, memPools, ts)
		writeGC(ns, gc, ts)
		writeCPU(ns, cpu, ts)
		writePause(pause, ts, pauseHistogram.Snapshot())

		snapshots := make(map[int]*Snapshot)
		count := make(map[int]int)
		for code, h := range responseTimeStats {
			s := h.Snapshot()
			snapshots[code] = s
			count[code] = s.Count()
		}
		writeTp(tpFs, tpWriters, ts, count)
		writeLatency(latencyFs, latencyWriters, ts, snapshots)
	}

	// TODO(danielfireman): Make cint a function parameter.
	fire := time.Tick(*cint)
	for {
		select {
		case <-fire:
			collect()

		case <-end:
			collect()
			return
		}
	}
}

func newFile(fName string) *os.File {
	// TODO(danielfireman): Make resultsPath a functionParamter
	// TODO(danielfireman): Make expID a functionParamter
	f, err := os.Create(filepath.Join(*resultsPath, fName+"_"+*expID+".csv"))
	if err != nil {
		logger.Fatal(err)
	}
	return f
}

func writeLatency(fMap map[int]*os.File, wMap map[int]*csv.Writer, ts int64, snapshots map[int]*Snapshot) {
	for code, s := range snapshots {
		f, ok := fMap[code]
		if !ok {
			f = newFile(fmt.Sprintf("latency.%d", code))
			fMap[code] = f
		}
		w, ok := wMap[code]
		if !ok {
			w = csv.NewWriter(bufio.NewWriter(f))
			if err := w.Write([]string{"ts", "p50", "p90", "p99", "p999"}); err != nil {
				logger.Fatal(err)
			}
			w.Flush()
			if err := w.Error(); err != nil {
				logger.Fatal(err)
			}
			wMap[code] = w
		}
		q := s.Quantile(0.5, 0.9, 0.99, 0.999)
		w.Write([]string{
			strconv.FormatInt(ts, 10),
			fmt.Sprintf("%.2f", float64(q[0])),
			fmt.Sprintf("%.2f", float64(q[1])),
			fmt.Sprintf("%.2f", float64(q[2])),
			fmt.Sprintf("%.2f", float64(q[3]))})
		w.Flush()
		if err := w.Error(); err != nil {
			logger.Fatal(err)
		}
	}
}

func writePauseHeader(w *csv.Writer) {
	if err := w.Write([]string{"ts", "p50", "p90", "p99", "p999"}); err != nil {
		logger.Fatal(err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		logger.Fatal(err)
	}
}

func writePause(w *csv.Writer, ts int64, s *Snapshot) {
	q := s.Quantile(0.5, 0.9, 0.99, 0.999)
	w.Write([]string{
		strconv.FormatInt(ts, 10),
		fmt.Sprintf("%.2f", float64(q[0])),
		fmt.Sprintf("%.2f", float64(q[1])),
		fmt.Sprintf("%.2f", float64(q[2])),
		fmt.Sprintf("%.2f", float64(q[3]))})
	w.Flush()
	if err := w.Error(); err != nil {
		logger.Fatal(err)
	}
}

func writeTp(fMap map[int]*os.File, wMap map[int]*csv.Writer, ts int64, countMap map[int]int) {
	for code, count := range countMap {
		f, ok := fMap[code]
		if !ok {
			f = newFile(fmt.Sprintf("tp.%d", code))
			fMap[code] = f
		}
		w, ok := wMap[code]
		if !ok {
			w = csv.NewWriter(bufio.NewWriter(f))
			wMap[code] = w
			if err := w.Write([]string{"ts", "count"}); err != nil {
				logger.Fatal(err)
			}
			w.Flush()
			if err := w.Error(); err != nil {
				logger.Fatal(err)
			}
		}
		if err := w.Write([]string{strconv.FormatInt(ts, 10), strconv.Itoa(count)}); err != nil {
			logger.Fatal(err)
		}
		w.Flush()
		if err := w.Error(); err != nil {
			logger.Fatal(err)
		}
	}
}

func writeGCHeader(w *csv.Writer) {
	w.Write([]string{"ts", "young.time", "young.count", "old.time", "old.count"})
	w.Flush()
}

func writeGC(stats *elastic.NodesStatsNode, w *csv.Writer, ts int64) {
	collectors := stats.JVM.GC.Collectors
	w.Write([]string{
		strconv.FormatInt(ts, 10),
		strconv.FormatInt(collectors["young"].CollectionTimeInMillis, 10),
		strconv.FormatInt(collectors["young"].CollectionCount, 10),
		strconv.FormatInt(collectors["old"].CollectionTimeInMillis, 10),
		strconv.FormatInt(collectors["old"].CollectionCount, 10)})
	w.Flush()
}

func writeMemHeader(w *csv.Writer) {
	w.Write([]string{"ts", "young.max", "young.used", "survivor.max", "survivor.used", "old.max", "old.used"})
	w.Flush()
}

func writeMem(stats *elastic.NodesStatsNode, w *csv.Writer, ts int64) {
	pools := stats.JVM.Mem.Pools
	w.Write([]string{
		strconv.FormatInt(ts, 10),
		strconv.FormatInt(pools["young"].MaxInBytes, 10),
		strconv.FormatInt(pools["young"].UsedInBytes, 10),
		strconv.FormatInt(pools["survivor"].MaxInBytes, 10),
		strconv.FormatInt(pools["survivor"].UsedInBytes, 10),
		strconv.FormatInt(pools["old"].MaxInBytes, 10),
		strconv.FormatInt(pools["old"].UsedInBytes, 10)})
	w.Flush()
}

func writeCPUHeader(w *csv.Writer) {
	w.Write([]string{"ts", "time", "percent"})
	w.Flush()
}

func writeCPU(stats *elastic.NodesStatsNode, w *csv.Writer, ts int64) {
	w.Write([]string{
		strconv.FormatInt(ts, 10),
		strconv.FormatInt(stats.Process.CPU.TotalInMillis, 10),
		strconv.FormatInt(int64(stats.Process.CPU.Percent), 10)})
	w.Flush()
}
