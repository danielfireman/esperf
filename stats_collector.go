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

	tpF := newFile("tp")
	defer tpF.Close()
	tp := csv.NewWriter(bufio.NewWriter(tpF))
	writeTpHeader(tp)

	cpuF := newFile("cpu")
	defer cpuF.Close()
	cpu := csv.NewWriter(bufio.NewWriter(cpuF))
	writeCPUHeader(cpu)

	lF := newFile("latency")
	defer lF.Close()
	latency := csv.NewWriter(bufio.NewWriter(lF))
	writeLatencyHeader(latency)

	collect := func() {
		nss := client.NodesStats().Metric("jvm", "indices", "process")
		resp, err := nss.Do(context.Background())
		if err != nil {
			logger.Printf("%q\n", err)
			return
		}
		var ns *elastic.NodesStatsNode
		for _, ns = range resp.Nodes {
		}
		ts := time.Now().UnixNano() / 1000000
		s, count := respTimeStats.Snapshot()
		writeMem(ns, memPools, ts)
		writeGC(ns, gc, ts)
		writeTp(tp, ts, count)
		writeCPU(ns, cpu, ts)
		writeLatency(s, latency, ts)
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

func writeLatencyHeader(w *csv.Writer) {
	w.Write([]string{"ts", "p50", "p90", "p99", "p999"})
	w.Flush()
}

func writeLatency(s *Snapshot, w *csv.Writer, ts int64) {
	w.Write([]string{
		strconv.FormatInt(ts, 10),
		fmt.Sprintf("%.2f", float64(s.Quantile(0.5))),
		fmt.Sprintf("%.2f", float64(s.Quantile(0.9))),
		fmt.Sprintf("%.2f", float64(s.Quantile(0.99))),
		fmt.Sprintf("%.2f", float64(s.Quantile(0.999)))})
	w.Flush()
}

func writeTpHeader(w *csv.Writer) {
	w.Write([]string{"ts", "count"})
	w.Flush()
}

func writeTp(w *csv.Writer, ts int64, count int64) {
	w.Write([]string{
		strconv.FormatInt(ts, 10),
		strconv.FormatInt(count, 10)})
	w.Flush()
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
