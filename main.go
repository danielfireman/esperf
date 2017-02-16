package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"sync"

	"gopkg.in/olivere/elastic.v5"
)

var (
	workers     = flag.Int("c", 50, "Number of concurrent workers.")
	qps         = flag.Int("qps", 10, "Number of requests per second impressed on the server.")
	addr        = flag.String("addr", "http://localhost:9200", "Elastic search HTTP address")
	index       = flag.String("index", "wikipediax", "Index to perform queries")
	resultsPath = flag.String("results_path", "~/results", "")
	expID       = flag.String("exp_id", "1", "")
	duration    = flag.Duration("duration", 30*time.Second, "Time sending load to the server.")
	cint        = flag.Duration("cint", 5*time.Second, "Interval between metrics collection.")
)

var (
	succs, errs, reqs, shed uint64
	pauseChan               = make(chan float64, *workers)
	endChan                 = make(chan struct{}, *workers)
	statsWaiter             = sync.WaitGroup{}
)

func main() {
	flag.Parse()
	fmt.Printf("Starting sending load:\n\t#Workers:%d\n\tGlobalQPS:%d\n\tDuration:%v\n", *workers, *qps, *duration)

	logger := log.New(os.Stdout, "elastic", log.LstdFlags)
	elastic.SetErrorLog(logger)
	elastic.SetTraceLog(logger)
	elastic.SetInfoLog(logger)
	elastic.SetMaxRetries(0)
	elastic.SetSniff(false)

	statsWaiter.Add(1)
	go statsCollector(endChan)
	for i := 0; i < *workers; i++ {
		go worker(pauseChan, endChan)
	}

	start := time.Now()
	end := time.Tick(*duration)
	<-end // Blocking until the of the load test.
	for i := 0; i < *workers+1; i++ {
		endChan <- struct{}{}
	}
	dur := time.Now().Sub(start).Seconds()
	fmt.Printf("ts,qps,totalReq,succReq,errReq,shed,throughput\n")
	fmt.Printf("%d,%.2f,%d,%d,%d,%d,%.2f\n", time.Now().Unix(), float64(reqs)/dur, reqs, succs, errs, shed, float64(succs)/dur)
	atomic.StoreUint64(&reqs, 0)
	atomic.StoreUint64(&succs, 0)
	atomic.StoreUint64(&errs, 0)
	atomic.StoreUint64(&shed, 0)

	statsWaiter.Wait()
	close(endChan)
	close(pauseChan)
}

func worker(pause chan float64, end chan struct{}) {
	durSecs := (*duration).Seconds()
	numReq := (*qps * int(durSecs)) / *workers
	if numReq == 0 {
		log.Fatalf("To few requests per worker, please increase the qps or decrease the number of workers")
	}

	client, err := elastic.NewClient(elastic.SetURL(*addr))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	fire := time.Tick(time.Duration((durSecs*float64(1e9))/float64(numReq)) * time.Nanosecond)
	for i := 0; i < numReq; i++ {
		select {
		case <-fire:
			sendRequest(ctx, client)
		case pt := <-pause:
			time.Sleep(time.Duration(pt*1000000000) * time.Nanosecond)
			fmt.Printf("Sleeping: %v\n", time.Duration(pt*1000000000)*time.Nanosecond)
			for range pause {
			} // Emptying pause channel before continue.
		case <-end:
			return
		}
	}
}

func sendRequest(ctx context.Context, client *elastic.Client) {
	atomic.AddUint64(&reqs, 1)
	q := elastic.NewTermQuery("text", "America")
	resp, err := client.Search().Index(*index).Query(q).Do(ctx)
	if err != nil {
		atomic.AddUint64(&errs, 1)
		return
	}
	s := resp.StatusCode
	switch {
	case s == http.StatusOK:
		atomic.AddUint64(&succs, 1)
	case s == http.StatusTooManyRequests || s == http.StatusServiceUnavailable:
		atomic.AddUint64(&shed, 1)
		ra := resp.Header.Get("Retry-After")
		if ra != "" {
			pt, err := strconv.ParseFloat(ra, 64)
			if err != nil {
				fmt.Printf("%q\n", err)
			} else {
				pauseChan <- pt
			}
		}
	default:
		atomic.AddUint64(&errs, 1)
	}
}

func statsCollector(end chan struct{}) {
	defer statsWaiter.Done()

	client, err := elastic.NewClient(elastic.SetURL(*addr))
	if err != nil {
		log.Fatalf("Error creating ES stats client: %q", err)
	}

	ctx := context.Background()
	r, err := client.Count(*index).Do(ctx)
	if err != nil {
		log.Fatalf("Could not infer the number of shards. Err: %q", err)
	}
	shards := r.Shards.Successful

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

	collect := func() {
		s := client.NodesStats().Metric("jvm", "indices", "process")
		resp, err := s.Do(ctx)
		if err != nil {
			fmt.Printf("%q\n", err)
		}
		var ns *elastic.NodesStatsNode
		for _, ns = range resp.Nodes {
		}
		ts := ns.JVM.Timestamp
		writeMem(ns, memPools, ts)
		writeGC(ns, gc, ts)
		writeTp(ns, tp, ts, shards)
		writeCPU(ns, cpu, ts)
	}

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
	f, err := os.Create(filepath.Join(*resultsPath, fName+"_"+*expID+".csv"))
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func writeTpHeader(w *csv.Writer) {
	w.Write([]string{"ts", "time", "count"})
	w.Flush()
}

func writeTp(stats *elastic.NodesStatsNode, w *csv.Writer, ts int64, shards int) {
	s := stats.Indices.Search
	w.Write([]string{
		strconv.FormatInt(ts, 10),
		fmt.Sprintf("%.2f", float64(s.QueryTotal)/float64(shards)),
		strconv.FormatInt(s.QueryTimeInMillis, 10)})
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
