package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/olivere/elastic.v5"
)

var (
	workers     = flag.Int("c", 50, "Number of concurrent workers.")
	addr        = flag.String("addr", "http://localhost:9200", "Elastic search HTTP address")
	index       = flag.String("index", "wikipediax", "Index to perform queries")
	resultsPath = flag.String("results_path", "~/results", "")
	expID       = flag.String("exp_id", "1", "")
	duration    = flag.Duration("duration", 30*time.Second, "Time sending load to the server.")
	cint        = flag.Duration("cint", 5*time.Second, "Interval between metrics collection.")
	load        = flag.String("load", "const:10", "Describes the load impressed on the server")
	dict        = flag.String("dict", "small_dict.txt", "Dictionary of terms to use. One term per line.")
)

var (
	succs, errs, reqs, shed uint64
	logger                  = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
	respTimeStats           = &ResponseTimeStats{}
)

func main() {
	flag.Parse()
	gen, err := ParseLoadDef(*load)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	logger.Printf("Starting sending load: #Workers:%d LoadDef:%s Duration:%v\n", *workers, *load, *duration)

	client, err := elastic.NewClient(
		elastic.SetErrorLog(logger),
		elastic.SetURL(*addr),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetMaxRetries(0),
		elastic.SetHttpClient(&http.Client{
			Timeout: 500 * time.Millisecond,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   500 * time.Millisecond,
					KeepAlive: 500 * time.Millisecond,
				}).Dial,
			},
		}))
	if err != nil {
		logger.Fatal(err)
	}

	if *dict == "" {
		logger.Fatalf("Dictionary file path can not be empty. Please set --dict flag.")
	}

	dictF, err := os.Open(*dict)
	if err != nil {
		logger.Fatal(err)
	}
	var terms []string
	scanner := bufio.NewScanner(dictF)
	for scanner.Scan() {
		terms = append(terms, scanner.Text())
	}
	logger.Printf("Terms dictionary successfully scanned. Loaded %d terms.", len(terms))

	endStatsChan := make(chan struct{})
	var statsWaiter sync.WaitGroup
	statsWaiter.Add(1)
	go statsCollector(client, endStatsChan, &statsWaiter)
	logger.Println("Stats collector started.")

	endLoadChan := make(chan struct{}, *workers)
	pauseLoadChan := make(chan float64, *workers)
	var loadWaiter sync.WaitGroup
	for i := 0; i < *workers; i++ {
		loadWaiter.Add(1)
		go worker(client, pauseLoadChan, endLoadChan, &loadWaiter, gen, terms)
	}
	logger.Printf("%d load workers have started. Waiting for their hard work...\n", *workers)

	start := time.Now()
	<-time.Tick(*duration)
	for i := 0; i < *workers; i++ {
		endLoadChan <- struct{}{}
	}
	loadWaiter.Wait()
	close(endLoadChan)
	close(pauseLoadChan)

	dur := time.Now().Sub(start).Seconds()
	logger.Printf("Done. QPS:%.2f #REQS:%d #SUCC:%d #ERR:%d #SHED:%d TP:%.2f", float64(reqs)/dur, reqs, succs, errs, shed, float64(succs)/dur)
	atomic.StoreUint64(&reqs, 0)
	atomic.StoreUint64(&succs, 0)
	atomic.StoreUint64(&errs, 0)
	atomic.StoreUint64(&shed, 0)

	logger.Println("Finishing stats collection...")
	endStatsChan <- struct{}{} // send signal to finish stats worker.
	statsWaiter.Wait()         // wait for stats worker to do its stuff.
	close(endStatsChan)
	logger.Printf("Done. Results can be found at %s\n", *resultsPath)
	logger.Println("Load test finished successfully.")
}

func worker(client *elastic.Client, pause chan float64, end chan struct{}, wg *sync.WaitGroup, gen LoadGen, terms []string) {
	defer wg.Done()
	ctx := context.Background()
	fire := gen.GetTicker()
	// Avoiding default random synchronization block. Each worker can have its own random source.
	// http://blog.sgmansfield.com/2016/01/the-hidden-dangers-of-default-rand/
	nTerms := len(terms)
	// NOTE: Tried to generate a random number per request and that damages performance in high load tests.
	// Modulo operation is a way cheaper than the random number generator, even if we get rid of the synchronization
	// lock from the default random generator.
	start := rand.New(rand.NewSource(time.Now().Unix())).Intn(nTerms)
	for {
		select {
		case <-fire:
			sendRequest(ctx, client, pause, terms[start%nTerms])
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

func sendRequest(ctx context.Context, client *elastic.Client, pauseChan chan float64, term string) {
	atomic.AddUint64(&reqs, 1)
	q := elastic.NewTermQuery("text", term)
	resp, err := client.Search().Index(*index).Query(q).Do(ctx)
	if err != nil {
		atomic.AddUint64(&errs, 1)
		return
	}
	s := resp.StatusCode
	switch {
	case s == http.StatusOK:
		atomic.AddUint64(&succs, 1)
		respTimeStats.Record(resp.TookInMillis)
	case s == http.StatusTooManyRequests || s == http.StatusServiceUnavailable:
		atomic.AddUint64(&shed, 1)
		ra := resp.Header.Get("Retry-After")
		if ra != "" {
			pt, err := strconv.ParseFloat(ra, 64)
			if err != nil {
				logger.Printf("%q\n", err)
			} else {
				pauseChan <- pt
			}
		}
	default:
		atomic.AddUint64(&errs, 1)
	}
}
