package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/olivere/elastic.v5"
)

var (
	addr        = flag.String("addr", "http://localhost:9200", "Elastic search HTTP address")
	index       = flag.String("index", "wikipediax", "Index to perform queries")
	resultsPath = flag.String("results_path", "~/results", "")
	expID       = flag.String("exp_id", "1", "")
	duration    = flag.Duration("duration", 30*time.Second, "Time sending load to the server.")
	cint        = flag.Duration("cint", 5*time.Second, "Interval between metrics collection.")
	load        = flag.String("load", "const:10", "Describes the load impressed on the server")
	dict        = flag.String("dict", "small_dict.txt", "Dictionary of terms to use. One term per line.")
	timeout     = flag.Duration("timeout", 5*time.Second, "Timeout to be used in connections to ES.")
)

var (
	succs, errs, reqs, shed uint64
	logger                  = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
	respTimeStats           = NewResponseTimeStats()
)

type Config struct {
	Dict            string        `json:"dict"`
	Index           string        `json:"index"`
	Addr            string        `json:"addr"`
	ResultsPath     string        `json:"results_path"`
	ExpID           string        `json:"exp_id"`
	Timeout         time.Duration `json:"timeout"`
	Duration        time.Duration `json:"duration"`
	CollectInterval time.Duration `json:"cint"`
	Load            string        `json:"load"`
}

func main() {
	flag.Parse()
	gen, err := NewLoadGen(*load)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	logger.Printf("Starting sending load: LoadDef:%s Duration:%v\n", *load, *duration)

	client, err := elastic.NewClient(
		elastic.SetErrorLog(logger),
		elastic.SetURL(*addr),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetMaxRetries(0),
		elastic.SetHealthcheckTimeout(*timeout),
		elastic.SetHttpClient(&http.Client{
			Timeout: 1 * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   *timeout,
					KeepAlive: *timeout,
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

	config := Config{
		Dict:            *dict,
		Index:           *index,
		ResultsPath:     *resultsPath,
		ExpID:           *expID,
		Timeout:         *timeout,
		Duration:        *duration,
		CollectInterval: *cint,
		Load:            *load,
	}
	b, err := json.MarshalIndent(config, "", "\t")
	if err != nil {
		logger.Fatal(err)
	}
	cFile, err := os.Create(filepath.Join(*resultsPath, "config_"+*expID+".json"))
	if err != nil {
		logger.Fatal(err)
	}
	if _, err := cFile.Write(b); err != nil {
		logger.Fatal(err)
	}
	if err := cFile.Close(); err != nil {
		logger.Fatal(err)
	}
	logger.Printf("Config file written to: %s", cFile.Name())

	endStatsChan := make(chan struct{})
	var statsWaiter sync.WaitGroup
	statsWaiter.Add(1)
	go statsCollector(client, endStatsChan, &statsWaiter)
	logger.Println("Stats collector started.")

	endLoadChan := make(chan struct{})
	pauseLoadChan := make(chan float64)
	nTerms := len(terms)
	fire := gen.GetTicker()
	go func() {
		// Note: Having a single worker or a single load generator is a way to guarantee the load will obey to a
		// certain  distribution. For instance, 10 workers generating load following a Poisson distribution is
		// different  from having Poisson ruling the overall load impressed on the service.
		for i := 0; ; i++ {
			select {
			case <-fire:
				go sendRequest(client, pauseLoadChan, terms[i%nTerms])
			case pt := <-pauseLoadChan:
				time.Sleep(time.Duration(pt*1e9) * time.Nanosecond)
				fmt.Printf("Sleeping: %v\n", time.Duration(pt*1000000000)*time.Nanosecond)
				for range pauseLoadChan {
				} // Emptying pause channel before continue.
			case <-endLoadChan:
				return
			}
		}
	}()
	logger.Printf("Worker has started. Waiting for their hard work...\n")
	start := time.Now()
	gen.Start() // Start firing load.
	<-time.Tick(*duration)
	endLoadChan <- struct{}{}
	close(endLoadChan)
	close(pauseLoadChan)
	dur := time.Now().Sub(start).Seconds()

	logger.Printf("Done. AVGQPS:%.2f #REQS:%d #SUCC:%d #ERR:%d #SHED:%d AVGTP:%.2f", float64(reqs)/dur, reqs, succs, errs, shed, float64(succs)/dur)
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

func sendRequest(client *elastic.Client, pauseChan chan float64, term string) {
	atomic.AddUint64(&reqs, 1)
	q := elastic.NewTermQuery("text", term)
	resp, err := client.Search().Index(*index).Query(q).Do(context.Background())
	if err != nil {
		atomic.AddUint64(&errs, 1)
		return
	}
	s := resp.StatusCode
	switch {
	case s == http.StatusOK:
		atomic.AddUint64(&succs, 1)
		respTimeStats.Record(s, resp.TookInMillis)
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
