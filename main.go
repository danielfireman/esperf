package main

import (
	"fmt"
	"os"

	"github.com/danielfireman/esperf/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

/*

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
	StartTime       time.Time     `json:"start_time"`
}


func main() {
	flag.Parse()
	gen, err := NewLoadGen(*load)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	logger.Printf("Starting sending load: LoadDef:%s Duration:%v\n", *load, *duration)

	opts := []elastic.ClientOptionFunc{
		elastic.SetErrorLog(logger),
		elastic.SetURL(*addr),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetMaxRetries(0),
		elastic.SetHealthcheckTimeout(*timeout),
		elastic.SetHttpClient(&http.Client{
			Timeout: *timeout,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   *timeout,
					KeepAlive: *timeout,
				}).Dial,
				TLSHandshakeTimeout: *timeout,
			},
		}),
	}
	if *verbose {
		opts = append(opts, elastic.SetTraceLog(logger))
		opts = append(opts, elastic.SetInfoLog(logger))
	}
	client, err := elastic.NewClient(opts...)
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
		StartTime:       time.Now(),
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
	pauseLoadChan := make(chan time.Duration)
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
				time.Sleep(pt)
				func() {
					for {
						select {
						case <-pauseLoadChan:
						default:
							return
						}
					} // Emptying pause channel before continue.
				}()
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

func sendRequest(client *elastic.Client, pauseChan chan time.Duration, term string) {
	atomic.AddUint64(&reqs, 1)
	q := elastic.NewMatchPhraseQuery("text", term)
	resp, err := client.Search().Index(*index).Query(q).Do(context.Background())
	if err != nil {
		elasticErr, ok := err.(*elastic.Error)
		if !ok {
			logger.Fatal(err)
		}
		if elasticErr.Status == http.StatusServiceUnavailable || elasticErr.Status == http.StatusTooManyRequests {
			atomic.AddUint64(&shed, 1)
			responseTimeStats.Record(elasticErr.Status, 0)
			ra := elasticErr.Header.Get("Retry-After")
			if ra == "" {
				logger.Fatal("Could not extract retry-after information")
			}
			pt, err := strconv.ParseFloat(ra, 64)
			if err != nil {
				logger.Fatal("Could not extract retry-after information")
			}
			pauseMillis := int64(pt * 1e9)
			pauseHistogram.Record(pauseMillis)
			pauseChan <- time.Duration(pauseMillis)
			return
		}
		atomic.AddUint64(&errs, 1)
		return
	}
	s := resp.StatusCode
	switch {
	case s == http.StatusOK:
		atomic.AddUint64(&succs, 1)
		responseTimeStats.Record(s, resp.TookInMillis)
	default:
		atomic.AddUint64(&errs, 1)
	}
}
*/
