package cmd

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/danielfireman/esperf/metrics"
	"github.com/danielfireman/esperf/reporter"
	"github.com/spf13/cobra"
	"gopkg.in/olivere/elastic.v5"
)

var (
	addr        string
	index       string
	resultsPath string
	expID       string
	duration    time.Duration
	cint        time.Duration
	load        string
	dict        string
	timeout     time.Duration
	verbose     bool
)

func init() {
	runCmd.Flags().StringVar(&addr, "addr", "http://localhost:9200", "Elastic search HTTP address")
	runCmd.Flags().StringVar(&index, "index", "wikipediax", "Index to perform queries")
	runCmd.Flags().StringVar(&resultsPath, "results_path", "", "")
	runCmd.Flags().StringVar(&expID, "exp_id", "1", "")
	runCmd.Flags().DurationVarP(&duration, "duration", "d", 30*time.Second, "Time sending load to the server.")
	runCmd.Flags().DurationVar(&cint, "cint", 5*time.Second, "Interval between metrics collection.")
	runCmd.Flags().StringVar(&load, "load", "const:10", "Describes the load impressed on the server")
	runCmd.Flags().StringVar(&dict, "dict", "small_dict.txt", "Dictionary of terms to use. One term per line.")
	runCmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "Timeout to be used in connections to ES.")
	runCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Prints out requests and responses. Good for debugging.")
}

var r runner
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Runs a performance testing and collects metrics.",
	Long:  "Multiplatform command line tool to load test and collect metrics from your ElasticSearch deployment.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		r = runner{}

		if resultsPath == "" {
			return fmt.Errorf("Results path can not be empty. Please set --results_path flag.")
		}

		// Load generator.
		r.loadGen, err = NewLoadGen(load)
		if err != nil {
			return err
		}
		log.Printf("Load spec successfully parsed.\n")

		// Dictionary of queries.
		if dict == "" {
			return fmt.Errorf("Dictionary file path can not be empty. Please set --dict flag.")
		}
		dictF, err := os.Open(dict)
		if err != nil {
			log.Fatal(err.Error())
		}
		scanner := bufio.NewScanner(dictF)
		for scanner.Scan() {
			r.terms = append(r.terms, scanner.Text())
		}
		log.Printf("Terms dictionary successfully scanned. Loaded %d terms.\n", len(r.terms))

		// Elastic client.
		elasticLogger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
		opts := []elastic.ClientOptionFunc{
			elastic.SetErrorLog(elasticLogger),
			elastic.SetURL(addr),
			elastic.SetHealthcheck(false),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false),
			elastic.SetMaxRetries(0),
			elastic.SetHealthcheckTimeout(timeout),
			elastic.SetHttpClient(&http.Client{
				Timeout: timeout,
				Transport: &http.Transport{
					Dial: (&net.Dialer{
						Timeout:   timeout,
						KeepAlive: timeout,
					}).Dial,
					TLSHandshakeTimeout: timeout,
				},
			}),
		}
		if verbose {
			opts = append(opts, elastic.SetTraceLog(elasticLogger))
			opts = append(opts, elastic.SetInfoLog(elasticLogger))
			log.Printf("Verbose mode activated.\n")
		}
		r.client, err = elastic.NewClient(opts...)
		if err != nil {
			return err
		}
		log.Printf("Elastic client successfully set.\n")

		r.config = Config{
			Dict:            dict,
			Index:           index,
			ResultsPath:     resultsPath,
			ExpID:           expID,
			Timeout:         timeout,
			Duration:        duration,
			CollectInterval: cint,
			Load:            load,
		}

		collector := NewESCollector(r.client)
		r.requestsSent = metrics.NewCounter()
		r.errors = metrics.NewCounter()
		r.responseTimes = metrics.NewHistogram()
		r.pauseTimes = metrics.NewHistogram()
		r.report, err = reporter.New(
			r.config.CollectInterval,
			r.config.Timeout,
			reporter.MetricToCSV(r.responseTimes, csvFilePath("response.time", r.config)),
			reporter.MetricToCSV(r.pauseTimes, csvFilePath("pause.time", r.config)),
			reporter.MetricToCSV(r.requestsSent, csvFilePath("requests.sent", r.config)),
			reporter.MetricToCSV(r.errors, csvFilePath("errors", r.config)),
			reporter.AddCollector(collector),
			reporter.MetricToCSV(collector.Mem.YoungPoolUsedBytes, csvFilePath("young.pool.used.bytes", r.config)),
			reporter.MetricToCSV(collector.Mem.YoungPoolMaxBytes, csvFilePath("young.pool.max.bytes", r.config)),
			reporter.MetricToCSV(collector.Mem.TenuredPoolUsedBytes, csvFilePath("tenured.pool.used.bytes", r.config)),
			reporter.MetricToCSV(collector.Mem.TenuredPoolMaxBytes, csvFilePath("tenured.pool.max.bytes", r.config)),
			reporter.MetricToCSV(collector.Mem.SurvivorPoolUsedBytes, csvFilePath("survivor.pool.used.bytes", r.config)),
			reporter.MetricToCSV(collector.Mem.SurvivorPoolMaxBytes, csvFilePath("survivor.pool.max.bytes", r.config)),
			reporter.MetricToCSV(collector.CPU.Percent, csvFilePath("cpu.percent", r.config)),
			reporter.MetricToCSV(collector.CPU.TotalMillis, csvFilePath("cpu.total.ms", r.config)),
			reporter.MetricToCSV(collector.GC.YoungCount, csvFilePath("gc.young.count", r.config)),
			reporter.MetricToCSV(collector.GC.YoungTimeMillis, csvFilePath("gc.young.time.ms", r.config)),
			reporter.MetricToCSV(collector.GC.FullCount, csvFilePath("gc.full.count", r.config)),
			reporter.MetricToCSV(collector.GC.FullTimeMillis, csvFilePath("gc.full.time.ms", r.config)),
		)
		if err != nil {
			return err
		}
		log.Printf("Collector and reporter successfully set.\n")
		log.Printf("Starting sending load: LoadDef:%s Duration:%v\n", load, duration)
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		defer r.config.Write()
		if err := r.Run(); err != nil {
			return err
		}
		return nil
	},
}

type runner struct {
	loadGen LoadGen
	client  *elastic.Client
	terms   []string
	config  Config
	report  *reporter.Reporter

	responseTimes *metrics.Histogram
	pauseTimes    *metrics.Histogram
	requestsSent  *metrics.Counter
	errors        *metrics.Counter
}

func csvFilePath(name string, c Config) string {
	return filepath.Join(c.ResultsPath, name+"_"+c.ExpID+".csv")
}

func (r *runner) Run() error {
	r.report.Start()

	endLoadChan := make(chan struct{})
	pauseLoadChan := make(chan time.Duration)
	nTerms := len(r.terms)
	fire := r.loadGen.GetTicker()
	go func() {
		// Note: Having a single worker or a single load generator is a way to guarantee the load will obey to a
		// certain  distribution. For instance, 10 workers generating load following a Poisson distribution is
		// different  from having Poisson ruling the overall load impressed on the service.
		for i := 0; ; i++ {
			select {
			case <-fire:
				go sendRequest(r.client, pauseLoadChan, r.terms[i%nTerms])
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
	log.Printf("Worker has started. Waiting for their hard work...\n")

	r.config.StartTime = time.Now()
	r.loadGen.Start() // Start firing load.
	<-time.Tick(duration)
	endLoadChan <- struct{}{}
	close(endLoadChan)
	close(pauseLoadChan)
	r.config.FinishTime = time.Now()

	log.Println("Finishing stats collection...")
	r.report.Finish()
	log.Printf("Done. Results can be found at %s\n", r.config.ResultsPath)
	log.Println("Load test finished successfully.")
	return nil
}

func sendRequest(client *elastic.Client, pauseChan chan time.Duration, term string) {
	r.requestsSent.Inc()
	q := elastic.NewMatchPhraseQuery("text", term)
	resp, err := client.Search().Index(r.config.Index).Query(q).Do(context.Background())
	if err != nil {
		elasticErr, ok := err.(*elastic.Error)
		if !ok {
			log.Fatal(err.Error())
		}
		if elasticErr.Status == http.StatusServiceUnavailable || elasticErr.Status == http.StatusTooManyRequests {
			ra := elasticErr.Header.Get("Retry-After")
			if ra == "" {
				log.Fatal("Could not extract retry-after information")
			}
			pt, err := strconv.ParseFloat(ra, 64)
			if err != nil {
				log.Fatal("Could not extract retry-after information")
			}
			pauseMillis := int64(pt * 1e9)
			r.pauseTimes.Record(pauseMillis)
			pauseChan <- time.Duration(pauseMillis)
			return
		}
		r.errors.Inc()
		return
	}
	s := resp.StatusCode
	switch {
	case s == http.StatusOK:
		r.responseTimes.Record(resp.TookInMillis)
	default:
		r.errors.Inc()
	}
}
