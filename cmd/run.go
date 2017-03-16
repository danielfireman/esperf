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
	"time"

	"github.com/danielfireman/esperf/metrics"
	"github.com/danielfireman/esperf/reporter"
	"github.com/spf13/cobra"
	"gopkg.in/olivere/elastic.v5"
)

var (
	addr        string
	index       string
	verbose     bool
	resultsPath string
	expID       string
	duration    time.Duration
	cint        time.Duration
	load        string
	dict        string
	timeout     time.Duration
)

func init() {
	runCmd.PersistentFlags().StringVar(&addr, "addr", "http://localhost:9200", "Elastic search HTTP address")
	runCmd.PersistentFlags().StringVar(&index, "index", "wikipediax", "Index to perform queries")
	runCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Prints out requests and responses. Good for debugging.")
	runCmd.Flags().StringVar(&resultsPath, "results_path", "", "")
	runCmd.Flags().StringVar(&expID, "exp_id", "1", "")
	runCmd.Flags().DurationVarP(&duration, "duration", "d", 30*time.Second, "Time sending load to the server.")
	runCmd.Flags().DurationVar(&cint, "cint", 5*time.Second, "Interval between metrics collection.")
	runCmd.Flags().StringVar(&load, "load", "const:10", "Describes the load impressed on the server")
	runCmd.Flags().StringVar(&dict, "dict", "small_dict.txt", "Dictionary of terms to use. One term per line.")
	runCmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "Timeout to be used in connections to ES.")
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

		r.pauseChan = make(chan time.Duration)
		r.requestsSent = metrics.NewCounter()
		r.errors = metrics.NewCounter()
		r.responseTimes = metrics.NewHistogram()
		r.pauseTimes = metrics.NewHistogram()
		pauseInterceptor := retrier{
			pauseChan:  r.pauseChan,
			pauseTimes: r.pauseTimes,
		}

		// Elastic client.
		elasticLogger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
		opts := []elastic.ClientOptionFunc{
			elastic.SetErrorLog(elasticLogger),
			elastic.SetURL(addr),
			elastic.SetHealthcheck(false),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false),
			elastic.SetMaxRetries(0),
			elastic.SetRetrier(&pauseInterceptor),
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

		// TODO(danielfireman): Review metrics collection design.
		collector := NewESCollector(r.client)
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
	loadGen   LoadGen
	client    *elastic.Client
	terms     []string
	config    Config
	report    *reporter.Reporter
	pauseChan chan time.Duration

	requestsSent  *metrics.Counter
	responseTimes *metrics.Histogram
	errors        *metrics.Counter
	pauseTimes    *metrics.Histogram
}

func csvFilePath(name string, c Config) string {
	return filepath.Join(c.ResultsPath, name+"_"+c.ExpID+".csv")
}

func (r *runner) Run() error {
	r.report.Start()

	endLoadChan := make(chan struct{})
	nTerms := len(r.terms)
	fire := r.loadGen.GetTicker()
	go func() {
		// Note: Having a single worker or a single load generator is a way to guarantee the load will obey to a
		// certain  distribution. For instance, 10 workers generating load following a Poisson distribution is
		// different  from having Poisson ruling the overall load impressed on the service.
		for i := 0; ; i++ {
			select {
			case <-fire:
				go r.sendRequest(r.terms[i%nTerms])
			case pt := <-r.pauseChan:
				time.Sleep(pt)
				func() {
					for {
						select {
						case <-r.pauseChan:
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
	close(r.pauseChan)
	r.config.FinishTime = time.Now()

	log.Println("Finishing stats collection...")
	r.report.Finish()
	log.Printf("Done. Results can be found at %s\n", r.config.ResultsPath)
	log.Println("Load test finished successfully.")
	return nil
}

func (r *runner) sendRequest(term string) {
	r.requestsSent.Inc()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := r.client.Search().Index(r.config.Index).Query(elastic.NewMatchPhraseQuery("text", term)).Do(ctx)
	if err == nil {
		r.responseTimes.Record(resp.TookInMillis)
	} else {
		r.errors.Inc()
	}
}
