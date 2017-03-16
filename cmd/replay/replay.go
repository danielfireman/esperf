package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/danielfireman/esperf/esmetrics"
	"github.com/danielfireman/esperf/metrics"
	"github.com/danielfireman/esperf/reporter"
	"github.com/spf13/cobra"

	// TODO(danielfireman): Review this dependency (commands depending on commands). This is a bad smell.
	"sync"

	"github.com/danielfireman/esperf/cmd/loadspec"
)

var (
	host        string
	resultsPath string
	expID       string
	cint        time.Duration
	timeout     time.Duration
)

func init() {
	RootCmd.Flags().StringVar(&host, "host", "", "")
	RootCmd.Flags().StringVar(&resultsPath, "results_path", "", "")
	RootCmd.Flags().StringVar(&expID, "exp_id", "1", "")
	RootCmd.Flags().DurationVar(&cint, "cint", 5*time.Second, "Interval between metrics collection.")
	RootCmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "Timeout to be used in connections to ES.")
}

var (
	// DefaultLocalAddr is the default local IP address an Attacker uses.
	defaultLocalAddr = net.IPAddr{IP: net.IPv4zero}
	// DefaultConnections is the default amount of max open idle connections per
	// target host.
	defaultConnections = 10000
	r                  runner
)

var RootCmd = &cobra.Command{
	Use:   "replay",
	Short: "Runs a performance testing and collects metrics.",
	Long:  "Multiplatform command line tool to load test and collect metrics from your ElasticSearch deployment.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		r = runner{}
		if resultsPath == "" {
			return fmt.Errorf("Results path can not be empty. Please set --results_path flag.")
		}

		r.requestsSent = metrics.NewCounter()
		r.errors = metrics.NewCounter()
		r.responseTimes = metrics.NewHistogram()
		r.pauseTimes = metrics.NewHistogram()
		r.client = http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				Dial: (&net.Dialer{
					LocalAddr: &net.TCPAddr{IP: defaultLocalAddr.IP, Zone: defaultLocalAddr.Zone},
					KeepAlive: 3 * timeout,
					Timeout:   timeout,
				}).Dial,
				ResponseHeaderTimeout: timeout,
				MaxIdleConnsPerHost:   defaultConnections,
			},
		}

		// TODO(danielfireman): Review metrics collection design.
		collector, err := esmetrics.NewCollector(host, timeout)
		if err != nil {
			return err
		}
		r.report, err = reporter.New(
			cint,
			timeout,
			reporter.MetricToCSV(r.responseTimes, csvFilePath("response.time", expID, resultsPath)),
			reporter.MetricToCSV(r.pauseTimes, csvFilePath("pause.time", expID, resultsPath)),
			reporter.MetricToCSV(r.requestsSent, csvFilePath("requests.sent", expID, resultsPath)),
			reporter.MetricToCSV(r.errors, csvFilePath("errors", expID, resultsPath)),
			reporter.AddCollector(collector),
			reporter.MetricToCSV(collector.Mem.YoungPoolUsedBytes, csvFilePath("young.pool.used.bytes", expID, resultsPath)),
			reporter.MetricToCSV(collector.Mem.YoungPoolMaxBytes, csvFilePath("young.pool.max.bytes", expID, resultsPath)),
			reporter.MetricToCSV(collector.Mem.TenuredPoolUsedBytes, csvFilePath("tenured.pool.used.bytes", expID, resultsPath)),
			reporter.MetricToCSV(collector.Mem.TenuredPoolMaxBytes, csvFilePath("tenured.pool.max.bytes", expID, resultsPath)),
			reporter.MetricToCSV(collector.Mem.SurvivorPoolUsedBytes, csvFilePath("survivor.pool.used.bytes", expID, resultsPath)),
			reporter.MetricToCSV(collector.Mem.SurvivorPoolMaxBytes, csvFilePath("survivor.pool.max.bytes", expID, resultsPath)),
			reporter.MetricToCSV(collector.CPU.Percent, csvFilePath("cpu.percent", expID, resultsPath)),
			reporter.MetricToCSV(collector.CPU.TotalMillis, csvFilePath("cpu.total.ms", expID, resultsPath)),
			reporter.MetricToCSV(collector.GC.YoungCount, csvFilePath("gc.young.count", expID, resultsPath)),
			reporter.MetricToCSV(collector.GC.YoungTimeMillis, csvFilePath("gc.young.time.ms", expID, resultsPath)),
			reporter.MetricToCSV(collector.GC.FullCount, csvFilePath("gc.full.count", expID, resultsPath)),
			reporter.MetricToCSV(collector.GC.FullTimeMillis, csvFilePath("gc.full.time.ms", expID, resultsPath)),
		)
		if err != nil {
			return err
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := r.Run(); err != nil {
			return err
		}
		return nil
	},
}

type runner struct {
	client http.Client
	report *reporter.Reporter

	requestsSent  *metrics.Counter
	responseTimes *metrics.Histogram
	errors        *metrics.Counter
	pauseTimes    *metrics.Histogram
}

func csvFilePath(name, expID, resultsPath string) string {
	return filepath.Join(resultsPath, name+"_"+expID+".csv")
}

func (r *runner) Run() error {
	r.report.Start()
	defer r.report.Finish()

	var wg sync.WaitGroup
	scanner := bufio.NewScanner(os.Stdin)
	for count := 0; scanner.Scan(); count++ {
		// Note: Having a single worker or a single load generator is a way to guarantee the load will obey to a
		// certain  distribution. For instance, 10 workers generating load following a Poisson distribution is
		// different  from having Poisson ruling the overall load impressed on the service.
		entry := loadspec.Entry{}
		if err := json.NewDecoder(strings.NewReader(scanner.Text())).Decode(&entry); err != nil {
			return err
		}
		time.Sleep(time.Duration(entry.TimestampNanos))

		req, err := Request(&entry)
		if err != nil {
			return err
		}

		pauseChan := make(chan time.Duration)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.requestsSent.Inc()
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			req.WithContext(ctx)

			resp, err := r.client.Do(req)
			if err != nil {
				r.errors.Inc()
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests {
				ra := resp.Header.Get("Retry-After")
				if ra == "" {
					// TODO(danielfireman): Make this more elegant. Leveraging cobra error messages.
					fmt.Println("Could not extract retry-after information")
					os.Exit(-1)
				}
				pt, err := strconv.ParseFloat(ra, 64)
				if err != nil {
					// TODO(danielfireman): Make this more elegant. Leveraging cobra error messages.
					fmt.Println("Could not extract retry-after information")
					os.Exit(-1)
				}
				pauseMillis := int64(pt * 1e9)
				r.pauseTimes.Record(pauseMillis)
				pauseChan <- time.Duration(pauseMillis)
			}
		}()

		// Non-blocking check of pauses.
		select {
		case pt := <-pauseChan:
			time.Sleep(pt)
			func() {
				for {
					select {
					case <-pauseChan:
					default:
						return
					}
				} // Emptying pause channel before continue.
			}()
		default:
		}

	}
	wg.Wait()
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func Request(entry *loadspec.Entry) (*http.Request, error) {
	pathEntries := []string{entry.Host}
	if entry.Index != "" {
		pathEntries = append(pathEntries, entry.Index)
	}
	if entry.Types != "" {
		pathEntries = append(pathEntries, entry.Types)
	}
	pathEntries = append(pathEntries, "_search")

	u, err := url.Parse(strings.Join(pathEntries, "/"))
	if err != nil {
		return nil, err
	}
	q := u.Query()
	if entry.SearchType != "" {
		q.Set("search_type", strings.ToLower(entry.SearchType))
	}
	u.RawQuery = q.Encode()
	return http.NewRequest("GET", u.RequestURI(), strings.NewReader(entry.Source))
}
