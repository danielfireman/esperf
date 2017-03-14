package cmd

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/net/context"

	"github.com/danielfireman/esperf/metrics"
)

type retrier struct {
	pauseChan  chan time.Duration
	pauseTimes *metrics.Histogram
}

func (e *retrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) {
	if err != nil {
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests {
			ra := resp.Header.Get("Retry-After")
			if ra == "" {
				log.Fatal("Could not extract retry-after information")
			}
			pt, err := strconv.ParseFloat(ra, 64)
			if err != nil {
				log.Fatal("Could not extract retry-after information")
			}
			pauseMillis := int64(pt * 1e9)
			e.pauseTimes.Record(pauseMillis)
			e.pauseChan <- time.Duration(pauseMillis)
		}
	}
	return 0, false, nil
}
