package esmetrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/danielfireman/esperf/metrics"
)

type Mem struct {
	YoungPoolUsedBytes    *metrics.IntGauge
	YoungPoolMaxBytes     *metrics.IntGauge
	TenuredPoolUsedBytes  *metrics.IntGauge
	TenuredPoolMaxBytes   *metrics.IntGauge
	SurvivorPoolMaxBytes  *metrics.IntGauge
	SurvivorPoolUsedBytes *metrics.IntGauge
}

type CPU struct {
	Percent     *metrics.IntGauge
	TotalMillis *metrics.IntGauge
}

type GC struct {
	YoungTimeMillis *metrics.IntGauge
	YoungCount      *metrics.IntGauge
	FullTimeMillis  *metrics.IntGauge
	FullCount       *metrics.IntGauge
}

// DefaultConnections is the default amount of max open idle connections per
// target host.
const defaultConnections = 10

func NewCollector(host string, timeout time.Duration, debug bool) (*ESCollector, error) {

	return &ESCollector{
		debug: debug,
		url:   strings.Join([]string{host, "_nodes", "stats"}, "/"),
		client: http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{KeepAlive: 3 * timeout, Timeout: timeout}).Dial,
				ResponseHeaderTimeout: timeout,
				MaxIdleConnsPerHost:   defaultConnections,
			},
		},
		Mem: Mem{
			YoungPoolUsedBytes:    metrics.NewIntGauge(),
			YoungPoolMaxBytes:     metrics.NewIntGauge(),
			TenuredPoolUsedBytes:  metrics.NewIntGauge(),
			TenuredPoolMaxBytes:   metrics.NewIntGauge(),
			SurvivorPoolUsedBytes: metrics.NewIntGauge(),
			SurvivorPoolMaxBytes:  metrics.NewIntGauge(),
		},
		CPU: CPU{
			Percent: metrics.NewIntGauge(),
		},
		GC: GC{
			YoungCount:      metrics.NewIntGauge(),
			YoungTimeMillis: metrics.NewIntGauge(),
			FullCount:       metrics.NewIntGauge(),
			FullTimeMillis:  metrics.NewIntGauge(),
		},
	}, nil

}

type ESCollector struct {
	debug  bool
	url    string
	client http.Client
	GC     GC
	CPU    CPU
	Mem    Mem
}

func (c *ESCollector) Name() string {
	return "ESCollector"
}

type MemPoolInfo struct {
	UsedInBytes int64 `json:"used_in_bytes"`
	MaxInBytes  int64 `json:"max_in_bytes"`
}

type CollectorInfo struct {
	CollectionCount        int64 `json:"collection_count"`
	CollectionTimeInMillis int64 `json:"collection_time_in_millis"`
}

type NodeStats struct {
	JVM struct {
		Mem struct {
			Pools struct {
				Young    MemPoolInfo `json:"young"`
				Old      MemPoolInfo `json:"old"`
				Survivor MemPoolInfo `json:"survivor"`
			} `json:"pools"`
		} `json:"mem"`
		GC struct {
			Collectors struct {
				Young CollectorInfo `json:"young"`
				Old   CollectorInfo `json:"old"`
			} `json:"collectors"`
		} `json:"gc"`
	} `json:"jvm"`
	OS struct {
		CPU struct {
			Percent int `json:"percent"`
		} `json:"cpu"`
	} `json:"os"`
}

type StatsResponse struct {
	Nodes map[string]NodeStats `json:"nodes"`
}

func (c *ESCollector) Collect(ctx context.Context) error {
	resp, err := c.client.Get(c.url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dResp, _ := httputil.DumpResponse(resp, true)
	if c.debug {
		fmt.Println(string(dResp))
	}

	stats := StatsResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return err
	}
	var ns NodeStats
	for _, ns = range stats.Nodes {
	}

	pools := ns.JVM.Mem.Pools
	c.Mem.YoungPoolMaxBytes.Set(pools.Young.MaxInBytes)
	c.Mem.YoungPoolUsedBytes.Set(pools.Young.UsedInBytes)
	c.Mem.TenuredPoolMaxBytes.Set(pools.Old.MaxInBytes)
	c.Mem.TenuredPoolUsedBytes.Set(pools.Old.UsedInBytes)
	c.Mem.SurvivorPoolMaxBytes.Set(pools.Survivor.MaxInBytes)
	c.Mem.SurvivorPoolUsedBytes.Set(pools.Survivor.UsedInBytes)

	cpu := ns.OS.CPU
	c.CPU.Percent.Set(int64(cpu.Percent))

	gc := ns.JVM.GC.Collectors
	c.GC.YoungCount.Set(gc.Young.CollectionCount)
	c.GC.YoungTimeMillis.Set(gc.Young.CollectionTimeInMillis)
	c.GC.FullCount.Set(gc.Old.CollectionCount)
	c.GC.FullTimeMillis.Set(gc.Old.CollectionTimeInMillis)
	return nil
}
