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
	YoungHeapPool    *metrics.IntGaugeSet
	TenuredHeapPool  *metrics.IntGaugeSet
	SurvivorHeapPool *metrics.IntGaugeSet
	Heap             *metrics.IntGaugeSet
	NonHeap          *metrics.IntGaugeSet
	OS               *metrics.IntGaugeSet
	Swap             *metrics.IntGaugeSet
}

type CPU struct {
	Percent     *metrics.IntGauge
	TotalMillis *metrics.IntGauge
}

type GC struct {
	Young *metrics.IntGaugeSet
	Full  *metrics.IntGaugeSet
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
			YoungHeapPool:    metrics.NewIntGaugeSet("used", "max"),
			TenuredHeapPool:  metrics.NewIntGaugeSet("used", "max"),
			SurvivorHeapPool: metrics.NewIntGaugeSet("used", "max"),
			Heap:             metrics.NewIntGaugeSet("used", "commited"),
			NonHeap:          metrics.NewIntGaugeSet("used", "commited"),
			OS:               metrics.NewIntGaugeSet("used", "total"),
			Swap:             metrics.NewIntGaugeSet("used", "total"),
		},
		CPU: CPU{
			Percent: metrics.NewIntGauge(),
		},
		GC: GC{
			Young: metrics.NewIntGaugeSet("count", "time"),
			Full:  metrics.NewIntGaugeSet("count", "time"),
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

type MemInfo struct {
	UsedInBytes  int64 `json:"used_in_bytes"`
	TotalInBytes int64 `json:"max_in_bytes"`
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
			HeapUsedInBytes         int64 `json:"heap_used_in_bytes"`
			HeapCommittedInBytes    int64 `json:"heap_committed_in_bytes"`
			NonHeapUsedInBytes      int64 `json:"non_heap_used_in_bytes"`
			NonHeapCommittedInBytes int64 `json:"non_heap_committed_in_bytes"`
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
		Mem  MemInfo `json:"mem"`
		Swap MemInfo `json:"swap"`
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
	c.Mem.YoungHeapPool.Set(pools.Young.UsedInBytes, pools.Young.MaxInBytes)
	c.Mem.TenuredHeapPool.Set(pools.Old.UsedInBytes, pools.Old.MaxInBytes)
	c.Mem.SurvivorHeapPool.Set(pools.Survivor.UsedInBytes, pools.Survivor.MaxInBytes)
	c.Mem.Heap.Set(ns.JVM.Mem.HeapUsedInBytes, ns.JVM.Mem.HeapCommittedInBytes)
	c.Mem.NonHeap.Set(ns.JVM.Mem.NonHeapUsedInBytes, ns.JVM.Mem.NonHeapCommittedInBytes)
	c.Mem.OS.Set(ns.OS.Mem.UsedInBytes, ns.OS.Mem.TotalInBytes)
	c.Mem.Swap.Set(ns.OS.Swap.UsedInBytes, ns.OS.Swap.TotalInBytes)

	cpu := ns.OS.CPU
	c.CPU.Percent.Set(int64(cpu.Percent))

	gc := ns.JVM.GC.Collectors
	c.GC.Young.Set(gc.Young.CollectionCount, gc.Young.CollectionTimeInMillis)
	c.GC.Full.Set(gc.Old.CollectionCount, gc.Old.CollectionTimeInMillis)
	return nil
}
