package esmetrics

import (
	"context"
	"time"

	"github.com/danielfireman/esperf/metrics"
	"gopkg.in/olivere/elastic.v5"
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

func NewCollector(host string, timeout time.Duration) (*ESCollector, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(host),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetHealthcheckTimeout(timeout))
	if err != nil {
		return nil, err
	}
	return &ESCollector{
		client: client,
		Mem: Mem{
			YoungPoolUsedBytes:    metrics.NewIntGauge(),
			YoungPoolMaxBytes:     metrics.NewIntGauge(),
			TenuredPoolUsedBytes:  metrics.NewIntGauge(),
			TenuredPoolMaxBytes:   metrics.NewIntGauge(),
			SurvivorPoolUsedBytes: metrics.NewIntGauge(),
			SurvivorPoolMaxBytes:  metrics.NewIntGauge(),
		},
		CPU: CPU{
			Percent:     metrics.NewIntGauge(),
			TotalMillis: metrics.NewIntGauge(),
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
	client *elastic.Client
	GC     GC
	CPU    CPU
	Mem    Mem
}

func (c *ESCollector) Name() string {
	return "ESCollector"
}

func (c *ESCollector) Collect(ctx context.Context) error {
	nss := c.client.NodesStats().Metric("jvm", "process")
	resp, err := nss.Do(ctx)
	if err != nil {
		return err
	}
	var ns *elastic.NodesStatsNode
	for _, ns = range resp.Nodes {
	}

	pools := ns.JVM.Mem.Pools
	c.Mem.YoungPoolMaxBytes.Set(pools["young"].MaxInBytes)
	c.Mem.YoungPoolUsedBytes.Set(pools["young"].UsedInBytes)
	c.Mem.TenuredPoolMaxBytes.Set(pools["old"].MaxInBytes)
	c.Mem.TenuredPoolUsedBytes.Set(pools["old"].UsedInBytes)
	c.Mem.SurvivorPoolMaxBytes.Set(pools["survivor"].MaxInBytes)
	c.Mem.SurvivorPoolUsedBytes.Set(pools["survivor"].UsedInBytes)

	cpu := ns.Process.CPU
	c.CPU.Percent.Set(int64(cpu.Percent))
	c.CPU.TotalMillis.Set(cpu.TotalInMillis)

	gc := ns.JVM.GC.Collectors
	c.GC.YoungCount.Set(gc["young"].CollectionCount)
	c.GC.YoungTimeMillis.Set(gc["young"].CollectionTimeInMillis)
	c.GC.FullCount.Set(gc["old"].CollectionCount)
	c.GC.FullTimeMillis.Set(gc["old"].CollectionTimeInMillis)
	return nil
}
