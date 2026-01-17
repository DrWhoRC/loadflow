package scheduler

import (
	"context"
	"time"

	"github.com/DrWhoRC/loadflow/pkg/pool"
)

type RuntimeWithMetrics interface {
	DumpMetrics() []pool.PoolMetrics
}

type MetricsProvider interface {
	Sample(ctx context.Context) (Snapshot, error)
}

type prevPoint struct {
	cnt uint64
	at  time.Time
}

type RuntimeMetricsProvider struct {
	rt   RuntimeWithMetrics
	prev map[string]prevPoint
}

func NewRuntimeMetricsProvider(rt RuntimeWithMetrics) *RuntimeMetricsProvider {
	return &RuntimeMetricsProvider{
		rt:   rt,
		prev: make(map[string]prevPoint),
	}
}

func (p *RuntimeMetricsProvider) Sample(ctx context.Context) (Snapshot, error) {
	now := time.Now()
	ms := p.rt.DumpMetrics()

	out := Snapshot{
		Pools: make(map[string]PoolStat, len(ms)),
		At:    now,
	}

	for _, m := range ms {
		name := m.Name
		pp, ok := p.prev[name]

		rate := 0.0
		if ok {
			dt := now.Sub(pp.at).Seconds()
			if dt > 0 {
				// ProcessedCount 是单调递增
				dc := float64(m.ProcessedCount - pp.cnt)
				rate = dc / dt
			}
		}

		out.Pools[name] = PoolStat{
			Name:          name,
			QueueDepth:    m.QueueDepth,
			WorkerCount:   m.WorkerCount,
			Processed:     m.ProcessedCount,
			ProcessRatePS: rate,
			At:            now,
		}

		p.prev[name] = prevPoint{cnt: m.ProcessedCount, at: now}
	}

	return out, nil
}
