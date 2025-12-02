package metrics

import (
	"context"
	"time"

	"github.com/DrWhoRC/loadflow/pkg/pool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 只依赖这个接口，避免和 runtime 具体类型强耦合
type RuntimeWithMetrics interface {
	DumpMetrics() []pool.PoolMetrics
}

type PrometheusExporter struct {
	rt RuntimeWithMetrics

	queueDepth    *prometheus.GaugeVec
	queueCapacity *prometheus.GaugeVec
	workerCount   *prometheus.GaugeVec
	processedCnt  *prometheus.GaugeVec // 这里先用 Gauge 承接总量
}

type ExporterOptions struct {
	Namespace string
	Subsystem string
}

func NewPrometheusExporter(
	rt RuntimeWithMetrics,
	reg prometheus.Registerer,
	opts ExporterOptions,
) *PrometheusExporter {
	ns := opts.Namespace
	ss := opts.Subsystem

	queueDepth := promauto.With(reg).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: ss,
			Name:      "pool_queue_depth",
			Help:      "Current queue depth of each pool",
		},
		[]string{"pool"},
	)

	workerCount := promauto.With(reg).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: ss,
			Name:      "pool_worker_count",
			Help:      "Current worker count of each pool",
		},
		[]string{"pool"},
	)

	// 你的 ProcessedCount 是“当前总处理数”，更像一个 monotonic gauge，
	// 在 Prometheus 用 rate() / increase() 时一样能得到吞吐。
	processedCnt := promauto.With(reg).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: ss,
			Name:      "pool_processed_total",
			Help:      "Total processed tasks of each pool (monotonic)",
		},
		[]string{"pool"},
	)

	queueCapacity := promauto.With(reg).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: ss,
			Name:      "pool_queue_capacity",
			Help:      "Configured Queue capacity of each pool",
		},
		[]string{"pool"},
	)

	return &PrometheusExporter{
		rt:            rt,
		queueDepth:    queueDepth,
		queueCapacity: queueCapacity,
		workerCount:   workerCount,
		processedCnt:  processedCnt,
	}
}

func (e *PrometheusExporter) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ms := e.rt.DumpMetrics()
			for _, m := range ms {
				labels := prometheus.Labels{"pool": m.Name}

				e.queueDepth.With(labels).Set(float64(m.QueueDepth))
				e.queueCapacity.With(labels).Set(float64(m.QueueCapacity))
				e.workerCount.With(labels).Set(float64(m.WorkerCount))
				e.processedCnt.With(labels).Set(float64(m.ProcessedCount)) // synchronize to prometheus time sequence
			}
		case <-ctx.Done():
			return
		}
	}
}

// 协程池 → 更新内部统计字段
// runtime.DumpMetrics() → 把每个 pool 的统计打包成 []PoolMetrics
// PrometheusExporter.Start → 定期读取 []PoolMetrics → 写入 GaugeVec
// Prometheus 服务 → 定期拉取 /metrics → 存进自己的 TSDB
// Grafana → 连 Prometheus → 画曲线
