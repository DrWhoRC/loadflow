package pool

type PoolMetrics struct {
	QueueDepth     int
	QueueCapacity  int
	ProcessedCount uint64
	WorkerCount    int
	Name           string

	// StripedPool 专用字段
	Size      int             // stripe 数量（并发度）
	Processed uint64          // 总处理数
	Submitted uint64          // 总提交数
	Panics    uint64          // 总 panic 数
	Stripes   []StripeMetrics // 各 stripe 的详细指标
}

type PoolWithMetrics interface {
	GetMetrics() PoolMetrics
}

func (p *FixedPool) GetMetrics() PoolMetrics {
	return PoolMetrics{
		QueueDepth:     p.GetQueueDepth(),
		QueueCapacity:  p.GetQueueCapacity(),
		ProcessedCount: p.ProcessedCount(),
		WorkerCount:    p.Size(),
		Name:           p.Name(),
	}
}
