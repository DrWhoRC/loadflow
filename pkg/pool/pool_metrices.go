package pool

type PoolMetrics struct {
	QueueDepth     int
	QueueCapacity  int
	ProcessedCount uint64
	WorkerCount    int
	Name           string
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
