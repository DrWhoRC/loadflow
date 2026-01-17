package scheduler

import (
	"time"
)

type PoolStat struct {
	Name          string
	QueueDepth    int
	WorkerCount   int
	Processed     uint64
	ProcessRatePS float64   // tasks / second（差分计算）
	At            time.Time // 采样时间
}

func (p PoolStat) Pressure(eps float64) float64 {
	// pressure 越大代表越“堵”
	// rate 越低 + queue 越大 => pressure 越大
	r := p.ProcessRatePS
	if r < eps {
		r = eps
	}
	return float64(p.QueueDepth) / r
}

type Snapshot struct {
	Pools map[string]PoolStat
	At    time.Time
}

type Plan struct {
	Stream      string
	OldWeights  []int
	NewWeights  []int
	Change      Change
	Trigger     Trigger
	Reason      string
	GeneratedAt time.Time
}

type Trigger struct {
	Metric   string // "pressure" / "latency_p95" / "fail_rate" 等
	FromPool string
	ToPool   string

	FromValue float64
	ToValue   float64
	Delta     float64 // FromValue - ToValue（或你定义的差值）
	Threshold float64 // 触发阈值（例如 MinPressureDelta）
}

type Change struct {
	FromPool string
	ToPool   string
	DeltaW   int // 例如 1：从 FromPool 挪 1 给 ToPool
}
