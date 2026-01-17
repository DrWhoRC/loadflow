package scheduler

import (
	"time"
)

type StreamStrategy interface {
	Name() string
	DecideStream(s Snapshot, stream string, poolNames []string, weights []int, policy Policy) (*Plan, bool)
}

// BindingView：策略只读当前绑定（stream -> poolNames + weights）
type BindingView interface {
	Streams() []string
	Get(stream string) (poolNames []string, weights []int, ok bool)
}

type PressureRebalanceStrategy struct {
	EpsRate          float64 // 防止除零
	MinPressureDelta float64 // 压力差不到这个值就不动
}

func NewPressureRebalanceStrategy() *PressureRebalanceStrategy {
	return &PressureRebalanceStrategy{
		EpsRate:          1.0,
		MinPressureDelta: 2.0, // 你可以调参：越大越保守
	}
}

func (st *PressureRebalanceStrategy) Decide(s Snapshot, b BindingView) (*Plan, bool) {
	streams := b.Streams()
	for _, stream := range streams {
		pools, weights, ok := b.Get(stream)
		if !ok || len(pools) == 0 || len(pools) != len(weights) {
			continue
		}

		// 找最大压力和最小压力的 pool
		maxIdx, minIdx := -1, -1
		maxP, minP := -1.0, 1e18

		for i, pn := range pools {
			ps, ok2 := s.Pools[pn]
			if !ok2 {
				continue
			}
			p := ps.Pressure(st.EpsRate)

			if p > maxP {
				maxP = p
				maxIdx = i
			}
			if p < minP {
				minP = p
				minIdx = i
			}
		}

		if maxIdx < 0 || minIdx < 0 || maxIdx == minIdx {
			continue
		}

		// 压力差不足，不调整
		if (maxP - minP) < st.MinPressureDelta {
			continue
		}

		// 从高压挪 1 个权重给低压（B1 最小步长）
		if weights[maxIdx] <= 1 {
			// 高压池权重已经不能再降（保持至少 1）
			continue
		}

		newW := make([]int, len(weights))
		copy(newW, weights)
		newW[maxIdx] -= 1
		newW[minIdx] += 1

		oldW := make([]int, len(weights))
		copy(oldW, weights)

		return &Plan{
			Stream:     stream,
			OldWeights: oldW,
			NewWeights: newW,
			Change: Change{
				FromPool: pools[maxIdx],
				ToPool:   pools[minIdx],
				DeltaW:   1,
			},
			Trigger: Trigger{
				Metric:    "pressure",
				FromPool:  pools[maxIdx],
				ToPool:    pools[minIdx],
				FromValue: maxP,
				ToValue:   minP,
				Delta:     maxP - minP,
				Threshold: st.MinPressureDelta,
			},
			Reason:      "pressure_rebalance",
			GeneratedAt: time.Now(),
		}, true
	}

	return nil, false
}
