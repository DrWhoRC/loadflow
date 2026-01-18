package scheduler

import (
	"log"
	"math"
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

// PressureRebalanceStrategy 基于压力的自动再平衡策略
//
// 参数说明:
//   - epsRate: 计算压力时防止除零的小量 (默认 1.0)
//   - minPressureDelta: 触发调整的最小压力差阈值 (默认 2.0)
//   - baseStep: 基准步长 (默认 1.0)
//   - maxStep: 单次调整的最大步长 (默认 5.0)
//   - minWeight: 每个 pool 的最小权重 (默认 1.0)
//   - maxFrac: 单次调整占总权重的最大比例 (默认 0.2, 即 20%)
//   - paceRate: 压力差每增加多少触发额外的步长 (默认 5.0)
type PressureRebalanceStrategy struct {
	defaultParams map[string]float64
}

func NewPressureRebalanceStrategy() *PressureRebalanceStrategy {
	return &PressureRebalanceStrategy{
		defaultParams: map[string]float64{
			"epsRate":          1.0,
			"minPressureDelta": 2.0,
			"baseStep":         1.0,
			"maxStep":          5.0,
			"minWeight":        1.0,
			"maxFrac":          0.2,
			"paceRate":         5.0,
		},
	}
}

func (st *PressureRebalanceStrategy) Name() string {
	return "pressure_rebalance"
}

// getParam 从 policy 或 defaultParams 获取参数值
// 优先使用 policy.Params 中的覆盖值,如果不存在则使用默认值
func (st *PressureRebalanceStrategy) getParam(policy Policy, key string) float64 {
	if v, ok := policy.GetParam(key); ok {
		return v
	}
	if v, ok := st.defaultParams[key]; ok {
		return v
	}
	// 如果默认值也不存在,返回 0 (理论上不应该发生)
	log.Printf("warning: param %s not found in policy or defaults", key)
	return 0.0
}

func (st *PressureRebalanceStrategy) DecideStream(s Snapshot, stream string, poolNames []string, weights []int, policy Policy) (*Plan, bool) {
	if !policy.Enabled {
		log.Println("pressure rebalance strategy is disabled by policy")
		return nil, false
	}

	if len(poolNames) == 0 || len(poolNames) != len(weights) {
		log.Println("pressure rebalance strategy: invalid poolNames or weights")
		return nil, false
	}

	// 获取参数
	epsRate := st.getParam(policy, "epsRate")
	minPressureDelta := st.getParam(policy, "minPressureDelta")
	baseStep := st.getParam(policy, "baseStep")
	maxStep := st.getParam(policy, "maxStep")
	minWeight := st.getParam(policy, "minWeight")
	maxFrac := st.getParam(policy, "maxFrac")
	paceRate := st.getParam(policy, "paceRate")

	// 找最大压力和最小压力的 pool
	maxIdx, minIdx := -1, -1
	maxP, minP := -1.0, 1e18

	for i, pn := range poolNames {
		ps, ok := s.Pools[pn]
		if !ok {
			continue
		}
		p := ps.Pressure(epsRate)
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
		log.Println("pressure rebalance strategy: no valid max/min pool found")
		return nil, false
	}

	// 压力差不足,不调整
	if (maxP - minP) < minPressureDelta {
		log.Printf("pressure rebalance strategy: pressure delta %.2f < threshold %.2f\n", maxP-minP, minPressureDelta)
		return nil, false
	}

	// 高压池权重已经不能再降
	if float64(weights[maxIdx]) <= minWeight {
		log.Printf("pressure rebalance strategy: max pressure pool weight %d already at minimum %.0f\n", weights[maxIdx], minWeight)
		return nil, false
	}

	// 计算步长
	// step = min(stepCandidate, maxStep, weights[maxIdx]-minWeight, max(1, ⌊sumW·maxFrac⌋))
	mult := 1 + math.Floor(((maxP-minP)-minPressureDelta)/paceRate)

	// 根据 worker 数量比例调整步长(小池子往大池子挪权重时更激进)
	cap := 1.0
	if minIdx >= 0 && maxIdx >= 0 {
		minWorkers := float64(s.Pools[poolNames[minIdx]].WorkerCount)
		maxWorkers := float64(s.Pools[poolNames[maxIdx]].WorkerCount)
		cap = math.Sqrt(minWorkers / math.Max(1.0, maxWorkers))
	}

	stepCandidate := int(mult * baseStep * cap)

	// 底线1: 不超过 maxStep
	step := minInt(stepCandidate, int(maxStep))

	// 底线2: 不让 maxIdx 低于 minWeight
	bottomLineofWeights := weights[maxIdx] - int(minWeight)
	step = minInt(step, bottomLineofWeights)

	// 底线3: 不超过总权重的 maxFrac
	sumWeights := 0
	for _, w := range weights {
		sumWeights += w
	}
	overallPortion := int(math.Max(1, math.Floor(float64(sumWeights)*maxFrac)))
	step = minInt(step, overallPortion)

	if step <= 0 {
		log.Println("pressure rebalance strategy: calculated step is non-positive")
		return nil, false
	}

	newW := make([]int, len(weights))
	copy(newW, weights)
	newW[maxIdx] -= step
	newW[minIdx] += step

	oldW := make([]int, len(weights))
	copy(oldW, weights)

	return &Plan{
		Stream:     stream,
		OldWeights: oldW,
		NewWeights: newW,
		Change: Change{
			FromPool: poolNames[maxIdx],
			ToPool:   poolNames[minIdx],
			DeltaW:   step,
		},
		Trigger: Trigger{
			Metric:    "pressure",
			FromPool:  poolNames[maxIdx],
			ToPool:    poolNames[minIdx],
			FromValue: maxP,
			ToValue:   minP,
			Delta:     maxP - minP,
			Threshold: minPressureDelta,
		},
		Reason:      "pressure_rebalance",
		GeneratedAt: time.Now(),
	}, true
}

func minInt(vals ...int) int {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}
