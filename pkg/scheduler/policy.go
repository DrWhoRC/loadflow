package scheduler

import "time"

type Policy struct {
	Enabled bool

	// StrategyName 指向注册表中的策略
	StrategyName string

	// Cooldown 覆盖 controller 默认 cooldown（<=0 表示用默认）
	Cooldown time.Duration

	// Params 给策略的参数（简单起见先用 map；后续你也可以做强类型）
	Params map[string]float64
}

type PolicyProvider interface {
	Get(stream string) Policy
}

// 默认策略：所有 stream 启用，使用 pressure_rebalance
type DefaultPolicyProvider struct{}

func (p DefaultPolicyProvider) Get(stream string) Policy {
	return Policy{
		Enabled:      true,
		StrategyName: "pressure_rebalance",
		Cooldown:     0,
		Params:       nil,
	}
}

// 简单的代码注入 provider
type StaticPolicyProvider struct {
	Default Policy
	Per     map[string]Policy
}

func (p StaticPolicyProvider) Get(stream string) Policy {
	if p.Per != nil {
		if v, ok := p.Per[stream]; ok {
			return normalizePolicy(v, p.Default)
		}
	}
	return normalizePolicy(p.Default, Policy{})
}

func normalizePolicy(v Policy, def Policy) Policy {
	// def 作为兜底
	out := def
	// v 显式字段覆盖 def
	out.Enabled = v.Enabled
	if v.StrategyName != "" {
		out.StrategyName = v.StrategyName
	}
	if v.Cooldown > 0 {
		out.Cooldown = v.Cooldown
	}
	if v.Params != nil {
		out.Params = v.Params
	}
	return out
}
