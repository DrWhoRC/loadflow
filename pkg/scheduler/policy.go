// filepath: /Users/castle/代码/loadflow/pkg/scheduler/policy.go
package scheduler

import (
	"time"
)

// Policy 定义每个流的调度策略配置
// Policy 本身只存储数据,不包含默认值逻辑(默认值由 Strategy 管理)
type Policy struct {
	Enabled    bool
	EnabledSet bool //标记 Enabled 字段是否被显式设置

	// StrategyName 指向注册表中的策略
	StrategyName string

	// Cooldown 覆盖 controller 默认 cooldown(<=0 表示用默认)
	Cooldown time.Duration

	// Params 给策略的参数覆盖值(nil 或不存在的 key 会使用 Strategy 的默认值)
	Params map[string]float64
}

// GetParam 从 Policy.Params 中获取参数,返回值和是否存在
func (p Policy) GetParam(key string) (float64, bool) {
	if p.Params == nil {
		return 0, false
	}
	v, ok := p.Params[key]
	return v, ok
}

type PolicyProvider interface {
	Get(stream string) Policy
}

// DefaultPolicyProvider 默认策略:所有 stream 启用,使用 pressure_rebalance
type DefaultPolicyProvider struct{}

func (p DefaultPolicyProvider) Get(stream string) Policy {
	return Policy{
		Enabled:      true,
		StrategyName: "pressure_rebalance",
		Cooldown:     0,   // 0 表示用 Controller 的全局默认值
		Params:       nil, // nil 表示用 Strategy 的默认值
	}
}

// StaticPolicyProvider 简单的代码注入 provider
//
// 使用示例:
//
//	policies := scheduler.StaticPolicyProvider{
//	    Default: Policy{
//	        Enabled: true,
//	        StrategyName: "pressure_rebalance",
//	        Cooldown: 10 * time.Second,
//	    },
//	    Per: map[string]Policy{
//	        "stream_a": {  // stream_a 特殊配置
//	            Enabled: true,
//	            StrategyName: "time_based",  // 用不同的策略
//	            Cooldown: 5 * time.Second,   // 更短的冷却期
//	            Params: map[string]float64{
//	                "peak_hour_start": 9,
//	                "peak_hour_end": 17,
//	            },
//	        },
//	        "stream_b": {  // stream_b 禁用自动调度
//	            Enabled: false,
//	        },
//	    },
//	}
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

// normalizePolicy 合并策略: v 的非零字段覆盖 def
func normalizePolicy(v Policy, def Policy) Policy {
	// def 作为兜底
	out := def
	// v 显式字段覆盖 def
	if v.EnabledSet {
		out.Enabled = v.Enabled
		out.EnabledSet = true
	}
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
