package scheduler

import "fmt"

type StrategyRegistry struct {
	m map[string]StreamStrategy //"pressure_rebalance" -> PressureRebalanceStrategy
}

func NewStrategyRegistry() *StrategyRegistry {
	return &StrategyRegistry{m: make(map[string]StreamStrategy)}
}

func (r *StrategyRegistry) Register(name string, s StreamStrategy) {
	r.m[name] = s
}

func (r *StrategyRegistry) Get(name string) (StreamStrategy, bool) {
	s, ok := r.m[name]
	return s, ok
}

func (r *StrategyRegistry) MustGet(name string) StreamStrategy {
	s, ok := r.Get(name)
	if !ok {
		panic(fmt.Sprintf("strategy not found: %s", name))
	}
	return s
}
