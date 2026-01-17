package scheduler

import (
	"context"
	"fmt"
	"sync"

	"github.com/DrWhoRC/loadflow/pkg/flow/router"
	"github.com/DrWhoRC/loadflow/pkg/pool"
)

type Binding struct {
	PoolNames []string
	Pools     []pool.WorkerPool
	Weights   []int
}

type BindingRegistry struct {
	mu   sync.RWMutex
	data map[string]*Binding
}

func NewBindingRegistry() *BindingRegistry {
	return &BindingRegistry{data: make(map[string]*Binding)}
}

func (r *BindingRegistry) Set(stream string, pools []pool.WorkerPool, weights []int) error {
	if len(pools) == 0 || len(pools) != len(weights) {
		return fmt.Errorf("invalid binding for %s", stream)
	}
	names := make([]string, len(pools))
	for i := range pools {
		if pools[i] == nil {
			return fmt.Errorf("nil pool in binding %s", stream)
		}
		names[i] = pools[i].Name()
	}

	r.mu.Lock()
	r.data[stream] = &Binding{
		PoolNames: names,
		Pools:     pools,
		Weights:   append([]int(nil), weights...),
	}
	r.mu.Unlock()
	return nil
}

func (r *BindingRegistry) Streams() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.data))
	for k := range r.data {
		out = append(out, k)
	}
	return out
}

func (r *BindingRegistry) Get(stream string) ([]string, []int, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b := r.data[stream]
	if b == nil {
		return nil, nil, false
	}
	w := make([]int, len(b.Weights))
	copy(w, b.Weights)
	return b.PoolNames, w, true
}

func (r *BindingRegistry) GetBinding(stream string) (*Binding, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b := r.data[stream]
	if b == nil {
		return nil, false
	}
	return b, true
}

type Applier interface {
	Apply(ctx context.Context, plan Plan) error
}

type RouterApplier struct {
	ro   router.MutableRouter
	bind *BindingRegistry
}

func NewRouterApplier(ro router.MutableRouter, bind *BindingRegistry) *RouterApplier {
	return &RouterApplier{ro: ro, bind: bind}
}

func (a *RouterApplier) Apply(ctx context.Context, plan Plan) error {
	b, ok := a.bind.GetBinding(plan.Stream)
	if !ok {
		return fmt.Errorf("unknown stream %s", plan.Stream)
	}
	if len(plan.NewWeights) != len(b.Pools) {
		return fmt.Errorf("weights size mismatch for %s", plan.Stream)
	}

	// 更新 registry（作为当前“期望配置”）
	a.bind.mu.Lock()
	b.Weights = append([]int(nil), plan.NewWeights...)
	a.bind.mu.Unlock()

	// 落地到 router
	return a.ro.SetMany(plan.Stream, b.Pools, plan.NewWeights)
}
