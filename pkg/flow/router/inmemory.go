package router

import (
	"fmt"
	"sync"

	"github.com/DrWhoRC/loadflow/pkg/pool"
)

// 适用读多写少，该场景绝大多数注册完后就是读了，所以很少写
// 如果用sync.Map，适用于读写激烈竞争的场景，并且内部store和load方法用的是interface，
// 需要断言，比较麻烦
type inMemory struct {
	mu     sync.RWMutex
	routes map[string]pool.WorkerPool // srcName -> pool
}

func NewInMemory() Router {
	return &inMemory{routes: make(map[string]pool.WorkerPool)}
}

func (r *inMemory) Bind(srcName string, p pool.WorkerPool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.routes[srcName]; ok {
		return fmt.Errorf("route exists for %s", srcName)
	}
	r.routes[srcName] = p
	return nil
}

func (r *inMemory) Route(srcName string) (pool.WorkerPool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.routes[srcName]
	return p, ok
}

// source_A : pool_1
func (r *inMemory) Snapshot() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]string, len(r.routes))
	for s, p := range r.routes {
		out[s] = p.Name()
	}
	return out
}
