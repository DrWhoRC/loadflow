package router

import (
	"fmt"
	"sync"

	"github.com/DrWhoRC/loadflow/pkg/pool"
)

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

func (r *inMemory) Snapshot() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]string, len(r.routes))
	for s, p := range r.routes {
		out[s] = p.Name()
	}
	return out
}
