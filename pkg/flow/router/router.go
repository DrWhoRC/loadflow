package router

import "github.com/DrWhoRC/loadflow/pkg/pool"

type Router interface {
	Bind(srcName string, p pool.WorkerPool) error
	Route(srcName string) (pool.WorkerPool, bool)
	Snapshot() map[string]string
}
