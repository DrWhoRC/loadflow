package router

import "github.com/DrWhoRC/loadflow/pkg/pool"

type Router interface {
	Bind(srcName string, p pool.WorkerPool) error
	Route(srcName string) (pool.WorkerPool, bool)
	Snapshot() map[string]string
}
type KeyFunc func(srcName string, payload []byte) []byte

type KeyRouter interface {
	Router
	RouteWithKey(srcName string, key []byte) (pool.WorkerPool, bool)
}

type MutableRouter interface {
	Router
	SetMany(srcName string, pools []pool.WorkerPool, weights []int) error
}
