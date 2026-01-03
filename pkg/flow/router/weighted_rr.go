package router

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"hash/fnv"

	"github.com/DrWhoRC/loadflow/pkg/pool"
)

type rrEntry struct {
	targets []pool.WorkerPool
	table   []int  // keyless : 预展开权重表：如 1:2:4 -> [0,1,1,2,2,2,2]
	ring    []int  // key : keyed hash
	cursor  uint64 // atomic
}

type WeightedRRRouter struct {
	mu     sync.RWMutex
	routes map[string]*rrEntry
}

func NewWeightedRR() *WeightedRRRouter {
	return &WeightedRRRouter{
		routes: make(map[string]*rrEntry),
	}
}

// Bind 兼容旧接口：等价于 BindMany(src,[p],[1])
func (r *WeightedRRRouter) Bind(srcName string, p pool.WorkerPool) error {
	return r.BindMany(srcName, []pool.WorkerPool{p}, []int{1})
}

// BindMany：一个 stream 绑定多个 pool（带权重）；若已存在则报错（延续你现在 Bind 的语义）
func (r *WeightedRRRouter) BindMany(srcName string, pools []pool.WorkerPool, weights []int) error {
	//strongly recommend that the weights have a greatest common divisor (GCD) greater than 1 to optimize the size of the table.
	e, err := buildRREntry(pools, weights)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.routes[srcName]; ok {
		return fmt.Errorf("route exists for %s", srcName)
	}
	r.routes[srcName] = e
	return nil
}

// SetMany：覆盖式更新（留给后续调度器），自动负载或者用户手动调控都调SetMany
func (r *WeightedRRRouter) SetMany(srcName string, pools []pool.WorkerPool, weights []int) error {
	e, err := buildRREntry(pools, weights)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.routes[srcName] = e
	r.mu.Unlock()
	return nil
}

// Route：兼容旧 runtime 调用方式（不带 key）
func (r *WeightedRRRouter) Route(srcName string) (pool.WorkerPool, bool) {
	return r.RouteWithKey(srcName, nil)
}

// RouteWithKey：A2 阶段：无论 key 是否为空，都先按 WRR 分摊（A3 再对非空 key 改成 hash）
func (r *WeightedRRRouter) RouteWithKey(srcName string, key []byte) (pool.WorkerPool, bool) {
	r.mu.RLock()
	e := r.routes[srcName]
	r.mu.RUnlock()

	if e == nil {
		return nil, false
	}

	// A3：有 key -> hash 粘性路由
	if len(key) > 0 {
		if len(e.ring) == 0 {
			return nil, false
		}
		idx := int(fnv1a64(key) % uint64(len(e.ring)))
		t := e.ring[idx]
		return e.targets[t], true
	}

	// A2：无 key -> WRR 吞吐路由
	if len(e.table) == 0 {
		return nil, false
	}
	n := uint64(len(e.table))
	i := atomic.AddUint64(&e.cursor, 1) - 1
	t := e.table[i%n]
	return e.targets[t], true
}

func (r *WeightedRRRouter) Snapshot() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]string, len(r.routes))
	for src, e := range r.routes {
		cnt := make(map[string]int)
		for _, idx := range e.table {
			cnt[e.targets[idx].Name()]++
		}
		names := make([]string, 0, len(cnt))
		for name := range cnt {
			names = append(names, name)
		}
		sort.Strings(names)

		parts := make([]string, 0, len(names))
		for _, name := range names {
			parts = append(parts, fmt.Sprintf("%s*%d", name, cnt[name]))
		}
		out[src] = strings.Join(parts, ",")
	}
	return out
}

// --- helpers ---
func buildRREntry(pools []pool.WorkerPool, weights []int) (*rrEntry, error) {
	if len(pools) == 0 {
		return nil, fmt.Errorf("empty pools")
	}
	if len(pools) != len(weights) {
		return nil, fmt.Errorf("pools/weights length mismatch")
	}

	ps := make([]pool.WorkerPool, 0, len(pools))
	ws := make([]int, 0, len(weights))
	for i := range pools {
		if pools[i] == nil {
			return nil, fmt.Errorf("nil pool at %d", i)
		}
		if weights[i] <= 0 {
			continue
		}
		ps = append(ps, pools[i])
		ws = append(ws, weights[i])
	}
	if len(ps) == 0 {
		return nil, fmt.Errorf("all weights are <=0")
	}

	// gcd 缩放一次，避免 table 太大
	g := ws[0]
	for i := 1; i < len(ws); i++ {
		g = gcd(g, ws[i])
	}
	if g > 1 {
		for i := range ws {
			ws[i] /= g
		}
	}

	sum := 0
	for _, w := range ws {
		sum += w
	}
	table := make([]int, 0, sum)
	for i, w := range ws {
		for k := 0; k < w; k++ {
			table = append(table, i)
		}
	}

	ring := make([]int, len(table))
	copy(ring, table)

	return &rrEntry{targets: ps, table: table, ring: ring}, nil
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	return a
}

func fnv1a64(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}
