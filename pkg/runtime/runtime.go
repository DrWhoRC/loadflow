package runtime

import (
	"context"
	"log"
	"sync"

	"github.com/yourname/loadflow/pkg/consumer"
	"github.com/yourname/loadflow/pkg/flow/router"
	"github.com/yourname/loadflow/pkg/flow/source"
	"github.com/yourname/loadflow/pkg/pool"
)

type Runtime interface {
	RegisterSource(src source.Source) error
	RegisterPool(p pool.WorkerPool) error
	UseRouter(r router.Router)
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type runtime struct {
	h       consumer.Handler
	rtMu    sync.RWMutex
	srcs    map[string]source.Source
	pools   map[string]pool.WorkerPool
	router  router.Router
	started bool

	ctx    context.Context
	cancel context.CancelFunc
	wgSrc  sync.WaitGroup
}

func New(h consumer.Handler) Runtime {
	return &runtime{
		h:     h,
		srcs:  make(map[string]source.Source),
		pools: make(map[string]pool.WorkerPool),
	}
}

func (r *runtime) RegisterSource(src source.Source) error {
	r.rtMu.Lock()
	defer r.rtMu.Unlock()
	r.srcs[src.Name()] = src
	return nil
}

func (r *runtime) RegisterPool(p pool.WorkerPool) error {
	r.rtMu.Lock()
	defer r.rtMu.Unlock()
	r.pools[p.Name()] = p
	return nil
}

func (r *runtime) UseRouter(ro router.Router) {
	r.rtMu.Lock()
	defer r.rtMu.Unlock()
	r.router = ro
}

func (r *runtime) Start(ctx context.Context) error {
	r.rtMu.Lock()
	if r.started {
		r.rtMu.Unlock()
		return nil
	}
	if r.router == nil {
		r.rtMu.Unlock()
		return nil
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.started = true
	r.rtMu.Unlock()

	// 为每个 Source 开 goroutine：Recv → Route → Submit
	r.rtMu.RLock()
	for name, src := range r.srcs {
		r.wgSrc.Add(1)
		go func(srcName string, s source.Source) {
			defer r.wgSrc.Done()
			for {
				msg, err := s.Recv(r.ctx)
				if err != nil {
					// ctx 取消或 source 关闭时退出
					select {
					case <-r.ctx.Done():
						return
					default:
						// 其它错误：简单记录继续
						log.Printf("[runtime] recv error src=%s err=%v", srcName, err)
						return
					}
				}
				p, ok := r.router.Route(srcName)
				if !ok {
					log.Printf("[runtime] no route for src=%s, drop", srcName)
					continue
				}
				task := func() {
					if err := r.h(msg); err != nil {
						log.Printf("[handler] error src=%s err=%v", srcName, err)
					}
				}
				if err := p.Submit(r.ctx, task); err != nil {
					log.Printf("[runtime] submit failed src=%s pool=%s err=%v", srcName, p.Name(), err)
				}
			}
		}(name, src)
	}
	r.rtMu.RUnlock()

	// 阻塞等待 Stop
	<-r.ctx.Done()
	return nil
}

func (r *runtime) Stop(ctx context.Context) error {
	r.rtMu.RLock()
	if !r.started {
		r.rtMu.RUnlock()
		return nil
	}
	cancel := r.cancel
	r.rtMu.RUnlock()

	// 1) 取消运行上下文 → 所有 Recv 退出
	if cancel != nil {
		cancel()
	}

	// 2) 等所有 Source goroutine 退出
	r.wgSrc.Wait()

	// 3) Drain 所有池
	r.rtMu.RLock()
	defer r.rtMu.RUnlock()
	for _, p := range r.pools {
		if err := p.DrainAndStop(ctx); err != nil {
			return err
		}
	}
	return nil
}
