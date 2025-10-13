package pool

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrPoolClosed = errors.New("pool closed")
)

type FixedPool struct {
	name  string
	size  int
	tasks chan func()

	wg sync.WaitGroup // 等待所有 worker 退出
	mu sync.RWMutex
	on bool // 接单开关
}

func NewFixedPool(name string, size int, queue int) *FixedPool {
	if size <= 0 {
		size = 1
	}
	if queue < 0 {
		queue = 0
	}
	p := &FixedPool{
		name:  name,
		size:  size,
		tasks: make(chan func(), queue),
		on:    true,
	}
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for t := range p.tasks {
				if t != nil {
					t()
				}
			}
		}()
	}
	return p
}

func (p *FixedPool) Name() string { return p.name }
func (p *FixedPool) Size() int    { return p.size }

func (p *FixedPool) Submit(ctx context.Context, task func()) error {
	p.mu.RLock()
	on := p.on
	p.mu.RUnlock()
	if !on {
		return ErrPoolClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.tasks <- task:
		return nil
	}
}

func (p *FixedPool) DrainAndStop(ctx context.Context) error {
	// 停止接单
	p.mu.Lock()
	if !p.on {
		p.mu.Unlock()
		return nil
	}
	p.on = false
	close(p.tasks) // 关闭后 worker 会自然退出
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
