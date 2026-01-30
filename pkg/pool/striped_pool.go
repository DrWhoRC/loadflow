package pool

import (
	"context"
	"errors"
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
)

var (
	ErrStripedPoolClosed = errors.New("striped pool closed")
)

// KeyedPool 接口：支持按 key 提交任务的 pool
// 强有序场景下使用，保证同一个 key 的任务按提交顺序串行执行
type KeyedPool interface {
	WorkerPool
	// SubmitWithKey 提交带 key 的任务
	// 同一个 key 的任务会被路由到同一个 stripe，串行执行
	// 不同 key 的任务会并行执行
	SubmitWithKey(ctx context.Context, key []byte, task func()) error
}

// stripe 代表一个独立的执行单元（队列 + 单个 goroutine）
// 保证该 stripe 内的任务严格按 FIFO 顺序串行执行
type stripe struct {
	id     int
	queue  chan func()
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 指标
	submitted uint64 // 提交到该 stripe 的任务数
	processed uint64 // 该 stripe 已处理的任务数
	panics    uint64 // 该 stripe 发生的 panic 次数
}

// StripedPool 是基于分片的工作池实现
// 核心设计：N 个 stripe，每个 stripe 有独立的队列和单个 goroutine
// 优势：
// 1. 同 key 任务按序执行（通过 hash(key) % N 固定分配到同一 stripe）
// 2. 不同 key 任务并行执行（分配到不同 stripe）
// 3. 单个 stripe 故障不影响其他 stripe
type StripedPool struct {
	name    string
	stripes []*stripe
	count   int // stripe 数量（等同于并发度）

	// 全局状态
	mu      sync.RWMutex
	closed  bool
	stopped chan struct{}

	// 全局指标
	totalSubmitted uint64
	totalProcessed uint64
	totalPanics    uint64
}

// NewStripedPool 创建一个新的分片池
// name: 池名称
// stripeCount: 分片数量（并发度），建议设置为 CPU 核心数或略小
// queueSizePerStripe: 每个分片的队列大小
func NewStripedPool(name string, stripeCount int, queueSizePerStripe int) *StripedPool {
	if stripeCount <= 0 {
		stripeCount = 1
	}
	if queueSizePerStripe < 0 {
		queueSizePerStripe = 0
	}

	stripes := make([]*stripe, stripeCount)
	for i := 0; i < stripeCount; i++ {
		stripes[i] = &stripe{
			id:    i,
			queue: make(chan func(), queueSizePerStripe),
		}
	}

	return &StripedPool{
		name:    name,
		stripes: stripes,
		count:   stripeCount,
		stopped: make(chan struct{}),
	}
}

// Start 启动所有 stripe 的 worker goroutine
// 必须在提交任务前调用
func (sp *StripedPool) Start(ctx context.Context) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if sp.closed {
		return
	}

	for _, s := range sp.stripes {
		s.ctx, s.cancel = context.WithCancel(ctx)
		s.wg.Add(1)

		go sp.runStripe(s)
	}

	log.Printf("[StripedPool] %s started with %d stripes", sp.name, sp.count)
}

// runStripe 运行单个 stripe 的消费循环
// 核心特性：
// 1. 串行消费（单 goroutine）
// 2. Panic 恢复并重启
// 3. 优雅退出
func (sp *StripedPool) runStripe(s *stripe) {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			// 上下文取消，优雅退出
			return

		case task, ok := <-s.queue:
			if !ok {
				// 队列关闭，退出
				return
			}

			// 执行任务，捕获 panic
			sp.executeTask(s, task)
		}
	}
}

// executeTask 执行单个任务，带 panic 恢复
func (sp *StripedPool) executeTask(s *stripe, task func()) {
	defer func() {
		if r := recover(); r != nil {
			// 记录 panic
			atomic.AddUint64(&s.panics, 1)
			atomic.AddUint64(&sp.totalPanics, 1)

			log.Printf("[StripedPool] %s stripe-%d panic recovered: %v", sp.name, s.id, r)

			// Stripe 继续运行，不影响其他任务
			// 这里不需要重启 goroutine，因为 runStripe 循环会继续
		}
	}()

	// 执行任务
	task()

	// 更新计数器
	atomic.AddUint64(&s.processed, 1)
	atomic.AddUint64(&sp.totalProcessed, 1)
}

// Name 返回池名称
func (sp *StripedPool) Name() string {
	return sp.name
}

// Size 返回并发度（stripe 数量）
func (sp *StripedPool) Size() int {
	return sp.count
}

// Submit 提交无 key 的任务
// 会随机分配到某个 stripe（使用轮询或随机）
func (sp *StripedPool) Submit(ctx context.Context, task func()) error {
	sp.mu.RLock()
	if sp.closed {
		sp.mu.RUnlock()
		return ErrStripedPoolClosed
	}
	sp.mu.RUnlock()

	// 使用原子计数器实现轮询分配
	// 注意：这里不增加 totalSubmitted，由 submitToStripe 统一处理
	idx := int(atomic.LoadUint64(&sp.totalSubmitted)) % sp.count
	if idx < 0 {
		idx = 0
	}
	return sp.submitToStripe(ctx, idx, task)
}

// SubmitWithKey 提交带 key 的任务
// 同一个 key 的任务会被分配到同一个 stripe，保证有序执行
func (sp *StripedPool) SubmitWithKey(ctx context.Context, key []byte, task func()) error {
	sp.mu.RLock()
	if sp.closed {
		sp.mu.RUnlock()
		return ErrStripedPoolClosed
	}
	sp.mu.RUnlock()

	// 根据 key 计算 stripe 索引
	idx := sp.selectStripe(key)
	return sp.submitToStripe(ctx, idx, task)
}

// selectStripe 根据 key 选择 stripe
// 使用 FNV-1a hash 算法，快速且分布均匀
func (sp *StripedPool) selectStripe(key []byte) int {
	if len(key) == 0 {
		// 无 key 的情况，轮询分配
		// 注意：这里不增加计数，由 submitToStripe 统一处理
		return int(atomic.LoadUint64(&sp.totalSubmitted)) % sp.count
	}

	// FNV-1a hash
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % sp.count
}

// submitToStripe 将任务提交到指定的 stripe
func (sp *StripedPool) submitToStripe(ctx context.Context, idx int, task func()) error {
	s := sp.stripes[idx]

	// 增加提交计数
	atomic.AddUint64(&s.submitted, 1)
	atomic.AddUint64(&sp.totalSubmitted, 1)

	// 非阻塞提交
	select {
	case s.queue <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-sp.stopped:
		return ErrStripedPoolClosed
	}
}

// DrainAndStop 停止接收新任务，等待所有已提交的任务执行完毕
func (sp *StripedPool) DrainAndStop(ctx context.Context) error {
	sp.mu.Lock()
	if sp.closed {
		sp.mu.Unlock()
		return nil
	}
	sp.closed = true
	close(sp.stopped)
	sp.mu.Unlock()

	log.Printf("[StripedPool] %s draining...", sp.name)

	// 1. 等待所有队列清空（在超时范围内）
	drainStart := make(chan struct{})
	go func() {
		for {
			allEmpty := true
			for _, s := range sp.stripes {
				if len(s.queue) > 0 {
					allEmpty = false
					break
				}
			}
			if allEmpty {
				close(drainStart)
				return
			}
			select {
			case <-ctx.Done():
				close(drainStart)
				return
			default:
				// 短暂休眠避免忙等待
			}
		}
	}()

	// 等待队列清空或超时
	select {
	case <-drainStart:
		// 队列已清空
	case <-ctx.Done():
		log.Printf("[StripedPool] %s drain timeout, forcing shutdown", sp.name)
	}

	// 2. 关闭所有 stripe 的队列
	for _, s := range sp.stripes {
		close(s.queue)
	}

	// 3. 取消所有 stripe 的上下文
	for _, s := range sp.stripes {
		if s.cancel != nil {
			s.cancel()
		}
	}

	// 4. 等待所有 stripe 的 goroutine 完成，带超时
	done := make(chan struct{})
	go func() {
		for _, s := range sp.stripes {
			s.wg.Wait()
		}
		close(done)
	}()

	select {
	case <-done:
		log.Printf("[StripedPool] %s drained successfully", sp.name)
		return nil
	case <-ctx.Done():
		log.Printf("[StripedPool] %s drain timeout", sp.name)
		return ctx.Err()
	}
}

// GetMetrics 返回池的指标信息
func (sp *StripedPool) GetMetrics() PoolMetrics {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	// 计算所有 stripe 的队列深度
	var totalQueueDepth int
	var totalQueueCapacity int
	stripeMetrics := make([]StripeMetrics, sp.count)

	for i, s := range sp.stripes {
		depth := len(s.queue)
		capacity := cap(s.queue)

		totalQueueDepth += depth
		totalQueueCapacity += capacity

		stripeMetrics[i] = StripeMetrics{
			ID:        s.id,
			Submitted: atomic.LoadUint64(&s.submitted),
			Processed: atomic.LoadUint64(&s.processed),
			Panics:    atomic.LoadUint64(&s.panics),
			QueueSize: depth,
			Capacity:  capacity,
		}
	}

	return PoolMetrics{
		Name:          sp.name,
		Size:          sp.count,
		QueueDepth:    totalQueueDepth,
		QueueCapacity: totalQueueCapacity,
		Processed:     atomic.LoadUint64(&sp.totalProcessed),
		Submitted:     atomic.LoadUint64(&sp.totalSubmitted),
		Panics:        atomic.LoadUint64(&sp.totalPanics),
		Stripes:       stripeMetrics,
	}
}

// StripeMetrics 单个 stripe 的指标
type StripeMetrics struct {
	ID        int
	Submitted uint64
	Processed uint64
	Panics    uint64
	QueueSize int
	Capacity  int
}
