package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrPoolClosed = errors.New("pool closed")
)

type FixedPool struct {
	name    string
	size    int
	tasks   chan func()
	wg      sync.WaitGroup
	once    sync.Once
	stopped chan struct{} // 新增：只读关闭信号

	processed uint64 // 处理的任务总数
}

// 按key把kafka下游的数据二次划分，可能key123都进了池1，456进了池2，
// 池1中的两个协程来消费key123的数据，比如key1-1，key1-2，key2-1，key2-2，key2-3，key3-1
// 这样其实会出现乱序的情况，在v0.1.0版本中先不考虑这个问题，后续版本再优化
// 后续优化思路如下：

// 那其实，假如我们每个key分配一个协程池，固定key和协程池的对应关系（1对1），
// 只要协程池的zise>1，都会发生顺序问题对吧，所以我觉得针对，
// 我们需要数据有序这种情况，解决手段无非4种：
// 1. 设size=1，一刀切死
// 2. 多key对应单协程池的时候，
// 控制一个key一个协程，想一个办法让协程捕捉同一个key的数据，
// 比如key123进入了池1，那么我们要找出key1有哪些，key2有哪些，3有哪些，
// 然后分给三个协程来消费确保顺序
// 3. 加锁
// 4. 用redis暂存顺序不对的数据，比如要求1234这个顺序，结果协程消费完12后，
// 发现进来的数据是4，并且知道上一条是2（这一块应该设计一个保存状态，
// 比去数据库找更快），那么就会把4存到redis，等3进来了，再刷redis回写
// 但这一块我想不到除了固定频次刷redis其他的方法了）

// 最好的解决方案其实是2，需要一个按key分片的执行的串行执行器，
// 需要注意的是，当key很多的时候，协程池的size可能不够用，要固定一个上限，最好是=size

// Suppose we assign one worker pool per key and fix the key↔pool mapping (1:1).
// If a pool’s size > 1, in-order processing is no longer guaranteed.
// For workloads that require ordering, there are essentially four approaches:
//
// 1) Set size = 1 (hard cap).
//    - Simplest and safest: strict global ordering per pool.
//    - Downside: throughput limited; all keys handled by this pool are serialized.
//
// 2) Multiple keys share one pool, but enforce per-key serialization.
//    - Build a keyed/striped executor so that each key is handled by exactly one goroutine.
//    - Example: keys 1,2,3 are routed to pool #1; within that pool,
//      route key1, key2, key3 to three distinct single-threaded stripes to preserve order.
//
// 3) Locks.
//    - Not recommended as a standalone solution. Global locks reduce to size=1.
//      Per-key locks only work if tasks for the same key are enqueued FIFO
//      and executed under the same lock; otherwise ordering still breaks.
//
// 4) Redis-based reordering buffer.
//    - If the required order is 1→2→3→4 but we receive 1,2,4 while 3 is missing,
//      temporarily stash “4” in Redis and apply it once “3” arrives.
//    - Requires a fast per-key state (faster than hitting the DB).
//    - Drawbacks: higher complexity/latency; needs replay triggers beyond naive periodic flush.
//
// The best pragmatic solution is (2): a per-key sharded, serial executor.
// Caveat: when the number of distinct keys is large, you must cap concurrency with a fixed upper
// bound (number of stripes). Ideally, this upper bound equals the pool size.

func NewFixedPool(name string, size, queue int) *FixedPool {
	p := &FixedPool{
		name:    name,
		size:    max(1, size),
		tasks:   make(chan func(), max(0, queue)),
		stopped: make(chan struct{}),
	}
	// 启动 size 个 worker
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for t := range p.tasks { // 通道关闭后自然退出
				if t != nil {
					t() // 这里真正执行 handler(msg)
					atomic.AddUint64(&p.processed, 1)
				}
			}
		}()
	}
	return p
}

func (p *FixedPool) Name() string { return p.name }
func (p *FixedPool) Size() int    { return p.size }

func (p *FixedPool) Submit(ctx context.Context, task func()) (err error) {
	if task == nil {
		return errors.New("nil task")
	} // 小优化：拒绝 nil
	defer func() {
		if r := recover(); r != nil {
			err = ErrPoolClosed // 兜底：极端竞态仍不崩
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stopped: // 先看是否已停止，避免触发 send-on-closed
		return ErrPoolClosed
	case p.tasks <- task:
		return nil
	}
}

func (p *FixedPool) DrainAndStop(ctx context.Context) error {
	p.once.Do(func() {
		close(p.stopped) // 先发停止信号
		close(p.tasks)   // 再关任务通道
	})
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
	// 该submit在队列满时会阻塞等待，除非 ctx 取消。这个是有意设计（背压给上游
}

func (p *FixedPool) ProcessedCount() uint64 {
	return atomic.LoadUint64(&p.processed)
}

func (p *FixedPool) GetQueueDepth() int {
	return len(p.tasks)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Chinese comments are below:
// Concurrency control considerations for the pool's shutdown mechanism:
//
// 1. Using an `on` flag with a fine-grained lock (locking only the read of the flag)
//    is unsafe due to a race condition. A goroutine (A) could read `on == true`,
//    then another goroutine (B) could call `DrainAndStop`, setting `on = false` and
//    closing the channel. When goroutine A proceeds to send the task, it would
//    panic because it's sending on a closed channel.
//
// 2. Using a coarse-grained lock that covers the entire `Submit` function is also
//    problematic. If the `tasks` channel is full, the send operation `p.tasks <- task`
//    will block. Since the function holds the lock while blocked, any call to
//    `DrainAndStop` would also block trying to acquire the lock, leading to a deadlock.
//
// 3. In summary: An additional `stopped` channel is used to act as the `on` flag.
//    Since `stopped` is a channel, it enables sharing memory by communicating,
//    which is different from the boolean `on` flag approach of communicating by sharing memory.

// 如果用带有on的标志位，那就需要用锁来获取on的状态，
// 在submit函数中，如果锁的粒度控制在只获取的on状态的话，会发生如下情况
// 		假如说A加锁获取后得到了true，
// 		然后B正好drain了，
// 		停止往里边加任务p，
// 		等到A往里加的时候（此时已经归还锁），on其实是false了。
//
// 但是如果把锁的粒度控制在submit函数的整个过程的话，
// 会将case p.tasks <- task 涵盖进去
// 这样会有阻塞的风险，加入p的task管道满了，里边的任务数量大于了queue，
// 那么会在此阻塞，直到有worker取走任务腾出来新的空间，queue也可以加入新的任务
// 所以把锁加到整个submit环节中，也是有风险的
//
// 综上：多加一个stopped，用stopped来当作on，
// 这个stopped由于是管道，所以他也是通过通信来进行共享内存，
// 而不是on那种bool通过共享内存来通信。
