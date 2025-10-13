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
	name    string
	size    int
	tasks   chan func()
	wg      sync.WaitGroup
	once    sync.Once
	stopped chan struct{} // 新增：只读关闭信号
}

func NewFixedPool(name string, size, queue int) *FixedPool {
	p := &FixedPool{
		name: name, size: max(1, size),
		tasks:   make(chan func(), max(0, queue)),
		stopped: make(chan struct{}),
	}
	return p
}

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
