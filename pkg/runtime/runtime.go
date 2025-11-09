package runtime

import (
	"context"
	"log"
	"sync"

	"github.com/DrWhoRC/loadflow/pkg/consumer"
	"github.com/DrWhoRC/loadflow/pkg/flow/router"
	"github.com/DrWhoRC/loadflow/pkg/flow/source"
	"github.com/DrWhoRC/loadflow/pkg/pool"
)

// Runtime 是整个数据流处理引擎的接口定义。
// 它定义了注册组件、启动和停止数据流处理的能力。
type Runtime interface {
	// RegisterSource 注册一个数据源。
	RegisterSource(src source.Source) error
	// RegisterPool 注册一个工作协程池。
	RegisterPool(p pool.WorkerPool) error
	// UseRouter 指定一个路由器用于连接数据源和协程池。
	UseRouter(r router.Router)
	// Start 启动整个运行时引擎。这是一个阻塞方法，直到上下文被取消或 Stop 被调用。
	Start(ctx context.Context) error
	// Stop 优雅地停止整个运行时引擎。
	Stop(ctx context.Context) error
}

// runtime 是 Runtime 接口的具体实现。
// 它作为核心协调器，管理数据源、协程池和路由器，并驱动整个数据流。
type runtime struct {
	h consumer.Handler // h 是最终处理消息的业务逻辑函数。

	rtMu    sync.RWMutex               // rtMu 是一个读写锁，用于保护整个 runtime 结构体的并发访问。
	srcs    map[string]source.Source   // srcs 存储所有已注册的数据源，以数据源名称为键。
	pools   map[string]pool.WorkerPool // pools 存储所有已注册的协程池，以协程池名称为键。
	router  router.Router              // router 定义了从数据源到协程池的路由规则。
	started bool                       // started 标记运行时是否已经启动。

	ctx    context.Context    // ctx 是整个运行时生命周期的上下文。
	cancel context.CancelFunc // cancel 是用于取消上述上下文的函数，调用它会触发整个运行时的停止流程。
	wgSrc  sync.WaitGroup     // wgSrc 用于等待所有数据源的读取 goroutine 安全退出。
}

// New 创建一个新的 Runtime 实例。
// 参数 h 是一个 consumer.Handler 函数，它定义了如何处理从数据源接收到的每一条消息。
func New(h consumer.Handler) Runtime {
	return &runtime{
		h:     h,
		srcs:  make(map[string]source.Source),
		pools: make(map[string]pool.WorkerPool),
	}
}

// RegisterSource 用于向运行时注册一个数据源。
// 这个操作是线程安全的。
func (r *runtime) RegisterSource(src source.Source) error {
	r.rtMu.Lock()
	defer r.rtMu.Unlock()
	r.srcs[src.Name()] = src
	return nil
}

// RegisterPool 用于向运行时注册一个工作协程池。
// 这个操作是线程安全的。
func (r *runtime) RegisterPool(p pool.WorkerPool) error {
	r.rtMu.Lock()
	defer r.rtMu.Unlock()
	r.pools[p.Name()] = p
	return nil
}

// UseRouter 用于为运行时设置一个路由器。
// 这个操作是线程安全的。
func (r *runtime) UseRouter(ro router.Router) {
	r.rtMu.Lock()
	defer r.rtMu.Unlock()
	r.router = ro
}

// Start 启动运行时引擎，开始处理数据流。
// 这是一个阻塞方法，它会一直运行直到传入的 ctx 被取消，或者 Stop 方法被调用。
func (r *runtime) Start(ctx context.Context) error {
	r.rtMu.Lock()
	// 防止重复启动
	if r.started {
		r.rtMu.Unlock()
		return nil
	}
	// 必须设置路由器
	if r.router == nil {
		r.rtMu.Unlock()
		return nil
	}
	// 创建一个内部的、可取消的上下文，用于控制整个运行时的生命周期。
	// 这样，即使外部传入的 ctx 不可取消，我们依然能通过调用 r.cancel() 来停止运行时。
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.started = true
	r.rtMu.Unlock()

	// --- 核心逻辑：为每个数据源启动一个专属的 goroutine ---
	// 这个 goroutine 负责不断地从数据源拉取数据，并将其提交到对应的协程池。
	r.rtMu.RLock()
	for name, src := range r.srcs {
		r.wgSrc.Add(1) // 为即将启动的 goroutine 增加等待计数
		go func(srcName string, s source.Source) {
			defer r.wgSrc.Done() // 当 goroutine 退出时，减少等待计数
			for {
				// 1. 从数据源阻塞式地接收一条消息。
				//    如果 r.ctx 被取消，Recv 会立刻返回一个错误。
				msg, err := s.Recv(r.ctx)
				if err != nil {
					// 当 Recv 返回错误时，检查是否是由于运行时被要求停止。
					select {
					case <-r.ctx.Done():
						// 如果是上下文被取消（即 Stop 被调用），则正常退出 goroutine。
						return
					default:
						// 如果是其他错误（如数据源自身关闭），记录日志并退出该 goroutine。
						log.Printf("[runtime] recv error src=%s err=%v", srcName, err)
						return
					}
				}

				// 2. 根据数据源名称，通过路由器查找应该处理这条消息的协程池。
				p, ok := r.router.Route(srcName)
				if !ok {
					log.Printf("[runtime] no route for src=%s, drop", srcName)
					continue // 如果没有找到路由，则丢弃消息，继续下一轮循环。
				}

				// 3. 创建一个任务（一个闭包函数），该任务封装了对消息的实际处理逻辑。
				task := func() {
					// 调用用户传入的 Handler 函数处理消息。
					if err := r.h(msg); err != nil {
						log.Printf("[handler] error src=%s err=%v", srcName, err)
					}
				}

				// 4. 将任务提交到协程池。
				//    如果协程池已关闭或上下文被取消，Submit 会返回错误。
				if err := p.Submit(r.ctx, task); err != nil {
					log.Printf("[runtime] submit failed src=%s pool=%s err=%v", srcName, p.Name(), err)
				}
			}
		}(name, src)
	}
	r.rtMu.RUnlock()

	// 阻塞 Start 函数，直到 r.ctx 被取消（即 Stop 被调用）。
	<-r.ctx.Done()
	return nil
}

// Stop 优雅地停止整个运行时。
// 它会确保所有正在处理的任务都执行完毕，并且不会接收新的任务。
func (r *runtime) Stop(ctx context.Context) error {
	r.rtMu.RLock()
	if !r.started {
		r.rtMu.RUnlock()
		return nil
	}
	cancel := r.cancel
	r.rtMu.RUnlock()

	// --- 优雅停机三部曲 ---

	// 1) 取消内部上下文，这将向所有正在 Recv() 的 goroutine 发送停止信号。
	//    这些 goroutine 将会从 Recv() 处唤醒，然后检查到 r.ctx.Done() 并退出。
	if cancel != nil {
		cancel()
	}

	// 2) 等待所有数据源的 goroutine 完全退出。
	//    这确保了不会再有新的任务被提交到协程池中。
	r.wgSrc.Wait()

	// 3) 排空并停止所有协程池。
	//    DrainAndStop 会等待池中所有已存在的任务执行完毕，然后关闭所有工作协程。
	r.rtMu.RLock()
	defer r.rtMu.RUnlock()
	for _, p := range r.pools {
		if err := p.DrainAndStop(ctx); err != nil {
			// 在关闭池时，使用传入的 Stop 方法的上下文，
			// 这允许为停机过程本身设置一个超时时间。
			return err
		}
	}
	return nil
}
