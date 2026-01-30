package runtime

import (
	"context"
	"log"
	"sync"

	"github.com/DrWhoRC/loadflow/pkg/consumer"
	"github.com/DrWhoRC/loadflow/pkg/flow/router"
	"github.com/DrWhoRC/loadflow/pkg/flow/source"
	"github.com/DrWhoRC/loadflow/pkg/message"
	"github.com/DrWhoRC/loadflow/pkg/pool"
)

// Runtime 是整个数据流处理引擎的接口定义。
// 它定义了注册组件、启动和停止数据流处理的能力。
type Runtime interface {
	RegisterSource(src source.Source) error
	RegisterPool(p pool.WorkerPool) error
	UseRouter(r router.Router)
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	DumpMetrics() []pool.PoolMetrics

	// UseMessageCodec 允许用户替换默认 JSON 协议（未来扩展用）
	UseMessageCodec(c message.Codec)
	UseKeyFunc(fn router.KeyFunc)

	// RegisterSource 注册一个数据源。
	// RegisterPool 注册一个工作协程池。
	// UseRouter 指定一个路由器用于连接数据源和协程池。
	// Start 启动整个运行时引擎。这是一个阻塞方法，直到上下文被取消或 Stop 被调用。
	// Stop 优雅地停止整个运行时引擎。
	// UseKeyFunc 允许用户定义 key 的来源：
	// - 若返回 nil/空：表示无 key
	// - 若返回非空：表示启用 key
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

	ctx      context.Context    // ctx 是整个运行时生命周期的上下文。
	cancel   context.CancelFunc // cancel 是用于取消上述上下文的函数，调用它会触发整个运行时的停止流程。
	wgSrc    sync.WaitGroup     // wgSrc 用于等待所有数据源的读取 goroutine 安全退出。
	msgCodec message.Codec

	// KeyFunc：允许用户覆盖/补充 key 的提取逻辑
	// - 输入：srcName + payload（注意：这里是 payload，不是 raw）
	// - 返回：key（nil/空表示无 key）
	keyFn router.KeyFunc

	// Dispatcher 管理：每个 (stream, pool) 组合有一个专属的 dispatcher
	// key 格式："streamName:poolName"
	dispatchers map[string]*RouteDispatcher
	dispMu      sync.RWMutex
	bufferSize  int // dispatcher 的缓冲队列大小
}

// New 创建一个新的 Runtime 实例。
// 参数 h 是一个 consumer.Handler 函数，它定义了如何处理从数据源接收到的每一条消息。
func New(h consumer.Handler) Runtime {
	return &runtime{
		h:           h,
		srcs:        make(map[string]source.Source),
		pools:       make(map[string]pool.WorkerPool),
		dispatchers: make(map[string]*RouteDispatcher),
		msgCodec:    message.NewJSONCodec(),
		bufferSize:  100,
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

// getOrCreateDispatcher 获取或创建指定 (stream, pool) 的 dispatcher
// 采用懒加载方式：首次路由到某个 pool 时才创建对应的 dispatcher
// 这样可以根据实际路由关系动态创建，无需提前配置
func (r *runtime) getOrCreateDispatcher(streamName string, p pool.WorkerPool, bufferSize int) *RouteDispatcher {
	key := streamName + ":" + p.Name()

	// 先尝试读锁（快速路径）
	r.dispMu.RLock()
	disp, exists := r.dispatchers[key]
	r.dispMu.RUnlock()

	if exists {
		return disp
	}

	// 不存在则创建（慢速路径）
	r.dispMu.Lock()
	defer r.dispMu.Unlock()

	// 再次检查（double-check，防止并发创建）
	if disp, exists := r.dispatchers[key]; exists {
		return disp
	}

	// 创建新 dispatcher
	// bufferSize=100: 可以根据实际场景调整
	disp = NewRouteDispatcher(streamName, p, bufferSize)
	disp.Start(r.ctx)
	r.dispatchers[key] = disp

	log.Printf("[runtime] created dispatcher: %s -> %s (buffer=%d)", streamName, p.Name(), bufferSize)
	return disp
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

			//取出当前配置的 codec、keyFn、router 和 handler，确保，就算中间有配置或者handler更改，当前的任务也要按照当时的配置和handler来处理
			r.rtMu.RLock()
			codec := r.msgCodec
			keyFn := r.keyFn
			ro := r.router
			h := r.h
			buffer_size := r.bufferSize
			if buffer_size <= 0 {
				buffer_size = 100 // 默认值
			}
			r.rtMu.RUnlock()

			if codec == nil {
				codec = message.NewJSONCodec()
			}

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

				//1.5 解析消息，提取 payload 和 key
				raw := msg
				// 先用 codec 解出 (key,payload)
				key, payload, ok := codec.Decode(raw)
				if !ok {
					// 降级：raw 当作 payload，key 为空
					payload = raw
					key = nil
				}

				// 用户自定义 keyFn：可覆盖或补充 key
				if keyFn != nil {
					k2 := keyFn(srcName, payload)
					if len(k2) > 0 {
						key = k2
					} else {
						key = nil
					}
				}

				// 2. 根据数据源名称，通过路由器查找应该处理这条消息的协程池。
				var (
					p     pool.WorkerPool
					isKey bool
				)
				if kr, isKeyRouter := ro.(router.KeyRouter); isKeyRouter {
					p, isKey = kr.RouteWithKey(srcName, key) // A2 用 keyless -> WRR；A3 再用 keyed -> hash
				} else {
					p, isKey = ro.Route(srcName)
				}
				if !isKey {
					log.Printf("[runtime] no route for src=%s, drop", srcName)
					continue // 如果没有找到路由，则丢弃消息，继续下一轮循环。
				}

				// 3. 创建一个任务（一个闭包函数），该任务封装了对消息的实际处理逻辑。
				currentPayload := payload // 捕获当前的 payload 变量
				task := func() {
					// 调用用户传入的 Handler 函数处理消息。
					if err := h(currentPayload); err != nil {
						log.Printf("[handler] error src=%s err=%v", srcName, err)
					}
				}

				// 4. 将任务提交到 dispatcher（而不是直接提交到 pool）
				//    dispatcher 内部有缓冲队列 + 单个 goroutine 处理
				//    这样避免了 goroutine 爆炸，同时保持了一个 pool 阻塞不影响其他 pool 的特性
				disp := r.getOrCreateDispatcher(srcName, p, buffer_size)
				if !disp.TrySubmit(task) {
					// 队列满时丢弃消息（已在 TrySubmit 内部记录日志）
					// 未来可以考虑其他策略：阻塞、降级等
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

	// 1) 取消内部上下文，这将向所有正在 Recv() 的 goroutine 发送停止信号。
	//    这些 goroutine 将会从 Recv() 处唤醒，然后检查到 r.ctx.Done() 并退出。
	if cancel != nil {
		cancel()
	}

	// 2) 等待所有数据源的 goroutine 完全退出。
	//    这确保了不会再有新的任务被提交到协程池中。
	r.wgSrc.Wait()

	// 3) 停止所有 dispatcher
	//    确保 dispatcher 的缓冲队列中的任务都被处理完
	r.dispMu.RLock()
	dispatchers := make([]*RouteDispatcher, 0, len(r.dispatchers))
	for _, disp := range r.dispatchers {
		dispatchers = append(dispatchers, disp)
	}
	r.dispMu.RUnlock()

	for _, disp := range dispatchers {
		disp.Stop()
	}

	// 4) 排空并停止所有协程池。
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

func (r *runtime) DumpMetrics() []pool.PoolMetrics {
	r.rtMu.RLock()
	defer r.rtMu.RUnlock()

	res := make([]pool.PoolMetrics, 0, len(r.pools))

	for name, p := range r.pools {
		mp, ok := p.(pool.PoolWithMetrics)
		if !ok {
			// 这个 pool 还没实现 Metrics，就先跳过
			continue
		}

		m := mp.GetMetrics()
		if m.Name == "" {
			m.Name = name
		}
		res = append(res, m)
	}

	return res
}

func (r *runtime) UseMessageCodec(c message.Codec) {
	r.rtMu.Lock()
	defer r.rtMu.Unlock()
	if c == nil {
		r.msgCodec = message.NewJSONCodec()
		return
	}
	r.msgCodec = c
}

func (r *runtime) UseKeyFunc(fn router.KeyFunc) {
	r.rtMu.Lock()
	defer r.rtMu.Unlock()
	r.keyFn = fn
}

func (r *runtime) SetDispatcherBufferSize(size int) {
	r.rtMu.Lock()
	defer r.rtMu.Unlock()
	if size <= 0 {
		size = 100 // 保证至少有合理的默认值
	}
	r.bufferSize = size
}
