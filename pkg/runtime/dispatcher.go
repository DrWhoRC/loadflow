package runtime

import (
	"context"
	"log"
	"sync"

	"github.com/DrWhoRC/loadflow/pkg/pool"
)

// RouteDispatcher 负责将消息从 stream 异步提交到指定 pool
// 每个 (stream, pool) 组合有一个专属的 dispatcher
// 通过有界缓冲队列 + 单个 goroutine 的方式，避免 goroutine 爆炸
type RouteDispatcher struct {
	streamName string
	poolName   string
	pool       pool.WorkerPool

	// 有界缓冲队列（防止内存爆炸）
	taskQueue chan func()

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRouteDispatcher 创建一个 dispatcher
// bufferSize: 缓冲队列大小，建议 100-1000
func NewRouteDispatcher(
	streamName string,
	pool pool.WorkerPool,
	bufferSize int,
) *RouteDispatcher {
	return &RouteDispatcher{
		streamName: streamName,
		poolName:   pool.Name(),
		pool:       pool,
		taskQueue:  make(chan func(), bufferSize),
	}
}

// Start 启动 dispatcher 的消费 goroutine
// 这个 goroutine 会持续从 taskQueue 中取出任务并提交到 pool
func (d *RouteDispatcher) Start(ctx context.Context) {
	d.ctx, d.cancel = context.WithCancel(ctx)
	d.wg.Add(1)

	go func() {
		defer d.wg.Done()
		for {
			select {
			case <-d.ctx.Done():
				return
			case task := <-d.taskQueue:
				// 阻塞调用 Submit（只影响这一个 route）
				// 如果 pool 队列满，会在这里阻塞，但不会创建新的 goroutine
				if err := d.pool.Submit(d.ctx, task); err != nil {
					log.Printf("[dispatcher] submit failed stream=%s pool=%s err=%v",
						d.streamName, d.poolName, err)
				}
			}
		}
	}()
}

// TrySubmit 尝试将任务放入缓冲队列
// 返回 true 表示成功，false 表示队列满
// 这个方法是非阻塞的，保证 source goroutine 不会因为某个 pool 满而卡死
func (d *RouteDispatcher) TrySubmit(task func()) bool {
	select {
	case d.taskQueue <- task:
		return true
	default:
		// 队列满，丢弃消息并记录日志
		log.Printf("[dispatcher] queue full, drop message stream=%s pool=%s queueSize=%d",
			d.streamName, d.poolName, len(d.taskQueue))
		return false
	}
}

// Stop 停止 dispatcher
// 会等待当前队列中的任务处理完成
func (d *RouteDispatcher) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
}
