package pool

import "context"

type WorkerPool interface {
	Name() string
	Size() int
	Submit(ctx context.Context, task func()) error // 队列满或 ctx 超时返回错误
	DrainAndStop(ctx context.Context) error        // 停接单，等待任务跑完
}
