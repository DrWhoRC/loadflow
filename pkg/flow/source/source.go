package source

import "context"

type Source interface {
	Name() string
	Recv(ctx context.Context) ([]byte, error) // 阻塞取一条；ctx 取消时返回
	Close() error                             // 幂等
}
