package source

import (
	"context"
	"errors"
)

var ErrClosed = errors.New("source closed")

type ChanSource struct {
	name string
	ch   <-chan []byte
	// 这里只读通道；Close 为幂等空操作
}

func NewChanSource(name string, ch <-chan []byte) *ChanSource {
	return &ChanSource{name: name, ch: ch}
}

func (c *ChanSource) Name() string { return c.name }

func (c *ChanSource) Recv(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m, ok := <-c.ch:
		if !ok {
			return nil, ErrClosed
		}
		return m, nil
	}
}

// 只读通道不能close，对于只读通道，关闭的渠道和责任在于发送数据一方，所以需要使用其他方法在发送方关闭
func (c *ChanSource) Close() error { return nil }
