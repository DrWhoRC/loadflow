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

func (c *ChanSource) Close() error { return nil }
