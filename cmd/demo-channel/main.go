package main

import (
	"context"
	"fmt"
	"time"

	"github.com/DrWhoRC/loadflow/pkg/consumer"
	"github.com/DrWhoRC/loadflow/pkg/flow/router"
	"github.com/DrWhoRC/loadflow/pkg/flow/source"
	"github.com/DrWhoRC/loadflow/pkg/pool"
	"github.com/DrWhoRC/loadflow/pkg/runtime"
)

func main() {
	// 业务处理函数（此处仅模拟耗时）
	h := consumer.Handler(func(msg []byte) error {
		_ = msg
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	// 两条数据流（chan 模拟）
	chA := make(chan []byte, 1024)
	chB := make(chan []byte, 1024)
	srcA := source.NewChanSource("stream_a", chA)
	srcB := source.NewChanSource("stream_b", chB)

	// 两个协程池（固定大小）
	poolFast := pool.NewFixedPool("pool_fast", 8, 2048)
	poolSlow := pool.NewFixedPool("pool_slow", 2, 256)

	// 固定路由
	r := router.NewInMemory()
	_ = r.Bind(srcA.Name(), poolFast)
	_ = r.Bind(srcB.Name(), poolSlow)

	// 运行时编排
	rt := runtime.New(h)
	_ = rt.RegisterSource(srcA)
	_ = rt.RegisterSource(srcB)
	_ = rt.RegisterPool(poolFast)
	_ = rt.RegisterPool(poolSlow)
	rt.UseRouter(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动
	go func() { _ = rt.Start(ctx) }()

	// 产生数据（A 更高吞吐，B 更低）
	go func() {
		t := time.NewTicker(2 * time.Millisecond)
		for i := 0; i < 5000; i++ {
			<-t.C
			chA <- []byte(fmt.Sprintf("A-%d", i))
		}
	}()
	go func() {
		t := time.NewTicker(10 * time.Millisecond)
		for i := 0; i < 1000; i++ {
			<-t.C
			chB <- []byte(fmt.Sprintf("B-%d", i))
		}
	}()

	// 运行一段时间后优雅关闭
	time.Sleep(15 * time.Second)
	_ = rt.Stop(context.Background())
}
