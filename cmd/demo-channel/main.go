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

	fmt.Println("[Main] Starting loadflow demo...")

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

	fmt.Println("[Main] Components registered, starting runtime...")

	// 启动 runtime
	go func() {
		fmt.Println("[Runtime] Starting...")
		_ = rt.Start(ctx)
		fmt.Println("[Runtime] Stopped.")
	}()

	// === 新增：metrics 监控协程 ===
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		var lastFast, lastSlow uint64

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pf := poolFast.ProcessedCount()
				ps := poolSlow.ProcessedCount()

				incFast := pf - lastFast
				incSlow := ps - lastSlow

				qf := poolFast.GetQueueDepth()
				qs := poolSlow.GetQueueDepth()

				fmt.Printf("[Metrics] fast: +%d (total=%d, q=%d) | slow: +%d (total=%d, q=%d)\n",
					incFast, pf, qf,
					incSlow, ps, qs,
				)

				lastFast = pf
				lastSlow = ps
			}
		}
	}()

	fmt.Println("[Main] Data producers starting...")

	// 产生数据（A 更高吞吐，B 更低）
	go func() {
		t := time.NewTicker(1 * time.Millisecond)
		defer t.Stop()
		for i := 0; i < 5000; i++ {
			<-t.C
			chA <- []byte(fmt.Sprintf("A-%d", i))
		}
		// 这里先不 close，后面目标 C 再一起处理生命周期
	}()

	go func() {
		t := time.NewTicker(2 * time.Millisecond)
		defer t.Stop()
		for i := 0; i < 1000; i++ {
			<-t.C
			chB <- []byte(fmt.Sprintf("B-%d", i))
		}
	}()

	fmt.Println("[Main] Running for 15 seconds...")
	// 运行一段时间后优雅关闭
	time.Sleep(15 * time.Second)

	fmt.Println("[Main] Initiating graceful shutdown...")
	_ = rt.Stop(context.Background())
	fmt.Println("[Main] Shutdown complete.")
}
