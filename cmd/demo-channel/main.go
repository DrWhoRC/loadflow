package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/DrWhoRC/loadflow/pkg/consumer"
	"github.com/DrWhoRC/loadflow/pkg/flow/router"
	"github.com/DrWhoRC/loadflow/pkg/flow/source"
	"github.com/DrWhoRC/loadflow/pkg/metrics"
	"github.com/DrWhoRC/loadflow/pkg/pool"
	"github.com/DrWhoRC/loadflow/pkg/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// 业务处理函数
	h := consumer.Handler(func(msg []byte) error {
		_ = msg
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	fmt.Println("[Main] Starting loadflow demo...")

	// 两条数据流
	chA := make(chan []byte, 1024)
	chB := make(chan []byte, 1024)
	srcA := source.NewChanSource("stream_a", chA)
	srcB := source.NewChanSource("stream_b", chB)

	// 两个协程池
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

	// Prometheus registry + exporter
	reg := prometheus.NewRegistry()
	exporter := metrics.NewPrometheusExporter(
		rt,
		reg,
		metrics.ExporterOptions{
			Namespace: "loadflow",
			Subsystem: "runtime",
		},
	)

	// 启动 exporter（定期从 runtime 拉指标）
	go exporter.Start(ctx, time.Second)

	// 暴露 /metrics HTTP server
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	srv := &http.Server{
		Addr:    ":2112",
		Handler: mux,
	}

	go func() {
		log.Println("metrics server listening on :2112")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics server error: %v", err)
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
	//time.Sleep(15 * time.Second)
	time.Sleep(2 * time.Minute)

	fmt.Println("[Main] Initiating graceful shutdown...")

	// 通知 runtime / exporter 退出
	cancel()

	// 优雅关闭 HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("metrics server shutdown error: %v", err)
	}

	// 如果你需要确保 runtime 内部 Stop 做一些额外清理，可以保留这一句
	_ = rt.Stop(context.Background())

	fmt.Println("[Main] Shutdown complete.")
}
