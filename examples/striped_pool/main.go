// Package main 演示如何使用 StripedPool 实现强有序处理
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/DrWhoRC/loadflow/pkg/pool"
)

func main() {
	// 创建一个 StripedPool
	// - 4 个 stripe（并发度 = 4）
	// - 每个 stripe 的队列大小 = 100
	stripedPool := pool.NewStripedPool("order-processor", 4, 100)

	// 启动 pool
	ctx := context.Background()
	stripedPool.Start(ctx)

	// 记得在退出时停止（带超时）
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stripedPool.DrainAndStop(stopCtx)
	}()

	// 模拟订单处理场景
	// 同一个 order_id 的事件必须按顺序处理
	orders := map[string][]string{
		"order-123": {"created", "paid", "shipped", "delivered"},
		"order-456": {"created", "paid", "cancelled"},
		"order-789": {"created", "paid", "shipped", "delivered"},
	}

	// 提交任务
	for orderID, events := range orders {
		for _, event := range events {
			// 捕获变量
			oid := orderID
			evt := event

			// 使用 orderID 作为 key，保证同一订单的事件按序处理
			err := stripedPool.SubmitWithKey(ctx, []byte(oid), func() {
				processOrderEvent(oid, evt)
			})

			if err != nil {
				log.Printf("Failed to submit: %v", err)
			}
		}
	}

	// 等待一段时间让任务完成
	time.Sleep(2 * time.Second)

	// 查看指标
	metrics := stripedPool.GetMetrics()
	fmt.Printf("\n=== Pool Metrics ===\n")
	fmt.Printf("Total Submitted: %d\n", metrics.Submitted)
	fmt.Printf("Total Processed: %d\n", metrics.Processed)
	fmt.Printf("Total Panics: %d\n", metrics.Panics)
	fmt.Printf("\nPer-Stripe Metrics:\n")
	for _, sm := range metrics.Stripes {
		fmt.Printf("  Stripe-%d: Submitted=%d, Processed=%d, Panics=%d, QueueSize=%d/%d\n",
			sm.ID, sm.Submitted, sm.Processed, sm.Panics, sm.QueueSize, sm.Capacity)
	}
}

func processOrderEvent(orderID, event string) {
	// 模拟处理延迟
	time.Sleep(100 * time.Millisecond)
	log.Printf("[%s] Processing event: %s", orderID, event)
}
