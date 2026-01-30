package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStripedPool_BasicFunctionality 测试基本功能
func TestStripedPool_BasicFunctionality(t *testing.T) {
	pool := NewStripedPool("test-pool", 4, 10)
	ctx := context.Background()
	pool.Start(ctx)
	defer pool.DrainAndStop(context.WithValue(ctx, "timeout", 5*time.Second))

	var counter int32
	var wg sync.WaitGroup

	// 提交 100 个任务
	for i := 0; i < 100; i++ {
		wg.Add(1)
		err := pool.Submit(ctx, func() {
			atomic.AddInt32(&counter, 1)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
	}

	// 等待所有任务完成
	wg.Wait()

	if counter != 100 {
		t.Errorf("Expected counter=100, got %d", counter)
	}

	metrics := pool.GetMetrics()
	if metrics.Submitted != 100 {
		t.Errorf("Expected submitted=100, got %d", metrics.Submitted)
	}
	if metrics.Processed != 100 {
		t.Errorf("Expected processed=100, got %d", metrics.Processed)
	}
}

// TestStripedPool_StrictOrdering 测试严格顺序性
// 关键测试：同一个 key 的任务必须按提交顺序执行
func TestStripedPool_StrictOrdering(t *testing.T) {
	pool := NewStripedPool("ordering-test", 4, 100)
	ctx := context.Background()
	pool.Start(ctx)
	defer pool.DrainAndStop(context.WithValue(ctx, "timeout", 10*time.Second))

	// 测试 10 个不同的 key，每个 key 发送 100 条消息
	keys := []string{"user1", "user2", "user3", "user4", "user5",
		"order1", "order2", "order3", "order4", "order5"}

	// 每个 key 的顺序记录器
	type seqRecorder struct {
		mu       sync.Mutex
		received []int
	}
	recorders := make(map[string]*seqRecorder)
	for _, key := range keys {
		recorders[key] = &seqRecorder{received: make([]int, 0, 100)}
	}

	var wg sync.WaitGroup

	// 为每个 key 提交 100 个有序任务
	for _, key := range keys {
		for seq := 0; seq < 100; seq++ {
			wg.Add(1)

			k := key // 捕获变量
			s := seq
			err := pool.SubmitWithKey(ctx, []byte(k), func() {
				defer wg.Done()

				// 记录接收顺序
				rec := recorders[k]
				rec.mu.Lock()
				rec.received = append(rec.received, s)
				rec.mu.Unlock()

				// 模拟一些处理时间
				time.Sleep(time.Microsecond)
			})

			if err != nil {
				t.Fatalf("SubmitWithKey failed for key=%s seq=%d: %v", k, s, err)
			}
		}
	}

	// 等待所有任务完成
	wg.Wait()

	// 验证每个 key 的顺序
	for _, key := range keys {
		rec := recorders[key]
		rec.mu.Lock()
		received := rec.received
		rec.mu.Unlock()

		if len(received) != 100 {
			t.Errorf("Key %s: expected 100 messages, got %d", key, len(received))
			continue
		}

		// 验证严格递增顺序
		for i, seq := range received {
			if seq != i {
				t.Errorf("Key %s: ordering violated at position %d: expected %d, got %d",
					key, i, i, seq)
				t.Errorf("Key %s received sequence: %v", key, received)
				break
			}
		}
	}

	t.Logf("✅ All %d keys maintained strict ordering across 100 messages each", len(keys))
}

// TestStripedPool_ParallelExecution 测试不同 key 的并行执行
func TestStripedPool_ParallelExecution(t *testing.T) {
	pool := NewStripedPool("parallel-test", 4, 10)
	ctx := context.Background()
	pool.Start(ctx)
	defer pool.DrainAndStop(context.WithValue(ctx, "timeout", 10*time.Second))

	keys := []string{"key1", "key2", "key3", "key4"}
	startTimes := make(map[string]time.Time)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 每个 key 提交一个耗时任务
	for _, key := range keys {
		wg.Add(1)
		k := key
		err := pool.SubmitWithKey(ctx, []byte(k), func() {
			defer wg.Done()
			mu.Lock()
			startTimes[k] = time.Now()
			mu.Unlock()

			// 模拟耗时操作
			time.Sleep(100 * time.Millisecond)
		})
		if err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
	}

	wg.Wait()

	// 验证至少有部分任务是并行执行的
	// 如果完全串行，总时间应该接近 400ms
	// 如果并行，应该接近 100ms
	mu.Lock()
	var minTime, maxTime time.Time
	for _, st := range startTimes {
		if minTime.IsZero() || st.Before(minTime) {
			minTime = st
		}
		if maxTime.IsZero() || st.After(maxTime) {
			maxTime = st
		}
	}
	mu.Unlock()

	spread := maxTime.Sub(minTime)
	if spread > 200*time.Millisecond {
		t.Errorf("Tasks appear to be serial (spread=%v), expected parallel execution", spread)
	}

	t.Logf("✅ Tasks executed in parallel (spread=%v)", spread)
}

// TestStripedPool_PanicRecovery 测试 panic 恢复和 stripe 继续工作
func TestStripedPool_PanicRecovery(t *testing.T) {
	pool := NewStripedPool("panic-test", 2, 10)
	ctx := context.Background()
	pool.Start(ctx)
	defer pool.DrainAndStop(context.WithValue(ctx, "timeout", 5*time.Second))

	var successCount int32
	var wg sync.WaitGroup

	key := []byte("panic-key")

	// 提交 10 个任务，其中第 5 个会 panic
	for i := 0; i < 10; i++ {
		wg.Add(1)
		idx := i
		err := pool.SubmitWithKey(ctx, key, func() {
			defer wg.Done()

			if idx == 4 {
				// 第 5 个任务触发 panic
				panic(fmt.Sprintf("intentional panic at task %d", idx))
			}

			// 正常任务
			atomic.AddInt32(&successCount, 1)
			time.Sleep(10 * time.Millisecond)
		})
		if err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
	}

	wg.Wait()

	// 验证 9 个正常任务都完成了
	if successCount != 9 {
		t.Errorf("Expected 9 successful tasks, got %d", successCount)
	}

	// 验证 panic 计数
	metrics := pool.GetMetrics()
	if metrics.Panics != 1 {
		t.Errorf("Expected 1 panic, got %d", metrics.Panics)
	}

	t.Logf("✅ Stripe recovered from panic and continued processing")
}

// TestStripedPool_QueueFull 测试队列满时的行为
func TestStripedPool_QueueFull(t *testing.T) {
	// 创建一个小队列的池
	pool := NewStripedPool("full-test", 1, 2)
	ctx := context.Background()
	pool.Start(ctx)
	defer pool.DrainAndStop(context.WithValue(ctx, "timeout", 5*time.Second))

	key := []byte("test-key")
	var blockChan = make(chan struct{})
	var submittedCount int32

	// 提交 3 个阻塞任务（队列容量是 2）
	for i := 0; i < 3; i++ {
		err := pool.SubmitWithKey(ctx, key, func() {
			atomic.AddInt32(&submittedCount, 1)
			<-blockChan // 阻塞直到收到信号
		})
		if err != nil {
			t.Logf("Task %d blocked as expected: %v", i, err)
		}
	}

	// 尝试再提交一个，应该超时
	ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	err := pool.SubmitWithKey(ctxTimeout, key, func() {})
	if err == nil {
		t.Error("Expected submit to timeout, but it succeeded")
	}

	// 释放阻塞
	close(blockChan)

	// 等待一下让任务完成
	time.Sleep(200 * time.Millisecond)

	t.Logf("✅ Queue backpressure works correctly")
}

// TestStripedPool_StopWithPendingTasks 测试带待处理任务的优雅停止
func TestStripedPool_StopWithPendingTasks(t *testing.T) {
	pool := NewStripedPool("stop-test", 2, 100)
	ctx := context.Background()
	pool.Start(ctx)

	var processedCount int32
	key := []byte("test-key")

	// 提交 50 个任务
	for i := 0; i < 50; i++ {
		_ = pool.SubmitWithKey(ctx, key, func() {
			atomic.AddInt32(&processedCount, 1)
			time.Sleep(5 * time.Millisecond)
		})
	}

	// 立即停止（带超时）
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := pool.DrainAndStop(stopCtx)
	if err != nil {
		t.Logf("Drain with timeout: %v", err)
	}

	// 验证所有任务都被处理了
	if processedCount != 50 {
		t.Errorf("Expected all 50 tasks to complete, got %d", processedCount)
	}

	t.Logf("✅ Graceful stop processed all %d pending tasks", processedCount)
}

// BenchmarkStripedPool_SubmitWithKey 性能基准测试
func BenchmarkStripedPool_SubmitWithKey(b *testing.B) {
	pool := NewStripedPool("bench-pool", 8, 1000)
	ctx := context.Background()
	pool.Start(ctx)
	defer pool.DrainAndStop(context.Background())

	keys := [][]byte{
		[]byte("key1"), []byte("key2"), []byte("key3"), []byte("key4"),
		[]byte("key5"), []byte("key6"), []byte("key7"), []byte("key8"),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%len(keys)]
			_ = pool.SubmitWithKey(ctx, key, func() {
				// 模拟轻量级任务
			})
			i++
		}
	})
}
