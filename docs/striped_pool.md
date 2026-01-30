# StripedPool - 强有序工作池

## 概述

StripedPool 是一个基于分片（Stripe）的工作池实现，专门用于需要**强有序**保证的场景。

### 核心特性

1. **同 key 严格有序**：相同 key 的任务按提交顺序串行执行
2. **不同 key 并行**：不同 key 的任务分配到不同 stripe，实现并行处理
3. **故障隔离**：单个 stripe 的 panic 不影响其他 stripe
4. **自动恢复**：stripe 发生 panic 后自动恢复，继续处理后续任务
5. **背压控制**：每个 stripe 有独立的有界队列，防止内存爆炸

---

## 设计原理

### 分片架构

```
StripedPool (4 stripes)
┌────────────────────────────────────────┐
│                                        │
│  Stripe 0: Queue[100] + Goroutine #0  │  ← key: user1, order3, ...
│  Stripe 1: Queue[100] + Goroutine #1  │  ← key: user2, order1, ...
│  Stripe 2: Queue[100] + Goroutine #2  │  ← key: user3, order2, ...
│  Stripe 3: Queue[100] + Goroutine #3  │  ← key: user4, order4, ...
│                                        │
└────────────────────────────────────────┘

Key 分配规则: hash(key) % stripe_count
```

### 顺序性保证

**关键机制**：
1. 相同 key → 相同 stripe（通过哈希计算）
2. 每个 stripe 只有**单个 goroutine** 串行消费
3. 队列是 **FIFO**（先进先出）

**示例**：
```
订单 order-123 的事件序列：created → paid → shipped → delivered

1. hash("order-123") % 4 = 2 → 所有事件都去 Stripe 2
2. Stripe 2 的 goroutine 串行执行：
   - created  (时刻 T1)
   - paid     (时刻 T2, T2 > T1)
   - shipped  (时刻 T3, T3 > T2)
   - delivered(时刻 T4, T4 > T3)

✅ 严格保证顺序
```

---

## 使用方法

### 1. 创建和启动

```go
import "github.com/DrWhoRC/loadflow/pkg/pool"

// 创建 StripedPool
// - name: 池名称
// - stripeCount: stripe 数量（并发度）
// - queueSizePerStripe: 每个 stripe 的队列大小
pool := pool.NewStripedPool("my-pool", 4, 100)

// 启动 pool（必须在提交任务前调用）
ctx := context.Background()
pool.Start(ctx)

// 记得在退出时停止
defer pool.DrainAndStop(ctx)
```

### 2. 提交任务

#### 方式 1: 带 key 提交（强有序）

```go
// 订单处理示例
orderID := "order-123"
event := "paid"

err := pool.SubmitWithKey(ctx, []byte(orderID), func() {
    processOrderEvent(orderID, event)
})
```

#### 方式 2: 无 key 提交（轮询分配）

```go
// 普通任务，随机分配到某个 stripe
err := pool.Submit(ctx, func() {
    doSomeWork()
})
```

### 3. 监控指标

```go
metrics := pool.GetMetrics()

fmt.Printf("Total Submitted: %d\n", metrics.Submitted)
fmt.Printf("Total Processed: %d\n", metrics.Processed)
fmt.Printf("Total Panics: %d\n", metrics.Panics)

// 查看每个 stripe 的详细指标
for _, sm := range metrics.Stripes {
    fmt.Printf("Stripe-%d: Processed=%d, QueueSize=%d/%d, Panics=%d\n",
        sm.ID, sm.Processed, sm.QueueSize, sm.Capacity, sm.Panics)
}
```

---

## 适用场景

### ✅ 适合使用 StripedPool 的场景

1. **订单处理系统**
   - 同一订单的状态变更必须有序
   - 不同订单可以并行处理

2. **用户会话管理**
   - 同一用户的操作需要按序执行
   - 不同用户互不影响

3. **状态机处理**
   - 同一实体的状态转换有严格顺序
   - 例如：工作流引擎、游戏状态同步

4. **消息队列消费**
   - 从 Kafka partition 消费，需要保持消息顺序
   - 按 key 分片处理

### ❌ 不适合使用 StripedPool 的场景

1. **无状态处理**：任务之间没有依赖关系 → 用 FixedPool 性能更好
2. **全局有序**：所有任务必须串行 → 设置 stripeCount=1 或直接用单线程
3. **Key 数量极少**：只有 1-2 个 key → 无法充分利用并发

---

## 参数调优

### Stripe 数量（stripeCount）

**推荐值**：
- CPU 密集型：`runtime.NumCPU()` 或略小
- IO 密集型：`2 * runtime.NumCPU()`
- 默认：`4-8`

**影响**：
- 太小：并发度不足，性能受限
- 太大：上下文切换开销增加

### 队列大小（queueSizePerStripe）

**推荐值**：
- 低延迟场景：`10-50`（快速背压）
- 高吞吐场景：`100-500`（缓冲突发）
- 默认：`100`

**影响**：
- 太小：频繁阻塞，吞吐量下降
- 太大：内存占用高，背压不及时

---

## 故障处理

### Panic 恢复

StripedPool 会自动捕获任务执行中的 panic：

```go
pool.SubmitWithKey(ctx, []byte("key1"), func() {
    panic("oops!")  // 这个 panic 会被捕获
})

// Stripe 会：
// 1. 记录 panic 日志
// 2. 增加 panic 计数器
// 3. 继续处理下一个任务（不会崩溃）
```

**监控 panic**：
```go
metrics := pool.GetMetrics()
if metrics.Panics > 0 {
    log.Printf("Warning: %d panics occurred", metrics.Panics)
}
```

### 队列满处理

当队列满时，`SubmitWithKey` 会阻塞直到：
- 队列有空位
- 上下文超时/取消

```go
ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
defer cancel()

err := pool.SubmitWithKey(ctx, []byte("key"), task)
if err != nil {
    // 可能是超时或队列满
    log.Printf("Submit failed: %v", err)
}
```

---

## 与 FixedPool 的对比

| 特性 | FixedPool | StripedPool |
|------|-----------|-------------|
| **并发模型** | N 个 worker 竞争单个队列 | N 个 stripe，每个单线程 |
| **顺序性** | ❌ 无保证 | ✅ 同 key 严格有序 |
| **适用场景** | 无状态任务 | 有状态任务、顺序敏感 |
| **性能** | 高（无额外开销） | 略低（哈希计算 + 分片） |
| **内存** | 单个大队列 | N 个小队列（总容量相同） |
| **故障隔离** | ❌ 一个 worker panic 可能影响整体 | ✅ stripe 之间隔离 |

**选择建议**：
- 需要顺序性 → StripedPool
- 纯粹追求吞吐 → FixedPool

---

## 测试覆盖

StripedPool 包含全面的测试：

1. ✅ **顺序性测试**：10 个 key × 100 条消息，验证严格有序
2. ✅ **并行性测试**：验证不同 key 的任务并行执行
3. ✅ **Panic 恢复测试**：验证 stripe 在 panic 后继续工作
4. ✅ **背压测试**：验证队列满时的阻塞行为
5. ✅ **优雅停止测试**：验证 DrainAndStop 等待任务完成
6. ✅ **性能基准测试**：提供性能参考数据

运行测试：
```bash
go test -v ./pkg/pool -run TestStripedPool
```

---

## 最佳实践

### 1. Key 选择

**好的 key 设计**：
```go
// ✅ 订单 ID
pool.SubmitWithKey(ctx, []byte(orderID), task)

// ✅ 用户 ID
pool.SubmitWithKey(ctx, []byte(userID), task)

// ✅ 设备 ID
pool.SubmitWithKey(ctx, []byte(deviceID), task)
```

**避免的 key 设计**：
```go
// ❌ 时间戳（每条消息都不同，失去分片意义）
pool.SubmitWithKey(ctx, []byte(timestamp), task)

// ❌ 固定值（所有消息去同一个 stripe，退化为单线程）
pool.SubmitWithKey(ctx, []byte("fixed"), task)
```

### 2. 任务粒度

```go
// ✅ 轻量级任务（推荐）
pool.SubmitWithKey(ctx, key, func() {
    updateOrderStatus(orderID, "paid")
})

// ❌ 重量级任务（避免）
pool.SubmitWithKey(ctx, key, func() {
    // 大量 CPU 计算、长时间 IO 会阻塞整个 stripe
    processHugeFile()
})
```

### 3. 错误处理

```go
// ✅ 在任务内部处理错误
pool.SubmitWithKey(ctx, key, func() {
    if err := doWork(); err != nil {
        log.Printf("Task failed: %v", err)
        // 可以选择重试、记录到死信队列等
    }
})

// ❌ 不要在任务中 panic（虽然会被捕获，但影响监控）
pool.SubmitWithKey(ctx, key, func() {
    if err := doWork(); err != nil {
        panic(err)  // 不推荐
    }
})
```

---

## 完整示例

参见 `examples/striped_pool/main.go`

---

## 限制和注意事项

1. **Key 不可变性**：一旦 key 分配到某个 stripe，无法迁移
   - 意味着：不支持动态再平衡（和 scheduler 的再平衡策略冲突）
   - 解决方案：v0.6 版本考虑支持安全的 key 迁移

2. **内存占用**：`stripeCount × queueSizePerStripe` 个任务槽位
   - 示例：4 stripes × 100 queue = 最多缓冲 400 个任务

3. **哈希分布**：使用 FNV-1a 哈希，通常分布均匀
   - 但极端情况下可能某些 stripe 负载过高
   - 监控各 stripe 的指标，必要时调整 key 设计

---

## FAQ

**Q: 如何保证同一个 key 一定去同一个 stripe？**
A: 使用哈希算法 `hash(key) % stripeCount`，只要 stripeCount 不变，结果就固定。

**Q: Stripe 数量可以动态调整吗？**
A: v0.5.0 不支持。调整 stripeCount 会导致 key 重新分配，破坏顺序性。

**Q: 任务执行失败怎么办？**
A: 框架不提供重试机制。用户需要在任务内部实现错误处理和重试逻辑。

**Q: 和数据库事务的关系？**
A: StripedPool 只保证任务的**调用顺序**，不保证任务内部的事务性。数据库事务由用户在任务函数中实现。

**Q: 性能开销有多大？**
A: 相比 FixedPool，主要开销是哈希计算（每次提交约 10-50ns）。对于 IO 密集型任务，影响可忽略。

---

## 未来计划（v0.6+）

1. **安全的 Key 迁移**：支持 stripe 数量动态调整
2. **批量提交**：`SubmitBatch([]KeyTask)` 提升吞吐
3. **优先级队列**：支持任务优先级
4. **详细指标**：每个 stripe 的延迟分布、队列深度历史等
5. **与 Scheduler 集成**：智能调度 stripe 资源

---

**版本**: v0.5.0  
**作者**: LoadFlow Team  
**最后更新**: 2026-01-30
