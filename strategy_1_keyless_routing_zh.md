# 策略一：无 Key 路由（Keyless Routing）

## 概述

无 Key 路由是 LoadFlow 框架的**基础策略**，适用于消息之间没有顺序依赖、可以完全并行处理的场景。该策略的核心思想是：**将消息流按照预定义的权重分配到多个工作池，实现负载分散和资源利用**。

**适用场景**：无状态任务处理、日志收集、统计分析、通知发送等。

---

## 核心原理

### 路由决策

```
消息到达 → Router.Route(streamName) → 根据权重选择 Pool → 提交执行
```

**关键点**：
- 不考虑消息内容（payload）
- 不考虑消息 key
- 只根据 stream 名称和预设权重分配

### 权重路由算法（Weighted Round-Robin）

```go
// 加权轮询实现
weights := [5, 3, 2]  // pool_fast, pool_medium, pool_slow
总权重 = 10

第1条消息 → 权重区间 [0, 5)   → pool_fast
第2条消息 → 权重区间 [5, 8)   → pool_medium
第3条消息 → 权重区间 [8, 10)  → pool_slow
第4条消息 → 权重区间 [0, 5)   → pool_fast（循环）
...
```

**分配结果**：
- pool_fast 获得 50% 流量（5/10）
- pool_medium 获得 30% 流量（3/10）
- pool_slow 获得 20% 流量（2/10）

---

## 实现细节

### 提交机制的演进

#### 早期实现（已废弃）

```go
// ❌ 每条消息创建一个 goroutine
go func(pool pool.WorkerPool, task func()) {
    pool.Submit(ctx, task)
}(p, task)
```

**问题**：
1. **Goroutine 爆炸**：高并发下可能创建数千个 goroutine
2. **背压失效**：即使 pool 队列满了，新消息仍会创建 goroutine，导致内存膨胀
3. **调度开销**：大量 goroutine 的上下文切换消耗 CPU

**场景重现**：
```
假设 pool_slow 队列满（256/256）：

时刻 T0: 100条新消息到达
      → 创建100个goroutine
      → 全部阻塞在 pool_slow.Submit()

时刻 T1: 又100条新消息
      → 再创建100个goroutine
      → 继续阻塞

时刻 T5: 已有500个goroutine堆积
      → 内存占用: 500 × 2KB = 1MB（栈空间）
      → 调度器压力剧增
```

#### 当前实现（Dispatcher 机制）

```go
// ✅ 使用 per-route dispatcher
disp := getOrCreateDispatcher(streamName, pool)
disp.TrySubmit(task)
```

**Dispatcher 架构**：
```
每个 (stream, pool) 组合 = 1个 dispatcher
  ├─ 有界缓冲队列（例如100）
  └─ 单个 goroutine 消费

示例：
stream_a → pool_fast  : Dispatcher-1 (1个goroutine)
stream_a → pool_medium: Dispatcher-2 (1个goroutine)
stream_a → pool_slow  : Dispatcher-3 (1个goroutine)

总计：3个goroutine（而非数千个）
```

**优势**：
1. **Goroutine 数量可控**：固定为 `stream数 × pool数`
2. **背压传导**：dispatcher 队列满时可选择丢弃或阻塞
3. **性能提升**：减少上下文切换，实测比原方案更快

**性能对比**：
```
场景：1000条消息/秒，3个pool

旧方案：
- Goroutine峰值: ~3000
- 内存占用: ~6MB
- CPU上下文切换: 高

新方案（Dispatcher）：
- Goroutine数量: 3（固定）
- 内存占用: ~300KB
- CPU上下文切换: 极低
- 吞吐量: 相同或更高（无调度开销）
```

---

## 优点

### 1. 简单高效
- 无需解析消息内容
- 无需计算哈希
- 路由决策时间复杂度：O(1)

### 2. 灵活的资源分配
```yaml
# 可以动态调整权重
routing:
  stream_logs:
    pools: [pool_fast, pool_slow]
    weights: [9, 1]  # 90% 给快池，10% 给慢池
```

### 3. 负载均衡
- 通过权重控制流量分布
- 避免单个 pool 过载
- 充分利用所有资源

### 4. 可扩展性强
- 可以轻松添加/移除 pool
- 支持动态调整权重（配合策略二）
- 水平扩展友好

---

## 缺点

### 1. 无法保证顺序
```
同一实体的多条消息可能被分配到不同 pool：

订单 order-123:
  msg1(created)  → pool_fast  ✅
  msg2(paid)     → pool_medium ⚠️ 可能先执行
  msg3(shipped)  → pool_fast   ⚠️ 顺序不确定

结果：paid → created → shipped（乱序）
```

**影响**：不适合有状态的业务场景

### 2. 热点问题无法解决
- 如果某类消息占比极高，无法根据内容动态调整
- 例如：VIP 用户消息占90%，但权重是均匀的

### 3. 队列饱和风险
```
观测数据（develope_thoughts&drafts.txt）：
fast: q=1013/2048  (50%)  ← 健康
slow: q=256/256    (100%) ← 饱和！

现象：slow pool 连续3个周期满载
原因：权重分配不合理 或 处理能力差异大
```

**后果**：
- 消息在生产端积压
- 无法入队，延迟增加
- 触发背压机制（如果配置得当）

---

## 使用指南

### 配置示例

```yaml
routing:
  stream_notifications:
    pools: 
      - pool_fast
      - pool_medium
      - pool_slow
    initial_weights: [5, 3, 2]
```

### 最佳实践

#### 1. 权重设置原则
```
权重应该反映 pool 的处理能力：

pool_fast:  5 workers × 10 msg/s = 50 msg/s  → 权重 5
pool_medium: 3 workers × 10 msg/s = 30 msg/s  → 权重 3
pool_slow:  2 workers × 10 msg/s = 20 msg/s  → 权重 2

总容量: 100 msg/s
```

#### 2. 队列大小设置
```
根据延迟敏感度调整：

低延迟场景（实时通知）：
  queue: 10-50   ← 快速背压

高吞吐场景（日志处理）：
  queue: 500-2048 ← 缓冲突发
```

#### 3. 监控指标
```go
// 定期检查队列深度
metrics := pool.GetMetrics()
if metrics.QueueDepth > metrics.QueueCapacity * 0.8 {
    log.Warn("Pool approaching saturation")
}
```

---

## 与其他策略的关系

### 策略二：动态再平衡

无 Key 路由是静态配置的，但可以配合**策略二（动态再平衡）**实现自适应：

```
初始权重: [5, 3, 2]
    ↓
监控发现 pool_slow 过载
    ↓
策略二调整: [7, 2, 1]
    ↓
流量重新分配，pool_slow 压力降低
```

**配合使用**：
- 策略一负责基础路由
- 策略二负责动态优化

### 策略三：强有序

策略一和策略三是**互斥的**：

```
策略一（Keyless）: 不关心消息内容，随机分配
策略三（Keyed）:   根据 key 固定分配，保证顺序

选择依据：
- 无状态任务 → 策略一
- 有状态任务 → 策略三
```

---

## 实际案例

### 案例 1：日志收集系统

```yaml
场景：收集来自多个服务的日志

routing:
  stream_logs:
    pools: [pool_fast, pool_slow]
    weights: [8, 2]

分析：
- 日志之间无依赖关系 ✅ 适合 Keyless
- 80% 日志写入快速存储
- 20% 日志写入归档存储
- 吞吐量优先，顺序不重要
```

### 案例 2：通知推送

```yaml
routing:
  stream_notifications:
    pools: [pool_sms, pool_email, pool_push]
    weights: [2, 3, 5]

配合策略二动态调整：
- 白天：push 权重增加（用户在线）
- 夜晚：email 权重增加（push 无效）
```

---

## 性能优化建议

### 1. Dispatcher 缓冲大小
```go
// 根据流量特征调整
bufferSize := 100  // 默认

如果流量突发严重：bufferSize = 500
如果内存敏感：bufferSize = 50
```

### 2. Pool 数量权衡
```
Pool 太多：
- 管理复杂
- Dispatcher goroutine 增加

Pool 太少：
- 灵活性降低
- 单点过载风险

建议：2-5 个 pool
```

### 3. 监控驱动调优
```
定期检查：
1. 各 pool 的队列深度分布
2. 处理延迟分位数
3. 丢弃消息数量

根据数据调整权重
```

---

## 总结

**策略一（Keyless Routing）是 LoadFlow 的基础策略**：
- ✅ 简单、高效、易于理解
- ✅ 通过 Dispatcher 优化，避免 goroutine 爆炸
- ✅ 适合无状态、高吞吐场景
- ❌ 不保证顺序，不适合有状态任务
- 🔄 可与策略二配合实现动态优化
- ⚡ 是策略三的性能基准（策略三牺牲一些性能换取顺序性）

**设计哲学**：
> 在不需要顺序性的场景下，用最简单的方式实现最高的吞吐量。

---

**下一步**：
- 如需动态调整权重 → 参见 [策略二：动态再平衡](strategy_2_dynamic_rebalancing_zh.md)
- 如需保证顺序性 → 参见 [策略三：强有序](strategy_3_strongly_ordered_zh.md)
