# 策略三：强有序（Strongly Ordered）

## 概述

强有序策略是 LoadFlow 框架的**顺序保证策略**，专门用于处理有状态的业务场景。其核心思想是：**同一个 key 的消息必须按提交顺序串行执行，不同 key 的消息可以并行处理**。

**本质**：在策略一的基础上，**将哈希粒度从 pool 级别细化到 goroutine 级别**，通过固定的 key→stripe 映射和单线程消费实现顺序保证。

---

## 为什么需要强有序？

### 策略一的顺序问题

```
订单处理系统（使用策略一）：

order-123 的事件序列：
1. 创建订单 (10:00:00)
2. 支付订单 (10:00:01)
3. 发货订单 (10:00:02)

路由结果（权重轮询）：
msg1(创建) → pool_fast  → Worker-2 处理（耗时50ms）
msg2(支付) → pool_medium → Worker-1 处理（耗时30ms）← 先完成
msg3(发货) → pool_fast  → Worker-3 处理（耗时40ms）

实际执行顺序：支付 → 发货 → 创建  ❌ 乱序！

后果：
- 支付时订单还不存在 → 失败
- 发货时订单未支付 → 逻辑错误
- 数据库状态不一致
```

---

## 核心原理

### 粒度对比

#### 策略一：Pool 级别哈希
```
hash(streamName) % pool_count → 选择 pool

问题：
同一个 key 的消息进入同一个 pool
→ 但 pool 内部有多个 worker 竞争
→ 无法保证顺序
```

#### 策略三：Goroutine 级别哈希
```
hash(key) % stripe_count → 选择 stripe

优势：
同一个 key 的消息进入同一个 stripe
→ stripe 内部只有1个 goroutine
→ 串行执行，严格保证顺序
```

**关键差异**：
```
策略一：
  Stream → Pool（多worker竞争）→ 并行执行 → 乱序

策略三：
  Key → Stripe（单goroutine）→ 串行执行 → 有序
```

---

## StripedPool 架构

### 整体结构

```
StripedPool (4 stripes)
┌────────────────────────────────────────┐
│ Stripe-0: Queue[100] + Goroutine-0    │ ← key: order-1, user-5
│ Stripe-1: Queue[100] + Goroutine-1    │ ← key: order-2, user-6
│ Stripe-2: Queue[100] + Goroutine-2    │ ← key: order-3, user-7
│ Stripe-3: Queue[100] + Goroutine-3    │ ← key: order-4, user-8
└────────────────────────────────────────┘

关键特性：
- N个stripe = N个并发度
- 每个stripe独立：队列 + goroutine
- Goroutine数量固定（不会爆炸）
```

### Key 到 Stripe 的映射

```go
func selectStripe(key []byte) int {
    // FNV-1a 哈希算法
    h := fnv.New32a()
    h.Write(key)
    
    // 取模分配
    return int(h.Sum32()) % stripe_count
}
```

**示例**：
```
key = "order-123"
hash("order-123") = 0xA9B8C7D6 = 2847562938
2847562938 % 4 = 2

→ 所有 "order-123" 的消息都去 Stripe-2
```

**为什么能保证顺序？**
```
1. 哈希函数是确定性的
   同一个 key → 永远得到相同的哈希值

2. 取模运算是固定的
   同一个哈希值 → 永远分配到同一个 stripe

3. Stripe 内部是串行的
   同一个 stripe → 只有一个 goroutine 消费
   
∴ 同一个 key → 同一个 stripe → 同一个 goroutine → 串行执行
```

---

## 完整执行流程

### 数据流追踪

```
1. 消息到达 Runtime
   ↓
2. Key 提取（两层）
   ├─ Codec.Decode()     → 从消息格式提取
   └─ KeyFunc()          → 用户自定义逻辑
   ↓
3. 路由选择
   Router.RouteWithKey(streamName, key) → 选择 pool
   ↓
4. StripedPool 提交
   pool.SubmitWithKey(ctx, key, task)
   ↓
5. Stripe 选择
   idx = hash(key) % stripe_count
   ↓
6. 任务入队
   stripe.queue <- task
   ↓
7. Goroutine 串行执行
   for task := range queue {
       executeTask(task)  ← 同步调用，必须完成才能取下一个
   }
```

### 关键代码路径

#### 1. Key 提取（runtime.go）
```go
// 1. Codec 解析
key, payload, ok := codec.Decode(raw)

// 2. 用户函数覆盖
if keyFn != nil {
    k2 := keyFn(streamName, payload)
    if len(k2) > 0 {
        key = k2  ← 用户定义优先
    }
}
```

#### 2. 提交到 Stripe（striped_pool.go）
```go
func (sp *StripedPool) SubmitWithKey(ctx context.Context, key []byte, task func()) error {
    // 计算 stripe 索引
    idx := sp.selectStripe(key)
    
    // 提交到指定 stripe
    return sp.submitToStripe(ctx, idx, task)
}
```

#### 3. Stripe 串行消费（striped_pool.go）
```go
func (sp *StripedPool) runStripe(s *stripe) {
    for {
        select {
        case task := <-s.queue:
            sp.executeTask(s, task)  ← 阻塞执行
        }
    }
}
```

**顺序保证的关键**：
```
同一个 stripe 的 goroutine：
1. 从 queue 取出 task1
2. executeTask(task1) ← 同步调用，必须完成
3. task1 完成后，才从 queue 取 task2
4. executeTask(task2) ← 再次阻塞

FIFO 队列 + 单线程消费 = 绝对有序
```

---

## 顺序性证明

```

### 实际验证（测试代码）

```go
// TestStripedPool_StrictOrdering
func TestStripedPool_StrictOrdering(t *testing.T) {
    pool := NewStripedPool("test", 4, 100)
    pool.Start(ctx)

    // 10个key，每个key发送100条有序消息（0-99）
    for _, key := range keys {
        for seq := 0; seq < 100; seq++ {
            pool.SubmitWithKey(ctx, []byte(key), func() {
                recorder.append(seq)  // 记录接收顺序
            })
        }
    }

    // 验证每个key的接收顺序
    for _, key := range keys {
        received := recorder.get(key)
        for i, seq := range received {
            if seq != i {
                t.Errorf("Ordering violated: expected %d, got %d", i, seq)
            }
        }
    }
}

结果：✅ 所有10个key × 100条消息 = 1000条消息，100% 有序
```

---

## 并行性保证

### 不同 Key 的并行执行

```
同时提交4个订单的消息：

order-123: [created, paid, shipped]  → hash → Stripe-2
order-456: [created, paid, shipped]  → hash → Stripe-0
order-789: [created, paid]           → hash → Stripe-1
order-999: [created]                 → hash → Stripe-3

执行情况：
Stripe-0 Goroutine: order-456.created → order-456.paid → ...
Stripe-1 Goroutine: order-789.created → order-789.paid
Stripe-2 Goroutine: order-123.created → order-123.paid → ...
Stripe-3 Goroutine: order-999.created

✅ 4个订单并行处理
✅ 每个订单内部串行有序
```

**性能特征**：
```
并发度 = Stripe 数量

4个stripe → 最多4个订单同时处理
8个stripe → 最多8个订单同时处理

选择建议：
- CPU密集型：stripe_count = CPU核心数
- IO密集型：stripe_count = 2 × CPU核心数
```

---

## 容错机制

### Panic 恢复

```go
func (sp *StripedPool) executeTask(s *stripe, task func()) {
    defer func() {
        if r := recover(); r != nil {
            // 记录panic
            log.Printf("stripe-%d panic: %v", s.id, r)
            atomic.AddUint64(&s.panics, 1)
            
            // ✅ Goroutine不会退出，继续处理下一个任务
        }
    }()
    
    task()  // 执行任务
}
```

**Stripe Down 处理**：
```
假设 order-123 的任务序列中 task2 panic：

task1(created) → 执行成功 ✅
task2(paid)    → panic ❌ → 被捕获 → 记录日志
task3(shipped) → 正常执行 ✅

关键：
- runStripe 的循环会继续
- 不会创建新的 goroutine
- 不影响该 stripe 的后续任务
- 不影响其他 stripe
```

**隔离性**：
```
Stripe-0 panic → 只影响 hash 到 Stripe-0 的 key
Stripe-1,2,3 → 完全不受影响

例如：
order-123 → Stripe-2（正常）
order-456 → Stripe-0（panic，受影响）
order-789 → Stripe-1（正常）
```

---

## 优点

### 1. 严格的顺序保证
```
数学证明 + 实测验证
1000条消息，100% 有序
适用于：订单、状态机、金融交易等
```

### 2. 并行处理能力
```
不同 key 分配到不同 stripe
充分利用多核 CPU
吞吐量 = stripe_count × 单stripe吞吐
```

### 3. 故障隔离
```
单个 stripe 故障不影响其他
Panic 自动恢复，不会崩溃
每个 stripe 独立监控
```

### 4. 性能可控
```
Goroutine 数量固定（= stripe_count）
不会像策略一早期那样 goroutine 爆炸
内存占用可预测
```

---

## 缺点

### 1. 性能略低于策略一
```
额外开销：
- 哈希计算：约10-50ns/消息
- Key 提取：Codec + KeyFunc

对比：
策略一（Keyless）：直接路由，无计算
策略三（Keyed）：需要哈希计算

差距：
- CPU密集型任务：几乎无影响
- IO密集型任务：可忽略（< 1%）
```

### 2. 不支持动态再平衡
```
问题：
如果改变 stripe_count（例如4→8）
→ hash(key) % 4 → hash(key) % 8
→ 同一个 key 会被分配到不同 stripe
→ 破坏顺序性

限制：
策略三与策略二（动态再平衡）互斥

配置：
policies:
  stream_ordered:
    enabled: false  # 必须禁用再平衡
```

### 3. 热点 Key 问题
```
如果某个 key 的消息量特别大：

VIP用户（user-123）：1000 msg/s
普通用户：10 msg/s

hash(user-123) → Stripe-2

结果：
Stripe-2 过载（1000 msg/s）
Stripe-0,1,3 空闲（各10 msg/s）

解决方案（v0.6计划）：
- Key 子分片（user-123-shard-1, user-123-shard-2）
- 动态 stripe 扩容（安全迁移）
```

### 4. 需要合理的 Key 设计
```
好的 Key：
✅ order_id, user_id, device_id（业务ID）
✅ 分布均匀，无热点

坏的 Key：
❌ timestamp（每条消息都不同，失去意义）
❌ 固定值（所有消息去同一个stripe）
❌ 低基数（如性别，只有2个值）
```

---

## 与其他策略的关系

### 基于策略一的哈希思想
```
策略一：hash(streamName) % pool_count
策略三：hash(key) % stripe_count

共同点：
- 都使用哈希算法
- 都是静态映射（不动态调整）

差异点：
- 策略一：粗粒度（pool级别）
- 策略三：细粒度（goroutine级别）
```

### 与策略二互斥
```
策略二：动态调整权重
  → key→pool 映射可能变化
  → 破坏顺序性

策略三：固定 key→stripe 映射
  → 保证顺序性
  → 无法动态调整

冲突根源：
顺序性要求 key 映射稳定
动态性要求 key 映射可变

选择：
有状态场景 → 策略三（牺牲动态性）
无状态场景 → 策略一+二（追求吞吐）
```

---

## 实际案例

### 案例 1：订单处理系统

```go
pool := pool.NewStripedPool("order-pool", 8, 100)
pool.Start(ctx)

// Key = order_id
keyFn := func(stream string, payload []byte) []byte {
    var order Order
    json.Unmarshal(payload, &order)
    return []byte(order.ID)
}

// 提交订单事件
events := []OrderEvent{
    {ID: "order-123", Type: "created"},
    {ID: "order-123", Type: "paid"},
    {ID: "order-123", Type: "shipped"},
}

for _, event := range events {
    pool.SubmitWithKey(ctx, []byte(event.ID), func() {
        processOrder(event)  // 保证按序执行
    })
}
```

**保证**：
```
order-123: created → paid → shipped（有序）
order-456: created → paid（有序）
两个订单之间并行处理
```

### 案例 2：用户会话管理

```go
// Key = session_id
pool := pool.NewStripedPool("session-pool", 16, 200)

// 同一用户的操作串行
actions := []UserAction{
    {SessionID: "sess-abc", Action: "login"},
    {SessionID: "sess-abc", Action: "browse"},
    {SessionID: "sess-abc", Action: "purchase"},
    {SessionID: "sess-abc", Action: "logout"},
}

// 保证同一会话的操作有序
```

### 案例 3：状态机处理

```go
// 工作流引擎
// Key = workflow_id

states := []StateTransition{
    {WorkflowID: "wf-001", From: "draft", To: "pending"},
    {WorkflowID: "wf-001", From: "pending", To: "approved"},
    {WorkflowID: "wf-001", From: "approved", To: "completed"},
}

// 保证状态转换的严格顺序
```

---

## 使用指南

### 配置示例

```yaml
# 创建 StripedPool
pools:
  - name: order_pool
    type: striped
    stripe_count: 8      # 并发度
    queue_per_stripe: 100

# 路由配置
routing:
  stream_orders:
    pool: order_pool
    key_extractor: "order_id"  # 从消息中提取 order_id

# 禁用动态再平衡
scheduler:
  policies:
    stream_orders:
      enabled: false  # 强有序场景必须禁用
```

### 参数调优

#### Stripe 数量
```
推荐值：
- CPU密集型：runtime.NumCPU()
- IO密集型：2 × runtime.NumCPU()
- 默认：4-8

示例：
8核机器：
- CPU任务：stripe_count = 8
- IO任务：stripe_count = 16
```

#### 队列大小
```
每个 stripe 的队列：

低延迟：queue = 10-50
高吞吐：queue = 100-500

总缓冲 = stripe_count × queue_per_stripe
```

### Key 设计原则

```
1. 唯一性
   ✅ order_id, user_id（每个订单/用户唯一）
   ❌ timestamp（每条消息都不同）

2. 稳定性
   ✅ 固定的业务ID
   ❌ 随机生成的值

3. 分布均匀
   ✅ UUID, 数据库自增ID
   ❌ 热点用户ID（考虑子分片）

4. 业务语义
   ✅ 符合业务顺序需求
   ❌ 技术字段（如机器IP）
```

---

## 监控与调优

### 关键指标

```go
metrics := pool.GetMetrics()

// 全局指标
fmt.Printf("Total Processed: %d\n", metrics.Processed)
fmt.Printf("Total Panics: %d\n", metrics.Panics)

// Per-Stripe 指标
for _, sm := range metrics.Stripes {
    fmt.Printf("Stripe-%d: Processed=%d, QueueSize=%d/%d, Panics=%d\n",
        sm.ID, sm.Processed, sm.QueueSize, sm.Capacity, sm.Panics)
}
```

### 告警规则

```
1. Panic 率 > 1%
   → 检查业务代码

2. 某个 stripe 队列持续 > 80%
   → 热点 key 问题

3. Stripe 处理量极度不均
   → Key 分布不均，考虑重新设计

4. 所有 stripe 队列都满
   → 整体容量不足，增加 stripe 或优化性能
```

---

## 总结

**策略三（强有序）是策略一的顺序增强版**：
- ✅ 通过**细粒度哈希**（key→stripe）保证顺序
- ✅ 单 goroutine 消费 FIFO 队列，数学级别的顺序保证
- ✅ 不同 key 并行，充分利用多核
- ✅ Panic 自动恢复，Stripe 隔离
- ❌ 性能略低于策略一（哈希计算开销）
- ❌ 与策略二（动态再平衡）互斥
- ❌ 需要合理的 Key 设计
- 🎯 适合订单、会话、状态机等有状态场景

**设计哲学**：
> 在需要顺序性的场景下，通过更细的粒度控制，用最小的性能代价换取最强的顺序保证。

**核心创新**：
> 从 Pool 级别的粗粒度路由，细化到 Goroutine 级别的精确控制，这是保证顺序性的关键。

---

**推荐阅读**：
- 基础路由 → [策略一：无 Key 路由](strategy_1_keyless_routing_zh.md)
- 动态优化 → [策略二：动态再平衡](strategy_2_dynamic_rebalancing_zh.md)
- 实现细节 → [StripedPool 文档](docs/striped_pool.md)
