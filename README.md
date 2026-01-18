**中文版在下方(Chinese Version Below)**
# LoadFlow

**Status:** Still developing  
**Fully-functioning in:** v0.6.0

---

## Introduction

This framework proposes a new paradigm for concurrent scheduling — a load-balanced consumption strategy based on **multi-coroutine pools** and **multi-data streams**.

Its core idea is to realize **self-balancing concurrency** through dynamic organization of coroutine pools and adaptive routing of data streams.

---
## THREE Strategies

#### 1. Pure Consumption
**Pure Consumption** is the simplest and most throughput-oriented strategy. It ignores message keys and ordering, and dispatches tasks to multiple pools according to routing weights, where each pool processes tasks concurrently with multiple goroutines. 

**The benefits** are minimal complexity, high throughput, and good resource utilization—ideal for logs, monitoring pipelines, ETL, and other workloads where ordering is irrelevant. 

**The downside** is that messages for the same logical entity may be processed concurrently across different workers, so ordering is not guaranteed and stateful workflows typically require idempotency or deduplication at the application layer.

#### 2. Sticky Per Pool
**Sticky Per Pool** adds “stickiness” on top of Pure Consumption: 
when a message carries a key, the framework hashes the key and consistently routes messages with the same key to the same pool (while still processing concurrently across multiple goroutines within that pool). 

**This betters** the observability for it enables the bussiness scenario and workerpool, reduces cross-pool reordering and contention, and enables localized caching, hotspot containment, and more stable load balancing. It’s suitable when you want best-effort locality and weaker ordering/consistency—e.g., session-oriented streams, per-user aggregation, near-real-time analytics. 

**The limitation** is that the same key can still be processed by different goroutines inside the pool, so strict ordering is not guaranteed; for strict ordering, you need Strategy 3.

#### 3. Strongly Ordered (strong data consistency)
Strongly Ordered targets **strong consistency**: messages with the same key are routed not only to the same pool but also to the same goroutine (or a dedicated shard queue) and executed serially, ensuring strict processing order per key. 

This is essential for workloads where **ordering defines correctness**, such as balance updates, order state machines, inventory deductions, and other stateful mutations with strict semantics. 

**The trade-off** is reduced parallelism—hot keys can become bottlenecks—and higher implementation complexity (queueing model, backpressure, and safe rebalancing without breaking ordering). It is typically offered as an advanced feature and enabled only when strict ordering is required.

---

## Default Parameters

| Name | Default Values | Description |
|--------|--------|------|
| epsRate | 1.0 | Epsilon value to prevent division by zero during pressure calculation | 
| minPressureDelta | 2.0 | Minimum pressure difference threshold required to trigger an adjustment | 
| baseStep | 1.0 | Base step size for adjustments | 
| maxStep | 5.0 | Maximum step size allowed for a single adjustment | 
| minWeight | 1.0 | Minimum weight limit for each pool | 
| maxFrac | 0.2 | Maximum fraction of total weight allowed for a single adjustment (20%) | 
| paceRate | 5.0 | Pressure difference interval required to trigger an additional unit of step size |

---

## 简介（中文版）

本框架提出了一种新的并发调度范式——基于**多协程池**和**多数据流**的负载均衡消费策略。

其核心思想是通过协程池的动态组织和数据流的自适应路由，实现**自平衡并发**。

---

## 三种策略

#### 1. 纯消费模式
**纯消费模式**是最简单且以吞吐量为优先的策略。它不关注消息键（Key）和顺序，而是根据路由权重将任务分发到多个资源池中；在每个池内，通过多个 Goroutine 并发处理任务。

**优势**在于复杂度极低、吞吐量高且资源利用率好——非常适合日志、监控链路、ETL 以及其他对顺序无要求的工作负载。

**不足之处**在于，属于同一逻辑实体的消息可能会被不同的 Worker 并发处理，因此无法保证顺序；有状态的工作流通常需要在应用层实现幂等性或去重机制。

#### 2. 池内粘性
**池内粘性**模式在纯消费模式的基础上增加了“粘性”机制： 当消息携带 Key 时，框架会对 Key 进行哈希计算，并将具有相同 Key 的消息一致地路由到同一个资源池（但在该池内部，仍然通过多个 Goroutine 并发处理）。

这提升了系统的**可观测性（建立了业务场景与 Worker 池的关联）**，减少了跨池的乱序和竞争，并支持本地缓存、热点隔离以及更稳定的负载均衡。它适用于追求尽力而为的本地性 (Best-effort locality) 以及弱顺序/一致性的场景——例如会话级数据流、基于用户的聚合计算或近实时分析。

**局限性**在于，同一个 Key 的消息仍可能由池内的不同 Goroutine 处理，因此无法保证严格顺序；若需要严格保序，请使用策略 3。

#### 3. 强有序 (强数据一致性)
**强有序**模式旨在实现强一致性：具有相同 Key 的消息不仅会被路由到同一个池，还会被分发到同一个 Goroutine（或专用的分片队列）中串行执行，从而确保每个 Key 的处理顺序严格一致。

这对于**顺序决定正确性**的场景至关重要，例如余额更新、订单状态机、库存扣减以及其他具有严格语义的有状态变更操作。

**代价**是并行度降低（热点 Key 可能成为瓶颈）以及实现复杂度较高（涉及队列模型、背压机制以及在不破坏顺序的前提下进行安全重平衡）。它通常作为一项高级功能提供，仅在必须保证严格顺序时开启。（但其背后的逻辑并不可以照抄: workerpool.size == 1）

---

## 默认参数

| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| epsRate | 1.0 | 计算压力时防止除零的极小值 |
| minPressureDelta | 2.0 | 触发调整所需的最小压力差阈值 |
| baseStep | 1.0 | 调整的基准步长 |
| maxStep | 5.0 | 单次调整允许的最大步长 |
| minWeight | 1.0 | 每个池的最小权重限制 |
| maxFrac | 0.2 | 单次调整占总权重的最大比例（20%） |
| paceRate | 5.0 | 触发额外步长单位所需的压力差间隔 |

