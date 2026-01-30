# Strategy 1: Keyless Routing

## Overview

Keyless Routing is the **foundational strategy** of the LoadFlow framework, designed for scenarios where messages have no ordering dependencies and can be processed completely in parallel. The core concept is: **distribute message streams to multiple worker pools according to predefined weights, achieving load distribution and resource utilization**.

**Use Cases**: Stateless task processing, log collection, statistical analysis, notification delivery, etc.

---

## Core Principles

### Routing Decision

```
Message Arrives → Router.Route(streamName) → Select Pool by Weight → Submit for Execution
```

**Key Points**:
- Does not consider message content (payload)
- Does not consider message key
- Only distributes based on stream name and preset weights

### Weighted Round-Robin Algorithm

```go
// Weighted round-robin implementation
weights := [5, 3, 2]  // pool_fast, pool_medium, pool_slow
Total weight = 10

Message 1 → Weight range [0, 5)   → pool_fast
Message 2 → Weight range [5, 8)   → pool_medium
Message 3 → Weight range [8, 10)  → pool_slow
Message 4 → Weight range [0, 5)   → pool_fast (cycle)
...
```

**Distribution Result**:
- pool_fast receives 50% traffic (5/10)
- pool_medium receives 30% traffic (3/10)
- pool_slow receives 20% traffic (2/10)

---

## Implementation Details

### Evolution of Submission Mechanism

#### Early Implementation (Deprecated)

```go
// ❌ Create a goroutine for each message
go func(pool pool.WorkerPool, task func()) {
    pool.Submit(ctx, task)
}(p, task)
```

**Problems**:
1. **Goroutine Explosion**: May create thousands of goroutines under high concurrency
2. **Backpressure Failure**: Even when pool queue is full, new messages still create goroutines, causing memory bloat
3. **Scheduling Overhead**: Context switching of massive goroutines consumes CPU

**Scenario Reproduction**:
```
Assume pool_slow queue is full (256/256):

Time T0: 100 new messages arrive
      → Create 100 goroutines
      → All blocked on pool_slow.Submit()

Time T1: Another 100 messages
      → Create 100 more goroutines
      → Continue blocking

Time T5: 500 goroutines accumulated
      → Memory usage: 500 × 2KB = 1MB (stack space)
      → Scheduler pressure increases dramatically
```

#### Current Implementation (Dispatcher Mechanism)

```go
// ✅ Use per-route dispatcher
disp := getOrCreateDispatcher(streamName, pool)
disp.TrySubmit(task)
```

**Dispatcher Architecture**:
```
Each (stream, pool) combination = 1 dispatcher
  ├─ Bounded buffer queue (e.g., 100)
  └─ Single goroutine consumer

Example:
stream_a → pool_fast  : Dispatcher-1 (1 goroutine)
stream_a → pool_medium: Dispatcher-2 (1 goroutine)
stream_a → pool_slow  : Dispatcher-3 (1 goroutine)

Total: 3 goroutines (not thousands)
```

**Advantages**:
1. **Controlled Goroutine Count**: Fixed at `stream_count × pool_count`
2. **Backpressure Propagation**: Can choose to drop or block when dispatcher queue is full
3. **Performance Improvement**: Reduced context switching, actual tests show faster than old approach

**Performance Comparison**:
```
Scenario: 1000 msg/s, 3 pools

Old Approach:
- Peak Goroutines: ~3000
- Memory Usage: ~6MB
- CPU Context Switching: High

New Approach (Dispatcher):
- Goroutine Count: 3 (fixed)
- Memory Usage: ~300KB
- CPU Context Switching: Minimal
- Throughput: Same or higher (no scheduling overhead)
```

---

## Advantages

### 1. Simple and Efficient
- No need to parse message content
- No need to compute hash
- Routing decision time complexity: O(1)

### 2. Flexible Resource Allocation
```yaml
# Can dynamically adjust weights
routing:
  stream_logs:
    pools: [pool_fast, pool_slow]
    weights: [9, 1]  # 90% to fast pool, 10% to slow pool
```

### 3. Load Balancing
- Control traffic distribution through weights
- Avoid single pool overload
- Fully utilize all resources

### 4. Strong Scalability
- Can easily add/remove pools
- Supports dynamic weight adjustment (with Strategy 2)
- Horizontal scaling friendly

---

## Disadvantages

### 1. No Ordering Guarantee
```
Multiple messages for the same entity may be assigned to different pools:

Order order-123:
  msg1(created)  → pool_fast  ✅
  msg2(paid)     → pool_medium ⚠️ May execute first
  msg3(shipped)  → pool_fast   ⚠️ Order uncertain

Result: paid → created → shipped (out of order)
```

**Impact**: Not suitable for stateful business scenarios

### 2. Cannot Solve Hotspot Issues
- If certain message types dominate, cannot dynamically adjust based on content
- Example: VIP user messages are 90%, but weights are uniform

### 3. Queue Saturation Risk
```
Observed data (develope_thoughts&drafts.txt):
fast: q=1013/2048  (50%)  ← Healthy
slow: q=256/256    (100%) ← Saturated!

Phenomenon: slow pool fully loaded for 3 consecutive cycles
Cause: Unreasonable weight allocation OR significant processing capability difference
```

**Consequences**:
- Messages backlog at producer side
- Cannot enqueue, latency increases
- Trigger backpressure mechanism (if configured properly)

---

## Usage Guide

### Configuration Example

```yaml
routing:
  stream_notifications:
    pools: 
      - pool_fast
      - pool_medium
      - pool_slow
    initial_weights: [5, 3, 2]
```

### Best Practices

#### 1. Weight Setting Principles
```
Weights should reflect pool processing capacity:

pool_fast:  5 workers × 10 msg/s = 50 msg/s  → weight 5
pool_medium: 3 workers × 10 msg/s = 30 msg/s  → weight 3
pool_slow:  2 workers × 10 msg/s = 20 msg/s  → weight 2

Total capacity: 100 msg/s
```

#### 2. Queue Size Settings
```
Adjust based on latency sensitivity:

Low latency scenarios (real-time notifications):
  queue: 10-50   ← Quick backpressure

High throughput scenarios (log processing):
  queue: 500-2048 ← Buffer bursts
```

#### 3. Monitoring Metrics
```go
// Periodically check queue depth
metrics := pool.GetMetrics()
if metrics.QueueDepth > metrics.QueueCapacity * 0.8 {
    log.Warn("Pool approaching saturation")
}
```

---

## Relationship with Other Strategies

### Strategy 2: Dynamic Rebalancing

Keyless routing is statically configured but can work with **Strategy 2 (Dynamic Rebalancing)** for adaptive behavior:

```
Initial weights: [5, 3, 2]
    ↓
Monitoring detects pool_slow overload
    ↓
Strategy 2 adjusts: [7, 2, 1]
    ↓
Traffic redistributed, pool_slow pressure reduced
```

**Combined Use**:
- Strategy 1 handles basic routing
- Strategy 2 handles dynamic optimization

### Strategy 3: Strongly Ordered

Strategy 1 and Strategy 3 are **mutually exclusive**:

```
Strategy 1 (Keyless): Ignores message content, random distribution
Strategy 3 (Keyed):   Fixed distribution based on key, ensures ordering

Selection Criteria:
- Stateless tasks → Strategy 1
- Stateful tasks → Strategy 3
```

---

## Real-World Examples

### Example 1: Log Collection System

```yaml
Scenario: Collect logs from multiple services

routing:
  stream_logs:
    pools: [pool_fast, pool_slow]
    weights: [8, 2]

Analysis:
- Logs have no dependencies ✅ Suitable for Keyless
- 80% logs to fast storage
- 20% logs to archival storage
- Throughput priority, order unimportant
```

### Example 2: Notification Push

```yaml
routing:
  stream_notifications:
    pools: [pool_sms, pool_email, pool_push]
    weights: [2, 3, 5]

With Strategy 2 dynamic adjustment:
- Daytime: push weight increases (users online)
- Nighttime: email weight increases (push ineffective)
```

---

## Performance Optimization Recommendations

### 1. Dispatcher Buffer Size
```go
// Adjust based on traffic characteristics
bufferSize := 100  // Default

If severe traffic bursts: bufferSize = 500
If memory sensitive: bufferSize = 50
```

### 2. Pool Count Trade-offs
```
Too many pools:
- Complex management
- More dispatcher goroutines

Too few pools:
- Reduced flexibility
- Single point overload risk

Recommendation: 2-5 pools
```

### 3. Monitoring-Driven Tuning
```
Periodically check:
1. Queue depth distribution across pools
2. Processing latency percentiles
3. Dropped message count

Adjust weights based on data
```

---

## Summary

**Strategy 1 (Keyless Routing) is the foundational strategy of LoadFlow**:
- ✅ Simple, efficient, easy to understand
- ✅ Optimized with Dispatcher to avoid goroutine explosion
- ✅ Suitable for stateless, high-throughput scenarios
- ❌ No ordering guarantee, not suitable for stateful tasks
- 🔄 Can combine with Strategy 2 for dynamic optimization
- ⚡ Performance baseline for Strategy 3 (Strategy 3 trades some performance for ordering)

**Design Philosophy**:
> Achieve maximum throughput in the simplest way when ordering is not required.

---

**Next Steps**:
- For dynamic weight adjustment → See [Strategy 2: Dynamic Rebalancing](strategy_2_dynamic_rebalancing_en.md)
- For ordering guarantee → See [Strategy 3: Strongly Ordered](strategy_3_strongly_ordered_en.md)
