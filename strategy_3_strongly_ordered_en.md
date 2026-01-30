# Strategy 3: Strongly Ordered

## Overview

Strongly Ordered strategy is the **ordering guarantee strategy** of the LoadFlow framework, specifically designed for stateful business scenarios. The core concept is: **messages with the same key must be executed serially in submission order, while messages with different keys can be processed in parallel**.

**Essence**: Building on Strategy 1, **refine the hash granularity from pool-level to goroutine-level**, achieving ordering guarantee through fixed key→stripe mapping and single-threaded consumption.

---

## Why Strongly Ordered is Needed?

### Ordering Problem in Strategy 1

```
Order processing system (using Strategy 1):

Event sequence for order-123:
1. Create order (10:00:00)
2. Pay order (10:00:01)
3. Ship order (10:00:02)

Routing result (weighted round-robin):
msg1(create) → pool_fast  → Worker-2 processes (50ms)
msg2(pay)    → pool_medium → Worker-1 processes (30ms) ← Finishes first
msg3(ship)   → pool_fast  → Worker-3 processes (40ms)

Actual execution order: pay → ship → create  ❌ Out of order!

Consequences:
- Pay when order doesn't exist → Failure
- Ship when order not paid → Logic error
- Database state inconsistency
```

---

## Core Principles

### Granularity Comparison

#### Strategy 1: Pool-Level Hashing
```
hash(streamName) % pool_count → Select pool

Problem:
Messages with same key enter same pool
→ But multiple workers compete within pool
→ Cannot guarantee ordering
```

#### Strategy 3: Goroutine-Level Hashing
```
hash(key) % stripe_count → Select stripe

Advantage:
Messages with same key enter same stripe
→ Only 1 goroutine within stripe
→ Serial execution, strictly guarantees ordering
```

**Key Difference**:
```
Strategy 1:
  Stream → Pool (multi-worker competition) → Parallel execution → Out of order

Strategy 3:
  Key → Stripe (single goroutine) → Serial execution → Ordered
```

---

## StripedPool Architecture

### Overall Structure

```
StripedPool (4 stripes)
┌────────────────────────────────────────┐
│ Stripe-0: Queue[100] + Goroutine-0    │ ← key: order-1, user-5
│ Stripe-1: Queue[100] + Goroutine-1    │ ← key: order-2, user-6
│ Stripe-2: Queue[100] + Goroutine-2    │ ← key: order-3, user-7
│ Stripe-3: Queue[100] + Goroutine-3    │ ← key: order-4, user-8
└────────────────────────────────────────┘

Key features:
- N stripes = N concurrency level
- Each stripe independent: queue + goroutine
- Fixed goroutine count (no explosion)
```

### Key to Stripe Mapping

```go
func selectStripe(key []byte) int {
    // FNV-1a hash algorithm
    h := fnv.New32a()
    h.Write(key)
    
    // Modulo distribution
    return int(h.Sum32()) % stripe_count
}
```

**Example**:
```
key = "order-123"
hash("order-123") = 0xA9B8C7D6 = 2847562938
2847562938 % 4 = 2

→ All "order-123" messages go to Stripe-2
```

**Why ordering is guaranteed?**
```
1. Hash function is deterministic
   Same key → Always same hash value

2. Modulo operation is fixed
   Same hash value → Always assigned to same stripe

3. Serial within stripe
   Same stripe → Only one goroutine consumes
   
∴ Same key → Same stripe → Same goroutine → Serial execution
```

---

## Complete Execution Flow

### Data Flow Trace

```
1. Message arrives at Runtime
   ↓
2. Key extraction (two layers)
   ├─ Codec.Decode()     → Extract from message format
   └─ KeyFunc()          → User-defined logic
   ↓
3. Routing selection
   Router.RouteWithKey(streamName, key) → Select pool
   ↓
4. StripedPool submission
   pool.SubmitWithKey(ctx, key, task)
   ↓
5. Stripe selection
   idx = hash(key) % stripe_count
   ↓
6. Task enqueue
   stripe.queue <- task
   ↓
7. Goroutine serial execution
   for task := range queue {
       executeTask(task)  ← Synchronous call, must complete before next
   }
```

### Key Code Paths

#### 1. Key Extraction (runtime.go)
```go
// 1. Codec parsing
key, payload, ok := codec.Decode(raw)

// 2. User function override
if keyFn != nil {
    k2 := keyFn(streamName, payload)
    if len(k2) > 0 {
        key = k2  ← User-defined takes priority
    }
}
```

#### 2. Submit to Stripe (striped_pool.go)
```go
func (sp *StripedPool) SubmitWithKey(ctx context.Context, key []byte, task func()) error {
    // Calculate stripe index
    idx := sp.selectStripe(key)
    
    // Submit to designated stripe
    return sp.submitToStripe(ctx, idx, task)
}
```

#### 3. Stripe Serial Consumption (striped_pool.go)
```go
func (sp *StripedPool) runStripe(s *stripe) {
    for {
        select {
        case task := <-s.queue:
            sp.executeTask(s, task)  ← Blocking execution
        }
    }
}
```

**Key to ordering guarantee**:
```
Same stripe's goroutine:
1. Retrieve task1 from queue
2. executeTask(task1) ← Synchronous call, must complete
3. After task1 completes, retrieve task2 from queue
4. executeTask(task2) ← Block again

FIFO queue + single-threaded consumption = absolute ordering
```

---

## Ordering Proof

### Mathematical Proof

```
Let message sequence with same key be: M1, M2, M3, ..., Mn

Proposition: Execution order = Submission order

Proof:
1. ∵ hash(key) is deterministic function
   ∴ idx = hash(key) % N is fixed value i

2. ∵ M1, M2, M3 all calculate idx = i
   ∴ All enter stripe-i's queue

3. ∵ Go channel is FIFO
   ∴ Enqueue order M1→M2→M3 ensures dequeue order M1→M2→M3

4. ∵ stripe-i has only one goroutine
   ∴ Must finish M1 before executing M2

5. ∴ Execution order = M1 → M2 → M3 = Submission order

Q.E.D. □
```

### Actual Verification (Test Code)

```go
// TestStripedPool_StrictOrdering
func TestStripedPool_StrictOrdering(t *testing.T) {
    pool := NewStripedPool("test", 4, 100)
    pool.Start(ctx)

    // 10 keys, each key sends 100 ordered messages (0-99)
    for _, key := range keys {
        for seq := 0; seq < 100; seq++ {
            pool.SubmitWithKey(ctx, []byte(key), func() {
                recorder.append(seq)  // Record receive order
            })
        }
    }

    // Verify receive order for each key
    for _, key := range keys {
        received := recorder.get(key)
        for i, seq := range received {
            if seq != i {
                t.Errorf("Ordering violated: expected %d, got %d", i, seq)
            }
        }
    }
}

Result: ✅ All 10 keys × 100 messages = 1000 messages, 100% ordered
```

---

## Parallelism Guarantee

### Parallel Execution of Different Keys

```
Simultaneously submit messages for 4 orders:

order-123: [created, paid, shipped]  → hash → Stripe-2
order-456: [created, paid, shipped]  → hash → Stripe-0
order-789: [created, paid]           → hash → Stripe-1
order-999: [created]                 → hash → Stripe-3

Execution:
Stripe-0 Goroutine: order-456.created → order-456.paid → ...
Stripe-1 Goroutine: order-789.created → order-789.paid
Stripe-2 Goroutine: order-123.created → order-123.paid → ...
Stripe-3 Goroutine: order-999.created

✅ 4 orders processed in parallel
✅ Each order internally serial and ordered
```

**Performance Characteristics**:
```
Concurrency level = Stripe count

4 stripes → At most 4 orders processed simultaneously
8 stripes → At most 8 orders processed simultaneously

Selection recommendation:
- CPU-intensive: stripe_count = CPU cores
- IO-intensive: stripe_count = 2 × CPU cores
```

---

## Fault Tolerance Mechanism

### Panic Recovery

```go
func (sp *StripedPool) executeTask(s *stripe, task func()) {
    defer func() {
        if r := recover(); r != nil {
            // Log panic
            log.Printf("stripe-%d panic: %v", s.id, r)
            atomic.AddUint64(&s.panics, 1)
            
            // ✅ Goroutine won't exit, continues processing next task
        }
    }()
    
    task()  // Execute task
}
```

**Stripe Down Handling**:
```
Assume task2 panics in order-123's task sequence:

task1(created) → Execute successfully ✅
task2(paid)    → panic ❌ → Caught → Log recorded
task3(shipped) → Execute normally ✅

Key:
- runStripe loop continues
- Won't create new goroutine
- Doesn't affect subsequent tasks in this stripe
- Doesn't affect other stripes
```

**Isolation**:
```
Stripe-0 panic → Only affects keys hashed to Stripe-0
Stripe-1,2,3 → Completely unaffected

Example:
order-123 → Stripe-2 (normal)
order-456 → Stripe-0 (panic, affected)
order-789 → Stripe-1 (normal)
```

---

## Advantages

### 1. Strict Ordering Guarantee
```
Mathematical proof + actual verification
1000 messages, 100% ordered
Suitable for: orders, state machines, financial transactions, etc.
```

### 2. Parallel Processing Capability
```
Different keys assigned to different stripes
Fully utilize multi-core CPUs
Throughput = stripe_count × per-stripe throughput
```

### 3. Fault Isolation
```
Single stripe failure doesn't affect others
Panic auto-recovery, no crash
Each stripe independently monitored
```

### 4. Controllable Performance
```
Fixed goroutine count (= stripe_count)
No goroutine explosion like early Strategy 1
Predictable memory usage
```

---

## Disadvantages

### 1. Slightly Lower Performance than Strategy 1
```
Additional overhead:
- Hash computation: ~10-50ns/message
- Key extraction: Codec + KeyFunc

Comparison:
Strategy 1 (Keyless): Direct routing, no computation
Strategy 3 (Keyed): Requires hash computation

Gap:
- CPU-intensive tasks: Almost no impact
- IO-intensive tasks: Negligible (< 1%)
```

### 2. No Dynamic Rebalancing Support
```
Problem:
If changing stripe_count (e.g., 4→8)
→ hash(key) % 4 → hash(key) % 8
→ Same key assigned to different stripe
→ Breaks ordering

Limitation:
Strategy 3 mutually exclusive with Strategy 2 (dynamic rebalancing)

Configuration:
policies:
  stream_ordered:
    enabled: false  # Must disable rebalancing
```

### 3. Hotspot Key Problem
```
If certain key has exceptionally high message volume:

VIP user (user-123): 1000 msg/s
Regular users: 10 msg/s

hash(user-123) → Stripe-2

Result:
Stripe-2 overloaded (1000 msg/s)
Stripe-0,1,3 idle (10 msg/s each)

Solution (v0.6 planned):
- Key sub-sharding (user-123-shard-1, user-123-shard-2)
- Dynamic stripe expansion (safe migration)
```

### 4. Requires Reasonable Key Design
```
Good keys:
✅ order_id, user_id, device_id (business IDs)
✅ Evenly distributed, no hotspots

Bad keys:
❌ timestamp (every message different, loses meaning)
❌ fixed value (all messages to same stripe)
❌ low cardinality (e.g., gender, only 2 values)
```

---

## Relationship with Other Strategies

### Based on Strategy 1's Hashing Concept
```
Strategy 1: hash(streamName) % pool_count
Strategy 3: hash(key) % stripe_count

Similarities:
- Both use hash algorithm
- Both static mapping (no dynamic adjustment)

Differences:
- Strategy 1: Coarse-grained (pool level)
- Strategy 3: Fine-grained (goroutine level)
```

### Mutually Exclusive with Strategy 2
```
Strategy 2: Dynamically adjust weights
  → key→pool mapping may change
  → Breaks ordering

Strategy 3: Fixed key→stripe mapping
  → Guarantees ordering
  → Cannot dynamically adjust

Conflict root cause:
Ordering requires stable key mapping
Dynamism requires changeable key mapping

Choice:
Stateful scenarios → Strategy 3 (sacrifice dynamism)
Stateless scenarios → Strategy 1+2 (pursue throughput)
```

---

## Real-World Examples

### Example 1: Order Processing System

```go
pool := pool.NewStripedPool("order-pool", 8, 100)
pool.Start(ctx)

// Key = order_id
keyFn := func(stream string, payload []byte) []byte {
    var order Order
    json.Unmarshal(payload, &order)
    return []byte(order.ID)
}

// Submit order events
events := []OrderEvent{
    {ID: "order-123", Type: "created"},
    {ID: "order-123", Type: "paid"},
    {ID: "order-123", Type: "shipped"},
}

for _, event := range events {
    pool.SubmitWithKey(ctx, []byte(event.ID), func() {
        processOrder(event)  // Guarantee ordered execution
    })
}
```

**Guarantee**:
```
order-123: created → paid → shipped (ordered)
order-456: created → paid (ordered)
Both orders processed in parallel
```

### Example 2: User Session Management

```go
// Key = session_id
pool := pool.NewStripedPool("session-pool", 16, 200)

// Same user's operations serial
actions := []UserAction{
    {SessionID: "sess-abc", Action: "login"},
    {SessionID: "sess-abc", Action: "browse"},
    {SessionID: "sess-abc", Action: "purchase"},
    {SessionID: "sess-abc", Action: "logout"},
}

// Guarantee same session's actions ordered
```

### Example 3: State Machine Processing

```go
// Workflow engine
// Key = workflow_id

states := []StateTransition{
    {WorkflowID: "wf-001", From: "draft", To: "pending"},
    {WorkflowID: "wf-001", From: "pending", To: "approved"},
    {WorkflowID: "wf-001", From: "approved", To: "completed"},
}

// Guarantee strict order of state transitions
```

---

## Usage Guide

### Configuration Example

```yaml
# Create StripedPool
pools:
  - name: order_pool
    type: striped
    stripe_count: 8      # Concurrency level
    queue_per_stripe: 100

# Routing configuration
routing:
  stream_orders:
    pool: order_pool
    key_extractor: "order_id"  # Extract order_id from message

# Disable dynamic rebalancing
scheduler:
  policies:
    stream_orders:
      enabled: false  # Must disable for strongly ordered scenarios
```

### Parameter Tuning

#### Stripe Count
```
Recommendations:
- CPU-intensive: runtime.NumCPU()
- IO-intensive: 2 × runtime.NumCPU()
- Default: 4-8

Example:
8-core machine:
- CPU tasks: stripe_count = 8
- IO tasks: stripe_count = 16
```

#### Queue Size
```
Each stripe's queue:

Low latency: queue = 10-50
High throughput: queue = 100-500

Total buffer = stripe_count × queue_per_stripe
```

### Key Design Principles

```
1. Uniqueness
   ✅ order_id, user_id (unique per order/user)
   ❌ timestamp (different for each message)

2. Stability
   ✅ Fixed business ID
   ❌ Randomly generated value

3. Even distribution
   ✅ UUID, database auto-increment ID
   ❌ Hotspot user ID (consider sub-sharding)

4. Business semantics
   ✅ Meets business ordering requirements
   ❌ Technical fields (like machine IP)
```

---

## Monitoring and Tuning

### Key Metrics

```go
metrics := pool.GetMetrics()

// Global metrics
fmt.Printf("Total Processed: %d\n", metrics.Processed)
fmt.Printf("Total Panics: %d\n", metrics.Panics)

// Per-stripe metrics
for _, sm := range metrics.Stripes {
    fmt.Printf("Stripe-%d: Processed=%d, QueueSize=%d/%d, Panics=%d\n",
        sm.ID, sm.Processed, sm.QueueSize, sm.Capacity, sm.Panics)
}
```

### Alert Rules

```
1. Panic rate > 1%
   → Check business code

2. Certain stripe queue continuously > 80%
   → Hotspot key problem

3. Extremely uneven stripe processing
   → Uneven key distribution, consider redesign

4. All stripe queues full
   → Insufficient overall capacity, increase stripes or optimize performance
```

---

## Summary

**Strategy 3 (Strongly Ordered) is an ordering-enhanced version of Strategy 1**:
- ✅ Guarantees ordering through **fine-grained hashing** (key→stripe)
- ✅ Single goroutine consuming FIFO queue, mathematical-level ordering guarantee
- ✅ Different keys parallel, fully utilize multi-core
- ✅ Panic auto-recovery, stripe isolation
- ❌ Slightly lower performance than Strategy 1 (hash computation overhead)
- ❌ Mutually exclusive with Strategy 2 (dynamic rebalancing)
- ❌ Requires reasonable key design
- 🎯 Suitable for stateful scenarios like orders, sessions, state machines

**Design Philosophy**:
> In scenarios requiring ordering, achieve the strongest ordering guarantee with minimal performance cost through finer granularity control.

**Core Innovation**:
> Refine from pool-level coarse-grained routing to goroutine-level precise control—this is the key to ensuring ordering.

---

**Recommended Reading**:
- Basic routing → [Strategy 1: Keyless Routing](strategy_1_keyless_routing_en.md)
- Dynamic optimization → [Strategy 2: Dynamic Rebalancing](strategy_2_dynamic_rebalancing_en.md)
- Implementation details → [StripedPool Documentation](docs/striped_pool.md)
