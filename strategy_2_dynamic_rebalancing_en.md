# Strategy 2: Dynamic Rebalancing

## Overview

Dynamic Rebalancing is the **adaptive optimization strategy** of the LoadFlow framework. Building on **Strategy 1 (Keyless Routing)**, it continuously monitors system metrics and automatically adjusts routing weights to achieve dynamic load balancing.

**Core Value**: Upgrade "static configuration" to "autopilot," enabling the system to self-optimize based on real-time state.

---

## Why Dynamic Rebalancing is Needed?

### Limitations of Strategy 1

```yaml
# Strategy 1: Static weight configuration
routing:
  stream_c:
    pools: [pool_fast, pool_medium, pool_slow]
    weights: [1, 1, 8]  # Fixed
```

**Problem Scenario**:
```
Initial config: 80% traffic to pool_slow

Actual runtime:
T1: pool_slow queue 35/256   (14%)   ← Normal
T2: pool_slow queue 227/256  (89%)   ← Pressure increasing
T3: pool_slow queue 256/256  (100%)  ← Saturated!

But weights remain [1, 1, 8], traffic keeps flowing to pool_slow
```

**Consequences**:
- Queue continuously saturated, messages backlog
- Latency increases dramatically
- May lead to message loss or timeout
- Meanwhile pool_fast resources idle (low utilization)

---

## Core Principles

### Feedback Control Loop

```
Monitor Metrics → Calculate Pressure Delta → Decide Adjustment → Apply Weights → Observe Effect
   ↑                                                                       ↓
   └────────────────────── Continuous Loop ←──────────────────────────────┘
```

### Key Components

#### 1. Controller (Scheduler Controller)
```go
type Controller struct {
    tick      time.Duration  // Monitoring interval (e.g., 1s)
    cooldown  time.Duration  // Cooldown period (e.g., 3s)
    policies  map[string]Policy
}
```

**Responsibilities**:
- Periodic metric sampling
- Invoke strategy calculation
- Apply adjustment decisions
- Prevent frequent oscillation

#### 2. Strategy (Rebalancing Strategy)
```go
type Strategy interface {
    DecideStream(ctx, streamName, pools, weights, metrics) Plan
}
```

**Built-in Strategies**:
- **PressureRebalanceStrategy**: Based on queue pressure
- **LatencyRebalanceStrategy**: Based on processing latency (future)
- **ErrorRateRebalanceStrategy**: Based on error rate (future)

#### 3. Policy (Strategy Configuration)
```yaml
scheduler:
  policies:
    stream_c:
      enabled: true
      strategy: "pressure_rebalance"
      cooldown: 3s
      params:
        minPressureDelta: 3.0  # Trigger threshold
        maxStep: 1             # Max adjustment step
```

---

## Pressure-Based Rebalancing Algorithm

### Pressure Calculation

```go
Pressure = QueueDepth / ProcessRatePS

Where:
- QueueDepth: Current queue depth (number of pending tasks)
- ProcessRatePS: Processing rate (tasks/second, calculated by differentiation)

Example:
pool_slow: QueueDepth=227, ProcessRatePS=6.6 → Pressure = 227/6.6 = 34.39
pool_fast: QueueDepth=0, ProcessRatePS=65.0 → Pressure = 0/65.0 = 0.00

Pressure delta = 34.39 - 0.00 = 34.39

Pressure Meaning:
- Higher pressure = More congested (large queue & slow processing)
- Large queue + slow processing → High pressure
- Empty queue + fast processing → Low pressure
```

### Decision Logic

```go
if pressure_delta >= minPressureDelta {  // e.g., 3.0
    if cooldown_elapsed {
        Transfer weight from high-pressure pool to low-pressure pool
    }
}
```

### Adjustment Step

```
Current weights: [1, 1, 8]
Max step: maxStep = 1

Adjustment:
- pool_slow weight -1 → 7
- pool_fast weight +1 → 2

New weights: [2, 1, 7]
```

**Why small steps?**
- Avoid over-adjustment (overreaction)
- Observe effect before next decision
- More stable and controllable system

---

## Complete Case Analysis

### Demo Execution Record (6 rebalances in 23 seconds)

```
Initial state:
Weights: [1, 1, 8]
stream_c: 100 msg/s
pool_slow capacity: 6.6 msg/s
→ 80 msg/s flooding pool_slow (12x overload!)
```

#### Timeline

**T0 (0s): Startup**
```
Weights: [1, 1, 8]
pool_slow: 0/256
```

**T1 (1.0s): First rebalance**
```
Queue: 35/256
Pressure: 37.00 >> threshold(3.0)
Cooldown: ✅ First adjustment

Adjustment: [1,1,8] → [2,1,7]
Effect: pool_slow from 80% → 70% traffic
```

**T2 (8.0s): Second rebalance**
```
Queue: 227/256 (89%)
Pressure: 3.41 > threshold
Cooldown: ✅ 3 seconds elapsed

Adjustment: [2,1,7] → [3,1,6]
Effect: pool_slow down to 60% traffic
```

**Why wait 7 seconds?**
```
Double-gate mechanism:
1. Cooldown: 1.0s + 3s = 4.0s (adjustment allowed) ✅
2. Pressure threshold: needs delta >= 3.0

4.0s: delta=1.74 ❌
5.0s: delta=2.14 ❌
6.0s: delta=2.61 ❌
7.0s: delta=2.98 ❌ (so close!)
8.0s: delta=3.41 ✅ → Triggered
```

**T3-T6 (11s-23s): Continuous optimization**
```
T3 (11.0s): [3,1,6] → [4,1,5]  Queue: 256/256 (saturated)
T4 (14.5s): [4,1,5] → [5,1,4]  Queue: 256/256
T5 (19.0s): [5,1,4] → [6,1,3]  Queue: 256/256
T6 (23.0s): [6,1,3] → [7,1,2]  Queue: 241/256 ← Starting to drop!
```

**T7 (29.5s): Stable**
```
Weights: [7, 1, 2]
pool_slow: 20% traffic (20 msg/s)
Queue: 91/256 (36%)
Pressure delta: 1.56 < 3.0 → No more adjustments

✅ Reached equilibrium
```

---

## Double-Gate Protection Mechanism

### Why Two Gates?

**Problem with only cooldown**:
```
Adjust every 3 seconds regardless of pressure
→ May adjust even when pressure is minimal
→ Waste resources, introduce noise
```

**Problem with only threshold**:
```
Adjust immediately when pressure slightly exceeds threshold
→ May be too sensitive, frequent adjustments
→ System oscillation
```

**Double-gate (both must be satisfied)**:
```
Condition 1: time_since_last >= cooldown  (Time gate)
Condition 2: pressure_delta >= threshold   (Pressure gate)

Adjust only when both satisfied
```

### Parameter Tuning

#### Cooldown
```
Demo value: 3s (fast response)
Production recommendation: 10s (stability priority)

Too short: System oscillation, frequent weight changes
Too long: Slow response, pressure accumulation
```

#### MinPressureDelta (Pressure Threshold)
```
Demo value: 3.0
Meaning: Adjust only when pressure difference > 3%

Too small (e.g., 1.0): Sensitive to noise, frequent adjustments
Too large (e.g., 10.0): Not sensitive enough, late problem detection
```

#### MaxStep (Max Step Size)
```
Demo value: 1 (gradual)
Production recommendation: 2-3 (balanced)

Total weight=10, step 1 → Each adjustment shifts 10% traffic
Total weight=10, step 3 → Each adjustment shifts 30% traffic

Small step: Stable but slow convergence (Demo: 6 steps, 23s)
Large step: Fast but may overshoot
```

---

## Advantages

### 1. Automated Operations
```
No manual intervention needed:
- Monitor queue depth
- Calculate optimal weights
- Execute configuration changes

System automates everything, runs 24/7
```

### 2. Adapts to Load Changes
```
Scenario: Traffic burst

12:00 - Normal traffic 1000 msg/s
12:30 - Burst traffic 5000 msg/s  ← Auto-adjust weights
13:00 - Back to normal 1000 msg/s  ← Auto-revert
```

### 3. Full Resource Utilization
```
Avoids:
- Some pools overloaded, queues saturated
- Some pools idle, resources wasted

Achieves:
- Dynamic balance, all pools reasonably utilized
```

### 4. Observability
```
Event log for each adjustment:
[EVENT] type=plan_applied stream=stream_c 
        from=pool_slow to=pool_fast deltaW=1
        metric=pressure delta=3.41

Useful for:
- Post-analysis
- Troubleshooting
- Performance tuning
```

---

## Disadvantages

### 1. Introduces Complexity
```
Additional components:
- Controller (scheduler)
- Strategy (algorithms)
- Policy (configuration)
- EventSink (event recording)

Need to understand:
- Cooldown mechanism
- Pressure calculation
- Adjustment logic
```

### 2. Parameter Tuning Challenges
```
Need to adjust based on business characteristics:
- tick (monitoring frequency)
- cooldown (cooldown period)
- minPressureDelta (trigger threshold)
- maxStep (adjustment step)

Wrong configuration may cause:
- Slow response
- System oscillation
- Poor effectiveness
```

### 3. Cannot Handle Extreme Cases
```
Scenario: Total traffic exceeds total capacity

stream_c: 200 msg/s
Total capacity: 71.6 msg/s

Even with perfect weight distribution, still saturated
→ Need horizontal scaling (add pools)
```

### 4. Delayed Convergence
```
Time from detection to resolution:

T0: Detect pressure → T3: First adjustment → T23: Reach balance

During this period: Queue may remain saturated
Recommendation: Combine with alerting mechanism
```

---

## Relationship with Other Strategies

### Based on Strategy 1
```
Strategy 1: Provides basic routing capability
Strategy 2: Dynamically optimizes weights on top of Strategy 1

Dependency:
Strategy 2 must run on top of Strategy 1
```

### Mutually Exclusive with Strategy 3
```
Strategy 2: Dynamically adjusts weights → Same key may route to different pools
Strategy 3: Fixed key→pool mapping → Ensures ordering

Conflict:
Strategy 2's weight changes break Strategy 3's ordering guarantee

Solution:
Disable Strategy 2 for strongly ordered scenarios:
  policies:
    stream_ordered:
      enabled: false  # Disable dynamic rebalancing
```

---

## Real-World Examples

### Example 1: LoadFlow Demo

```yaml
Configuration:
  tick: 1s
  cooldown: 3s
  minPressureDelta: 3.0
  maxStep: 1

Results:
- 6 adjustments, reached balance in 23s
- Weights from [1,1,8] → [7,1,2]
- Queue from saturated(256) → healthy(91)
- Pressure from 37.00 → 1.56
```

**Lessons**:
- maxStep=1 too conservative, slow convergence
- Production recommendation: maxStep=2-3

### Example 2: E-commerce Flash Sale

```
Scenario: Black Friday traffic spike

00:00 - Warm-up, 1000 msg/s
00:01 - Sale starts, 10000 msg/s ← 10x burst

Strategy 2 automatically:
- Detects pool pressure surge
- Quickly adjusts weight distribution
- Directs traffic to high-performance pools
- Avoids system crash

01:00 - After peak, restore normal configuration
```

---

## Usage Guide

### Configuration Example

```yaml
scheduler:
  tick: 2s              # Check every 2s
  default_cooldown: 10s # Default cooldown
  
  policies:
    stream_a:
      enabled: true
      strategy: "pressure_rebalance"
      cooldown: 5s      # Override default
      params:
        minPressureDelta: 3.0
        maxStep: 2
        
    stream_b:
      enabled: false    # Disable auto-adjustment
```

### Monitoring Recommendations

```go
// Subscribe to adjustment events
sink.Subscribe(func(e Event) {
    if e.Type == "plan_applied" {
        log.Info("Rebalance occurred",
            "stream", e.StreamName,
            "from", e.FromPool,
            "to", e.ToPool,
            "delta", e.PressureDelta)
        
        // Alert if frequent adjustments
        if rebalanceCount > 10 {
            alert("Stream unstable")
        }
    }
})
```

### Best Practices

#### 1. Phased Enablement
```
Week 1: Observation mode (enabled: false)
  - Collect metrics
  - Simulate adjustment decisions
  
Week 2: Conservative enablement (maxStep: 1, cooldown: 15s)
  - Observe actual effects
  - Tune parameters
  
Week 3: Normal operation (maxStep: 2, cooldown: 10s)
```

#### 2. Tune Based on Business Characteristics
```
Low latency business (real-time trading):
  tick: 1s
  cooldown: 5s
  minPressureDelta: 2.0

High throughput business (log processing):
  tick: 5s
  cooldown: 30s
  minPressureDelta: 5.0
```

#### 3. Set Alerts
```
Trigger conditions:
- Single stream adjusted >5 times in 10 minutes
- Any pool queue depth > 90% for 1 minute continuously
- Pressure delta > 2x threshold continuously

Actions:
- Manual intervention and inspection
- Consider horizontal scaling
```

---

## Summary

**Strategy 2 (Dynamic Rebalancing) is an intelligent upgrade to Strategy 1**:
- ✅ Automated operations, reduced manual intervention
- ✅ Adapts to load changes, full resource utilization
- ✅ Ensures stability through double-gate mechanism
- ❌ Increases system complexity, requires parameter tuning
- ❌ Mutually exclusive with Strategy 3 (Strongly Ordered)
- 🎯 Suitable for scenarios with significant traffic fluctuations and latency sensitivity

**Design Philosophy**:
> Enable the system to think like humans: Observe → Analyze → Decide → Execute → Feedback

**Next Steps**:
- For basic routing → See [Strategy 1: Keyless Routing](strategy_1_keyless_routing_en.md)
- For ordered processing → See [Strategy 3: Strongly Ordered](strategy_3_strongly_ordered_en.md)
