# LoadFlow Demo - Pressure-Based Rebalancing Demonstration

English | [简体中文](README_zh.md)

This demo showcases LoadFlow's **automatic load balancing** capability through a pressure-based rebalancing mechanism. Watch as the system autonomously detects queue saturation and adjusts routing weights to achieve equilibrium.

## 🎯 What This Demo Proves

This demonstration validates LoadFlow's core capabilities:

### ✅ Automatic Pressure Detection
- Monitors queue depth and pool capacity in real-time
- Calculates pressure metrics across all worker pools
- Triggers rebalancing when pressure exceeds configured thresholds

### ✅ Intelligent Weight Adjustment
- Gradually shifts traffic from saturated to underutilized pools
- Uses configurable step sizes for controlled, observable changes
- Prevents oscillation through cooldown mechanisms

### ✅ Self-Healing Convergence
- Achieves stable equilibrium without manual intervention
- Adapts to varying capacity constraints across heterogeneous pools
- Demonstrates feedback-driven iteration (detect → adjust → stabilize)

## 📋 Quick Start

### 1. Run the Demo

```bash
cd cmd/demo-channel
go run main.go
```

The demo runs for 30 seconds by default (configurable in `config.yaml`).

**Expected Output:**
```
[Demo] Starting LoadFlow demo (duration=30s)...
[Pool] Created: pool_fast (workers=5, queue=1024)
[Pool] Created: pool_medium (workers=3, queue=512)
[Pool] Created: pool_slow (workers=2, queue=256)
...
═══════════════════════════════════════════════════════════════
Time    pool_fast       pool_medium     pool_slow
───────────────────────────────────────────────────────────────
0.5s    0/1024          0/512           15/256
1.0s    0/1024          0/512           35/256
[EVENT] type=plan_applied stream=stream_c from=pool_slow to=pool_fast deltaW=1
...
```

### 2. Observe Rebalancing Events

Watch the console logs for rebalancing events:
- `plan_generated` - System detected pressure and created adjustment plan
- `plan_applied` - Weight change successfully applied
- `plan_rejected` - Adjustment blocked (cooldown or insufficient pressure)

### 3. Monitor Metrics (Optional)

View Prometheus metrics in your browser:
```
http://localhost:2112/metrics
```

Key metrics to watch:
- `loadflow_pool_queue_depth` - Queue saturation levels
- `loadflow_router_weight` - Current routing weights per stream/pool
- `loadflow_rebalance_plan_total` - Rebalancing event counts

### 4. Start Prometheus (Optional)

For graphical visualization:

```bash
cd prometheus
prometheus --config.file=prometheus.yaml
```

Access Prometheus UI at `http://localhost:9090`

## 📊 Understanding the Demo Scenario

### Initial Configuration (Intentionally Imbalanced)

**Pool Capacities:**
- `pool_fast`: 5 workers × 100ms latency = **50 msg/s**
- `pool_medium`: 3 workers × 200ms latency = **15 msg/s**
- `pool_slow`: 2 workers × 300ms latency = **6.6 msg/s**

**Traffic Load:**
- `stream_a`, `stream_b`: 20 msg/s each (background)
- `stream_c`: **100 msg/s** (main demo stream)

**Initial Weights for stream_c: `[1, 1, 8]`**
- pool_fast: 10% → 10 msg/s ✅ (well below capacity)
- pool_medium: 10% → 10 msg/s ✅ (below capacity)
- pool_slow: **80%** → **80 msg/s** ⚠️ (12× capacity!)

**Predicted Outcome:**
`pool_slow` will saturate rapidly, triggering automatic rebalancing.

### What Happens During Demo

1. **Phase 1 (0-1s): Pressure Build-up**
   - pool_slow accumulates 35 messages/second
   - Pressure delta exceeds threshold (37.0 > 3.0)

2. **Phase 2 (1-8s): First Adjustments**
   - Rebalance #1: Weights shift to [2, 1, 7] (70% → slow)
   - Rebalance #2: Weights shift to [3, 1, 6] (60% → slow)
   - Queue continues growing but at reduced rate

3. **Phase 3 (8-22s): Saturation & Iteration**
   - Queue reaches full capacity (256/256) at 10s
   - System performs 4 more adjustments (#3-#6)
   - Cooldown mechanism prevents oscillation

4. **Phase 4 (22-30s): Recovery & Stabilization**
   - Final weights: [7, 1, 2] (20% → slow, 70% → fast)
   - Queue drains from 256 → 91 in 6.5 seconds
   - Pressure drops below threshold (1.56 < 3.0)
   - **System reaches stable equilibrium** 🎯

### Key Tuning Parameters (Demo vs. Production)

| Parameter | Demo Value | Production Recommendation | Purpose |
|-----------|------------|---------------------------|---------|
| `maxStep` | 1 | 2-3 | Step size for weight changes |
| `cooldown` | 3s | 10s | Minimum time between adjustments |
| `minPressureDelta` | 3.0 | 5.0 | Threshold for triggering rebalance |
| `tick` | 1s | 5s | Monitoring frequency |

**Why demo uses smaller values?**
- Faster iterations make the rebalancing process visible
- Each adjustment's effect can be clearly observed
- Trade-off: 6 steps over 23s vs. 2 steps over 6s (production)

## 🔧 Configuration Guide

All behavior is controlled through `config.yaml`:

### Modify Demo Duration

```yaml
demo:
  duration: 120s  # Change to 2 minutes
  metrics_port: 2112
```

### Adjust Pool Configuration

```yaml
pools:
  - name: pool_fast
    workers: 10        # Increase capacity
    queue: 2048        # Larger buffer
    base_latency: 50ms # Faster processing
```

### Change Traffic Patterns

```yaml
streams:
  - name: stream_c
    type: weighted
    rate: 200          # Double the load!
```

### Tune Rebalancing Behavior

```yaml
scheduler:
  tick: 2s             # Check every 2 seconds
  default_cooldown: 10s
  policies:
    stream_c:
      enabled: true
      strategy: pressure_rebalance
      cooldown: 5s     # More aggressive
      params:
        minPressureDelta: 2.0  # More sensitive
        maxStep: 3             # Larger adjustments
```

## 📈 Key Metrics Explained

### Rebalancing Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `loadflow_rebalance_plan_total` | Counter | stream, result | Count of rebalancing events (generated/applied/rejected) |
| `loadflow_rebalance_step_size` | Gauge | stream | Size of the last weight adjustment |
| `loadflow_router_weight` | Gauge | stream, pool | Current routing weight for each stream-pool pair |

### Pool Health Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `loadflow_pool_queue_depth` | Gauge | pool | Current queue depth |
| `loadflow_pool_queue_capacity` | Gauge | pool | Maximum queue capacity |
| `loadflow_pool_tasks_processed_total` | Counter | pool | Total messages processed |
| `loadflow_pool_worker_count` | Gauge | pool | Number of active workers |

### Routing Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `loadflow_stream_messages_in_total` | Counter | stream | Messages received per stream |
| `loadflow_router_routed_total` | Counter | stream, pool | Messages routed from stream to pool |
| `loadflow_router_routing_violations_total` | Counter | stream | Sticky routing violations (same key to different pools) |

## 📝 Detailed Analysis

For a complete timeline analysis of the rebalancing process, see:

- **English**: [`demo_outcomes_detailed.md`](./demo_outcomes_detailed.md) - Full event timeline with metrics
- **中文**: [`demo_outcomes_detailed_zh.md`](./demo_outcomes_detailed_zh.md) - 完整事件时间线和指标分析

These documents provide:
- ✅ Step-by-step breakdown of all 6 rebalancing cycles
- ✅ Pressure metrics at each decision point
- ✅ Explanation of the dual-gate mechanism (cooldown + threshold)
- ✅ Queue depth progression and recovery timeline
- ✅ Design insights and production tuning recommendations

## 🔍 Troubleshooting

### Why aren't weights changing for stream_c?

**Check these conditions:**

1. **Policy enabled?**
   ```yaml
   policies:
     stream_c:
       enabled: true  # Must be true
   ```

2. **Sufficient pressure delta?**
   - View `loadflow_pool_queue_depth` metric
   - Calculate pressure: `queue_depth / queue_capacity`
   - Must exceed `minPressureDelta` threshold

3. **Cooldown period?**
   - Check event logs for `plan_rejected` with `msg=in cooldown`
   - Wait for configured `cooldown` duration to elapse

### What if I see routing_violations_total > 0?

**This indicates a serious bug!** Possible causes:

- Router implementation not respecting key-based routing
- Concurrent modification of routing tables during rebalancing
- Hash function not deterministic

**Debug steps:**
```bash
# Search logs for VIOLATION events
grep "VIOLATION" demo.log

# Check which keys are affected
curl http://localhost:2112/metrics | grep routing_violations
```

### How to make rebalancing faster?

**Reduce these values in `config.yaml`:**

```yaml
scheduler:
  tick: 500ms           # Check more frequently (default: 1s)
  policies:
    stream_c:
      cooldown: 1s      # Faster iteration (default: 3s)
      params:
        maxStep: 5      # Larger weight shifts (default: 1)
```

**Warning**: Too aggressive settings may cause oscillation!

## 🎓 Learning Outcomes

After running this demo, you'll understand:

✅ **How pressure-based rebalancing works**
   - Automatic detection of queue saturation
   - Incremental weight adjustment to redistribute load
   - Convergence to stable equilibrium

✅ **The dual-gate safety mechanism**
   - Cooldown prevents rapid oscillation
   - Threshold ensures meaningful adjustments
   - Both conditions must be met for changes

✅ **Trade-offs in parameter tuning**
   - `maxStep`: Gradual (stable) vs. aggressive (fast)
   - `cooldown`: Responsive vs. oscillation-resistant
   - `minPressureDelta`: Sensitive vs. noise-tolerant

✅ **Observable system behavior**
   - Real-time metrics via Prometheus
   - Event logs explaining every decision
   - Visual feedback through console output

## 🚀 Next Steps

**Experiment with different scenarios:**

1. **Simulate traffic spikes**
   - Change `stream_c.rate` from 100 to 500
   - Watch how system adapts to extreme overload

2. **Test heterogeneous pools**
   - Vary `base_latency` across pools (10ms, 100ms, 1000ms)
   - Observe how capacity differences affect weight distribution

3. **Try different strategies**
   - Implement custom rebalancing strategies
   - Compare pressure-based vs. latency-based vs. error-rate-based

4. **Add more streams**
   - Create competing streams with different priorities
   - Test multi-stream rebalancing interactions

**Have fun experimenting with LoadFlow!** 🎉

