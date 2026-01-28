# LoadFlow Demo - Configuration-Driven Load Balancing Demo

本 Demo 用于验证 LoadFlow 框架的核心能力：**路由语义正确性**、**闭环重均衡**和**可观测性**。

## 🎯 Demo 目标

通过一个配置驱动的演示场景，证明以下能力：

### A 块：路由与消息协议
- **A1**: 默认 JSON Envelope + key 可为空 + keyFn 覆盖
- **A2**: 无 key 时按权重分发（纯吞吐路由）
- **A3**: 有 key 时 sticky per pool（同 key 始终路由到同一个池）

### B 块：负载均衡闭环
- **B1**: 采样 → Decide → Apply → Cooldown 的完整闭环
- **B2.2**: per-stream policy（不同流使用不同策略/参数/冷却期）
- **B2.5**: Explainability（事件日志和指标解释调整原因）

## 📋 快速开始

### 1. 运行 Demo

```bash
cd cmd/demo-channel
go run main.go
```

Demo 将运行 60 秒，你可以在配置文件中修改 `demo.duration`。

### 2. 查看 Prometheus 指标

在浏览器中打开：
```
http://localhost:2112/metrics
```

### 3. (可选) 启动 Prometheus

如果想用 Prometheus UI 查看图表：

```bash
cd prometheus
prometheus --config.file=prometheus.yaml
```

然后访问 `http://localhost:9090`

## 🔧 配置说明

所有场景配置都在 `config.yaml` 中定义，你可以通过修改配置来改变演示行为：

### 修改运行时间

```yaml
demo:
  duration: 120s  # 改为 2 分钟
```

### 修改池配置

```yaml
pools:
  - name: pool_fast
    workers: 16      # 增加 worker 数量
    queue: 4096      # 增加队列容量
    base_latency: 3ms  # 减少延迟
```

### 注入故障

```yaml
pools:
  - name: pool_medium
    # ...其他配置
    bad_phase:
      start: 20s     # 20秒后开始故障
      end: 40s       # 40秒后恢复
      latency: 200ms # 故障期间延迟
      error_rate: 0.2  # 20% 错误率
```

### 修改流量模式

```yaml
streams:
  - name: stream_c
    rate: 2000  # 增加到每秒 2000 条消息
    phases:
      - start: 0s
        duration: 30s
        bias_pool: pool_fast  # 前30秒偏向快池
```

### 调整调度策略

```yaml
scheduler:
  tick: 1s  # 每秒采样一次
  policies:
    stream_c:
      enabled: true
      cooldown: 3s  # 更短的冷却期
      params:
        minPressureDelta: 5.0  # 更高的阈值（更保守）
        maxStep: 5.0           # 更大的步长（更激进）
```

## 📊 验收标准

Demo 运行后，通过以下指标验证各项能力：

### ✅ A2: 纯吞吐路由正确性

**指标**:
```promql
rate(loadflow_pool_messages_processed_total{stream="stream_a"}[30s])
```

**预期**: pool_fast : pool_medium : pool_slow ≈ 1 : 2 : 4 (±10%)

**说明**: stream_a 没有 key，应该严格按照权重比例分发

---

### ✅ A3: Sticky 路由正确性

**指标**:
```promql
loadflow_router_routing_violations_total{stream="stream_b"}
```

**预期**: 始终为 0

**说明**: stream_b 有 1000 个不同的 key，每个 key 应始终路由到同一个池

---

### ✅ B2.2: per-stream policy 生效

**指标**:
```promql
loadflow_router_weight{stream=~"stream_.*"}
```

**预期**:
- `stream_a` 权重保持 [1, 2, 4] 不变（disabled）
- `stream_b` 权重保持 [1, 2, 4] 不变（disabled）
- `stream_c` 权重**会变化**（enabled + pressure_rebalance）

**说明**: 只有 stream_c 启用了自动调度，其他流保持初始权重

---

### ✅ B1: 闭环重均衡工作

**指标**:
```promql
loadflow_rebalance_plan_total{stream="stream_c",result="applied"}
```

**预期**: > 0（至少发生过调整）

**事件日志示例**:
```
[EVENT] type=plan_generated stream=stream_c reason=pressure_rebalance 
        from=pool_slow to=pool_fast deltaW=2 metric=pressure 
        delta=8.50 threshold=3.00 msg=plan generated

[EVENT] type=plan_applied stream=stream_c ... msg=applied successfully
```

---

### ✅ B2.5: Explainability（可解释性）

**检查项**:

1. **事件日志完整性**
   - 每次调整都有 `plan_generated` 和 `plan_applied`/`plan_rejected` 事件
   - 日志包含 from/to pool、delta、threshold

2. **指标维度完整性**
   ```promql
   loadflow_rebalance_plan_total{result=~"generated|applied|rejected|failed"}
   ```

3. **权重变化可追踪**
   ```promql
   loadflow_router_weight{stream="stream_c"}
   ```
   可以看到权重的时间序列变化

---

### ✅ 故障注入效果

**观察时间**: T=30s 到 T=40s (pool_medium bad_phase)

**预期指标变化**:

1. **错误率上升**
   ```promql
   rate(loadflow_pool_errors_total{pool="pool_medium"}[10s])
   ```
   应该接近 0.1 (10%)

2. **延迟上升**
   ```promql
   histogram_quantile(0.99, 
     rate(loadflow_pool_processing_duration_seconds_bucket{pool="pool_medium"}[10s])
   )
   ```
   应该接近 0.1s (100ms)

3. **权重调整**
   ```promql
   loadflow_router_weight{stream="stream_c",pool="pool_medium"}
   ```
   应该看到权重**下降**（scheduler 远离坏池）

## 🏗️ Demo 架构

```
┌─────────────┐
│  config.yaml│ ← 配置驱动
└──────┬──────┘
       │
       ↓
┌─────────────────────────────────────────┐
│           main.go (单文件)               │
├─────────────────────────────────────────┤
│                                          │
│  ┌───────────────────────────────────┐  │
│  │  MessageGenerator (3个)           │  │
│  │  - stream_a: 纯吞吐 (no key)       │  │
│  │  - stream_b: Sticky (1000 keys)   │  │
│  │  - stream_c: Rebalance Demo        │  │
│  └───────┬───────────────────────────┘  │
│          ↓ Envelope{stream,key,payload} │
│  ┌───────────────────────────────────┐  │
│  │  Runtime                          │  │
│  │  - codec.Decode()                 │  │
│  │  - router.RouteWithKey()          │  │
│  │  - pool.Submit(task)              │  │
│  └───────┬───────────────────────────┘  │
│          ↓ task                          │
│  ┌───────────────────────────────────┐  │
│  │  InstrumentedHandler              │  │
│  │  - 延迟注入 (base + bad phase)      │  │
│  │  - 错误注入 (error_rate)            │  │
│  │  - 指标埋点 (histogram, counter)    │  │
│  └───────────────────────────────────┘  │
│                                          │
│  ┌───────────────────────────────────┐  │
│  │  Scheduler Controller              │  │
│  │  - Sample (每 2s)                   │  │
│  │  - DecideStream (pressure策略)      │  │
│  │  - Apply (更新权重)                 │  │
│  │  - Cooldown (防震荡)                │  │
│  └───────┬───────────────────────────┘  │
│          ↓ Plan{NewWeights}             │
│  ┌───────────────────────────────────┐  │
│  │  InstrumentedApplier              │  │
│  │  - 更新 router 权重                 │  │
│  │  - 更新 weightGauge 指标            │  │
│  └───────────────────────────────────┘  │
│                                          │
│  ┌───────────────────────────────────┐  │
│  │  KeyTracker                       │  │
│  │  - 跟踪 key -> pool 映射            │  │
│  │  - 检测 sticky 违规                 │  │
│  └───────────────────────────────────┘  │
│                                          │
└──────────────────┬───────────────────────┘
                   ↓
            ┌──────────────┐
            │ :2112/metrics│ ← Prometheus
            └──────────────┘
```

## 📈 关键指标说明

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `loadflow_stream_messages_in_total` | Counter | stream | 每个流接收的消息总数 |
| `loadflow_pool_messages_processed_total` | Counter | stream, pool | 每个池处理的消息总数 |
| `loadflow_pool_processing_duration_seconds` | Histogram | stream, pool | 处理延迟分布 |
| `loadflow_pool_errors_total` | Counter | stream, pool | 处理错误总数 |
| `loadflow_router_routing_violations_total` | Counter | stream | 路由违规次数（应为0） |
| `loadflow_router_weight` | Gauge | stream, pool | 当前路由权重 |
| `loadflow_rebalance_plan_total` | Counter | stream, result | 调度计划统计 |
| `loadflow_rebalance_step_size` | Gauge | stream | 最近一次调整步长 |
| `loadflow_pool_queue_depth` | Gauge | pool | 队列深度 |
| `loadflow_pool_worker_count` | Gauge | pool | Worker 数量 |

## 🔍 常见问题

### Q: 为什么 stream_c 的权重没变化？

**A**: 检查以下几点：
1. 配置中 `scheduler.policies.stream_c.enabled` 是否为 `true`
2. 是否存在压力差：查看 `loadflow_pool_queue_depth`
3. 是否在 cooldown：查看日志中的 `plan_rejected` 事件
4. 阈值是否过高：降低 `minPressureDelta` 参数

### Q: 看到 routing_violations_total > 0 怎么办？

**A**: 这是严重 bug！可能原因：
1. Router 实现有问题（hash 不稳定）
2. 并发问题（权重更新时的竞态）
3. 配置问题（stream_b 的 key_enabled 应为 true）

### Q: 如何加速观察效果？

**A**: 修改配置：
```yaml
demo:
  duration: 30s  # 缩短总时长

scheduler:
  tick: 1s       # 更频繁采样
  
  policies:
    stream_c:
      cooldown: 2s  # 更短冷却期
      params:
        minPressureDelta: 1.0  # 更低阈值（更敏感）
```

## 📝 扩展场景

你可以通过修改 `config.yaml` 创建新的测试场景：

### 场景 1: 4 个池的复杂拓扑

```yaml
pools:
  - name: pool_a
    workers: 4
    ...
  - name: pool_b
    workers: 4
    ...
  - name: pool_c
    workers: 4
    ...
  - name: pool_d
    workers: 4
    ...

routing:
  stream_c:
    pools: [pool_a, pool_b, pool_c, pool_d]
    initial_weights: [1, 1, 1, 1]
```

### 场景 2: 多阶段负载迁移

```yaml
streams:
  - name: stream_c
    phases:
      - {start: 0s, duration: 10s, bias_pool: pool_slow}
      - {start: 10s, duration: 10s, bias_pool: pool_medium}
      - {start: 20s, duration: 10s, bias_pool: pool_fast}
      - {start: 30s, duration: 10s, bias_pool: pool_medium}
      - {start: 40s, duration: 20s, bias_pool: pool_slow}
```

### 场景 3: 更激进的策略参数

```yaml
scheduler:
  policies:
    stream_c:
      params:
        minPressureDelta: 1.0  # 更敏感
        maxStep: 10.0          # 更大步长
        maxFrac: 0.5           # 允许单次改动 50% 权重
```

## 🚀 下一步

1. **集成 Grafana**: 导入 `grafana/dashboard.json` 可视化指标
2. **增加指标**: 在 handler 中添加更多业务指标
3. **自定义策略**: 实现 `StreamStrategy` 接口并注册
4. **配置热更新**: 监听配置文件变化，动态调整参数

---

**Have fun experimenting with LoadFlow!** 🎉
