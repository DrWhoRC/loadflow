# LoadFlow 演示程序

[English](README.md) | 简体中文

## 概述

这是一个交互式演示程序，展示了 LoadFlow 的**基于压力的自动再平衡机制**。该演示创建了一个故意不平衡的路由配置，并展示系统如何通过反馈驱动的迭代自动检测和纠正问题。

## 核心演示场景

### 问题设置
- **3 个工作池**，处理能力不同：
  - `pool_fast`: 5 个工作线程，100ms 延迟 → **50 msg/s**
  - `pool_medium`: 3 个工作线程，200ms 延迟 → **15 msg/s**  
  - `pool_slow`: 2 个工作线程，300ms 延迟 → **6.6 msg/s**

- **3 个消息流**：
  - `stream_a`: 20 msg/s（背景负载）
  - `stream_b`: 20 msg/s（背景负载）
  - `stream_c`: 100 msg/s（**主演示流** - 超过系统容量）

- **故意的不平衡初始权重** `[1, 1, 8]`：
  - 将 stream_c 的 **80% 流量路由到最慢的池**
  - 预测结果：pool_slow 快速饱和 → 触发再平衡

### 观察到的行为

演示展示了 6 个增量式再平衡循环：

| 时间 | 权重 [快:中:慢] | pool_slow % | 队列深度 | 状态 |
|------|----------------|-------------|----------|------|
| 0s | 1:1:8 | 80% | 0 → 35 | 🟥 快速累积 |
| 1.0s | 2:1:7 | 70% | 35 → 227 | 🟡 调整 #1 |
| 8.0s | 3:1:6 | 60% | 227 → 256 | 🟡 调整 #2（等待压力阈值）|
| 11.0s | 4:1:5 | 50% | 256 (饱和) | 🔴 调整 #3 |
| 14.5s | 5:1:4 | 40% | 256 → 255 | 🟡 调整 #4 |
| 19.0s | 6:1:3 | 30% | 255 → 241 | 🟢 调整 #5（开始下降）|
| 23.0s | 7:1:2 | 20% | 241 → 91 | ✅ 调整 #6（快速恢复）|
| 29.5s | 7:1:2 | 20% | 91 | 🟢 稳定 |

**关键结果**：
- ✅ 将 pool_slow 从 **80% → 20%**（减少 4 倍）
- ✅ 队列从饱和（256/256）恢复到健康水平（91/256）
- ✅ 压力从 37.00 降至 1.56（低于 3.0 阈值）
- ✅ **无需人工干预即达到稳定平衡**

## 快速开始

### 前置要求
- Go 1.21 或更高版本
- Docker 和 Docker Compose（可选，用于 Prometheus/Grafana）

### 运行演示

```bash
# 1. 进入演示目录
cd /Users/castle/代码/loadflow/cmd/demo-channel

# 2. 运行演示（默认 30 秒）
go run main.go

# 3. 在另一个终端查看实时指标
curl http://localhost:9090/metrics
```

### 预期输出

```
[Demo] Starting LoadFlow demo (duration=30s)...
[Pool] Created: pool_fast (workers=5, queue=1024)
[Pool] Created: pool_medium (workers=3, queue=512)
[Pool] Created: pool_slow (workers=2, queue=256)
[Router] Bound: stream_c -> [pool_fast pool_medium pool_slow] (weights=[1 1 8])
[Scheduler] Controller created (tick=1s, cooldown=3s)

═══════════════════════════════════════════════════════════════
Time    pool_fast       pool_medium     pool_slow
───────────────────────────────────────────────────────────────
0.5s    0/1024          0/512           15/256
1.0s    0/1024          0/512           35/256

[EVENT] type=plan_applied stream=stream_c from=pool_slow to=pool_fast 
        deltaW=1 metric=pressure delta=37.00 threshold=3.00

1.5s    0/1024          0/512           48/256
...
23.0s   0/1024          0/512           241/256  ← 开始恢复
29.5s   0/1024          0/512           91/256   ← 稳定！
```

## 配置参数

### 为可见性调优的关键参数

在 `config.yaml` 中：

```yaml
scheduler:
  tick: 1s                    # 频繁监控（生产环境：5-10s）
  default_cooldown: 5s
  policies:
    stream_c:
      enabled: true
      strategy: "pressure_rebalance"
      cooldown: 3s             # 快速响应（生产环境：10s）
      params:
        minPressureDelta: 3.0  # 触发阈值
        maxStep: 1             # 小步长用于可见性（生产环境：2-3）

routing:
  stream_c:
    pools: ["pool_fast", "pool_medium", "pool_slow"]
    initial_weights: [1, 1, 8]  # 故意不平衡！
```

### 为什么这些设置？

| 参数 | 演示值 | 生产建议 | 原因 |
|------|--------|----------|------|
| `maxStep` | 1 | 2-3 | 展示渐进过程 vs. 更快收敛 |
| `cooldown` | 3s | 10s | 快速迭代 vs. 防止振荡 |
| `tick` | 1s | 5-10s | 快速检测 vs. CPU 效率 |
| `initial_weights` | [1,1,8] | 均衡 | 创建可见问题 vs. 稳定启动 |

## 监控

### Prometheus 指标

演示在 `:9090/metrics` 上暴露以下指标：

**队列指标**：
- `loadflow_pool_queue_depth` - 当前队列大小
- `loadflow_pool_queue_capacity` - 最大队列大小
- `loadflow_pool_pressure` - 计算的压力值（0-100）

**再平衡指标**：
- `loadflow_rebalance_plan_total{result="applied|rejected"}` - 计划计数
- `loadflow_rebalance_step_size` - 最后一步的权重变化
- `loadflow_router_weight{stream,pool}` - 当前路由权重

**流量指标**：
- `loadflow_stream_messages_in_total` - 每个流接收的消息
- `loadflow_router_routed_total{stream,pool}` - 路由决策

### 使用 Grafana（可选）

```bash
# 启动 Prometheus + Grafana
cd prometheus
docker-compose up -d

# 访问 Grafana: http://localhost:3000
# 默认凭证: admin/admin
```

导入预配置的仪表板：
1. 前往 Dashboards → Import
2. 上传 `grafana/dashboard.json`
3. 观察实时再平衡！

## 理解再平衡机制

### 双门触发系统

再平衡只有在以下情况下发生：

1. **冷却期已过**: `time_since_last >= cooldown`（防止振荡）
2. **压力超过阈值**: `pressure_delta >= minPressureDelta`（确保有意义的变化）

### 为什么再平衡 #2 用了 7 秒？

```
1.0s: 应用调整 #1 → 冷却期开始
      |
4.0s: |--- 冷却期到期 ✅
      |    但 pressure_delta=1.74 < 3.00 ❌
5.0s: |    delta=2.14 < 3.00 ❌
6.0s: |    delta=2.61 < 3.00 ❌  
7.0s: |    delta=2.98 < 3.00 ❌
      |
8.0s: V--- delta=3.41 > 3.00 ✅ → 应用调整 #2
```

**两个条件都必须满足** - 这防止了过早的调整，同时仍允许必要时的快速响应。

### 压力计算

```
Pressure = (queue_depth / queue_capacity) × 100

例如: 227/256 = 88.67% 压力
```

当池之间的压力差异超过 `minPressureDelta` 时，系统将权重从高压池转移到低压池。

## 详细分析

完整的时间线分析，包括：
- 每个再平衡循环的逐步分解
- 权重变化可视化
- 队列增长/下降率分析
- 设计权衡讨论

请参阅：
- [详细结果分析（中文）](demo_outcomes_detailed_zh.md)
- [Detailed Results Analysis (English)](demo_outcomes_detailed.md)

## 实验

尝试调整 `config.yaml` 中的参数以观察不同的行为：

### 实验 1：更激进的再平衡
```yaml
params:
  maxStep: 3              # 从 1 → 3
  cooldown: 1s            # 从 3s → 1s
```
**预期**：更快收敛（约 10 秒 vs. 23 秒），但步骤更少

### 实验 2：更保守的阈值
```yaml
params:
  minPressureDelta: 5.0   # 从 3.0 → 5.0
```
**预期**：更少的再平衡事件，但队列可能增长更多

### 实验 3：均衡的初始权重
```yaml
routing:
  stream_c:
    initial_weights: [4, 2, 1]  # 从 [1, 1, 8] → [4, 2, 1]
```
**预期**：无需再平衡 - 系统从一开始就保持稳定

### 实验 4：极端负载
```yaml
streams:
  - name: stream_c
    rate: 200              # 从 100 → 200
```
**预期**：即使经过再平衡，仍会饱和（超过总容量 71.6 msg/s）

## 架构亮点

该演示展示了几个高级 LoadFlow 特性：

1. **指标感知调度**：实时 Prometheus 指标用于决策
2. **反馈驱动控制**：调整基于观察到的系统状态，而非静态规则
3. **无状态路由器**：权重可以动态更改而不会中断流量
4. **安全机制**：冷却期 + 阈值防止不稳定
5. **可观察性优先**：每个决策都被记录和导出

## 故障排除

### 演示立即退出
```bash
# 检查是否有其他进程使用端口 9090
lsof -i :9090
kill -9 <PID>
```

### 未看到再平衡事件
- 确认 `stream_c` 的初始权重为 `[1, 1, 8]`（不平衡）
- 检查 `scheduler.policies.stream_c.enabled: true`
- 验证 `minPressureDelta: 3.0`（不要太高）

### 队列从不下降
- 正常！使用 `maxStep: 1`，在 22 秒之前队列不会下降
- 增加 `maxStep` 到 2-3 以更快收敛
- 或者减少 `stream_c.rate` 从 100 → 80

## 接下来做什么？

1. **阅读详细分析**：[demo_outcomes_detailed_zh.md](demo_outcomes_detailed_zh.md)
2. **探索代码**：查看 `main.go` 中的指标仪器化
3. **检查策略**：`pkg/scheduler/strategy_pressure_rebalance.go`
4. **自定义演示**：修改 `config.yaml` 并重新运行
5. **集成到你的系统**：将 LoadFlow 包用于生产工作负载

## 生产使用注意事项

将此演示转换为生产系统时：

✅ **保留**：
- 压力再平衡策略（经过验证！）
- Prometheus 指标导出
- 双门触发系统（冷却期 + 阈值）

⚙️ **调整**：
- `maxStep: 1 → 2-3`（更快收敛）
- `cooldown: 3s → 10s`（防止振荡）
- `tick: 1s → 5-10s`（降低 CPU 使用）
- `initial_weights`（均衡启动）

🔧 **添加**：
- 基于密钥的粘性路由（如果需要会话亲和性）
- 多个策略（压力 + 延迟 + 错误率）
- 告警（在调度器故障时）
- 自动缩放（添加/移除工作线程）

## 许可证

本演示是 LoadFlow 项目的一部分。参见根目录 LICENSE。

## 支持

问题或问题？
- 📖 查看 [主项目 README](../../README.md)
- 🐛 提交 issue 到 GitHub
- 📧 联系维护者

---

**享受演示吧！** 观察 LoadFlow 如何将混乱转变为平衡。 🎯
