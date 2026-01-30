# 策略二：动态再平衡（Dynamic Rebalancing）

## 概述

动态再平衡是 LoadFlow 框架的**自适应优化策略**，它在**策略一（无 Key 路由）的基础上**，通过持续监控系统指标，自动调整路由权重，实现负载的动态平衡。

**核心价值**：将"静态配置"升级为"自动驾驶"，让系统根据实时状态自我优化。

---

## 为什么需要动态再平衡？

### 策略一的局限性

```yaml
# 策略一：静态权重配置
routing:
  stream_c:
    pools: [pool_fast, pool_medium, pool_slow]
    weights: [1, 1, 8]  # 固定不变
```

**问题场景**：
```
初始配置：80% 流量给 pool_slow

实际运行：
T1: pool_slow 队列 35/256   (14%)   ← 正常
T2: pool_slow 队列 227/256  (89%)   ← 压力增大
T3: pool_slow 队列 256/256  (100%)  ← 饱和！

但权重仍然是 [1, 1, 8]，流量持续涌入 pool_slow
```

**后果**：
- 队列持续饱和，消息积压
- 延迟急剧增加
- 可能导致消息丢失或超时
- 而 pool_fast 资源闲置（利用率不足）

---

## 核心原理

### 反馈控制循环

```
监控指标 → 计算压力差 → 判断是否调整 → 应用新权重 → 观察效果
   ↑                                                    ↓
   └────────────────────── 持续循环 ←─────────────────────┘
```

### 关键组件

#### 1. Controller（调度控制器）
```go
type Controller struct {
    tick      time.Duration  // 监控周期（例如 1s）
    cooldown  time.Duration  // 冷却时间（例如 3s）
    policies  map[string]Policy
}
```

**职责**：
- 周期性采样指标
- 调用策略计算
- 应用调整决策
- 防止频繁抖动

#### 2. Strategy（再平衡策略）
```go
type Strategy interface {
    DecideStream(ctx, streamName, pools, weights, metrics) Plan
}
```

**内置策略**：
- **PressureRebalanceStrategy**：基于队列压力
- **LatencyRebalanceStrategy**：基于处理延迟（未来）
- **ErrorRateRebalanceStrategy**：基于错误率（未来）

#### 3. Policy（策略配置）
```yaml
scheduler:
  policies:
    stream_c:
      enabled: true
      strategy: "pressure_rebalance"
      cooldown: 3s
      params:
        minPressureDelta: 3.0  # 触发阈值
        maxStep: 1             # 最大调整步长
```

---

## 基于压力的再平衡算法

### 压力计算

```go
Pressure = (QueueDepth / QueueCapacity) × 100

示例：
pool_slow: 227/256 → 88.67% 压力
pool_fast: 0/1024  → 0% 压力

压力差 = 88.67 - 0 = 88.67
```

### 决策逻辑

```go
if 压力差 >= minPressureDelta {  // 例如 3.0
    if 冷却期已过 {
        从高压 pool 转移权重到低压 pool
    }
}
```

### 调整步长

```
当前权重: [1, 1, 8]
最大步长: maxStep = 1

调整：
- pool_slow 权重 -1 → 7
- pool_fast 权重 +1 → 2

新权重: [2, 1, 7]
```

**为什么小步长？**
- 避免过度调整（overreact）
- 观察效果后再决定下一步
- 系统更加稳定可控

---

## 完整案例分析

### Demo 运行记录（23秒内6次再平衡）

```
初始状态：
权重: [1, 1, 8]
stream_c: 100 msg/s
pool_slow 容量: 6.6 msg/s
→ 80 msg/s 涌入 pool_slow（超载12倍！）
```

#### 时间线

**T0 (0s)：启动**
```
权重: [1, 1, 8]
pool_slow: 0/256
```

**T1 (1.0s)：首次再平衡**
```
队列: 35/256
压力: 37.00 >> 阈值(3.0)
冷却: ✅ 首次调整

调整: [1,1,8] → [2,1,7]
效果: pool_slow 从 80% → 70% 流量
```

**T2 (8.0s)：第二次再平衡**
```
队列: 227/256 (89%)
压力: 3.41 > 阈值
冷却: ✅ 已过3秒

调整: [2,1,7] → [3,1,6]
效果: pool_slow 降至 60% 流量
```

**为什么等了7秒？**
```
双门机制：
1. 冷却期: 1.0s + 3s = 4.0s（允许调整） ✅
2. 压力阈值: 需要 delta >= 3.0

4.0s: delta=1.74 ❌
5.0s: delta=2.14 ❌
6.0s: delta=2.61 ❌
7.0s: delta=2.98 ❌（差一点！）
8.0s: delta=3.41 ✅ → 触发
```

**T3-T6 (11s-23s)：持续优化**
```
T3 (11.0s): [3,1,6] → [4,1,5]  队列: 256/256 (饱和)
T4 (14.5s): [4,1,5] → [5,1,4]  队列: 256/256
T5 (19.0s): [5,1,4] → [6,1,3]  队列: 256/256
T6 (23.0s): [6,1,3] → [7,1,2]  队列: 241/256 ← 开始下降！
```

**T7 (29.5s)：稳定**
```
权重: [7, 1, 2]
pool_slow: 20% 流量（20 msg/s）
队列: 91/256 (36%)
压力差: 1.56 < 3.0 → 不再调整

✅ 达到平衡状态
```

---

## 双门保护机制

### 为什么需要两道门？

**只有冷却期的问题**：
```
每3秒调整一次，无论压力大小
→ 可能在压力很小时也调整
→ 浪费资源，引入噪音
```

**只有阈值的问题**：
```
压力略超阈值就立即调整
→ 可能过于敏感，频繁调整
→ 系统抖动
```

**双门机制（两者都满足）**：
```
条件1: time_since_last >= cooldown  （时间门）
条件2: pressure_delta >= threshold   （压力门）

只有同时满足才调整
```

### 参数调优

#### Cooldown（冷却期）
```
Demo 值: 3s（快速响应）
生产建议: 10s（稳定优先）

过短：系统抖动，权重频繁变化
过长：响应慢，压力累积过高
```

#### MinPressureDelta（压力阈值）
```
Demo 值: 3.0
含义: 两个 pool 压力差超过3%才调整

过小（如1.0）：对噪音敏感，频繁调整
过大（如10.0）：不够敏感，问题发现晚
```

#### MaxStep（最大步长）
```
Demo 值: 1（渐进式）
生产建议: 2-3（平衡）

权重总和=10，步长1 → 每次调整10%流量
权重总和=10，步长3 → 每次调整30%流量

小步长：稳定但收敛慢（Demo用6步，23秒）
大步长：快速但可能过冲
```

---

## 优点

### 1. 自动化运维
```
无需人工：
- 监控队列深度
- 计算最优权重
- 执行配置变更

系统自动完成，7×24小时运行
```

### 2. 适应负载变化
```
场景：突发流量

12:00 - 正常流量 1000 msg/s
12:30 - 突发流量 5000 msg/s  ← 自动调整权重
13:00 - 恢复正常 1000 msg/s  ← 自动调回
```

### 3. 充分利用资源
```
避免：
- 某些 pool 过载，队列饱和
- 某些 pool 闲置，资源浪费

实现：
- 动态平衡，所有 pool 合理利用
```

### 4. 可观察性
```
每次调整都有事件日志：
[EVENT] type=plan_applied stream=stream_c 
        from=pool_slow to=pool_fast deltaW=1
        metric=pressure delta=3.41

便于：
- 事后分析
- 问题排查
- 性能调优
```

---

## 缺点

### 1. 引入复杂性
```
组件增加：
- Controller（调度器）
- Strategy（策略）
- Policy（配置）
- EventSink（事件记录）

需要理解：
- 冷却期机制
- 压力计算方法
- 调整逻辑
```

### 2. 参数调优挑战
```
需要根据业务特点调整：
- tick（监控频率）
- cooldown（冷却时间）
- minPressureDelta（触发阈值）
- maxStep（调整步长）

错误配置可能：
- 响应过慢
- 系统抖动
- 效果不佳
```

### 3. 无法处理极端情况
```
场景：总流量超过总容量

stream_c: 200 msg/s
总容量: 71.6 msg/s

即使完美分配权重，仍然会饱和
→ 需要水平扩展（增加 pool）
```

### 4. 延迟收敛
```
从检测到解决需要时间：

T0: 发现压力 → T3: 第一次调整 → T23: 达到平衡

期间：队列可能持续饱和
建议：配合告警机制
```

---

## 与其他策略的关系

### 基于策略一
```
策略一：提供基础路由能力
策略二：在策略一的基础上动态优化权重

依赖关系：
策略二必须在策略一之上运行
```

### 与策略三互斥
```
策略二：动态调整权重 → 同一 key 可能路由到不同 pool
策略三：固定 key→pool 映射 → 保证顺序性

冲突：
策略二的权重变化会破坏策略三的顺序保证

解决：
强有序场景下禁用策略二：
  policies:
    stream_ordered:
      enabled: false  # 禁用动态再平衡
```

---

## 实际案例

### 案例 1：LoadFlow Demo

```yaml
配置：
  tick: 1s
  cooldown: 3s
  minPressureDelta: 3.0
  maxStep: 1

结果：
- 6次调整，23秒达到平衡
- 权重从 [1,1,8] → [7,1,2]
- 队列从饱和(256) → 健康(91)
- 压力从 37.00 → 1.56
```

**教训**：
- maxStep=1 太保守，收敛慢
- 生产环境建议 maxStep=2-3

### 案例 2：电商大促

```
场景：双11流量高峰

00:00 - 预热，1000 msg/s
00:01 - 开抢，10000 msg/s ← 突发10倍

策略二自动：
- 检测 pool 压力飙升
- 快速调整权重分配
- 将流量导向高性能 pool
- 避免系统崩溃

01:00 - 高峰过后，恢复正常配置
```

---

## 使用指南

### 配置示例

```yaml
scheduler:
  tick: 2s              # 每2秒检查一次
  default_cooldown: 10s # 默认冷却期
  
  policies:
    stream_a:
      enabled: true
      strategy: "pressure_rebalance"
      cooldown: 5s      # 覆盖默认值
      params:
        minPressureDelta: 3.0
        maxStep: 2
        
    stream_b:
      enabled: false    # 禁用自动调整
```

### 监控建议

```go
// 订阅调整事件
sink.Subscribe(func(e Event) {
    if e.Type == "plan_applied" {
        log.Info("Rebalance occurred",
            "stream", e.StreamName,
            "from", e.FromPool,
            "to", e.ToPool,
            "delta", e.PressureDelta)
        
        // 发送告警（如果频繁调整）
        if rebalanceCount > 10 {
            alert("Stream unstable")
        }
    }
})
```

### 最佳实践

#### 1. 分阶段启用
```
第一周：观察模式（enabled: false）
  - 收集指标
  - 模拟调整决策
  
第二周：保守启用（maxStep: 1, cooldown: 15s）
  - 观察实际效果
  - 调优参数
  
第三周：正常运行（maxStep: 2, cooldown: 10s）
```

#### 2. 根据业务特性调参
```
低延迟业务（实时交易）：
  tick: 1s
  cooldown: 5s
  minPressureDelta: 2.0

高吞吐业务（日志处理）：
  tick: 5s
  cooldown: 30s
  minPressureDelta: 5.0
```

#### 3. 设置告警
```
触发条件：
- 单个 stream 10分钟内调整超过5次
- 任何 pool 队列深度 > 90% 持续1分钟
- 压力差持续 > 阈值2倍

行动：
- 人工介入检查
- 考虑水平扩展
```

---

## 总结

**策略二（动态再平衡）是策略一的智能升级**：
- ✅ 自动化运维，减少人工干预
- ✅ 适应负载变化，充分利用资源
- ✅ 通过双门机制保证稳定性
- ❌ 增加系统复杂度，需要参数调优
- ❌ 与策略三（强有序）互斥
- 🎯 适合流量波动大、对延迟敏感的场景

**设计哲学**：
> 让系统像人一样思考：观察 → 分析 → 决策 → 执行 → 反馈

**下一步**：
- 了解基础路由 → 参见 [策略一：无 Key 路由](strategy_1_keyless_routing_zh.md)
- 了解有序处理 → 参见 [策略三：强有序](strategy_3_strongly_ordered_zh.md)
