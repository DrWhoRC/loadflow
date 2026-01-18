**中文版在下方(Chinese Version Below)**
# LoadFlow

**Status:** Still developing  
**Fully-functioning in:** v0.6.0

---

## Introduction

This framework proposes a new paradigm for concurrent scheduling — a load-balanced consumption strategy based on **multi-coroutine pools** and **multi-data streams**.

Its core idea is to realize **self-balancing concurrency** through dynamic organization of coroutine pools and adaptive routing of data streams.


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

