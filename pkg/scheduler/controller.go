package scheduler

import (
	"context"
	"sync"
	"time"
)

// 每个stream通过registry对应一个strategy，straregy
type Controller struct {
	provider MetricsProvider
	applier  Applier
	bindings *BindingRegistry

	registry *StrategyRegistry // 新增：策略注册表
	policies PolicyProvider    // 新增：每个 stream 的 policy

	tick     time.Duration
	cooldown time.Duration

	sink EventSink

	mu        sync.Mutex
	lastApply map[string]time.Time
}

type ControllerOptions struct {
	Tick     time.Duration
	Cooldown time.Duration
}

func NewController(
	provider MetricsProvider,
	registry *StrategyRegistry,
	policies PolicyProvider,
	applier Applier,
	bindings *BindingRegistry,
	sink EventSink,
	opts ControllerOptions,
) *Controller {
	tick := opts.Tick
	if tick <= 0 {
		tick = 2 * time.Second
	}
	cd := opts.Cooldown
	if cd <= 0 {
		cd = 10 * time.Second
	}
	if sink == nil {
		sink = LogSink{}
	}
	if policies == nil {
		policies = DefaultPolicyProvider{}
	}
	if registry == nil {
		// 这里宁愿 panic，避免运行时 silent fail
		panic("nil strategy registry")
	}

	return &Controller{
		provider:  provider,
		registry:  registry,
		policies:  policies,
		applier:   applier,
		bindings:  bindings,
		tick:      tick,
		cooldown:  cd,
		sink:      sink,
		lastApply: make(map[string]time.Time),
	}
}

func (c *Controller) Run(ctx context.Context) {
	tk := time.NewTicker(c.tick)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			snap, err := c.provider.Sample(ctx)
			if err != nil {
				// sample 失败通常不是“策略拒绝”，直接日志即可
				c.sink.Emit(ctx, Event{
					Type:    EventApplyFailed,
					At:      time.Now(),
					Stream:  "",
					Plan:    nil,
					Message: "sample error",
					Err:     err,
				})
				continue
			}

			streams := c.bindings.Streams()
			for _, stream := range streams {
				policy := c.policies.Get(stream)
				if !policy.Enabled {
					c.sink.Emit(ctx, Event{
						Type:    EventPlanRejected,
						At:      time.Now(),
						Stream:  stream,
						Plan:    nil,
						Message: "disabled by policy",
					})
					continue
				}

				stratName := policy.StrategyName
				if stratName == "" {
					stratName = "pressure_rebalance" // 兜底，或让 DefaultPolicyProvider 保证不为空
				}

				strat, ok := c.registry.Get(stratName)
				if !ok || strat == nil {
					c.sink.Emit(ctx, Event{
						Type:    EventPlanRejected,
						At:      time.Now(),
						Stream:  stream,
						Plan:    nil,
						Message: "unknown strategy: " + stratName,
					})
					continue
				}

				poolNames, weights, ok := c.bindings.Get(stream)
				if !ok {
					c.sink.Emit(ctx, Event{
						Type:    EventPlanRejected,
						At:      time.Now(),
						Stream:  stream,
						Plan:    nil,
						Message: "no binding",
					})
					continue
				}

				plan, ok := strat.DecideStream(snap, stream, poolNames, weights, policy)
				if !ok || plan == nil {
					// 这里不建议每 tick 都 emit "no plan"（会刷屏）
					continue
				}

				// 计划生成事件（B2.5）
				c.sink.Emit(ctx, Event{
					Type:    EventPlanGenerated,
					At:      time.Now(),
					Stream:  stream,
					Plan:    plan,
					Message: "plan generated",
				})

				// per-stream cooldown 覆盖
				cd := policy.Cooldown
				if cd <= 0 {
					cd = c.cooldown
				}
				if c.inCooldown(stream, cd) {
					c.sink.Emit(ctx, Event{
						Type:    EventPlanRejected,
						At:      time.Now(),
						Stream:  stream,
						Plan:    plan,
						Message: "in cooldown",
					})
					continue
				}

				if err := c.applier.Apply(ctx, *plan); err != nil {
					c.sink.Emit(ctx, Event{
						Type:    EventApplyFailed,
						At:      time.Now(),
						Stream:  stream,
						Plan:    plan,
						Message: "apply failed",
						Err:     err,
					})
					continue
				}

				c.markApplied(stream)
				c.sink.Emit(ctx, Event{
					Type:    EventPlanApplied,
					At:      time.Now(),
					Stream:  stream,
					Plan:    plan,
					Message: "applied successfully",
				})
			}

		case <-ctx.Done():
			return
		}
	}
}

func (c *Controller) inCooldown(stream string, cd time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.lastApply[stream]
	if !ok {
		return false
	}
	return time.Since(t) < cd
}

func (c *Controller) markApplied(stream string) {
	c.mu.Lock()
	c.lastApply[stream] = time.Now()
	c.mu.Unlock()
}
