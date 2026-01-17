package scheduler

import (
	"context"
	"log"
	"time"
)

type EventType string

const (
	EventPlanGenerated EventType = "plan_generated"
	EventPlanApplied   EventType = "plan_applied"
	EventPlanRejected  EventType = "plan_rejected" // 被 cooldown/校验挡住
	EventApplyFailed   EventType = "apply_failed"
	// B2.3 预留：
	EventRollback EventType = "rollback"
)

type Event struct {
	Type    EventType
	At      time.Time
	Stream  string
	Plan    *Plan  // 某些事件可以没有 plan
	Message string // 额外信息
	Err     error  // apply failed 等场景
}

type EventSink interface {
	Emit(ctx context.Context, ev Event)
}

// 默认实现：打日志（你也可以后面接 Prometheus）
type LogSink struct{}

func (s LogSink) Emit(ctx context.Context, ev Event) {
	// 这里只放最关键字段，别刷屏；细节可以 Message/Plan 里拼
	// 你可以按你项目的日志风格调整
	if ev.Plan != nil {
		p := ev.Plan
		log.Printf("[EVENT] type=%s at=%s stream=%s reason=%s old=%v new=%v move=%s->%s deltaW=%d metric=%s from=%.3f to=%.3f delta=%.3f thr=%.3f msg=%s err=%v",
			ev.Type, ev.At.Format(time.RFC3339), ev.Stream, p.Reason,
			p.OldWeights, p.NewWeights,
			p.Change.FromPool, p.Change.ToPool, p.Change.DeltaW,
			p.Trigger.Metric, p.Trigger.FromValue, p.Trigger.ToValue, p.Trigger.Delta, p.Trigger.Threshold,
			ev.Message, ev.Err,
		)
		return
	}
}
