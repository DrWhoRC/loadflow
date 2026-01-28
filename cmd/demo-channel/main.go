package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/DrWhoRC/loadflow/pkg/consumer"
	"github.com/DrWhoRC/loadflow/pkg/flow/router"
	"github.com/DrWhoRC/loadflow/pkg/flow/source"
	"github.com/DrWhoRC/loadflow/pkg/metrics"
	"github.com/DrWhoRC/loadflow/pkg/pool"
	"github.com/DrWhoRC/loadflow/pkg/runtime"
	"github.com/DrWhoRC/loadflow/pkg/scheduler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

// ============================================================================
// 配置结构定义
// ============================================================================

type DemoConfig struct {
	Demo      DemoParams             `yaml:"demo"`
	Pools     []PoolConfig           `yaml:"pools"`
	Streams   []StreamConfig         `yaml:"streams"`
	Routing   map[string]RouteConfig `yaml:"routing"`
	Scheduler SchedulerConfig        `yaml:"scheduler"`
}

type DemoParams struct {
	Duration    time.Duration `yaml:"duration"`
	MetricsPort int           `yaml:"metrics_port"`
}

type PoolConfig struct {
	Name        string          `yaml:"name"`
	Workers     int             `yaml:"workers"`
	Queue       int             `yaml:"queue"`
	BaseLatency time.Duration   `yaml:"base_latency"`
	BadPhase    *BadPhaseConfig `yaml:"bad_phase,omitempty"`
}

type BadPhaseConfig struct {
	Start     time.Duration `yaml:"start"`
	End       time.Duration `yaml:"end"`
	Latency   time.Duration `yaml:"latency"`
	ErrorRate float64       `yaml:"error_rate"`
}

type StreamConfig struct {
	Name       string        `yaml:"name"`
	Type       string        `yaml:"type"`
	Rate       int           `yaml:"rate"`
	KeyEnabled bool          `yaml:"key_enabled"`
	KeyCount   int           `yaml:"key_count,omitempty"`
	Phases     []PhaseConfig `yaml:"phases,omitempty"`
}

type PhaseConfig struct {
	Start    time.Duration `yaml:"start"`
	Duration time.Duration `yaml:"duration"`
	BiasPool string        `yaml:"bias_pool"`
}

type RouteConfig struct {
	Pools          []string `yaml:"pools"`
	InitialWeights []int    `yaml:"initial_weights"`
}

type SchedulerConfig struct {
	Tick            time.Duration                `yaml:"tick"`
	DefaultCooldown time.Duration                `yaml:"default_cooldown"`
	Policies        map[string]PolicyConfigEntry `yaml:"policies"`
}

type PolicyConfigEntry struct {
	Enabled  bool               `yaml:"enabled"`
	Strategy string             `yaml:"strategy,omitempty"`
	Cooldown time.Duration      `yaml:"cooldown,omitempty"`
	Params   map[string]float64 `yaml:"params,omitempty"`
}

// ============================================================================
// Prometheus 指标定义
// ============================================================================

var (
	streamInCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "loadflow",
			Subsystem: "stream",
			Name:      "messages_in_total",
			Help:      "Total messages received by each stream",
		},
		[]string{"stream"},
	)

	errorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "loadflow",
			Subsystem: "handler",
			Name:      "errors_total",
			Help:      "Total processing errors by stream",
		},
		[]string{"stream"},
	)

	violationCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "loadflow",
			Subsystem: "router",
			Name:      "routing_violations_total",
			Help:      "Total routing violations (same key routed to different pools)",
		},
		[]string{"stream"},
	)

	planCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "loadflow",
			Subsystem: "rebalance",
			Name:      "plan_total",
			Help:      "Total rebalance plans by result",
		},
		[]string{"stream", "result"},
	)

	stepGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "loadflow",
			Subsystem: "rebalance",
			Name:      "step_size",
			Help:      "Size of the last rebalance step",
		},
		[]string{"stream"},
	)

	weightGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "loadflow",
			Subsystem: "router",
			Name:      "weight",
			Help:      "Current routing weight for each stream and pool",
		},
		[]string{"stream", "pool"},
	)

	// 新增：追踪每个 stream 路由到每个 pool 的消息数
	routedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "loadflow",
			Subsystem: "router",
			Name:      "routed_total",
			Help:      "Total messages routed from each stream to each pool",
		},
		[]string{"stream", "pool"},
	)
)

func initMetrics(reg *prometheus.Registry) {
	reg.MustRegister(streamInCounter)
	reg.MustRegister(errorCounter)
	reg.MustRegister(violationCounter)
	reg.MustRegister(planCounter)
	reg.MustRegister(stepGauge)
	reg.MustRegister(weightGauge)
	reg.MustRegister(routedCounter)
}

// ============================================================================
// 配置加载
// ============================================================================

func loadConfig(path string) (*DemoConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg DemoConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &cfg, nil
}

// ============================================================================
// 增强型组件
// ============================================================================

// Envelope 是消息的标准格式
type Envelope struct {
	Stream     string          `json:"stream"`
	Key        *string         `json:"key,omitempty"`
	TargetPool string          `json:"target_pool,omitempty"` // 用于 phase bias
	Payload    json.RawMessage `json:"payload"`
}

// InstrumentedHandler 带指标埋点的 handler
type InstrumentedHandler struct{}

func NewInstrumentedHandler(cfg *DemoConfig, startTime time.Time) *InstrumentedHandler {
	return &InstrumentedHandler{}
}

func (h *InstrumentedHandler) Handle(msg []byte) error {
	// 解析消息
	var env Envelope
	if err := json.Unmarshal(msg, &env); err != nil {
		return fmt.Errorf("unmarshal envelope: %w", err)
	}

	// 统一使用基础延迟，让压力差异由 pool 的处理能力决定
	time.Sleep(30 * time.Millisecond)

	// Handler 处理成功（具体的 per-pool 指标由 metrics.PrometheusExporter 提供）
	return nil
}

// KeyTracker 跟踪 key 的路由情况，验证 sticky 正确性
type KeyTracker struct {
	mu      sync.Mutex
	mapping map[string]map[string]string // stream -> key -> pool
}

func NewKeyTracker() *KeyTracker {
	return &KeyTracker{
		mapping: make(map[string]map[string]string),
	}
}

func (kt *KeyTracker) Track(stream, key, pool string) {
	kt.mu.Lock()
	defer kt.mu.Unlock()

	if _, ok := kt.mapping[stream]; !ok {
		kt.mapping[stream] = make(map[string]string)
	}

	if firstPool, exists := kt.mapping[stream][key]; exists {
		if firstPool != pool {
			// 违反 sticky 语义！
			violationCounter.WithLabelValues(stream).Inc()
			log.Printf("[VIOLATION] stream=%s key=%s: first_pool=%s, current_pool=%s",
				stream, key, firstPool, pool)
		}
	} else {
		kt.mapping[stream][key] = pool
	}
}

// MessageGenerator 根据配置生成消息
type MessageGenerator struct {
	cfg       StreamConfig
	startTime time.Time
	tracker   *KeyTracker
}

func NewMessageGenerator(cfg StreamConfig, startTime time.Time, tracker *KeyTracker) *MessageGenerator {
	return &MessageGenerator{
		cfg:       cfg,
		startTime: startTime,
		tracker:   tracker,
	}
}

func (mg *MessageGenerator) Start(ctx context.Context, ch chan<- []byte) {
	ticker := time.NewTicker(time.Second / time.Duration(mg.cfg.Rate))
	defer ticker.Stop()

	msgID := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			env := mg.generateMessage(msgID)
			msg, _ := json.Marshal(env)
			ch <- msg
			streamInCounter.WithLabelValues(mg.cfg.Name).Inc()
			msgID++
		}
	}
}

func (mg *MessageGenerator) generateMessage(msgID int) Envelope {
	env := Envelope{
		Stream:  mg.cfg.Name,
		Payload: json.RawMessage(fmt.Sprintf(`{"id":%d}`, msgID)),
	}

	// Key 生成
	if mg.cfg.KeyEnabled && mg.cfg.KeyCount > 0 {
		key := fmt.Sprintf("key_%d", msgID%mg.cfg.KeyCount)
		env.Key = &key
	}

	return env
}

// PrometheusEventSink 将 scheduler 事件转换为 Prometheus 指标
type PrometheusEventSink struct{}

func (s *PrometheusEventSink) Emit(ctx context.Context, ev scheduler.Event) {
	switch ev.Type {
	case scheduler.EventPlanGenerated:
		planCounter.WithLabelValues(ev.Stream, "generated").Inc()
	case scheduler.EventPlanApplied:
		planCounter.WithLabelValues(ev.Stream, "applied").Inc()
		if ev.Plan != nil {
			stepGauge.WithLabelValues(ev.Stream).Set(float64(ev.Plan.Change.DeltaW))
		}
	case scheduler.EventPlanRejected:
		planCounter.WithLabelValues(ev.Stream, "rejected").Inc()
	case scheduler.EventApplyFailed:
		planCounter.WithLabelValues(ev.Stream, "failed").Inc()
	}

	// 同时输出日志
	if ev.Plan != nil {
		p := ev.Plan
		log.Printf("[EVENT] type=%s stream=%s reason=%s from=%s to=%s deltaW=%d metric=%s delta=%.2f threshold=%.2f msg=%s",
			ev.Type, ev.Stream, p.Reason,
			p.Change.FromPool, p.Change.ToPool, p.Change.DeltaW,
			p.Trigger.Metric, p.Trigger.Delta, p.Trigger.Threshold,
			ev.Message)
	} else {
		log.Printf("[EVENT] type=%s stream=%s msg=%s", ev.Type, ev.Stream, ev.Message)
	}
}

// InstrumentedApplier 包装 applier 并更新权重指标
type InstrumentedApplier struct {
	inner    scheduler.Applier
	bindings *scheduler.BindingRegistry
}

func NewInstrumentedApplier(inner scheduler.Applier, bindings *scheduler.BindingRegistry) *InstrumentedApplier {
	return &InstrumentedApplier{
		inner:    inner,
		bindings: bindings,
	}
}

func (a *InstrumentedApplier) Apply(ctx context.Context, plan scheduler.Plan) error {
	err := a.inner.Apply(ctx, plan)
	if err == nil {
		// 更新权重指标
		poolNames, _, ok := a.bindings.Get(plan.Stream)
		if ok {
			for i, pName := range poolNames {
				if i < len(plan.NewWeights) {
					weightGauge.WithLabelValues(plan.Stream, pName).Set(float64(plan.NewWeights[i]))
				}
			}
		}
	}
	return err
}

// InstrumentedRouter 包装 router 并记录路由指标
type InstrumentedRouter struct {
	inner interface {
		router.KeyRouter
		router.MutableRouter
	}
}

func NewInstrumentedRouter(inner interface {
	router.KeyRouter
	router.MutableRouter
}) *InstrumentedRouter {
	return &InstrumentedRouter{inner: inner}
}

func (r *InstrumentedRouter) Route(srcName string) (pool.WorkerPool, bool) {
	p, ok := r.inner.Route(srcName)
	if ok && p != nil {
		routedCounter.WithLabelValues(srcName, p.Name()).Inc()
	}
	return p, ok
}

func (r *InstrumentedRouter) RouteWithKey(srcName string, key []byte) (pool.WorkerPool, bool) {
	p, ok := r.inner.RouteWithKey(srcName, key)
	if ok && p != nil {
		routedCounter.WithLabelValues(srcName, p.Name()).Inc()
	}
	return p, ok
}

func (r *InstrumentedRouter) Bind(srcName string, p pool.WorkerPool) error {
	return r.inner.Bind(srcName, p)
}

func (r *InstrumentedRouter) SetMany(srcName string, pools []pool.WorkerPool, weights []int) error {
	return r.inner.SetMany(srcName, pools, weights)
}

func (r *InstrumentedRouter) Snapshot() map[string]string {
	return r.inner.Snapshot()
}

// 确保实现了必要的接口
var _ router.KeyRouter = (*InstrumentedRouter)(nil)
var _ router.MutableRouter = (*InstrumentedRouter)(nil)

// ============================================================================
// 组件创建函数
// ============================================================================

func createPools(cfg *DemoConfig) map[string]pool.WorkerPool {
	pools := make(map[string]pool.WorkerPool)
	for _, pc := range cfg.Pools {
		p := pool.NewFixedPool(pc.Name, pc.Workers, pc.Queue)
		pools[pc.Name] = p
		log.Printf("[Pool] Created: %s (workers=%d, queue=%d)", pc.Name, pc.Workers, pc.Queue)
	}
	return pools
}

func createSourcesAndGenerators(
	ctx context.Context,
	cfg *DemoConfig,
	tracker *KeyTracker,
	startTime time.Time,
) map[string]source.Source {
	sources := make(map[string]source.Source)

	for _, sc := range cfg.Streams {
		ch := make(chan []byte, 10000) // 增加 Source Channel 缓冲（从 1024 → 10000）
		src := source.NewChanSource(sc.Name, ch)
		sources[sc.Name] = src

		// 启动消息生成器
		gen := NewMessageGenerator(sc, startTime, tracker)
		go gen.Start(ctx, ch)

		log.Printf("[Source] Created: %s (type=%s, rate=%d/s)", sc.Name, sc.Type, sc.Rate)
	}

	return sources
}

func createRouter(cfg *DemoConfig, pools map[string]pool.WorkerPool) router.MutableRouter {
	baseRouter := router.NewWeightedRR()

	for streamName, routeCfg := range cfg.Routing {
		poolList := make([]pool.WorkerPool, 0, len(routeCfg.Pools))
		for _, pName := range routeCfg.Pools {
			if p, ok := pools[pName]; ok {
				poolList = append(poolList, p)
			} else {
				log.Fatalf("[Router] Pool not found: %s", pName)
			}
		}

		if err := baseRouter.BindMany(streamName, poolList, routeCfg.InitialWeights); err != nil {
			log.Fatalf("[Router] Bind failed: %v", err)
		}

		// 初始化权重指标
		for i, pName := range routeCfg.Pools {
			if i < len(routeCfg.InitialWeights) {
				weightGauge.WithLabelValues(streamName, pName).Set(float64(routeCfg.InitialWeights[i]))
			}
		}

		log.Printf("[Router] Bound: %s -> %v (weights=%v)",
			streamName, routeCfg.Pools, routeCfg.InitialWeights)
	}

	// 包装为 InstrumentedRouter
	return NewInstrumentedRouter(baseRouter)
}

func setupScheduler(
	cfg *DemoConfig,
	rt runtime.Runtime,
	r router.MutableRouter,
	pools map[string]pool.WorkerPool,
) *scheduler.Controller {
	// 创建 metrics provider
	provider := scheduler.NewRuntimeMetricsProvider(rt.(scheduler.RuntimeWithMetrics))

	// 创建 strategy registry
	registry := scheduler.NewStrategyRegistry()
	registry.Register("pressure_rebalance", scheduler.NewPressureRebalanceStrategy())

	// 创建 policy provider
	policyMap := make(map[string]scheduler.Policy)
	for streamName, policyEntry := range cfg.Scheduler.Policies {
		policy := scheduler.Policy{
			Enabled:      policyEntry.Enabled,
			EnabledSet:   true,
			StrategyName: policyEntry.Strategy,
			Cooldown:     policyEntry.Cooldown,
			Params:       policyEntry.Params,
		}
		policyMap[streamName] = policy
		// 调试: 输出每个stream的cooldown配置
		if policyEntry.Enabled {
			log.Printf("[Scheduler] Policy for %s: cooldown=%v, strategy=%s",
				streamName, policyEntry.Cooldown, policyEntry.Strategy)
		}
	}

	policies := scheduler.StaticPolicyProvider{
		Default: scheduler.Policy{
			Enabled:    false,
			EnabledSet: true,
		},
		Per: policyMap,
	}

	// 创建 bindings
	bindings := scheduler.NewBindingRegistry()
	for streamName, routeCfg := range cfg.Routing {
		poolList := make([]pool.WorkerPool, 0, len(routeCfg.Pools))
		for _, pName := range routeCfg.Pools {
			poolList = append(poolList, pools[pName])
		}
		if err := bindings.Set(streamName, poolList, routeCfg.InitialWeights); err != nil {
			log.Fatalf("[Scheduler] Bindings.Set failed: %v", err)
		}
	}

	// 创建 event sink
	eventSink := &PrometheusEventSink{}

	// 创建 applier
	baseApplier := scheduler.NewRouterApplier(r, bindings)
	applier := NewInstrumentedApplier(baseApplier, bindings)

	// 创建 controller
	controller := scheduler.NewController(
		provider,
		registry,
		policies,
		applier,
		bindings,
		eventSink,
		scheduler.ControllerOptions{
			Tick:     cfg.Scheduler.Tick,
			Cooldown: cfg.Scheduler.DefaultCooldown,
		},
	)

	log.Printf("[Scheduler] Controller created (tick=%v, cooldown=%v)",
		cfg.Scheduler.Tick, cfg.Scheduler.DefaultCooldown)

	return controller
}

// ============================================================================
// 主函数
// ============================================================================

func main() {
	// 加载配置
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("[Demo] Starting LoadFlow demo (duration=%v)...", cfg.Demo.Duration)
	startTime := time.Now()

	// 创建 Prometheus registry 并注册指标
	reg := prometheus.NewRegistry()
	initMetrics(reg)

	// 创建协程池
	pools := createPools(cfg)

	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Demo.Duration)
	defer cancel()

	// 创建 key tracker
	tracker := NewKeyTracker()

	// 创建数据源并启动消息生成器
	sources := createSourcesAndGenerators(ctx, cfg, tracker, startTime)

	// 创建 router
	r := createRouter(cfg, pools)

	// 创建 handler
	handler := NewInstrumentedHandler(cfg, startTime)

	// 创建 runtime
	rt := runtime.New(consumer.Handler(handler.Handle))
	for _, src := range sources {
		if err := rt.RegisterSource(src); err != nil {
			log.Fatalf("RegisterSource failed: %v", err)
		}
	}
	for _, p := range pools {
		if err := rt.RegisterPool(p); err != nil {
			log.Fatalf("RegisterPool failed: %v", err)
		}
	}
	rt.UseRouter(r)

	// 创建 scheduler
	controller := setupScheduler(cfg, rt, r, pools)

	// 启动 runtime
	go func() {
		log.Println("[Runtime] Starting...")
		if err := rt.Start(ctx); err != nil {
			log.Printf("[Runtime] Error: %v", err)
		}
		log.Println("[Runtime] Stopped.")
	}()

	// 启动 pool metrics exporter
	poolExporter := metrics.NewPrometheusExporter(
		rt,
		reg,
		metrics.ExporterOptions{
			Namespace: "loadflow",
			Subsystem: "pool",
		},
	)
	go poolExporter.Start(ctx, time.Second)

	// 启动 scheduler controller
	go controller.Run(ctx)
	log.Println("[Scheduler] Controller started")

	// 启动 HTTP metrics server
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Demo.MetricsPort),
		Handler: mux,
	}

	go func() {
		log.Printf("[Metrics] Server listening on :%d", cfg.Demo.MetricsPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Metrics] Server error: %v", err)
		}
	}()

	log.Printf("[Demo] Running for %v...", cfg.Demo.Duration)

	// 启动指标打印 goroutine (每 0.5 秒)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		log.Println("═══════════════════════════════════════════════════════════════")
		log.Println("Time\tpool_fast\tpool_medium\tpool_slow")
		log.Println("───────────────────────────────────────────────────────────────")
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime)

				// 获取队列深度 (使用类型断言)
				var fastDepth, mediumDepth, slowDepth int
				var fastCap, mediumCap, slowCap int

				if p, ok := pools["pool_fast"].(*pool.FixedPool); ok {
					fastDepth = p.GetQueueDepth()
					fastCap = p.GetQueueCapacity()
				}
				if p, ok := pools["pool_medium"].(*pool.FixedPool); ok {
					mediumDepth = p.GetQueueDepth()
					mediumCap = p.GetQueueCapacity()
				}
				if p, ok := pools["pool_slow"].(*pool.FixedPool); ok {
					slowDepth = p.GetQueueDepth()
					slowCap = p.GetQueueCapacity()
				}

				log.Printf("%.1fs\t%d/%d\t\t%d/%d\t\t%d/%d",
					elapsed.Seconds(),
					fastDepth, fastCap,
					mediumDepth, mediumCap,
					slowDepth, slowCap)
			}
		}
	}()

	// 等待完成
	<-ctx.Done()

	log.Println("[Demo] Initiating graceful shutdown...")

	// 优雅关闭 HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Metrics] Shutdown error: %v", err)
	}

	// 停止 runtime
	if err := rt.Stop(context.Background()); err != nil {
		log.Printf("[Runtime] Stop error: %v", err)
	}

	log.Println("[Demo] Shutdown complete.")
	log.Printf("[Demo] Total duration: %v", time.Since(startTime))
}
