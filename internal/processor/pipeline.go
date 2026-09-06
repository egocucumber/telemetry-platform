package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	telemetryv1 "github.com/egocucumber/telemetry-platform/gen/go/telemetry/v1"
	"github.com/egocucumber/telemetry-platform/internal/domain"
	"github.com/egocucumber/telemetry-platform/internal/kafkax"
	"github.com/egocucumber/telemetry-platform/internal/observability"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

var (
	metricRecords   = observability.Counter("processor_records_total", "Consumed records by outcome.", "outcome")
	metricAlerts    = observability.Counter("processor_alerts_total", "Alerts emitted by state.", "state")
	metricBatchTime = observability.Histogram("processor_batch_seconds", "End-to-end batch processing time.", "stage")
	metricBatchSize = observability.Gauge("processor_batch_size", "Records in the last batch.")
	metricLag       = observability.Histogram("processor_event_age_seconds", "Age of samples when processed (ingest → processor).")
	metricRules     = observability.Gauge("processor_rules_active", "Active rules loaded.")
)

type Pipeline struct {
	cl        *kafkax.Client
	pool      *pgxpool.Pool
	rdb       *redis.Client
	engine    *Engine
	agg       *Aggregator
	rules     RuleSource
	log       *slog.Logger
	tracer    trace.Tracer
	latestTTL time.Duration

	mu      sync.Mutex
	pending []*kgo.Record
}

func NewPipeline(cl *kafkax.Client, pool *pgxpool.Pool, rdb *redis.Client, rules RuleSource, log *slog.Logger, windowSize, latestTTL time.Duration) *Pipeline {
	return &Pipeline{
		cl:        cl,
		pool:      pool,
		rdb:       rdb,
		engine:    NewEngine(log),
		agg:       NewAggregator(windowSize),
		rules:     rules,
		log:       log,
		tracer:    otel.Tracer("processor"),
		latestTTL: latestTTL,
	}
}

func (p *Pipeline) ReloadRules(ctx context.Context) error {
	rules, err := p.rules.Load(ctx)
	if err != nil {
		return err
	}
	p.engine.Reload(rules)
	metricRules.WithLabelValues().Set(float64(len(rules)))
	p.log.InfoContext(ctx, "rules loaded", "count", len(rules))
	return nil
}

func (p *Pipeline) WatchRules(ctx context.Context, every time.Duration) error {
	sub := p.rdb.Subscribe(ctx, redisx.ChannelRulesChanged)
	defer func() { _ = sub.Close() }()
	ch := sub.Channel()

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ch:
		case <-ticker.C:
		}
		if err := p.ReloadRules(ctx); err != nil && ctx.Err() == nil {
			p.log.ErrorContext(ctx, "reload rules", "err", err)
		}
	}
}

func (p *Pipeline) EvictStale(ctx context.Context, every, maxAge time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := p.agg.Evict(time.Now().Add(-maxAge)); n > 0 {
				p.log.InfoContext(ctx, "evicted stale windows", "count", n)
			}
		}
	}
}

type point struct {
	ts       time.Time
	deviceID string
	metric   string
	value    float64
	seq      int64
}

func (p *Pipeline) Handle(ctx context.Context, recs []*kgo.Record) error {
	start := time.Now()
	metricBatchSize.WithLabelValues().Set(float64(len(recs)))

	ctx, batchSpan := p.tracer.Start(ctx, "processor.batch",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.Int("batch.size", len(recs))),
	)
	defer batchSpan.End()

	if err := p.flushPending(ctx); err != nil {
		return err
	}

	points := make([]point, 0, len(recs))
	latest := map[string]map[string]redisx.LatestValue{}
	windows := map[string]map[string]redisx.WindowValue{}
	var alerts []domain.Alert
	var dlq []*kgo.Record

	for i, r := range recs {
		rctx, span := p.cl.Tracer.WithProcessSpan(r)
		if i == 0 {
			batchSpan.AddLink(trace.LinkFromContext(rctx))
		}
		var ev telemetryv1.TelemetryEvent
		if err := proto.Unmarshal(r.Value, &ev); err != nil || ev.GetMeasurement() == nil {
			span.SetStatus(codes.Error, "undecodable record")
			span.End()
			dlq = append(dlq, dlqRecord(r, "unmarshal: "+errString(err)))
			metricRecords.WithLabelValues("dlq").Inc()
			continue
		}
		m := toDomain(ev.GetMeasurement())
		span.SetAttributes(attribute.String("device.id", m.DeviceID), attribute.String("metric", m.Metric))
		if ev.GetIngestedAt() != nil {
			metricLag.WithLabelValues().Observe(time.Since(ev.GetIngestedAt().AsTime()).Seconds())
		}

		points = append(points, point{ts: m.TS, deviceID: m.DeviceID, metric: m.Metric, value: m.Value, seq: int64(m.Seq)}) //nolint:gosec // seq fits

		if cur, ok := latest[m.DeviceID][m.Metric]; !ok || m.TS.After(cur.TS) {
			if latest[m.DeviceID] == nil {
				latest[m.DeviceID] = map[string]redisx.LatestValue{}
			}
			latest[m.DeviceID][m.Metric] = redisx.NewLatestValue(m.Value, m.TS)
		}

		if closed, ok := p.agg.Add(m.DeviceID, m.Metric, m.Value, m.TS); ok {
			if windows[closed.DeviceID] == nil {
				windows[closed.DeviceID] = map[string]redisx.WindowValue{}
			}
			windows[closed.DeviceID][closed.Metric] = redisx.WindowValue{
				Bucket: closed.Bucket, Avg: closed.Stats.Avg(), Min: closed.Stats.Min, Max: closed.Stats.Max, Count: closed.Stats.Count,
			}
		}

		alerts = append(alerts, p.engine.Evaluate(m)...)
		metricRecords.WithLabelValues("ok").Inc()
		span.End()
	}
	metricBatchTime.WithLabelValues("decode").Observe(time.Since(start).Seconds())

	t := time.Now()
	if err := p.insertPoints(ctx, points); err != nil {
		return fmt.Errorf("timescale insert: %w", err)
	}
	metricBatchTime.WithLabelValues("timescale").Observe(time.Since(t).Seconds())

	t = time.Now()
	if err := redisx.SetLatest(ctx, p.rdb, latest, p.latestTTL); err != nil {
		return err
	}
	if err := redisx.SetWindows(ctx, p.rdb, windows, p.latestTTL); err != nil {
		return err
	}
	metricBatchTime.WithLabelValues("redis").Observe(time.Since(t).Seconds())

	t = time.Now()
	out := make([]*kgo.Record, 0, len(alerts)+len(dlq))
	for _, a := range alerts {
		out = append(out, alertRecord(ctx, a))
		metricAlerts.WithLabelValues(string(a.State)).Inc()
		p.log.InfoContext(ctx, "alert", "state", a.State, "rule", a.RuleName, "device", a.DeviceID, "value", a.Value)
	}
	out = append(out, dlq...)
	if len(out) > 0 {
		if err := p.cl.ProduceSync(ctx, out...).FirstErr(); err != nil {
			p.mu.Lock()
			p.pending = out
			p.mu.Unlock()
			return fmt.Errorf("produce events: %w", err)
		}
	}
	metricBatchTime.WithLabelValues("produce").Observe(time.Since(t).Seconds())
	metricBatchTime.WithLabelValues("total").Observe(time.Since(start).Seconds())
	return nil
}

func (p *Pipeline) flushPending(ctx context.Context) error {
	p.mu.Lock()
	pending := p.pending
	p.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	if err := p.cl.ProduceSync(ctx, pending...).FirstErr(); err != nil {
		return fmt.Errorf("flush pending events: %w", err)
	}
	p.mu.Lock()
	p.pending = nil
	p.mu.Unlock()
	return nil
}

func (p *Pipeline) insertPoints(ctx context.Context, pts []point) error {
	if len(pts) == 0 {
		return nil
	}
	ts := make([]time.Time, len(pts))
	dev := make([]string, len(pts))
	met := make([]string, len(pts))
	val := make([]float64, len(pts))
	seq := make([]int64, len(pts))
	for i, pt := range pts {
		ts[i], dev[i], met[i], val[i], seq[i] = pt.ts, pt.deviceID, pt.metric, pt.value, pt.seq
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO measurements (ts, device_id, metric, value, seq)
		SELECT * FROM unnest($1::timestamptz[], $2::text[], $3::text[], $4::float8[], $5::bigint[])
		ON CONFLICT (device_id, metric, ts) DO NOTHING`,
		ts, dev, met, val, seq)
	return err
}

func alertRecord(ctx context.Context, a domain.Alert) *kgo.Record {
	ev := &telemetryv1.AlertEvent{
		AlertId:    a.ID,
		RuleId:     a.RuleID,
		RuleName:   a.RuleName,
		DeviceId:   a.DeviceID,
		Metric:     a.Metric,
		Value:      a.Value,
		Threshold:  a.Threshold,
		Severity:   toProtoSeverity(a.Severity),
		State:      toProtoState(a.State),
		OccurredAt: timestamppb.New(a.OccurredAt),
	}
	b, _ := proto.Marshal(ev)
	return &kgo.Record{
		Topic:   kafkax.TopicAlerts,
		Key:     []byte(a.DeviceID),
		Value:   b,
		Context: ctx,
		Headers: []kgo.RecordHeader{{Key: "state", Value: []byte(a.State)}},
	}
}

func dlqRecord(r *kgo.Record, reason string) *kgo.Record {
	return &kgo.Record{
		Topic: kafkax.TopicDLQ,
		Key:   r.Key,
		Value: r.Value,
		Headers: append(r.Headers,
			kgo.RecordHeader{Key: "dlq-reason", Value: []byte(reason)},
			kgo.RecordHeader{Key: "dlq-source-topic", Value: []byte(r.Topic)},
			kgo.RecordHeader{Key: "dlq-source-offset", Value: []byte(fmt.Sprintf("%d:%d", r.Partition, r.Offset))},
		),
	}
}

func toDomain(pm *telemetryv1.Measurement) domain.Measurement {
	m := domain.Measurement{
		DeviceID: pm.GetDeviceId(),
		Metric:   pm.GetMetric(),
		Value:    pm.GetValue(),
		Seq:      pm.GetSeq(),
		Tags:     pm.GetTags(),
	}
	if pm.GetTs() != nil {
		m.TS = pm.GetTs().AsTime()
	}
	return m
}

func toProtoSeverity(s domain.Severity) telemetryv1.Severity {
	switch s {
	case domain.SeverityInfo:
		return telemetryv1.Severity_SEVERITY_INFO
	case domain.SeverityWarning:
		return telemetryv1.Severity_SEVERITY_WARNING
	case domain.SeverityCritical:
		return telemetryv1.Severity_SEVERITY_CRITICAL
	}
	return telemetryv1.Severity_SEVERITY_UNSPECIFIED
}

func toProtoState(s domain.AlertState) telemetryv1.AlertState {
	if s == domain.AlertResolved {
		return telemetryv1.AlertState_ALERT_STATE_RESOLVED
	}
	return telemetryv1.AlertState_ALERT_STATE_FIRING
}

func errString(err error) string {
	if err == nil {
		return "empty measurement"
	}
	return err.Error()
}
