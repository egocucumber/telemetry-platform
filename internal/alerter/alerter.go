package alerter

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	telemetryv1 "github.com/egocucumber/telemetry-platform/gen/go/telemetry/v1"
	"github.com/egocucumber/telemetry-platform/internal/domain"
	"github.com/egocucumber/telemetry-platform/internal/kafkax"
	"github.com/egocucumber/telemetry-platform/internal/observability"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

var (
	metricProcessed  = observability.Counter("alerter_alerts_total", "Alerts by outcome.", "outcome")
	metricNotify     = observability.Counter("alerter_notifications_total", "Notification attempts.", "notifier", "outcome")
	metricNotifyTime = observability.Histogram("alerter_notify_seconds", "Notifier latency.", "notifier")
)

type Alerter struct {
	cl        *kafkax.Client
	pool      *pgxpool.Pool
	rdb       *redis.Client
	notifiers []Notifier
	cooldown  time.Duration
	log       *slog.Logger
}

func New(cl *kafkax.Client, pool *pgxpool.Pool, rdb *redis.Client, notifiers []Notifier, cooldown time.Duration, log *slog.Logger) *Alerter {
	return &Alerter{cl: cl, pool: pool, rdb: rdb, notifiers: notifiers, cooldown: cooldown, log: log}
}

func (a *Alerter) Handle(ctx context.Context, recs []*kgo.Record) error {
	alerts := make([]domain.Alert, 0, len(recs))
	for _, r := range recs {
		var ev telemetryv1.AlertEvent
		if err := proto.Unmarshal(r.Value, &ev); err != nil {
			a.log.ErrorContext(ctx, "undecodable alert, skipping", "offset", r.Offset, "err", err)
			metricProcessed.WithLabelValues("undecodable").Inc()
			continue
		}
		alerts = append(alerts, fromProto(&ev))
	}
	if len(alerts) == 0 {
		return nil
	}

	if err := a.persist(ctx, alerts); err != nil {
		return err
	}

	for i, al := range alerts {
		rctx, span := a.cl.Tracer.WithProcessSpan(recs[i])
		a.notify(rctx, al)
		span.End()
	}
	return nil
}

func (a *Alerter) persist(ctx context.Context, alerts []domain.Alert) error {
	batch := &pgx.Batch{}
	for _, al := range alerts {
		switch al.State {
		case domain.AlertFiring:
			batch.Queue(`
				INSERT INTO alerts (id, rule_id, rule_name, device_id, metric, value, threshold, severity, state, fired_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'firing', $9)
				ON CONFLICT (id) DO NOTHING`,
				al.ID, al.RuleID, al.RuleName, al.DeviceID, al.Metric, al.Value, al.Threshold, al.Severity, al.OccurredAt)
		case domain.AlertResolved:
			batch.Queue(`
				INSERT INTO alerts (id, rule_id, rule_name, device_id, metric, value, threshold, severity, state, fired_at, resolved_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'resolved', $9, $9)
				ON CONFLICT (id) DO UPDATE SET state = 'resolved', resolved_at = EXCLUDED.resolved_at`,
				al.ID, al.RuleID, al.RuleName, al.DeviceID, al.Metric, al.Value, al.Threshold, al.Severity, al.OccurredAt)
		}
	}
	res := a.pool.SendBatch(ctx, batch)
	defer func() { _ = res.Close() }()
	for range alerts {
		if _, err := res.Exec(); err != nil {
			return fmt.Errorf("persist alerts: %w", err)
		}
	}
	return nil
}

func (a *Alerter) notify(ctx context.Context, al domain.Alert) {
	if al.State == domain.AlertFiring {
		ok, suppressed, err := redisx.AcquireCooldown(ctx, a.rdb, redisx.KeyCooldown(al.RuleID, al.DeviceID), a.cooldown)
		if err != nil {
			a.log.ErrorContext(ctx, "cooldown check failed, notifying anyway", "err", err)
		} else if !ok {
			metricProcessed.WithLabelValues("suppressed").Inc()
			a.log.InfoContext(ctx, "alert suppressed by cooldown", "rule", al.RuleName, "device", al.DeviceID, "suppressed", suppressed)
			return
		}
	}
	metricProcessed.WithLabelValues("notified").Inc()

	for _, n := range a.notifiers {
		start := time.Now()
		err := retry(ctx, 3, 500*time.Millisecond, func() error { return n.Notify(ctx, al) })
		metricNotifyTime.WithLabelValues(n.Name()).Observe(time.Since(start).Seconds())
		if err != nil {
			metricNotify.WithLabelValues(n.Name(), "error").Inc()
			a.log.ErrorContext(ctx, "notify failed", "notifier", n.Name(), "alert", al.ID, "err", err)
			continue
		}
		metricNotify.WithLabelValues(n.Name(), "ok").Inc()
	}
	if _, err := a.pool.Exec(ctx, `UPDATE alerts SET notified_at = now() WHERE id = $1 AND notified_at IS NULL`, al.ID); err != nil {
		a.log.WarnContext(ctx, "mark notified", "alert", al.ID, "err", err)
	}
}

func retry(ctx context.Context, attempts int, base time.Duration, fn func() error) error {
	var err error
	for i := range attempts {
		if err = fn(); err == nil {
			return nil
		}
		if i == attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(base << i):
		}
	}
	return err
}

func fromProto(ev *telemetryv1.AlertEvent) domain.Alert {
	a := domain.Alert{
		ID:        ev.GetAlertId(),
		RuleID:    ev.GetRuleId(),
		RuleName:  ev.GetRuleName(),
		DeviceID:  ev.GetDeviceId(),
		Metric:    ev.GetMetric(),
		Value:     ev.GetValue(),
		Threshold: ev.GetThreshold(),
		State:     domain.AlertFiring,
	}
	if ev.GetState() == telemetryv1.AlertState_ALERT_STATE_RESOLVED {
		a.State = domain.AlertResolved
	}
	switch ev.GetSeverity() {
	case telemetryv1.Severity_SEVERITY_CRITICAL:
		a.Severity = domain.SeverityCritical
	case telemetryv1.Severity_SEVERITY_WARNING:
		a.Severity = domain.SeverityWarning
	default:
		a.Severity = domain.SeverityInfo
	}
	if ev.GetOccurredAt() != nil {
		a.OccurredAt = ev.GetOccurredAt().AsTime()
	}
	return a
}
