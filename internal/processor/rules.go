package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/egocucumber/telemetry-platform/internal/domain"
)

type RuleSource interface {
	Load(ctx context.Context) ([]domain.Rule, error)
}

type PGRuleSource struct{ Pool *pgxpool.Pool }

func (s PGRuleSource) Load(ctx context.Context) ([]domain.Rule, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, name, COALESCE(device_id, ''), metric, op, threshold, for_seconds, severity, enabled
		FROM rules WHERE enabled`)
	if err != nil {
		return nil, fmt.Errorf("query rules: %w", err)
	}
	defer rows.Close()

	var out []domain.Rule
	for rows.Next() {
		var r domain.Rule
		var forSec int
		if err := rows.Scan(&r.ID, &r.Name, &r.DeviceID, &r.Metric, &r.Op, &r.Threshold, &forSec, &r.Severity, &r.Enabled); err != nil {
			return nil, err
		}
		r.For = time.Duration(forSec) * time.Second
		out = append(out, r)
	}
	return out, rows.Err()
}

var _ RuleSource = PGRuleSource{}

type Engine struct {
	mu    sync.RWMutex
	rules []domain.Rule

	byMetric map[string][]domain.Rule
	state    map[stateKey]*ruleState
	log      *slog.Logger
	newID    func() string
}

type stateKey struct {
	ruleID   int64
	deviceID string
}

type ruleState struct {
	holdingSince time.Time
	firing       bool
	alertID      string
}

func NewEngine(log *slog.Logger) *Engine {
	return &Engine{
		byMetric: map[string][]domain.Rule{},
		state:    map[stateKey]*ruleState{},
		log:      log,
		newID:    uuid.NewString,
	}
}

func (e *Engine) Reload(rules []domain.Rule) {
	byMetric := make(map[string][]domain.Rule, len(rules))
	alive := make(map[int64]struct{}, len(rules))
	for _, r := range rules {
		if !r.Enabled {
			continue
		}
		byMetric[r.Metric] = append(byMetric[r.Metric], r)
		alive[r.ID] = struct{}{}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = rules
	e.byMetric = byMetric
	for k := range e.state {
		if _, ok := alive[k.ruleID]; !ok {
			delete(e.state, k)
		}
	}
}

func (e *Engine) RuleCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.rules)
}

func (e *Engine) Evaluate(m domain.Measurement) []domain.Alert {
	e.mu.RLock()
	rules := e.byMetric[m.Metric]
	e.mu.RUnlock()
	if len(rules) == 0 {
		return nil
	}

	var alerts []domain.Alert
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range rules {
		if !r.Matches(m.DeviceID, m.Metric) {
			continue
		}
		key := stateKey{ruleID: r.ID, deviceID: m.DeviceID}
		st := e.state[key]
		if st == nil {
			st = &ruleState{}
			e.state[key] = st
		}

		if !r.Holds(m.Value) {
			st.holdingSince = time.Time{}
			if st.firing {
				st.firing = false
				alerts = append(alerts, e.alert(r, m, st.alertID, domain.AlertResolved))
				st.alertID = ""
			}
			continue
		}

		if st.holdingSince.IsZero() {
			st.holdingSince = m.TS
		}
		if st.firing || m.TS.Sub(st.holdingSince) < r.For {
			continue
		}
		st.firing = true
		st.alertID = e.newID()
		alerts = append(alerts, e.alert(r, m, st.alertID, domain.AlertFiring))
	}
	return alerts
}

func (e *Engine) alert(r domain.Rule, m domain.Measurement, id string, state domain.AlertState) domain.Alert {
	return domain.Alert{
		ID:         id,
		RuleID:     r.ID,
		RuleName:   r.Name,
		DeviceID:   m.DeviceID,
		Metric:     m.Metric,
		Value:      m.Value,
		Threshold:  r.Threshold,
		Severity:   r.Severity,
		State:      state,
		OccurredAt: m.TS,
	}
}
