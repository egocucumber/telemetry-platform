package processor

import (
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/egocucumber/telemetry-platform/internal/domain"
)

func newTestEngine(rules ...domain.Rule) *Engine {
	e := NewEngine(slog.Default())
	n := 0
	e.newID = func() string { n++; return "alert-" + strconv.Itoa(n) }
	e.Reload(rules)
	return e
}

func sample(dev string, v float64, at time.Time) domain.Measurement {
	return domain.Measurement{DeviceID: dev, Metric: "temperature", Value: v, TS: at}
}

func TestEngineFiresAfterForDuration(t *testing.T) {
	rule := domain.Rule{ID: 1, Name: "hot", Metric: "temperature", Op: domain.OpGT, Threshold: 80, For: 10 * time.Second, Severity: domain.SeverityCritical, Enabled: true}
	e := newTestEngine(rule)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	if got := e.Evaluate(sample("d1", 90, t0)); len(got) != 0 {
		t.Fatalf("should not fire on first sample, got %v", got)
	}
	if got := e.Evaluate(sample("d1", 91, t0.Add(5*time.Second))); len(got) != 0 {
		t.Fatalf("should not fire before For elapsed, got %v", got)
	}
	got := e.Evaluate(sample("d1", 92, t0.Add(10*time.Second)))
	if len(got) != 1 || got[0].State != domain.AlertFiring {
		t.Fatalf("expected one firing alert, got %v", got)
	}
	if got[0].Value != 92 || got[0].RuleID != 1 || got[0].DeviceID != "d1" {
		t.Errorf("alert payload wrong: %+v", got[0])
	}

	if got := e.Evaluate(sample("d1", 95, t0.Add(20*time.Second))); len(got) != 0 {
		t.Fatalf("must not re-fire while already firing, got %v", got)
	}

	res := e.Evaluate(sample("d1", 70, t0.Add(30*time.Second)))
	if len(res) != 1 || res[0].State != domain.AlertResolved || res[0].ID != "alert-1" {
		t.Fatalf("expected resolved alert-1, got %v", res)
	}
}

func TestEngineSpikeDoesNotFire(t *testing.T) {
	rule := domain.Rule{ID: 1, Metric: "temperature", Op: domain.OpGT, Threshold: 80, For: 10 * time.Second, Enabled: true}
	e := newTestEngine(rule)
	t0 := time.Now()

	e.Evaluate(sample("d1", 90, t0))
	e.Evaluate(sample("d1", 70, t0.Add(4*time.Second)))
	got := e.Evaluate(sample("d1", 90, t0.Add(12*time.Second)))
	if len(got) != 0 {
		t.Fatalf("holding window must reset after the condition breaks, got %v", got)
	}
}

func TestEngineForZeroFiresImmediately(t *testing.T) {
	rule := domain.Rule{ID: 1, Metric: "temperature", Op: domain.OpLT, Threshold: 5, Enabled: true}
	e := newTestEngine(rule)
	got := e.Evaluate(sample("d1", 1, time.Now()))
	if len(got) != 1 || got[0].State != domain.AlertFiring {
		t.Fatalf("expected immediate firing, got %v", got)
	}
}

func TestEngineIsolatesDevices(t *testing.T) {
	rule := domain.Rule{ID: 1, Metric: "temperature", Op: domain.OpGT, Threshold: 80, Enabled: true}
	e := newTestEngine(rule)
	now := time.Now()
	e.Evaluate(sample("d1", 90, now))
	if got := e.Evaluate(sample("d2", 90, now)); len(got) != 1 {
		t.Fatalf("d2 should fire independently of d1, got %v", got)
	}
}

func TestEngineReloadDropsRemovedRuleState(t *testing.T) {
	rule := domain.Rule{ID: 1, Metric: "temperature", Op: domain.OpGT, Threshold: 80, Enabled: true}
	e := newTestEngine(rule)
	e.Evaluate(sample("d1", 90, time.Now()))
	e.Reload(nil)
	if e.RuleCount() != 0 || len(e.state) != 0 {
		t.Fatalf("state must be dropped with the rule, rules=%d state=%d", e.RuleCount(), len(e.state))
	}
	if got := e.Evaluate(sample("d1", 70, time.Now())); len(got) != 0 {
		t.Fatalf("no rules, no alerts, got %v", got)
	}
}

func BenchmarkEngineEvaluate(b *testing.B) {
	rules := make([]domain.Rule, 0, 50)
	for i := range 50 {
		rules = append(rules, domain.Rule{ID: int64(i), Metric: "temperature", Op: domain.OpGT, Threshold: 1000, Enabled: true})
	}
	e := newTestEngine(rules...)
	m := sample("d1", 20, time.Now())
	b.ReportAllocs()
	for b.Loop() {
		e.Evaluate(m)
	}
}
