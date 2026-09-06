package query

import (
	"errors"
	"testing"
	"time"

	"github.com/egocucumber/telemetry-platform/internal/domain"
)

func TestValidateRule(t *testing.T) {
	good := RuleJSON{Name: "hot", Metric: "temperature", Op: "gt", Threshold: 80, ForSeconds: 10, Severity: "critical", Enabled: true}

	tests := []struct {
		name   string
		mutate func(*RuleJSON)
		ok     bool
	}{
		{"valid", func(*RuleJSON) {}, true},
		{"missing name", func(r *RuleJSON) { r.Name = "" }, false},
		{"bad op", func(r *RuleJSON) { r.Op = "==" }, false},
		{"bad severity", func(r *RuleJSON) { r.Severity = "meh" }, false},
		{"negative for", func(r *RuleJSON) { r.ForSeconds = -1 }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := good
			tt.mutate(&in)
			rl, err := validateRule(in)
			if tt.ok != (err == nil) {
				t.Fatalf("ok=%v err=%v", tt.ok, err)
			}
			if err != nil && !errors.Is(err, errBadRequest) {
				t.Fatalf("validation errors must map to 400, got %v", err)
			}
			if err == nil && (rl.For != 10*time.Second || rl.Op != domain.OpGT) {
				t.Fatalf("conversion wrong: %+v", rl)
			}
		})
	}
}
