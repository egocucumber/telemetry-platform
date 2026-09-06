package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestMeasurementValidate(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	base := Measurement{DeviceID: "d1", Metric: "temp", Value: 1, TS: now, Seq: 1}

	tests := []struct {
		name    string
		mutate  func(*Measurement)
		wantErr error
	}{
		{"ok", func(*Measurement) {}, nil},
		{"empty device", func(m *Measurement) { m.DeviceID = " " }, ErrEmptyDeviceID},
		{"empty metric", func(m *Measurement) { m.Metric = "" }, ErrEmptyMetric},
		{"zero ts", func(m *Measurement) { m.TS = time.Time{} }, ErrBadTimestamp},
		{"too old", func(m *Measurement) { m.TS = now.Add(-25 * time.Hour) }, ErrBadTimestamp},
		{"in future", func(m *Measurement) { m.TS = now.Add(25 * time.Hour) }, ErrBadTimestamp},
		{"nan", func(m *Measurement) { m.Value = math.NaN() }, ErrBadValue},
		{"inf", func(m *Measurement) { m.Value = math.Inf(1) }, ErrBadValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := base
			tt.mutate(&m)
			err := m.Validate(now)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRuleHolds(t *testing.T) {
	tests := []struct {
		op   Op
		v    float64
		want bool
	}{
		{OpGT, 11, true}, {OpGT, 10, false},
		{OpGE, 10, true}, {OpGE, 9.9, false},
		{OpLT, 9, true}, {OpLT, 10, false},
		{OpLE, 10, true}, {OpLE, 10.1, false},
		{Op("bogus"), 100, false},
	}
	for _, tt := range tests {
		r := Rule{Op: tt.op, Threshold: 10}
		if got := r.Holds(tt.v); got != tt.want {
			t.Errorf("op=%s v=%v: got %v want %v", tt.op, tt.v, got, tt.want)
		}
	}
}

func TestRuleMatches(t *testing.T) {
	wildcard := Rule{Metric: "temp", Enabled: true}
	single := Rule{Metric: "temp", DeviceID: "d1", Enabled: true}
	disabled := Rule{Metric: "temp", Enabled: false}

	if !wildcard.Matches("d9", "temp") || wildcard.Matches("d9", "rpm") {
		t.Error("wildcard rule should match any device with the same metric")
	}
	if !single.Matches("d1", "temp") || single.Matches("d2", "temp") {
		t.Error("device rule should match only its device")
	}
	if disabled.Matches("d1", "temp") {
		t.Error("disabled rule must never match")
	}
}
