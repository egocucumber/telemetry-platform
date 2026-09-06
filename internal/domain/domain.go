package domain

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

var (
	ErrEmptyDeviceID = errors.New("device_id is required")
	ErrEmptyMetric   = errors.New("metric is required")
	ErrBadTimestamp  = errors.New("timestamp is missing or outside the accepted window")
	ErrBadValue      = errors.New("value must be a finite number")
)

const MaxClockSkew = 24 * time.Hour

type Measurement struct {
	DeviceID string
	Metric   string
	Value    float64
	TS       time.Time
	Seq      uint64
	Tags     map[string]string
}

func (m Measurement) Validate(now time.Time) error {
	if strings.TrimSpace(m.DeviceID) == "" {
		return ErrEmptyDeviceID
	}
	if strings.TrimSpace(m.Metric) == "" {
		return ErrEmptyMetric
	}
	if m.TS.IsZero() {
		return ErrBadTimestamp
	}
	if d := now.Sub(m.TS); d > MaxClockSkew || d < -MaxClockSkew {
		return fmt.Errorf("%w: ts=%s now=%s", ErrBadTimestamp, m.TS.Format(time.RFC3339), now.Format(time.RFC3339))
	}
	if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
		return ErrBadValue
	}
	return nil
}

func (m Measurement) IdempotencyKey() string {
	return fmt.Sprintf("%s:%d", m.DeviceID, m.Seq)
}

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type Op string

const (
	OpGT Op = "gt"
	OpGE Op = "ge"
	OpLT Op = "lt"
	OpLE Op = "le"
)

type Rule struct {
	ID        int64
	Name      string
	DeviceID  string
	Metric    string
	Op        Op
	Threshold float64
	For       time.Duration
	Severity  Severity
	Enabled   bool
}

func (r Rule) Matches(deviceID, metric string) bool {
	if !r.Enabled || r.Metric != metric {
		return false
	}
	return r.DeviceID == "" || r.DeviceID == deviceID
}

func (r Rule) Holds(v float64) bool {
	switch r.Op {
	case OpGT:
		return v > r.Threshold
	case OpGE:
		return v >= r.Threshold
	case OpLT:
		return v < r.Threshold
	case OpLE:
		return v <= r.Threshold
	}
	return false
}

type AlertState string

const (
	AlertFiring   AlertState = "firing"
	AlertResolved AlertState = "resolved"
)

type Alert struct {
	ID         string
	RuleID     int64
	RuleName   string
	DeviceID   string
	Metric     string
	Value      float64
	Threshold  float64
	Severity   Severity
	State      AlertState
	OccurredAt time.Time
}

type Device struct {
	ID        string
	GatewayID string
	Name      string
	LastSeen  time.Time
	CreatedAt time.Time
}

type Gateway struct {
	ID   string
	Name string
}
