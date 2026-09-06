package query

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/egocucumber/telemetry-platform/internal/domain"
)

var ErrNotFound = errors.New("not found")

type Repo struct{ pool *pgxpool.Pool }

func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) ListDevices(ctx context.Context, limit int) ([]domain.Device, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, gateway_id, name, COALESCE(last_seen, 'epoch'::timestamptz), created_at
		FROM devices ORDER BY last_seen DESC NULLS LAST LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Device
	for rows.Next() {
		var d domain.Device
		if err := rows.Scan(&d.ID, &d.GatewayID, &d.Name, &d.LastSeen, &d.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (r *Repo) GetDevice(ctx context.Context, id string) (domain.Device, error) {
	var d domain.Device
	err := r.pool.QueryRow(ctx, `
		SELECT id, gateway_id, name, COALESCE(last_seen, 'epoch'::timestamptz), created_at
		FROM devices WHERE id = $1`, id).Scan(&d.ID, &d.GatewayID, &d.Name, &d.LastSeen, &d.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Device{}, ErrNotFound
	}
	return d, err
}

type HistoryPoint struct {
	Bucket time.Time `json:"bucket"`
	Avg    float64   `json:"avg"`
	Min    float64   `json:"min"`
	Max    float64   `json:"max"`
	Count  int64     `json:"count"`
}

func (r *Repo) History(ctx context.Context, deviceID, metric string, from, to time.Time, step time.Duration) ([]HistoryPoint, error) {
	if step <= 0 {
		step = time.Minute
	}
	interval := fmt.Sprintf("%d seconds", int64(step.Seconds()))

	var sql string
	if step%time.Minute == 0 {
		sql = `
			SELECT time_bucket($4::interval, bucket) AS b,
			       sum(avg * count) / sum(count), min(min), max(max), sum(count)::bigint
			FROM measurements_1m
			WHERE device_id = $1 AND metric = $2 AND bucket >= $3 AND bucket < $5
			GROUP BY b ORDER BY b`
	} else {
		sql = `
			SELECT time_bucket($4::interval, ts) AS b,
			       avg(value), min(value), max(value), count(*)
			FROM measurements
			WHERE device_id = $1 AND metric = $2 AND ts >= $3 AND ts < $5
			GROUP BY b ORDER BY b`
	}
	rows, err := r.pool.Query(ctx, sql, deviceID, metric, from, interval, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []HistoryPoint{}
	for rows.Next() {
		var p HistoryPoint
		if err := rows.Scan(&p.Bucket, &p.Avg, &p.Min, &p.Max, &p.Count); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *Repo) ListRules(ctx context.Context) ([]domain.Rule, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, COALESCE(device_id, ''), metric, op, threshold, for_seconds, severity, enabled
		FROM rules ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Rule
	for rows.Next() {
		var rl domain.Rule
		var forSec int
		if err := rows.Scan(&rl.ID, &rl.Name, &rl.DeviceID, &rl.Metric, &rl.Op, &rl.Threshold, &forSec, &rl.Severity, &rl.Enabled); err != nil {
			return nil, err
		}
		rl.For = time.Duration(forSec) * time.Second
		out = append(out, rl)
	}
	return out, rows.Err()
}

func (r *Repo) CreateRule(ctx context.Context, rl domain.Rule) (domain.Rule, error) {
	var dev *string
	if rl.DeviceID != "" {
		dev = &rl.DeviceID
	}
	err := r.pool.QueryRow(ctx, `
		INSERT INTO rules (name, device_id, metric, op, threshold, for_seconds, severity, enabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
		rl.Name, dev, rl.Metric, rl.Op, rl.Threshold, int(rl.For.Seconds()), rl.Severity, rl.Enabled,
	).Scan(&rl.ID)
	return rl, err
}

func (r *Repo) DeleteRule(ctx context.Context, id int64) error {
	tag, err := r.pool.Exec(ctx, `DELETE FROM rules WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

type AlertRow struct {
	ID         string     `json:"id"`
	RuleID     int64      `json:"rule_id"`
	RuleName   string     `json:"rule_name"`
	DeviceID   string     `json:"device_id"`
	Metric     string     `json:"metric"`
	Value      float64    `json:"value"`
	Threshold  float64    `json:"threshold"`
	Severity   string     `json:"severity"`
	State      string     `json:"state"`
	FiredAt    time.Time  `json:"fired_at"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty"`
	NotifiedAt *time.Time `json:"notified_at,omitempty"`
}

func (r *Repo) ListAlerts(ctx context.Context, deviceID, state string, limit int) ([]AlertRow, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, rule_id, rule_name, device_id, metric, value, threshold, severity, state, fired_at, resolved_at, notified_at
		FROM alerts
		WHERE ($1 = '' OR device_id = $1) AND ($2 = '' OR state = $2)
		ORDER BY fired_at DESC LIMIT $3`, deviceID, state, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AlertRow{}
	for rows.Next() {
		var a AlertRow
		if err := rows.Scan(&a.ID, &a.RuleID, &a.RuleName, &a.DeviceID, &a.Metric, &a.Value, &a.Threshold, &a.Severity, &a.State, &a.FiredAt, &a.ResolvedAt, &a.NotifiedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
