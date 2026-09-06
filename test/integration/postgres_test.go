//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/domain"
	"github.com/egocucumber/telemetry-platform/internal/postgres"
	"github.com/egocucumber/telemetry-platform/internal/processor"
	"github.com/egocucumber/telemetry-platform/internal/query"
	"github.com/egocucumber/telemetry-platform/migrations"
)

func TestPostgres(t *testing.T) {
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "timescale/timescaledb:2.17.2-pg16",
		tcpostgres.WithDatabase("telemetry"),
		tcpostgres.WithUsername("telemetry"),
		tcpostgres.WithPassword("telemetry"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := postgres.New(ctx, config.Postgres{DSN: dsn, MaxConns: 4})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, postgres.Migrate(ctx, pool, migrations.FS))

	require.NoError(t, postgres.Migrate(ctx, pool, migrations.FS))

	t.Run("seed rules load into the engine", func(t *testing.T) {
		rules, err := processor.PGRuleSource{Pool: pool}.Load(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, rules)
		require.Equal(t, 10*time.Second, rules[0].For)
	})

	t.Run("rules CRUD publishes and deletes", func(t *testing.T) {
		repo := query.NewRepo(pool)
		created, err := repo.CreateRule(ctx, domain.Rule{
			Name: "test", Metric: "rpm", Op: domain.OpLT, Threshold: 100, For: 5 * time.Second,
			Severity: domain.SeverityInfo, Enabled: true,
		})
		require.NoError(t, err)
		require.NotZero(t, created.ID)

		require.NoError(t, repo.DeleteRule(ctx, created.ID))
		require.ErrorIs(t, repo.DeleteRule(ctx, created.ID), query.ErrNotFound)
	})

	t.Run("measurement insert is idempotent and history buckets", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO devices (id, gateway_id) VALUES ('d1', 'gw-demo') ON CONFLICT DO NOTHING`)
		require.NoError(t, err)

		base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
		ts := []time.Time{base, base.Add(time.Second), base.Add(2 * time.Second)}
		dev := []string{"d1", "d1", "d1"}
		met := []string{"temperature", "temperature", "temperature"}
		val := []float64{10, 20, 30}
		seq := []int64{1, 2, 3}
		const insert = `
			INSERT INTO measurements (ts, device_id, metric, value, seq)
			SELECT * FROM unnest($1::timestamptz[], $2::text[], $3::text[], $4::float8[], $5::bigint[])
			ON CONFLICT (device_id, metric, ts) DO NOTHING`
		_, err = pool.Exec(ctx, insert, ts, dev, met, val, seq)
		require.NoError(t, err)

		_, err = pool.Exec(ctx, insert, ts, dev, met, val, seq)
		require.NoError(t, err)

		var n int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM measurements WHERE device_id = 'd1'`).Scan(&n))
		require.Equal(t, 3, n)

		repo := query.NewRepo(pool)
		points, err := repo.History(ctx, "d1", "temperature", base, base.Add(time.Minute), 10*time.Second)
		require.NoError(t, err)
		require.Len(t, points, 1)
		require.Equal(t, 20.0, points[0].Avg)
		require.Equal(t, int64(3), points[0].Count)

		agg, err := repo.History(ctx, "d1", "temperature", base, base.Add(time.Hour), time.Minute)
		require.NoError(t, err)
		require.Len(t, agg, 1)
		require.Equal(t, 30.0, agg[0].Max)
	})
}
