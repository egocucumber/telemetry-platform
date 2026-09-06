//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

func TestRedis(t *testing.T) {
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	addr, err := c.ConnectionString(ctx)
	require.NoError(t, err)

	addr = addr[len("redis://"):]

	rdb, err := redisx.New(ctx, config.Redis{Addr: addr})
	require.NoError(t, err)

	t.Run("dedup marks first occurrence only", func(t *testing.T) {
		keys := []string{"d1:1", "d1:2", "d1:1"}
		fresh, err := redisx.MarkSeen(ctx, rdb, keys, time.Minute)
		require.NoError(t, err)
		require.Equal(t, []bool{true, true, false}, fresh)

		again, err := redisx.MarkSeen(ctx, rdb, []string{"d1:2"}, time.Minute)
		require.NoError(t, err)
		require.Equal(t, []bool{false}, again)
	})

	t.Run("latest keeps the freshest sample", func(t *testing.T) {
		newer := time.Date(2026, 1, 1, 12, 0, 10, 0, time.UTC)
		older := newer.Add(-5 * time.Second)

		require.NoError(t, redisx.SetLatest(ctx, rdb, map[string]map[string]redisx.LatestValue{
			"dev": {"temp": redisx.NewLatestValue(70, newer)},
		}, time.Hour))

		require.NoError(t, redisx.SetLatest(ctx, rdb, map[string]map[string]redisx.LatestValue{
			"dev": {"temp": redisx.NewLatestValue(10, older)},
		}, time.Hour))

		got, err := redisx.GetLatest(ctx, rdb, "dev")
		require.NoError(t, err)
		require.Equal(t, 70.0, got["temp"].Value)
		require.True(t, got["temp"].TS.Equal(newer))
	})

	t.Run("cooldown admits one notification per window", func(t *testing.T) {
		key := redisx.KeyCooldown(7, "dev")
		ok, n, err := redisx.AcquireCooldown(ctx, rdb, key, 500*time.Millisecond)
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, 0, n)

		ok, n, err = redisx.AcquireCooldown(ctx, rdb, key, 500*time.Millisecond)
		require.NoError(t, err)
		require.False(t, ok)
		require.EqualValues(t, 1, n)

		time.Sleep(600 * time.Millisecond)
		ok, _, err = redisx.AcquireCooldown(ctx, rdb, key, 500*time.Millisecond)
		require.NoError(t, err)
		require.True(t, ok, "window expired, next alert must pass")
	})
}
