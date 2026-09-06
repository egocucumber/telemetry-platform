package ingest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
)

type DeviceRegistry struct {
	pool          *pgxpool.Pool
	touchInterval time.Duration

	mu   sync.Mutex
	seen map[string]seenEntry
	sf   singleflight.Group
}

type seenEntry struct {
	gatewayID string
	touched   time.Time
}

type ErrDeviceOwnedByOther struct{ DeviceID, Owner string }

func (e ErrDeviceOwnedByOther) Error() string {
	return fmt.Sprintf("device %s belongs to gateway %s", e.DeviceID, e.Owner)
}

func NewDeviceRegistry(pool *pgxpool.Pool, touchInterval time.Duration) *DeviceRegistry {
	return &DeviceRegistry{pool: pool, touchInterval: touchInterval, seen: map[string]seenEntry{}}
}

func (r *DeviceRegistry) Ensure(ctx context.Context, gatewayID, deviceID string, now time.Time) error {
	r.mu.Lock()
	e, ok := r.seen[deviceID]
	r.mu.Unlock()
	if ok {
		if e.gatewayID != gatewayID {
			return ErrDeviceOwnedByOther{DeviceID: deviceID, Owner: e.gatewayID}
		}
		if now.Sub(e.touched) < r.touchInterval {
			return nil
		}
	}

	_, err, _ := r.sf.Do(deviceID, func() (any, error) {
		var owner string
		err := r.pool.QueryRow(ctx, `
			INSERT INTO devices (id, gateway_id, last_seen)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE SET last_seen = EXCLUDED.last_seen
			RETURNING gateway_id`, deviceID, gatewayID, now,
		).Scan(&owner)
		if err != nil {
			return nil, fmt.Errorf("upsert device: %w", err)
		}
		r.mu.Lock()
		r.seen[deviceID] = seenEntry{gatewayID: owner, touched: now}
		r.mu.Unlock()
		if owner != gatewayID {
			return nil, ErrDeviceOwnedByOther{DeviceID: deviceID, Owner: owner}
		}
		return nil, nil
	})
	return err
}
