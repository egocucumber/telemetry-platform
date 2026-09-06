package redisx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func KeyWindow(deviceID string) string { return "window:" + deviceID }

type WindowValue struct {
	Bucket time.Time `json:"bucket"`
	Avg    float64   `json:"avg"`
	Min    float64   `json:"min"`
	Max    float64   `json:"max"`
	Count  int64     `json:"count"`
}

func SetWindows(ctx context.Context, rdb *redis.Client, windows map[string]map[string]WindowValue, ttl time.Duration) error {
	if len(windows) == 0 {
		return nil
	}
	pipe := rdb.Pipeline()
	for device, metrics := range windows {
		key := KeyWindow(device)
		fields := make(map[string]any, len(metrics))
		for metric, w := range metrics {
			b, err := json.Marshal(w)
			if err != nil {
				return err
			}
			fields[metric] = string(b)
		}
		pipe.HSet(ctx, key, fields)
		pipe.Expire(ctx, key, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("set windows: %w", err)
	}
	return nil
}

func GetWindows(ctx context.Context, rdb *redis.Client, deviceID string) (map[string]WindowValue, error) {
	raw, err := rdb.HGetAll(ctx, KeyWindow(deviceID)).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}
	out := make(map[string]WindowValue, len(raw))
	for metric, s := range raw {
		var w WindowValue
		if err := json.Unmarshal([]byte(s), &w); err != nil {
			continue
		}
		out[metric] = w
	}
	return out, nil
}
