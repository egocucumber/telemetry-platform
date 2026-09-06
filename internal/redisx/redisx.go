package redisx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/egocucumber/telemetry-platform/internal/config"
)

const ChannelRulesChanged = "rules.changed"

func New(ctx context.Context, cfg config.Redis) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     32,
	})
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		return nil, fmt.Errorf("redis otel: %w", err)
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping %s: %w", cfg.Addr, err)
	}
	return rdb, nil
}

func Ping(rdb *redis.Client) func(context.Context) error {
	return func(ctx context.Context) error { return rdb.Ping(ctx).Err() }
}

func KeyDedup(key string) string       { return "dedup:" + key }
func KeyLatest(deviceID string) string { return "latest:" + deviceID }
func KeyCooldown(ruleID int64, deviceID string) string {
	return fmt.Sprintf("cooldown:%d:%s", ruleID, deviceID)
}

type LatestValue struct {
	Value float64   `json:"v"`
	TS    time.Time `json:"ts"`
	MS    int64     `json:"ms"`
}

func NewLatestValue(v float64, ts time.Time) LatestValue {
	ts = ts.UTC()
	return LatestValue{Value: v, TS: ts, MS: ts.UnixMilli()}
}

func MarkSeen(ctx context.Context, rdb *redis.Client, keys []string, ttl time.Duration) ([]bool, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := rdb.Pipeline()
	cmds := make([]*redis.BoolCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.SetNX(ctx, KeyDedup(k), 1, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("dedup pipeline: %w", err)
	}
	out := make([]bool, len(keys))
	for i, c := range cmds {
		out[i] = c.Val()
	}
	return out, nil
}

func SetLatest(ctx context.Context, rdb *redis.Client, values map[string]map[string]LatestValue, ttl time.Duration) error {
	if len(values) == 0 {
		return nil
	}
	pipe := rdb.Pipeline()
	for device, metrics := range values {
		key := KeyLatest(device)
		for metric, lv := range metrics {
			b, err := json.Marshal(lv)
			if err != nil {
				return err
			}
			pipe.EvalSha(ctx, setLatestSHA, []string{key}, metric, string(b), lv.MS)
		}
		pipe.Expire(ctx, key, ttl)
	}
	_, err := pipe.Exec(ctx)
	if err == nil {
		return nil
	}

	if redis.HasErrorPrefix(err, "NOSCRIPT") {
		if _, lerr := setLatestScript.Load(ctx, rdb).Result(); lerr != nil {
			return fmt.Errorf("load script: %w", lerr)
		}
		return SetLatest(ctx, rdb, values, ttl)
	}
	return fmt.Errorf("set latest: %w", err)
}

func GetLatest(ctx context.Context, rdb *redis.Client, deviceID string) (map[string]LatestValue, error) {
	raw, err := rdb.HGetAll(ctx, KeyLatest(deviceID)).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}
	out := make(map[string]LatestValue, len(raw))
	for metric, s := range raw {
		var lv LatestValue
		if err := json.Unmarshal([]byte(s), &lv); err != nil {
			continue
		}
		out[metric] = lv
	}
	return out, nil
}

var setLatestScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  local ms = string.match(cur, '"ms":(%d+)')
  if ms and tonumber(ms) > tonumber(ARGV[3]) then
    return 0
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

var setLatestSHA = setLatestScript.Hash()

func AcquireCooldown(ctx context.Context, rdb *redis.Client, key string, window time.Duration) (bool, int64, error) {
	res, err := cooldownScript.Run(ctx, rdb, []string{key}, window.Milliseconds()).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("cooldown script: %w", err)
	}
	return res[0] == 1, res[1], nil
}

var cooldownScript = redis.NewScript(`
if redis.call('SET', KEYS[1], 0, 'NX', 'PX', ARGV[1]) then
  return {1, 0}
end
local n = redis.call('INCR', KEYS[1])
return {0, n}
`)
