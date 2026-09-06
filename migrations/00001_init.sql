-- +goose Up
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE gateways (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    api_key_hash TEXT NOT NULL UNIQUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE devices (
    id         TEXT PRIMARY KEY,
    gateway_id TEXT NOT NULL REFERENCES gateways(id),
    name       TEXT NOT NULL DEFAULT '',
    last_seen  TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX devices_gateway_idx ON devices (gateway_id);

CREATE TABLE rules (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    device_id   TEXT REFERENCES devices(id) ON DELETE CASCADE,
    metric      TEXT NOT NULL,
    op          TEXT NOT NULL CHECK (op IN ('gt', 'ge', 'lt', 'le')),
    threshold   DOUBLE PRECISION NOT NULL,
    for_seconds INTEGER NOT NULL DEFAULT 0 CHECK (for_seconds >= 0),
    severity    TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    enabled     BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE alerts (
    id          UUID PRIMARY KEY,
    rule_id     BIGINT NOT NULL,
    rule_name   TEXT NOT NULL,
    device_id   TEXT NOT NULL,
    metric      TEXT NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    threshold   DOUBLE PRECISION NOT NULL,
    severity    TEXT NOT NULL,
    state       TEXT NOT NULL CHECK (state IN ('firing', 'resolved')),
    fired_at    TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    notified_at TIMESTAMPTZ
);
CREATE INDEX alerts_device_fired_idx ON alerts (device_id, fired_at DESC);
CREATE INDEX alerts_fired_idx ON alerts (fired_at DESC);

CREATE TABLE measurements (
    ts        TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    metric    TEXT NOT NULL,
    value     DOUBLE PRECISION NOT NULL,
    seq       BIGINT NOT NULL
);
SELECT create_hypertable('measurements', 'ts', chunk_time_interval => INTERVAL '1 hour');
CREATE UNIQUE INDEX measurements_uniq ON measurements (device_id, metric, ts);

-- +goose Down
DROP TABLE IF EXISTS measurements;
DROP TABLE IF EXISTS alerts;
DROP TABLE IF EXISTS rules;
DROP TABLE IF EXISTS devices;
DROP TABLE IF EXISTS gateways;
