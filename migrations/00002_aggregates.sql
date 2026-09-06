-- +goose NO TRANSACTION
-- +goose Up
CREATE MATERIALIZED VIEW measurements_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 minute', ts) AS bucket,
    device_id,
    metric,
    avg(value)   AS avg,
    min(value)   AS min,
    max(value)   AS max,
    count(*)     AS count
FROM measurements
GROUP BY bucket, device_id, metric
WITH NO DATA;

SELECT add_continuous_aggregate_policy('measurements_1m',
    start_offset    => INTERVAL '1 hour',
    end_offset      => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

SELECT add_retention_policy('measurements', INTERVAL '7 days');
SELECT add_retention_policy('measurements_1m', INTERVAL '90 days');

-- +goose Down
DROP MATERIALIZED VIEW IF EXISTS measurements_1m;
