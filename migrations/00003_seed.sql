-- +goose Up
INSERT INTO gateways (id, name, api_key_hash) VALUES
    ('gw-demo', 'Demo workshop gateway', encode(sha256('demo-gateway-key'::bytea), 'hex'))
ON CONFLICT (id) DO NOTHING;

INSERT INTO rules (name, device_id, metric, op, threshold, for_seconds, severity) VALUES
    ('Spindle overheating',   NULL, 'temperature',  'gt', 85,   10, 'critical'),
    ('Excessive vibration',   NULL, 'vibration',    'gt', 7.0,  5,  'warning'),
    ('Spindle overload',      NULL, 'spindle_load', 'gt', 95,   3,  'warning'),
    ('Coolant pressure low',  NULL, 'coolant_bar',  'lt', 1.5,  15, 'warning');

-- +goose Down
DELETE FROM rules;
DELETE FROM gateways WHERE id = 'gw-demo';
