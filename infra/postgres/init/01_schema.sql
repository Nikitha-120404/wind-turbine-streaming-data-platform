-- infra/postgres/init/01_schema.sql
--
-- Wind Turbine Platform — TimescaleDB Schema
-- Executed once on container first boot.
-- Safe to re-run (IF NOT EXISTS throughout).
--
-- This schema merges:
--   1. The production platform schema (turbine_telemetry hypertable, alerts,
--      hourly aggregate, DLQ tracking, compression, retention)
--   2. The original TimeScaleDB_Queries.txt from the legacy scripts — all 7
--      extended sensor columns and the 5-minute continuous aggregate are
--      preserved here as first-class objects.
--
-- Legacy field → column mapping:
--   Nacelle_Position  → nacelle_position_deg
--   Wind_direction    → wind_direction_deg
--   Ambient_Air_temp  → ambient_temp_c
--   Bearing_Temp      → bearing_temp_c
--   GearBoxSumpTemp   → gearbox_sump_temp_c
--   GearTemp          → gear_temp_c
--   Hub_Speed         → hub_speed

-- ─── Extensions ──────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ─── Raw telemetry hypertable ─────────────────────────────────────────────────
--
-- Replaces the original wind_turbine_streamdata table from TimeScaleDB_Queries.txt.
-- Improvements:
--   • UUID event_id instead of SERIAL (globally unique, safe for distributed ingestion)
--   • All original columns preserved with canonical snake_case names
--   • UNIQUE constraint on (turbine_id, recorded_at) for idempotent inserts
--   • Extended sensor columns added from original generate_data() fields

CREATE TABLE IF NOT EXISTS turbine_telemetry (
    -- Platform identity
    event_id            UUID             NOT NULL,
    turbine_id          VARCHAR(64)      NOT NULL,
    recorded_at         TIMESTAMPTZ      NOT NULL,

    -- ── Core operational fields ───────────────────────────────────────────────
    -- These drive anomaly detection and are required (NOT NULL).
    wind_speed_ms       DOUBLE PRECISION NOT NULL,           -- Wind_Speed in original
    rotor_rpm           DOUBLE PRECISION NOT NULL,           -- Generator_Speed/10 in original
    power_output_kw     DOUBLE PRECISION NOT NULL,           -- Power in original
    temperature_c       DOUBLE PRECISION NOT NULL,           -- GeneratorTemp in original
    vibration_mms       DOUBLE PRECISION NOT NULL DEFAULT 0, -- Not in original, defaulted
    blade_pitch_deg     DOUBLE PRECISION NOT NULL,           -- BladePitchAngle (clamped 0–90) in original
    status              VARCHAR(16)       NOT NULL DEFAULT 'online',

    -- ── Extended sensor fields (from original wind_turbine_sensorlog.py) ─────
    -- Optional (NULL allowed) so existing events without these fields still insert.
    nacelle_position_deg  DOUBLE PRECISION,  -- Nacelle_Position: nacelle yaw (0–360°)
    wind_direction_deg    DOUBLE PRECISION,  -- Wind_direction: wind bearing (0–360°)
    ambient_temp_c        DOUBLE PRECISION,  -- Ambient_Air_temp: outside air temp
    bearing_temp_c        DOUBLE PRECISION,  -- Bearing_Temp: main bearing temp
    gearbox_sump_temp_c   DOUBLE PRECISION,  -- GearBoxSumpTemp: gearbox oil sump temp
    gear_temp_c           DOUBLE PRECISION,  -- GearTemp: gear mesh temperature
    hub_speed             DOUBLE PRECISION,  -- Hub_Speed: hub rotation speed

    -- Pipeline metadata
    ingested_at         TIMESTAMPTZ      NOT NULL DEFAULT NOW(),

    -- Dedup constraint: one reading per turbine per recorded timestamp
    CONSTRAINT uq_turbine_telemetry_dedup UNIQUE (turbine_id, recorded_at)
);

-- Convert to hypertable partitioned by recorded_at
-- chunk_time_interval: 1 day — tune based on data volume
SELECT create_hypertable(
    'turbine_telemetry',
    'recorded_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_telemetry_turbine_time
    ON turbine_telemetry (turbine_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_status
    ON turbine_telemetry (status, recorded_at DESC)
    WHERE status != 'online';

-- Index for bearing temp queries (maintenance dashboard)
CREATE INDEX IF NOT EXISTS idx_telemetry_bearing_temp
    ON turbine_telemetry (turbine_id, bearing_temp_c, recorded_at DESC)
    WHERE bearing_temp_c IS NOT NULL;

-- ─── Alerts table ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS turbine_alerts (
    alert_id        UUID            PRIMARY KEY,
    turbine_id      VARCHAR(64)     NOT NULL,
    recorded_at     TIMESTAMPTZ     NOT NULL,
    severity        VARCHAR(16)     NOT NULL,
    rule_name       VARCHAR(128)    NOT NULL,
    message         TEXT            NOT NULL,
    metric_name     VARCHAR(64)     NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    threshold       DOUBLE PRECISION NOT NULL,
    source_event_id UUID            NOT NULL,
    alert_ts        TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_turbine_time
    ON turbine_alerts (turbine_id, alert_ts DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_severity_time
    ON turbine_alerts (severity, alert_ts DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_rule
    ON turbine_alerts (rule_name, alert_ts DESC);

-- ─── Continuous aggregates ────────────────────────────────────────────────────

-- ── 5-minute aggregate (from original TimeScaleDB_Queries.txt) ───────────────
--
-- Original query:
--   CREATE MATERIALIZED VIEW wind_turbine_5min_avg WITH (timescaledb.continuous) AS
--   SELECT time_bucket('5 minutes', timestamp) AS bucket, turbine_id,
--          AVG(power) AS avg_power, AVG(wind_speed) AS avg_wind_speed,
--          AVG(generator_speed) AS avg_generator_speed
--   FROM wind_turbine_streamdata GROUP BY bucket, turbine_id WITH NO DATA;
--
-- Improvements: canonical column names, all original metrics retained,
-- extended sensor averages added so maintenance queries work on the fast path.

CREATE MATERIALIZED VIEW IF NOT EXISTS turbine_metrics_5min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', recorded_at) AS bucket,
    turbine_id,
    -- Original avg_power / avg_wind_speed / avg_generator_speed preserved
    AVG(power_output_kw)      AS avg_power_kw,       -- original: avg_power
    AVG(wind_speed_ms)        AS avg_wind_speed_ms,   -- original: avg_wind_speed
    AVG(rotor_rpm)            AS avg_generator_speed, -- original: avg_generator_speed
    -- Additional metrics for operational visibility
    MAX(power_output_kw)      AS max_power_kw,
    AVG(temperature_c)        AS avg_temp_c,
    MAX(temperature_c)        AS max_temp_c,
    AVG(vibration_mms)        AS avg_vibration,
    -- Extended sensor averages (NULL-safe: AVG ignores NULLs)
    AVG(bearing_temp_c)       AS avg_bearing_temp_c,
    MAX(bearing_temp_c)       AS max_bearing_temp_c,
    AVG(gearbox_sump_temp_c)  AS avg_gearbox_sump_temp_c,
    AVG(gear_temp_c)          AS avg_gear_temp_c,
    AVG(ambient_temp_c)       AS avg_ambient_temp_c,
    COUNT(*)                  AS sample_count
FROM turbine_telemetry
GROUP BY bucket, turbine_id
WITH NO DATA;

-- Refresh policy: update every 5 minutes with 1 minute lag
SELECT add_continuous_aggregate_policy(
    'turbine_metrics_5min',
    start_offset => INTERVAL '30 minutes',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

-- ── Hourly aggregate (platform-level, for trending and Grafana) ───────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS turbine_metrics_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', recorded_at) AS bucket,
    turbine_id,
    AVG(power_output_kw)      AS avg_power_kw,
    MAX(power_output_kw)      AS max_power_kw,
    MIN(power_output_kw)      AS min_power_kw,
    AVG(wind_speed_ms)        AS avg_wind_ms,
    MAX(wind_speed_ms)        AS max_wind_ms,
    AVG(rotor_rpm)            AS avg_rpm,
    AVG(temperature_c)        AS avg_temp_c,
    MAX(temperature_c)        AS max_temp_c,
    AVG(vibration_mms)        AS avg_vibration,
    MAX(vibration_mms)        AS max_vibration,
    AVG(bearing_temp_c)       AS avg_bearing_temp_c,
    AVG(gearbox_sump_temp_c)  AS avg_gearbox_sump_temp_c,
    COUNT(*)                  AS sample_count,
    COUNT(*) FILTER (WHERE status != 'online') AS non_online_count
FROM turbine_telemetry
GROUP BY bucket, turbine_id
WITH NO DATA;

-- Refresh policy: keep hourly aggregate up-to-date with 10 min lag
SELECT add_continuous_aggregate_policy(
    'turbine_metrics_1h',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '10 minutes',
    if_not_exists => TRUE
);

-- ─── Retention policies ───────────────────────────────────────────────────────
-- Original: 365 days. Platform default: 90 days.
-- We use 90 days for raw telemetry (configurable). The continuous aggregates
-- are retained indefinitely unless a separate policy is added.

SELECT add_retention_policy(
    'turbine_telemetry',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

-- ─── Compression ─────────────────────────────────────────────────────────────
-- Original: compress_segmentby = 'turbine_id', compress after 30 days.
-- Platform: same segmentby, compress after 7 days (more aggressive = more savings).

ALTER TABLE turbine_telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'turbine_id',
    timescaledb.compress_orderby   = 'recorded_at DESC'
);

SELECT add_compression_policy(
    'turbine_telemetry',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- ─── DLQ tracking table ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dlq_events (
    dlq_id          UUID            PRIMARY KEY,
    original_topic  VARCHAR(128)    NOT NULL,
    original_partition INTEGER,
    original_offset BIGINT,
    raw_payload     TEXT            NOT NULL,
    error_type      VARCHAR(128)    NOT NULL,
    error_message   TEXT            NOT NULL,
    failed_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    retry_count     INTEGER         NOT NULL DEFAULT 0,
    resolved        BOOLEAN         NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dlq_failed_at
    ON dlq_events (failed_at DESC);

CREATE INDEX IF NOT EXISTS idx_dlq_resolved
    ON dlq_events (resolved, failed_at DESC);

-- ─── Helpful views ────────────────────────────────────────────────────────────

-- Latest status per turbine (used by ops API)
CREATE OR REPLACE VIEW turbine_latest_status AS
SELECT DISTINCT ON (turbine_id)
    turbine_id,
    recorded_at           AS last_seen,
    power_output_kw       AS last_power_kw,
    wind_speed_ms         AS last_wind_ms,
    temperature_c         AS last_temp_c,
    rotor_rpm             AS last_rpm,
    vibration_mms         AS last_vibration,
    bearing_temp_c        AS last_bearing_temp_c,
    gearbox_sump_temp_c   AS last_gearbox_sump_temp_c,
    gear_temp_c           AS last_gear_temp_c,
    nacelle_position_deg  AS last_nacelle_deg,
    wind_direction_deg    AS last_wind_direction_deg,
    ambient_temp_c        AS last_ambient_temp_c,
    hub_speed             AS last_hub_speed,
    status
FROM turbine_telemetry
ORDER BY turbine_id, recorded_at DESC;

-- Alert counts by severity for dashboard
CREATE OR REPLACE VIEW alert_summary_by_severity AS
SELECT
    severity,
    COUNT(*) FILTER (WHERE alert_ts > NOW() - INTERVAL '1 hour')   AS last_1h,
    COUNT(*) FILTER (WHERE alert_ts > NOW() - INTERVAL '24 hours')  AS last_24h,
    COUNT(*)                                                         AS all_time
FROM turbine_alerts
GROUP BY severity;

-- Maintenance view: turbines with elevated bearing or gearbox temperatures
-- Mirrors the kind of maintenance query the original system was designed for.
CREATE OR REPLACE VIEW turbine_thermal_health AS
SELECT DISTINCT ON (turbine_id)
    turbine_id,
    recorded_at,
    bearing_temp_c,
    gearbox_sump_temp_c,
    gear_temp_c,
    temperature_c,
    CASE
        WHEN bearing_temp_c > 65      THEN 'bearing_warning'
        WHEN gearbox_sump_temp_c > 95 THEN 'gearbox_warning'
        WHEN gear_temp_c > 280        THEN 'gear_warning'
        WHEN temperature_c > 85       THEN 'generator_warning'
        ELSE 'ok'
    END AS thermal_status
FROM turbine_telemetry
WHERE bearing_temp_c IS NOT NULL
ORDER BY turbine_id, recorded_at DESC;
