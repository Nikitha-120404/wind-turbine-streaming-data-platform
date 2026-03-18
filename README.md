# Wind Turbine Telemetry Platform

A  real-time streaming platform for wind turbine telemetry, anomaly detection, and operational monitoring. Built on Apache Kafka, TimescaleDB, FastAPI, Prometheus, and Grafana — fully runnable locally with Docker Compose.

This platform was evolved from a working Kafka + TimescaleDB prototype with three original scripts:
- `wind_turbine_sensorlog.py` — 12-turbine simulator with 14 sensor fields
- `v2_kafka_producer_windt_sensorlog_to_kafka.py` — file-based Kafka producer
- `v2_Kafka_consumer_checkdata.py` — debug consumer
- `TimeScaleDB_Queries.txt` — original hypertable and 5-minute aggregate setup

All original logic is preserved and integrated. See [Original Script Integration](#original-script-integration) below.

---

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────────┐     ┌────────────────┐
│  Simulator  │────▶│  windturbine-raw (6 partitions, 7d TTL)  │────▶│ Stream         │
│  (Producer) │     └──────────────────────────────────────────┘     │ Processor      │──▶ windturbine-alerts
└─────────────┘                                                        │ (Anomaly Det.) │──▶ windturbine-dlq
                                                                       └────────────────┘
                                ↓ (separate consumer group)
                         ┌────────────┐       ┌──────────────────┐
                         │ DB Writer  │──────▶│   TimescaleDB    │
                         └────────────┘       │ turbine_telemetry│
                                              │ turbine_alerts   │
                                              │ turbine_metrics_1h│
                                              └──────────────────┘
                                                       ↑
                                              ┌────────────────┐
                                              │  FastAPI       │  ← /health /turbines /alerts
                                              │  Ops API       │
                                              └────────────────┘
                                              ┌────────────────┐
                                              │  Prometheus +  │  ← scrapes all services
                                              │  Grafana       │
                                              └────────────────┘
```

## Services

| Service | Port | Purpose |
|---|---|---|
| `producer` | 8001 (metrics) | Simulates turbines, publishes to Kafka |
| `processor` | 8002 (metrics) | Anomaly detection, routes alerts + DLQ |
| `db_writer` | 8003 (metrics) | Batched inserts into TimescaleDB |
| `ops_api` | 8000 (API), 8004 (metrics) | Health, status, and data endpoints |
| `kafka` | 9092 | External Kafka broker |
| `timescaledb` | 5432 | Time-series database |
| `prometheus` | 9090 | Metrics collection |
| `grafana` | 3000 | Dashboards |

---

## Prerequisites

- Docker ≥ 24.0
- Docker Compose ≥ 2.20
- Python 3.11+ (for running tests and scripts locally)
- 8 GB RAM recommended (Kafka + TimescaleDB are memory-hungry)

---

## Quick Start

### 1. Clone and configure

```bash
cd wind-turbine-platform
cp .env.example .env
# .env defaults work out of the box — no changes needed for local dev
```

### 2. Start the full platform

```bash
docker compose up --build -d
```

Watch startup (takes ~60s for all health checks to pass):

```bash
docker compose ps
docker compose logs -f
```

### 3. Verify everything is running

```bash
# All services should show "healthy" or "running"
docker compose ps

# Check ops API
curl http://localhost:8000/health
curl http://localhost:8000/ready
curl http://localhost:8000/status
```

---

## Verifying the Data Pipeline

### Check Kafka message flow

```bash
# Install Python deps locally first
pip install -r requirements.txt

# List topics and partition info
python scripts/kafka_inspect.py --topics

# Show consumer group lag (should be near 0 when all services are up)
python scripts/kafka_inspect.py --groups

# Tail last 5 raw telemetry messages
python scripts/kafka_inspect.py --tail raw 5

# Tail last 5 alert messages
python scripts/kafka_inspect.py --tail alerts 5

# Watch DLQ live
python scripts/kafka_inspect.py --tail dlq 10
```

### Check database inserts

```bash
# Show row counts and latest readings
python scripts/db_inspect.py --all

# Or individual checks
python scripts/db_inspect.py --counts
python scripts/db_inspect.py --latest
python scripts/db_inspect.py --alerts
python scripts/db_inspect.py --chunks
```

### Check via Ops API

```bash
# Live turbine status
curl http://localhost:8000/turbines | python3 -m json.tool

# Recent anomaly alerts
curl http://localhost:8000/alerts/recent | python3 -m json.tool

# Critical alerts only
curl "http://localhost:8000/alerts/recent?severity=CRITICAL" | python3 -m json.tool

# Platform metrics summary
curl http://localhost:8000/metrics/summary | python3 -m json.tool
```

---

## Monitoring

### Prometheus

Open: http://localhost:9090

Key metrics to query:

```promql
# Event throughput
sum(rate(wt_events_published_total[1m])) by (turbine_id)

# Processing latency p95
histogram_quantile(0.95, sum(rate(wt_processing_latency_seconds_bucket[2m])) by (le))

# Anomaly alert rate
sum(rate(wt_anomaly_alerts_total[2m])) by (rule_name, severity)

# DLQ message rate
sum(rate(wt_dlq_messages_total[2m])) by (error_type)

# DB insert failures
sum(rate(wt_db_insert_failures_total[2m])) by (table_name)

# All services up
sum(wt_service_up)
```

### Grafana

Open: http://localhost:3000  
Login: `admin` / `windturbine` (or as set in .env)

The **Wind Turbine Platform — Overview** dashboard is auto-provisioned and shows:
- Services up gauge
- Event throughput
- Processing latency (p50/p95/p99)
- Anomaly alerts by rule and severity
- DB write latency
- DLQ message rate
- Producer lag

---

## Testing Anomaly Detection

The simulator injects anomalies automatically with ~2% probability each tick. To observe them:

```bash
# Watch alerts appear in real-time (within 30-60s typically)
python scripts/kafka_inspect.py --tail alerts 20

# Or watch DLQ for any malformed events
python scripts/dlq_consumer.py --watch

# Database: count alerts by rule in the last hour
psql "postgresql://wt_user:wt_password@localhost:5432/windturbine" -c "
  SELECT rule_name, severity, COUNT(*)
  FROM turbine_alerts
  WHERE alert_ts > NOW() - INTERVAL '1 hour'
  GROUP BY rule_name, severity
  ORDER BY count DESC;
"
```

Anomaly rules fire on these thresholds:

| Rule | Condition | Severity |
|---|---|---|
| `nacelle_temp_warning` | temperature > 85°C | WARNING |
| `nacelle_temp_critical` | temperature > 95°C | CRITICAL |
| `vibration_warning` | vibration > 6.0 mm/s | WARNING |
| `vibration_critical` | vibration > 9.0 mm/s | CRITICAL |
| `low_power_output` | wind > 5 m/s, power < 50 kW | WARNING |
| `power_dropout` | wind > 8 m/s, rpm > 2, power < 5 kW | CRITICAL |
| `unplanned_downtime` | wind > 3 m/s, rpm ≈ 0, power ≈ 0 | CRITICAL |
| `blade_pitch_extreme` | pitch > 88° at wind < 20 m/s | WARNING |
| `rotor_overspeed` | RPM > 18 | CRITICAL |

---

## Testing DLQ Handling

```bash
# Publish a deliberately malformed message to the raw topic
docker exec wt_kafka kafka-console-producer \
  --bootstrap-server localhost:29092 \
  --topic windturbine-raw <<< '{"this": "is not valid telemetry"}'

# Within a few seconds, the processor will route it to DLQ
python scripts/dlq_consumer.py --watch

# See DLQ breakdown by error type
python scripts/dlq_consumer.py --summary

# Dry-run replay (shows what would be replayed without publishing)
python scripts/dlq_consumer.py --replay --dry-run

# Actually replay DLQ messages to original topic
python scripts/dlq_consumer.py --replay --execute
```

---

## Running Tests

```bash
# Install deps
pip install -r requirements.txt

# Run full test suite
pytest

# Run specific test modules
pytest tests/test_schema.py -v
pytest tests/test_anomaly_detector.py -v
pytest tests/test_simulator.py -v
pytest tests/test_ops_api.py -v

# Run with coverage
pip install pytest-cov
pytest --cov=shared --cov=services --cov-report=term-missing
```

The test suite covers:
- Schema validation (valid payloads, boundary conditions, invalid inputs, coercion logic)
- Every anomaly detection rule (trigger conditions, non-trigger conditions, severity)
- Simulator physics (state transitions, anomaly injection, fleet management)
- Ops API endpoints (HTTP status codes, response structure, error handling)
- Config loading

---

## Running Services Locally (outside Docker)

If you prefer to run Python services directly (e.g., for debugging in VS Code):

```bash
# 1. Start only infrastructure
docker compose up -d zookeeper kafka kafka-init timescaledb

# 2. Wait for health checks (~30s)
docker compose ps

# 3. Set environment (uses localhost ports)
export $(cat .env | grep -v '#' | xargs)
# Override to use localhost instead of container hostnames:
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export DB_HOST=localhost

# 4. Run each service in a separate VS Code terminal
python -m services.producer.main
python -m services.processor.main
python -m services.db_writer.main
python -m services.ops_api.main
```

---

## Project Structure

```
wind-turbine-platform/
├── shared/                        # Shared modules imported by all services
│   ├── schema.py                  # Pydantic models: TurbineTelemetry, TurbineAlert, DLQEvent
│   ├── config.py                  # Environment-based config (pydantic-settings)
│   ├── logging_config.py          # Structured JSON logging setup
│   ├── metrics.py                 # Prometheus metric definitions
│   └── kafka_admin.py             # Topic creation utility
│
├── services/
│   ├── producer/
│   │   ├── main.py                # Service entrypoint + main loop
│   │   ├── simulator.py           # Physics-based turbine fleet simulator
│   │   ├── kafka_producer.py      # Reliable Kafka producer wrapper
│   │   └── Dockerfile
│   ├── processor/
│   │   ├── main.py                # Stream processor consumer loop
│   │   ├── anomaly_detector.py    # Rules-based anomaly detection engine
│   │   └── Dockerfile
│   ├── db_writer/
│   │   ├── main.py                # Batched DB writer (telemetry + alerts)
│   │   └── Dockerfile
│   └── ops_api/
│       ├── main.py                # FastAPI ops API
│       └── Dockerfile
│
├── infra/
│   ├── postgres/init/
│   │   └── 01_schema.sql          # TimescaleDB schema, hypertables, aggregates
│   ├── prometheus/
│   │   └── prometheus.yml         # Scrape config for all services
│   └── grafana/
│       ├── provisioning/          # Auto-wires datasource + dashboard loader
│       └── dashboards/
│           └── platform_overview.json
│
├── tests/
│   ├── conftest.py                # Shared fixtures
│   ├── test_schema.py             # Pydantic schema tests
│   ├── test_anomaly_detector.py   # Rule tests
│   ├── test_simulator.py          # Simulator physics tests
│   ├── test_ops_api.py            # FastAPI endpoint tests
│   └── test_config.py             # Config loading tests
│
├── scripts/
│   ├── kafka_inspect.py           # Topic/consumer/tail tool
│   ├── db_inspect.py              # DB query verification tool
│   └── dlq_consumer.py            # DLQ watch/summarise/replay tool
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── pytest.ini
└── README.md
```

---

## Stopping and Resetting

```bash
# Stop all services
docker compose down

# Stop and remove volumes (full reset — deletes all data)
docker compose down -v

# Rebuild a single service after code change
docker compose up --build producer -d
```

---

## Extending the Platform

**Add a new anomaly rule:** Edit `services/processor/anomaly_detector.py` — add a new `AnomalyRule` entry to the `RULES` list. Add corresponding tests in `tests/test_anomaly_detector.py`.

**Add a new turbine:** Set `PRODUCER_TURBINE_IDS` in `.env` — e.g., `turbine-01,...,turbine-12` (default is already 12 turbines matching the original simulator).

**Scale the processor:** Kafka's consumer group protocol handles this automatically. Run multiple processor containers and they will rebalance across partitions.

**Add a new metric:** Define it in `shared/metrics.py`, instrument it in the relevant service, and add a Grafana panel pointing to it.

**Connect a real data source:** Replace `services/producer/simulator.py` with a reader for your actual data source (SCADA files, MQTT, REST API). The Kafka producer and downstream pipeline require no changes.

---

## Original Script Integration

This section documents exactly how each original script maps into the platform.

### `wind_turbine_sensorlog.py` → `services/producer/simulator.py`

All 14 original sensor fields are now first-class fields:

| Original Field | Canonical Name | Notes |
|---|---|---|
| `Turbine_ID` | `turbine_id` | `"Turbine_3"` → `"turbine-03"` |
| `Wind_Speed` | `wind_speed_ms` | Direct rename |
| `Generator_Speed` | `rotor_rpm` | Divided by 10 (gearbox ratio) |
| `Power` | `power_output_kw` | Direct rename |
| `GeneratorTemp` | `temperature_c` | Direct rename |
| `BladePitchAngle` | `blade_pitch_deg` | Clamped from 0–210 to 0–90 |
| `Timestamp` | `recorded_at` | Parsed from `"%Y-%m-%d %H:%M:%S"` |
| `Nacelle_Position` | `nacelle_position_deg` | New column in DB |
| `Wind_direction` | `wind_direction_deg` | New column in DB |
| `Ambient_Air_temp` | `ambient_temp_c` | New column in DB |
| `Bearing_Temp` | `bearing_temp_c` | New column in DB + anomaly rules |
| `GearBoxSumpTemp` | `gearbox_sump_temp_c` | New column in DB + anomaly rules |
| `GearTemp` | `gear_temp_c` | New column in DB + anomaly rules |
| `Hub_Speed` | `hub_speed` | New column in DB |

The original `generate_data()` loop for Turbine_1..12 is now `TurbineFleet` with 12 turbines by default. The simulator adds physics coupling: nacelle tracks wind direction, bearing temp rises with RPM, gearbox temp rises with load.

### `v2_kafka_producer_windt_sensorlog_to_kafka.py` → `services/producer/legacy_ingestor.py`

Feed your existing `wind_turbine.log` file into the production pipeline:

```bash
# Process an existing log file (all field names are auto-normalised)
LOG_FILE=/path/to/wind_turbine.log python -m services.producer.legacy_ingestor

# With Docker (mount the log file):
docker run --rm \
  -v /path/to/wind_turbine.log:/data/wind_turbine.log \
  -e LOG_FILE=/data/wind_turbine.log \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  wt_producer python -m services.producer.legacy_ingestor
```

Every original pattern is preserved:
- `json.loads()` + `JSONDecodeError` → invalid lines go to DLQ instead of silently dropped
- `producer.produce()` + `producer.poll(0)` → same non-blocking publish pattern
- `producer.flush()` on completion and `KeyboardInterrupt` → unchanged
- `delivery_report` callback → `_delivery_callback` in `kafka_producer.py`

Hardcoded IP `192.168.59.128:9092` is replaced by `KAFKA_BOOTSTRAP_SERVERS` env var.
Topic `windturbine-data` is replaced by `windturbine-raw` (configurable via `KAFKA_TOPIC_RAW`).

### `v2_Kafka_consumer_checkdata.py` → `scripts/kafka_inspect.py --tail`

The original debug consumer pattern (print every message) is now:

```bash
# Equivalent to running the original consumer against windturbine-raw
python scripts/kafka_inspect.py --tail raw 20

# See the original windturbine-data topic (if still exists)
python scripts/kafka_inspect.py --tail windturbine-data 10
```

The production consumer loop (`consumer.poll()` + `msg.error()` + commit) lives in `services/processor/main.py` and `services/db_writer/main.py`.

### `TimeScaleDB_Queries.txt` → `infra/postgres/init/01_schema.sql`

Every object from the original SQL is preserved:

| Original | Platform Equivalent | Notes |
|---|---|---|
| `wind_turbine_streamdata` | `turbine_telemetry` | UUID PK, dedup constraint, 7 new columns |
| `create_hypertable(... 'timestamp')` | `create_hypertable(... 'recorded_at')` | Same concept, canonical column name |
| `wind_turbine_5min_avg` | `turbine_metrics_5min` | Original 3 metrics + 8 more, auto-refreshed |
| `AVG(power)` | `avg_power_kw` | Same metric, renamed |
| `AVG(wind_speed)` | `avg_wind_speed_ms` | Same metric, renamed |
| `AVG(generator_speed)` | `avg_generator_speed` | Same name preserved exactly |
| `compress_segmentby = 'turbine_id'` | Same | Unchanged |
| `add_retention_policy(365 days)` | `add_retention_policy(90 days)` | More aggressive default |
| `add_compression_policy(30 days)` | `add_compression_policy(7 days)` | More aggressive default |

The `turbine_metrics_1h` hourly aggregate is added alongside the 5-minute one. A `turbine_thermal_health` view is added for maintenance queries covering all temperature sensors.

---

## New Anomaly Rules (Extended Sensor Fields)

Six new rules fire on the bearing, gearbox, and gear temperature fields:

| Rule | Sensor Field | Threshold | Severity | Basis |
|---|---|---|---|---|
| `bearing_temp_warning` | `bearing_temp_c` | > 60°C | WARNING | Reduced oil film thickness |
| `bearing_temp_critical` | `bearing_temp_c` | > 70°C | CRITICAL | Seizure risk (top of original range) |
| `gearbox_sump_temp_warning` | `gearbox_sump_temp_c` | > 95°C | WARNING | ISO VG 320 viscosity loss |
| `gearbox_sump_temp_critical` | `gearbox_sump_temp_c` | > 120°C | CRITICAL | Oil oxidation risk |
| `gear_temp_warning` | `gear_temp_c` | > 250°C | WARNING | Inadequate lubrication at mesh |
| `gear_temp_critical` | `gear_temp_c` | > 320°C | CRITICAL | Gear tooth tempering risk |

---

## Verifying Extended Sensor Data

```bash
# Check extended sensor readings per turbine (bearing, gearbox, nacelle, etc.)
python scripts/db_inspect.py --extended

# Thermal health status across all turbines
python scripts/db_inspect.py --thermal

# Check both continuous aggregates (5min and 1h)
python scripts/db_inspect.py --aggregates

# Verify extended sensor alert rules are firing
curl "http://localhost:8000/alerts/recent" | python3 -m json.tool | grep -E "bearing|gearbox|gear_temp"

# Thermal health endpoint
curl http://localhost:8000/turbines/thermal | python3 -m json.tool

# Direct SQL for extended sensor data
psql "postgresql://wt_user:wt_password@localhost:5432/windturbine" -c "
SELECT turbine_id, bearing_temp_c, gearbox_sump_temp_c, gear_temp_c, thermal_status
FROM turbine_thermal_health ORDER BY turbine_id;"

# Verify 5-minute aggregate (from original TimeScaleDB_Queries.txt)
psql "postgresql://wt_user:wt_password@localhost:5432/windturbine" -c "
SELECT bucket, turbine_id, avg_power_kw, avg_wind_speed_ms, avg_generator_speed
FROM turbine_metrics_5min ORDER BY bucket DESC LIMIT 24;"
```
