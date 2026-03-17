"""
services/ops_api/main.py

Operational API — health, status, and summary endpoints.

Endpoints:
  GET /health          — liveness probe (always 200 if process is up)
  GET /ready           — readiness probe (checks Kafka + DB connectivity)
  GET /status          — platform operational summary
  GET /turbines        — recent turbine status from DB
  GET /alerts/recent   — recent anomaly alerts
  GET /metrics/summary — aggregated metric snapshot for dashboards
"""

from __future__ import annotations

import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from confluent_kafka.admin import AdminClient

from shared.config import get_database_settings, get_kafka_settings, get_ops_api_settings
from shared.logging_config import setup_logging
from shared.metrics import SERVICE_UP, start_metrics_server

logger = setup_logging("ops_api", json_logs=True)

kafka_cfg = get_kafka_settings()
db_cfg = get_database_settings()
ops_cfg = get_ops_api_settings()


# ─── Pydantic response models ─────────────────────────────────────────────────


class HealthResponse(BaseModel):
    status: str
    service: str
    ts: datetime


class ReadinessResponse(BaseModel):
    ready: bool
    kafka: bool
    database: bool
    ts: datetime
    details: Dict[str, Any] = {}


class TurbineStatusRow(BaseModel):
    turbine_id: str
    last_seen: Optional[datetime]
    last_power_kw: Optional[float]
    last_wind_ms: Optional[float]
    last_temp_c: Optional[float]
    last_status: Optional[str]


class AlertRow(BaseModel):
    alert_id: str
    turbine_id: str
    recorded_at: datetime
    severity: str
    rule_name: str
    message: str
    metric_value: float


class MetricsSummary(BaseModel):
    turbine_count: int
    alerts_last_hour: int
    critical_alerts_last_hour: int
    avg_power_kw: Optional[float]
    avg_wind_ms: Optional[float]
    ts: datetime


# ─── DB helpers ───────────────────────────────────────────────────────────────


def _get_db_conn():
    return psycopg2.connect(
        host=db_cfg.db_host,
        port=db_cfg.db_port,
        dbname=db_cfg.db_name,
        user=db_cfg.db_user,
        password=db_cfg.db_password,
        connect_timeout=5,
    )


def _check_kafka() -> bool:
    try:
        admin = AdminClient(
            {
                "bootstrap.servers": kafka_cfg.kafka_bootstrap_servers,
                "socket.timeout.ms": 3000,
            }
        )
        meta = admin.list_topics(timeout=3)
        return len(meta.topics) >= 0
    except Exception as exc:
        logger.warning(f"Kafka health check failed: {exc}")
        return False


def _check_db() -> bool:
    try:
        conn = _get_db_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return True
    except Exception as exc:
        logger.warning(f"DB health check failed: {exc}")
        return False


# ─── App setup ────────────────────────────────────────────────────────────────

_startup_time = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Ops API starting")
    start_metrics_server(ops_cfg.ops_api_metrics_port, service_name="ops_api")
    SERVICE_UP.labels(service_name="ops_api").set(1)
    yield
    SERVICE_UP.labels(service_name="ops_api").set(0)
    logger.info("Ops API stopped")


app = FastAPI(
    title="Wind Turbine Platform — Ops API",
    description="Operational health, status, and summary endpoints for the telemetry platform.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ─── Endpoints ────────────────────────────────────────────────────────────────


@app.get("/health", response_model=HealthResponse, tags=["ops"])
async def health():
    """Liveness probe — returns 200 if process is running."""
    return HealthResponse(
        status="ok",
        service="ops_api",
        ts=datetime.now(timezone.utc),
    )


@app.get("/ready", response_model=ReadinessResponse, tags=["ops"])
async def ready():
    """Readiness probe — checks Kafka and database connectivity."""
    kafka_ok = _check_kafka()
    db_ok = _check_db()
    ready = kafka_ok and db_ok
    return ReadinessResponse(
        ready=ready,
        kafka=kafka_ok,
        database=db_ok,
        ts=datetime.now(timezone.utc),
        details={
            "kafka_bootstrap": kafka_cfg.kafka_bootstrap_servers,
            "db_host": db_cfg.db_host,
            "uptime_seconds": round(time.time() - _startup_time, 1),
        },
    )


@app.get("/status", tags=["ops"])
async def status():
    """Platform-wide operational summary."""
    return {
        "platform": "Wind Turbine Telemetry Platform",
        "version": "1.0.0",
        "uptime_seconds": round(time.time() - _startup_time, 1),
        "topics": {
            "raw": kafka_cfg.kafka_topic_raw,
            "alerts": kafka_cfg.kafka_topic_alerts,
            "dlq": kafka_cfg.kafka_topic_dlq,
        },
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/turbines", response_model=List[TurbineStatusRow], tags=["data"])
async def get_turbine_status():
    """Most recent telemetry reading per turbine — uses turbine_latest_status view."""
    sql = """
        SELECT turbine_id,
               last_seen,
               last_power_kw,
               last_wind_ms,
               last_temp_c,
               status AS last_status
        FROM turbine_latest_status
        ORDER BY turbine_id
    """
    conn = None
    try:
        conn = _get_db_conn()
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return [TurbineStatusRow(**dict(zip(cols, row))) for row in rows]
    except Exception as exc:
        logger.error(f"Failed to query turbine status: {exc}")
        raise HTTPException(status_code=503, detail="Database unavailable")
    finally:
        if conn:
            conn.close()


@app.get("/alerts/recent", response_model=List[AlertRow], tags=["data"])
async def get_recent_alerts(limit: int = 50, severity: Optional[str] = None):
    """Recent anomaly alerts, newest first. Filter by severity=WARNING|CRITICAL."""
    sql = """
        SELECT alert_id, turbine_id, recorded_at, severity,
               rule_name, message, metric_value
        FROM turbine_alerts
        {where}
        ORDER BY recorded_at DESC
        LIMIT %s
    """
    params: list = [min(limit, 500)]
    where = ""
    if severity:
        where = "WHERE severity = %s"
        params = [severity.upper()] + params

    conn = None
    try:
        conn = _get_db_conn()
        with conn.cursor() as cur:
            cur.execute(sql.format(where=where), params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return [AlertRow(**dict(zip(cols, row))) for row in rows]
    except Exception as exc:
        logger.error(f"Failed to query alerts: {exc}")
        raise HTTPException(status_code=503, detail="Database unavailable")
    finally:
        if conn:
            conn.close()


@app.get("/metrics/summary", response_model=MetricsSummary, tags=["data"])
async def get_metrics_summary():
    """High-level metrics snapshot for use in dashboards."""
    sql = """
        SELECT
            COUNT(DISTINCT turbine_id) AS turbine_count,
            AVG(power_output_kw)       AS avg_power_kw,
            AVG(wind_speed_ms)         AS avg_wind_ms
        FROM turbine_telemetry
        WHERE recorded_at > NOW() - INTERVAL '5 minutes'
    """
    alert_sql = """
        SELECT
            COUNT(*)                                     AS total,
            COUNT(*) FILTER (WHERE severity = 'CRITICAL') AS critical
        FROM turbine_alerts
        WHERE alert_ts > NOW() - INTERVAL '1 hour'
    """
    conn = None
    try:
        conn = _get_db_conn()
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            turbine_count = row[0] or 0
            avg_power = round(float(row[1]), 1) if row[1] else None
            avg_wind = round(float(row[2]), 2) if row[2] else None

            cur.execute(alert_sql)
            arow = cur.fetchone()
            alerts_1h = arow[0] or 0
            critical_1h = arow[1] or 0
        return MetricsSummary(
            turbine_count=turbine_count,
            alerts_last_hour=alerts_1h,
            critical_alerts_last_hour=critical_1h,
            avg_power_kw=avg_power,
            avg_wind_ms=avg_wind,
            ts=datetime.now(timezone.utc),
        )
    except Exception as exc:
        logger.error(f"Failed to query metrics summary: {exc}")
        raise HTTPException(status_code=503, detail="Database unavailable")
    finally:
        if conn:
            conn.close()


@app.get("/turbines/thermal", tags=["data"])
async def get_thermal_health():
    """
    Thermal health status for all turbines.
    Uses the turbine_thermal_health view which covers bearing, gearbox,
    gear, and generator temperatures from the original sensor field set.
    """
    sql = """
        SELECT turbine_id, recorded_at, bearing_temp_c,
               gearbox_sump_temp_c, gear_temp_c, temperature_c,
               thermal_status
        FROM turbine_thermal_health
        ORDER BY turbine_id
    """
    conn = None
    try:
        conn = _get_db_conn()
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in rows]
    except Exception as exc:
        logger.error(f"Failed to query thermal health: {exc}")
        raise HTTPException(status_code=503, detail="Database unavailable")
    finally:
        if conn:
            conn.close()


def main() -> None:
    uvicorn.run(
        "services.ops_api.main:app",
        host=ops_cfg.ops_api_host,
        port=ops_cfg.ops_api_port,
        log_level="warning",
        access_log=False,
    )


if __name__ == "__main__":
    main()
