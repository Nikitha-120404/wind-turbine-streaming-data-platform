"""
services/db_writer/main.py

Database writer service.

Consumes from:
- windturbine-raw  → inserts into turbine_telemetry (hypertable)
- windturbine-alerts → inserts into turbine_alerts

Design choices:
- Micro-batch inserts (configurable size + time window) for throughput
- executemany with COPY-style batching via psycopg2
- ON CONFLICT DO NOTHING for idempotent inserts (at-least-once delivery safe)
- Separate consumer group from processor — both services read the raw topic independently
- Manual offset commit after successful DB flush
"""

from __future__ import annotations

import json
import signal
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import List

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pydantic import ValidationError

from shared.config import get_database_settings, get_db_writer_settings, get_kafka_settings
from shared.logging_config import setup_logging
from shared.metrics import (
    DB_BATCH_SIZE,
    DB_INSERT_FAILURES_TOTAL,
    DB_INSERTS_TOTAL,
    DB_WRITE_LATENCY_SECONDS,
    DLQ_MESSAGES_TOTAL,
    EVENTS_CONSUMED_TOTAL,
    start_metrics_server,
)
from shared.schema import TurbineAlert, TurbineTelemetry

logger = setup_logging("db_writer", json_logs=True)

INSERT_TELEMETRY_SQL = """
    INSERT INTO turbine_telemetry (
        event_id, turbine_id, recorded_at,
        wind_speed_ms, rotor_rpm, power_output_kw,
        temperature_c, vibration_mms, blade_pitch_deg, status,
        nacelle_position_deg, wind_direction_deg, ambient_temp_c,
        bearing_temp_c, gearbox_sump_temp_c, gear_temp_c, hub_speed
    ) VALUES %s
    ON CONFLICT (turbine_id, recorded_at) DO NOTHING
"""

INSERT_ALERT_SQL = """
    INSERT INTO turbine_alerts (
        alert_id, turbine_id, recorded_at, severity,
        rule_name, message, metric_name, metric_value, threshold,
        source_event_id, alert_ts
    ) VALUES %s
    ON CONFLICT (alert_id) DO NOTHING
"""


def _telemetry_to_tuple(event: TurbineTelemetry) -> tuple:
    return (
        event.event_id,
        event.turbine_id,
        event.recorded_at,
        event.wind_speed_ms,
        event.rotor_rpm,
        event.power_output_kw,
        event.temperature_c,
        event.vibration_mms,
        event.blade_pitch_deg,
        event.status.value,
        # Extended sensor fields from original wind_turbine_sensorlog.py
        # NULL is inserted when not present (legacy events without these fields)
        event.nacelle_position_deg,
        event.wind_direction_deg,
        event.ambient_temp_c,
        event.bearing_temp_c,
        event.gearbox_sump_temp_c,
        event.gear_temp_c,
        event.hub_speed,
    )


def _alert_to_tuple(alert: TurbineAlert) -> tuple:
    return (
        alert.alert_id,
        alert.turbine_id,
        alert.recorded_at,
        alert.severity.value,
        alert.rule_name,
        alert.message,
        alert.metric_name,
        alert.metric_value,
        alert.threshold,
        alert.source_event_id,
        alert.alert_ts,
    )


class DbWriterService:
    def __init__(self):
        self._kafka_cfg = get_kafka_settings()
        self._db_cfg = get_database_settings()
        self._writer_cfg = get_db_writer_settings()
        self._running = False
        self._conn: psycopg2.extensions.connection | None = None
        self._raw_consumer: Consumer | None = None
        self._alert_consumer: Consumer | None = None

        # In-memory batches
        self._telemetry_batch: List[tuple] = []
        self._alert_batch: List[tuple] = []
        self._pending_raw_msgs = []
        self._pending_alert_msgs = []
        self._last_flush = time.monotonic()

    def _get_connection(self) -> psycopg2.extensions.connection:
        conn = psycopg2.connect(
            host=self._db_cfg.db_host,
            port=self._db_cfg.db_port,
            dbname=self._db_cfg.db_name,
            user=self._db_cfg.db_user,
            password=self._db_cfg.db_password,
            connect_timeout=self._db_cfg.db_connect_timeout,
        )
        conn.autocommit = False
        return conn

    def _ensure_connection(self) -> None:
        if self._conn is None or self._conn.closed:
            logger.info("Opening database connection")
            self._conn = self._get_connection()

    def _build_consumer(self, group_id: str, topics: List[str]) -> Consumer:
        c = Consumer(
            {
                "bootstrap.servers": self._kafka_cfg.kafka_bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 30_000,
            }
        )
        c.subscribe(topics)
        return c

    def _flush_telemetry(self) -> None:
        if not self._telemetry_batch:
            return
        batch = self._telemetry_batch[:]
        t0 = time.monotonic()
        try:
            self._ensure_connection()
            with self._conn.cursor() as cur:  # type: ignore[union-attr]
                psycopg2.extras.execute_values(cur, INSERT_TELEMETRY_SQL, batch, page_size=200)
            self._conn.commit()  # type: ignore[union-attr]
            elapsed = time.monotonic() - t0
            DB_INSERTS_TOTAL.labels(table_name="turbine_telemetry").inc(len(batch))
            DB_BATCH_SIZE.labels(table_name="turbine_telemetry").observe(len(batch))
            DB_WRITE_LATENCY_SECONDS.labels(table_name="turbine_telemetry").observe(elapsed)
            logger.info(
                f"Flushed {len(batch)} telemetry rows in {elapsed:.3f}s"
            )
            # Commit Kafka offsets
            for msg in self._pending_raw_msgs:
                self._raw_consumer.commit(message=msg, asynchronous=False)  # type: ignore
            self._telemetry_batch.clear()
            self._pending_raw_msgs.clear()
        except Exception as exc:
            logger.error(f"Telemetry batch insert failed: {exc}", exc_info=True)
            DB_INSERT_FAILURES_TOTAL.labels(
                table_name="turbine_telemetry", error_type=type(exc).__name__
            ).inc()
            if self._conn and not self._conn.closed:
                self._conn.rollback()
            self._conn = None  # force reconnect on next attempt

    def _flush_alerts(self) -> None:
        if not self._alert_batch:
            return
        batch = self._alert_batch[:]
        t0 = time.monotonic()
        try:
            self._ensure_connection()
            with self._conn.cursor() as cur:  # type: ignore[union-attr]
                psycopg2.extras.execute_values(cur, INSERT_ALERT_SQL, batch, page_size=100)
            self._conn.commit()  # type: ignore[union-attr]
            elapsed = time.monotonic() - t0
            DB_INSERTS_TOTAL.labels(table_name="turbine_alerts").inc(len(batch))
            DB_BATCH_SIZE.labels(table_name="turbine_alerts").observe(len(batch))
            DB_WRITE_LATENCY_SECONDS.labels(table_name="turbine_alerts").observe(elapsed)
            logger.info(f"Flushed {len(batch)} alert rows in {elapsed:.3f}s")
            for msg in self._pending_alert_msgs:
                self._alert_consumer.commit(message=msg, asynchronous=False)  # type: ignore
            self._alert_batch.clear()
            self._pending_alert_msgs.clear()
        except Exception as exc:
            logger.error(f"Alert batch insert failed: {exc}", exc_info=True)
            DB_INSERT_FAILURES_TOTAL.labels(
                table_name="turbine_alerts", error_type=type(exc).__name__
            ).inc()
            if self._conn and not self._conn.closed:
                self._conn.rollback()
            self._conn = None

    def _should_flush(self) -> bool:
        time_elapsed = (
            time.monotonic() - self._last_flush
        ) >= self._writer_cfg.db_writer_flush_interval_seconds
        batch_full = (
            len(self._telemetry_batch) >= self._writer_cfg.db_writer_batch_size
            or len(self._alert_batch) >= self._writer_cfg.db_writer_batch_size
        )
        return time_elapsed or batch_full

    def _poll_consumers(self) -> None:
        # Poll raw telemetry consumer
        msg = self._raw_consumer.poll(timeout=0.1)  # type: ignore
        if msg and not msg.error():
            try:
                data = json.loads(msg.value().decode("utf-8"))
                event = TurbineTelemetry.model_validate(data)
                self._telemetry_batch.append(_telemetry_to_tuple(event))
                self._pending_raw_msgs.append(msg)
                EVENTS_CONSUMED_TOTAL.labels(
                    consumer_group=self._kafka_cfg.kafka_consumer_group_db_writer,
                    topic=self._kafka_cfg.kafka_topic_raw,
                ).inc()
            except (json.JSONDecodeError, ValidationError) as exc:
                logger.warning(f"Skipping invalid telemetry message: {exc}")

        # Poll alerts consumer
        msg = self._alert_consumer.poll(timeout=0.1)  # type: ignore
        if msg and not msg.error():
            try:
                data = json.loads(msg.value().decode("utf-8"))
                alert = TurbineAlert.model_validate(data)
                self._alert_batch.append(_alert_to_tuple(alert))
                self._pending_alert_msgs.append(msg)
            except (json.JSONDecodeError, ValidationError) as exc:
                logger.warning(f"Skipping invalid alert message: {exc}")

    def start(self) -> None:
        logger.info("DB writer service starting")

        start_metrics_server(
            self._writer_cfg.db_writer_metrics_port, service_name="db_writer"
        )

        self._raw_consumer = self._build_consumer(
            group_id=self._kafka_cfg.kafka_consumer_group_db_writer,
            topics=[self._kafka_cfg.kafka_topic_raw],
        )
        self._alert_consumer = self._build_consumer(
            group_id="wt-alert-writer",
            topics=[self._kafka_cfg.kafka_topic_alerts],
        )

        self._ensure_connection()
        self._running = True
        self._last_flush = time.monotonic()

        logger.info("DB writer ready — entering consume loop")

        while self._running:
            self._poll_consumers()
            if self._should_flush():
                self._flush_telemetry()
                self._flush_alerts()
                self._last_flush = time.monotonic()

        # Final flush on shutdown
        self._flush_telemetry()
        self._flush_alerts()

    def stop(self) -> None:
        logger.info("DB writer stopping")
        self._running = False

    def close(self) -> None:
        if self._raw_consumer:
            self._raw_consumer.close()
        if self._alert_consumer:
            self._alert_consumer.close()
        if self._conn and not self._conn.closed:
            self._conn.close()
        logger.info("DB writer closed")


def main() -> None:
    service = DbWriterService()

    def _handle_signal(signum, frame):
        service.stop()
        service.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        service.start()
    finally:
        service.close()


if __name__ == "__main__":
    main()
