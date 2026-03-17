"""
shared/metrics.py

Prometheus metrics definitions shared across the platform.
Each service imports only the metrics it needs.
Metrics are registered on the default registry.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ── Producer metrics ──────────────────────────────────────────────────────────

EVENTS_PUBLISHED_TOTAL = Counter(
    "wt_events_published_total",
    "Total telemetry events successfully published to Kafka",
    ["turbine_id", "topic"],
)

EVENTS_PUBLISH_FAILED_TOTAL = Counter(
    "wt_events_publish_failed_total",
    "Total telemetry events that failed to publish (after retries)",
    ["turbine_id", "error_type"],
)

EVENTS_VALIDATION_FAILED_TOTAL = Counter(
    "wt_events_validation_failed_total",
    "Total events that failed schema validation before publish",
    ["turbine_id", "error_type"],
)

PRODUCER_LAG_SECONDS = Gauge(
    "wt_producer_lag_seconds",
    "Seconds between event recorded_at and producer publish time",
    ["turbine_id"],
)

# ── Consumer / processor metrics ──────────────────────────────────────────────

EVENTS_CONSUMED_TOTAL = Counter(
    "wt_events_consumed_total",
    "Total events consumed from Kafka",
    ["consumer_group", "topic"],
)

EVENTS_PROCESSING_FAILED_TOTAL = Counter(
    "wt_events_processing_failed_total",
    "Total events that failed processing and were routed to DLQ",
    ["consumer_group", "error_type"],
)

PROCESSING_LATENCY_SECONDS = Histogram(
    "wt_processing_latency_seconds",
    "Time from event recorded_at to processor completion",
    ["consumer_group"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)

# ── Anomaly detection metrics ─────────────────────────────────────────────────

ANOMALY_ALERTS_TOTAL = Counter(
    "wt_anomaly_alerts_total",
    "Total anomaly alerts generated",
    ["turbine_id", "rule_name", "severity"],
)

# ── Dead-letter queue metrics ─────────────────────────────────────────────────

DLQ_MESSAGES_TOTAL = Counter(
    "wt_dlq_messages_total",
    "Total messages sent to the dead-letter queue",
    ["source_topic", "error_type"],
)

# ── Database writer metrics ───────────────────────────────────────────────────

DB_INSERTS_TOTAL = Counter(
    "wt_db_inserts_total",
    "Total rows successfully inserted into TimescaleDB",
    ["table_name"],
)

DB_INSERT_FAILURES_TOTAL = Counter(
    "wt_db_insert_failures_total",
    "Total row insert failures",
    ["table_name", "error_type"],
)

DB_BATCH_SIZE = Histogram(
    "wt_db_batch_size",
    "Number of rows per database flush batch",
    ["table_name"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500],
)

DB_WRITE_LATENCY_SECONDS = Histogram(
    "wt_db_write_latency_seconds",
    "Latency of database batch insert operations",
    ["table_name"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

# ── Operational metrics ───────────────────────────────────────────────────────

SERVICE_UP = Gauge(
    "wt_service_up",
    "Whether the service is running and healthy (1=up, 0=down)",
    ["service_name"],
)


def start_metrics_server(port: int, service_name: str) -> None:
    """Start Prometheus HTTP metrics endpoint on the given port."""
    start_http_server(port)
    SERVICE_UP.labels(service_name=service_name).set(1)
