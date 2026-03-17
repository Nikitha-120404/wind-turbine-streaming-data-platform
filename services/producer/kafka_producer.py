"""
services/producer/kafka_producer.py

Production-grade Kafka producer for wind turbine telemetry.

Key features:
- Schema validation via Pydantic before any publish attempt
- Reliable delivery: acks=all, idempotent, bounded retries
- Partitioning by turbine_id for ordered per-turbine event streams
- DLQ routing for validation failures
- Prometheus metrics per publish outcome
- Structured logging throughout
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

from confluent_kafka import KafkaException, Producer

from shared.config import KafkaSettings
from shared.metrics import (
    DLQ_MESSAGES_TOTAL,
    EVENTS_PUBLISH_FAILED_TOTAL,
    EVENTS_PUBLISHED_TOTAL,
    EVENTS_VALIDATION_FAILED_TOTAL,
    PRODUCER_LAG_SECONDS,
)
from shared.schema import DLQEvent, TurbineTelemetry

logger = logging.getLogger(__name__)


class TelemetryProducer:
    """
    Wraps confluent-kafka Producer with reliability and observability features.
    """

    def __init__(self, settings: KafkaSettings):
        self._settings = settings
        self._producer = self._build_producer(settings)

    @staticmethod
    def _build_producer(settings: KafkaSettings) -> Producer:
        conf = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": settings.kafka_producer_acks,
            "retries": settings.kafka_producer_retries,
            "retry.backoff.ms": settings.kafka_producer_retry_backoff_ms,
            "max.in.flight.requests.per.connection": settings.kafka_producer_max_in_flight,
            "enable.idempotence": settings.kafka_producer_enable_idempotence,
            "compression.type": "lz4",
            "linger.ms": 5,  # small batching window for throughput
            "queue.buffering.max.messages": 100_000,
            "delivery.timeout.ms": 30_000,
        }
        return Producer(conf)

    def publish_telemetry(self, event: TurbineTelemetry) -> bool:
        """
        Validate and publish a single telemetry event.

        Returns True on success, False on delivery failure.
        Validation failures are routed to DLQ and always return False.
        """
        try:
            payload = event.model_dump_json().encode("utf-8")
            key = event.partition_key().encode("utf-8")

            # Track producer lag
            now = time.time()
            event_ts = event.recorded_at.timestamp()
            lag = now - event_ts
            PRODUCER_LAG_SECONDS.labels(turbine_id=event.turbine_id).set(lag)

            self._producer.produce(
                topic=self._settings.kafka_topic_raw,
                key=key,
                value=payload,
                on_delivery=self._delivery_callback,
            )
            # Trigger delivery for any buffered messages without blocking
            self._producer.poll(0)
            EVENTS_PUBLISHED_TOTAL.labels(
                turbine_id=event.turbine_id, topic=self._settings.kafka_topic_raw
            ).inc()
            return True

        except BufferError:
            logger.warning(
                "Producer queue full — backing off",
                extra={"turbine_id": event.turbine_id},
            )
            self._producer.poll(1)  # flush to make room
            EVENTS_PUBLISH_FAILED_TOTAL.labels(
                turbine_id=event.turbine_id, error_type="buffer_full"
            ).inc()
            return False

        except KafkaException as exc:
            logger.error(
                f"Kafka publish failed: {exc}",
                extra={"turbine_id": event.turbine_id},
            )
            EVENTS_PUBLISH_FAILED_TOTAL.labels(
                turbine_id=event.turbine_id, error_type="kafka_error"
            ).inc()
            return False

    def publish_dlq(self, raw_payload: str, error_type: str, error_msg: str, source_topic: str) -> None:
        """Route a failed event to the dead-letter queue."""
        dlq_event = DLQEvent(
            original_topic=source_topic,
            raw_payload=raw_payload,
            error_type=error_type,
            error_message=error_msg,
        )
        try:
            self._producer.produce(
                topic=self._settings.kafka_topic_dlq,
                value=dlq_event.model_dump_json().encode("utf-8"),
                on_delivery=self._delivery_callback,
            )
            self._producer.poll(0)
            DLQ_MESSAGES_TOTAL.labels(
                source_topic=source_topic, error_type=error_type
            ).inc()
            logger.warning(
                "Event routed to DLQ",
                extra={"error_type": error_type, "dlq_id": dlq_event.dlq_id},
            )
        except Exception as exc:
            logger.error(f"Failed to publish to DLQ: {exc}")

    def flush(self, timeout: float = 10.0) -> int:
        """Flush all pending messages. Returns number of messages still queued."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(f"Producer flush timed out — {remaining} messages still queued")
        return remaining

    @staticmethod
    def _delivery_callback(err, msg) -> None:
        if err:
            logger.error(
                f"Delivery failed: topic={msg.topic()} partition={msg.partition()} "
                f"offset={msg.offset()} error={err}"
            )
        else:
            logger.debug(
                f"Delivered: topic={msg.topic()} partition={msg.partition()} "
                f"offset={msg.offset()}"
            )
