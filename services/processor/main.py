"""
services/processor/main.py

Stream processor service.

Lifecycle per message:
1. Deserialise JSON payload
2. Validate against TurbineTelemetry schema
3. Run anomaly detection rules
4. Publish any alerts to windturbine-alerts topic
5. Commit offset only after successful processing
6. Route failures to DLQ

Consumer group: wt-stream-processor
This service does NOT write to the database — that is the db_writer's responsibility.
Separation of concerns: this service is stateless and horizontally scalable.
"""

from __future__ import annotations

import json
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from shared.config import get_kafka_settings, get_processor_settings
from shared.logging_config import setup_logging
from shared.metrics import (
    DLQ_MESSAGES_TOTAL,
    EVENTS_CONSUMED_TOTAL,
    EVENTS_PROCESSING_FAILED_TOTAL,
    PROCESSING_LATENCY_SECONDS,
    start_metrics_server,
)
from shared.schema import DLQEvent, TurbineAlert, TurbineTelemetry
from pydantic import ValidationError

from services.processor.anomaly_detector import detect_anomalies

logger = setup_logging("processor", json_logs=True)


class StreamProcessor:
    def __init__(self):
        self._kafka_cfg = get_kafka_settings()
        self._proc_cfg = get_processor_settings()
        self._running = False
        self._consumer: Consumer | None = None
        self._alert_producer: Producer | None = None
        self._dlq_producer: Producer | None = None

    def _build_consumer(self) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": self._kafka_cfg.kafka_bootstrap_servers,
                "group.id": self._kafka_cfg.kafka_consumer_group_processor,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,  # manual commit for at-least-once
                "max.poll.interval.ms": 300_000,
                "session.timeout.ms": self._proc_cfg.processor_session_timeout_ms,
                "max.partition.fetch.bytes": 1_048_576,
            }
        )

    def _build_producer(self) -> Producer:
        return Producer(
            {
                "bootstrap.servers": self._kafka_cfg.kafka_bootstrap_servers,
                "acks": "all",
                "retries": 5,
                "enable.idempotence": True,
                "compression.type": "lz4",
            }
        )

    def _publish_alert(self, alert: TurbineAlert) -> None:
        payload = alert.model_dump_json().encode("utf-8")
        self._alert_producer.produce(  # type: ignore[union-attr]
            topic=self._kafka_cfg.kafka_topic_alerts,
            key=alert.turbine_id.encode("utf-8"),
            value=payload,
        )
        self._alert_producer.poll(0)  # type: ignore[union-attr]

    def _publish_dlq(
        self, raw: str, error_type: str, error_msg: str, partition: int, offset: int
    ) -> None:
        dlq = DLQEvent(
            original_topic=self._kafka_cfg.kafka_topic_raw,
            original_partition=partition,
            original_offset=offset,
            raw_payload=raw,
            error_type=error_type,
            error_message=error_msg,
        )
        self._dlq_producer.produce(  # type: ignore[union-attr]
            topic=self._kafka_cfg.kafka_topic_dlq,
            value=dlq.model_dump_json().encode("utf-8"),
        )
        self._dlq_producer.poll(0)  # type: ignore[union-attr]
        DLQ_MESSAGES_TOTAL.labels(
            source_topic=self._kafka_cfg.kafka_topic_raw, error_type=error_type
        ).inc()

    def _process_message(self, msg) -> None:
        raw = msg.value().decode("utf-8")
        partition = msg.partition()
        offset = msg.offset()

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error(f"JSON decode error at {partition}:{offset}: {exc}")
            self._publish_dlq(raw, "json_decode_error", str(exc), partition, offset)
            EVENTS_PROCESSING_FAILED_TOTAL.labels(
                consumer_group=self._kafka_cfg.kafka_consumer_group_processor,
                error_type="json_decode_error",
            ).inc()
            return

        try:
            event = TurbineTelemetry.model_validate(data)
        except ValidationError as exc:
            logger.error(
                f"Schema validation failed at {partition}:{offset}",
                extra={"errors": exc.errors()},
            )
            self._publish_dlq(raw, "schema_validation_error", str(exc), partition, offset)
            EVENTS_PROCESSING_FAILED_TOTAL.labels(
                consumer_group=self._kafka_cfg.kafka_consumer_group_processor,
                error_type="validation_error",
            ).inc()
            return

        # Track end-to-end latency
        now = time.time()
        event_ts = event.recorded_at.timestamp()
        latency = now - event_ts
        PROCESSING_LATENCY_SECONDS.labels(
            consumer_group=self._kafka_cfg.kafka_consumer_group_processor
        ).observe(latency)

        EVENTS_CONSUMED_TOTAL.labels(
            consumer_group=self._kafka_cfg.kafka_consumer_group_processor,
            topic=self._kafka_cfg.kafka_topic_raw,
        ).inc()

        # Run anomaly detection
        alerts = detect_anomalies(event)
        for alert in alerts:
            try:
                self._publish_alert(alert)
            except Exception as exc:
                logger.error(f"Failed to publish alert: {exc}", exc_info=True)

        logger.debug(
            f"Processed event",
            extra={
                "turbine_id": event.turbine_id,
                "event_id": event.event_id,
                "alerts_generated": len(alerts),
                "latency_s": round(latency, 3),
            },
        )

    def start(self) -> None:
        logger.info("Stream processor starting")

        start_metrics_server(
            self._proc_cfg.processor_metrics_port, service_name="processor"
        )

        self._consumer = self._build_consumer()
        self._alert_producer = self._build_producer()
        self._dlq_producer = self._build_producer()

        self._consumer.subscribe([self._kafka_cfg.kafka_topic_raw])
        self._running = True

        logger.info(
            f"Subscribed to {self._kafka_cfg.kafka_topic_raw}, "
            f"group={self._kafka_cfg.kafka_consumer_group_processor}"
        )

        while self._running:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"End of partition: {msg.topic()}[{msg.partition()}]")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                self._process_message(msg)
                self._consumer.commit(message=msg, asynchronous=False)
            except Exception as exc:
                logger.error(f"Fatal error processing message: {exc}", exc_info=True)
                # Don't commit — message will be reprocessed after rebalance

    def stop(self) -> None:
        logger.info("Stream processor stopping")
        self._running = False
        if self._consumer:
            self._consumer.close()
        if self._alert_producer:
            self._alert_producer.flush(10)
        if self._dlq_producer:
            self._dlq_producer.flush(10)
        logger.info("Stream processor stopped")


def main() -> None:
    service = StreamProcessor()

    def _handle_signal(signum, frame):
        service.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    service.start()


if __name__ == "__main__":
    main()
