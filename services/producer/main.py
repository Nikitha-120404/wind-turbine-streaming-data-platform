"""
services/producer/main.py

Producer service entrypoint.

Lifecycle:
1. Load config from environment
2. Ensure Kafka topics exist
3. Start Prometheus metrics HTTP server
4. Run simulation loop — emit events at configured interval
5. Graceful shutdown on SIGINT / SIGTERM
"""

from __future__ import annotations

import signal
import sys
import time
from pathlib import Path

# Allow running directly or as a module
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from shared.config import get_kafka_settings, get_producer_settings
from shared.kafka_admin import ensure_topics, get_standard_topics
from shared.logging_config import setup_logging
from shared.metrics import start_metrics_server

from services.producer.kafka_producer import TelemetryProducer
from services.producer.simulator import TurbineFleet

logger = setup_logging("producer", json_logs=True)


class ProducerService:
    def __init__(self):
        self._kafka_cfg = get_kafka_settings()
        self._producer_cfg = get_producer_settings()
        self._running = False
        self._producer: TelemetryProducer | None = None
        self._fleet: TurbineFleet | None = None

    def start(self) -> None:
        logger.info(
            "Producer service starting",
            extra={
                "turbines": self._producer_cfg.turbine_id_list,
                "interval_s": self._producer_cfg.producer_emit_interval_seconds,
            },
        )

        # Topic setup
        ensure_topics(
            self._kafka_cfg.kafka_bootstrap_servers,
            get_standard_topics(
                self._kafka_cfg.kafka_topic_raw,
                self._kafka_cfg.kafka_topic_alerts,
                self._kafka_cfg.kafka_topic_dlq,
                self._kafka_cfg.kafka_num_partitions_raw,
                self._kafka_cfg.kafka_num_partitions_alerts,
                self._kafka_cfg.kafka_num_partitions_dlq,
            ),
        )

        # Metrics server
        start_metrics_server(
            self._producer_cfg.producer_metrics_port, service_name="producer"
        )

        self._producer = TelemetryProducer(self._kafka_cfg)
        self._fleet = TurbineFleet(self._producer_cfg.turbine_id_list)
        self._running = True

        logger.info("Producer service ready — starting emit loop")

        while self._running:
            loop_start = time.monotonic()
            try:
                events = self._fleet.emit_all()
                for event in events:
                    success = self._producer.publish_telemetry(event)
                    if not success:
                        # Validation or publish failure — already logged and metered
                        pass
            except Exception as exc:
                logger.error(f"Unexpected error in emit loop: {exc}", exc_info=True)

            elapsed = time.monotonic() - loop_start
            sleep_for = max(
                0.0,
                self._producer_cfg.producer_emit_interval_seconds - elapsed,
            )
            time.sleep(sleep_for)

    def stop(self) -> None:
        logger.info("Producer service stopping — flushing remaining messages")
        self._running = False
        if self._producer:
            remaining = self._producer.flush(timeout=15.0)
            if remaining:
                logger.warning(f"{remaining} messages were not delivered before shutdown")
        logger.info("Producer service stopped")


def main() -> None:
    service = ProducerService()

    def _handle_signal(signum, frame):
        logger.info(f"Received signal {signum} — initiating graceful shutdown")
        service.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    service.start()


if __name__ == "__main__":
    main()
