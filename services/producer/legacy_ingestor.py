"""
services/producer/legacy_ingestor.py

Legacy log file ingestor — direct evolution of:
    v2_kafka_producer_windt_sensorlog_to_kafka.py

PURPOSE:
    Reads an existing wind_turbine.log file (produced by the original
    wind_turbine_sensorlog.py) and feeds every JSON line into the production
    Kafka pipeline via the same TelemetryProducer used by the live simulator.

ORIGINAL LOGIC PRESERVED:
    ✓  Read log file line by line
    ✓  json.loads() each line, skip empty/invalid lines (json.JSONDecodeError)
    ✓  producer.produce() with delivery callback
    ✓  producer.poll(0) between messages (non-blocking)
    ✓  producer.flush() at end / on KeyboardInterrupt

IMPROVEMENTS OVER ORIGINAL:
    +  Schema validation via TurbineTelemetry.model_validate() — malformed
       events are routed to the DLQ instead of silently dropped
    +  Field name normalisation — original PascalCase fields (Turbine_ID,
       Wind_Speed, Generator_Speed, etc.) are automatically mapped to canonical
       names by the model_validator in shared/schema.py
    +  Partitioning by turbine_id (original used no partition key)
    +  Structured logging instead of bare print()
    +  Prometheus metrics on published / failed / DLQ counts
    +  Config from environment instead of hardcoded IP

HOW TO USE:
    # Process a log file that was produced by the original simulator:
    LOG_FILE=/path/to/wind_turbine.log python -m services.producer.legacy_ingestor

    # Or with Docker:
    docker run --rm -v /path/to/wind_turbine.log:/data/wind_turbine.log \\
        -e LOG_FILE=/data/wind_turbine.log \\
        -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \\
        wt_producer python -m services.producer.legacy_ingestor
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pydantic import ValidationError

from shared.config import get_kafka_settings
from shared.logging_config import setup_logging
from shared.schema import TurbineTelemetry

from services.producer.kafka_producer import TelemetryProducer

logger = setup_logging("legacy_ingestor", json_logs=True)


def ingest_log_file(log_file_path: str) -> None:
    """
    Read a wind_turbine.log file and publish every valid event to Kafka.

    This is the direct equivalent of the original producer loop:

        with open(log_file_path, "r") as logfile:
            for line in logfile:
                json_data = json.loads(line)
                producer.produce(TOPIC_NAME, value=json.dumps(json_data), callback=delivery_report)
                producer.poll(0)
        producer.flush()

    The difference is that events are now validated and normalised before
    publishing, and failures go to the DLQ instead of being discarded.
    """
    kafka_cfg = get_kafka_settings()
    producer = TelemetryProducer(kafka_cfg)

    if not os.path.exists(log_file_path):
        logger.error(f"Log file not found: {log_file_path}")
        sys.exit(1)

    published = 0
    skipped_empty = 0
    validation_failures = 0
    json_failures = 0

    logger.info(
        f"Starting legacy log ingestion",
        extra={
            "log_file": log_file_path,
            "topic": kafka_cfg.kafka_topic_raw,
            "broker": kafka_cfg.kafka_bootstrap_servers,
        },
    )

    try:
        with open(log_file_path, "r", encoding="utf-8") as logfile:
            for line_num, line in enumerate(logfile, start=1):

                # ── Original logic: strip and skip blank lines ────────────────
                line = line.strip()
                if not line:
                    skipped_empty += 1
                    continue

                # ── Original logic: json.loads() with JSONDecodeError handling ─
                try:
                    raw_data = json.loads(line)
                except json.JSONDecodeError as exc:
                    # Original: print(f"Invalid JSON line skipped: {line}")
                    # Improved: log structured warning + route to DLQ
                    logger.warning(
                        f"Invalid JSON at line {line_num} — routing to DLQ",
                        extra={"error": str(exc), "preview": line[:120]},
                    )
                    producer.publish_dlq(
                        raw_payload=line,
                        error_type="json_decode_error",
                        error_msg=str(exc),
                        source_topic=kafka_cfg.kafka_topic_raw,
                    )
                    json_failures += 1
                    continue

                # ── NEW: schema validation + field normalisation ───────────────
                # The model_validator in TurbineTelemetry.normalise_legacy_fields()
                # handles all the PascalCase → snake_case mapping automatically:
                #   Turbine_ID → turbine_id (with "Turbine_03" → "turbine-03")
                #   Wind_Speed → wind_speed_ms
                #   Generator_Speed → rotor_rpm (with gearbox ratio scaling)
                #   Power → power_output_kw
                #   GeneratorTemp → temperature_c
                #   BladePitchAngle → blade_pitch_deg (clamped 0–90)
                #   Timestamp → recorded_at (parsed from "%Y-%m-%d %H:%M:%S")
                #   + all 7 extended sensor fields
                try:
                    event = TurbineTelemetry.model_validate(raw_data)
                except ValidationError as exc:
                    logger.warning(
                        f"Schema validation failed at line {line_num} — routing to DLQ",
                        extra={"errors": exc.errors(), "preview": line[:120]},
                    )
                    producer.publish_dlq(
                        raw_payload=line,
                        error_type="schema_validation_error",
                        error_msg=str(exc),
                        source_topic=kafka_cfg.kafka_topic_raw,
                    )
                    validation_failures += 1
                    continue

                # ── Original logic: producer.produce() + producer.poll(0) ──────
                success = producer.publish_telemetry(event)
                if success:
                    published += 1
                    if published % 500 == 0:
                        logger.info(f"Progress: {published} events published")

        # ── Original logic: producer.flush() after all lines processed ────────
        remaining = producer.flush(timeout=15.0)
        logger.info(
            "Log ingestion complete",
            extra={
                "published": published,
                "validation_failures": validation_failures,
                "json_failures": json_failures,
                "skipped_empty": skipped_empty,
                "unflushed": remaining,
            },
        )
        # Original: print("All messages sent!")
        print(
            f"All messages sent! "
            f"published={published}  failures={validation_failures + json_failures}  "
            f"skipped_empty={skipped_empty}"
        )

    except KeyboardInterrupt:
        # ── Original logic: graceful Ctrl+C with final flush ──────────────────
        # Original: print("\nStopping ingestion...") / producer.flush()
        logger.info("Ingestion interrupted by user — flushing remaining messages")
        producer.flush(timeout=10.0)
        print(f"\nStopping ingestion... {published} events published before stop.")


def main() -> None:
    log_file = os.environ.get("LOG_FILE", "wind_turbine.log")
    ingest_log_file(log_file)


if __name__ == "__main__":
    main()
