#!/usr/bin/env python3
"""
scripts/dlq_consumer.py

Inspect and optionally replay events from the dead-letter queue.

Usage:
  python scripts/dlq_consumer.py --watch          # tail DLQ live
  python scripts/dlq_consumer.py --summary        # count by error_type
  python scripts/dlq_consumer.py --replay --dry-run   # show what would be replayed
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka import Consumer, Producer

from shared.config import get_kafka_settings
from shared.schema import DLQEvent

cfg = get_kafka_settings()


def watch_dlq(timeout_seconds: int = 30) -> None:
    """Print DLQ messages as they arrive."""
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap_servers,
            "group.id": "_dlq_watcher",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([cfg.kafka_topic_dlq])
    print(f"Watching DLQ topic: {cfg.kafka_topic_dlq} (Ctrl+C to stop)\n")

    try:
        empty_polls = 0
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                empty_polls += 1
                if empty_polls > timeout_seconds:
                    print("No DLQ messages received — exiting.")
                    break
                continue
            empty_polls = 0
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                dlq = DLQEvent.model_validate(data)
                print(
                    f"[{dlq.failed_at}] error_type={dlq.error_type}\n"
                    f"  message={dlq.error_message}\n"
                    f"  topic={dlq.original_topic} partition={dlq.original_partition} offset={dlq.original_offset}\n"
                    f"  payload_preview={dlq.raw_payload[:120]}...\n"
                )
            except Exception as exc:
                print(f"Failed to parse DLQ message: {exc}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def summarise_dlq() -> None:
    """Read all DLQ messages and show error_type breakdown."""
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap_servers,
            "group.id": "_dlq_summary",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([cfg.kafka_topic_dlq])

    error_counts: Counter = Counter()
    total = 0

    print(f"Reading DLQ from beginning: {cfg.kafka_topic_dlq}")
    try:
        empty = 0
        while empty < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                empty += 1
                continue
            empty = 0
            if msg.error():
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                error_type = data.get("error_type", "unknown")
                error_counts[error_type] += 1
                total += 1
            except Exception:
                pass
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"\nTotal DLQ messages read: {total}")
    print(f"\n{'Error Type':<35} {'Count':>8}")
    print("─" * 45)
    for error_type, count in error_counts.most_common():
        print(f"{error_type:<35} {count:>8}")
    print()


def replay_dlq(dry_run: bool = True) -> None:
    """
    Attempt to re-publish DLQ messages back to their original topic.
    Only replays events whose raw_payload is valid JSON.
    With --dry-run, prints what would happen without publishing.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap_servers,
            "group.id": "_dlq_replayer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": cfg.kafka_bootstrap_servers})
    consumer.subscribe([cfg.kafka_topic_dlq])

    replayed = 0
    skipped = 0
    action = "Would replay" if dry_run else "Replaying"

    print(f"{'[DRY RUN] ' if dry_run else ''}Replaying DLQ messages...\n")

    try:
        empty = 0
        while empty < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                empty += 1
                continue
            empty = 0
            if msg.error():
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                dlq = DLQEvent.model_validate(data)

                # Only replay if raw_payload is parseable JSON
                try:
                    json.loads(dlq.raw_payload)
                except json.JSONDecodeError:
                    print(f"  Skipping binary/invalid payload: {dlq.dlq_id}")
                    skipped += 1
                    continue

                print(f"  {action}: {dlq.original_topic} | error={dlq.error_type}")
                if not dry_run:
                    producer.produce(
                        topic=dlq.original_topic,
                        value=dlq.raw_payload.encode("utf-8"),
                    )
                    producer.poll(0)
                replayed += 1
            except Exception as exc:
                print(f"  Error processing DLQ event: {exc}")
                skipped += 1
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        if not dry_run:
            producer.flush(10)

    print(f"\nSummary: {action.lower()} {replayed}, skipped {skipped}")


def main() -> None:
    parser = argparse.ArgumentParser(description="DLQ manager for Wind Turbine Platform")
    parser.add_argument("--watch", action="store_true", help="Watch DLQ live")
    parser.add_argument("--summary", action="store_true", help="Show error_type counts")
    parser.add_argument("--replay", action="store_true", help="Replay DLQ to original topics")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Replay without publishing (default)")
    parser.add_argument("--execute", action="store_true", help="Actually execute replay (disables dry-run)")
    args = parser.parse_args()

    if args.watch:
        watch_dlq()
    elif args.summary:
        summarise_dlq()
    elif args.replay:
        replay_dlq(dry_run=not args.execute)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
