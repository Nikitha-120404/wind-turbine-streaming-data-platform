#!/usr/bin/env python3
"""
scripts/kafka_inspect.py

Inspect Kafka topics and consumer group lag for the Wind Turbine Platform.
Run from the project root with: python scripts/kafka_inspect.py

Usage:
  python scripts/kafka_inspect.py --topics         # list topics and partition info
  python scripts/kafka_inspect.py --groups         # show consumer group lag
  python scripts/kafka_inspect.py --tail raw 10    # print last N messages from a topic
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from shared.config import get_kafka_settings

cfg = get_kafka_settings()
TOPIC_MAP = {
    "raw": cfg.kafka_topic_raw,
    "alerts": cfg.kafka_topic_alerts,
    "dlq": cfg.kafka_topic_dlq,
}


def list_topics(admin: AdminClient) -> None:
    meta = admin.list_topics(timeout=10)
    wt_topics = {
        name: t
        for name, t in meta.topics.items()
        if name.startswith("windturbine")
    }
    print(f"\n{'Topic':<35} {'Partitions':>10} {'Error'}")
    print("─" * 60)
    for name, topic in sorted(wt_topics.items()):
        err = str(topic.error) if topic.error else "none"
        print(f"{name:<35} {len(topic.partitions):>10} {err}")
    print()


def show_consumer_groups(admin: AdminClient) -> None:
    groups = ["wt-stream-processor", "wt-db-writer", "wt-alert-writer"]
    print(f"\n{'Group':<30} {'Topic':<30} {'Partition':>9} {'Lag':>8}")
    print("─" * 80)

    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap_servers,
            "group.id": "_inspector",
            "auto.offset.reset": "latest",
        }
    )

    for group in groups:
        for topic_alias, topic_name in TOPIC_MAP.items():
            try:
                # Get committed offsets and high watermarks to compute lag
                meta = consumer.list_topics(topic_name, timeout=5)
                if topic_name not in meta.topics:
                    continue
                partitions = list(meta.topics[topic_name].partitions.keys())

                from confluent_kafka import TopicPartition
                tps = [TopicPartition(topic_name, p) for p in partitions]

                committed = consumer.committed(tps, timeout=5)
                low_high = [consumer.get_watermark_offsets(tp, timeout=5) for tp in tps]

                for tp, (low, high) in zip(committed, low_high):
                    committed_offset = tp.offset if tp.offset >= 0 else low
                    lag = max(0, high - committed_offset)
                    print(f"{group:<30} {topic_name:<30} {tp.partition:>9} {lag:>8}")
            except Exception as exc:
                print(f"  [error for {group}/{topic_name}]: {exc}")
    consumer.close()
    print()


def tail_topic(topic_alias: str, n: int) -> None:
    topic_name = TOPIC_MAP.get(topic_alias, topic_alias)
    print(f"\nLast {n} messages from topic: {topic_name}\n{'─'*60}")
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap_servers,
            "group.id": "_tail_inspector",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic_name])

    # Seek to end minus n (approximate)
    from confluent_kafka import TopicPartition, OFFSET_END
    meta = consumer.list_topics(topic_name, timeout=5)
    if topic_name not in meta.topics:
        print(f"Topic {topic_name} not found")
        consumer.close()
        return

    messages_collected = []
    timeout_counter = 0
    consumer.subscribe([topic_name])

    # Poll briefly to get assignment then rewind
    consumer.poll(2.0)
    assignment = consumer.assignment()
    watermarks = {tp: consumer.get_watermark_offsets(tp, timeout=5) for tp in assignment}

    for tp, (low, high) in watermarks.items():
        seek_to = max(low, high - n)
        consumer.seek(TopicPartition(tp.topic, tp.partition, seek_to))

    while len(messages_collected) < n:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            timeout_counter += 1
            if timeout_counter > 3:
                break
            continue
        if msg.error():
            break
        timeout_counter = 0
        try:
            data = json.loads(msg.value().decode("utf-8"))
            messages_collected.append((msg.partition(), msg.offset(), data))
        except Exception:
            messages_collected.append((msg.partition(), msg.offset(), msg.value()))

    consumer.close()

    for partition, offset, data in messages_collected[-n:]:
        print(f"[p={partition} o={offset}] {json.dumps(data, indent=2, default=str)}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka inspector for Wind Turbine Platform")
    parser.add_argument("--topics", action="store_true", help="List topic info")
    parser.add_argument("--groups", action="store_true", help="Show consumer group lag")
    parser.add_argument("--tail", nargs=2, metavar=("TOPIC", "N"), help="Tail N messages")
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": cfg.kafka_bootstrap_servers})

    if args.topics:
        list_topics(admin)
    elif args.groups:
        show_consumer_groups(admin)
    elif args.tail:
        tail_topic(args.tail[0], int(args.tail[1]))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
