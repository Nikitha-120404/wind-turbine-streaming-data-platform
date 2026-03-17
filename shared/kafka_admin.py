"""
shared/kafka_admin.py

Kafka topic setup utility.
Run at startup to ensure all required topics exist with correct configuration.
Safe to call multiple times — idempotent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

logger = logging.getLogger(__name__)


@dataclass
class TopicSpec:
    name: str
    num_partitions: int
    replication_factor: int = 1
    config: Optional[Dict[str, str]] = None


def ensure_topics(bootstrap_servers: str, topics: List[TopicSpec]) -> None:
    """
    Create Kafka topics if they do not already exist.
    Existing topics with matching config are left untouched.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [t for t in topics if t.name not in existing]

    if not to_create:
        logger.info("All Kafka topics already exist — nothing to create")
        return

    new_topics = [
        NewTopic(
            t.name,
            num_partitions=t.num_partitions,
            replication_factor=t.replication_factor,
            config=t.config or {},
        )
        for t in to_create
    ]

    futures = admin.create_topics(new_topics)
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info(f"Created Kafka topic: {topic_name}")
        except Exception as exc:
            if "already exists" in str(exc).lower():
                logger.debug(f"Topic already exists (race): {topic_name}")
            else:
                logger.error(f"Failed to create topic {topic_name}: {exc}")
                raise


def get_standard_topics(
    topic_raw: str,
    topic_alerts: str,
    topic_dlq: str,
    partitions_raw: int = 6,
    partitions_alerts: int = 3,
    partitions_dlq: int = 2,
) -> List[TopicSpec]:
    """Return the standard set of platform topics."""
    return [
        TopicSpec(
            name=topic_raw,
            num_partitions=partitions_raw,
            config={
                "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
                "compression.type": "lz4",
                "cleanup.policy": "delete",
            },
        ),
        TopicSpec(
            name=topic_alerts,
            num_partitions=partitions_alerts,
            config={
                "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
                "compression.type": "lz4",
            },
        ),
        TopicSpec(
            name=topic_dlq,
            num_partitions=partitions_dlq,
            config={
                "retention.ms": str(14 * 24 * 60 * 60 * 1000),  # 14 days
                "compression.type": "lz4",
            },
        ),
    ]
