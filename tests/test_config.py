"""
tests/test_config.py

Tests for environment-based configuration loading.
"""

from __future__ import annotations

import os
import pytest


class TestKafkaSettings:
    def test_defaults_load(self):
        os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        from shared.config import KafkaSettings
        cfg = KafkaSettings()
        assert cfg.kafka_topic_raw == "windturbine-raw"
        assert cfg.kafka_topic_alerts == "windturbine-alerts"
        assert cfg.kafka_topic_dlq == "windturbine-dlq"

    def test_producer_idempotence_default(self):
        from shared.config import KafkaSettings
        cfg = KafkaSettings()
        assert cfg.kafka_producer_enable_idempotence is True

    def test_producer_acks_default(self):
        from shared.config import KafkaSettings
        cfg = KafkaSettings()
        assert cfg.kafka_producer_acks == "all"


class TestDatabaseSettings:
    def test_dsn_property_format(self):
        os.environ.setdefault("DB_HOST", "localhost")
        os.environ.setdefault("DB_USER", "wt_user")
        os.environ.setdefault("DB_PASSWORD", "wt_password")
        os.environ.setdefault("DB_NAME", "windturbine")
        from shared.config import DatabaseSettings
        cfg = DatabaseSettings()
        assert cfg.dsn.startswith("postgresql://")
        assert "wt_user" in cfg.dsn
        assert "windturbine" in cfg.dsn

    def test_asyncpg_dsn_uses_correct_driver(self):
        from shared.config import DatabaseSettings
        cfg = DatabaseSettings()
        assert "asyncpg" in cfg.asyncpg_dsn


class TestProducerSettings:
    def test_turbine_id_list_parsing(self):
        os.environ["PRODUCER_TURBINE_IDS"] = "t-01,t-02,t-03"
        from shared.config import ProducerSettings
        cfg = ProducerSettings()
        assert cfg.turbine_id_list == ["t-01", "t-02", "t-03"]

    def test_turbine_id_list_strips_whitespace(self):
        os.environ["PRODUCER_TURBINE_IDS"] = "t-01, t-02 , t-03"
        from shared.config import ProducerSettings
        cfg = ProducerSettings()
        assert "t-02" in cfg.turbine_id_list
