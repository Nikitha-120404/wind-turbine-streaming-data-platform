"""
shared/config.py

Centralised environment-based configuration using pydantic-settings.
Every service imports the Settings object it needs from here.
No magic strings scattered across the codebase.
"""

from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = Field("localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_raw: str = Field("windturbine-raw", alias="KAFKA_TOPIC_RAW")
    kafka_topic_alerts: str = Field("windturbine-alerts", alias="KAFKA_TOPIC_ALERTS")
    kafka_topic_dlq: str = Field("windturbine-dlq", alias="KAFKA_TOPIC_DLQ")
    kafka_consumer_group_processor: str = Field(
        "wt-stream-processor", alias="KAFKA_CONSUMER_GROUP_PROCESSOR"
    )
    kafka_consumer_group_db_writer: str = Field(
        "wt-db-writer", alias="KAFKA_CONSUMER_GROUP_DB_WRITER"
    )
    kafka_num_partitions_raw: int = Field(6, alias="KAFKA_NUM_PARTITIONS_RAW")
    kafka_num_partitions_alerts: int = Field(3, alias="KAFKA_NUM_PARTITIONS_ALERTS")
    kafka_num_partitions_dlq: int = Field(2, alias="KAFKA_NUM_PARTITIONS_DLQ")
    # Producer reliability settings
    kafka_producer_acks: str = Field("all", alias="KAFKA_PRODUCER_ACKS")
    kafka_producer_retries: int = Field(5, alias="KAFKA_PRODUCER_RETRIES")
    kafka_producer_retry_backoff_ms: int = Field(300, alias="KAFKA_PRODUCER_RETRY_BACKOFF_MS")
    kafka_producer_max_in_flight: int = Field(1, alias="KAFKA_PRODUCER_MAX_IN_FLIGHT")
    kafka_producer_enable_idempotence: bool = Field(
        True, alias="KAFKA_PRODUCER_ENABLE_IDEMPOTENCE"
    )


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    db_host: str = Field("localhost", alias="DB_HOST")
    db_port: int = Field(5432, alias="DB_PORT")
    db_name: str = Field("windturbine", alias="DB_NAME")
    db_user: str = Field("wt_user", alias="DB_USER")
    db_password: str = Field("wt_password", alias="DB_PASSWORD")
    db_pool_min: int = Field(2, alias="DB_POOL_MIN")
    db_pool_max: int = Field(10, alias="DB_POOL_MAX")
    db_connect_timeout: int = Field(10, alias="DB_CONNECT_TIMEOUT")

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def asyncpg_dsn(self) -> str:
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


class ProducerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    producer_turbine_ids: str = Field(
        "turbine-01,turbine-02,turbine-03,turbine-04,turbine-05,turbine-06,"
        "turbine-07,turbine-08,turbine-09,turbine-10,turbine-11,turbine-12",
        alias="PRODUCER_TURBINE_IDS",
    )
    producer_emit_interval_seconds: float = Field(
        1.0, alias="PRODUCER_EMIT_INTERVAL_SECONDS"
    )
    producer_metrics_port: int = Field(8001, alias="PRODUCER_METRICS_PORT")

    @property
    def turbine_id_list(self) -> List[str]:
        return [t.strip() for t in self.producer_turbine_ids.split(",") if t.strip()]


class ProcessorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    processor_metrics_port: int = Field(8002, alias="PROCESSOR_METRICS_PORT")
    processor_max_poll_records: int = Field(500, alias="PROCESSOR_MAX_POLL_RECORDS")
    processor_session_timeout_ms: int = Field(30000, alias="PROCESSOR_SESSION_TIMEOUT_MS")


class DbWriterSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    db_writer_metrics_port: int = Field(8003, alias="DB_WRITER_METRICS_PORT")
    db_writer_batch_size: int = Field(100, alias="DB_WRITER_BATCH_SIZE")
    db_writer_flush_interval_seconds: float = Field(
        2.0, alias="DB_WRITER_FLUSH_INTERVAL_SECONDS"
    )


class OpsApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ops_api_host: str = Field("0.0.0.0", alias="OPS_API_HOST")
    ops_api_port: int = Field(8000, alias="OPS_API_PORT")
    ops_api_metrics_port: int = Field(8004, alias="OPS_API_METRICS_PORT")


@lru_cache()
def get_kafka_settings() -> KafkaSettings:
    return KafkaSettings()


@lru_cache()
def get_database_settings() -> DatabaseSettings:
    return DatabaseSettings()


@lru_cache()
def get_producer_settings() -> ProducerSettings:
    return ProducerSettings()


@lru_cache()
def get_processor_settings() -> ProcessorSettings:
    return ProcessorSettings()


@lru_cache()
def get_db_writer_settings() -> DbWriterSettings:
    return DbWriterSettings()


@lru_cache()
def get_ops_api_settings() -> OpsApiSettings:
    return OpsApiSettings()
