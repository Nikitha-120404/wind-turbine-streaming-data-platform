"""
tests/test_ops_api.py

Unit tests for the FastAPI ops API.
Uses httpx.AsyncClient — does not require a running database or Kafka.
DB and Kafka calls are mocked.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from unittest.mock import MagicMock, patch

# Import the app — patching env before import to prevent real config reads
import os
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "test")
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.setdefault("DB_NAME", "test")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("OPS_API_METRICS_PORT", "18004")

from services.ops_api.main import app


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health_returns_200(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_health_body_structure(self, client):
        resp = await client.get("/health")
        body = resp.json()
        assert body["status"] == "ok"
        assert body["service"] == "ops_api"
        assert "ts" in body


class TestReadinessEndpoint:
    @pytest.mark.asyncio
    async def test_ready_returns_200(self, client):
        with patch("services.ops_api.main._check_kafka", return_value=True), \
             patch("services.ops_api.main._check_db", return_value=True):
            resp = await client.get("/ready")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_ready_body_when_all_up(self, client):
        with patch("services.ops_api.main._check_kafka", return_value=True), \
             patch("services.ops_api.main._check_db", return_value=True):
            resp = await client.get("/ready")
        body = resp.json()
        assert body["ready"] is True
        assert body["kafka"] is True
        assert body["database"] is True

    @pytest.mark.asyncio
    async def test_ready_body_when_kafka_down(self, client):
        with patch("services.ops_api.main._check_kafka", return_value=False), \
             patch("services.ops_api.main._check_db", return_value=True):
            resp = await client.get("/ready")
        body = resp.json()
        assert body["ready"] is False
        assert body["kafka"] is False
        assert body["database"] is True

    @pytest.mark.asyncio
    async def test_ready_body_when_db_down(self, client):
        with patch("services.ops_api.main._check_kafka", return_value=True), \
             patch("services.ops_api.main._check_db", return_value=False):
            resp = await client.get("/ready")
        body = resp.json()
        assert body["ready"] is False


class TestStatusEndpoint:
    @pytest.mark.asyncio
    async def test_status_returns_200(self, client):
        resp = await client.get("/status")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_status_has_platform_name(self, client):
        resp = await client.get("/status")
        assert "Wind Turbine" in resp.json()["platform"]

    @pytest.mark.asyncio
    async def test_status_has_topic_names(self, client):
        resp = await client.get("/status")
        topics = resp.json()["topics"]
        assert "raw" in topics
        assert "alerts" in topics
        assert "dlq" in topics


class TestTurbinesEndpoint:
    @pytest.mark.asyncio
    async def test_turbines_returns_503_when_db_down(self, client):
        with patch("services.ops_api.main._get_db_conn", side_effect=Exception("no db")):
            resp = await client.get("/turbines")
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_turbines_returns_list(self, client):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = []
        mock_conn.cursor.return_value = mock_cursor

        with patch("services.ops_api.main._get_db_conn", return_value=mock_conn):
            resp = await client.get("/turbines")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestAlertsEndpoint:
    @pytest.mark.asyncio
    async def test_alerts_returns_503_when_db_down(self, client):
        with patch("services.ops_api.main._get_db_conn", side_effect=Exception("no db")):
            resp = await client.get("/alerts/recent")
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_alerts_limit_param(self, client):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = []
        mock_conn.cursor.return_value = mock_cursor

        with patch("services.ops_api.main._get_db_conn", return_value=mock_conn):
            resp = await client.get("/alerts/recent?limit=10")
        assert resp.status_code == 200
