"""
tests/conftest.py

Shared pytest fixtures for the Wind Turbine Platform test suite.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Ensure the project root is on PYTHONPATH so tests can import shared + services
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.schema import TurbineStatus, TurbineTelemetry


@pytest.fixture
def valid_telemetry_payload() -> dict:
    """A complete, valid telemetry payload dict."""
    return {
        "turbine_id": "turbine-01",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "wind_speed_ms": 9.5,
        "rotor_rpm": 12.3,
        "power_output_kw": 1450.0,
        "temperature_c": 42.0,
        "vibration_mms": 1.8,
        "blade_pitch_deg": 12.0,
        "status": "online",
    }


@pytest.fixture
def valid_telemetry_event(valid_telemetry_payload) -> TurbineTelemetry:
    """A validated TurbineTelemetry instance."""
    return TurbineTelemetry.model_validate(valid_telemetry_payload)


@pytest.fixture
def high_temp_event(valid_telemetry_payload) -> TurbineTelemetry:
    payload = {**valid_telemetry_payload, "temperature_c": 91.0}
    return TurbineTelemetry.model_validate(payload)


@pytest.fixture
def critical_temp_event(valid_telemetry_payload) -> TurbineTelemetry:
    payload = {**valid_telemetry_payload, "temperature_c": 97.0}
    return TurbineTelemetry.model_validate(payload)


@pytest.fixture
def high_vibration_event(valid_telemetry_payload) -> TurbineTelemetry:
    payload = {**valid_telemetry_payload, "vibration_mms": 7.5}
    return TurbineTelemetry.model_validate(payload)


@pytest.fixture
def critical_vibration_event(valid_telemetry_payload) -> TurbineTelemetry:
    payload = {**valid_telemetry_payload, "vibration_mms": 10.2}
    return TurbineTelemetry.model_validate(payload)


@pytest.fixture
def downtime_event(valid_telemetry_payload) -> TurbineTelemetry:
    """Turbine appears stopped despite good wind — unplanned downtime."""
    payload = {
        **valid_telemetry_payload,
        "wind_speed_ms": 10.0,
        "rotor_rpm": 0.0,
        "power_output_kw": 0.0,
        "status": "fault",
    }
    return TurbineTelemetry.model_validate(payload)


@pytest.fixture
def low_power_event(valid_telemetry_payload) -> TurbineTelemetry:
    """Wind is present, rotor is spinning, but power is abnormally low."""
    payload = {
        **valid_telemetry_payload,
        "wind_speed_ms": 8.0,
        "rotor_rpm": 11.0,
        "power_output_kw": 30.0,
    }
    return TurbineTelemetry.model_validate(payload)


@pytest.fixture
def power_dropout_event(valid_telemetry_payload) -> TurbineTelemetry:
    """RPM present, strong wind, but power has dropped to near zero."""
    payload = {
        **valid_telemetry_payload,
        "wind_speed_ms": 12.0,
        "rotor_rpm": 14.0,
        "power_output_kw": 2.0,
    }
    return TurbineTelemetry.model_validate(payload)


@pytest.fixture
def overspeed_event(valid_telemetry_payload) -> TurbineTelemetry:
    payload = {**valid_telemetry_payload, "rotor_rpm": 19.5}
    return TurbineTelemetry.model_validate(payload)


@pytest.fixture
def normal_event(valid_telemetry_payload) -> TurbineTelemetry:
    """A completely normal event that should trigger zero alerts."""
    return TurbineTelemetry.model_validate(valid_telemetry_payload)
