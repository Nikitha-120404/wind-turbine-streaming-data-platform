"""
tests/test_schema.py

Schema validation tests for TurbineTelemetry and related models.
Covers valid payloads, field boundary conditions, and coercion logic.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from shared.schema import DLQEvent, TurbineAlert, TurbineTelemetry, TurbineStatus, AlertSeverity


class TestTurbineTelemetryValid:
    def test_valid_payload_passes(self, valid_telemetry_payload):
        event = TurbineTelemetry.model_validate(valid_telemetry_payload)
        assert event.turbine_id == "turbine-01"
        assert event.status == TurbineStatus.ONLINE

    def test_event_id_auto_generated(self, valid_telemetry_payload):
        event = TurbineTelemetry.model_validate(valid_telemetry_payload)
        assert event.event_id is not None
        assert len(event.event_id) == 36  # UUID format

    def test_producer_ts_auto_set(self, valid_telemetry_payload):
        event = TurbineTelemetry.model_validate(valid_telemetry_payload)
        assert event.producer_ts is not None

    def test_recorded_at_string_parsed(self, valid_telemetry_payload):
        event = TurbineTelemetry.model_validate(valid_telemetry_payload)
        assert isinstance(event.recorded_at, datetime)

    def test_partition_key_returns_turbine_id(self, valid_telemetry_event):
        assert valid_telemetry_event.partition_key() == "turbine-01"

    def test_boundary_wind_speed_zero(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "wind_speed_ms": 0.0}
        event = TurbineTelemetry.model_validate(payload)
        assert event.wind_speed_ms == 0.0

    def test_boundary_wind_speed_max(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "wind_speed_ms": 100.0}
        event = TurbineTelemetry.model_validate(payload)
        assert event.wind_speed_ms == 100.0

    def test_all_statuses_accepted(self, valid_telemetry_payload):
        for status in TurbineStatus:
            payload = {**valid_telemetry_payload, "status": status.value}
            event = TurbineTelemetry.model_validate(payload)
            assert event.status == status

    def test_model_dump_json_roundtrip(self, valid_telemetry_event):
        json_str = valid_telemetry_event.model_dump_json()
        restored = TurbineTelemetry.model_validate_json(json_str)
        assert restored.event_id == valid_telemetry_event.event_id
        assert restored.turbine_id == valid_telemetry_event.turbine_id


class TestTurbineTelemetryInvalid:
    def test_missing_turbine_id_raises(self, valid_telemetry_payload):
        payload = {k: v for k, v in valid_telemetry_payload.items() if k != "turbine_id"}
        with pytest.raises(ValidationError) as exc_info:
            TurbineTelemetry.model_validate(payload)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("turbine_id",) for e in errors)

    def test_blank_turbine_id_raises(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "turbine_id": "   "}
        with pytest.raises(ValidationError):
            TurbineTelemetry.model_validate(payload)

    def test_negative_wind_speed_raises(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "wind_speed_ms": -1.0}
        with pytest.raises(ValidationError):
            TurbineTelemetry.model_validate(payload)

    def test_wind_speed_exceeds_max_raises(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "wind_speed_ms": 101.0}
        with pytest.raises(ValidationError):
            TurbineTelemetry.model_validate(payload)

    def test_negative_rpm_raises(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "rotor_rpm": -0.1}
        with pytest.raises(ValidationError):
            TurbineTelemetry.model_validate(payload)

    def test_rpm_exceeds_max_raises(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "rotor_rpm": 31.0}
        with pytest.raises(ValidationError):
            TurbineTelemetry.model_validate(payload)

    def test_temperature_too_high_raises(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "temperature_c": 151.0}
        with pytest.raises(ValidationError):
            TurbineTelemetry.model_validate(payload)

    def test_temperature_too_low_raises(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "temperature_c": -41.0}
        with pytest.raises(ValidationError):
            TurbineTelemetry.model_validate(payload)

    def test_invalid_status_raises(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "status": "BROKEN"}
        with pytest.raises(ValidationError):
            TurbineTelemetry.model_validate(payload)

    def test_missing_required_field_raises(self, valid_telemetry_payload):
        for required_field in ["wind_speed_ms", "rotor_rpm", "power_output_kw", "temperature_c"]:
            payload = {k: v for k, v in valid_telemetry_payload.items() if k != required_field}
            with pytest.raises(ValidationError):
                TurbineTelemetry.model_validate(payload)

    def test_completely_empty_payload_raises(self):
        with pytest.raises(ValidationError):
            TurbineTelemetry.model_validate({})


class TestTurbineTelemetryCoercion:
    def test_downtime_coerces_status_to_fault(self, valid_telemetry_payload):
        """
        When wind > 3 m/s, RPM ≈ 0, power ≈ 0, and status is 'online',
        the model validator should coerce status to FAULT.
        """
        payload = {
            **valid_telemetry_payload,
            "wind_speed_ms": 8.0,
            "rotor_rpm": 0.0,
            "power_output_kw": 0.0,
            "status": "online",
        }
        event = TurbineTelemetry.model_validate(payload)
        assert event.status == TurbineStatus.FAULT

    def test_low_wind_online_not_coerced(self, valid_telemetry_payload):
        """Below cut-in wind speed, zero power/RPM is normal — status stays online."""
        payload = {
            **valid_telemetry_payload,
            "wind_speed_ms": 2.0,
            "rotor_rpm": 0.0,
            "power_output_kw": 0.0,
            "status": "online",
        }
        event = TurbineTelemetry.model_validate(payload)
        assert event.status == TurbineStatus.ONLINE


class TestDLQEvent:
    def test_dlq_event_creation(self):
        dlq = DLQEvent(
            original_topic="windturbine-raw",
            raw_payload='{"bad": "data"}',
            error_type="json_decode_error",
            error_message="Expecting value",
        )
        assert dlq.dlq_id is not None
        assert dlq.retry_count == 0

    def test_dlq_roundtrip(self):
        dlq = DLQEvent(
            original_topic="windturbine-raw",
            raw_payload="garbage",
            error_type="validation_error",
            error_message="Field required",
        )
        restored = DLQEvent.model_validate_json(dlq.model_dump_json())
        assert restored.dlq_id == dlq.dlq_id


class TestTurbineAlert:
    def test_alert_creation(self, valid_telemetry_event):
        alert = TurbineAlert(
            turbine_id=valid_telemetry_event.turbine_id,
            recorded_at=valid_telemetry_event.recorded_at,
            severity=AlertSeverity.WARNING,
            rule_name="nacelle_temp_warning",
            message="Temperature too high",
            metric_name="temperature_c",
            metric_value=88.0,
            threshold=85.0,
            source_event_id=valid_telemetry_event.event_id,
        )
        assert alert.alert_id is not None
        assert alert.severity == AlertSeverity.WARNING
