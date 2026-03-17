"""
tests/test_legacy_integration.py

Tests for the integration of original wind_turbine_sensorlog.py field names
into the new platform schema, and for the new extended-sensor anomaly rules.

These tests verify that:
1. Raw events from wind_turbine.log (original format) can be parsed directly
   by TurbineTelemetry.model_validate() without any pre-processing.
2. All original field names are correctly normalised to canonical names.
3. Edge cases in field normalisation are handled correctly.
4. New anomaly rules for bearing/gearbox/gear temperatures fire correctly.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.schema import TurbineTelemetry, TurbineStatus, AlertSeverity
from services.processor.anomaly_detector import detect_anomalies


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures: original-format payloads from wind_turbine_sensorlog.py
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def original_format_payload():
    """
    A complete payload exactly as produced by wind_turbine_sensorlog.py.
    Uses the original PascalCase field names, original ID format, and
    original Timestamp format.
    """
    return {
        "Turbine_ID":       "Turbine_3",
        "Nacelle_Position": 182.45,
        "Wind_direction":   271.3,
        "Ambient_Air_temp": 12.5,
        "Bearing_Temp":     42.1,
        "BladePitchAngle":  15.0,
        "GearBoxSumpTemp":  67.3,
        "Generator_Speed":  52.4,
        "Hub_Speed":        3,
        "Power":            876.5,
        "Wind_Speed":       9.2,
        "GearTemp":         187.6,
        "GeneratorTemp":    78.3,
        "Timestamp":        "2024-06-15 14:32:01",
    }


@pytest.fixture
def original_format_event(original_format_payload):
    return TurbineTelemetry.model_validate(original_format_payload)


@pytest.fixture
def high_bearing_temp_payload(original_format_payload):
    return {**original_format_payload, "Bearing_Temp": 63.5}


@pytest.fixture
def critical_bearing_temp_payload(original_format_payload):
    return {**original_format_payload, "Bearing_Temp": 72.0}


@pytest.fixture
def high_gearbox_sump_payload(original_format_payload):
    return {**original_format_payload, "GearBoxSumpTemp": 98.0}


@pytest.fixture
def critical_gearbox_sump_payload(original_format_payload):
    return {**original_format_payload, "GearBoxSumpTemp": 122.0}


@pytest.fixture
def high_gear_temp_payload(original_format_payload):
    return {**original_format_payload, "GearTemp": 260.0}


@pytest.fixture
def critical_gear_temp_payload(original_format_payload):
    return {**original_format_payload, "GearTemp": 325.0}


# ─────────────────────────────────────────────────────────────────────────────
# Turbine ID normalisation tests
# ─────────────────────────────────────────────────────────────────────────────

class TestTurbineIdNormalisation:
    def test_turbine_id_numeric_prefix_stripped(self, original_format_event):
        """'Turbine_3' → 'turbine-03'"""
        assert original_format_event.turbine_id == "turbine-03"

    def test_turbine_id_12_normalises(self):
        payload = {
            "Turbine_ID": "Turbine_12",
            "Wind_Speed": 9.0, "Generator_Speed": 50.0, "Power": 800.0,
            "GeneratorTemp": 60.0, "BladePitchAngle": 10.0,
            "Timestamp": "2024-01-01 00:00:00",
        }
        event = TurbineTelemetry.model_validate(payload)
        assert event.turbine_id == "turbine-12"

    def test_turbine_id_single_digit_zero_padded(self):
        payload = {
            "Turbine_ID": "Turbine_1",
            "Wind_Speed": 9.0, "Generator_Speed": 50.0, "Power": 800.0,
            "GeneratorTemp": 60.0, "BladePitchAngle": 10.0,
            "Timestamp": "2024-01-01 00:00:00",
        }
        event = TurbineTelemetry.model_validate(payload)
        assert event.turbine_id == "turbine-01"

    def test_canonical_id_passthrough(self, valid_telemetry_payload):
        """Canonical format 'turbine-01' passes through unchanged."""
        event = TurbineTelemetry.model_validate(valid_telemetry_payload)
        assert event.turbine_id == "turbine-01"


# ─────────────────────────────────────────────────────────────────────────────
# Field name normalisation tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFieldNormalisation:
    def test_wind_speed_mapped(self, original_format_event):
        """Wind_Speed → wind_speed_ms"""
        assert original_format_event.wind_speed_ms == pytest.approx(9.2)

    def test_generator_speed_scaled_to_rotor_rpm(self, original_format_event):
        """Generator_Speed 52.4 → rotor_rpm 5.24 (divide by 10 gearbox ratio)"""
        assert original_format_event.rotor_rpm == pytest.approx(5.24)

    def test_power_mapped(self, original_format_event):
        """Power → power_output_kw"""
        assert original_format_event.power_output_kw == pytest.approx(876.5)

    def test_generator_temp_mapped(self, original_format_event):
        """GeneratorTemp → temperature_c"""
        assert original_format_event.temperature_c == pytest.approx(78.3)

    def test_timestamp_parsed(self, original_format_event):
        """Timestamp '2024-06-15 14:32:01' → recorded_at datetime"""
        assert isinstance(original_format_event.recorded_at, datetime)
        assert original_format_event.recorded_at.year == 2024
        assert original_format_event.recorded_at.month == 6
        assert original_format_event.recorded_at.day == 15


class TestExtendedFieldNormalisation:
    def test_nacelle_position_mapped(self, original_format_event):
        """Nacelle_Position → nacelle_position_deg"""
        assert original_format_event.nacelle_position_deg == pytest.approx(182.45)

    def test_wind_direction_mapped(self, original_format_event):
        """Wind_direction → wind_direction_deg"""
        assert original_format_event.wind_direction_deg == pytest.approx(271.3)

    def test_ambient_temp_mapped(self, original_format_event):
        """Ambient_Air_temp → ambient_temp_c"""
        assert original_format_event.ambient_temp_c == pytest.approx(12.5)

    def test_bearing_temp_mapped(self, original_format_event):
        """Bearing_Temp → bearing_temp_c"""
        assert original_format_event.bearing_temp_c == pytest.approx(42.1)

    def test_gearbox_sump_temp_mapped(self, original_format_event):
        """GearBoxSumpTemp → gearbox_sump_temp_c"""
        assert original_format_event.gearbox_sump_temp_c == pytest.approx(67.3)

    def test_gear_temp_mapped(self, original_format_event):
        """GearTemp → gear_temp_c"""
        assert original_format_event.gear_temp_c == pytest.approx(187.6)

    def test_hub_speed_mapped(self, original_format_event):
        """Hub_Speed → hub_speed"""
        assert original_format_event.hub_speed == pytest.approx(3.0)

    def test_extended_fields_absent_are_none(self):
        """Events without extended fields (e.g. new-style events) get None, not error."""
        from tests.conftest import *
        payload = {
            "turbine_id": "turbine-01",
            "recorded_at": "2024-01-01T00:00:00Z",
            "wind_speed_ms": 9.0, "rotor_rpm": 8.0, "power_output_kw": 900.0,
            "temperature_c": 55.0, "vibration_mms": 1.5, "blade_pitch_deg": 12.0,
        }
        event = TurbineTelemetry.model_validate(payload)
        assert event.nacelle_position_deg is None
        assert event.bearing_temp_c is None
        assert event.gearbox_sump_temp_c is None


class TestBladePitchClamping:
    def test_pitch_above_90_clamped(self):
        """BladePitchAngle=95 → blade_pitch_deg=90 (clamped, was 0-210 in original)"""
        payload = {
            "Turbine_ID": "Turbine_1",
            "Wind_Speed": 9.0, "Generator_Speed": 50.0, "Power": 800.0,
            "GeneratorTemp": 60.0, "BladePitchAngle": 95.0,
            "Timestamp": "2024-01-01 00:00:00",
        }
        event = TurbineTelemetry.model_validate(payload)
        assert 0.0 <= event.blade_pitch_deg <= 90.0

    def test_pitch_within_range_unchanged(self):
        """BladePitchAngle=25 stays 25."""
        payload = {
            "Turbine_ID": "Turbine_1",
            "Wind_Speed": 9.0, "Generator_Speed": 50.0, "Power": 800.0,
            "GeneratorTemp": 60.0, "BladePitchAngle": 25.0,
            "Timestamp": "2024-01-01 00:00:00",
        }
        event = TurbineTelemetry.model_validate(payload)
        assert event.blade_pitch_deg == pytest.approx(25.0)

    def test_pitch_zero_accepted(self):
        payload = {
            "Turbine_ID": "Turbine_1",
            "Wind_Speed": 9.0, "Generator_Speed": 50.0, "Power": 800.0,
            "GeneratorTemp": 60.0, "BladePitchAngle": 0.0,
            "Timestamp": "2024-01-01 00:00:00",
        }
        event = TurbineTelemetry.model_validate(payload)
        assert event.blade_pitch_deg == pytest.approx(0.0)


class TestVibrationDefault:
    def test_missing_vibration_defaults_to_zero(self, original_format_payload):
        """Original data had no Vibration field — should default to 0.0, not fail."""
        assert "vibration_mms" not in original_format_payload
        assert "Vibration" not in original_format_payload
        event = TurbineTelemetry.model_validate(original_format_payload)
        assert event.vibration_mms == pytest.approx(0.0)


# ─────────────────────────────────────────────────────────────────────────────
# New anomaly rules: extended sensor fields
# ─────────────────────────────────────────────────────────────────────────────

class TestBearingTempRules:
    def test_bearing_warning_fires(self, high_bearing_temp_payload):
        event = TurbineTelemetry.model_validate(high_bearing_temp_payload)
        alerts = detect_anomalies(event)
        rule_names = {a.rule_name for a in alerts}
        assert "bearing_temp_warning" in rule_names

    def test_bearing_critical_fires(self, critical_bearing_temp_payload):
        event = TurbineTelemetry.model_validate(critical_bearing_temp_payload)
        alerts = detect_anomalies(event)
        rule_names = {a.rule_name for a in alerts}
        assert "bearing_temp_warning" in rule_names
        assert "bearing_temp_critical" in rule_names

    def test_bearing_critical_severity(self, critical_bearing_temp_payload):
        event = TurbineTelemetry.model_validate(critical_bearing_temp_payload)
        alerts = detect_anomalies(event)
        crit = [a for a in alerts if a.rule_name == "bearing_temp_critical"]
        assert crit[0].severity == AlertSeverity.CRITICAL

    def test_normal_bearing_no_alert(self, original_format_event):
        """bearing_temp_c=42.1 is within normal range — no bearing alert."""
        alerts = detect_anomalies(original_format_event)
        bearing_alerts = [a for a in alerts if "bearing" in a.rule_name]
        assert len(bearing_alerts) == 0

    def test_none_bearing_no_alert(self, valid_telemetry_payload):
        """Events without bearing_temp_c should not trigger bearing rules."""
        event = TurbineTelemetry.model_validate(valid_telemetry_payload)
        assert event.bearing_temp_c is None
        alerts = detect_anomalies(event)
        bearing_alerts = [a for a in alerts if "bearing" in a.rule_name]
        assert len(bearing_alerts) == 0


class TestGearboxSumpTempRules:
    def test_gearbox_warning_fires(self, high_gearbox_sump_payload):
        event = TurbineTelemetry.model_validate(high_gearbox_sump_payload)
        alerts = detect_anomalies(event)
        rule_names = {a.rule_name for a in alerts}
        assert "gearbox_sump_temp_warning" in rule_names

    def test_gearbox_critical_fires(self, critical_gearbox_sump_payload):
        event = TurbineTelemetry.model_validate(critical_gearbox_sump_payload)
        alerts = detect_anomalies(event)
        rule_names = {a.rule_name for a in alerts}
        assert "gearbox_sump_temp_warning" in rule_names
        assert "gearbox_sump_temp_critical" in rule_names

    def test_normal_gearbox_sump_no_alert(self, original_format_event):
        """gearbox_sump_temp_c=67.3 — well within normal range."""
        alerts = detect_anomalies(original_format_event)
        gearbox_alerts = [a for a in alerts if "gearbox" in a.rule_name]
        assert len(gearbox_alerts) == 0

    def test_none_gearbox_no_alert(self, valid_telemetry_payload):
        event = TurbineTelemetry.model_validate(valid_telemetry_payload)
        assert event.gearbox_sump_temp_c is None
        alerts = detect_anomalies(event)
        gearbox_alerts = [a for a in alerts if "gearbox" in a.rule_name]
        assert len(gearbox_alerts) == 0


class TestGearTempRules:
    def test_gear_temp_warning_fires(self, high_gear_temp_payload):
        event = TurbineTelemetry.model_validate(high_gear_temp_payload)
        alerts = detect_anomalies(event)
        rule_names = {a.rule_name for a in alerts}
        assert "gear_temp_warning" in rule_names

    def test_gear_temp_critical_fires(self, critical_gear_temp_payload):
        event = TurbineTelemetry.model_validate(critical_gear_temp_payload)
        alerts = detect_anomalies(event)
        rule_names = {a.rule_name for a in alerts}
        assert "gear_temp_warning" in rule_names
        assert "gear_temp_critical" in rule_names

    def test_normal_gear_temp_no_alert(self, original_format_event):
        """gear_temp_c=187.6 is within normal operating range."""
        alerts = detect_anomalies(original_format_event)
        gear_alerts = [a for a in alerts if "gear_temp" in a.rule_name]
        assert len(gear_alerts) == 0

    def test_none_gear_temp_no_alert(self, valid_telemetry_payload):
        event = TurbineTelemetry.model_validate(valid_telemetry_payload)
        assert event.gear_temp_c is None
        alerts = detect_anomalies(event)
        gear_alerts = [a for a in alerts if "gear_temp" in a.rule_name]
        assert len(gear_alerts) == 0


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end: original log line → full pipeline validation
# ─────────────────────────────────────────────────────────────────────────────

class TestEndToEndLegacyPipeline:
    def test_original_payload_validates_completely(self, original_format_payload):
        """A line from wind_turbine.log validates without errors."""
        event = TurbineTelemetry.model_validate(original_format_payload)
        assert event is not None

    def test_partition_key_uses_normalised_id(self, original_format_event):
        """Partition key must be the normalised canonical ID."""
        assert original_format_event.partition_key() == "turbine-03"

    def test_event_serialises_to_json(self, original_format_event):
        """Validated event can be serialised for Kafka publish."""
        json_str = original_format_event.model_dump_json()
        assert "turbine-03" in json_str
        assert "nacelle_position_deg" in json_str

    def test_json_roundtrip_preserves_extended_fields(self, original_format_event):
        """JSON roundtrip (Kafka → consumer) preserves all extended fields."""
        restored = TurbineTelemetry.model_validate_json(
            original_format_event.model_dump_json()
        )
        assert restored.nacelle_position_deg == pytest.approx(
            original_format_event.nacelle_position_deg
        )
        assert restored.bearing_temp_c == pytest.approx(
            original_format_event.bearing_temp_c
        )
        assert restored.gear_temp_c == pytest.approx(
            original_format_event.gear_temp_c
        )

    def test_twelve_turbine_ids_all_normalise(self):
        """All 12 original turbine IDs normalise correctly."""
        for i in range(1, 13):
            payload = {
                "Turbine_ID": f"Turbine_{i}",
                "Wind_Speed": 9.0, "Generator_Speed": 50.0, "Power": 800.0,
                "GeneratorTemp": 60.0, "BladePitchAngle": 10.0,
                "Timestamp": "2024-01-01 00:00:00",
            }
            event = TurbineTelemetry.model_validate(payload)
            assert event.turbine_id == f"turbine-{i:02d}", (
                f"Turbine_{i} should become turbine-{i:02d}, got {event.turbine_id}"
            )

    def test_mixed_critical_fault_original_format(self, original_format_payload):
        """
        Inject critical values in original format and verify alerts fire.
        This is the realistic scenario: bad readings coming in from the log file.
        """
        bad_payload = {
            **original_format_payload,
            "Bearing_Temp": 73.0,       # → bearing_temp_critical
            "GearBoxSumpTemp": 125.0,   # → gearbox_sump_temp_critical
            "GearTemp": 330.0,          # → gear_temp_critical
        }
        event = TurbineTelemetry.model_validate(bad_payload)
        alerts = detect_anomalies(event)
        rule_names = {a.rule_name for a in alerts}

        assert "bearing_temp_critical" in rule_names
        assert "gearbox_sump_temp_critical" in rule_names
        assert "gear_temp_critical" in rule_names

        # All alerts reference the correct turbine
        for alert in alerts:
            assert alert.turbine_id == "turbine-03"
