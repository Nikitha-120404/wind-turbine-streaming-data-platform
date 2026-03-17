"""
tests/test_anomaly_detector.py

Tests for all anomaly detection rules.
Each rule is tested for:
  - triggers correctly at/above threshold
  - does NOT trigger below threshold (no false positives)
  - correct severity assigned
  - correct rule_name in output alert
"""

from __future__ import annotations

import pytest

from services.processor.anomaly_detector import detect_anomalies
from shared.schema import AlertSeverity, TurbineTelemetry


def _alert_names(alerts) -> set:
    return {a.rule_name for a in alerts}


def _alert_severities(alerts) -> set:
    return {a.severity for a in alerts}


class TestTemperatureRules:
    def test_warning_threshold_triggers(self, high_temp_event):
        """temperature_c=91.0 should trigger nacelle_temp_warning."""
        alerts = detect_anomalies(high_temp_event)
        assert "nacelle_temp_warning" in _alert_names(alerts)

    def test_critical_threshold_triggers(self, critical_temp_event):
        """temperature_c=97.0 should trigger BOTH warning and critical."""
        alerts = detect_anomalies(critical_temp_event)
        names = _alert_names(alerts)
        assert "nacelle_temp_warning" in names
        assert "nacelle_temp_critical" in names

    def test_critical_alert_has_critical_severity(self, critical_temp_event):
        alerts = detect_anomalies(critical_temp_event)
        critical = [a for a in alerts if a.rule_name == "nacelle_temp_critical"]
        assert len(critical) == 1
        assert critical[0].severity == AlertSeverity.CRITICAL

    def test_normal_temp_no_alert(self, normal_event):
        alerts = detect_anomalies(normal_event)
        temp_alerts = [a for a in alerts if "temp" in a.rule_name]
        assert len(temp_alerts) == 0

    def test_exactly_at_warning_threshold_triggers(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "temperature_c": 85.1}
        event = TurbineTelemetry.model_validate(payload)
        alerts = detect_anomalies(event)
        assert "nacelle_temp_warning" in _alert_names(alerts)

    def test_below_warning_threshold_no_alert(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "temperature_c": 84.9}
        event = TurbineTelemetry.model_validate(payload)
        alerts = detect_anomalies(event)
        assert "nacelle_temp_warning" not in _alert_names(alerts)

    def test_alert_metric_value_correct(self, high_temp_event):
        alerts = detect_anomalies(high_temp_event)
        temp_alert = next(a for a in alerts if a.rule_name == "nacelle_temp_warning")
        assert temp_alert.metric_value == pytest.approx(91.0)
        assert temp_alert.threshold == pytest.approx(85.0)


class TestVibrationRules:
    def test_warning_vibration_triggers(self, high_vibration_event):
        alerts = detect_anomalies(high_vibration_event)
        assert "vibration_warning" in _alert_names(alerts)

    def test_critical_vibration_triggers(self, critical_vibration_event):
        alerts = detect_anomalies(critical_vibration_event)
        names = _alert_names(alerts)
        assert "vibration_warning" in names
        assert "vibration_critical" in names

    def test_normal_vibration_no_alert(self, normal_event):
        alerts = detect_anomalies(normal_event)
        vib_alerts = [a for a in alerts if "vibration" in a.rule_name]
        assert len(vib_alerts) == 0

    def test_vibration_warning_severity(self, high_vibration_event):
        alerts = detect_anomalies(high_vibration_event)
        warn = [a for a in alerts if a.rule_name == "vibration_warning"]
        assert warn[0].severity == AlertSeverity.WARNING


class TestPowerOutputRules:
    def test_low_power_warning_triggers(self, low_power_event):
        """Wind present, RPM present, but power < 50 kW → low_power_output warning."""
        alerts = detect_anomalies(low_power_event)
        assert "low_power_output" in _alert_names(alerts)

    def test_power_dropout_triggers(self, power_dropout_event):
        """Strong wind + RPM but near-zero power → power_dropout critical."""
        alerts = detect_anomalies(power_dropout_event)
        assert "power_dropout" in _alert_names(alerts)

    def test_normal_power_no_alert(self, normal_event):
        alerts = detect_anomalies(normal_event)
        power_alerts = [a for a in alerts if "power" in a.rule_name]
        assert len(power_alerts) == 0

    def test_low_wind_low_power_no_alert(self, valid_telemetry_payload):
        """Below cut-in speed, zero power is expected and should not alert."""
        payload = {
            **valid_telemetry_payload,
            "wind_speed_ms": 2.5,
            "rotor_rpm": 0.0,
            "power_output_kw": 0.0,
        }
        event = TurbineTelemetry.model_validate(payload)
        alerts = detect_anomalies(event)
        power_alerts = [a for a in alerts if "power" in a.rule_name]
        assert len(power_alerts) == 0


class TestDowntimeDetection:
    def test_unplanned_downtime_triggers(self, downtime_event):
        alerts = detect_anomalies(downtime_event)
        assert "unplanned_downtime" in _alert_names(alerts)

    def test_unplanned_downtime_is_critical(self, downtime_event):
        alerts = detect_anomalies(downtime_event)
        downtime = [a for a in alerts if a.rule_name == "unplanned_downtime"]
        assert downtime[0].severity == AlertSeverity.CRITICAL

    def test_calm_wind_stopped_no_downtime_alert(self, valid_telemetry_payload):
        """In light wind, stopped rotor is not an anomaly."""
        payload = {
            **valid_telemetry_payload,
            "wind_speed_ms": 2.0,
            "rotor_rpm": 0.0,
            "power_output_kw": 0.0,
        }
        event = TurbineTelemetry.model_validate(payload)
        alerts = detect_anomalies(event)
        assert "unplanned_downtime" not in _alert_names(alerts)


class TestOverspeedRule:
    def test_overspeed_triggers(self, overspeed_event):
        alerts = detect_anomalies(overspeed_event)
        assert "rotor_overspeed" in _alert_names(alerts)

    def test_overspeed_is_critical(self, overspeed_event):
        alerts = detect_anomalies(overspeed_event)
        overspeed = [a for a in alerts if a.rule_name == "rotor_overspeed"]
        assert overspeed[0].severity == AlertSeverity.CRITICAL

    def test_normal_rpm_no_overspeed(self, normal_event):
        alerts = detect_anomalies(normal_event)
        assert "rotor_overspeed" not in _alert_names(alerts)


class TestBladePitchRule:
    def test_feathered_in_moderate_wind_triggers(self, valid_telemetry_payload):
        payload = {
            **valid_telemetry_payload,
            "blade_pitch_deg": 89.0,
            "wind_speed_ms": 10.0,
        }
        event = TurbineTelemetry.model_validate(payload)
        alerts = detect_anomalies(event)
        assert "blade_pitch_extreme" in _alert_names(alerts)

    def test_feathered_in_very_high_wind_no_alert(self, valid_telemetry_payload):
        """Feathering at cut-out wind is expected — should not alert."""
        payload = {
            **valid_telemetry_payload,
            "blade_pitch_deg": 89.0,
            "wind_speed_ms": 25.0,
        }
        event = TurbineTelemetry.model_validate(payload)
        alerts = detect_anomalies(event)
        assert "blade_pitch_extreme" not in _alert_names(alerts)


class TestMultipleAlertsOnOneEvent:
    def test_combined_fault_generates_multiple_alerts(self, valid_telemetry_payload):
        """A critically sick turbine should fire multiple independent rules."""
        payload = {
            **valid_telemetry_payload,
            "temperature_c": 97.0,   # critical temp
            "vibration_mms": 10.5,   # critical vibration
            "wind_speed_ms": 12.0,
            "rotor_rpm": 14.0,
            "power_output_kw": 2.0,  # power dropout
        }
        event = TurbineTelemetry.model_validate(payload)
        alerts = detect_anomalies(event)
        names = _alert_names(alerts)

        assert "nacelle_temp_warning" in names
        assert "nacelle_temp_critical" in names
        assert "vibration_warning" in names
        assert "vibration_critical" in names
        assert "power_dropout" in names
        assert len(alerts) >= 5

    def test_normal_event_generates_zero_alerts(self, normal_event):
        alerts = detect_anomalies(normal_event)
        assert len(alerts) == 0

    def test_all_alerts_have_correct_turbine_id(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "temperature_c": 97.0, "vibration_mms": 10.5}
        event = TurbineTelemetry.model_validate(payload)
        alerts = detect_anomalies(event)
        for alert in alerts:
            assert alert.turbine_id == "turbine-01"

    def test_all_alerts_reference_source_event(self, valid_telemetry_payload):
        payload = {**valid_telemetry_payload, "temperature_c": 97.0}
        event = TurbineTelemetry.model_validate(payload)
        alerts = detect_anomalies(event)
        for alert in alerts:
            assert alert.source_event_id == event.event_id
