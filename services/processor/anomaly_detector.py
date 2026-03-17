"""
services/processor/anomaly_detector.py

Rules-based anomaly detection for wind turbine telemetry.

Design philosophy:
- Rules are explicit, auditable, and easily tunable via config.
- Each rule returns zero or more alerts — a single event can trigger multiple rules.
- Rules include rationale comments so engineers understand why thresholds exist.
- No ML black boxes — operators must be able to explain every alert.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List

from shared.metrics import ANOMALY_ALERTS_TOTAL
from shared.schema import AlertSeverity, TurbineAlert, TurbineTelemetry

logger = logging.getLogger(__name__)


@dataclass
class AnomalyRule:
    name: str
    severity: AlertSeverity
    metric_name: str
    threshold: float
    message_template: str
    check: Callable[[TurbineTelemetry], bool]


# ─────────────────────────────────────────────────────────────────────────────
# Rule definitions
# Thresholds derived from IEC 61400-1 and typical SCADA alarm setpoints.
# ─────────────────────────────────────────────────────────────────────────────

RULES: List[AnomalyRule] = [
    # ── Temperature rules ─────────────────────────────────────────────────────
    AnomalyRule(
        name="nacelle_temp_warning",
        severity=AlertSeverity.WARNING,
        metric_name="temperature_c",
        threshold=85.0,
        message_template="Nacelle temperature {value:.1f}°C exceeds warning threshold {threshold}°C",
        # 85°C is the typical alarm setpoint; sustained operation risks insulation degradation
        check=lambda e: e.temperature_c > 85.0,
    ),
    AnomalyRule(
        name="nacelle_temp_critical",
        severity=AlertSeverity.CRITICAL,
        metric_name="temperature_c",
        threshold=95.0,
        message_template="CRITICAL: Nacelle temperature {value:.1f}°C — emergency shutdown risk",
        # Above 95°C, most turbine controllers initiate emergency stop
        check=lambda e: e.temperature_c > 95.0,
    ),
    # ── Vibration rules ───────────────────────────────────────────────────────
    AnomalyRule(
        name="vibration_warning",
        severity=AlertSeverity.WARNING,
        metric_name="vibration_mms",
        threshold=6.0,
        message_template="Vibration {value:.2f} mm/s RMS exceeds warning threshold {threshold} mm/s",
        # ISO 10816-21 class IIA boundary for wind turbines; indicates bearing wear or imbalance
        check=lambda e: e.vibration_mms > 6.0,
    ),
    AnomalyRule(
        name="vibration_critical",
        severity=AlertSeverity.CRITICAL,
        metric_name="vibration_mms",
        threshold=9.0,
        message_template="CRITICAL: Vibration {value:.2f} mm/s — imminent mechanical failure risk",
        # Above 9.0 mm/s, structural damage to drivetrain is likely; turbine should stop
        check=lambda e: e.vibration_mms > 9.0,
    ),
    # ── Power output rules ────────────────────────────────────────────────────
    AnomalyRule(
        name="low_power_output",
        severity=AlertSeverity.WARNING,
        metric_name="power_output_kw",
        threshold=50.0,
        message_template=(
            "Low power {value:.0f} kW at wind speed {wind_speed:.1f} m/s "
            "— possible curtailment or underperformance"
        ),
        # Wind > 5 m/s should produce meaningful power; <50 kW suggests a problem
        check=lambda e: e.wind_speed_ms > 5.0 and e.power_output_kw < 50.0 and e.rotor_rpm > 1.0,
    ),
    AnomalyRule(
        name="power_dropout",
        severity=AlertSeverity.CRITICAL,
        metric_name="power_output_kw",
        threshold=5.0,
        message_template=(
            "CRITICAL: Power dropout — {value:.1f} kW at wind {wind_speed:.1f} m/s "
            "with RPM {rpm:.1f}"
        ),
        # Near-zero power in good wind with RPM present → likely grid disconnect or converter fault
        check=lambda e: (
            e.wind_speed_ms > 8.0
            and e.power_output_kw < 5.0
            and e.rotor_rpm > 2.0
        ),
    ),
    # ── Downtime detection ────────────────────────────────────────────────────
    AnomalyRule(
        name="unplanned_downtime",
        severity=AlertSeverity.CRITICAL,
        metric_name="rotor_rpm",
        threshold=0.1,
        message_template=(
            "CRITICAL: Unplanned downtime — RPM={value:.2f}, power={power:.0f} kW "
            "while wind={wind_speed:.1f} m/s"
        ),
        # Zero RPM and zero power in wind > 3 m/s = stopped turbine in operable conditions
        check=lambda e: (
            e.wind_speed_ms > 3.0
            and e.rotor_rpm < 0.1
            and e.power_output_kw < 1.0
        ),
    ),
    # ── Sensor anomaly rules ──────────────────────────────────────────────────
    AnomalyRule(
        name="blade_pitch_extreme",
        severity=AlertSeverity.WARNING,
        metric_name="blade_pitch_deg",
        threshold=88.0,
        message_template=(
            "Blade pitch {value:.1f}° at wind {wind_speed:.1f} m/s "
            "— feathered outside expected conditions"
        ),
        # Blades fully feathered (>88°) in moderate wind = emergency feather or control fault
        check=lambda e: e.blade_pitch_deg > 88.0 and e.wind_speed_ms < 20.0,
    ),
    AnomalyRule(
        name="rotor_overspeed",
        severity=AlertSeverity.CRITICAL,
        metric_name="rotor_rpm",
        threshold=18.0,
        message_template="CRITICAL: Rotor overspeed {value:.1f} RPM — protection system may activate",
        # Most 2 MW turbines trip at ~18-20 RPM; detection before trip allows soft response
        check=lambda e: e.rotor_rpm > 18.0,
    ),
    # ── Extended sensor rules (from original wind_turbine_sensorlog.py fields) ─
    # These rules use the bearing, gearbox, and gear temperature fields that were
    # present in the original generate_data() but had no detection logic.
    # Thresholds derived from original field ranges and industry maintenance standards.
    AnomalyRule(
        name="bearing_temp_warning",
        severity=AlertSeverity.WARNING,
        metric_name="bearing_temp_c",
        threshold=60.0,
        message_template=(
            "Bearing temperature {value:.1f}°C exceeds warning threshold {threshold}°C "
            "— check lubrication"
        ),
        # Original Bearing_Temp range: 10–70°C. Above 60°C indicates reduced lubrication
        # film thickness; sustained operation risks spalling damage.
        check=lambda e: e.bearing_temp_c is not None and e.bearing_temp_c > 60.0,
    ),
    AnomalyRule(
        name="bearing_temp_critical",
        severity=AlertSeverity.CRITICAL,
        metric_name="bearing_temp_c",
        threshold=70.0,
        message_template=(
            "CRITICAL: Bearing temperature {value:.1f}°C — imminent bearing failure risk"
        ),
        # Above 70°C (top of original range), bearing seizure risk is high.
        check=lambda e: e.bearing_temp_c is not None and e.bearing_temp_c > 70.0,
    ),
    AnomalyRule(
        name="gearbox_sump_temp_warning",
        severity=AlertSeverity.WARNING,
        metric_name="gearbox_sump_temp_c",
        threshold=95.0,
        message_template=(
            "Gearbox sump temperature {value:.1f}°C — oil viscosity may be compromised"
        ),
        # Original GearBoxSumpTemp range: 20–130°C. Above 95°C, ISO VG 320 gear oil
        # loses adequate viscosity; increases wear rate significantly.
        check=lambda e: e.gearbox_sump_temp_c is not None and e.gearbox_sump_temp_c > 95.0,
    ),
    AnomalyRule(
        name="gearbox_sump_temp_critical",
        severity=AlertSeverity.CRITICAL,
        metric_name="gearbox_sump_temp_c",
        threshold=120.0,
        message_template=(
            "CRITICAL: Gearbox sump {value:.1f}°C — oil breakdown risk, shutdown advised"
        ),
        # Above 120°C (near top of original range), oil oxidation is rapid.
        check=lambda e: e.gearbox_sump_temp_c is not None and e.gearbox_sump_temp_c > 120.0,
    ),
    AnomalyRule(
        name="gear_temp_warning",
        severity=AlertSeverity.WARNING,
        metric_name="gear_temp_c",
        threshold=250.0,
        message_template=(
            "Gear mesh temperature {value:.1f}°C — elevated friction or lubrication loss"
        ),
        # Original GearTemp range: 50–350°C. Above 250°C indicates inadequate
        # lubrication at the gear mesh interface.
        check=lambda e: e.gear_temp_c is not None and e.gear_temp_c > 250.0,
    ),
    AnomalyRule(
        name="gear_temp_critical",
        severity=AlertSeverity.CRITICAL,
        metric_name="gear_temp_c",
        threshold=320.0,
        message_template=(
            "CRITICAL: Gear temperature {value:.1f}°C — gear tooth damage risk"
        ),
        # Above 320°C, surface hardening of gear teeth begins to reverse (tempering).
        check=lambda e: e.gear_temp_c is not None and e.gear_temp_c > 320.0,
    ),
]


def _format_message(rule: AnomalyRule, event: TurbineTelemetry) -> str:
    """Fill in the message template with event values."""
    return rule.message_template.format(
        value=getattr(event, rule.metric_name, 0.0),
        threshold=rule.threshold,
        wind_speed=event.wind_speed_ms,
        rpm=event.rotor_rpm,
        power=event.power_output_kw,
    )


def detect_anomalies(event: TurbineTelemetry) -> List[TurbineAlert]:
    """
    Run all anomaly rules against a telemetry event.
    Returns a (possibly empty) list of TurbineAlert objects.
    """
    alerts = []
    for rule in RULES:
        try:
            if rule.check(event):
                alert = TurbineAlert(
                    turbine_id=event.turbine_id,
                    recorded_at=event.recorded_at,
                    severity=rule.severity,
                    rule_name=rule.name,
                    message=_format_message(rule, event),
                    metric_name=rule.metric_name,
                    metric_value=getattr(event, rule.metric_name, 0.0),
                    threshold=rule.threshold,
                    source_event_id=event.event_id,
                )
                alerts.append(alert)
                ANOMALY_ALERTS_TOTAL.labels(
                    turbine_id=event.turbine_id,
                    rule_name=rule.name,
                    severity=rule.severity.value,
                ).inc()
                logger.warning(
                    f"Anomaly detected: {rule.name}",
                    extra={
                        "turbine_id": event.turbine_id,
                        "rule": rule.name,
                        "severity": rule.severity.value,
                        "metric": rule.metric_name,
                        "value": getattr(event, rule.metric_name, 0.0),
                        "threshold": rule.threshold,
                    },
                )
        except Exception as exc:
            logger.error(f"Rule {rule.name} raised exception: {exc}", exc_info=True)

    return alerts
