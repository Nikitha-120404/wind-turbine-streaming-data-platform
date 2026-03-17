"""
shared/schema.py

Canonical Pydantic schemas for wind turbine telemetry and alerts.
All services import from here to guarantee consistency across the platform.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class TurbineStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"
    FAULT = "fault"


class AlertSeverity(str, Enum):
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class TurbineTelemetry(BaseModel):
    """
    Raw telemetry event published to windturbine-raw topic.
    Represents a single sensor reading from one turbine at one point in time.

    Field coverage:
    - Core fields (wind, power, RPM, temp, vibration, pitch) used by anomaly detection
    - Extended sensor fields from original wind_turbine_sensorlog.py:
        nacelle_position_deg, wind_direction_deg, ambient_temp_c,
        bearing_temp_c, gearbox_sump_temp_c, gear_temp_c, hub_speed

    Alias support:
    - Accepts original PascalCase/mixed field names from legacy log files via
      model_validate() with aliases. This means wind_turbine.log events flow
      in without a conversion layer.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    turbine_id: str = Field(..., min_length=1, max_length=64)
    recorded_at: datetime

    # ── Core operational fields ───────────────────────────────────────────────
    wind_speed_ms: float = Field(..., ge=0.0, le=100.0, description="Wind speed in m/s")
    rotor_rpm: float = Field(..., ge=0.0, le=30.0, description="Rotor revolutions per minute")
    power_output_kw: float = Field(..., ge=0.0, le=5000.0, description="Power output in kilowatts")
    temperature_c: float = Field(..., ge=-40.0, le=150.0, description="Nacelle/generator temperature °C")
    vibration_mms: float = Field(..., ge=0.0, le=50.0, description="Vibration in mm/s RMS")
    blade_pitch_deg: float = Field(..., ge=0.0, le=90.0, description="Blade pitch angle degrees")
    status: TurbineStatus = TurbineStatus.ONLINE

    # ── Extended sensor fields (from original wind_turbine_sensorlog.py) ─────
    # These were present in your original generate_data() but absent from the
    # initial schema. They are now first-class fields stored in TimescaleDB.
    nacelle_position_deg: Optional[float] = Field(
        None, ge=0.0, le=360.0,
        description="Nacelle yaw position in degrees (0–360). "
                    "Original field: Nacelle_Position",
    )
    wind_direction_deg: Optional[float] = Field(
        None, ge=0.0, le=360.0,
        description="Wind direction in degrees (0–360). "
                    "Original field: Wind_direction",
    )
    ambient_temp_c: Optional[float] = Field(
        None, ge=-50.0, le=60.0,
        description="Ambient air temperature °C. "
                    "Original field: Ambient_Air_temp",
    )
    bearing_temp_c: Optional[float] = Field(
        None, ge=0.0, le=150.0,
        description="Main bearing temperature °C. "
                    "Original field: Bearing_Temp",
    )
    gearbox_sump_temp_c: Optional[float] = Field(
        None, ge=0.0, le=200.0,
        description="Gearbox sump oil temperature °C. "
                    "Original field: GearBoxSumpTemp",
    )
    gear_temp_c: Optional[float] = Field(
        None, ge=0.0, le=400.0,
        description="Gearbox gear temperature °C. "
                    "Original field: GearTemp",
    )
    hub_speed: Optional[float] = Field(
        None, ge=0.0, le=50.0,
        description="Hub rotation speed (arbitrary units). "
                    "Original field: Hub_Speed",
    )

    producer_ts: Optional[datetime] = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when event entered the pipeline",
    )

    @model_validator(mode="before")
    @classmethod
    def normalise_legacy_fields(cls, data: dict) -> dict:
        """
        Accept original PascalCase/mixed field names from wind_turbine_sensorlog.py
        and normalise them to canonical snake_case names before validation.

        This validator is the bridge between the original scripts and the new
        platform — it means a raw line from wind_turbine.log can be fed directly
        into TurbineTelemetry.model_validate() without a separate translation layer.

        Original → Canonical mapping:
          Turbine_ID         → turbine_id        (also strips "Turbine_" prefix)
          Wind_Speed         → wind_speed_ms
          Generator_Speed    → rotor_rpm         (generator RPM ≈ rotor RPM at scale)
          Power              → power_output_kw
          GeneratorTemp      → temperature_c
          BladePitchAngle    → blade_pitch_deg   (clamped 0–90, original was 0–210)
          Timestamp          → recorded_at
          Nacelle_Position   → nacelle_position_deg
          Wind_direction     → wind_direction_deg
          Ambient_Air_temp   → ambient_temp_c
          Bearing_Temp       → bearing_temp_c
          GearBoxSumpTemp    → gearbox_sump_temp_c
          GearTemp           → gear_temp_c
          Hub_Speed          → hub_speed
        """
        if not isinstance(data, dict):
            return data

        d = dict(data)

        # turbine_id: "Turbine_ID" key, value like "Turbine_3" → "turbine-03"
        if "turbine_id" not in d and "Turbine_ID" in d:
            raw_id = str(d.pop("Turbine_ID"))
            # Normalise "Turbine_3" → "turbine-03"
            numeric = raw_id.replace("Turbine_", "").replace("turbine-", "").strip()
            try:
                d["turbine_id"] = f"turbine-{int(numeric):02d}"
            except ValueError:
                d["turbine_id"] = raw_id.lower().replace("_", "-")

        # recorded_at: accept "Timestamp" key with strptime format
        if "recorded_at" not in d and "Timestamp" in d:
            ts_raw = d.pop("Timestamp")
            try:
                # Original format: '2024-01-15 14:32:01'
                from datetime import datetime as _dt
                d["recorded_at"] = _dt.strptime(str(ts_raw), "%Y-%m-%d %H:%M:%S").isoformat()
            except ValueError:
                d["recorded_at"] = ts_raw  # fall through to field_validator

        # wind_speed_ms
        if "wind_speed_ms" not in d and "Wind_Speed" in d:
            d["wind_speed_ms"] = d.pop("Wind_Speed")

        # rotor_rpm: Generator_Speed in original is generator shaft RPM.
        # Scale factor from generator (1500 RPM range) to rotor (0–20 RPM):
        # original range 40–60 RPM maps to ~4–6 rotor RPM after /10 gearbox ratio.
        if "rotor_rpm" not in d and "Generator_Speed" in d:
            gen_rpm = float(d.pop("Generator_Speed"))
            d["rotor_rpm"] = round(gen_rpm / 10.0, 2)  # approximate gearbox ratio

        # power_output_kw
        if "power_output_kw" not in d and "Power" in d:
            d["power_output_kw"] = d.pop("Power")

        # temperature_c: GeneratorTemp is the closest to nacelle temperature
        if "temperature_c" not in d and "GeneratorTemp" in d:
            d["temperature_c"] = d.pop("GeneratorTemp")

        # blade_pitch_deg: original range was 0–210 (includes feather + reverse).
        # Clamp to 0–90 — the physically meaningful range for operational control.
        if "blade_pitch_deg" not in d and "BladePitchAngle" in d:
            raw_pitch = float(d.pop("BladePitchAngle"))
            d["blade_pitch_deg"] = min(90.0, max(0.0, raw_pitch % 90.0 if raw_pitch > 90 else raw_pitch))

        # vibration_mms: original script did not include vibration.
        # Default to 0.0 so the field passes validation.
        if "vibration_mms" not in d:
            d["vibration_mms"] = 0.0

        # Extended sensor fields (direct renames, same units)
        _renames = {
            "Nacelle_Position": "nacelle_position_deg",
            "Wind_direction":   "wind_direction_deg",
            "Ambient_Air_temp": "ambient_temp_c",
            "Bearing_Temp":     "bearing_temp_c",
            "GearBoxSumpTemp":  "gearbox_sump_temp_c",
            "GearTemp":         "gear_temp_c",
            "Hub_Speed":        "hub_speed",
        }
        for orig_key, canon_key in _renames.items():
            if canon_key not in d and orig_key in d:
                d[canon_key] = d.pop(orig_key)

        # Downtime coercion: physics says stopped in good wind → FAULT status
        wind = float(d.get("wind_speed_ms", 0))
        rpm = float(d.get("rotor_rpm", 0))
        power = float(d.get("power_output_kw", 0))
        status = d.get("status", TurbineStatus.ONLINE)
        status_str = status.value if isinstance(status, TurbineStatus) else str(status)
        if (
            wind > 3.0
            and rpm < 0.1
            and power < 1.0
            and status_str == TurbineStatus.ONLINE.value
        ):
            d["status"] = TurbineStatus.FAULT.value

        return d

    @field_validator("recorded_at", mode="before")
    @classmethod
    def parse_recorded_at(cls, v):
        if isinstance(v, str):
            # Try ISO format first, then original strptime format
            try:
                return datetime.fromisoformat(v)
            except ValueError:
                return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        return v

    @field_validator("turbine_id")
    @classmethod
    def turbine_id_format(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("turbine_id cannot be blank")
        return v

    def partition_key(self) -> str:
        """Key used for Kafka partitioning — routes all events from one turbine to one partition."""
        return self.turbine_id

    model_config = {"populate_by_name": True}


class TurbineAlert(BaseModel):
    """
    Alert event published to windturbine-alerts topic and stored in turbine_alerts table.
    """

    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    turbine_id: str
    recorded_at: datetime
    severity: AlertSeverity
    rule_name: str = Field(..., max_length=128)
    message: str = Field(..., max_length=512)
    metric_name: str
    metric_value: float
    threshold: float
    source_event_id: str
    alert_ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DLQEvent(BaseModel):
    """
    Dead-letter queue envelope. Wraps the raw bytes that failed validation or processing.
    """

    dlq_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    original_topic: str
    original_partition: Optional[int] = None
    original_offset: Optional[int] = None
    raw_payload: str  # JSON string or hex if binary
    error_type: str
    error_message: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = 0
