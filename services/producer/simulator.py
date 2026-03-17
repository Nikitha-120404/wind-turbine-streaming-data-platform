"""
services/producer/simulator.py

Realistic wind turbine telemetry simulator.

This simulator merges two approaches:
1. Physics-based state machine for core operational fields (wind, RPM, power,
   temperature, vibration, pitch) — produces realistic correlations and anomalies.
2. Original sensor field set from wind_turbine_sensorlog.py — nacelle position,
   wind direction, ambient temperature, bearing temp, gearbox temperatures,
   and hub speed are now modelled with appropriate ranges and physics coupling
   where meaningful (e.g. bearing temp correlates with RPM, gearbox temp with load).

Original generate_data() contributions preserved here:
  - Turbine IDs: Turbine_1..12 → now turbine-01..12 (12 turbines by default)
  - Nacelle_Position (0–360°) → nacelle_position_deg, follows wind direction slowly
  - Wind_direction (0–360°)   → wind_direction_deg, random walk
  - Ambient_Air_temp (-30–45°C) → ambient_temp_c, diurnal cycle approximation
  - Bearing_Temp (10–70°C)    → bearing_temp_c, correlated with RPM load
  - GearBoxSumpTemp (20–130°C)→ gearbox_sump_temp_c, correlated with power output
  - Hub_Speed (1–5)           → hub_speed, correlated with rotor_rpm
  - GearTemp (50–350°C)       → gear_temp_c, correlated with load
  - GeneratorTemp (25–150°C)  → temperature_c (already existed, now wider range)
"""

from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from shared.schema import TurbineStatus, TurbineTelemetry


@dataclass
class TurbineState:
    """
    Mutable state for a single simulated turbine.

    Tracks both the physics-based core state and the extended sensor
    readings introduced in the original wind_turbine_sensorlog.py.
    """

    turbine_id: str

    # ── Core physics state ────────────────────────────────────────────────────
    wind_speed_ms: float = field(default_factory=lambda: random.uniform(4.0, 12.0))
    rotor_rpm: float = 0.0
    temperature_c: float = field(default_factory=lambda: random.uniform(25.0, 60.0))
    vibration_mms: float = field(default_factory=lambda: random.uniform(0.5, 2.0))
    blade_pitch_deg: float = 15.0
    status: TurbineStatus = TurbineStatus.ONLINE

    # ── Extended sensor state (from original wind_turbine_sensorlog.py) ───────
    # Wind direction: slow random walk (0–360°)
    wind_direction_deg: float = field(default_factory=lambda: random.uniform(0.0, 360.0))
    # Nacelle yaw: lags behind wind direction (turbine slowly tracks wind)
    nacelle_position_deg: float = field(default_factory=lambda: random.uniform(0.0, 360.0))
    # Ambient air temperature: diurnal-ish variation around a seasonal mean
    ambient_temp_c: float = field(default_factory=lambda: random.uniform(5.0, 25.0))
    # Bearing temperature: correlated with RPM load
    bearing_temp_c: float = field(default_factory=lambda: random.uniform(15.0, 35.0))
    # Gearbox sump temperature: correlated with power output
    gearbox_sump_temp_c: float = field(default_factory=lambda: random.uniform(30.0, 55.0))
    # Gear mesh temperature: higher than sump, spikes with load transients
    gear_temp_c: float = field(default_factory=lambda: random.uniform(60.0, 100.0))
    # Hub speed: integer-scale approximation of rotor speed (1–5 in original)
    hub_speed: float = 2.0

    # ── Anomaly injection state ───────────────────────────────────────────────
    _anomaly_mode: Optional[str] = field(default=None, repr=False)
    _anomaly_ticks_remaining: int = field(default=0, repr=False)

    def step(self, dt: float = 1.0) -> "TurbineState":
        """Advance simulation by dt seconds. Updates all sensor fields."""
        self._maybe_inject_anomaly()
        self._update_wind(dt)
        self._update_physics(dt)
        self._update_temperature(dt)
        self._update_extended_sensors(dt)
        return self

    # ── Core physics updaters ─────────────────────────────────────────────────

    def _update_wind(self, dt: float) -> None:
        """Wind speed follows a mean-reverting random walk."""
        if self._anomaly_mode == "downtime":
            self.wind_speed_ms = random.uniform(8.0, 15.0)
            return
        mean_wind = 9.0
        revert = 0.05 * (mean_wind - self.wind_speed_ms) * dt
        noise = random.gauss(0, 0.3) * math.sqrt(dt)
        self.wind_speed_ms = max(0.0, min(30.0, self.wind_speed_ms + revert + noise))

    def _update_physics(self, dt: float) -> None:
        """RPM and power follow wind speed via simplified power curve."""
        if self._anomaly_mode == "downtime":
            self.rotor_rpm = 0.0
            self.blade_pitch_deg = 87.0
            self.status = TurbineStatus.OFFLINE
            self.vibration_mms = random.uniform(0.0, 0.3)
            return

        self.status = TurbineStatus.ONLINE
        ws = self.wind_speed_ms
        if ws < 3.0:
            target_rpm = 0.0
            self.blade_pitch_deg = 87.0
        elif ws < 12.0:
            target_rpm = 2.0 + (ws - 3.0) * 1.3
            self.blade_pitch_deg = max(0.0, 30.0 - ws * 2.0)
        else:
            target_rpm = 15.5
            self.blade_pitch_deg = min(45.0, (ws - 12.0) * 2.5)

        self.rotor_rpm += 0.3 * (target_rpm - self.rotor_rpm) * dt
        self.rotor_rpm = max(0.0, min(20.0, self.rotor_rpm))

        if self._anomaly_mode == "high_vibration":
            self.vibration_mms = random.uniform(7.0, 11.0)
        else:
            base_vib = 0.3 + self.rotor_rpm * 0.12
            self.vibration_mms = max(0.0, base_vib + random.gauss(0, 0.2))

    def _update_temperature(self, dt: float) -> None:
        """Generator/nacelle temperature rises with load."""
        ambient = self.ambient_temp_c
        if self._anomaly_mode == "high_temp":
            target_temp = 130.0 + random.uniform(-5, 5)
        else:
            load_fraction = min(1.0, self.rotor_rpm / 15.5)
            # Generator temp range 25–150°C (from original GeneratorTemp field)
            target_temp = ambient + 15.0 + (125.0 - 15.0) * load_fraction * 0.6
        self.temperature_c += 0.1 * (target_temp - self.temperature_c) * dt
        self.temperature_c = max(-10.0, min(150.0, self.temperature_c))

    # ── Extended sensor updaters (from original wind_turbine_sensorlog.py) ────

    def _update_extended_sensors(self, dt: float) -> None:
        """
        Update all 7 extended sensor readings introduced in the original
        wind_turbine_sensorlog.py. Physics-coupled where meaningful.
        """
        load_fraction = min(1.0, self.rotor_rpm / 15.5)

        # Wind direction: slow Brownian walk (wind shifts gradually)
        wind_dir_noise = random.gauss(0, 1.0) * math.sqrt(dt)
        self.wind_direction_deg = (self.wind_direction_deg + wind_dir_noise) % 360.0

        # Nacelle position: yaw control tracks wind direction with lag
        yaw_error = (self.wind_direction_deg - self.nacelle_position_deg + 180) % 360 - 180
        yaw_rate = 0.1 * yaw_error * dt  # slow tracking
        self.nacelle_position_deg = (self.nacelle_position_deg + yaw_rate) % 360.0

        # Ambient temperature: slow variation (-30–45°C original range)
        ambient_noise = random.gauss(0, 0.05) * math.sqrt(dt)
        self.ambient_temp_c += ambient_noise
        self.ambient_temp_c = max(-30.0, min(45.0, self.ambient_temp_c))

        # Bearing temperature: rises with RPM load (10–70°C original range)
        bearing_target = 10.0 + 60.0 * load_fraction + random.gauss(0, 1)
        if self._anomaly_mode == "bearing_fault":
            bearing_target = 65.0 + random.uniform(0, 10)
        self.bearing_temp_c += 0.05 * (bearing_target - self.bearing_temp_c) * dt
        self.bearing_temp_c = max(10.0, min(120.0, self.bearing_temp_c))

        # Gearbox sump temperature: rises with power output (20–130°C original)
        sump_target = 20.0 + 110.0 * load_fraction + random.gauss(0, 2)
        self.gearbox_sump_temp_c += 0.03 * (sump_target - self.gearbox_sump_temp_c) * dt
        self.gearbox_sump_temp_c = max(20.0, min(130.0, self.gearbox_sump_temp_c))

        # Gear temperature: hotter than sump, faster transients (50–350°C original)
        gear_target = self.gearbox_sump_temp_c * 2.2 + random.gauss(0, 5)
        self.gear_temp_c += 0.08 * (gear_target - self.gear_temp_c) * dt
        self.gear_temp_c = max(50.0, min(350.0, self.gear_temp_c))

        # Hub speed: integer-scale proxy for rotor RPM (1–5 in original)
        # Map rotor_rpm 0–20 → hub_speed 0–5
        self.hub_speed = round(max(0.0, min(5.0, self.rotor_rpm / 4.0)), 1)

    def _maybe_inject_anomaly(self) -> None:
        """Randomly inject anomalies at low probability each tick."""
        if self._anomaly_ticks_remaining > 0:
            self._anomaly_ticks_remaining -= 1
            if self._anomaly_ticks_remaining == 0:
                self._anomaly_mode = None
            return

        if random.random() < 0.02:
            self._anomaly_mode = random.choice(
                ["high_temp", "high_vibration", "downtime", "bearing_fault"]
            )
            self._anomaly_ticks_remaining = random.randint(5, 20)

    @property
    def power_output_kw(self) -> float:
        """Compute power from RPM using simplified Cp curve."""
        if self.status == TurbineStatus.OFFLINE or self.rotor_rpm < 1.0:
            return 0.0
        fraction = (self.rotor_rpm / 15.5) ** 2.8
        base = 2000.0 * fraction
        if self._anomaly_mode == "low_power":
            return max(0.0, base * 0.08)
        return max(0.0, min(2000.0, base + random.gauss(0, 10)))


class TurbineFleet:
    """
    Manages a collection of turbine simulators.

    Default fleet size is 12 turbines (matching the original wind_turbine_sensorlog.py
    which simulated Turbine_1 through Turbine_12). IDs are normalised to
    turbine-01..12 format for consistency with the Kafka partitioning strategy.
    """

    def __init__(self, turbine_ids: list[str]):
        self._turbines = {tid: TurbineState(turbine_id=tid) for tid in turbine_ids}

    def emit_all(self) -> list[TurbineTelemetry]:
        """
        Step all turbines and return one TurbineTelemetry event per turbine.

        All fields from the original generate_data() are now included:
          wind_speed_ms, rotor_rpm, power_output_kw, temperature_c,
          vibration_mms, blade_pitch_deg (core) plus nacelle_position_deg,
          wind_direction_deg, ambient_temp_c, bearing_temp_c,
          gearbox_sump_temp_c, gear_temp_c, hub_speed (extended).
        """
        events = []
        now = datetime.now(timezone.utc)
        for state in self._turbines.values():
            state.step(dt=1.0)
            events.append(
                TurbineTelemetry(
                    turbine_id=state.turbine_id,
                    recorded_at=now,
                    # Core operational fields
                    wind_speed_ms=round(state.wind_speed_ms, 2),
                    rotor_rpm=round(state.rotor_rpm, 2),
                    power_output_kw=round(state.power_output_kw, 1),
                    temperature_c=round(state.temperature_c, 1),
                    vibration_mms=round(state.vibration_mms, 3),
                    blade_pitch_deg=round(state.blade_pitch_deg, 1),
                    status=state.status,
                    # Extended sensor fields (from original wind_turbine_sensorlog.py)
                    nacelle_position_deg=round(state.nacelle_position_deg, 2),
                    wind_direction_deg=round(state.wind_direction_deg, 2),
                    ambient_temp_c=round(state.ambient_temp_c, 2),
                    bearing_temp_c=round(state.bearing_temp_c, 2),
                    gearbox_sump_temp_c=round(state.gearbox_sump_temp_c, 2),
                    gear_temp_c=round(state.gear_temp_c, 1),
                    hub_speed=state.hub_speed,
                )
            )
        return events
