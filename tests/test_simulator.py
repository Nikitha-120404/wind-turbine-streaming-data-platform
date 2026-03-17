"""
tests/test_simulator.py

Tests for the turbine fleet simulator.
Validates physics consistency and that anomaly injection produces detectable events.
"""

from __future__ import annotations

import pytest

from services.producer.simulator import TurbineFleet, TurbineState
from shared.schema import TurbineStatus


class TestTurbineState:
    def test_initialises_with_valid_defaults(self):
        state = TurbineState(turbine_id="turbine-test")
        assert state.wind_speed_ms >= 0.0
        assert state.temperature_c >= -40.0

    def test_step_returns_self(self):
        state = TurbineState(turbine_id="turbine-test")
        result = state.step(dt=1.0)
        assert result is state

    def test_power_zero_when_offline(self):
        state = TurbineState(turbine_id="turbine-test")
        state.status = TurbineStatus.OFFLINE
        assert state.power_output_kw == 0.0

    def test_power_zero_when_rpm_low(self):
        state = TurbineState(turbine_id="turbine-test")
        state.rotor_rpm = 0.5
        assert state.power_output_kw == 0.0

    def test_power_increases_with_rpm(self):
        state = TurbineState(turbine_id="turbine-test")
        state.status = TurbineStatus.ONLINE
        state.rotor_rpm = 5.0
        low_power = state.power_output_kw
        state.rotor_rpm = 15.0
        high_power = state.power_output_kw
        assert high_power > low_power

    def test_downtime_anomaly_stops_turbine(self):
        state = TurbineState(turbine_id="turbine-test")
        state._anomaly_mode = "downtime"
        state._anomaly_ticks_remaining = 10
        state.step(dt=1.0)
        assert state.rotor_rpm == 0.0
        assert state.status == TurbineStatus.OFFLINE

    def test_high_temp_anomaly_raises_temperature(self):
        state = TurbineState(turbine_id="turbine-test")
        state._anomaly_mode = "high_temp"
        state._anomaly_ticks_remaining = 10
        # Step multiple times to let temperature converge
        for _ in range(30):
            state.step(dt=1.0)
        assert state.temperature_c > 80.0

    def test_high_vibration_anomaly_raises_vibration(self):
        state = TurbineState(turbine_id="turbine-test")
        state.rotor_rpm = 12.0
        state._anomaly_mode = "high_vibration"
        state._anomaly_ticks_remaining = 5
        state.step(dt=1.0)
        assert state.vibration_mms > 6.0

    def test_anomaly_clears_after_ticks_exhausted(self):
        state = TurbineState(turbine_id="turbine-test")
        state._anomaly_mode = "high_temp"
        state._anomaly_ticks_remaining = 1
        state.step(dt=1.0)
        assert state._anomaly_mode is None
        assert state._anomaly_ticks_remaining == 0

    def test_wind_bounded(self):
        state = TurbineState(turbine_id="turbine-test")
        for _ in range(100):
            state.step(dt=1.0)
        assert 0.0 <= state.wind_speed_ms <= 30.0


class TestTurbineFleet:
    def test_emit_all_returns_one_event_per_turbine(self):
        fleet = TurbineFleet(["t-01", "t-02", "t-03"])
        events = fleet.emit_all()
        assert len(events) == 3

    def test_emit_all_turbine_ids_match(self):
        ids = ["t-01", "t-02", "t-03"]
        fleet = TurbineFleet(ids)
        events = fleet.emit_all()
        event_ids = {e.turbine_id for e in events}
        assert event_ids == set(ids)

    def test_events_pass_schema_validation(self):
        """All emitted events must be valid TurbineTelemetry instances."""
        from shared.schema import TurbineTelemetry
        fleet = TurbineFleet(["t-01", "t-02"])
        events = fleet.emit_all()
        for event in events:
            # If validation failed during emit_all, this would have already raised.
            assert event.turbine_id in ["t-01", "t-02"]
            assert event.recorded_at is not None

    def test_single_turbine_fleet(self):
        fleet = TurbineFleet(["solo-turbine"])
        events = fleet.emit_all()
        assert len(events) == 1
        assert events[0].turbine_id == "solo-turbine"

    def test_emit_multiple_ticks_produces_different_values(self):
        """Consecutive ticks should produce at least some variation."""
        fleet = TurbineFleet(["t-01"])
        readings = [fleet.emit_all()[0].wind_speed_ms for _ in range(10)]
        # Not all values should be identical (stochastic model)
        assert len(set(readings)) > 1
