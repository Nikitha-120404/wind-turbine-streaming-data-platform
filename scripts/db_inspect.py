#!/usr/bin/env python3
"""
scripts/db_inspect.py

Query TimescaleDB to verify data ingestion for the Wind Turbine Platform.
Run from the project root: python scripts/db_inspect.py

Usage:
  python scripts/db_inspect.py --counts          # row counts for all tables
  python scripts/db_inspect.py --latest          # latest reading per turbine (core fields)
  python scripts/db_inspect.py --extended        # latest extended sensor readings per turbine
  python scripts/db_inspect.py --thermal         # thermal health status per turbine
  python scripts/db_inspect.py --alerts          # recent alerts (includes extended sensor rules)
  python scripts/db_inspect.py --dlq             # DLQ events
  python scripts/db_inspect.py --chunks          # TimescaleDB chunk info
  python scripts/db_inspect.py --aggregates      # check 5min and 1h aggregate rows
  python scripts/db_inspect.py --all             # run all checks
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
import psycopg2.extras

from shared.config import get_database_settings

cfg = get_database_settings()


def get_conn():
    return psycopg2.connect(
        host=cfg.db_host,
        port=cfg.db_port,
        dbname=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
    )


def _print_table(rows, headers):
    if not rows:
        print("  (no rows)")
        return
    widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0)) for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  " + "  ".join("─" * w for w in widths))
    for row in rows:
        print(fmt.format(*[str(v) if v is not None else "NULL" for v in row]))
    print()


def show_counts(conn) -> None:
    print("\n── Row counts ──────────────────────────────────")
    queries = [
        ("turbine_telemetry (all)",        "SELECT COUNT(*) FROM turbine_telemetry"),
        ("turbine_telemetry (last 5m)",     "SELECT COUNT(*) FROM turbine_telemetry WHERE recorded_at > NOW() - INTERVAL '5 minutes'"),
        ("turbine_telemetry (last 1h)",     "SELECT COUNT(*) FROM turbine_telemetry WHERE recorded_at > NOW() - INTERVAL '1 hour'"),
        ("turbine_alerts (all)",            "SELECT COUNT(*) FROM turbine_alerts"),
        ("turbine_alerts (last 1h)",        "SELECT COUNT(*) FROM turbine_alerts WHERE alert_ts > NOW() - INTERVAL '1 hour'"),
        ("turbine_alerts CRITICAL (last 1h)","SELECT COUNT(*) FROM turbine_alerts WHERE severity='CRITICAL' AND alert_ts > NOW() - INTERVAL '1 hour'"),
        ("dlq_events",                      "SELECT COUNT(*) FROM dlq_events"),
        ("dlq_events unresolved",           "SELECT COUNT(*) FROM dlq_events WHERE resolved = FALSE"),
    ]
    with conn.cursor() as cur:
        rows = []
        for label, sql in queries:
            cur.execute(sql)
            count = cur.fetchone()[0]
            rows.append((label, f"{count:,}"))
        _print_table(rows, ["Query", "Count"])


def show_latest(conn) -> None:
    print("\n── Latest core telemetry per turbine ────────────")
    sql = """
        SELECT turbine_id,
               last_seen AT TIME ZONE 'UTC'  AS ts,
               last_wind_ms,
               last_rpm,
               last_power_kw,
               last_temp_c,
               last_vibration,
               status
        FROM turbine_latest_status
        ORDER BY turbine_id
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        _print_table(rows, ["turbine_id", "last_seen", "wind_ms", "rpm", "power_kw", "temp_c", "vib", "status"])


def show_extended(conn) -> None:
    """
    Show extended sensor readings — the fields that came from the original
    wind_turbine_sensorlog.py (nacelle, bearing, gearbox, gear, ambient, hub).
    """
    print("\n── Latest extended sensor readings per turbine ──")
    sql = """
        SELECT turbine_id,
               last_seen AT TIME ZONE 'UTC'        AS ts,
               last_nacelle_deg,
               last_wind_direction_deg,
               last_ambient_temp_c,
               last_bearing_temp_c,
               last_gearbox_sump_temp_c,
               last_gear_temp_c,
               last_hub_speed
        FROM turbine_latest_status
        ORDER BY turbine_id
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        _print_table(rows, [
            "turbine_id", "last_seen",
            "nacelle_deg", "wind_dir",
            "ambient_c", "bearing_c",
            "gbx_sump_c", "gear_c", "hub_spd"
        ])


def show_thermal(conn) -> None:
    """
    Thermal health view — shows turbines with elevated temperatures.
    Covers generator, bearing, gearbox, and gear temperatures.
    """
    print("\n── Thermal health status ────────────────────────")
    sql = """
        SELECT turbine_id,
               recorded_at AT TIME ZONE 'UTC' AS ts,
               temperature_c     AS gen_temp_c,
               bearing_temp_c,
               gearbox_sump_temp_c AS gbx_sump_c,
               gear_temp_c,
               thermal_status
        FROM turbine_thermal_health
        ORDER BY
            CASE thermal_status
                WHEN 'bearing_warning'  THEN 1
                WHEN 'gear_warning'     THEN 2
                WHEN 'gearbox_warning'  THEN 3
                WHEN 'generator_warning' THEN 4
                ELSE 5
            END,
            turbine_id
    """
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            _print_table(rows, ["turbine_id", "ts", "gen_c", "bearing_c", "gbx_sump_c", "gear_c", "status"])
        except Exception as exc:
            print(f"  (thermal view unavailable — run after DB is seeded: {exc})")


def show_alerts(conn, limit: int = 20) -> None:
    print(f"\n── Recent alerts (last {limit}) ──────────────────")
    sql = """
        SELECT turbine_id,
               recorded_at AT TIME ZONE 'UTC' AS ts,
               severity,
               rule_name,
               metric_value
        FROM turbine_alerts
        ORDER BY alert_ts DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
        _print_table(rows, ["turbine_id", "recorded_at", "severity", "rule_name", "metric_value"])

    # Rule breakdown
    print("  Alert counts by rule (all time):")
    sql2 = """
        SELECT rule_name, severity, COUNT(*) AS cnt
        FROM turbine_alerts
        GROUP BY rule_name, severity
        ORDER BY cnt DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql2)
        rows = cur.fetchall()
        _print_table(rows, ["rule_name", "severity", "count"])


def show_dlq(conn) -> None:
    print("\n── DLQ events ──────────────────────────────────")
    sql = """
        SELECT dlq_id, original_topic, error_type,
               failed_at AT TIME ZONE 'UTC' AS ts,
               retry_count, resolved
        FROM dlq_events
        ORDER BY failed_at DESC
        LIMIT 20
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        _print_table(rows, ["dlq_id", "topic", "error_type", "failed_at", "retries", "resolved"])


def show_chunks(conn) -> None:
    print("\n── TimescaleDB chunks (turbine_telemetry) ────────")
    sql = """
        SELECT chunk_name,
               range_start AT TIME ZONE 'UTC',
               range_end AT TIME ZONE 'UTC',
               pg_size_pretty(total_bytes) AS size,
               is_compressed
        FROM timescaledb_information.chunks
        WHERE hypertable_name = 'turbine_telemetry'
        ORDER BY range_start DESC
        LIMIT 10
    """
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            _print_table(rows, ["chunk", "range_start", "range_end", "size", "compressed"])
        except Exception as exc:
            print(f"  (chunk info unavailable: {exc})")


def show_aggregates(conn) -> None:
    """
    Check both continuous aggregates — the 5-minute one from the original
    TimeScaleDB_Queries.txt and the 1-hour one from the platform.
    """
    print("\n── Continuous aggregates ────────────────────────")
    for view, label in [
        ("turbine_metrics_5min", "5-minute (from original TimeScaleDB_Queries.txt)"),
        ("turbine_metrics_1h",   "1-hour   (platform aggregate)"),
    ]:
        sql = f"""
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT turbine_id) AS turbines,
                   MIN(bucket) AT TIME ZONE 'UTC' AS earliest,
                   MAX(bucket) AT TIME ZONE 'UTC' AS latest
            FROM {view}
        """
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
                row = cur.fetchone()
                print(f"  {label}")
                print(f"    rows={row[0]:,}  turbines={row[1]}  earliest={row[2]}  latest={row[3]}")
            except Exception as exc:
                print(f"  {label}: unavailable ({exc})")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="DB inspector for Wind Turbine Platform")
    parser.add_argument("--counts",     action="store_true", help="Row counts")
    parser.add_argument("--latest",     action="store_true", help="Latest core readings per turbine")
    parser.add_argument("--extended",   action="store_true", help="Latest extended sensor readings (bearing, gearbox, etc.)")
    parser.add_argument("--thermal",    action="store_true", help="Thermal health status")
    parser.add_argument("--alerts",     action="store_true", help="Recent alerts")
    parser.add_argument("--dlq",        action="store_true", help="DLQ events")
    parser.add_argument("--chunks",     action="store_true", help="TimescaleDB chunk info")
    parser.add_argument("--aggregates", action="store_true", help="Continuous aggregate status")
    parser.add_argument("--all",        action="store_true", help="Run all checks")
    args = parser.parse_args()

    run_all = args.all or not any([
        args.counts, args.latest, args.extended, args.thermal,
        args.alerts, args.dlq, args.chunks, args.aggregates
    ])

    try:
        conn = get_conn()
    except Exception as exc:
        print(f"Cannot connect to database: {exc}")
        sys.exit(1)

    if args.counts     or run_all: show_counts(conn)
    if args.latest     or run_all: show_latest(conn)
    if args.extended   or run_all: show_extended(conn)
    if args.thermal    or run_all: show_thermal(conn)
    if args.alerts     or run_all: show_alerts(conn)
    if args.dlq        or run_all: show_dlq(conn)
    if args.aggregates or run_all: show_aggregates(conn)
    if args.chunks     or run_all: show_chunks(conn)

    conn.close()


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
import psycopg2.extras

from shared.config import get_database_settings

cfg = get_database_settings()


def get_conn():
    return psycopg2.connect(
        host=cfg.db_host,
        port=cfg.db_port,
        dbname=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
    )


def _print_table(rows, headers):
    if not rows:
        print("  (no rows)")
        return
    widths = [max(len(str(h)), max(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  " + "  ".join("─" * w for w in widths))
    for row in rows:
        print(fmt.format(*[str(v) for v in row]))
    print()


def show_counts(conn) -> None:
    print("\n── Row counts ──────────────────────────────────")
    queries = [
        ("turbine_telemetry", "SELECT COUNT(*) FROM turbine_telemetry"),
        ("turbine_alerts", "SELECT COUNT(*) FROM turbine_alerts"),
        ("dlq_events", "SELECT COUNT(*) FROM dlq_events"),
        ("telemetry (last 5m)", "SELECT COUNT(*) FROM turbine_telemetry WHERE recorded_at > NOW() - INTERVAL '5 minutes'"),
        ("alerts (last 1h)", "SELECT COUNT(*) FROM turbine_alerts WHERE alert_ts > NOW() - INTERVAL '1 hour'"),
    ]
    with conn.cursor() as cur:
        rows = []
        for label, sql in queries:
            cur.execute(sql)
            count = cur.fetchone()[0]
            rows.append((label, f"{count:,}"))
        _print_table(rows, ["Table / Window", "Count"])


def show_latest(conn) -> None:
    print("\n── Latest telemetry per turbine ─────────────────")
    sql = """
        SELECT turbine_id,
               recorded_at AT TIME ZONE 'UTC' AS ts,
               wind_speed_ms,
               rotor_rpm,
               power_output_kw,
               temperature_c,
               vibration_mms,
               status
        FROM turbine_latest_status
        ORDER BY turbine_id
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        headers = ["turbine_id", "recorded_at", "wind_ms", "rpm", "power_kw", "temp_c", "vib_mms", "status"]
        _print_table(rows, headers)


def show_alerts(conn, limit: int = 20) -> None:
    print(f"\n── Recent alerts (last {limit}) ──────────────────")
    sql = """
        SELECT turbine_id,
               recorded_at AT TIME ZONE 'UTC' AS ts,
               severity,
               rule_name,
               metric_value
        FROM turbine_alerts
        ORDER BY alert_ts DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
        _print_table(rows, ["turbine_id", "recorded_at", "severity", "rule_name", "metric_value"])


def show_dlq(conn) -> None:
    print("\n── DLQ events ──────────────────────────────────")
    sql = """
        SELECT dlq_id, original_topic, error_type,
               failed_at AT TIME ZONE 'UTC' AS ts,
               retry_count, resolved
        FROM dlq_events
        ORDER BY failed_at DESC
        LIMIT 20
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        _print_table(rows, ["dlq_id", "topic", "error_type", "failed_at", "retries", "resolved"])


def show_chunks(conn) -> None:
    print("\n── TimescaleDB chunks (turbine_telemetry) ────────")
    sql = """
        SELECT chunk_name,
               range_start AT TIME ZONE 'UTC',
               range_end AT TIME ZONE 'UTC',
               pg_size_pretty(total_bytes) AS size,
               is_compressed
        FROM timescaledb_information.chunks
        WHERE hypertable_name = 'turbine_telemetry'
        ORDER BY range_start DESC
        LIMIT 10
    """
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            _print_table(rows, ["chunk", "range_start", "range_end", "size", "compressed"])
        except Exception as exc:
            print(f"  (chunk info unavailable: {exc})")


def main() -> None:
    parser = argparse.ArgumentParser(description="DB inspector for Wind Turbine Platform")
    parser.add_argument("--counts", action="store_true")
    parser.add_argument("--latest", action="store_true")
    parser.add_argument("--alerts", action="store_true")
    parser.add_argument("--dlq", action="store_true")
    parser.add_argument("--chunks", action="store_true")
    parser.add_argument("--all", action="store_true", help="Run all checks")
    args = parser.parse_args()

    run_all = args.all or not any([args.counts, args.latest, args.alerts, args.dlq, args.chunks])

    try:
        conn = get_conn()
    except Exception as exc:
        print(f"Cannot connect to database: {exc}")
        sys.exit(1)

    if args.counts or run_all:
        show_counts(conn)
    if args.latest or run_all:
        show_latest(conn)
    if args.alerts or run_all:
        show_alerts(conn)
    if args.dlq or run_all:
        show_dlq(conn)
    if args.chunks or run_all:
        show_chunks(conn)

    conn.close()


if __name__ == "__main__":
    main()
