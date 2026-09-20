"""Read-only integrity and timing check for the BTCUSDT evidence database."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def audit(path: str | Path) -> tuple[dict[str, object], list[str]]:
    database = Path(path).resolve()
    if not database.is_file():
        raise FileNotFoundError(database)
    uri = database.as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    failures: list[str] = []
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        counts = {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("sessions", "book_ticker", "agg_trades", "stream_anomalies")
        }
        open_sessions = int(conn.execute(
            "SELECT COUNT(*) FROM sessions WHERE status!='CLOSED' OR ended_wall_ns IS NULL"
        ).fetchone()[0])
        crossed_quotes = int(conn.execute(
            "SELECT COUNT(*) FROM book_ticker WHERE CAST(bid_price AS REAL)>CAST(ask_price AS REAL)"
        ).fetchone()[0])
        bad_quotes = int(conn.execute(
            "SELECT COUNT(*) FROM book_ticker WHERE CAST(bid_price AS REAL)<=0 "
            "OR CAST(ask_price AS REAL)<=0 OR CAST(bid_qty AS REAL)<0 OR CAST(ask_qty AS REAL)<0"
        ).fetchone()[0])
        bad_trades = int(conn.execute(
            "SELECT COUNT(*) FROM agg_trades WHERE CAST(price AS REAL)<=0 "
            "OR CAST(quantity AS REAL)<=0 OR event_time_ms<=0 OR trade_time_ms<=0"
        ).fetchone()[0])
        negative_latency = 0
        latencies: list[float] = []
        latency_rows = []
        for agg_id, session_id, receive_ns, event_ms, trade_ms in conn.execute(
            "SELECT agg_trade_id,session_id,receive_wall_ns,event_time_ms,trade_time_ms "
            "FROM agg_trades"
        ):
            latency = receive_ns / 1_000_000 - event_ms
            latencies.append(latency)
            latency_rows.append((latency, agg_id, session_id, receive_ns, event_ms, trade_ms))
            negative_latency += latency < 0
        raw_mismatches = 0
        for agg_id, raw in conn.execute("SELECT agg_trade_id,raw_json FROM agg_trades"):
            try:
                payload = json.loads(raw)["data"]
                raw_mismatches += int(int(payload["a"]) != agg_id)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                raw_mismatches += 1
        anomaly_details = []
        in_session_anomalies = 0
        for kind, session_id, previous_id, current_id, first_id in conn.execute(
            "SELECT a.anomaly_type,a.session_id,a.previous_id,a.current_id,"
            "(SELECT MIN(t.agg_trade_id) FROM agg_trades t WHERE t.session_id=a.session_id) "
            "FROM stream_anomalies a ORDER BY a.id"
        ):
            at_session_start = current_id is not None and current_id == first_id
            classification = "SESSION_START_GAP" if (
                kind == "POSSIBLE_AGG_TRADE_ID_GAP" and at_session_start
            ) else "IN_SESSION_ANOMALY"
            in_session_anomalies += classification == "IN_SESSION_ANOMALY"
            anomaly_details.append({
                "type": kind,
                "classification": classification,
                "session_id": session_id,
                "previous_id": previous_id,
                "current_id": current_id,
            })
        if integrity != "ok":
            failures.append("SQLite integrity_check failed")
        if counts["book_ticker"] == 0 or counts["agg_trades"] == 0:
            failures.append("required evidence table is empty")
        if open_sessions:
            failures.append("session was not closed cleanly")
        if crossed_quotes or bad_quotes:
            failures.append("invalid best bid/ask row found")
        if bad_trades or raw_mismatches:
            failures.append("invalid or inconsistent aggregate trade row found")
        if negative_latency:
            failures.append("exchange event time is ahead of local receive clock")
        if in_session_anomalies:
            failures.append("stream anomaly occurred after a session had started receiving trades")
        latency_p95 = percentile(latencies, 0.95)
        latency_p99 = percentile(latencies, 0.99)
        latency_max = percentile(latencies, 1)
        timing_warnings = []
        if latency_p95 is not None and latency_p95 > 500:
            timing_warnings.append("trade receive latency p95 exceeds 500 ms")
        if latency_max is not None and latency_max > 5_000:
            timing_warnings.append("trade receive latency maximum exceeds 5000 ms")
        clock_rows = conn.execute(
            "SELECT session_id,receive_monotonic_ns,receive_wall_ns FROM book_ticker "
            "UNION ALL "
            "SELECT session_id,receive_monotonic_ns,receive_wall_ns FROM agg_trades "
            "ORDER BY session_id,receive_monotonic_ns"
        )
        clock_by_session: dict[str, dict[str, float | int]] = {}
        previous_session = None
        previous_offset = None
        for session_id, monotonic_ns, wall_ns in clock_rows:
            offset_ms = (wall_ns - monotonic_ns) / 1_000_000
            stats = clock_by_session.setdefault(session_id, {
                "rows": 0,
                "min_offset_ms": offset_ms,
                "max_offset_ms": offset_ms,
                "max_adjacent_step_ms": 0.0,
            })
            stats["rows"] = int(stats["rows"]) + 1
            stats["min_offset_ms"] = min(float(stats["min_offset_ms"]), offset_ms)
            stats["max_offset_ms"] = max(float(stats["max_offset_ms"]), offset_ms)
            if session_id == previous_session and previous_offset is not None:
                stats["max_adjacent_step_ms"] = max(
                    float(stats["max_adjacent_step_ms"]), abs(offset_ms - previous_offset)
                )
            previous_session, previous_offset = session_id, offset_ms
        for stats in clock_by_session.values():
            stats["offset_span_ms"] = (
                float(stats["max_offset_ms"]) - float(stats["min_offset_ms"])
            )
        max_clock_span = max(
            (float(stats["offset_span_ms"]) for stats in clock_by_session.values()),
            default=0.0,
        )
        max_clock_step = max(
            (float(stats["max_adjacent_step_ms"]) for stats in clock_by_session.values()),
            default=0.0,
        )
        if max_clock_span > 250:
            timing_warnings.append("local wall clock moved by more than 250 ms within a session")
        runtime_table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_diagnostics'"
        ).fetchone() is not None
        runtime_by_type = {}
        stale_socket_waits: list[float] = []
        if runtime_table_exists:
            for kind, count, maximum in conn.execute(
                "SELECT diagnostic_type,COUNT(*),MAX(duration_ms) "
                "FROM runtime_diagnostics GROUP BY diagnostic_type"
            ):
                runtime_by_type[kind] = {"count": count, "max_duration_ms": maximum}
            stale_socket_waits = [row[0] for row in conn.execute(
                "SELECT socket_wait_ms FROM runtime_diagnostics "
                "WHERE diagnostic_type='STALE_AGG_TRADE' AND socket_wait_ms IS NOT NULL"
            )]
        worst_latency_rows = []
        for latency, agg_id, session_id, receive_ns, event_ms, trade_ms in sorted(
            latency_rows, reverse=True
        )[:10]:
            worst_latency_rows.append({
                "agg_trade_id": agg_id,
                "session_id": session_id,
                "receive_utc": datetime.fromtimestamp(
                    receive_ns / 1_000_000_000, tz=timezone.utc
                ).isoformat(timespec="milliseconds"),
                "receive_minus_event_ms": latency,
                "event_minus_trade_ms": event_ms - trade_ms,
            })
        report: dict[str, object] = {
            "database": str(database),
            "integrity": integrity,
            "counts": counts,
            "open_sessions": open_sessions,
            "crossed_quotes": crossed_quotes,
            "bad_quotes": bad_quotes,
            "bad_trades": bad_trades,
            "raw_trade_mismatches": raw_mismatches,
            "negative_latency_rows": negative_latency,
            "anomalies": {
                "session_start_gaps": len(anomaly_details) - in_session_anomalies,
                "in_session": in_session_anomalies,
                "details": anomaly_details,
            },
            "trade_event_latency_ms": {
                "min": percentile(latencies, 0),
                "p50": percentile(latencies, 0.50),
                "p95": latency_p95,
                "p99": latency_p99,
                "max": latency_max,
                "over_250ms": sum(value > 250 for value in latencies),
                "over_1000ms": sum(value > 1_000 for value in latencies),
                "over_5000ms": sum(value > 5_000 for value in latencies),
                "worst_rows": worst_latency_rows,
            },
            "dual_clock_diagnostics": {
                "max_offset_span_ms": max_clock_span,
                "max_adjacent_step_ms": max_clock_step,
                "sessions": clock_by_session,
            },
            "runtime_diagnostics": {
                "available": runtime_table_exists,
                "by_type": runtime_by_type,
                "stale_trade_socket_wait_ms": {
                    "p50": percentile(stale_socket_waits, 0.50),
                    "p95": percentile(stale_socket_waits, 0.95),
                    "max": percentile(stale_socket_waits, 1),
                },
            },
            "timing_warnings": timing_warnings,
        }
        return report, failures
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", nargs="?", default="microstructure_btcusdt.db")
    args = parser.parse_args()
    try:
        report, failures = audit(args.database)
    except (FileNotFoundError, sqlite3.Error) as exc:
        print(f"AUDIT_ERROR {type(exc).__name__}: {exc}")
        return 2
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if failures:
        print("AUDIT_FAIL " + "; ".join(failures))
        return 1
    if report["timing_warnings"]:
        print("AUDIT_WARN read_only=true; " + "; ".join(report["timing_warnings"]))
        return 0
    print("AUDIT_PASS read_only=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
