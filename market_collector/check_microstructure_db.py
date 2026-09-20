"""Read-only integrity and timing check for the BTCUSDT evidence database."""

from __future__ import annotations

import argparse
import json
import sqlite3
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
        for receive_ns, event_ms in conn.execute(
            "SELECT receive_wall_ns,event_time_ms FROM agg_trades"
        ):
            latency = receive_ns / 1_000_000 - event_ms
            latencies.append(latency)
            negative_latency += latency < 0
        raw_mismatches = 0
        for agg_id, raw in conn.execute("SELECT agg_trade_id,raw_json FROM agg_trades"):
            try:
                payload = json.loads(raw)["data"]
                raw_mismatches += int(int(payload["a"]) != agg_id)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                raw_mismatches += 1
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
            "trade_event_latency_ms": {
                "min": percentile(latencies, 0),
                "p50": percentile(latencies, 0.50),
                "p95": percentile(latencies, 0.95),
                "p99": percentile(latencies, 0.99),
                "max": percentile(latencies, 1),
            },
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
    print("AUDIT_PASS read_only=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
