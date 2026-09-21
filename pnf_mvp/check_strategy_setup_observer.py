"""Read-only integrity and lifecycle audit for the ideal-entry observer database."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def check(database_path: str | Path) -> dict[str, object]:
    database = Path(database_path).resolve()
    if not database.is_file():
        raise FileNotFoundError(database)
    conn = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if "setup_occurrences" not in tables:
            raise RuntimeError("setup_occurrences table missing")
        lifecycle = dict(conn.execute(
            "SELECT lifecycle,COUNT(*) FROM setup_occurrences GROUP BY lifecycle"
        ).fetchall())
        total = conn.execute("SELECT COUNT(*) FROM setup_occurrences").fetchone()[0]
        invalid_windows = conn.execute(
            "SELECT COUNT(*) FROM setup_occurrences WHERE expires_wall_ns IS NOT NULL "
            "AND expires_wall_ns<=available_wall_ns"
        ).fetchone()[0]
        duplicate_open_keys = conn.execute(
            "SELECT COUNT(*) FROM (SELECT symbol,setup_key,COUNT(*) AS n "
            "FROM setup_occurrences WHERE lifecycle='OPEN' GROUP BY symbol,setup_key HAVING n>1)"
        ).fetchone()[0]
        invalid_open_expiry = conn.execute(
            "SELECT COUNT(*) FROM setup_occurrences WHERE lifecycle='OPEN' "
            "AND expires_wall_ns IS NOT NULL"
        ).fetchone()[0]
        duplicate_setup_keys = conn.execute(
            "SELECT COUNT(*) FROM (SELECT symbol,setup_key,COUNT(*) AS n "
            "FROM setup_occurrences GROUP BY symbol,setup_key HAVING n>1)"
        ).fetchone()[0]
        invalid_scheduled_expiry = conn.execute(
            "SELECT COUNT(*) FROM setup_occurrences "
            "WHERE scheduled_expires_wall_ns IS NULL "
            "OR scheduled_expires_wall_ns<=available_wall_ns"
        ).fetchone()[0]
        exceeded_scheduled_expiry = conn.execute(
            "SELECT COUNT(*) FROM setup_occurrences WHERE expires_wall_ns IS NOT NULL "
            "AND scheduled_expires_wall_ns IS NOT NULL "
            "AND expires_wall_ns>scheduled_expires_wall_ns"
        ).fetchone()[0]
        latest = conn.execute(
            "SELECT occurrence_id,symbol,side,status,quality_score,reference_close_ms,"
            "available_wall_ns,expires_wall_ns,lifecycle,ideal_entry,close_reason "
            "FROM setup_occurrences ORDER BY available_wall_ns DESC LIMIT 5"
        ).fetchall()
        columns = ["occurrence_id", "symbol", "side", "status", "quality_score",
                   "reference_close_ms", "available_wall_ns", "expires_wall_ns",
                   "lifecycle", "ideal_entry", "close_reason"]
        report = {
            "database": str(database), "integrity": integrity,
            "total_occurrences": total, "lifecycle_counts": lifecycle,
            "invalid_windows": invalid_windows,
            "duplicate_open_keys": duplicate_open_keys,
            "duplicate_setup_keys": duplicate_setup_keys,
            "invalid_open_expiry": invalid_open_expiry,
            "invalid_scheduled_expiry": invalid_scheduled_expiry,
            "exceeded_scheduled_expiry": exceeded_scheduled_expiry,
            "latest_occurrences": [dict(zip(columns, row)) for row in latest],
            "read_only": True,
        }
        report["pass"] = (
            integrity == "ok" and invalid_windows == 0 and
            duplicate_open_keys == 0 and duplicate_setup_keys == 0 and
            invalid_open_expiry == 0 and invalid_scheduled_expiry == 0 and
            exceeded_scheduled_expiry == 0
        )
        return report
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", nargs="?", default="data/ideal_entry_observer.sqlite3")
    args = parser.parse_args()
    try:
        report = check(args.database)
    except (FileNotFoundError, RuntimeError, sqlite3.Error) as exc:
        print(f"OBSERVER_AUDIT_FAIL {type(exc).__name__}: {exc}")
        return 1
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("OBSERVER_AUDIT_PASS read_only=true" if report["pass"] else "OBSERVER_AUDIT_FAIL integrity_or_lifecycle")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
