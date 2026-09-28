"""Export completed observer lifecycles to the causal fill-evaluator CSV schema."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path


FIELDS = ["order_id", "symbol", "side", "available_wall_ns", "expires_wall_ns",
          "ideal_entry", "tick_size"]


def export_orders(database_path, output_path, tick_size) -> int:
    database, output = Path(database_path).resolve(), Path(output_path).resolve()
    try:
        tick = Decimal(str(tick_size))
    except InvalidOperation as exc:
        raise ValueError("invalid tick size") from exc
    if tick <= 0:
        raise ValueError("tick size must be positive")
    if not database.is_file():
        raise FileNotFoundError(database)
    conn = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT occurrence_id,symbol,side,available_wall_ns,expires_wall_ns,ideal_entry "
            "FROM setup_occurrences WHERE lifecycle IN ('WITHDRAWN','INTERRUPTED','EXPIRED') "
            "AND expires_wall_ns IS NOT NULL AND expires_wall_ns>available_wall_ns "
            "ORDER BY available_wall_ns,occurrence_id"
        ).fetchall()
    finally:
        conn.close()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(zip(FIELDS[:-1], row), tick_size=str(tick)))
    temporary.replace(output)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="data/ideal_entry_observer.sqlite3")
    parser.add_argument("--output", default="data/observed_ideal_entry_orders.csv")
    parser.add_argument("--tick-size", required=True)
    args = parser.parse_args()
    try:
        rows = export_orders(args.database, args.output, args.tick_size)
    except (FileNotFoundError, ValueError, sqlite3.Error) as exc:
        print(f"OBSERVER_EXPORT_FAIL {type(exc).__name__}: {exc}")
        return 1
    print(f"OBSERVER_EXPORT_PASS rows={rows} output={Path(args.output).resolve()} read_only_source=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
