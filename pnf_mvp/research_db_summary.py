"""Terminal summary utility for research candle coverage in SQLite."""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB_PATH = "data/pnf_mvp_research_clean.sqlite3"


@dataclass
class SymbolSummary:
    symbol: str
    first_open_time: int | None
    last_open_time: int | None
    candle_rows: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Visual summary of imported research candles per symbol"
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"SQLite DB path (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated symbols (e.g. BTCUSDT,ETHUSDT or BINANCE_FUT:BTCUSDT)",
    )
    return parser.parse_args()


def parse_symbols_filter(raw: str) -> list[str]:
    if not raw.strip():
        return []

    requested: list[str] = []
    for token in raw.split(","):
        symbol = token.strip().upper()
        if not symbol:
            continue
        if ":" in symbol:
            requested.append(symbol)
        else:
            requested.append(f"BINANCE_FUT:{symbol}")

    deduped: list[str] = []
    seen: set[str] = set()
    for symbol in requested:
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped.append(symbol)
    return deduped


def ms_to_utc_text(ms_value: int | None) -> str:
    if ms_value is None:
        return "-"
    dt = datetime.fromtimestamp(ms_value / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def estimate_days(first_ms: int | None, last_ms: int | None) -> float:
    if first_ms is None or last_ms is None:
        return 0.0
    return max(0.0, (last_ms - first_ms) / 86_400_000)


def month_hint(first_ms: int | None, last_ms: int | None) -> str:
    if first_ms is None or last_ms is None:
        return "-"

    first_dt = datetime.fromtimestamp(first_ms / 1000, tz=timezone.utc)
    last_dt = datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc)

    span_months = (last_dt.year - first_dt.year) * 12 + (last_dt.month - first_dt.month) + 1
    if span_months <= 1:
        return first_dt.strftime("~1 month (%Y-%m)")
    return f"~{span_months} months ({first_dt.strftime('%Y-%m')} to {last_dt.strftime('%Y-%m')})"


def status_label(rows: int, days: float) -> str:
    if rows == 0:
        return "EMPTY"
    if days < 14:
        return "PARTIAL"
    return "OK"


def fetch_summary(conn: sqlite3.Connection, symbols_filter: list[str]) -> list[SymbolSummary]:
    if symbols_filter:
        placeholders = ",".join("?" for _ in symbols_filter)
        query = f"""
            SELECT
                symbol,
                MIN(open_time) AS first_open_time,
                MAX(open_time) AS last_open_time,
                COUNT(*) AS candle_rows
            FROM candles
            WHERE symbol IN ({placeholders})
            GROUP BY symbol
            ORDER BY symbol ASC
        """
        rows = conn.execute(query, symbols_filter).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                symbol,
                MIN(open_time) AS first_open_time,
                MAX(open_time) AS last_open_time,
                COUNT(*) AS candle_rows
            FROM candles
            GROUP BY symbol
            ORDER BY symbol ASC
            """
        ).fetchall()

    return [
        SymbolSummary(
            symbol=row["symbol"],
            first_open_time=row["first_open_time"],
            last_open_time=row["last_open_time"],
            candle_rows=row["candle_rows"],
        )
        for row in rows
    ]


def render_table(summaries: list[SymbolSummary]) -> str:
    headers = [
        "Status",
        "Symbol",
        "First candle (UTC)",
        "Last candle (UTC)",
        "Candles",
        "Days",
        "Coverage hint",
    ]

    body_rows: list[list[str]] = []
    for summary in summaries:
        days = estimate_days(summary.first_open_time, summary.last_open_time)
        body_rows.append(
            [
                status_label(summary.candle_rows, days),
                summary.symbol,
                ms_to_utc_text(summary.first_open_time),
                ms_to_utc_text(summary.last_open_time),
                f"{summary.candle_rows:,}",
                f"{days:,.1f}",
                month_hint(summary.first_open_time, summary.last_open_time),
            ]
        )

    widths = [len(header) for header in headers]
    for row in body_rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    def fmt(row: list[str]) -> str:
        return " | ".join(value.ljust(widths[idx]) for idx, value in enumerate(row))

    divider = "-+-".join("-" * width for width in widths)
    lines = [fmt(headers), divider]
    lines.extend(fmt(row) for row in body_rows)
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)
    symbols_filter = parse_symbols_filter(args.symbols)

    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return 1

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        total_candles = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
        summaries = fetch_summary(conn, symbols_filter)

    print("Research DB Candle Coverage Summary")
    print(f"DB path: {db_path}")
    print(f"Total symbols found: {len(summaries)}")
    print(f"Total candles in DB: {total_candles:,}")
    if symbols_filter:
        print(f"Symbol filter: {', '.join(symbols_filter)}")

    if not summaries:
        print("\nNo candle rows matched the current filter.")
        return 0

    print()
    print(render_table(summaries))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
