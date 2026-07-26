"""Standalone, research-only MEXC Futures Min1 candle backfill.

MEXC's existing public candle parser exposes OHLC but not volume.  Rows written
by this collector therefore use ``0.0`` as a neutral, explicitly unavailable
volume value; no market data is inferred.
"""

from __future__ import annotations

import argparse
import math
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mexc_pole_missed_signal_audit import (  # noqa: E402
    MEXC_INTERVAL_NAMES,
    MEXC_KLINE_PAGE_LIMIT,
    MEXC_REQUIRED_INTERVAL_SECONDS,
    _fetch_mexc_public_candle_page,
    fetch_mexc_public_candles,
    mexc_contract_symbol,
)
from pnf_mvp.storage import Storage  # noqa: E402

MEXC_FUTURES_BASE_URL = "https://contract.mexc.com"
INTERVAL = "1m"
INTERVAL_SECONDS = MEXC_REQUIRED_INTERVAL_SECONDS
INTERVAL_MS = INTERVAL_SECONDS * 1000
MEXC_INTERVAL = MEXC_INTERVAL_NAMES[INTERVAL_SECONDS]
DEFAULT_SYMBOLS = (
    "MEXC_FUT:BTCUSDT", "MEXC_FUT:ETHUSDT", "MEXC_FUT:BNBUSDT",
    "MEXC_FUT:SOLUSDT", "MEXC_FUT:XRPUSDT", "MEXC_FUT:SUIUSDT",
    "MEXC_FUT:TAOUSDT", "MEXC_FUT:HYPEUSDT", "MEXC_FUT:ENAUSDT",
)
OBVIOUS_LIVE_DB_NAMES = {"mexc_live_candles.db", "live_candles.db"}


@dataclass(frozen=True)
class Validation:
    symbol: str
    requested_start_ms: int
    requested_end_ms: int
    first_available_ms: int | None
    count: int
    earliest_open_ms: int | None
    latest_open_ms: int | None
    earliest_close_ms: int | None
    latest_close_ms: int | None
    duplicates: int
    missing: int
    invalid_ohlc: int
    timestamp_errors: int

    @property
    def continuous(self) -> bool:
        return not (self.duplicates or self.missing or self.invalid_ohlc or self.timestamp_errors)


def parse_iso_utc_to_ms(value: str) -> int:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def normalize_symbol(value: str) -> str:
    symbol = value.strip().upper()
    if not symbol.startswith("MEXC_FUT:") or not symbol.split(":", 1)[1]:
        raise ValueError(f"MEXC Futures symbol must use MEXC_FUT:<CONTRACT> format: {value!r}")
    # Exercise the shared conversion and reject malformed internal names early.
    if "_" not in mexc_contract_symbol(symbol):
        raise ValueError(f"cannot convert MEXC Futures symbol: {value!r}")
    return symbol


def assert_second_timestamp(ts: int) -> None:
    if ts <= 0 or ts > 10_000_000_000:
        raise ValueError(f"MEXC candle timestamp is not Unix seconds: {ts}")
    if ts % INTERVAL_SECONDS:
        raise ValueError(f"MEXC candle timestamp is not Min1-aligned: {ts}")


def storage_times(close_ts_seconds: int) -> tuple[int, int]:
    assert_second_timestamp(close_ts_seconds)
    close_ms = close_ts_seconds * 1000
    open_ms = close_ms - INTERVAL_MS
    assert close_ms > 10_000_000_000 and open_ms > 10_000_000_000
    assert close_ms - open_ms == INTERVAL_MS
    return open_ms, close_ms


def _refuse_live_path(db_path: Path) -> None:
    resolved = db_path.expanduser().resolve()
    configured_live = (REPO_ROOT / "pnf_mvp/data/mexc_live_candles.db").resolve()
    if resolved == configured_live or resolved.name.lower() in OBVIOUS_LIVE_DB_NAMES:
        raise ValueError(f"refusing live candle database path: {db_path}")


def _existing_close_times(conn: sqlite3.Connection, symbol: str, start_ms: int, end_ms: int) -> set[int]:
    rows = conn.execute(
        "SELECT close_time FROM candles WHERE symbol=? AND interval=? AND open_time>=? AND open_time<?",
        (symbol, INTERVAL, start_ms, end_ms),
    ).fetchall()
    return {int(row[0]) // 1000 for row in rows if int(row[0]) % 1000 == 0}


def find_first_available(
    symbol: str,
    first_close_s: int,
    last_close_s: int,
    page_fetcher: Callable = _fetch_mexc_public_candle_page,
) -> int | None:
    """Find the first returned candle without treating pre-listing time as a gap."""
    cursor = first_close_s
    span = INTERVAL_SECONDS * (MEXC_KLINE_PAGE_LIMIT - 1)
    while cursor <= last_close_s:
        page_end = min(last_close_s, cursor + span)
        rows, _ = page_fetcher(MEXC_FUTURES_BASE_URL, symbol, cursor, page_end, MEXC_INTERVAL)
        timestamps = sorted(int(row[0]) for row in rows if cursor <= int(row[0]) <= page_end)
        if timestamps:
            assert_second_timestamp(timestamps[0])
            return timestamps[0]
        cursor = page_end + INTERVAL_SECONDS
    return None


def _ranges(values: Iterable[int]) -> list[tuple[int, int]]:
    ordered = sorted(set(values))
    if not ordered:
        return []
    out: list[tuple[int, int]] = []
    start = previous = ordered[0]
    for value in ordered[1:]:
        if value == previous + INTERVAL_SECONDS:
            previous = value
        else:
            out.append((start, previous))
            start = previous = value
    out.append((start, previous))
    return out


def backfill_symbol(
    storage: Storage,
    symbol: str,
    start_ms: int,
    end_ms: int,
    pause_seconds: float,
    *,
    verify_only: bool = False,
    fetcher: Callable = fetch_mexc_public_candles,
    page_fetcher: Callable = _fetch_mexc_public_candle_page,
) -> Validation:
    symbol = normalize_symbol(symbol)
    first_close_s, last_close_s = start_ms // 1000 + INTERVAL_SECONDS, end_ms // 1000
    available_s = find_first_available(symbol, first_close_s, last_close_s, page_fetcher)
    if available_s is not None:
        present = _existing_close_times(storage.conn, symbol, start_ms, end_ms)
        expected = range(available_s, last_close_s + 1, INTERVAL_SECONDS)
        missing_ranges = _ranges(ts for ts in expected if ts not in present)
        if not verify_only:
            storage.upsert_symbol(symbol, "MEXC_FUT", "PERP", "USDT" if symbol.endswith("USDT") else "")
            for range_start, range_end in missing_ranges:
                rows = fetcher(MEXC_FUTURES_BASE_URL, symbol, range_start, range_end, MEXC_INTERVAL, INTERVAL_SECONDS)
                for close_s, open_, high, low, close in rows:
                    open_ms, close_ms = storage_times(int(close_s))
                    storage.insert_candle(symbol, INTERVAL, open_ms, close_ms, float(open_), float(high), float(low), float(close), 0.0)
                if pause_seconds > 0:
                    time.sleep(pause_seconds)
    return validate_symbol(storage.conn, symbol, start_ms, end_ms, available_s)


def validate_symbol(conn: sqlite3.Connection, symbol: str, start_ms: int, end_ms: int, first_available_s: int | None) -> Validation:
    rows = conn.execute(
        "SELECT open_time,close_time,open,high,low,close FROM candles "
        "WHERE symbol=? AND interval=? AND open_time>=? AND open_time<? ORDER BY open_time",
        (symbol, INTERVAL, start_ms, end_ms),
    ).fetchall()
    groups = conn.execute(
        "SELECT COUNT(*)-1 FROM candles WHERE symbol=? AND interval=? AND open_time>=? AND open_time<? "
        "GROUP BY open_time HAVING COUNT(*)>1",
        (symbol, INTERVAL, start_ms, end_ms),
    ).fetchall()
    duplicates = sum(int(row[0]) for row in groups)
    timestamp_errors = sum(
        1 for row in rows
        if int(row[0]) <= 10_000_000_000 or int(row[1]) <= 10_000_000_000
        or int(row[1]) - int(row[0]) != INTERVAL_MS or int(row[0]) % INTERVAL_MS
    )
    # A seconds-valued row cannot satisfy the millisecond range query above;
    # search its corresponding seconds range explicitly so it is reported rather
    # than merely looking like a missing candle.
    timestamp_errors += int(conn.execute(
        "SELECT COUNT(*) FROM candles WHERE symbol=? AND interval=? AND open_time>=? AND open_time<?",
        (symbol, INTERVAL, start_ms // 1000, end_ms // 1000),
    ).fetchone()[0])
    invalid_ohlc = 0
    for row in rows:
        open_, high, low, close = map(float, row[2:6])
        if (
            not all(math.isfinite(value) for value in (open_, high, low, close))
            or min(open_, high, low, close) <= 0
            or low > high
            or high < max(open_, close)
            or low > min(open_, close)
        ):
            invalid_ohlc += 1
    closes = {int(row[1]) // 1000 for row in rows if int(row[1]) % 1000 == 0}
    missing = 0 if first_available_s is None else sum(
        1 for ts in range(first_available_s, end_ms // 1000 + 1, INTERVAL_SECONDS) if ts not in closes
    )
    return Validation(
        symbol, start_ms, end_ms, None if first_available_s is None else first_available_s * 1000,
        len(rows), int(rows[0][0]) if rows else None, int(rows[-1][0]) if rows else None,
        int(rows[0][1]) if rows else None, int(rows[-1][1]) if rows else None,
        duplicates, missing, invalid_ohlc, timestamp_errors,
    )


def print_validation(result: Validation) -> None:
    print(
        f"symbol={result.symbol} REQUESTED_START={result.requested_start_ms} REQUESTED_END={result.requested_end_ms} "
        f"FIRST_AVAILABLE={result.first_available_ms} stored_rows={result.count} "
        f"earliest_open_time={result.earliest_open_ms} latest_open_time={result.latest_open_ms} "
        f"earliest_close_time={result.earliest_close_ms} latest_close_time={result.latest_close_ms} "
        f"duplicates={result.duplicates} missing_1m_intervals={result.missing} "
        f"invalid_ohlc={result.invalid_ohlc} timestamp_errors={result.timestamp_errors} "
        f"continuity={'PASS' if result.continuous else 'FAIL'}"
    )


def run(db_path: str, symbols: Iterable[str], start_ms: int, end_ms: int, pause_seconds: float, verify_only: bool = False) -> int:
    path = Path(db_path)
    _refuse_live_path(path)
    storage = Storage(str(path))
    failed = False
    for symbol in symbols:
        result = backfill_symbol(storage, symbol, start_ms, end_ms, pause_seconds, verify_only=verify_only)
        print_validation(result)
        failed |= not result.continuous
    return 1 if failed else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only MEXC Futures Min1 historical backfill")
    parser.add_argument("--db-path", required=True, help="New or existing isolated research SQLite database")
    parser.add_argument("--symbol", action="append", dest="symbols", help="Repeatable MEXC_FUT:<CONTRACT> symbol")
    parser.add_argument("--start", required=True, help="Inclusive candle open UTC time (ISO-8601)")
    parser.add_argument("--end", required=True, help="Exclusive candle open UTC time (ISO-8601)")
    parser.add_argument("--pause-seconds", type=float, default=0.05)
    parser.add_argument("--verify-only", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    start_ms, end_ms = parse_iso_utc_to_ms(args.start), parse_iso_utc_to_ms(args.end)
    if start_ms % INTERVAL_MS or end_ms % INTERVAL_MS or start_ms >= end_ms:
        raise ValueError("--start/--end must be minute-aligned and start must precede end")
    return run(args.db_path, args.symbols or DEFAULT_SYMBOLS, start_ms, end_ms, max(0.0, args.pause_seconds), args.verify_only)


if __name__ == "__main__":
    raise SystemExit(main())
