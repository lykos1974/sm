"""Standalone research backfill for Binance USD-M futures candles.

- Writes only to a research SQLite DB (default: data/pnf_mvp_research.sqlite3)
- Stores symbols under BINANCE_FUT:<SYMBOL> namespace
- Supports CLI overrides for DB path, symbols, interval, and date range
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Iterable

from storage import Storage

BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"
DEFAULT_DB_PATH = "data/pnf_mvp_research.sqlite3"
DEFAULT_INTERVAL = "1m"
DEFAULT_LIMIT = 1500
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]


def parse_iso_utc_to_ms(value: str) -> int:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def interval_to_ms(interval: str) -> int:
    unit = interval[-1]
    amount = int(interval[:-1])
    unit_map = {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }
    if unit not in unit_map:
        raise ValueError(f"Unsupported interval: {interval}")
    return amount * unit_map[unit]


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int) -> list[list]:
    params = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
    )
    url = f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/klines?{params}"
    with urllib.request.urlopen(url, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected response for {symbol}: {payload}")
    return payload


def namespace_symbol(symbol: str) -> str:
    return f"BINANCE_FUT:{symbol.upper()}"


def resolve_base_quote(symbol: str) -> str:
    upper = symbol.upper()
    known_quotes = ("USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH")
    for quote in known_quotes:
        if upper.endswith(quote):
            return quote
    return ""


def backfill_symbol(
    storage: Storage,
    raw_symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    pause_seconds: float,
) -> int:
    symbol = raw_symbol.upper().strip()
    ns_symbol = namespace_symbol(symbol)
    storage.upsert_symbol(
        symbol=ns_symbol,
        exchange="BINANCE_FUT",
        asset_type="PERP",
        base_quote=resolve_base_quote(symbol),
    )

    step_ms = interval_to_ms(interval)
    cursor = start_ms
    inserted = 0

    while cursor < end_ms:
        page_end = min(end_ms, cursor + (step_ms * limit) - 1)
        rows = fetch_klines(symbol, interval, cursor, page_end, limit)
        if not rows:
            break

        prev_open_time: int | None = None
        for row in rows:
            open_time = int(row[0])
            close_time = int(row[6])
            if prev_open_time is not None and open_time - prev_open_time != step_ms:
                raise RuntimeError(
                    "Non-continuous kline page for "
                    f"{symbol} {interval}: prev_open={prev_open_time} "
                    f"current_open={open_time} expected_step_ms={step_ms} "
                    f"window_start={cursor} window_end={page_end}"
                )
            storage.insert_candle(
                symbol=ns_symbol,
                interval=interval,
                open_time=open_time,
                close_time=close_time,
                open_price=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
            )
            inserted += 1
            prev_open_time = open_time

        last_open = int(rows[-1][0])
        next_cursor = last_open + step_ms
        if next_cursor <= cursor:
            break
        cursor = next_cursor

        if len(rows) < limit:
            break

        if pause_seconds > 0:
            time.sleep(pause_seconds)

    return inserted


def parse_symbols(symbols_text: str) -> list[str]:
    out = []
    for token in symbols_text.split(","):
        symbol = token.strip().upper()
        if symbol:
            out.append(symbol)
    if not out:
        raise ValueError("At least one symbol is required.")
    return out


def run(symbols: Iterable[str], interval: str, db_path: str, start_ms: int, end_ms: int, limit: int, pause_seconds: float) -> int:
    storage = Storage(db_path)
    total = 0
    for symbol in symbols:
        inserted = backfill_symbol(
            storage=storage,
            raw_symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=limit,
            pause_seconds=pause_seconds,
        )
        total += inserted
        print(f"[{symbol}] upserts={inserted}")
    return total


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only Binance Futures historical backfill")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="SQLite output DB path")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help="Comma-separated Binance Futures symbols")
    parser.add_argument("--interval", default=DEFAULT_INTERVAL, help="Binance kline interval, e.g. 1m,5m,1h")
    parser.add_argument(
        "--start",
        required=True,
        help="UTC start datetime (ISO-8601), e.g. 2026-01-01T00:00:00Z",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="UTC end datetime (ISO-8601), e.g. 2026-01-02T00:00:00Z",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Klines per request (max 1500)")
    parser.add_argument("--pause-seconds", type=float, default=0.05, help="Pause between paginated API requests")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.limit <= 0 or args.limit > 1500:
        raise ValueError("--limit must be between 1 and 1500")

    symbols = parse_symbols(args.symbols)
    start_ms = parse_iso_utc_to_ms(args.start)
    end_ms = parse_iso_utc_to_ms(args.end)
    if start_ms >= end_ms:
        raise ValueError("--start must be before --end")

    total = run(
        symbols=symbols,
        interval=args.interval,
        db_path=args.db_path,
        start_ms=start_ms,
        end_ms=end_ms,
        limit=args.limit,
        pause_seconds=max(0.0, args.pause_seconds),
    )
    print(f"total_upserts={total} db_path={args.db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
