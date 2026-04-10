"""Standalone research-only importer for Binance Vision USD-M futures monthly 1m klines.

- Reads monthly ZIP kline files from Binance Vision (UM futures)
- Writes only to research SQLite DB (default: data/pnf_mvp_research.sqlite3)
- Uses existing Storage upsert APIs for idempotent UPSERT behavior
"""

from __future__ import annotations

import argparse
import csv
import io
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from storage import Storage

BINANCE_VISION_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"
DEFAULT_DB_PATH = "data/pnf_mvp_research.sqlite3"
DEFAULT_INTERVAL = "1m"
DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "SUIUSDT",
    "TAOUSDT",
    "ENAUSDT",
    "HYPEUSDT",
]


@dataclass(frozen=True)
class YearMonth:
    year: int
    month: int

    def to_token(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def parse_year_month(text: str) -> YearMonth:
    try:
        parsed = datetime.strptime(text.strip(), "%Y-%m")
    except ValueError as exc:
        raise ValueError(f"Invalid month format '{text}'. Expected YYYY-MM.") from exc
    return YearMonth(parsed.year, parsed.month)


def iter_months(start: YearMonth, end: YearMonth) -> list[YearMonth]:
    if (start.year, start.month) > (end.year, end.month):
        raise ValueError("--start-month must be <= --end-month")

    out: list[YearMonth] = []
    year = start.year
    month = start.month
    while (year, month) <= (end.year, end.month):
        out.append(YearMonth(year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return out


def parse_symbols(symbols_text: str) -> list[str]:
    symbols: list[str] = []
    for token in symbols_text.split(","):
        symbol = token.strip().upper()
        if symbol:
            symbols.append(symbol)
    if not symbols:
        raise ValueError("At least one symbol is required")
    return symbols


def namespace_symbol(symbol: str) -> str:
    return f"BINANCE_FUT:{symbol.upper()}"


def resolve_base_quote(symbol: str) -> str:
    upper = symbol.upper()
    known_quotes = ("USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH")
    for quote in known_quotes:
        if upper.endswith(quote):
            return quote
    return ""


def build_month_zip_url(symbol: str, ym: YearMonth) -> str:
    token = ym.to_token()
    return (
        f"{BINANCE_VISION_BASE}/{symbol}/{DEFAULT_INTERVAL}/"
        f"{symbol}-{DEFAULT_INTERVAL}-{token}.zip"
    )


def download_month_rows(symbol: str, ym: YearMonth) -> list[list[str]] | None:
    url = build_month_zip_url(symbol, ym)
    try:
        with urlopen(url, timeout=30) as response:
            payload = response.read()
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    except URLError:
        raise

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = archive.namelist()
        if not names:
            return []
        with archive.open(names[0], "r") as fh:
            text_stream = io.TextIOWrapper(fh, encoding="utf-8")
            reader = csv.reader(text_stream)
            return [row for row in reader if row]


def import_symbol_month(storage: Storage, ns_symbol: str, rows: list[list[str]]) -> int:
    upserts = 0
    for row in rows:
        # Binance kline row:
        # 0 open_time, 1 open, 2 high, 3 low, 4 close, 5 volume, 6 close_time, ...
        open_time = int(row[0])
        close_time = int(row[6])
        storage.insert_candle(
            symbol=ns_symbol,
            interval=DEFAULT_INTERVAL,
            open_time=open_time,
            close_time=close_time,
            open_price=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
        )
        upserts += 1
    return upserts


def run(db_path: str, symbols: list[str], start_month: YearMonth, end_month: YearMonth) -> int:
    storage = Storage(db_path)
    months = iter_months(start_month, end_month)
    total_upserts = 0

    for raw_symbol in symbols:
        symbol = raw_symbol.upper().strip()
        ns_symbol = namespace_symbol(symbol)

        storage.upsert_symbol(
            symbol=ns_symbol,
            exchange="BINANCE_FUT",
            asset_type="PERP",
            base_quote=resolve_base_quote(symbol),
        )

        symbol_upserts = 0
        found_any_month = False

        for ym in months:
            rows = download_month_rows(symbol, ym)
            if rows is None:
                continue
            found_any_month = True
            symbol_upserts += import_symbol_month(storage, ns_symbol, rows)

        if not found_any_month:
            print("symbol not available on Binance Vision")

        total_upserts += symbol_upserts
        print(f"[{symbol}] upserts={symbol_upserts}")

    print(f"total_upserts={total_upserts} db_path={db_path}")
    return total_upserts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only Binance Vision UM futures monthly 1m importer")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="SQLite output DB path")
    parser.add_argument("--start-month", required=True, help="Inclusive start month, format YYYY-MM")
    parser.add_argument("--end-month", required=True, help="Inclusive end month, format YYYY-MM")
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated symbols, e.g. BTCUSDT,ETHUSDT,SOLUSDT",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    start_month = parse_year_month(args.start_month)
    end_month = parse_year_month(args.end_month)
    symbols = parse_symbols(args.symbols)

    run(
        db_path=args.db_path,
        symbols=symbols,
        start_month=start_month,
        end_month=end_month,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
