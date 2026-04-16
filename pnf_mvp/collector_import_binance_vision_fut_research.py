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
from pathlib import Path
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


def build_local_zip_candidates(local_root: str, symbol: str, ym: YearMonth) -> list[Path]:
    token = ym.to_token()
    root = Path(local_root)
    return [
        root / symbol / DEFAULT_INTERVAL / f"{symbol}-{DEFAULT_INTERVAL}-{token}.zip",
        root / f"{symbol}-{DEFAULT_INTERVAL}-{token}.zip",
    ]


def resolve_local_month_zip_path(local_root: str, symbol: str, ym: YearMonth) -> Path:
    token = ym.to_token()
    root = Path(local_root)
    return root / symbol / DEFAULT_INTERVAL / f"{symbol}-{DEFAULT_INTERVAL}-{token}.zip"


def download_month_zip_to_path(symbol: str, ym: YearMonth, target_path: str | Path) -> str:
    url = build_month_zip_url(symbol, ym)
    with urlopen(url, timeout=30) as response:
        payload = response.read()

    out_path = Path(target_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(payload)
    return url


def load_month_zip_payload(symbol: str, ym: YearMonth, local_root: str | None) -> tuple[bytes | None, str]:
    if local_root:
        candidates = build_local_zip_candidates(local_root, symbol, ym)
        for candidate in candidates:
            if candidate.exists():
                return candidate.read_bytes(), str(candidate)
        return None, f"LOCAL_MISSING ({candidates[0]} | {candidates[1]})"

    url = build_month_zip_url(symbol, ym)
    try:
        with urlopen(url, timeout=30) as response:
            return response.read(), f"REMOTE {url}"
    except HTTPError as exc:
        if exc.code == 404:
            return None, f"REMOTE_404 {url}"
        raise
    except URLError:
        raise


def parse_month_rows_from_payload(payload: bytes) -> list[list[str]]:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = archive.namelist()
        if not names:
            return []
        with archive.open(names[0], "r") as fh:
            text_stream = io.TextIOWrapper(fh, encoding="utf-8")
            reader = csv.reader(text_stream)
            return [row for row in reader if row]


def download_month_rows(symbol: str, ym: YearMonth, local_root: str | None = None) -> tuple[list[list[str]] | None, str]:
    payload, source = load_month_zip_payload(symbol, ym, local_root)
    if payload is None:
        return None, source

    return parse_month_rows_from_payload(payload), source


def import_symbol_month(storage: Storage, ns_symbol: str, rows: list[list[str]]) -> int:
    batch: list[tuple[object, ...]] = []

    for row in rows:
        if not row:
            continue
        if row[0].strip().lower() == "open_time":
            continue
        # Binance kline row:
        # 0 open_time, 1 open, 2 high, 3 low, 4 close, 5 volume, 6 close_time, ...
        batch.append(
            (
                ns_symbol,
                DEFAULT_INTERVAL,
                int(row[0]),
                int(row[6]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
            )
        )

    if not batch:
        return 0

    storage.conn.executemany(
        """
        INSERT INTO candles(symbol, interval, open_time, close_time, open, high, low, close, volume)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, interval, open_time) DO UPDATE SET
          close_time=excluded.close_time,
          open=excluded.open,
          high=excluded.high,
          low=excluded.low,
          close=excluded.close,
          volume=excluded.volume
        """,
        batch,
    )
    storage.conn.commit()
    return len(batch)


def run(
    db_path: str,
    symbols: list[str],
    start_month: YearMonth,
    end_month: YearMonth,
    local_root: str | None = None,
    dry_run: bool = False,
) -> int:
    storage = None if dry_run else Storage(db_path)
    months = iter_months(start_month, end_month)
    total_upserts = 0

    for raw_symbol in symbols:
        symbol = raw_symbol.upper().strip()
        ns_symbol = namespace_symbol(symbol)

        if not dry_run and storage is not None:
            storage.upsert_symbol(
                symbol=ns_symbol,
                exchange="BINANCE_FUT",
                asset_type="PERP",
                base_quote=resolve_base_quote(symbol),
            )

        symbol_upserts = 0
        found_any_month = False

        for ym in months:
            rows, source = download_month_rows(symbol, ym, local_root=local_root)
            month_token = ym.to_token()

            if dry_run:
                if rows is None:
                    print(f"[DRY-RUN] [{symbol}] {month_token} SKIP {source}")
                else:
                    print(f"[DRY-RUN] [{symbol}] {month_token} USE {source} rows={len(rows)}")

            if rows is None:
                if local_root:
                    print(f"[{symbol}] {month_token} missing local ZIP -> skip")
                continue
            found_any_month = True
            if not dry_run and storage is not None:
                symbol_upserts += import_symbol_month(storage, ns_symbol, rows)

        if not found_any_month:
            if local_root:
                print(f"[{symbol}] no matching local monthly ZIP files in --local-root")
            else:
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
        "--local-root",
        default=None,
        help="Optional local root for monthly ZIP files; when set, remote download is disabled",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List symbol/month/file decisions without writing to DB",
    )
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
        local_root=args.local_root,
        dry_run=bool(args.dry_run),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
