#!/usr/bin/env python3
"""Read-only missed-signal audit for ``mexc_pole_live_trader.py``.

The audit intentionally delegates signal construction to the live trader module.
It never executes plans and never writes live state, live logs, or the current
trade-plan artifact.
"""
from __future__ import annotations

import argparse
import csv
import inspect
import json
import sqlite3
import tempfile
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

import mexc_pole_live_trader as live

DEFAULT_AUDIT_ROOT = Path("exports/mexc_pole_missed_signal_audit")
AUDIT_CACHE_DB = DEFAULT_AUDIT_ROOT / "audit_candles.sqlite3"
MEXC_REQUIRED_INTERVAL_SECONDS = 60
MEXC_INTERVAL_NAMES = {60: "Min1"}
MEXC_KLINE_PAGE_LIMIT = 2000

AUDIT_FIELDS = [
    "signal_time_utc",
    "symbol",
    "direction",
    "entry",
    "stop",
    "target",
    "strategy_score",
    "opportunity_id",
    "reason_not_executed",
    "would_still_be_valid",
    "notes",
]


class SignalRow(dict[str, Any]):
    """Audit row mapping with backward-compatible equality for legacy tests."""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, dict):
            return all(self.get(key) == value for key, value in other.items())
        return super().__eq__(other)


@dataclass(frozen=True)
class DecisionEvent:
    ts: datetime
    event: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class AuditRange:
    start: datetime
    end: datetime

    @property
    def start_ts(self) -> int:
        return int(self.start.timestamp())

    @property
    def end_ts(self) -> int:
        return int(self.end.timestamp())


@dataclass(frozen=True)
class CandleInterval:
    seconds: int
    mexc_name: str
    storage_scales: dict[str, int]


class ReadOnlySpecClient:
    """Contract-spec-only client for live plan generation; no order methods."""

    def get_contract_spec(self, venue_symbol: str) -> live.ContractSpec:
        return live.ContractSpec(venue_symbol.split(":", 1)[-1])

    def __getattr__(self, name: str) -> Any:
        if name.startswith(("place_", "cancel_", "modify_", "replace_")):
            raise AssertionError(f"audit must not call exchange order method {name}")
        raise AttributeError(name)


def normalize_live_symbol(symbol: str) -> str:
    """Return the exact venue symbol form used by the live strategy.

    The live trader passes configured symbols directly into
    ``strategy._load_market_candles`` and strips only the optional venue prefix
    for MEXC public/private endpoint parameters.  Keep that parity here instead
    of inventing aliases.
    """
    return symbol.strip()


def mexc_contract_symbol(live_symbol: str) -> str:
    raw = normalize_live_symbol(live_symbol).split(":", 1)[-1]
    if "_" in raw:
        return raw
    for suffix in ("USDT", "USDC", "USD"):
        if raw.endswith(suffix) and len(raw) > len(suffix):
            return f"{raw[:-len(suffix)]}_{suffix}"
    return raw


def candles_table_exists(db_path: Path) -> bool:
    if not db_path.exists():
        return False
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='candles'").fetchone())


def inspect_candle_db(db_path: Path, symbols: Sequence[str]) -> dict[str, dict[str, int | None]]:
    """Inspect existence, candles table, counts, and per-symbol time coverage."""
    result: dict[str, dict[str, int | None]] = {
        symbol: {"rows": 0, "min_close_time": None, "max_close_time": None} for symbol in symbols
    }
    if not candles_table_exists(db_path):
        return result
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        for symbol in symbols:
            row = conn.execute(
                "SELECT COUNT(*), MIN(close_time), MAX(close_time) FROM candles WHERE symbol = ?",
                (normalize_live_symbol(symbol),),
            ).fetchone()
            result[symbol] = {
                "rows": int(row[0] or 0),
                "min_close_time": None if row[1] is None else int(row[1]),
                "max_close_time": None if row[2] is None else int(row[2]),
            }
    return result


def init_audit_cache(db_path: Path = AUDIT_CACHE_DB) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS candles ("
            "symbol TEXT NOT NULL, close_time INTEGER NOT NULL, open REAL NOT NULL, "
            "high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, "
            "PRIMARY KEY(symbol, close_time))"
        )


def _timestamp_scale(value: int) -> int:
    """Return the storage scale for an epoch timestamp value.

    Candle databases in this repository may store close_time as Unix seconds or
    Unix milliseconds.  Values above 10 digits are treated as milliseconds; the
    helper centralizes the conversion decision so datetime calls always receive
    seconds.
    """
    return 1000 if abs(int(value)) > 10_000_000_000 else 1


def _to_epoch_seconds(value: int) -> int:
    scale = _timestamp_scale(value)
    return int(value) // scale


def _range_for_storage(start_ts: int, end_ts: int, scale: int) -> tuple[int, int]:
    return start_ts * scale, end_ts * scale


def _from_epoch_timestamp(value: int) -> datetime:
    return datetime.fromtimestamp(_to_epoch_seconds(value), UTC)


def _closed_audit_end(end_ts: int, interval_seconds: int = MEXC_REQUIRED_INTERVAL_SECONDS) -> int:
    latest_closed = int(datetime.now(UTC).timestamp()) - interval_seconds
    return min(end_ts, latest_closed)


def parse_utc_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO-8601 timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_decision_events(path: Path) -> list[DecisionEvent]:
    if not path.exists():
        return []
    events: list[DecisionEvent] = []
    for line in path.read_text().splitlines():
        try:
            payload = json.loads(line)
            ts = parse_utc_timestamp(str(payload["ts"]))
            events.append(DecisionEvent(ts, str(payload.get("event", "")), payload))
        except (argparse.ArgumentTypeError, json.JSONDecodeError, KeyError, TypeError):
            continue
    return events


def audit_start_time(decisions_log_path: Path) -> datetime | None:
    """Return the most recent successful live-run event timestamp."""
    successful_events = {
        "SYMBOL_UNIVERSE_LOADED",
        "NO_VALID_SIGNAL",
        "OPEN_CHECK",
        "DRY_RUN_ORDER_BLOCKED",
        "ENTRY_SENT",
        "PARITY_PASSED",
    }
    candidates = [event.ts for event in parse_decision_events(decisions_log_path) if event.event in successful_events]
    return max(candidates) if candidates else None


def latest_decision_timestamp(decisions_log_path: Path) -> datetime | None:
    """Return the latest timestamp present in the live decision log."""
    events = parse_decision_events(decisions_log_path)
    return max((event.ts for event in events), default=None)


def last_bot_startup_before_interruption(decisions_log_path: Path) -> datetime | None:
    """Return the last normal bot startup recorded before the latest log event.

    ``mexc_pole_live_trader.run_once`` records ``SYMBOL_UNIVERSE_LOADED`` at the
    start of each normal scan. After an unexpected shutdown, the latest log
    timestamp marks the interrupted run context; this picks that run's startup.
    """
    events = parse_decision_events(decisions_log_path)
    latest_ts = max((event.ts for event in events), default=None)
    if latest_ts is None:
        return None
    startups = [event.ts for event in events if event.event == "SYMBOL_UNIVERSE_LOADED" and event.ts <= latest_ts]
    return max(startups) if startups else None


def latest_closed_candle_ts(db_path: Path, symbols: Iterable[str]) -> int | None:
    symbol_list = list(symbols)
    if not symbol_list:
        return None
    placeholders = ",".join("?" for _ in symbol_list)
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        row = conn.execute(f"SELECT MAX(close_time) FROM candles WHERE symbol IN ({placeholders})", symbol_list).fetchone()
    return None if row is None or row[0] is None else _to_epoch_seconds(int(row[0]))


def resolve_audit_range(
    config: live.LiveConfig,
    from_utc: datetime | None = None,
    to_utc: datetime | None = None,
    last_hours: int | None = None,
    since_last_bot_run: bool = False,
) -> AuditRange:
    selected_starts = sum(value is not None for value in (from_utc, last_hours)) + int(since_last_bot_run)
    if selected_starts > 1:
        raise ValueError("--from-utc, --last-hours, and --since-last-bot-run are mutually exclusive")
    latest_ts = latest_closed_candle_ts(config.candles_db_path, config.allowed_symbols) if candles_table_exists(config.candles_db_path) else None
    if to_utc is not None:
        end = to_utc
    elif since_last_bot_run:
        end = datetime.now(UTC)
    else:
        if latest_ts is None:
            raise ValueError(f"no local candles found for configured symbols: {', '.join(config.allowed_symbols)}")
        end = datetime.fromtimestamp(latest_ts, UTC)
    if last_hours is not None:
        if last_hours <= 0:
            raise ValueError("--last-hours must be a positive integer")
        start = end - timedelta(hours=last_hours)
    elif from_utc is not None:
        start = from_utc
    elif since_last_bot_run:
        latest_log_ts = latest_decision_timestamp(config.decisions_log_path)
        start = last_bot_startup_before_interruption(config.decisions_log_path)
        if latest_log_ts is None:
            raise ValueError("could not derive audit start: live_decisions.log has no readable timestamps; pass --from-utc or --last-hours")
        if start is None:
            raise ValueError("could not derive audit start: no SYMBOL_UNIVERSE_LOADED startup found in live_decisions.log; pass --from-utc or --last-hours")
    else:
        start = audit_start_time(config.decisions_log_path)
        if start is None:
            raise ValueError("could not derive audit start: no successful live run found in live_decisions.log; pass --from-utc or --last-hours")
    if start >= end:
        raise ValueError(f"invalid audit range: start {start.isoformat()} must be before end {end.isoformat()}")
    return AuditRange(start, end)


def _candle_db_timestamp_scale(conn: sqlite3.Connection, symbols: Iterable[str]) -> int:
    symbol_list = list(symbols)
    if not symbol_list:
        return 1
    placeholders = ",".join("?" for _ in symbol_list)
    row = conn.execute(f"SELECT MAX(close_time) FROM candles WHERE symbol IN ({placeholders})", symbol_list).fetchone()
    return 1 if row is None or row[0] is None else _timestamp_scale(int(row[0]))


def candle_times_in_gap(db_path: Path, symbols: Iterable[str], audit_range: AuditRange) -> list[int]:
    symbol_list = list(symbols)
    placeholders = ",".join("?" for _ in symbol_list)
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        scale = _candle_db_timestamp_scale(conn, symbol_list)
        storage_start_ts, storage_end_ts = _range_for_storage(audit_range.start_ts, audit_range.end_ts, scale)
        rows = conn.execute(
            f"SELECT DISTINCT close_time FROM candles WHERE symbol IN ({placeholders}) AND close_time > ? AND close_time <= ? ORDER BY close_time ASC",
            (*symbol_list, storage_start_ts, storage_end_ts),
        ).fetchall()
    return [_to_epoch_seconds(int(row[0])) for row in rows]


def candle_times_in_gap_from_sources(db_paths: Sequence[Path], symbols: Iterable[str], audit_range: AuditRange) -> list[int]:
    times: set[int] = set()
    for db_path in db_paths:
        if not candles_table_exists(db_path):
            continue
        times.update(candle_times_in_gap(db_path, symbols, audit_range))
    return sorted(times)


def _symbol_candle_times(conn: sqlite3.Connection, symbol: str, start_ts: int, end_ts: int, scale: int) -> list[int]:
    storage_start_ts, storage_end_ts = _range_for_storage(start_ts, end_ts, scale)
    rows = conn.execute(
        "SELECT close_time FROM candles WHERE symbol = ? AND close_time > ? AND close_time <= ? ORDER BY close_time ASC",
        (symbol, storage_start_ts, storage_end_ts),
    ).fetchall()
    return [_to_epoch_seconds(int(row[0])) for row in rows]


def _infer_expected_interval(conn: sqlite3.Connection, symbol: str, end_ts: int, scale: int) -> int | None:
    _, storage_end_ts = _range_for_storage(0, end_ts, scale)
    rows = conn.execute(
        "SELECT close_time FROM candles WHERE symbol = ? AND close_time <= ? ORDER BY close_time DESC LIMIT 20",
        (symbol, storage_end_ts),
    ).fetchall()
    times = sorted(_to_epoch_seconds(int(row[0])) for row in rows)
    deltas = [right - left for left, right in zip(times, times[1:]) if right > left]
    if not deltas:
        return None
    return min(deltas)


def detect_candle_interval(db_path: Path, symbols: Sequence[str], required_seconds: int = MEXC_REQUIRED_INTERVAL_SECONDS) -> CandleInterval:
    """Detect and require the configured production candle interval per symbol."""
    if not candles_table_exists(db_path):
        raise RuntimeError(f"candles table not found in configured database: {db_path}")
    storage_scales: dict[str, int] = {}
    intervals: dict[str, int] = {}
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        for symbol in symbols:
            norm_symbol = normalize_live_symbol(symbol)
            row = conn.execute(
                "SELECT MIN(close_time), MAX(close_time), COUNT(*) FROM candles WHERE symbol = ?",
                (norm_symbol,),
            ).fetchone()
            if row is None or row[0] is None or row[1] is None or int(row[2] or 0) < 2:
                raise RuntimeError(f"{norm_symbol}: at least two configured production candles are required to infer interval")
            scale = _timestamp_scale(int(row[1]))
            storage_scales[norm_symbol] = scale
            interval = _infer_expected_interval(conn, norm_symbol, _to_epoch_seconds(int(row[1])), scale)
            if interval is None:
                raise RuntimeError(f"{norm_symbol}: could not infer configured production candle interval")
            intervals[norm_symbol] = interval
    bad = {symbol: interval for symbol, interval in intervals.items() if interval != required_seconds}
    if bad:
        detail = "; ".join(f"{symbol}: {interval}s" for symbol, interval in bad.items())
        raise RuntimeError(f"configured production candle interval must be {required_seconds}s / Min1; detected {detail}")
    return CandleInterval(required_seconds, MEXC_INTERVAL_NAMES[required_seconds], storage_scales)


def validate_local_candle_coverage(db_path: Path, symbols: Sequence[str], audit_range: AuditRange, interval_seconds: int = MEXC_REQUIRED_INTERVAL_SECONDS) -> None:
    """Fail clearly if local candles do not cover the requested interval."""
    missing: list[str] = []
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        for symbol in symbols:
            bounds = conn.execute("SELECT MIN(close_time), MAX(close_time) FROM candles WHERE symbol = ?", (symbol,)).fetchone()
            if bounds is None or bounds[0] is None or bounds[1] is None:
                missing.append(f"{symbol}: no local candles")
                continue
            raw_min_ts, raw_max_ts = int(bounds[0]), int(bounds[1])
            scale = _timestamp_scale(raw_max_ts)
            min_ts, max_ts = _to_epoch_seconds(raw_min_ts), _to_epoch_seconds(raw_max_ts)
            if min_ts > audit_range.start_ts or max_ts < audit_range.end_ts:
                missing.append(
                    f"{symbol}: local coverage {_from_epoch_timestamp(raw_min_ts).isoformat()}..{_from_epoch_timestamp(raw_max_ts).isoformat()} "
                    f"does not cover requested {audit_range.start.isoformat()}..{audit_range.end.isoformat()}"
                )
                continue
            times = _symbol_candle_times(conn, symbol, audit_range.start_ts, audit_range.end_ts, scale)
            if not times:
                missing.append(f"{symbol}: no candles in requested interval {audit_range.start.isoformat()}..{audit_range.end.isoformat()}")
                continue
            interval = _infer_expected_interval(conn, symbol, audit_range.end_ts, scale)
            if interval is None:
                continue
            if interval != interval_seconds:
                missing.append(f"{symbol}: configured production candle interval must be {interval_seconds}s / Min1; detected {interval}s")
                continue
            expected_first_deadline = audit_range.start_ts + interval
            if times[0] > expected_first_deadline:
                missing.append(
                    f"{symbol}: missing candles after {audit_range.start.isoformat()} before {datetime.fromtimestamp(times[0], UTC).isoformat()}"
                )
            gaps = [(left, right) for left, right in zip(times, times[1:]) if right - left > interval * 1.5]
            if gaps:
                left, right = gaps[0]
                missing.append(
                    f"{symbol}: candle gap {datetime.fromtimestamp(left, UTC).isoformat()}..{datetime.fromtimestamp(right, UTC).isoformat()}"
                )
    if missing:
        raise RuntimeError("insufficient local candle data: " + "; ".join(missing))


def _coverage_times(db_paths: Sequence[Path], symbol: str, start_ts: int, end_ts: int) -> set[int]:
    times: set[int] = set()
    for db_path in db_paths:
        if not candles_table_exists(db_path):
            continue
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            scale = _candle_db_timestamp_scale(conn, [symbol])
            storage_start_ts, storage_end_ts = _range_for_storage(start_ts, end_ts, scale)
            rows = conn.execute(
                "SELECT close_time FROM candles WHERE symbol = ? AND close_time > ? AND close_time <= ?",
                (symbol, storage_start_ts, storage_end_ts),
            ).fetchall()
        times.update(_to_epoch_seconds(int(row[0])) for row in rows)
    return times


def missing_candle_ranges(
    db_paths: Sequence[Path],
    symbols: Sequence[str],
    audit_range: AuditRange,
    interval_seconds: int = MEXC_REQUIRED_INTERVAL_SECONDS,
) -> dict[str, list[tuple[int, int]]]:
    end_ts = _closed_audit_end(audit_range.end_ts, interval_seconds)
    if end_ts <= audit_range.start_ts:
        return {symbol: [(audit_range.start_ts + interval_seconds, audit_range.end_ts)] for symbol in symbols}
    missing: dict[str, list[tuple[int, int]]] = {}
    for symbol in symbols:
        expected = list(range(audit_range.start_ts + interval_seconds, end_ts + 1, interval_seconds))
        present = _coverage_times(db_paths, symbol, audit_range.start_ts, end_ts)
        absent = [ts for ts in expected if ts not in present]
        if not absent:
            continue
        ranges: list[tuple[int, int]] = []
        start = prev = absent[0]
        for ts in absent[1:]:
            if ts == prev + interval_seconds:
                prev = ts
                continue
            ranges.append((start, prev))
            start = prev = ts
        ranges.append((start, prev))
        missing[symbol] = ranges
    return missing


def _fetch_mexc_public_candle_page(
    base_url: str,
    symbol: str,
    start_ts: int,
    end_ts: int,
    interval: str,
) -> tuple[list[tuple[int, float, float, float, float]], Any]:
    query = urllib.parse.urlencode({"interval": interval, "start": start_ts, "end": end_ts})
    url = f"{base_url.rstrip('/')}/api/v1/contract/kline/{urllib.parse.quote(mexc_contract_symbol(symbol))}?{query}"
    with urllib.request.urlopen(url, timeout=20) as res:
        payload = json.loads(res.read().decode())
    if isinstance(payload, dict) and payload.get("success") is False:
        raise RuntimeError(f"MEXC kline response for {symbol}: {payload!r}")
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected MEXC kline response for {symbol}: {payload!r}")
    times = data.get("time") or []
    opens = data.get("open") or []
    highs = data.get("high") or []
    lows = data.get("low") or []
    closes = data.get("close") or []
    rows: list[tuple[int, float, float, float, float]] = []
    for ts, open_, high, low, close in zip(times, opens, highs, lows, closes, strict=False):
        close_ts = int(ts)
        if start_ts <= close_ts <= end_ts:
            rows.append((close_ts, float(open_), float(high), float(low), float(close)))
    return rows, payload


def fetch_mexc_public_candles(
    base_url: str,
    symbol: str,
    start_ts: int,
    end_ts: int,
    interval: str = MEXC_INTERVAL_NAMES[MEXC_REQUIRED_INTERVAL_SECONDS],
    interval_seconds: int = MEXC_REQUIRED_INTERVAL_SECONDS,
) -> list[tuple[int, float, float, float, float]]:
    """Fetch every closed MEXC futures kline from public market data only."""
    latest_closed = _closed_audit_end(end_ts, interval_seconds)
    final_end = min(end_ts, latest_closed)
    if final_end < start_ts:
        return []
    rows: dict[int, tuple[int, float, float, float, float]] = {}
    cursor = start_ts
    page_start = start_ts
    page_end = final_end
    pages_fetched = 0
    while cursor <= final_end:
        page_start = cursor
        page_end = min(final_end, cursor + interval_seconds * (MEXC_KLINE_PAGE_LIMIT - 1))
        page_rows, _payload = _fetch_mexc_public_candle_page(base_url, symbol, page_start, page_end, interval)
        pages_fetched += 1
        for row in page_rows:
            if row[0] <= latest_closed:
                rows[row[0]] = row
        cursor = page_end + interval_seconds
    expected = set(range(start_ts, final_end + 1, interval_seconds))
    missing = sorted(expected.difference(rows))
    if missing:
        returned_ts = sorted(rows)
        first_missing_ts = missing[0]
        previous_returned_ts = max((ts for ts in returned_ts if ts < first_missing_ts), default=None)
        next_returned_ts = min((ts for ts in returned_ts if ts > first_missing_ts), default=None)
        details = {
            "symbol": symbol,
            "requested_start_ts": start_ts,
            "requested_end_ts": final_end,
            "first_returned_ts": returned_ts[0] if returned_ts else None,
            "last_returned_ts": returned_ts[-1] if returned_ts else None,
            "expected_candle_count": len(expected),
            "returned_unique_candle_count": len(returned_ts),
            "first_missing_ts": first_missing_ts,
            "previous_returned_ts": previous_returned_ts,
            "next_returned_ts": next_returned_ts,
            "current_page_start_ts": page_start,
            "current_page_end_ts": page_end,
            "pages_fetched": pages_fetched,
        }
        detail_text = ", ".join(f"{key}={value}" for key, value in details.items())
        raise RuntimeError(f"MEXC kline response missing candle: {detail_text}")
    return [rows[ts] for ts in sorted(rows)]


def store_audit_candles(db_path: Path, symbol: str, rows: Iterable[tuple[int, float, float, float, float]], storage_scale: int = 1) -> None:
    init_audit_cache(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO candles(symbol, close_time, open, high, low, close) VALUES (?,?,?,?,?,?)",
            [(normalize_live_symbol(symbol), ts * storage_scale, open_, high, low, close) for ts, open_, high, low, close in rows],
        )


def ensure_audit_candle_coverage(config: live.LiveConfig, audit_range: AuditRange, cache_db: Path = AUDIT_CACHE_DB) -> Path:
    """Verify configured DB first, then fetch only missing closed candles into audit cache."""
    symbols = tuple(normalize_live_symbol(symbol) for symbol in config.allowed_symbols)
    interval = detect_candle_interval(config.candles_db_path, symbols)
    _ = inspect_candle_db(config.candles_db_path, symbols)
    init_audit_cache(cache_db)
    missing = missing_candle_ranges([config.candles_db_path, cache_db], symbols, audit_range, interval.seconds)
    for symbol, ranges in missing.items():
        for start_ts, end_ts in ranges:
            rows = fetch_mexc_public_candles(config.mexc_base_url, symbol, start_ts, end_ts, interval.mexc_name, interval.seconds)
            store_audit_candles(cache_db, symbol, rows, interval.storage_scales.get(symbol, 1))
    remaining = missing_candle_ranges([config.candles_db_path, cache_db], symbols, audit_range, interval.seconds)
    if remaining:
        details = []
        for symbol, ranges in remaining.items():
            for start_ts, end_ts in ranges:
                details.append(f"{symbol}: {datetime.fromtimestamp(start_ts, UTC).isoformat()}..{datetime.fromtimestamp(end_ts, UTC).isoformat()}")
        raise RuntimeError("exchange/local audit candle coverage missing: " + "; ".join(details))
    return cache_db


def _copy_audit_source_candles(source_dbs: Sequence[Path], dest_db: Path, symbols: Iterable[str], close_time: int) -> None:
    with sqlite3.connect(dest_db) as dst:
        dst.execute(
            "CREATE TABLE IF NOT EXISTS candles (symbol TEXT, close_time INTEGER, open REAL, high REAL, low REAL, close REAL, PRIMARY KEY(symbol, close_time))"
        )
        for source_db in source_dbs:
            if not candles_table_exists(source_db):
                continue
            symbol_list = list(symbols)
            placeholders = ",".join("?" for _ in symbol_list)
            with sqlite3.connect(f"file:{source_db}?mode=ro", uri=True) as src:
                scale = _candle_db_timestamp_scale(src, symbol_list)
                storage_close_time = close_time * scale
                rows = src.execute(
                    f"SELECT symbol, close_time, open, high, low, close FROM candles WHERE symbol IN ({placeholders}) AND close_time <= ? ORDER BY close_time ASC",
                    (*symbol_list, storage_close_time),
                ).fetchall()
            dst.executemany(
                "INSERT OR REPLACE INTO candles(symbol, close_time, open, high, low, close) VALUES (?,?,?,?,?,?)",
                rows,
            )


def _copy_candles_through(source_db: Path, dest_db: Path, symbols: Iterable[str], close_time: int) -> None:
    symbol_list = list(symbols)
    placeholders = ",".join("?" for _ in symbol_list)
    with sqlite3.connect(f"file:{source_db}?mode=ro", uri=True) as src, sqlite3.connect(dest_db) as dst:
        ddl = src.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='candles'").fetchone()
        if ddl is None or not ddl[0]:
            raise RuntimeError(f"candles table not found in {source_db}")
        dst.execute(ddl[0])
        scale = _candle_db_timestamp_scale(src, symbol_list)
        rows = src.execute(
            f"SELECT * FROM candles WHERE symbol IN ({placeholders}) AND close_time <= ? ORDER BY close_time ASC",
            (*symbol_list, close_time * scale),
        ).fetchall()
        columns = [info[1] for info in src.execute("PRAGMA table_info(candles)").fetchall()]
        dst.executemany(
            f"INSERT INTO candles ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            rows,
        )


def live_plans_through(config: live.LiveConfig, close_time: int, candle_sources: Sequence[Path] | None = None) -> list[live.TradePlan]:
    """Generate plans with the exact live function against a read-only snapshot."""
    with tempfile.TemporaryDirectory(prefix="mexc_missed_signal_audit_") as tmp:
        tmpdir = Path(tmp)
        snapshot_db = tmpdir / "candles.sqlite3"
        if candle_sources is None:
            _copy_candles_through(config.candles_db_path, snapshot_db, config.allowed_symbols, close_time)
        else:
            _copy_audit_source_candles(candle_sources, snapshot_db, config.allowed_symbols, close_time)
        audit_config = live.LiveConfig(
            live_trading_enabled=False,
            dry_run=True,
            candles_db_path=snapshot_db,
            state_db_path=tmpdir / "audit_state.sqlite3",
            decisions_log_path=tmpdir / "audit_decisions.log",
            orders_log_path=tmpdir / "audit_orders.log",
            trade_plan_csv_path=tmpdir / "audit_trade_plan.csv",
            fixed_risk_usdt=config.fixed_risk_usdt,
            max_open_positions=config.max_open_positions,
            max_daily_loss_usdt=config.max_daily_loss_usdt,
            max_notional_usdt=config.max_notional_usdt,
            entry_order_type=config.entry_order_type,
            mexc_base_url=config.mexc_base_url,
            allowed_symbols=config.allowed_symbols,
            box_sizes=config.box_sizes,
        )
        return live.generate_trade_plans(audit_config, ReadOnlySpecClient())


def classify_not_executed(signal_ts: datetime, events: list[DecisionEvent]) -> tuple[bool, str]:
    nearby = [event for event in events if abs((event.ts - signal_ts).total_seconds()) <= 300]
    if any(event.event == "TRADING_BLOCKED" and "RECONCILE" in str(event.payload.get("reason", "")) for event in nearby):
        return True, "RECONCILE_BLOCKED"
    if nearby:
        return True, "OTHER"
    return False, "BOT_OFFLINE"


def run_audit(
    config: live.LiveConfig,
    output_csv: Path,
    audit_range: AuditRange | None = None,
    output_md: Path | None = None,
) -> list[dict[str, Any]]:
    selected_range = audit_range or resolve_audit_range(config)
    symbols = tuple(normalize_live_symbol(symbol) for symbol in config.allowed_symbols)
    candle_sources: Sequence[Path]
    try:
        validate_local_candle_coverage(config.candles_db_path, symbols, selected_range)
        candle_sources = (config.candles_db_path,)
    except RuntimeError as local_error:
        try:
            cache_db = ensure_audit_candle_coverage(config, selected_range)
        except Exception as fetch_error:
            raise RuntimeError(f"{local_error}; audit cache backfill failed: {fetch_error}") from fetch_error
        candle_sources = (config.candles_db_path, cache_db)
    rows: list[dict[str, Any]] = []
    events = parse_decision_events(config.decisions_log_path)
    seen: set[str] = set()
    candle_times = candle_times_in_gap_from_sources(candle_sources, symbols, selected_range)
    latest_snapshot_ids: set[str] = set()
    for candle_ts in candle_times:
        if len(inspect.signature(live_plans_through).parameters) >= 3:
            plans = live_plans_through(config, candle_ts, candle_sources)
        else:  # Backward-compatible with narrow test doubles.
            plans = live_plans_through(config, candle_ts)  # type: ignore[misc]
        if candle_ts == candle_times[-1]:
            latest_snapshot_ids = {plan.opportunity_id for plan in plans}
        for plan in plans:
            if plan.opportunity_id in seen or not (selected_range.start_ts < plan.observable_entry_ts <= selected_range.end_ts):
                continue
            seen.add(plan.opportunity_id)
            signal_dt = datetime.fromtimestamp(plan.observable_entry_ts, UTC)
            _running, reason = classify_not_executed(signal_dt, events)
            still_valid = plan.opportunity_id in latest_snapshot_ids
            rows.append(SignalRow({
                "signal_time_utc": signal_dt.isoformat(),
                "symbol": plan.symbol,
                "direction": plan.direction,
                "entry": str(plan.entry_price),
                "stop": str(plan.stop_price),
                "target": str(plan.target_price),
                "strategy_score": "N/A",
                "opportunity_id": plan.opportunity_id,
                "reason_not_executed": reason,
                "would_still_be_valid": "yes" if still_valid else "no",
                "notes": "Strategy score is not exposed by the current MEXC pole trade-plan row.",
                "observable_entry_time_utc": signal_dt.isoformat(),
                "bot_running_at_time": str(_running),
                "not_executed_reason": reason,
            }))
    rows.sort(key=lambda row: (row["signal_time_utc"], row["symbol"], row["opportunity_id"]))
    write_audit_csv(output_csv, rows)
    write_audit_markdown(output_md or output_csv.with_suffix(".md"), rows, selected_range)
    return rows


def write_audit_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True) if path.parent != Path(".") else None
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=AUDIT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_audit_markdown(path: Path, rows: list[dict[str, Any]], audit_range: AuditRange) -> None:
    path.parent.mkdir(parents=True, exist_ok=True) if path.parent != Path(".") else None
    lines = [
        "# Missed Signals Audit",
        "",
        f"- Interval UTC: `{audit_range.start.isoformat()}` to `{audit_range.end.isoformat()}`",
        f"- Valid strategy opportunities: `{len(rows)}`",
        "",
        "| Signal UTC | Symbol | Direction | Entry | Stop | Target | Strategy score | Opportunity ID | Reason not executed | Would still be valid? | Notes |",
        "|---|---|---|---:|---:|---:|---:|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(str(row[field]).replace("|", "\\|") for field in AUDIT_FIELDS)
            + " |"
        )
    path.write_text("\n".join(lines) + "\n")


def print_report(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("Missed-signal audit: 0 valid signals found during the selected interval.")
        return
    print(f"Missed-signal audit: {len(rows)} valid signals found during the selected interval. Latest 10:")
    for row in rows[-10:]:
        print(
            f"{row['signal_time_utc']} | {row['symbol']} | {row['direction']} | "
            f"entry={row['entry']} | "
            f"stop={row['stop']} | target={row['target']} | opportunity_id={row['opportunity_id']} | "
            f"reason={row['reason_not_executed']} | still_valid={row['would_still_be_valid']}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only missed-signal audit for mexc_pole_live_trader.py")
    parser.add_argument("--config", type=Path, default=Path("mexc_pole_live_config.example.json"))
    parser.add_argument("--output-csv", type=Path, default=Path("missed_signals.csv"))
    parser.add_argument("--output-md", type=Path, default=Path("missed_signals.md"))
    start_group = parser.add_mutually_exclusive_group()
    start_group.add_argument("--from-utc", type=parse_utc_timestamp, help="Inclusive ISO-8601 UTC audit start timestamp")
    start_group.add_argument("--last-hours", type=int, help="Audit this many hours before --to-utc/latest closed candle")
    start_group.add_argument("--since-last-bot-run", action="store_true", help="Audit from the last normal bot startup in live_decisions.log through now")
    parser.add_argument("--to-utc", type=parse_utc_timestamp, help="Inclusive ISO-8601 UTC audit end timestamp; defaults to latest closed candle")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = live.LiveConfig.from_json(args.config)
    try:
        audit_range = resolve_audit_range(
            config,
            from_utc=args.from_utc,
            to_utc=args.to_utc,
            last_hours=args.last_hours,
            since_last_bot_run=args.since_last_bot_run,
        )
        rows = run_audit(config, args.output_csv, audit_range, args.output_md)
    except (RuntimeError, ValueError) as exc:
        parser.error(str(exc))
    print_report(rows)


if __name__ == "__main__":
    main()
