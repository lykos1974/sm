#!/usr/bin/env python3
"""Read-only missed-signal audit for ``mexc_pole_live_trader.py``.

The audit intentionally delegates signal construction to the live trader module.
It never executes plans and never writes live state, live logs, or the current
trade-plan artifact.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

import mexc_pole_live_trader as live

AUDIT_FIELDS = [
    "signal_time_utc",
    "symbol",
    "direction",
    "observable_entry_time_utc",
    "entry",
    "stop",
    "target",
    "opportunity_id",
    "bot_running_at_time",
    "not_executed_reason",
]


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


class ReadOnlySpecClient:
    """Contract-spec-only client for live plan generation; no order methods."""

    def get_contract_spec(self, venue_symbol: str) -> live.ContractSpec:
        return live.ContractSpec(venue_symbol.split(":", 1)[-1])

    def __getattr__(self, name: str) -> Any:
        if name.startswith(("place_", "cancel_", "modify_", "replace_")):
            raise AssertionError(f"audit must not call exchange order method {name}")
        raise AttributeError(name)


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
    """Return the last successful live-run timestamp recorded before the gap."""
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


def latest_closed_candle_ts(db_path: Path, symbols: Iterable[str]) -> int | None:
    symbol_list = list(symbols)
    if not symbol_list:
        return None
    placeholders = ",".join("?" for _ in symbol_list)
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        row = conn.execute(f"SELECT MAX(close_time) FROM candles WHERE symbol IN ({placeholders})", symbol_list).fetchone()
    return None if row is None or row[0] is None else int(row[0])


def resolve_audit_range(
    config: live.LiveConfig,
    from_utc: datetime | None = None,
    to_utc: datetime | None = None,
    last_hours: int | None = None,
) -> AuditRange:
    if from_utc is not None and last_hours is not None:
        raise ValueError("--from-utc and --last-hours are mutually exclusive")
    latest_ts = latest_closed_candle_ts(config.candles_db_path, config.allowed_symbols)
    if latest_ts is None:
        raise ValueError(f"no local candles found for configured symbols: {', '.join(config.allowed_symbols)}")
    end = to_utc or datetime.fromtimestamp(latest_ts, UTC)
    if last_hours is not None:
        if last_hours <= 0:
            raise ValueError("--last-hours must be a positive integer")
        start = end - timedelta(hours=last_hours)
    elif from_utc is not None:
        start = from_utc
    else:
        start = audit_start_time(config.decisions_log_path)
        if start is None:
            raise ValueError("could not derive audit start: no successful live run found in live_decisions.log; pass --from-utc or --last-hours")
    if start >= end:
        raise ValueError(f"invalid audit range: start {start.isoformat()} must be before end {end.isoformat()}")
    return AuditRange(start, end)


def candle_times_in_gap(db_path: Path, symbols: Iterable[str], audit_range: AuditRange) -> list[int]:
    symbol_list = list(symbols)
    placeholders = ",".join("?" for _ in symbol_list)
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            f"SELECT DISTINCT close_time FROM candles WHERE symbol IN ({placeholders}) AND close_time > ? AND close_time <= ? ORDER BY close_time ASC",
            (*symbol_list, audit_range.start_ts, audit_range.end_ts),
        ).fetchall()
    return [int(row[0]) for row in rows]


def _symbol_candle_times(conn: sqlite3.Connection, symbol: str, start_ts: int, end_ts: int) -> list[int]:
    rows = conn.execute(
        "SELECT close_time FROM candles WHERE symbol = ? AND close_time > ? AND close_time <= ? ORDER BY close_time ASC",
        (symbol, start_ts, end_ts),
    ).fetchall()
    return [int(row[0]) for row in rows]


def _infer_expected_interval(conn: sqlite3.Connection, symbol: str, end_ts: int) -> int | None:
    rows = conn.execute(
        "SELECT close_time FROM candles WHERE symbol = ? AND close_time <= ? ORDER BY close_time DESC LIMIT 20",
        (symbol, end_ts),
    ).fetchall()
    times = sorted(int(row[0]) for row in rows)
    deltas = [right - left for left, right in zip(times, times[1:]) if right > left]
    if not deltas:
        return None
    return min(deltas)


def validate_local_candle_coverage(db_path: Path, symbols: Sequence[str], audit_range: AuditRange) -> None:
    """Fail clearly if local candles do not cover the requested interval."""
    missing: list[str] = []
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        for symbol in symbols:
            bounds = conn.execute("SELECT MIN(close_time), MAX(close_time) FROM candles WHERE symbol = ?", (symbol,)).fetchone()
            if bounds is None or bounds[0] is None or bounds[1] is None:
                missing.append(f"{symbol}: no local candles")
                continue
            min_ts, max_ts = int(bounds[0]), int(bounds[1])
            if min_ts > audit_range.start_ts or max_ts < audit_range.end_ts:
                missing.append(
                    f"{symbol}: local coverage {datetime.fromtimestamp(min_ts, UTC).isoformat()}..{datetime.fromtimestamp(max_ts, UTC).isoformat()} "
                    f"does not cover requested {audit_range.start.isoformat()}..{audit_range.end.isoformat()}"
                )
                continue
            times = _symbol_candle_times(conn, symbol, audit_range.start_ts, audit_range.end_ts)
            if not times:
                missing.append(f"{symbol}: no candles in requested interval {audit_range.start.isoformat()}..{audit_range.end.isoformat()}")
                continue
            interval = _infer_expected_interval(conn, symbol, audit_range.end_ts)
            if interval is None:
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


def _copy_candles_through(source_db: Path, dest_db: Path, symbols: Iterable[str], close_time: int) -> None:
    symbol_list = list(symbols)
    placeholders = ",".join("?" for _ in symbol_list)
    with sqlite3.connect(f"file:{source_db}?mode=ro", uri=True) as src, sqlite3.connect(dest_db) as dst:
        ddl = src.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='candles'").fetchone()
        if ddl is None or not ddl[0]:
            raise RuntimeError(f"candles table not found in {source_db}")
        dst.execute(ddl[0])
        rows = src.execute(
            f"SELECT * FROM candles WHERE symbol IN ({placeholders}) AND close_time <= ? ORDER BY close_time ASC",
            (*symbol_list, close_time),
        ).fetchall()
        columns = [info[1] for info in src.execute("PRAGMA table_info(candles)").fetchall()]
        dst.executemany(
            f"INSERT INTO candles ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            rows,
        )


def live_plans_through(config: live.LiveConfig, close_time: int) -> list[live.TradePlan]:
    """Generate plans with the exact live function against a read-only snapshot."""
    with tempfile.TemporaryDirectory(prefix="mexc_missed_signal_audit_") as tmp:
        tmpdir = Path(tmp)
        snapshot_db = tmpdir / "candles.sqlite3"
        _copy_candles_through(config.candles_db_path, snapshot_db, config.allowed_symbols, close_time)
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


def run_audit(config: live.LiveConfig, output_csv: Path, audit_range: AuditRange | None = None) -> list[dict[str, Any]]:
    selected_range = audit_range or resolve_audit_range(config)
    validate_local_candle_coverage(config.candles_db_path, config.allowed_symbols, selected_range)
    rows: list[dict[str, Any]] = []
    events = parse_decision_events(config.decisions_log_path)
    seen: set[str] = set()
    for candle_ts in candle_times_in_gap(config.candles_db_path, config.allowed_symbols, selected_range):
        for plan in live_plans_through(config, candle_ts):
            if plan.opportunity_id in seen or not (selected_range.start_ts < plan.observable_entry_ts <= selected_range.end_ts):
                continue
            seen.add(plan.opportunity_id)
            signal_dt = datetime.fromtimestamp(plan.observable_entry_ts, UTC)
            running, reason = classify_not_executed(signal_dt, events)
            rows.append({
                "signal_time_utc": signal_dt.isoformat(),
                "symbol": plan.symbol,
                "direction": plan.direction,
                "observable_entry_time_utc": signal_dt.isoformat(),
                "entry": str(plan.entry_price),
                "stop": str(plan.stop_price),
                "target": str(plan.target_price),
                "opportunity_id": plan.opportunity_id,
                "bot_running_at_time": str(running),
                "not_executed_reason": reason,
            })
    rows.sort(key=lambda row: (row["signal_time_utc"], row["symbol"], row["opportunity_id"]))
    write_audit_csv(output_csv, rows)
    return rows


def write_audit_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True) if path.parent != Path(".") else None
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=AUDIT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def print_report(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("Missed-signal audit: 0 valid signals found during the selected interval.")
        return
    print(f"Missed-signal audit: {len(rows)} valid signals found during the selected interval. Latest 10:")
    for row in rows[-10:]:
        print(
            f"{row['signal_time_utc']} | {row['symbol']} | {row['direction']} | "
            f"observable_entry={row['observable_entry_time_utc']} | entry={row['entry']} | "
            f"stop={row['stop']} | target={row['target']} | opportunity_id={row['opportunity_id']} | "
            f"bot_running={row['bot_running_at_time']} | reason={row['not_executed_reason']}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only missed-signal audit for mexc_pole_live_trader.py")
    parser.add_argument("--config", type=Path, default=Path("mexc_pole_live_config.example.json"))
    parser.add_argument("--output-csv", type=Path, default=Path("mexc_missed_signal_audit.csv"))
    start_group = parser.add_mutually_exclusive_group()
    start_group.add_argument("--from-utc", type=parse_utc_timestamp, help="Inclusive ISO-8601 UTC audit start timestamp")
    start_group.add_argument("--last-hours", type=int, help="Audit this many hours before --to-utc/latest closed candle")
    parser.add_argument("--to-utc", type=parse_utc_timestamp, help="Inclusive ISO-8601 UTC audit end timestamp; defaults to latest closed candle")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = live.LiveConfig.from_json(args.config)
    try:
        audit_range = resolve_audit_range(config, from_utc=args.from_utc, to_utc=args.to_utc, last_hours=args.last_hours)
        rows = run_audit(config, args.output_csv, audit_range)
    except (RuntimeError, ValueError) as exc:
        parser.error(str(exc))
    print_report(rows)


if __name__ == "__main__":
    main()
