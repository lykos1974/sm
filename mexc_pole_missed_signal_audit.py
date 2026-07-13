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
    return None if row is None or row[0] is None else int(row[0])


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
    latest_ts = latest_closed_candle_ts(config.candles_db_path, config.allowed_symbols)
    if latest_ts is None:
        raise ValueError(f"no local candles found for configured symbols: {', '.join(config.allowed_symbols)}")
    end = to_utc or (datetime.now(UTC) if since_last_bot_run else datetime.fromtimestamp(latest_ts, UTC))
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


def run_audit(
    config: live.LiveConfig,
    output_csv: Path,
    audit_range: AuditRange | None = None,
    output_md: Path | None = None,
) -> list[dict[str, Any]]:
    selected_range = audit_range or resolve_audit_range(config)
    validate_local_candle_coverage(config.candles_db_path, config.allowed_symbols, selected_range)
    rows: list[dict[str, Any]] = []
    events = parse_decision_events(config.decisions_log_path)
    seen: set[str] = set()
    candle_times = candle_times_in_gap(config.candles_db_path, config.allowed_symbols, selected_range)
    latest_snapshot_ids: set[str] = set()
    for candle_ts in candle_times:
        plans = live_plans_through(config, candle_ts)
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
