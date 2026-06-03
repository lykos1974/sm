#!/usr/bin/env python3
"""Shadow-only pole forward validator.

Phase 1 forward validation harness for the fixed pole execution hypothesis.
This module is intentionally isolated from live traders: it reads historical or
collector candle data, writes only to a dedicated shadow SQLite database, and
never imports exchange clients, reads API credentials, or submits orders.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parent
PNF_MVP = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP) not in sys.path:
    sys.path.insert(0, str(PNF_MVP))

from pnf_engine import PnFEngine, PnFProfile  # noqa: E402
from patterns.poles import detect_pole_patterns  # noqa: E402

DEFAULT_SHADOW_DB = REPO_ROOT / "data" / "research" / "pole_shadow_forward.sqlite"
DEFAULT_PROFILE_NAME = "POLE_SHADOW_bs1_rev3"
DEFAULT_INTERVAL = "1m"
ENTRY_MODEL = "NEXT_COLUMN_OPEN_ENTRY"
STOP_BOXES = 3.0
TARGET_R = 2.5
BREAK_EVEN_TRIGGER_R = 2.0
OPENISH_STATES = {"PENDING_ENTRY", "OPEN"}
TERMINAL_STATES = {"TARGET_FIRST", "STOP_FIRST", "BREAK_EVEN_EXIT"}
VALID_STATES = {*OPENISH_STATES, *TERMINAL_STATES}


@dataclass(frozen=True)
class Candle:
    close_time: int
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class ShadowSetup:
    symbol: str
    profile_name: str
    pattern_name: str
    direction: str
    setup_key: str
    pole_column_index: int
    reversal_column_index: int
    confirmation_column_index: int
    signal_ts: int
    entry_after_ts: int
    box_size: float


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_candle_db_readonly(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def connect_shadow_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_shadow_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS pole_shadow_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            setup_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            profile_name TEXT NOT NULL,
            pattern_name TEXT NOT NULL,
            direction TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('PENDING_ENTRY','OPEN','TARGET_FIRST','STOP_FIRST','BREAK_EVEN_EXIT')),
            entry_model TEXT NOT NULL,
            pole_column_index INTEGER NOT NULL,
            reversal_column_index INTEGER NOT NULL,
            confirmation_column_index INTEGER NOT NULL,
            signal_ts INTEGER NOT NULL,
            entry_after_ts INTEGER NOT NULL,
            entry_ts INTEGER,
            exit_ts INTEGER,
            box_size REAL NOT NULL,
            stop_boxes REAL NOT NULL,
            target_r REAL NOT NULL,
            break_even_trigger_r REAL NOT NULL,
            entry_price REAL,
            initial_stop_price REAL,
            active_stop_price REAL,
            target_price REAL,
            break_even_trigger_price REAL,
            break_even_armed INTEGER NOT NULL DEFAULT 0,
            realized_r REAL,
            last_processed_close_time INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_pole_shadow_one_active_per_symbol
        ON pole_shadow_trades(symbol)
        WHERE state IN ('PENDING_ENTRY','OPEN');

        CREATE TABLE IF NOT EXISTS pole_shadow_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER,
            setup_key TEXT,
            symbol TEXT NOT NULL,
            event_type TEXT NOT NULL,
            from_state TEXT,
            to_state TEXT,
            candle_close_time INTEGER,
            price REAL,
            details_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(trade_id) REFERENCES pole_shadow_trades(id)
        );
        """
    )
    conn.commit()


def record_event(
    conn: sqlite3.Connection,
    *,
    trade_id: int | None,
    setup_key: str | None,
    symbol: str,
    event_type: str,
    from_state: str | None = None,
    to_state: str | None = None,
    candle_close_time: int | None = None,
    price: float | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO pole_shadow_events(
          trade_id, setup_key, symbol, event_type, from_state, to_state,
          candle_close_time, price, details_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            trade_id,
            setup_key,
            symbol,
            event_type,
            from_state,
            to_state,
            candle_close_time,
            price,
            json.dumps(details or {}, sort_keys=True),
            utc_now(),
        ),
    )


def load_candles(conn: sqlite3.Connection, symbol: str, *, interval: str, limit: int) -> list[Candle]:
    rows = conn.execute(
        """
        SELECT close_time, open, high, low, close
        FROM candles
        WHERE symbol = ? AND interval = ?
        ORDER BY close_time DESC
        LIMIT ?
        """,
        (symbol, interval, int(limit)),
    ).fetchall()
    return [Candle(int(r["close_time"]), float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])) for r in reversed(rows)]


def replay_engine(profile: PnFProfile, candles: Iterable[Candle]) -> PnFEngine:
    engine = PnFEngine(profile)
    for candle in candles:
        engine.update_from_price(candle.close_time, candle.close)
    return engine


def direction_for_pattern(pattern_name: str) -> str:
    if pattern_name == "LOW_POLE":
        return "LONG"
    if pattern_name == "HIGH_POLE":
        return "SHORT"
    raise ValueError(f"unsupported pole pattern: {pattern_name}")


def setup_key(symbol: str, profile_name: str, pattern_name: str, pole_idx: int, reversal_idx: int, confirmation_idx: int) -> str:
    return f"{symbol}|{profile_name}|{pattern_name}|{pole_idx}|{reversal_idx}|{confirmation_idx}|{ENTRY_MODEL}|SL{STOP_BOXES:g}|T{TARGET_R:g}|BE{BREAK_EVEN_TRIGGER_R:g}"


def discover_latest_shadow_setup(symbol: str, profile: PnFProfile, candles: list[Candle]) -> ShadowSetup | None:
    engine = replay_engine(profile, candles)
    if len(engine.columns) < 3:
        return None
    patterns = detect_pole_patterns(engine.columns, box_size=profile.box_size)
    core = [
        pattern
        for pattern in patterns
        if pattern.get("opposing_pole_distance_columns") == 3
        and pattern.get("enhanced_by_opposing_pole") is False
        and pattern.get("reversal_column_index") is not None
    ]
    if not core:
        return None

    by_idx = {int(col.idx): col for col in engine.columns}
    latest: ShadowSetup | None = None
    for pattern in core:
        pattern_name = str(pattern["pattern_name"]).upper()
        pole_idx = int(pattern["pole_column_index"])
        reversal_idx = int(pattern["reversal_column_index"])
        confirmation_idx = reversal_idx + 1
        confirmation = by_idx.get(confirmation_idx)
        reversal = by_idx.get(reversal_idx)
        if confirmation is None or reversal is None or getattr(confirmation, "start_ts", None) is None:
            continue
        signal_ts = int(getattr(reversal, "end_ts", None) or getattr(confirmation, "start_ts"))
        candidate = ShadowSetup(
            symbol=symbol,
            profile_name=profile.name,
            pattern_name=pattern_name,
            direction=direction_for_pattern(pattern_name),
            setup_key=setup_key(symbol, profile.name, pattern_name, pole_idx, reversal_idx, confirmation_idx),
            pole_column_index=pole_idx,
            reversal_column_index=reversal_idx,
            confirmation_column_index=confirmation_idx,
            signal_ts=signal_ts,
            entry_after_ts=int(getattr(confirmation, "start_ts")),
            box_size=float(profile.box_size),
        )
        if latest is None or candidate.reversal_column_index > latest.reversal_column_index:
            latest = candidate
    return latest


def has_active_symbol_trade(conn: sqlite3.Connection, symbol: str) -> bool:
    row = conn.execute("SELECT 1 FROM pole_shadow_trades WHERE symbol = ? AND state IN ('PENDING_ENTRY','OPEN') LIMIT 1", (symbol,)).fetchone()
    return row is not None


def insert_pending_setup(conn: sqlite3.Connection, setup: ShadowSetup) -> bool:
    if has_active_symbol_trade(conn, setup.symbol):
        record_event(
            conn,
            trade_id=None,
            setup_key=setup.setup_key,
            symbol=setup.symbol,
            event_type="DUPLICATE_ACTIVE_SYMBOL_BLOCKED",
            details={"state_machine": "one shadow position per symbol"},
        )
        return False
    now = utc_now()
    try:
        cur = conn.execute(
            """
            INSERT INTO pole_shadow_trades(
              setup_key, symbol, profile_name, pattern_name, direction, state, entry_model,
              pole_column_index, reversal_column_index, confirmation_column_index,
              signal_ts, entry_after_ts, box_size, stop_boxes, target_r,
              break_even_trigger_r, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                setup.setup_key,
                setup.symbol,
                setup.profile_name,
                setup.pattern_name,
                setup.direction,
                "PENDING_ENTRY",
                ENTRY_MODEL,
                setup.pole_column_index,
                setup.reversal_column_index,
                setup.confirmation_column_index,
                setup.signal_ts,
                setup.entry_after_ts,
                setup.box_size,
                STOP_BOXES,
                TARGET_R,
                BREAK_EVEN_TRIGGER_R,
                now,
                now,
            ),
        )
    except sqlite3.IntegrityError:
        record_event(
            conn,
            trade_id=None,
            setup_key=setup.setup_key,
            symbol=setup.symbol,
            event_type="DUPLICATE_SETUP_BLOCKED",
            details={"setup_key": setup.setup_key},
        )
        return False
    trade_id = int(cur.lastrowid)
    record_event(
        conn,
        trade_id=trade_id,
        setup_key=setup.setup_key,
        symbol=setup.symbol,
        event_type="SETUP_REGISTERED",
        to_state="PENDING_ENTRY",
        candle_close_time=setup.signal_ts,
        details={"entry_model": ENTRY_MODEL, "stop_boxes": STOP_BOXES, "target_r": TARGET_R, "break_even_after_r": BREAK_EVEN_TRIGGER_R},
    )
    return True


def price_levels(direction: str, entry_price: float, box_size: float) -> tuple[float, float, float]:
    risk = STOP_BOXES * box_size
    if direction == "LONG":
        return entry_price - risk, entry_price + (risk * TARGET_R), entry_price + (risk * BREAK_EVEN_TRIGGER_R)
    return entry_price + risk, entry_price - (risk * TARGET_R), entry_price - (risk * BREAK_EVEN_TRIGGER_R)


def open_pending_trades(conn: sqlite3.Connection, candles_by_symbol: dict[str, list[Candle]]) -> None:
    pending = conn.execute("SELECT * FROM pole_shadow_trades WHERE state = 'PENDING_ENTRY' ORDER BY id ASC").fetchall()
    for trade in pending:
        candles = candles_by_symbol.get(str(trade["symbol"]), [])
        entry_candle = next((c for c in candles if c.close_time > int(trade["entry_after_ts"])), None)
        if entry_candle is None:
            continue
        entry = float(entry_candle.open)
        stop, target, be_trigger = price_levels(str(trade["direction"]), entry, float(trade["box_size"]))
        conn.execute(
            """
            UPDATE pole_shadow_trades
            SET state = 'OPEN', entry_ts = ?, entry_price = ?, initial_stop_price = ?,
                active_stop_price = ?, target_price = ?, break_even_trigger_price = ?,
                last_processed_close_time = NULL, updated_at = ?
            WHERE id = ? AND state = 'PENDING_ENTRY'
            """,
            (entry_candle.close_time, entry, stop, stop, target, be_trigger, utc_now(), int(trade["id"])),
        )
        record_event(
            conn,
            trade_id=int(trade["id"]),
            setup_key=str(trade["setup_key"]),
            symbol=str(trade["symbol"]),
            event_type="ENTRY_FILLED_SHADOW",
            from_state="PENDING_ENTRY",
            to_state="OPEN",
            candle_close_time=entry_candle.close_time,
            price=entry,
            details={"execution": "shadow only", "entry_model": ENTRY_MODEL},
        )


def hit_stop(candle: Candle, price: float, direction: str) -> bool:
    return candle.low <= price if direction == "LONG" else candle.high >= price


def hit_target(candle: Candle, price: float, direction: str) -> bool:
    return candle.high >= price if direction == "LONG" else candle.low <= price


def hit_be_trigger(candle: Candle, price: float, direction: str) -> bool:
    return candle.high >= price if direction == "LONG" else candle.low <= price


def terminal_update(conn: sqlite3.Connection, trade: sqlite3.Row, candle: Candle, state: str, price: float, realized_r: float, details: dict[str, Any]) -> None:
    conn.execute(
        """
        UPDATE pole_shadow_trades
        SET state = ?, exit_ts = ?, active_stop_price = ?, realized_r = ?,
            last_processed_close_time = ?, updated_at = ?
        WHERE id = ? AND state = 'OPEN'
        """,
        (state, candle.close_time, price, realized_r, candle.close_time, utc_now(), int(trade["id"])),
    )
    record_event(
        conn,
        trade_id=int(trade["id"]),
        setup_key=str(trade["setup_key"]),
        symbol=str(trade["symbol"]),
        event_type=state,
        from_state="OPEN",
        to_state=state,
        candle_close_time=candle.close_time,
        price=price,
        details=details,
    )


def update_open_trades(conn: sqlite3.Connection, candles_by_symbol: dict[str, list[Candle]]) -> None:
    open_trades = conn.execute("SELECT * FROM pole_shadow_trades WHERE state = 'OPEN' ORDER BY id ASC").fetchall()
    for trade in open_trades:
        direction = str(trade["direction"])
        entry_ts = int(trade["entry_ts"])
        last_processed = trade["last_processed_close_time"]
        start_after = int(last_processed) if last_processed is not None else entry_ts - 1
        candles = [c for c in candles_by_symbol.get(str(trade["symbol"]), []) if c.close_time > start_after and c.close_time >= entry_ts]
        if not candles:
            continue
        armed = bool(trade["break_even_armed"])
        entry_price = float(trade["entry_price"])
        initial_stop = float(trade["initial_stop_price"])
        active_stop = float(trade["active_stop_price"])
        target = float(trade["target_price"])
        be_trigger = float(trade["break_even_trigger_price"])
        risk = abs(entry_price - initial_stop)

        for candle in candles:
            if armed:
                target_hit = hit_target(candle, target, direction)
                stop_hit = hit_stop(candle, entry_price, direction)
                if target_hit and stop_hit:
                    terminal_update(conn, trade, candle, "BREAK_EVEN_EXIT", entry_price, 0.0, {"same_candle_policy": "conservative BE stop before target"})
                    break
                if target_hit:
                    terminal_update(conn, trade, candle, "TARGET_FIRST", target, TARGET_R, {"target_r": TARGET_R})
                    break
                if stop_hit:
                    terminal_update(conn, trade, candle, "BREAK_EVEN_EXIT", entry_price, 0.0, {"break_even_armed": True})
                    break
            else:
                target_hit = hit_target(candle, target, direction)
                stop_hit = hit_stop(candle, initial_stop, direction)
                trigger_hit = hit_be_trigger(candle, be_trigger, direction)
                if target_hit and stop_hit:
                    terminal_update(conn, trade, candle, "STOP_FIRST", initial_stop, -1.0, {"same_candle_policy": "conservative initial stop before target"})
                    break
                if target_hit:
                    terminal_update(conn, trade, candle, "TARGET_FIRST", target, TARGET_R, {"target_r": TARGET_R})
                    break
                if trigger_hit and stop_hit:
                    terminal_update(conn, trade, candle, "STOP_FIRST", initial_stop, -1.0, {"same_candle_policy": "conservative initial stop before BE trigger"})
                    break
                if stop_hit:
                    terminal_update(conn, trade, candle, "STOP_FIRST", initial_stop, -1.0, {"initial_stop": True})
                    break
                if trigger_hit:
                    armed = True
                    active_stop = entry_price
                    conn.execute(
                        """
                        UPDATE pole_shadow_trades
                        SET break_even_armed = 1, active_stop_price = ?,
                            last_processed_close_time = ?, updated_at = ?
                        WHERE id = ? AND state = 'OPEN'
                        """,
                        (entry_price, candle.close_time, utc_now(), int(trade["id"])),
                    )
                    record_event(
                        conn,
                        trade_id=int(trade["id"]),
                        setup_key=str(trade["setup_key"]),
                        symbol=str(trade["symbol"]),
                        event_type="BREAK_EVEN_ARMED",
                        from_state="OPEN",
                        to_state="OPEN",
                        candle_close_time=candle.close_time,
                        price=entry_price,
                        details={"break_even_trigger_r": BREAK_EVEN_TRIGGER_R},
                    )
                    if hit_stop(candle, entry_price, direction):
                        terminal_update(conn, trade, candle, "BREAK_EVEN_EXIT", entry_price, 0.0, {"same_candle_policy": "conservative BE exit after trigger"})
                        break
                    continue
            conn.execute(
                "UPDATE pole_shadow_trades SET last_processed_close_time = ?, updated_at = ? WHERE id = ? AND state = 'OPEN'",
                (candle.close_time, utc_now(), int(trade["id"])),
            )
        else:
            # Keep the final active stop persisted when BE has been armed without exit.
            if armed and active_stop == entry_price:
                conn.execute(
                    "UPDATE pole_shadow_trades SET break_even_armed = 1, active_stop_price = ?, updated_at = ? WHERE id = ? AND state = 'OPEN'",
                    (entry_price, utc_now(), int(trade["id"])),
                )


def process_once(args: argparse.Namespace) -> dict[str, Any]:
    profile = PnFProfile(name=args.profile_name, box_size=float(args.box_size), reversal_boxes=int(args.reversal_boxes))
    with closing(connect_candle_db_readonly(Path(args.candle_db))) as candle_conn, closing(connect_shadow_db(Path(args.shadow_db))) as shadow_conn:
        init_shadow_tables(shadow_conn)
        candles_by_symbol = {
            symbol: load_candles(candle_conn, symbol, interval=args.interval, limit=int(args.candle_limit))
            for symbol in args.symbol
        }
        for symbol, candles in candles_by_symbol.items():
            setup = discover_latest_shadow_setup(symbol, profile, candles)
            if setup is not None:
                insert_pending_setup(shadow_conn, setup)
        open_pending_trades(shadow_conn, candles_by_symbol)
        update_open_trades(shadow_conn, candles_by_symbol)
        shadow_conn.commit()
        return daily_summary(shadow_conn)


def daily_summary(conn: sqlite3.Connection, day: str | None = None) -> dict[str, Any]:
    day = day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    states = {row["state"]: int(row["count"]) for row in conn.execute("SELECT state, COUNT(*) AS count FROM pole_shadow_trades GROUP BY state")}
    event_rows = conn.execute(
        """
        SELECT event_type, COUNT(*) AS count
        FROM pole_shadow_events
        WHERE substr(created_at, 1, 10) = ?
        GROUP BY event_type
        ORDER BY event_type
        """,
        (day,),
    ).fetchall()
    terminal = conn.execute(
        "SELECT COUNT(*) AS n, COALESCE(SUM(realized_r), 0.0) AS total_r, COALESCE(AVG(realized_r), 0.0) AS avg_r FROM pole_shadow_trades WHERE state IN ('TARGET_FIRST','STOP_FIRST','BREAK_EVEN_EXIT')"
    ).fetchone()
    return {
        "day": day,
        "mode": "SHADOW_ONLY_NO_EXCHANGE_NO_DEMO_NO_API_KEYS",
        "entry_model": ENTRY_MODEL,
        "risk_model": {"stop_boxes": STOP_BOXES, "target_r": TARGET_R, "break_even_after_r": BREAK_EVEN_TRIGGER_R},
        "states": states,
        "events_today": {row["event_type"]: int(row["count"]) for row in event_rows},
        "terminal_trades": int(terminal["n"]),
        "total_realized_r": round(float(terminal["total_r"]), 6),
        "avg_realized_r": round(float(terminal["avg_r"]), 6),
    }


def write_daily_summary(conn: sqlite3.Connection, output_path: Path, day: str | None = None) -> dict[str, Any]:
    summary = daily_summary(conn, day)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as handle:
        handle.write("# Pole Shadow Forward Daily Summary\n\n")
        handle.write("Shadow-only validation. No exchange orders, demo orders, API keys, production strategy paths, or live trader integrations are used.\n\n")
        handle.write(f"- day: {summary['day']}\n")
        handle.write(f"- mode: {summary['mode']}\n")
        handle.write(f"- entry: `{ENTRY_MODEL}`\n")
        handle.write(f"- stop: fixed {STOP_BOXES:g}-box stop\n")
        handle.write(f"- target: fixed {TARGET_R:g}R target\n")
        handle.write(f"- break-even: after +{BREAK_EVEN_TRIGGER_R:g}R\n")
        handle.write(f"- terminal trades: {summary['terminal_trades']}\n")
        handle.write(f"- total realized R: {summary['total_realized_r']}\n")
        handle.write(f"- average realized R: {summary['avg_realized_r']}\n\n")
        handle.write("## States\n\n")
        for state in ("PENDING_ENTRY", "OPEN", "TARGET_FIRST", "STOP_FIRST", "BREAK_EVEN_EXIT"):
            handle.write(f"- {state}: {summary['states'].get(state, 0)}\n")
        handle.write("\n## Events today\n\n")
        if summary["events_today"]:
            for event_type, count in summary["events_today"].items():
                handle.write(f"- {event_type}: {count}\n")
        else:
            handle.write("- none\n")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase 1 shadow-only pole forward validator")
    parser.add_argument("--candle-db", required=True, help="Read-only candle SQLite database path")
    parser.add_argument("--shadow-db", default=str(DEFAULT_SHADOW_DB), help="Dedicated shadow SQLite database path")
    parser.add_argument("--symbol", action="append", required=True, help="Candle symbol to shadow-validate; may be repeated")
    parser.add_argument("--interval", default=DEFAULT_INTERVAL)
    parser.add_argument("--profile-name", default=DEFAULT_PROFILE_NAME)
    parser.add_argument("--box-size", type=float, default=1.0)
    parser.add_argument("--reversal-boxes", type=int, default=3)
    parser.add_argument("--candle-limit", type=int, default=5000)
    parser.add_argument("--daily-summary-output", type=Path, default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    summary = process_once(args)
    if args.daily_summary_output is not None:
        with closing(connect_shadow_db(Path(args.shadow_db))) as conn:
            init_shadow_tables(conn)
            summary = write_daily_summary(conn, args.daily_summary_output)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
