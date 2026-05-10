#!/usr/bin/env python3
"""Binance USD-M futures live micro-trading forward validator.

Standalone guarded forward validator for strict close-confirmed PnF triangle
signals.  It deliberately does not modify collectors, strategy logic, or the
MEXC validator.  Live exchange writes are fail-closed and require
``LIVE_TRADING_ENABLED=1`` plus Binance USD-M futures API credentials.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent
PNF_MVP = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP) not in sys.path:
    sys.path.insert(0, str(PNF_MVP))

from pnf_engine import PnFEngine, PnFProfile  # noqa: E402

BINANCE_BASE_URL = "https://fapi.binance.com"
BINANCE_DEMO_BASE_URL = "https://demo-fapi.binance.com"
BINANCE_API_KEY_ENV = "BINANCE_FUTURES_API_KEY"
BINANCE_API_SECRET_ENV = "BINANCE_FUTURES_API_SECRET"
BINANCE_DEMO_API_KEY_ENV = "BINANCE_DEMO_FUTURES_API_KEY"
BINANCE_DEMO_API_SECRET_ENV = "BINANCE_DEMO_FUTURES_API_SECRET"
MAX_NOTIONAL_USDT = Decimal("1")
DEFAULT_NOTIONAL_USDT = Decimal("1")
RECV_WINDOW_MS = 5000

ALLOWED_SYMBOLS = {"BINANCE_FUT:BTCUSDT", "BINANCE_FUT:ETHUSDT", "BINANCE_FUT:SOLUSDT"}
ALLOWED_PATTERNS = {"bullish_triangle", "bearish_triangle"}
DEMO_DOUBLE_PATTERNS = {"double_top_breakout", "double_bottom_breakdown"}
CATAPULT_SIGNAL_NAMES = {"bullish_catapult", "bearish_catapult"}
OPEN_TRADE_STATUSES = {"OPEN", "ORDER_SENT", "POSITION_OPEN", "EXIT_PENDING"}


@dataclass(frozen=True)
class Candle:
    close_time: int
    close: float
    high: float
    low: float


@dataclass(frozen=True)
class TriangleSignal:
    symbol: str
    pattern: str
    side: str
    trigger_ts: int
    entry_price: Decimal
    stop_price: Decimal
    tp1_price: Decimal
    tp2_price: Decimal
    trigger_column_idx: int
    support_level: Decimal
    resistance_level: Decimal
    break_distance_boxes: Decimal
    pattern_quality: str


@dataclass(frozen=True)
class SymbolSpec:
    symbol: str
    status: str
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal
    price_precision: int
    quantity_precision: int


class BinanceFuturesClient:
    """Small official REST client for Binance USD-M futures endpoints."""

    def __init__(self, api_key: str | None, api_secret: str | None, *, base_url: str = BINANCE_BASE_URL) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _signed_params(self, params: dict[str, Any] | None = None, *, timestamp: int | None = None) -> dict[str, Any]:
        if not self.has_credentials:
            raise RuntimeError("missing Binance USD-M futures API credentials")
        signed: dict[str, Any] = {k: v for k, v in (params or {}).items() if v is not None}
        signed.setdefault("recvWindow", RECV_WINDOW_MS)
        signed["timestamp"] = int(timestamp if timestamp is not None else time.time() * 1000)
        query = urllib.parse.urlencode(signed, doseq=True)
        signed["signature"] = hmac.new(str(self.api_secret).encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        return signed

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> Any:
        method = method.upper()
        request_params = self._signed_params(params) if signed else {k: v for k, v in (params or {}).items() if v is not None}
        query = urllib.parse.urlencode(request_params, doseq=True)
        url = f"{self.base_url}{path}"
        body_bytes: bytes | None = None
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if signed:
            headers["X-MBX-APIKEY"] = str(self.api_key)
        if method in {"GET", "DELETE"} and query:
            url = f"{url}?{query}"
        elif method == "POST":
            body_bytes = query.encode("utf-8")

        request = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Binance HTTP {exc.code}: {raw}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Binance request failed: {exc}") from exc
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Binance non-JSON response: {raw[:500]}") from exc
        return data

    def get_exchange_info(self) -> dict[str, Any]:
        return self._request_json("GET", "/fapi/v1/exchangeInfo")

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        return parse_symbol_spec(self.get_exchange_info(), symbol)

    def get_position_mode(self) -> dict[str, Any]:
        return self._request_json("GET", "/fapi/v1/positionSide/dual", signed=True)

    def get_position_risk(self, symbol: str) -> Any:
        return self._request_json("GET", "/fapi/v3/positionRisk", params={"symbol": symbol}, signed=True)

    def submit_order(self, order: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("POST", "/fapi/v1/order", params=order, signed=True)

    def get_order(self, symbol: str, *, order_id: str | None = None, client_order_id: str | None = None) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/fapi/v1/order",
            params={"symbol": symbol, "orderId": order_id, "origClientOrderId": client_order_id},
            signed=True,
        )

    def get_user_trades(self, symbol: str, *, order_id: str | None = None) -> Any:
        return self._request_json("GET", "/fapi/v1/userTrades", params={"symbol": symbol, "orderId": order_id}, signed=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc


def console(event: str, message: str, details: dict[str, Any] | None = None) -> None:
    suffix = f" {json.dumps(details, sort_keys=True, default=str)}" if details else ""
    message_part = f" {message}" if message else ""
    print(f"{now_iso()} {event}{message_part}{suffix}", flush=True)


def db_info_payload(db_path: str | os.PathLike[str], conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    absolute_path = Path(db_path).expanduser().absolute()
    exists = absolute_path.exists()
    payload: dict[str, Any] = {
        "db_path": str(absolute_path),
        "exists": exists,
        "mtime": datetime.fromtimestamp(absolute_path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds") if exists else None,
        "resolved_path": str(absolute_path.resolve(strict=False)),
    }
    if conn is not None:
        database_rows = conn.execute("PRAGMA database_list").fetchall()
        main_file = next((row[2] for row in database_rows if row[1] == "main"), None)
        payload["sqlite_database_list"] = [
            {"seq": row[0], "name": row[1], "file": row[2], "realpath": os.path.realpath(row[2]) if row[2] else None}
            for row in database_rows
        ]
        payload["sqlite_realpath"] = os.path.realpath(main_file) if main_file else None
    else:
        payload["sqlite_realpath"] = os.path.realpath(absolute_path)
    return payload


def log_db_info(db_path: str | os.PathLike[str], conn: sqlite3.Connection | None = None) -> None:
    console("DB_INFO", "", db_info_payload(db_path, conn))


def compact_visibility_payload(data: dict[str, Any]) -> dict[str, Any]:
    return {key: (float(value) if isinstance(value, Decimal) else value) for key, value in data.items()}


def log_market_snapshot(symbol: str, profile: PnFProfile, candles: list[Candle]) -> None:
    engine, _latest_ts = _replay_close_confirmed_pnf(profile, candles)
    last_candle = candles[-1] if candles else None
    console(
        "MARKET",
        symbol,
        compact_visibility_payload(
            {
                "last_close": last_candle.close if last_candle is not None else None,
                "last_candle_time": last_candle.close_time if last_candle is not None else None,
                "pnf_last_price": engine.last_price,
                "trend": engine.market_state(),
                "latest_signal": engine.latest_signal_name(),
            }
        ),
    )


def log_signal_detail(signal: TriangleSignal, profile: PnFProfile, last_close: float | None) -> None:
    console(
        "SIGNAL_DETAIL",
        signal.symbol,
        compact_visibility_payload(
            {
                "pattern": signal.pattern,
                "side": signal.side,
                "entry": signal.entry_price,
                "stop": signal.stop_price,
                "tp1": signal.tp1_price,
                "tp2": signal.tp2_price,
                "breakout_level": signal.entry_price,
                "support": signal.support_level,
                "resistance": signal.resistance_level,
                "box_size": _dec(profile.box_size),
                "reversal": int(profile.reversal_boxes),
                "trigger_column": signal.trigger_column_idx,
                "last_close": last_close,
                "trigger_timestamp": signal.trigger_ts,
            }
        ),
    )


def log_order_detail(symbol: str, order: dict[str, Any]) -> None:
    price = _dec(order["price"]) if order.get("price") not in (None, "") else None
    quantity = _dec(order["quantity"]) if order.get("quantity") not in (None, "") else None
    notional = price * quantity if price is not None and quantity is not None else None
    reduce_only_raw = order.get("reduceOnly", False)
    reduce_only = str(reduce_only_raw).lower() == "true" if isinstance(reduce_only_raw, str) else bool(reduce_only_raw)
    console(
        "ORDER_DETAIL",
        symbol,
        compact_visibility_payload(
            {
                "type": order.get("type"),
                "side": order.get("side"),
                "price": price,
                "qty": quantity,
                "notional": notional,
                "reduce_only": reduce_only,
            }
        ),
    )


def log_position_open_detail(signal: TriangleSignal, lifecycle: dict[str, Any]) -> None:
    console(
        "POSITION_OPEN_DETAIL",
        "",
        {
            "symbol": signal.symbol,
            "avg_fill_price": lifecycle.get("avg_fill_price"),
            "requested_entry": float(signal.entry_price),
            "slippage": lifecycle.get("entry_slippage"),
            "executed_qty": lifecycle.get("executed_qty"),
            "fees": lifecycle.get("entry_commission"),
            "position_side": signal.side,
        },
    )


def log_position_closed_detail(
    *,
    exit_price: Decimal,
    requested_exit: Decimal | None,
    realized_pnl: float,
    fees: Decimal | float | None,
    reason: str,
) -> None:
    requested = float(requested_exit) if requested_exit is not None else None
    slippage = float(exit_price - requested_exit) if requested_exit is not None else None
    console(
        "POSITION_CLOSED_DETAIL",
        "",
        {
            "exit_price": float(exit_price),
            "requested_exit": requested,
            "slippage": slippage,
            "realized_pnl": realized_pnl,
            "fees": float(fees) if isinstance(fees, Decimal) else fees,
            "reason": reason,
        },
    )


def ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def init_live_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS live_signals_binance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            pattern TEXT NOT NULL,
            side TEXT,
            trigger_timestamp INTEGER NOT NULL,
            entry_price REAL,
            stop_price REAL,
            tp1_price REAL,
            tp2_price REAL,
            notional_usdt REAL,
            decision TEXT NOT NULL,
            block_reason TEXT,
            dry_run INTEGER NOT NULL,
            exchange_order_id TEXT,
            exit_time INTEGER,
            exit_price REAL,
            realized_r REAL,
            fees REAL,
            raw_order_response TEXT,
            notes TEXT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_live_signals_binance_symbol_pattern_ts
        ON live_signals_binance(symbol, pattern, trigger_timestamp);

        CREATE TABLE IF NOT EXISTS live_trades_binance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            pattern TEXT NOT NULL,
            side TEXT NOT NULL,
            trigger_timestamp INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            stop_price REAL NOT NULL,
            tp1_price REAL NOT NULL,
            tp2_price REAL NOT NULL,
            notional_usdt REAL NOT NULL,
            decision TEXT,
            status TEXT NOT NULL,
            block_reason TEXT,
            dry_run INTEGER NOT NULL,
            exchange_order_id TEXT,
            exit_time INTEGER,
            exit_price REAL,
            realized_r REAL,
            fees REAL,
            raw_order_response TEXT,
            notes TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_live_trades_binance_symbol_status
        ON live_trades_binance(symbol, status);

        CREATE TABLE IF NOT EXISTS live_binance_trader_state (
            symbol TEXT PRIMARY KEY,
            last_processed_close_time INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    ensure_columns(
        conn,
        "live_trades_binance",
        {
            "entry_order_status": "TEXT",
            "avg_fill_price": "REAL",
            "executed_qty": "REAL",
            "entry_order_update_time": "INTEGER",
            "entry_commission": "REAL",
            "entry_commission_asset": "TEXT",
            "entry_slippage": "REAL",
            "entry_slippage_bps": "REAL",
            "order_status_response": "TEXT",
            "user_trades_response": "TEXT",
        },
    )
    conn.commit()


def load_settings(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def get_profile(settings: dict[str, Any], symbol: str) -> PnFProfile | None:
    profiles = settings.get("profiles") or {}
    candidates = [symbol, symbol.split(":", 1)[-1]] if ":" in symbol else [symbol, f"BINANCE_FUT:{symbol}"]
    for candidate in candidates:
        profile = profiles.get(candidate)
        if isinstance(profile, dict):
            return PnFProfile(candidate, float(profile["box_size"]), int(profile.get("reversal_boxes", 3)))
    return None


def binance_symbol(symbol: str) -> str:
    return symbol.split(":", 1)[-1].replace("/", "")


def log_candle_query(runtime_symbol: str, query_symbol: str) -> None:
    console(
        "CANDLE_QUERY",
        "",
        {
            "runtime_symbol": runtime_symbol,
            "normalized_symbol": binance_symbol(runtime_symbol),
            "query_symbol": query_symbol,
            "table": "candles",
            "interval": "1m",
        },
    )


def log_candle_result(symbol: str, candles: list[Candle]) -> None:
    first_candle = candles[0] if candles else None
    last_candle = candles[-1] if candles else None
    console(
        "CANDLE_RESULT",
        "",
        {
            "symbol": symbol,
            "rows": len(candles),
            "first_close_time": first_candle.close_time if first_candle is not None else None,
            "last_close_time": last_candle.close_time if last_candle is not None else None,
            "last_close": last_candle.close if last_candle is not None else None,
            "last_high": last_candle.high if last_candle is not None else None,
            "last_low": last_candle.low if last_candle is not None else None,
        },
    )


def log_raw_candle_symbol_max_times(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT symbol, MAX(close_time)
        FROM candles
        GROUP BY symbol
        """
    ).fetchall()
    target_symbols = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    matching_rows = [
        {"symbol": str(symbol), "max_close_time": int(max_close_time) if max_close_time is not None else None}
        for symbol, max_close_time in rows
        if str(symbol) in target_symbols
    ]
    console("RAW_CANDLE_SYMBOL_MAX", "", {"table": "candles", "rows": matching_rows})


def log_market_runtime_compare(symbol: str, profile: PnFProfile, candles: list[Candle]) -> None:
    engine, _latest_ts = _replay_close_confirmed_pnf(profile, candles)
    last_candle = candles[-1] if candles else None
    console(
        "MARKET_RUNTIME_COMPARE",
        "",
        compact_visibility_payload(
            {
                "symbol": symbol,
                "latest_candle_close": last_candle.close if last_candle is not None else None,
                "pnf_last_price": engine.last_price,
            }
        ),
    )


def load_candles(conn: sqlite3.Connection, symbol: str, limit: int) -> list[Candle]:
    query_symbol = symbol
    log_candle_query(symbol, query_symbol)
    rows = conn.execute(
        """
        SELECT close_time, close, high, low
        FROM candles
        WHERE symbol = ? AND interval = '1m'
        ORDER BY close_time DESC
        LIMIT ?
        """,
        (query_symbol, limit),
    ).fetchall()
    candles = [Candle(int(ts), float(close), float(high), float(low)) for ts, close, high, low in reversed(rows)]
    log_candle_result(query_symbol, candles)
    return candles


def latest_candle_close_time(candles: list[Candle]) -> int | None:
    return candles[-1].close_time if candles else None


def get_last_processed_close_time(conn: sqlite3.Connection, symbol: str) -> int | None:
    row = conn.execute(
        "SELECT last_processed_close_time FROM live_binance_trader_state WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    return int(row[0]) if row is not None else None


def set_last_processed_close_time(conn: sqlite3.Connection, symbol: str, close_time: int) -> None:
    conn.execute(
        """
        INSERT INTO live_binance_trader_state(symbol, last_processed_close_time, updated_at)
        VALUES(?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
          last_processed_close_time=excluded.last_processed_close_time,
          updated_at=excluded.updated_at
        """,
        (symbol, int(close_time), now_iso()),
    )
    conn.commit()


def _column_kind(col: Any) -> str:
    return str(getattr(col, "kind", "")).upper()


def _column_top(col: Any) -> Decimal:
    return _dec(getattr(col, "top", 0.0))


def _column_bottom(col: Any) -> Decimal:
    return _dec(getattr(col, "bottom", 0.0))


def _consecutive_indices(columns: list[Any]) -> bool:
    indices = [int(getattr(col, "idx", -1)) for col in columns]
    return bool(indices) and indices == list(range(indices[0], indices[0] + len(indices)))


def _replay_close_confirmed_pnf(profile: PnFProfile, candles: list[Candle]) -> tuple[PnFEngine, int | None]:
    engine = PnFEngine(profile)
    latest_ts = None
    for candle in candles:
        engine.update_from_price(candle.close_time, candle.close)
        latest_ts = candle.close_time
    return engine, latest_ts


def detect_latest_strict_triangle(symbol: str, profile: PnFProfile, candles: list[Candle]) -> TriangleSignal | None:
    engine, latest_ts = _replay_close_confirmed_pnf(profile, candles)
    if latest_ts is None or len(engine.columns) < 5:
        return None

    sequence = engine.columns[-5:]
    if not _consecutive_indices(sequence):
        return None
    kinds = [_column_kind(col) for col in sequence]
    box_size = _dec(profile.box_size)

    if kinds == ["X", "O", "X", "O", "X"] and engine.latest_signal_name() == "BUY":
        first_x, first_o, lower_high_x, higher_low_o, breakout_x = sequence
        resistance = _column_top(lower_high_x)
        support = _column_bottom(higher_low_o)
        if _column_top(lower_high_x) >= _column_top(first_x):
            return None
        if _column_bottom(higher_low_o) <= _column_bottom(first_o):
            return None
        if _column_top(breakout_x) <= resistance:
            return None
        entry = resistance
        risk = max((_column_top(breakout_x) - resistance) / box_size, Decimal("1")) * box_size
        return TriangleSignal(
            symbol=symbol,
            pattern="bullish_triangle",
            side="LONG",
            trigger_ts=latest_ts,
            entry_price=entry,
            stop_price=entry - risk,
            tp1_price=entry + (Decimal("2") * risk),
            tp2_price=entry + (Decimal("3") * risk),
            trigger_column_idx=int(getattr(breakout_x, "idx")),
            support_level=support,
            resistance_level=resistance,
            break_distance_boxes=(_column_top(breakout_x) - resistance) / box_size,
            pattern_quality="STRICT_CONSECUTIVE_5_COL_TRIANGLE_UP_BREAK",
        )

    if kinds == ["O", "X", "O", "X", "O"] and engine.latest_signal_name() == "SELL":
        first_o, first_x, higher_low_o, lower_high_x, breakdown_o = sequence
        support = _column_bottom(higher_low_o)
        resistance = _column_top(lower_high_x)
        if _column_bottom(higher_low_o) <= _column_bottom(first_o):
            return None
        if _column_top(lower_high_x) >= _column_top(first_x):
            return None
        if _column_bottom(breakdown_o) >= support:
            return None
        entry = support
        risk = max((support - _column_bottom(breakdown_o)) / box_size, Decimal("1")) * box_size
        return TriangleSignal(
            symbol=symbol,
            pattern="bearish_triangle",
            side="SHORT",
            trigger_ts=latest_ts,
            entry_price=entry,
            stop_price=entry + risk,
            tp1_price=entry - (Decimal("2") * risk),
            tp2_price=entry - (Decimal("3") * risk),
            trigger_column_idx=int(getattr(breakdown_o, "idx")),
            support_level=support,
            resistance_level=resistance,
            break_distance_boxes=(support - _column_bottom(breakdown_o)) / box_size,
            pattern_quality="STRICT_CONSECUTIVE_5_COL_TRIANGLE_DOWN_BREAK",
        )
    return None


def detect_latest_strict_double(symbol: str, profile: PnFProfile, candles: list[Candle]) -> TriangleSignal | None:
    engine, latest_ts = _replay_close_confirmed_pnf(profile, candles)
    if latest_ts is None or len(engine.columns) < 3:
        return None

    sequence = engine.columns[-3:]
    if not _consecutive_indices(sequence):
        return None
    kinds = [_column_kind(col) for col in sequence]
    box_size = _dec(profile.box_size)

    if kinds == ["X", "O", "X"] and engine.latest_signal_name() == "BUY":
        prior_x, middle_o, trigger_x = sequence
        breakout_level = _column_top(prior_x)
        trigger_level = _column_top(trigger_x)
        if trigger_level <= breakout_level:
            return None
        break_distance_boxes = (trigger_level - breakout_level) / box_size
        risk = max(break_distance_boxes, Decimal("1")) * box_size
        return TriangleSignal(
            symbol=symbol,
            pattern="double_top_breakout",
            side="LONG",
            trigger_ts=latest_ts,
            entry_price=breakout_level,
            stop_price=breakout_level - risk,
            tp1_price=breakout_level + (Decimal("2") * risk),
            tp2_price=breakout_level + (Decimal("3") * risk),
            trigger_column_idx=int(getattr(trigger_x, "idx")),
            support_level=_column_bottom(middle_o),
            resistance_level=breakout_level,
            break_distance_boxes=break_distance_boxes,
            pattern_quality="STRICT_CONSECUTIVE_3_COL_DOUBLE_TOP_BREAKOUT",
        )

    if kinds == ["O", "X", "O"] and engine.latest_signal_name() == "SELL":
        prior_o, middle_x, trigger_o = sequence
        breakdown_level = _column_bottom(prior_o)
        trigger_level = _column_bottom(trigger_o)
        if trigger_level >= breakdown_level:
            return None
        break_distance_boxes = (breakdown_level - trigger_level) / box_size
        risk = max(break_distance_boxes, Decimal("1")) * box_size
        return TriangleSignal(
            symbol=symbol,
            pattern="double_bottom_breakdown",
            side="SHORT",
            trigger_ts=latest_ts,
            entry_price=breakdown_level,
            stop_price=breakdown_level + risk,
            tp1_price=breakdown_level - (Decimal("2") * risk),
            tp2_price=breakdown_level - (Decimal("3") * risk),
            trigger_column_idx=int(getattr(trigger_o, "idx")),
            support_level=breakdown_level,
            resistance_level=_column_top(middle_x),
            break_distance_boxes=break_distance_boxes,
            pattern_quality="STRICT_CONSECUTIVE_3_COL_DOUBLE_BOTTOM_BREAKDOWN",
        )
    return None


def is_demo_double_signal(signal: TriangleSignal) -> bool:
    return signal.pattern in DEMO_DOUBLE_PATTERNS


def append_demo_double_note(signal: TriangleSignal, notes: str | None) -> str | None:
    if not is_demo_double_signal(signal):
        return notes
    return "DEMO_DOUBLE_SMOKE_TEST" if not notes else f"DEMO_DOUBLE_SMOKE_TEST; {notes}"


def has_existing_open_trade(conn: sqlite3.Connection, symbol: str) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM live_trades_binance WHERE symbol = ? AND status IN ({','.join('?' for _ in OPEN_TRADE_STATUSES)}) LIMIT 1",
        (symbol, *sorted(OPEN_TRADE_STATUSES)),
    ).fetchone()
    return row is not None


def signal_exists(conn: sqlite3.Connection, signal: TriangleSignal) -> bool:
    row = conn.execute(
        "SELECT 1 FROM live_signals_binance WHERE symbol = ? AND pattern = ? AND trigger_timestamp = ? LIMIT 1",
        (signal.symbol, signal.pattern, signal.trigger_ts),
    ).fetchone()
    return row is not None


def record_signal(
    conn: sqlite3.Connection,
    signal: TriangleSignal,
    *,
    decision: str,
    block_reason: str | None,
    dry_run: bool,
    notional_usdt: Decimal,
    exchange_order_id: str | None = None,
    raw_order_response: dict[str, Any] | None = None,
    notes: str | None = None,
) -> None:
    raw = json.dumps(raw_order_response if raw_order_response is not None else signal.__dict__, sort_keys=True, default=str)
    conn.execute(
        """
        INSERT OR IGNORE INTO live_signals_binance(
            created_at, symbol, pattern, side, trigger_timestamp, entry_price,
            stop_price, tp1_price, tp2_price, notional_usdt, decision,
            block_reason, dry_run, exchange_order_id, raw_order_response, notes
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_iso(),
            signal.symbol,
            signal.pattern,
            signal.side,
            signal.trigger_ts,
            float(signal.entry_price),
            float(signal.stop_price),
            float(signal.tp1_price),
            float(signal.tp2_price),
            float(notional_usdt),
            decision,
            block_reason,
            int(dry_run),
            exchange_order_id,
            raw,
            notes,
        ),
    )
    conn.commit()


def record_trade(
    conn: sqlite3.Connection,
    signal: TriangleSignal,
    *,
    notional_usdt: Decimal,
    exchange_order_id: str | None,
    status: str,
    dry_run: bool,
    decision: str | None = None,
    block_reason: str | None = None,
    raw_order_response: dict[str, Any] | None = None,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO live_trades_binance(
            created_at, symbol, pattern, side, trigger_timestamp, entry_price,
            stop_price, tp1_price, tp2_price, notional_usdt, decision, status,
            block_reason, dry_run, exchange_order_id, raw_order_response, notes
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_iso(),
            signal.symbol,
            signal.pattern,
            signal.side,
            signal.trigger_ts,
            float(signal.entry_price),
            float(signal.stop_price),
            float(signal.tp1_price),
            float(signal.tp2_price),
            float(notional_usdt),
            decision,
            status,
            block_reason,
            int(dry_run),
            exchange_order_id,
            json.dumps(raw_order_response or {}, sort_keys=True, default=str),
            notes,
        ),
    )
    conn.commit()



def extract_order_ids(order_request: dict[str, Any] | None, order_response: dict[str, Any] | None) -> tuple[str | None, str | None]:
    order_id = None
    client_order_id = None
    if isinstance(order_response, dict):
        raw_order_id = order_response.get("orderId")
        order_id = str(raw_order_id) if raw_order_id not in (None, "") else None
        raw_client_id = order_response.get("clientOrderId")
        client_order_id = str(raw_client_id) if raw_client_id not in (None, "") else None
    if client_order_id is None and isinstance(order_request, dict):
        raw_client_id = order_request.get("newClientOrderId")
        client_order_id = str(raw_client_id) if raw_client_id not in (None, "") else None
    return order_id, client_order_id


def commission_from_user_trades(user_trades: Any) -> tuple[Decimal | None, str | None]:
    if not isinstance(user_trades, list):
        return None, None
    total = Decimal("0")
    asset: str | None = None
    found = False
    for trade in user_trades:
        if not isinstance(trade, dict) or trade.get("commission") in (None, ""):
            continue
        try:
            total += _dec(trade.get("commission"))
        except ValueError:
            continue
        trade_asset = str(trade.get("commissionAsset", "")) or None
        asset = trade_asset if asset is None else (asset if asset == trade_asset else "MIXED")
        found = True
    return (total, asset) if found else (None, None)


def slippage_from_fill(signal: TriangleSignal, avg_fill_price: Decimal | None) -> tuple[Decimal | None, Decimal | None]:
    if avg_fill_price is None or signal.entry_price == 0:
        return None, None
    signed = avg_fill_price - signal.entry_price if signal.side == "LONG" else signal.entry_price - avg_fill_price
    return signed, (signed / signal.entry_price) * Decimal("10000")


def poll_entry_order_status(
    client: BinanceFuturesClient,
    signal: TriangleSignal,
    order_request: dict[str, Any] | None,
    order_response: dict[str, Any] | None,
) -> tuple[dict[str, Any], Any, dict[str, Any]]:
    order_id, client_order_id = extract_order_ids(order_request, order_response)
    status_response = client.get_order(binance_symbol(signal.symbol), order_id=order_id, client_order_id=client_order_id)
    trades_response: Any = []
    if order_id is not None:
        try:
            trades_response = client.get_user_trades(binance_symbol(signal.symbol), order_id=order_id)
        except Exception as exc:
            trades_response = {"error": str(exc)}

    status = str(status_response.get("status", ""))
    avg_fill_price = _dec(status_response.get("avgPrice")) if status_response.get("avgPrice") not in (None, "", "0", 0) else None
    executed_qty = _dec(status_response.get("executedQty")) if status_response.get("executedQty") not in (None, "") else None
    commission, commission_asset = commission_from_user_trades(trades_response)
    slippage, slippage_bps = slippage_from_fill(signal, avg_fill_price)
    lifecycle = {
        "entry_order_status": status,
        "avg_fill_price": float(avg_fill_price) if avg_fill_price is not None else None,
        "executed_qty": float(executed_qty) if executed_qty is not None else None,
        "entry_order_update_time": int(status_response.get("updateTime")) if status_response.get("updateTime") not in (None, "") else None,
        "entry_commission": float(commission) if commission is not None else None,
        "entry_commission_asset": commission_asset,
        "entry_slippage": float(slippage) if slippage is not None else None,
        "entry_slippage_bps": float(slippage_bps) if slippage_bps is not None else None,
    }
    return status_response, trades_response, lifecycle


def apply_entry_lifecycle(
    conn: sqlite3.Connection,
    trade_id: int,
    *,
    signal: TriangleSignal,
    order_request: dict[str, Any] | None,
    order_response: dict[str, Any] | None,
    status_response: dict[str, Any],
    trades_response: Any,
    lifecycle: dict[str, Any],
    verbose_market_logs: bool = False,
) -> None:
    entry_status = str(lifecycle.get("entry_order_status") or "")
    trade_status = "POSITION_OPEN" if entry_status == "FILLED" else "ORDER_SENT"
    notes = "entry order filled; POSITION_OPEN" if entry_status == "FILLED" else f"entry order not filled; status={entry_status}; exits disabled until FILLED"
    conn.execute(
        """
        UPDATE live_trades_binance
        SET status = ?, entry_order_status = ?, avg_fill_price = ?, executed_qty = ?,
            entry_order_update_time = ?, entry_commission = ?, entry_commission_asset = ?,
            entry_slippage = ?, entry_slippage_bps = ?, fees = ?, order_status_response = ?,
            user_trades_response = ?, raw_order_response = ?, notes = ?
        WHERE id = ?
        """,
        (
            trade_status,
            lifecycle.get("entry_order_status"),
            lifecycle.get("avg_fill_price"),
            lifecycle.get("executed_qty"),
            lifecycle.get("entry_order_update_time"),
            lifecycle.get("entry_commission"),
            lifecycle.get("entry_commission_asset"),
            lifecycle.get("entry_slippage"),
            lifecycle.get("entry_slippage_bps"),
            lifecycle.get("entry_commission"),
            json.dumps(status_response, sort_keys=True, default=str),
            json.dumps(trades_response, sort_keys=True, default=str),
            json.dumps(
                {"order_request": order_request, "order_response": order_response, "order_status": status_response, "user_trades": trades_response},
                sort_keys=True,
                default=str,
            ),
            notes,
            int(trade_id),
        ),
    )
    conn.commit()
    if verbose_market_logs and entry_status == "FILLED":
        log_position_open_detail(signal, lifecycle)


def poll_pending_entry_orders(conn: sqlite3.Connection, client: BinanceFuturesClient, *, live_enabled: bool, verbose_market_logs: bool = False) -> None:
    if not live_enabled:
        return
    rows = conn.execute(
        """
        SELECT id, symbol, pattern, side, trigger_timestamp, entry_price, stop_price, tp1_price, tp2_price,
               raw_order_response
        FROM live_trades_binance
        WHERE status = 'ORDER_SENT'
        """
    ).fetchall()
    for row in rows:
        trade_id, symbol, pattern, side, trigger_ts, entry, stop, tp1, tp2, raw_order_response = row
        signal = TriangleSignal(
            symbol=symbol,
            pattern=pattern,
            side=side,
            trigger_ts=int(trigger_ts),
            entry_price=_dec(entry),
            stop_price=_dec(stop),
            tp1_price=_dec(tp1),
            tp2_price=_dec(tp2),
            trigger_column_idx=0,
            support_level=Decimal("0"),
            resistance_level=Decimal("0"),
            break_distance_boxes=Decimal("0"),
            pattern_quality="PERSISTED_ENTRY_ORDER",
        )
        try:
            raw = json.loads(raw_order_response or "{}")
            order_request = raw.get("order_request") if isinstance(raw, dict) else None
            order_response = raw.get("order_response") if isinstance(raw, dict) else None
            status_response, trades_response, lifecycle = poll_entry_order_status(client, signal, order_request, order_response)
            apply_entry_lifecycle(
                conn,
                int(trade_id),
                signal=signal,
                order_request=order_request,
                order_response=order_response,
                status_response=status_response,
                trades_response=trades_response,
                lifecycle=lifecycle,
                verbose_market_logs=verbose_market_logs,
            )
            console("ORDER_STATUS", f"{symbol} trade_id={trade_id}", lifecycle)
        except Exception as exc:
            console("ORDER_FAILED", f"{symbol} trade_id={trade_id} status poll", {"error": str(exc)})


def record_submitted_entry_order(
    conn: sqlite3.Connection,
    client: BinanceFuturesClient,
    signal: TriangleSignal,
    *,
    order: dict[str, Any],
    notional_usdt: Decimal,
    verbose_market_logs: bool = False,
) -> dict[str, Any]:
    raw_response = client.submit_order(order)
    order_id, client_order_id = extract_order_ids(order, raw_response)
    exchange_order_id = order_id or client_order_id
    record_signal(
        conn,
        signal,
        decision="ORDER_SENT",
        block_reason=None,
        dry_run=False,
        notional_usdt=notional_usdt,
        exchange_order_id=exchange_order_id,
        raw_order_response={"order_request": order, "order_response": raw_response},
        notes=append_demo_double_note(signal, json.dumps(raw_response)),
    )
    record_trade(
        conn,
        signal,
        notional_usdt=notional_usdt,
        exchange_order_id=exchange_order_id,
        status="ORDER_SENT",
        dry_run=False,
        decision="ORDER_SENT",
        raw_order_response={"order_request": order, "order_response": raw_response},
        notes=append_demo_double_note(signal, "live Binance USD-M futures limit order submitted; awaiting FILLED status before exit management"),
    )
    trade_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    try:
        status_response, trades_response, lifecycle = poll_entry_order_status(client, signal, order, raw_response)
    except Exception as exc:
        poll_error = {"error": str(exc)}
        conn.execute(
            "UPDATE live_trades_binance SET order_status_response = ?, notes = ? WHERE id = ?",
            (json.dumps(poll_error, sort_keys=True), f"entry order submitted; status poll failed and will retry: {exc}", trade_id),
        )
        conn.commit()
        return {"order_response": raw_response, "order_status": poll_error, "user_trades": [], "lifecycle": {"entry_order_status": "POLL_FAILED"}}
    apply_entry_lifecycle(
        conn,
        trade_id,
        signal=signal,
        order_request=order,
        order_response=raw_response,
        status_response=status_response,
        trades_response=trades_response,
        lifecycle=lifecycle,
        verbose_market_logs=verbose_market_logs,
    )
    return {"order_response": raw_response, "order_status": status_response, "user_trades": trades_response, "lifecycle": lifecycle}

def order_request_from_trade(row: sqlite3.Row | tuple[Any, ...]) -> dict[str, Any] | None:
    try:
        raw = json.loads(row[-1] or "{}")
    except (TypeError, json.JSONDecodeError):
        return None
    if isinstance(raw, dict):
        request = raw.get("order_request") or raw.get("would_submit_order")
        return request if isinstance(request, dict) else None
    return None


def decimals_for_step(step: Decimal) -> int:
    normalized = step.normalize()
    return max(0, -normalized.as_tuple().exponent)


def quantize_down(value: Decimal, unit: Decimal) -> Decimal:
    if unit <= 0:
        raise ValueError("precision unit must be positive")
    return ((value / unit).to_integral_value(rounding=ROUND_DOWN) * unit).quantize(unit.normalize(), rounding=ROUND_DOWN)


def aligned(value: Decimal, unit: Decimal) -> bool:
    if unit <= 0:
        return False
    return (value / unit) == (value / unit).to_integral_value()


def parse_symbol_spec(exchange_info: dict[str, Any], symbol: str) -> SymbolSpec:
    rows = exchange_info.get("symbols") or []
    row = next((item for item in rows if item.get("symbol") == symbol), None)
    if not isinstance(row, dict):
        raise RuntimeError(f"exchangeInfo missing symbol {symbol}")
    filters = {item.get("filterType"): item for item in row.get("filters", []) if isinstance(item, dict)}
    price_filter = filters.get("PRICE_FILTER") or {}
    lot_filter = filters.get("LOT_SIZE") or {}
    notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}
    min_notional = notional_filter.get("notional", notional_filter.get("minNotional", "0"))
    tick_size = _dec(price_filter.get("tickSize"))
    step_size = _dec(lot_filter.get("stepSize"))
    return SymbolSpec(
        symbol=symbol,
        status=str(row.get("status", "")),
        base_asset=str(row.get("baseAsset", "")),
        quote_asset=str(row.get("quoteAsset", "")),
        tick_size=tick_size,
        step_size=step_size,
        min_qty=_dec(lot_filter.get("minQty")),
        max_qty=_dec(lot_filter.get("maxQty")),
        min_notional=_dec(min_notional),
        price_precision=decimals_for_step(tick_size),
        quantity_precision=decimals_for_step(step_size),
    )


def build_entry_order(signal: TriangleSignal, spec: SymbolSpec, notional_usdt: Decimal) -> tuple[dict[str, Any] | None, str | None]:
    if notional_usdt > MAX_NOTIONAL_USDT:
        return None, "notional exceeds 1 USDT"
    if spec.status != "TRADING":
        return None, f"symbol status not TRADING: {spec.status}"
    if spec.quote_asset != "USDT":
        return None, f"symbol quote asset is not USDT: {spec.quote_asset}"
    price = quantize_down(signal.entry_price, spec.tick_size)
    if price <= 0:
        return None, "entry price is non-positive"
    quantity = quantize_down(notional_usdt / price, spec.step_size)
    if not aligned(price, spec.tick_size):
        return None, "price precision invalid"
    if not aligned(quantity, spec.step_size):
        return None, "quantity precision invalid"
    if quantity < spec.min_qty:
        return None, f"quantity below minQty after 1 USDT cap: quantity={quantity} minQty={spec.min_qty}"
    if quantity > spec.max_qty:
        return None, f"quantity above maxQty: quantity={quantity} maxQty={spec.max_qty}"
    actual_notional = quantity * price
    if actual_notional > MAX_NOTIONAL_USDT:
        return None, f"rounded notional exceeds 1 USDT: {actual_notional}"
    if actual_notional < spec.min_notional:
        return None, f"min order notional cannot support 1 USDT cap: actual={actual_notional} minNotional={spec.min_notional}"
    return (
        {
            "symbol": spec.symbol,
            "side": "BUY" if signal.side == "LONG" else "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": str(quantity),
            "price": str(price),
            "newClientOrderId": f"pnf-{signal.pattern[:4]}-{signal.trigger_ts}-{signal.side[0]}"[:36],
        },
        None,
    )


def build_reduce_only_close_order(
    *,
    trade_id: int,
    symbol: str,
    side: str,
    exit_price: Decimal,
    entry_order: dict[str, Any],
    spec: SymbolSpec,
) -> tuple[dict[str, Any] | None, str | None]:
    if "quantity" not in entry_order:
        return None, "missing entry order quantity for reduce-only close"
    price = quantize_down(exit_price, spec.tick_size)
    quantity = _dec(entry_order["quantity"])
    if not aligned(price, spec.tick_size):
        return None, "close price precision invalid"
    if not aligned(quantity, spec.step_size):
        return None, "close quantity precision invalid"
    if quantity <= 0:
        return None, "close quantity is non-positive"
    return (
        {
            "symbol": binance_symbol(symbol),
            "side": "SELL" if side == "LONG" else "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": str(quantity),
            "price": str(price),
            "reduceOnly": "true",
            "newClientOrderId": f"pnf-exit-{trade_id}"[:36],
        },
        None,
    )


def position_mode_is_unambiguous(mode_response: dict[str, Any]) -> bool:
    return mode_response.get("dualSidePosition") is False


def has_exchange_position(position_response: dict[str, Any]) -> bool:
    rows: Any = position_response if isinstance(position_response, list) else position_response.get("positions", position_response.get("data"))
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        return True
    for row in rows:
        try:
            if _dec(row.get("positionAmt", "0")) != 0:
                return True
        except (AttributeError, ValueError):
            return True
    return False


def validate_guards(
    conn: sqlite3.Connection,
    client: BinanceFuturesClient,
    signal: TriangleSignal,
    *,
    notional_usdt: Decimal,
    live_enabled: bool,
    demo: bool = False,
    allow_demo_doubles: bool = False,
) -> tuple[SymbolSpec | None, dict[str, Any] | None, str | None]:
    if signal.symbol not in ALLOWED_SYMBOLS:
        return None, None, "symbol outside live allowlist"
    demo_doubles_enabled = bool(demo and live_enabled and allow_demo_doubles)
    if signal.pattern not in ALLOWED_PATTERNS and not (demo_doubles_enabled and signal.pattern in DEMO_DOUBLE_PATTERNS):
        return None, None, "pattern outside live allowlist"
    if signal.pattern in CATAPULT_SIGNAL_NAMES:
        return None, None, "catapult patterns are log-only"
    if signal_exists(conn, signal):
        return None, None, "duplicate signal for same symbol/pattern/trigger timestamp"
    if has_existing_open_trade(conn, signal.symbol):
        return None, None, "existing open live trade on symbol"
    if notional_usdt > MAX_NOTIONAL_USDT:
        return None, None, "notional exceeds 1 USDT"
    if live_enabled:
        if not client.has_credentials:
            return None, None, "API credentials missing"
        try:
            mode_response = client.get_position_mode()
        except Exception as exc:
            return None, None, f"position mode API error: {exc}"
        if not position_mode_is_unambiguous(mode_response):
            return None, None, f"one-way position mode required; hedge mode is ambiguous: {mode_response}"
    try:
        spec = client.get_symbol_spec(binance_symbol(signal.symbol))
    except Exception as exc:
        return None, None, f"exchangeInfo API error: {exc}"
    order, reason = build_entry_order(signal, spec, notional_usdt)
    if reason is not None:
        return spec, None, reason
    return spec, order, None


def process_self_test_signal(
    conn: sqlite3.Connection,
    client: BinanceFuturesClient,
    *,
    notional_usdt: Decimal,
    symbol: str = "BINANCE_FUT:SOLUSDT",
) -> bool:
    signal = TriangleSignal(
        symbol=symbol,
        pattern="bullish_triangle",
        side="LONG",
        trigger_ts=1,
        entry_price=Decimal("100"),
        stop_price=Decimal("99"),
        tp1_price=Decimal("102"),
        tp2_price=Decimal("103"),
        trigger_column_idx=1,
        support_level=Decimal("99"),
        resistance_level=Decimal("100"),
        break_distance_boxes=Decimal("1"),
        pattern_quality="SELF_TEST_SIGNAL",
    )
    _spec, order, block_reason = validate_guards(
        conn, client, signal, notional_usdt=notional_usdt, live_enabled=False
    )
    if block_reason is not None:
        console("BLOCKED", f"{signal.symbol} self-test", {"reason": block_reason})
        return False
    record_signal(
        conn,
        signal,
        decision="DRY_RUN",
        block_reason=None,
        dry_run=True,
        notional_usdt=notional_usdt,
        raw_order_response={"would_submit_order": order},
        notes="SELF_TEST_SIGNAL",
    )
    record_trade(
        conn,
        signal,
        notional_usdt=notional_usdt,
        exchange_order_id=None,
        status="DRY_RUN",
        dry_run=True,
        decision="DRY_RUN",
        raw_order_response={"would_submit_order": order},
        notes="SELF_TEST_SIGNAL",
    )
    console("BLOCKED", f"{signal.symbol} self-test dry-run; order not sent", {"order": order})
    return True


def update_open_trade_exits(conn: sqlite3.Connection, client: BinanceFuturesClient, *, live_enabled: bool, verbose_market_logs: bool = False) -> None:
    rows = conn.execute(
        """
        SELECT id, symbol, side, trigger_timestamp, entry_price, stop_price, tp1_price, tp2_price, raw_order_response, executed_qty
        FROM live_trades_binance
        WHERE status IN ('POSITION_OPEN','EXIT_PENDING')
        """
    ).fetchall()
    for row in rows:
        trade_id, symbol, side, entry_time, entry, stop, _tp1, tp2, raw_order_response, executed_qty = row
        candles = conn.execute(
            """
            SELECT close_time, high, low
            FROM candles
            WHERE symbol = ? AND interval = '1m' AND close_time > ?
            ORDER BY close_time ASC
            """,
            (symbol, int(entry_time)),
        ).fetchall()
        for close_time, high, low in candles:
            exit_price = None
            exit_reason = None
            if side == "LONG":
                if float(low) <= float(stop):
                    exit_price = Decimal(str(stop))
                    exit_reason = "STOP"
                elif float(high) >= float(tp2):
                    exit_price = Decimal(str(tp2))
                    exit_reason = "TP2"
            else:
                if float(high) >= float(stop):
                    exit_price = Decimal(str(stop))
                    exit_reason = "STOP"
                elif float(low) <= float(tp2):
                    exit_price = Decimal(str(tp2))
                    exit_reason = "TP2"
            if exit_price is None:
                continue

            denom = abs(float(entry) - float(stop))
            realized_r = 0.0 if denom == 0 else (
                (float(exit_price) - float(entry)) / denom if side == "LONG" else (float(entry) - float(exit_price)) / denom
            )
            if not live_enabled:
                conn.execute(
                    """
                    UPDATE live_trades_binance
                    SET status = 'POSITION_CLOSED', exit_time = ?, exit_price = ?, realized_r = ?
                    WHERE id = ?
                    """,
                    (int(close_time), float(exit_price), realized_r, int(trade_id)),
                )
                conn.commit()
                console("POSITION_CLOSED", f"{symbol} trade_id={trade_id}", {"exit_price": float(exit_price), "realized_r": realized_r})
                if verbose_market_logs:
                    log_position_closed_detail(
                        exit_price=exit_price,
                        requested_exit=exit_price,
                        realized_pnl=realized_r,
                        fees=None,
                        reason=exit_reason or "STOP",
                    )
                break

            if not client.has_credentials:
                console("ORDER_FAILED", f"{symbol} trade_id={trade_id} close blocked", {"reason": "API credentials missing"})
                break
            try:
                mode_response = client.get_position_mode()
                if not position_mode_is_unambiguous(mode_response):
                    raise RuntimeError(f"one-way position mode required for reduceOnly exits: {mode_response}")
                spec = client.get_symbol_spec(binance_symbol(symbol))
                entry_order = order_request_from_trade((raw_order_response,))
                if entry_order is not None and executed_qty not in (None, ""):
                    entry_order = {**entry_order, "quantity": str(executed_qty)}
                close_order, reason = build_reduce_only_close_order(
                    trade_id=int(trade_id),
                    symbol=symbol,
                    side=side,
                    exit_price=exit_price,
                    entry_order=entry_order or {},
                    spec=spec,
                )
                if reason is not None:
                    raise RuntimeError(reason)
                raw_close_response = client.submit_order(close_order or {})
                requested_exit = _dec(close_order["price"]) if close_order is not None else exit_price
            except Exception as exc:
                conn.execute(
                    "UPDATE live_trades_binance SET status = 'EXIT_PENDING', notes = ? WHERE id = ?",
                    (f"reduce-only exit failed closed: {exc}", int(trade_id)),
                )
                conn.commit()
                console("ORDER_FAILED", f"{symbol} trade_id={trade_id} reduce-only close", {"error": str(exc)})
                break

            conn.execute(
                """
                UPDATE live_trades_binance
                SET status = 'POSITION_CLOSED', exit_time = ?, exit_price = ?, realized_r = ?, notes = ?
                WHERE id = ?
                """,
                (
                    int(close_time),
                    float(exit_price),
                    realized_r,
                    f"local closed-candle exit; reduce-only close response={json.dumps(raw_close_response, sort_keys=True)}",
                    int(trade_id),
                ),
            )
            conn.commit()
            console("POSITION_CLOSED", f"{symbol} trade_id={trade_id}", {"exit_price": float(exit_price), "realized_r": realized_r})
            if verbose_market_logs:
                log_position_closed_detail(
                    exit_price=exit_price,
                    requested_exit=requested_exit,
                    realized_pnl=realized_r,
                    fees=None,
                    reason=exit_reason or "STOP",
                )
            break


def binance_env_names(demo: bool) -> tuple[str, str]:
    return (BINANCE_DEMO_API_KEY_ENV, BINANCE_DEMO_API_SECRET_ENV) if demo else (BINANCE_API_KEY_ENV, BINANCE_API_SECRET_ENV)


def binance_base_url(demo: bool) -> str:
    return BINANCE_DEMO_BASE_URL if demo else BINANCE_BASE_URL


def execution_mode_label(*, demo: bool, dry_run: bool) -> str:
    venue = "DEMO" if demo else "PRODUCTION"
    execution = "DRY_RUN" if dry_run else "LIVE"
    return f"{venue}_{execution}"


def log_startup(args: argparse.Namespace) -> None:
    dry_run = not (os.environ.get("LIVE_TRADING_ENABLED") == "1" and not args.dry_run)
    console(
        "STARTUP",
        f"mode={execution_mode_label(demo=bool(getattr(args, 'demo', False)), dry_run=dry_run)}",
        {
            "venue": "DEMO" if bool(getattr(args, 'demo', False)) else "PRODUCTION",
            "execution": "DRY_RUN" if dry_run else "LIVE",
            "base_url": binance_base_url(bool(getattr(args, 'demo', False))),
            "dry_run": dry_run,
            "api_key_env": binance_env_names(bool(getattr(args, 'demo', False)))[0],
            "enable_demo_doubles": bool(getattr(args, "enable_demo_doubles", False)),
        },
    )
    log_db_info(args.db_path)


def process_once(args: argparse.Namespace, *, iteration: int | None = None) -> None:
    demo = bool(getattr(args, "demo", False))
    enable_demo_doubles = bool(getattr(args, "enable_demo_doubles", False))
    verbose_market_logs = bool(getattr(args, "verbose_market_logs", False))
    live_enabled = bool(os.environ.get("LIVE_TRADING_ENABLED") == "1" and not args.dry_run)
    dry_run = not live_enabled
    api_key_env, api_secret_env = binance_env_names(demo)
    client = BinanceFuturesClient(
        os.environ.get(api_key_env),
        os.environ.get(api_secret_env),
        base_url=binance_base_url(demo),
    )
    loop_iteration = 1 if iteration is None else iteration
    console("LOOP_BEGIN", f"iteration={loop_iteration}")
    notional_usdt = _dec(args.notional_usdt)

    with closing(sqlite3.connect(args.db_path)) as conn:
        log_db_info(args.db_path, conn)
        if verbose_market_logs:
            log_raw_candle_symbol_max_times(conn)
        init_live_tables(conn)
        if args.self_test_signal:
            if os.environ.get("LIVE_TRADING_ENABLED") == "1" and not args.dry_run:
                console("BLOCKED", "self-test refuses to run unless --dry-run is supplied when LIVE_TRADING_ENABLED=1")
                return
            process_self_test_signal(conn, client, notional_usdt=notional_usdt)
            return

        settings = load_settings(Path(args.settings))
        poll_pending_entry_orders(conn, client, live_enabled=live_enabled, verbose_market_logs=verbose_market_logs)
        update_open_trade_exits(conn, client, live_enabled=live_enabled, verbose_market_logs=verbose_market_logs)
        configured_symbols = [s for s in settings.get("symbols", []) if s in ALLOWED_SYMBOLS]
        for symbol in configured_symbols:
            console("SCAN", symbol)
            profile = get_profile(settings, symbol)
            if profile is None:
                console("BLOCKED", f"{symbol} missing PnF profile")
                continue
            candles = load_candles(conn, symbol, args.history_bars)
            if not candles:
                console("BLOCKED", f"{symbol} has no 1m candles")
                continue
            log_market_runtime_compare(symbol, profile, candles)
            if verbose_market_logs:
                log_market_snapshot(symbol, profile, candles)
            latest_close_time = latest_candle_close_time(candles)
            last_processed = get_last_processed_close_time(conn, symbol)
            if latest_close_time is None or last_processed == latest_close_time:
                console("NO_SIGNAL", symbol)
                continue
            signal = detect_latest_strict_triangle(symbol, profile, candles)
            allow_demo_doubles = bool(enable_demo_doubles and demo and live_enabled)
            if signal is None and allow_demo_doubles:
                signal = detect_latest_strict_double(symbol, profile, candles)
            set_last_processed_close_time(conn, symbol, latest_close_time)
            if signal is None:
                console("NO_SIGNAL", symbol)
                continue

            console("SIGNAL", f"{signal.symbol} {signal.pattern} {signal.side}", signal.__dict__)
            if verbose_market_logs:
                log_signal_detail(signal, profile, candles[-1].close if candles else None)
            _spec, order, block_reason = validate_guards(
                conn, client, signal, notional_usdt=notional_usdt, live_enabled=live_enabled, demo=demo, allow_demo_doubles=allow_demo_doubles
            )
            if verbose_market_logs and order is not None:
                log_order_detail(signal.symbol, order)
            if block_reason is not None:
                record_signal(
                    conn,
                    signal,
                    decision="BLOCKED",
                    block_reason=block_reason,
                    dry_run=dry_run,
                    notional_usdt=notional_usdt,
                    notes=append_demo_double_note(signal, "fail-closed guard"),
                )
                console("BLOCKED", f"{signal.symbol} {signal.pattern}", {"reason": block_reason})
                continue

            if dry_run:
                record_signal(
                    conn,
                    signal,
                    decision="DRY_RUN",
                    block_reason=None,
                    dry_run=True,
                    notional_usdt=notional_usdt,
                    raw_order_response={"would_submit_order": order},
                    notes=append_demo_double_note(signal, "LIVE_TRADING_ENABLED is not 1 or --dry-run was supplied"),
                )
                record_trade(
                    conn,
                    signal,
                    notional_usdt=notional_usdt,
                    exchange_order_id=None,
                    status="DRY_RUN",
                    dry_run=True,
                    decision="DRY_RUN",
                    raw_order_response={"would_submit_order": order},
                    notes=append_demo_double_note(signal, "dry-run only; no exchange order submitted"),
                )
                console("BLOCKED", f"{signal.symbol} dry-run; order not sent", {"order": order})
                continue

            try:
                position_response = client.get_position_risk(binance_symbol(signal.symbol))
                console("POSITION_CHECK", f"{signal.symbol} exchange position precheck", position_response)
                if has_exchange_position(position_response):
                    reason = "exchange reports existing open position"
                    record_signal(conn, signal, decision="BLOCKED", block_reason=reason, dry_run=False, notional_usdt=notional_usdt, notes=append_demo_double_note(signal, json.dumps(position_response)))
                    console("BLOCKED", f"{signal.symbol} {signal.pattern}", {"reason": reason})
                    continue
                lifecycle_result = record_submitted_entry_order(
                    conn,
                    client,
                    signal,
                    order=order or {},
                    notional_usdt=notional_usdt,
                    verbose_market_logs=verbose_market_logs,
                )
            except Exception as exc:
                raw_response = {"error": str(exc)}
                record_signal(conn, signal, decision="ORDER_FAILED", block_reason=str(exc), dry_run=False, notional_usdt=notional_usdt, raw_order_response={"order_request": order, "order_response": raw_response}, notes=append_demo_double_note(signal, "API/order error; fail closed"))
                record_trade(conn, signal, notional_usdt=notional_usdt, exchange_order_id=None, status="ORDER_FAILED", dry_run=False, decision="ORDER_FAILED", block_reason=str(exc), raw_order_response={"order_request": order, "order_response": raw_response}, notes=append_demo_double_note(signal, "API/order error; fail closed"))
                console("ORDER_FAILED", f"{signal.symbol} {signal.pattern}", raw_response)
                continue

            console("ORDER_SENT", f"{signal.symbol} {signal.pattern}", lifecycle_result)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forward-validate strict PnF triangles with guarded Binance USD-M futures micro-orders")
    parser.add_argument("--db-path", required=True, help="Path to existing market_data.db")
    parser.add_argument("--settings", required=True, help="Path to settings.json with PnF profiles")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run mode even if LIVE_TRADING_ENABLED=1")
    parser.add_argument("--demo", action="store_true", help="Use Binance Demo USD-M Futures at https://demo-fapi.binance.com with demo API credentials")
    parser.add_argument("--enable-demo-doubles", action="store_true", help="Enable strict double top/bottom smoke-test execution only for DEMO LIVE mode")
    parser.add_argument("--notional-usdt", default=str(DEFAULT_NOTIONAL_USDT), help="Fixed order notional; hard-capped at 1 USDT")
    parser.add_argument("--history-bars", type=int, default=5000, help="Number of recent 1m candles used to reconstruct close-confirmed PnF state")
    parser.add_argument("--loop", action="store_true", help="Run continuously instead of one pass")
    parser.add_argument("--poll-seconds", type=float, default=30.0, help="Sleep between loop iterations")
    parser.add_argument("--self-test-signal", action="store_true", help="Inject one synthetic allowed dry-run signal through the guarded pipeline")
    parser.add_argument("--verbose-market-logs", action="store_true", help="Emit compact per-symbol market, signal, order, fill, and exit visibility logs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_startup(args)

    if args.loop:
        iteration = 1
        while True:
            process_once(args, iteration=iteration)
            iteration += 1
            time.sleep(args.poll_seconds)
    else:
        process_once(args, iteration=1)


if __name__ == "__main__":
    main()
