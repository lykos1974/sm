#!/usr/bin/env python3
"""Binance USD-M futures live micro-trading forward validator.

Standalone guarded forward validator for strict close-confirmed PnF triangle
signals.  It deliberately does not modify collectors, strategy logic, or the
MEXC validator.  Live exchange writes are fail-closed and require
``LIVE_TRADING_ENABLED=1`` plus Binance USD-M futures API credentials.
"""
from __future__ import annotations

import argparse
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
BINANCE_API_KEY_ENV = "BINANCE_FUTURES_API_KEY"
BINANCE_API_SECRET_ENV = "BINANCE_FUTURES_API_SECRET"
MAX_NOTIONAL_USDT = Decimal("1")
DEFAULT_NOTIONAL_USDT = Decimal("1")
RECV_WINDOW_MS = 5000

ALLOWED_SYMBOLS = {"BINANCE_FUT:BTCUSDT", "BINANCE_FUT:ETHUSDT", "BINANCE_FUT:SOLUSDT"}
ALLOWED_PATTERNS = {"bullish_triangle", "bearish_triangle"}
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


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc


def console(event: str, message: str, details: dict[str, Any] | None = None) -> None:
    suffix = f" {json.dumps(details, sort_keys=True, default=str)}" if details else ""
    print(f"{now_iso()} {event} {message}{suffix}", flush=True)


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


def load_candles(conn: sqlite3.Connection, symbol: str, limit: int) -> list[Candle]:
    rows = conn.execute(
        """
        SELECT close_time, close, high, low
        FROM candles
        WHERE symbol = ? AND interval = '1m'
        ORDER BY close_time DESC
        LIMIT ?
        """,
        (symbol, limit),
    ).fetchall()
    return [Candle(int(ts), float(close), float(high), float(low)) for ts, close, high, low in reversed(rows)]


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


def detect_latest_strict_triangle(symbol: str, profile: PnFProfile, candles: list[Candle]) -> TriangleSignal | None:
    engine = PnFEngine(profile)
    latest_ts = None
    for candle in candles:
        engine.update_from_price(candle.close_time, candle.close)
        latest_ts = candle.close_time
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
) -> tuple[SymbolSpec | None, dict[str, Any] | None, str | None]:
    if signal.symbol not in ALLOWED_SYMBOLS:
        return None, None, "symbol outside live allowlist"
    if signal.pattern not in ALLOWED_PATTERNS:
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


def update_open_trade_exits(conn: sqlite3.Connection, client: BinanceFuturesClient, *, live_enabled: bool) -> None:
    rows = conn.execute(
        """
        SELECT id, symbol, side, trigger_timestamp, entry_price, stop_price, tp1_price, tp2_price, raw_order_response
        FROM live_trades_binance
        WHERE status IN ('OPEN','ORDER_SENT','POSITION_OPEN','EXIT_PENDING')
        """
    ).fetchall()
    for row in rows:
        trade_id, symbol, side, entry_time, entry, stop, _tp1, tp2, raw_order_response = row
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
            if side == "LONG":
                if float(low) <= float(stop):
                    exit_price = Decimal(str(stop))
                elif float(high) >= float(tp2):
                    exit_price = Decimal(str(tp2))
            else:
                if float(high) >= float(stop):
                    exit_price = Decimal(str(stop))
                elif float(low) <= float(tp2):
                    exit_price = Decimal(str(tp2))
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
            break


def process_once(args: argparse.Namespace) -> None:
    settings = load_settings(Path(args.settings))
    live_enabled = bool(os.environ.get("LIVE_TRADING_ENABLED") == "1" and not args.dry_run)
    dry_run = not live_enabled
    client = BinanceFuturesClient(os.environ.get(BINANCE_API_KEY_ENV), os.environ.get(BINANCE_API_SECRET_ENV))
    notional_usdt = _dec(args.notional_usdt)

    conn = sqlite3.connect(args.db_path)
    try:
        init_live_tables(conn)
        update_open_trade_exits(conn, client, live_enabled=live_enabled)
        configured_symbols = [s for s in settings.get("symbols", []) if s in ALLOWED_SYMBOLS]
        for symbol in configured_symbols:
            profile = get_profile(settings, symbol)
            if profile is None:
                console("BLOCKED", f"{symbol} missing PnF profile")
                continue
            candles = load_candles(conn, symbol, args.history_bars)
            if not candles:
                console("BLOCKED", f"{symbol} has no 1m candles")
                continue
            latest_close_time = latest_candle_close_time(candles)
            last_processed = get_last_processed_close_time(conn, symbol)
            if latest_close_time is None or last_processed == latest_close_time:
                continue
            signal = detect_latest_strict_triangle(symbol, profile, candles)
            set_last_processed_close_time(conn, symbol, latest_close_time)
            if signal is None:
                continue

            console("SIGNAL", f"{signal.symbol} {signal.pattern} {signal.side}", signal.__dict__)
            _spec, order, block_reason = validate_guards(
                conn, client, signal, notional_usdt=notional_usdt, live_enabled=live_enabled
            )
            if block_reason is not None:
                record_signal(
                    conn,
                    signal,
                    decision="BLOCKED",
                    block_reason=block_reason,
                    dry_run=dry_run,
                    notional_usdt=notional_usdt,
                    notes="fail-closed guard",
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
                    notes="LIVE_TRADING_ENABLED is not 1 or --dry-run was supplied",
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
                    notes="dry-run only; no exchange order submitted",
                )
                console("BLOCKED", f"{signal.symbol} dry-run; order not sent", {"order": order})
                continue

            try:
                position_response = client.get_position_risk(binance_symbol(signal.symbol))
                console("POSITION_OPEN", f"{signal.symbol} exchange position precheck", position_response)
                if has_exchange_position(position_response):
                    reason = "exchange reports existing open position"
                    record_signal(conn, signal, decision="BLOCKED", block_reason=reason, dry_run=False, notional_usdt=notional_usdt, notes=json.dumps(position_response))
                    console("BLOCKED", f"{signal.symbol} {signal.pattern}", {"reason": reason})
                    continue
                raw_response = client.submit_order(order or {})
            except Exception as exc:
                raw_response = {"error": str(exc)}
                record_signal(conn, signal, decision="ORDER_FAILED", block_reason=str(exc), dry_run=False, notional_usdt=notional_usdt, raw_order_response={"order_request": order, "order_response": raw_response}, notes="API/order error; fail closed")
                record_trade(conn, signal, notional_usdt=notional_usdt, exchange_order_id=None, status="ORDER_FAILED", dry_run=False, decision="ORDER_FAILED", block_reason=str(exc), raw_order_response={"order_request": order, "order_response": raw_response}, notes="API/order error; fail closed")
                console("ORDER_FAILED", f"{signal.symbol} {signal.pattern}", raw_response)
                continue

            order_id = str(raw_response.get("orderId") or raw_response.get("clientOrderId") or "") or None
            record_signal(conn, signal, decision="ORDER_SENT", block_reason=None, dry_run=False, notional_usdt=notional_usdt, exchange_order_id=order_id, raw_order_response={"order_request": order, "order_response": raw_response}, notes=json.dumps(raw_response))
            record_trade(conn, signal, notional_usdt=notional_usdt, exchange_order_id=order_id, status="ORDER_SENT", dry_run=False, decision="ORDER_SENT", raw_order_response={"order_request": order, "order_response": raw_response}, notes="live Binance USD-M futures limit order submitted")
            console("ORDER_SENT", f"{signal.symbol} {signal.pattern}", raw_response)
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forward-validate strict PnF triangles with guarded Binance USD-M futures micro-orders")
    parser.add_argument("--db-path", required=True, help="Path to existing market_data.db")
    parser.add_argument("--settings", required=True, help="Path to settings.json with PnF profiles")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run mode even if LIVE_TRADING_ENABLED=1")
    parser.add_argument("--notional-usdt", default=str(DEFAULT_NOTIONAL_USDT), help="Fixed order notional; hard-capped at 1 USDT")
    parser.add_argument("--history-bars", type=int, default=5000, help="Number of recent 1m candles used to reconstruct close-confirmed PnF state")
    parser.add_argument("--loop", action="store_true", help="Run continuously instead of one pass")
    parser.add_argument("--poll-seconds", type=float, default=30.0, help="Sleep between loop iterations")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    mode = "LIVE" if os.environ.get("LIVE_TRADING_ENABLED") == "1" and not args.dry_run else "DRY_RUN"
    console("BLOCKED" if mode == "DRY_RUN" else "POSITION_OPEN", f"startup mode={mode}")
    if args.loop:
        while True:
            process_once(args)
            time.sleep(args.poll_seconds)
    else:
        process_once(args)


if __name__ == "__main__":
    main()
