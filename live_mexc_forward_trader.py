#!/usr/bin/env python3
"""MEXC futures live micro-trading forward validator.

This module is intentionally separate from collection/backfill code.  It reads
close-confirmed 1m candles from the existing SQLite ``candles`` table, detects
only strict PnF triangle patterns on an explicit allowlist, records every signal,
and places a tiny MEXC futures order only when live trading is explicitly enabled.
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

MEXC_BASE_URL = "https://contract.mexc.com"
MEXC_API_KEY_ENV = "MEXC_FUTURES_API_KEY"
MEXC_API_SECRET_ENV = "MEXC_FUTURES_API_SECRET"
MAX_NOTIONAL_USDT = Decimal("1")
DEFAULT_NOTIONAL_USDT = Decimal("1")
DEFAULT_LEVERAGE = 1

ALLOWED_SYMBOLS = {
    "MEXC_FUT:BTCUSDT",
    "MEXC_FUT:ETHUSDT",
    "MEXC_FUT:SOLUSDT",
    "MEXC_FUT:SUIUSDT",
    "MEXC_FUT:TAOUSDT",
    "MEXC_FUT:HYPEUSDT",
    "MEXC_FUT:ENAUSDT",
}
ALLOWED_PATTERNS = {"bullish_triangle", "bearish_triangle"}
CATAPULT_SIGNAL_NAMES = {"bullish_catapult", "bearish_catapult"}
OPEN_TRADE_STATUSES = {"OPEN", "ORDER_SENT", "POSITION_OPEN"}


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
class ContractSpec:
    symbol: str
    contract_size: Decimal
    price_scale: int
    vol_scale: int
    price_unit: Decimal
    vol_unit: Decimal
    min_vol: Decimal
    max_vol: Decimal
    api_allowed: bool
    state: int
    position_open_type: int


class MexcFuturesClient:
    """Small official REST client for MEXC contract endpoints."""

    def __init__(self, api_key: str | None, api_secret: str | None, *, base_url: str = MEXC_BASE_URL) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> dict[str, Any]:
        method = method.upper()
        params = {k: v for k, v in (params or {}).items() if v is not None}
        url = f"{self.base_url}{path}"
        body_bytes: bytes | None = None
        headers = {"Content-Type": "application/json"}

        sign_payload = ""
        if method in {"GET", "DELETE"} and params:
            query = urllib.parse.urlencode(sorted(params.items()))
            url = f"{url}?{query}"
            sign_payload = query
        elif method == "POST":
            body_text = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)
            body_bytes = body_text.encode("utf-8")
            sign_payload = body_text

        if signed:
            if not self.has_credentials:
                raise RuntimeError("missing MEXC futures API credentials")
            request_time = str(int(time.time() * 1000))
            signature_base = f"{self.api_key}{request_time}{sign_payload}"
            signature = hmac.new(
                str(self.api_secret).encode("utf-8"), signature_base.encode("utf-8"), hashlib.sha256
            ).hexdigest()
            headers.update({"ApiKey": str(self.api_key), "Request-Time": request_time, "Signature": signature})

        request = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"MEXC HTTP {exc.code}: {raw}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"MEXC request failed: {exc}") from exc
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"MEXC non-JSON response: {raw[:500]}") from exc
        return data

    def get_contract_detail(self, symbol: str) -> ContractSpec:
        payload = self._request_json("GET", "/api/v1/contract/detail", params={"symbol": symbol})
        if not payload.get("success"):
            raise RuntimeError(f"contract detail failed: {payload}")
        rows = payload.get("data") or []
        if isinstance(rows, dict):
            rows = [rows]
        if not rows:
            raise RuntimeError(f"contract detail missing for {symbol}: {payload}")
        row = rows[0]
        return ContractSpec(
            symbol=str(row["symbol"]),
            contract_size=_dec(row["contractSize"]),
            price_scale=int(row["priceScale"]),
            vol_scale=int(row["volScale"]),
            price_unit=_dec(row["priceUnit"]),
            vol_unit=_dec(row["volUnit"]),
            min_vol=_dec(row["minVol"]),
            max_vol=_dec(row["maxVol"]),
            api_allowed=bool(row.get("apiAllowed")),
            state=int(row.get("state", -1)),
            position_open_type=int(row.get("positionOpenType", 0)),
        )

    def get_open_positions(self, symbol: str) -> dict[str, Any]:
        return self._request_json("GET", "/api/v1/private/position/open_positions", params={"symbol": symbol}, signed=True)

    def submit_order(self, order: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("POST", "/api/v1/private/order/submit", body=order, signed=True)


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
        CREATE TABLE IF NOT EXISTS live_signals (
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
            raw_signal_json TEXT,
            notes TEXT
        );

        DROP INDEX IF EXISTS ux_live_signals_symbol_pattern_ts;

        CREATE INDEX IF NOT EXISTS idx_live_signals_symbol_pattern_ts
        ON live_signals(symbol, pattern, trigger_timestamp);

        CREATE TABLE IF NOT EXISTS live_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            pattern TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_time INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            stop_price REAL NOT NULL,
            tp1_price REAL NOT NULL,
            tp2_price REAL NOT NULL,
            notional_usdt REAL NOT NULL,
            exchange_order_id TEXT,
            status TEXT NOT NULL,
            exit_time INTEGER,
            exit_price REAL,
            realized_r REAL,
            fees REAL,
            raw_order_response TEXT,
            notes TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_live_trades_symbol_status
        ON live_trades(symbol, status);
        """
    )
    conn.commit()


def load_settings(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def get_profile(settings: dict[str, Any], symbol: str) -> PnFProfile | None:
    profiles = settings.get("profiles") or {}
    candidates = [symbol, symbol.split(":", 1)[-1]] if ":" in symbol else [symbol, f"MEXC_FUT:{symbol}"]
    for candidate in candidates:
        profile = profiles.get(candidate)
        if isinstance(profile, dict):
            return PnFProfile(candidate, float(profile["box_size"]), int(profile.get("reversal_boxes", 3)))
    return None


def mexc_contract_symbol(symbol: str) -> str:
    base = symbol.split(":", 1)[-1]
    if base.endswith("USDT"):
        return f"{base[:-4]}_USDT"
    return base.replace("/", "_")


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
        f"SELECT 1 FROM live_trades WHERE symbol = ? AND status IN ({','.join('?' for _ in OPEN_TRADE_STATUSES)}) LIMIT 1",
        (symbol, *sorted(OPEN_TRADE_STATUSES)),
    ).fetchone()
    return row is not None


def signal_exists(conn: sqlite3.Connection, signal: TriangleSignal) -> bool:
    row = conn.execute(
        "SELECT 1 FROM live_signals WHERE symbol = ? AND pattern = ? AND trigger_timestamp = ? LIMIT 1",
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
    notes: str | None,
) -> None:
    raw = json.dumps(signal.__dict__, sort_keys=True, default=str)
    conn.execute(
        """
        INSERT INTO live_signals(
            created_at, symbol, pattern, side, trigger_timestamp, entry_price,
            stop_price, tp1_price, tp2_price, notional_usdt, decision,
            block_reason, dry_run, raw_signal_json, notes
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
    raw_order_response: dict[str, Any] | None,
    notes: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO live_trades(
            created_at, symbol, pattern, side, entry_time, entry_price,
            stop_price, tp1_price, tp2_price, notional_usdt, exchange_order_id,
            status, raw_order_response, notes
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
            exchange_order_id,
            status,
            json.dumps(raw_order_response or {}, sort_keys=True, default=str),
            notes,
        ),
    )
    conn.commit()


def update_open_trade_exits(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT id, symbol, side, entry_time, entry_price, stop_price, tp1_price, tp2_price
        FROM live_trades
        WHERE status IN ('OPEN','ORDER_SENT','POSITION_OPEN')
        """
    ).fetchall()
    for row in rows:
        trade_id, symbol, side, entry_time, entry, stop, _tp1, tp2 = row
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
                    exit_price = float(stop)
                elif float(high) >= float(tp2):
                    exit_price = float(tp2)
            else:
                if float(high) >= float(stop):
                    exit_price = float(stop)
                elif float(low) <= float(tp2):
                    exit_price = float(tp2)
            if exit_price is None:
                continue
            denom = abs(float(entry) - float(stop))
            realized_r = 0.0 if denom == 0 else ((exit_price - float(entry)) / denom if side == "LONG" else (float(entry) - exit_price) / denom)
            conn.execute(
                """
                UPDATE live_trades
                SET status = 'POSITION_CLOSED', exit_time = ?, exit_price = ?, realized_r = ?
                WHERE id = ?
                """,
                (int(close_time), exit_price, realized_r, int(trade_id)),
            )
            conn.commit()
            console("POSITION_CLOSED", f"{symbol} trade_id={trade_id}", {"exit_price": exit_price, "realized_r": realized_r})
            break


def aligned(value: Decimal, unit: Decimal, scale: int) -> bool:
    if unit <= 0:
        return False
    quantized = value.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_DOWN)
    if value != quantized:
        return False
    return (value / unit) == (value / unit).to_integral_value()


def quantize_down(value: Decimal, unit: Decimal, scale: int) -> Decimal:
    if unit <= 0:
        raise ValueError("precision unit must be positive")
    stepped = (value / unit).to_integral_value(rounding=ROUND_DOWN) * unit
    return stepped.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_DOWN)


def build_order(signal: TriangleSignal, spec: ContractSpec, notional_usdt: Decimal) -> tuple[dict[str, Any] | None, str | None]:
    if notional_usdt > MAX_NOTIONAL_USDT:
        return None, "notional exceeds 1 USDT"
    if spec.state != 0:
        return None, f"contract state not enabled: {spec.state}"
    if not spec.api_allowed:
        return None, "MEXC contract detail reports apiAllowed=false"
    if spec.position_open_type not in {1, 3}:
        return None, f"isolated mode not supported by contract positionOpenType={spec.position_open_type}"

    price = quantize_down(signal.entry_price, spec.price_unit, spec.price_scale)
    stop = quantize_down(signal.stop_price, spec.price_unit, spec.price_scale)
    tp2 = quantize_down(signal.tp2_price, spec.price_unit, spec.price_scale)
    if not all(aligned(v, spec.price_unit, spec.price_scale) for v in (price, stop, tp2)):
        return None, "price precision invalid"
    if price <= 0:
        return None, "entry price is non-positive"

    raw_vol = notional_usdt / (price * spec.contract_size)
    vol = quantize_down(raw_vol, spec.vol_unit, spec.vol_scale)
    if not aligned(vol, spec.vol_unit, spec.vol_scale):
        return None, "quantity precision invalid"
    if vol < spec.min_vol:
        return None, f"quantity below minVol after 1 USDT cap: vol={vol} minVol={spec.min_vol}"
    if vol > spec.max_vol:
        return None, f"quantity above maxVol: vol={vol} maxVol={spec.max_vol}"

    actual_notional = vol * price * spec.contract_size
    if actual_notional > MAX_NOTIONAL_USDT:
        return None, f"rounded contract notional exceeds 1 USDT: {actual_notional}"

    external_oid = f"pnf-{signal.pattern[:4]}-{signal.trigger_ts}-{signal.side[0]}"[:32]
    order = {
        "symbol": spec.symbol,
        "price": str(price),
        "vol": str(vol),
        "leverage": DEFAULT_LEVERAGE,
        "side": 1 if signal.side == "LONG" else 3,
        "type": 5,
        "openType": 1,
        "externalOid": external_oid,
        "stopLossPrice": str(stop),
        "takeProfitPrice": str(tp2),
        "positionMode": 2,
    }
    return order, None


def validate_guards(
    conn: sqlite3.Connection,
    client: MexcFuturesClient,
    signal: TriangleSignal,
    *,
    notional_usdt: Decimal,
    live_enabled: bool,
) -> tuple[ContractSpec | None, dict[str, Any] | None, str | None]:
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
    if not client.has_credentials:
        return None, None, "API credentials missing"

    mexc_symbol = mexc_contract_symbol(signal.symbol)
    try:
        spec = client.get_contract_detail(mexc_symbol)
    except Exception as exc:
        return None, None, f"contract detail API error: {exc}"
    order, reason = build_order(signal, spec, notional_usdt)
    if reason is not None:
        return spec, None, reason
    return spec, order, None


def process_once(args: argparse.Namespace) -> None:
    settings = load_settings(Path(args.settings))
    live_enabled = bool(os.environ.get("LIVE_TRADING_ENABLED") == "1" and not args.dry_run)
    dry_run = not live_enabled
    client = MexcFuturesClient(os.environ.get(MEXC_API_KEY_ENV), os.environ.get(MEXC_API_SECRET_ENV))
    notional_usdt = _dec(args.notional_usdt)

    conn = sqlite3.connect(args.db_path)
    try:
        init_live_tables(conn)
        update_open_trade_exits(conn)
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
            signal = detect_latest_strict_triangle(symbol, profile, candles)
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
                    notes="LIVE_TRADING_ENABLED is not 1 or --dry-run was supplied",
                )
                record_trade(
                    conn,
                    signal,
                    notional_usdt=notional_usdt,
                    exchange_order_id=None,
                    status="DRY_RUN",
                    raw_order_response={"would_submit_order": order},
                    notes="dry-run only; no exchange order submitted",
                )
                console("BLOCKED", f"{signal.symbol} dry-run; order not sent", {"order": order})
                continue

            try:
                position_response = client.get_open_positions(mexc_contract_symbol(signal.symbol))
                console("POSITION_OPEN", f"{signal.symbol} exchange position precheck", position_response)
                if position_response.get("success") and position_response.get("data"):
                    reason = "exchange reports existing open position"
                    record_signal(conn, signal, decision="BLOCKED", block_reason=reason, dry_run=False, notional_usdt=notional_usdt, notes=json.dumps(position_response))
                    console("BLOCKED", f"{signal.symbol} {signal.pattern}", {"reason": reason})
                    continue
                raw_response = client.submit_order(order or {})
            except Exception as exc:
                raw_response = {"error": str(exc)}
                record_signal(conn, signal, decision="ORDER_FAILED", block_reason=str(exc), dry_run=False, notional_usdt=notional_usdt, notes="API/order error; fail closed")
                record_trade(conn, signal, notional_usdt=notional_usdt, exchange_order_id=None, status="ORDER_FAILED", raw_order_response=raw_response, notes="API/order error; fail closed")
                console("ORDER_FAILED", f"{signal.symbol} {signal.pattern}", raw_response)
                continue

            if not raw_response.get("success") or raw_response.get("code") not in (0, None):
                record_signal(conn, signal, decision="ORDER_FAILED", block_reason="MEXC returned unsuccessful order response", dry_run=False, notional_usdt=notional_usdt, notes=json.dumps(raw_response))
                record_trade(conn, signal, notional_usdt=notional_usdt, exchange_order_id=None, status="ORDER_FAILED", raw_order_response=raw_response, notes="MEXC returned unsuccessful order response")
                console("ORDER_FAILED", f"{signal.symbol} {signal.pattern}", raw_response)
                continue

            order_id = str(raw_response.get("data") or "") or None
            record_signal(conn, signal, decision="ORDER_SENT", block_reason=None, dry_run=False, notional_usdt=notional_usdt, notes=json.dumps(raw_response))
            record_trade(conn, signal, notional_usdt=notional_usdt, exchange_order_id=order_id, status="ORDER_SENT", raw_order_response=raw_response, notes="live MEXC futures order submitted")
            console("ORDER_SENT", f"{signal.symbol} {signal.pattern}", raw_response)
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forward-validate strict PnF triangles with guarded MEXC futures micro-orders")
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
