#!/usr/bin/env python3
"""Binance USD-M futures live micro-trading forward validator.

Standalone guarded forward validator for strict close-confirmed PnF triangle
signals.  It deliberately does not modify collectors, strategy logic, or the
MEXC validator.  Live exchange writes are fail-closed and require
``LIVE_TRADING_ENABLED=1`` plus Binance USD-M futures API credentials.
"""
from __future__ import annotations

import argparse
import csv
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
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent
PNF_MVP = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP) not in sys.path:
    sys.path.insert(0, str(PNF_MVP))

from pnf_engine import PnFEngine, PnFProfile  # noqa: E402
from patterns.poles import detect_pole_patterns  # noqa: E402

BINANCE_BASE_URL = "https://fapi.binance.com"
BINANCE_DEMO_BASE_URL = "https://demo-fapi.binance.com"
BINANCE_API_KEY_ENV = "BINANCE_FUTURES_API_KEY"
BINANCE_API_SECRET_ENV = "BINANCE_FUTURES_API_SECRET"
BINANCE_DEMO_API_KEY_ENV = "BINANCE_DEMO_FUTURES_API_KEY"
BINANCE_DEMO_API_SECRET_ENV = "BINANCE_DEMO_FUTURES_API_SECRET"
MEXC_FUTURES_BASE_URL = "https://contract.mexc.com"
MEXC_FUTURES_API_KEY_ENV = "MEXC_FUTURES_API_KEY"
MEXC_FUTURES_API_SECRET_ENV = "MEXC_FUTURES_API_SECRET"
MEXC_FUTURES_ALLOWED_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT", "ENAUSDT"}
MEXC_FUTURES_ALLOWED_VENUE_SYMBOLS = {f"MEXC_FUT:{symbol}" for symbol in MEXC_FUTURES_ALLOWED_SYMBOLS}
MEXC_FUTURES_INSPECT_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT", "ENAUSDT", "TAOUSDT", "HYPEUSDT")
MEXC_FUTURES_DEFAULT_LEVERAGE = Decimal("5")
MEXC_FUTURES_MAX_BANKROLL_USDT = Decimal("20")
MEXC_FUTURES_RISK_PER_TRADE_USDT = Decimal("0.20")
MEXC_FUTURES_MAX_OPEN_POSITIONS = 1

MEXC_DRY_RUN_SEED_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "SUIUSDT",
    "ENAUSDT",
    "TAOUSDT",
    "HYPEUSDT",
)
MEXC_DRY_RUN_SEED_REFERENCE_TS = 1_700_000_000
MEXC_DRY_RUN_SEED_CREATED_TS = 1_700_000_001
MAX_NOTIONAL_USDT = Decimal("1")
EXECUTION_INTENT_DEMO_MAX_NOTIONAL_USDT = Decimal("100")
DEFAULT_NOTIONAL_USDT = Decimal("1")
RECV_WINDOW_MS = 10000
BINANCE_SIGNED_TIMESTAMP_SAFETY_MARGIN_MS = 1500

ALLOWED_SYMBOLS = {"BINANCE_FUT:BTCUSDT", "BINANCE_FUT:ETHUSDT", "BINANCE_FUT:BNBUSDT", "BINANCE_FUT:SOLUSDT", "BINANCE_FUT:XRPUSDT"}
BINANCE_DEMO_SETUP_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "BINANCE_FUT:BTCUSDT",
    "BINANCE_FUT:ETHUSDT",
    "BINANCE_FUT:BNBUSDT",
    "BINANCE_FUT:SOLUSDT",
    "BINANCE_FUT:XRPUSDT",
}
UNSUPPORTED_EXECUTION_VENUE = "UNSUPPORTED_EXECUTION_VENUE"
EXECUTION_INTENT_STATUS_NEW = "NEW"
EXECUTION_INTENT_STATUS_READY = "READY"
EXECUTION_INTENT_STATUS_CANCELLED = "CANCELLED"
EXECUTION_INTENT_STATUSES = {EXECUTION_INTENT_STATUS_NEW, EXECUTION_INTENT_STATUS_READY, EXECUTION_INTENT_STATUS_CANCELLED}
ALLOWED_PATTERNS = {"bullish_triangle", "bearish_triangle"}
DEMO_DOUBLE_PATTERNS = {"double_top_breakout", "double_bottom_breakdown"}
DEMO_POLE_MOTIF_PATTERNS = {"pole_motif_low", "pole_motif_high"}
P2_SURVIVOR_STRATEGY_ID = "P2_SURVIVOR_V1"
P2_SURVIVOR_CANDIDATE_ID = "CAND-000053"
P2_SURVIVOR_PATTERN = "p2_survivor_v1"
P2_SURVIVOR_RELATIVE_POLE_SIZE = "NEAR_RECENT_AVG_0_75X_1_25X"
P2_SURVIVOR_REVERSAL_BOXES = "NORMAL_REVERSAL_4_6_BOXES"
P2_SURVIVOR_STATUS = "VALIDATED_RESEARCH_EDGE|FORWARD_EDGE_SURVIVES|NOT_PRODUCTION|NOT_LIVE_APPROVED"
RECENT_COLUMN_LOOKBACK = 6
CATAPULT_SIGNAL_NAMES = {"bullish_catapult", "bearish_catapult"}
OPEN_TRADE_STATUSES = {"OPEN", "ORDER_SENT", "POSITION_OPEN", "POSITION_OPEN_UNPROTECTED", "EXIT_PENDING"}
DUPLICATE_SETUP_COOLDOWN_HOURS = 12
DUPLICATE_SETUP_COOLDOWN_SECONDS = DUPLICATE_SETUP_COOLDOWN_HOURS * 60 * 60
DUPLICATE_SETUP_PRICE_TOLERANCE = Decimal("0.00000001")
RECONCILE_PRICE_MISMATCH_BPS = Decimal("1")
RECONCILE_MIN_PRICE_MISMATCH = Decimal("0.00000001")
POLE_MOTIF_ENTRY_MODEL = "NEXT_COLUMN_OPEN_ENTRY"
POLE_MOTIF_STOP_BOXES = Decimal("3")
POLE_MOTIF_TARGET_R = Decimal("2.5")
POLE_MOTIF_BREAK_EVEN_TRIGGER_R = Decimal("2")

TRADE_JOURNAL_COLUMNS = (
    "trade_id",
    "created_at",
    "symbol",
    "pattern",
    "strategy_id",
    "candidate_id",
    "side",
    "trigger_timestamp",
    "entry_price",
    "stop_price",
    "tp1_price",
    "tp2_price",
    "decision",
    "status",
    "block_reason",
    "exchange_order_id",
    "entry_order_status",
    "avg_fill_price",
    "executed_qty",
    "stop_algo_id",
    "tp_algo_id",
    "protective_orders_status",
    "exit_time",
    "exit_price",
    "realized_r",
    "fees",
    "notes",
)


@dataclass(frozen=True)
class Candle:
    close_time: int
    close: float
    high: float
    low: float
    open: float | None = None


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
class SetupExecutionIntent:
    setup_id: str
    symbol: str
    side: str
    entry: Decimal
    stop: Decimal
    tp1: Decimal
    tp2: Decimal
    rr1: Decimal | None
    rr2: Decimal | None
    reference_ts: int


@dataclass(frozen=True)
class PoleMotifSignal:
    symbol: str
    direction: str
    entry: Decimal
    stop: Decimal
    target: Decimal
    be_trigger: Decimal
    trigger_ts: int
    pattern_name: str
    pole_column_index: int
    reversal_column_index: int
    confirmation_column_index: int
    setup_key: str

    def to_triangle_signal(self) -> "TriangleSignal":
        return TriangleSignal(
            symbol=self.symbol,
            pattern="pole_motif_low" if self.pattern_name == "LOW_POLE" else "pole_motif_high",
            side=self.direction,
            trigger_ts=self.trigger_ts,
            entry_price=self.entry,
            stop_price=self.stop,
            tp1_price=self.be_trigger,
            tp2_price=self.target,
            trigger_column_idx=self.confirmation_column_index,
            support_level=min(self.entry, self.stop),
            resistance_level=max(self.entry, self.stop),
            break_distance_boxes=POLE_MOTIF_STOP_BOXES,
            pattern_quality=f"POLE_MOTIF_DEMO_FORWARD|{POLE_MOTIF_ENTRY_MODEL}|setup={self.setup_key}",
        )


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
        self._time_offset_ms = 0
        self._last_server_time_ms: int | None = None

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def get_server_time(self) -> int:
        data = self._request_json("GET", "/fapi/v1/time")
        try:
            return int(data["serverTime"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"Binance server-time response missing serverTime: {data!r}") from exc

    def sync_server_time(self) -> int:
        local_time = int(time.time() * 1000)
        server_time = self.get_server_time()
        self._time_offset_ms = server_time - local_time
        self._last_server_time_ms = server_time
        console(
            "BINANCE_TIME_SYNC",
            "",
            {"server_time": server_time, "local_time": local_time, "offset_ms": self._time_offset_ms},
        )
        return self._time_offset_ms

    def _current_signed_timestamp_ms(self) -> int:
        if self._last_server_time_ms is not None:
            return self._last_server_time_ms - BINANCE_SIGNED_TIMESTAMP_SAFETY_MARGIN_MS
        return int((time.time() * 1000) + self._time_offset_ms - BINANCE_SIGNED_TIMESTAMP_SAFETY_MARGIN_MS)

    @staticmethod
    def _is_timestamp_ahead_error(raw: str) -> bool:
        payload = raw[raw.find("{") :] if "{" in raw else raw
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return False
        return data.get("code") == -1021

    def _signed_params(self, params: dict[str, Any] | None = None, *, timestamp: int | None = None) -> dict[str, Any]:
        if not self.has_credentials:
            raise RuntimeError("missing Binance USD-M futures API credentials")
        signed: dict[str, Any] = {k: v for k, v in (params or {}).items() if v is not None}
        signed.setdefault("recvWindow", RECV_WINDOW_MS)
        if timestamp is None:
            self.sync_server_time()
        signed["timestamp"] = int(timestamp if timestamp is not None else self._current_signed_timestamp_ms())
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

        def send_once() -> Any:
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
                return json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Binance non-JSON response: {raw[:500]}") from exc

        try:
            return send_once()
        except RuntimeError as original_exc:
            cause = original_exc.__cause__
            if not signed or not isinstance(cause, urllib.error.HTTPError):
                raise
            raw = getattr(original_exc, "args", [""])[0]
            if not self._is_timestamp_ahead_error(str(raw)):
                raise
            console("BINANCE_SIGNED_RETRY_TIMESTAMP", "", {"method": method, "path": path})
            try:
                return send_once()
            except RuntimeError as retry_exc:
                raise original_exc from retry_exc

    def get_exchange_info(self) -> dict[str, Any]:
        return self._request_json("GET", "/fapi/v1/exchangeInfo")

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        return parse_symbol_spec(self.get_exchange_info(), symbol)

    def get_position_mode(self) -> dict[str, Any]:
        return self._request_json("GET", "/fapi/v1/positionSide/dual", signed=True)

    def get_position_risk(self, symbol: str) -> Any:
        return self._request_json("GET", "/fapi/v3/positionRisk", params={"symbol": symbol}, signed=True)

    def get_mark_price(self, symbol: str) -> Decimal:
        data = self._request_json("GET", "/fapi/v1/premiumIndex", params={"symbol": symbol})
        try:
            return _dec(data["markPrice"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"Binance mark-price response missing markPrice: {data!r}") from exc

    def submit_order(self, order: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("POST", "/fapi/v1/order", params=order, signed=True)

    def submit_algo_order(self, order: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("POST", "/fapi/v1/algoOrder", params=order, signed=True)

    def get_order(self, symbol: str, *, order_id: str | None = None, client_order_id: str | None = None) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/fapi/v1/order",
            params={"symbol": symbol, "orderId": order_id, "origClientOrderId": client_order_id},
            signed=True,
        )

    def cancel_order(
        self,
        symbol: str,
        *,
        order_id: str | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        if not symbol:
            raise ValueError("symbol is required to cancel a Binance USD-M futures order")
        if order_id in (None, "") and orig_client_order_id in (None, ""):
            raise ValueError("order_id or orig_client_order_id is required to cancel a Binance USD-M futures order")
        return self._request_json(
            "DELETE",
            "/fapi/v1/order",
            params={"symbol": symbol, "orderId": order_id, "origClientOrderId": orig_client_order_id},
            signed=True,
        )

    def get_user_trades(
        self,
        symbol: str,
        *,
        order_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._request_json(
            "GET",
            "/fapi/v1/userTrades",
            params={"symbol": symbol, "orderId": order_id, "startTime": start_time, "endTime": end_time, "limit": limit},
            signed=True,
        )

    def get_algo_order(self, symbol: str, *, algo_id: str | None = None, client_algo_id: str | None = None) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/fapi/v1/algoOrder",
            params={"symbol": symbol, "algoId": algo_id, "clientAlgoId": client_algo_id},
            signed=True,
        )


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


def sqlite_readonly_uri(db_path: str | os.PathLike[str]) -> str:
    return Path(db_path).expanduser().absolute().as_uri() + "?mode=ro"


def connect_strategy_setups_db_readonly(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    conn = sqlite3.connect(sqlite_readonly_uri(db_path), uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_executable_strategy_setups(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            setup_id, symbol, side, ideal_entry, invalidation, tp1, tp2, rr1, rr2,
            reference_ts, status, breakout_context, pullback_quality,
            active_leg_boxes, is_extended_move, resolution_status
        FROM strategy_setups
        WHERE status = 'CANDIDATE'
          AND side = 'LONG'
          AND breakout_context = 'POST_BREAKOUT_PULLBACK'
          AND pullback_quality = 'HEALTHY'
          AND active_leg_boxes = 2
          AND COALESCE(is_extended_move, 0) = 0
          AND resolution_status = 'PENDING'
        ORDER BY reference_ts ASC, symbol ASC, setup_id ASC
        """
    ).fetchall()


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return _dec(value)


def setup_row_to_execution_intent(row: sqlite3.Row | dict[str, Any]) -> tuple[SetupExecutionIntent | None, str | None]:
    raw = dict(row)
    setup_id = str(raw.get("setup_id") or "").strip()
    if not setup_id:
        return None, "missing setup_id"
    symbol = str(raw.get("symbol") or "").strip()
    if not symbol:
        return None, "missing symbol"
    side = str(raw.get("side") or "").upper()
    if side != "LONG":
        return None, "unsupported side"
    required_prices = {
        "entry": raw.get("ideal_entry"),
        "stop": raw.get("invalidation"),
        "tp1": raw.get("tp1"),
        "tp2": raw.get("tp2"),
    }
    missing = [name for name, value in required_prices.items() if value in (None, "")]
    if missing:
        return None, f"missing required price fields: {','.join(missing)}"
    try:
        entry = _dec(raw.get("ideal_entry"))
        stop = _dec(raw.get("invalidation"))
        tp1 = _dec(raw.get("tp1"))
        tp2 = _dec(raw.get("tp2"))
        rr1 = _optional_decimal(raw.get("rr1"))
        rr2 = _optional_decimal(raw.get("rr2"))
        reference_ts = int(raw.get("reference_ts"))
    except (TypeError, ValueError, InvalidOperation) as exc:
        return None, f"invalid setup execution field: {exc}"
    if not (stop < entry < tp1 < tp2):
        return None, "invalid LONG risk levels: expected stop < entry < tp1 < tp2"
    return (
        SetupExecutionIntent(
            setup_id=setup_id,
            symbol=symbol,
            side=side,
            entry=entry,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            rr1=rr1,
            rr2=rr2,
            reference_ts=reference_ts,
        ),
        None,
    )


def setup_execution_payload(intent: SetupExecutionIntent) -> dict[str, Any]:
    return {
        "setup_id": intent.setup_id,
        "symbol": intent.symbol,
        "side": intent.side,
        "entry": str(intent.entry),
        "stop": str(intent.stop),
        "tp1": str(intent.tp1),
        "tp2": str(intent.tp2),
        "rr1": str(intent.rr1) if intent.rr1 is not None else None,
        "rr2": str(intent.rr2) if intent.rr2 is not None else None,
        "reference_ts": intent.reference_ts,
    }


RESEARCH_RULE_REQUIRED_FIELDS = (
    "symbol",
    "side",
    "status",
    "breakout_context",
    "pullback_quality",
    "trend_regime",
    "continuation_execution_class",
    "entry_distance_bucket",
    "active_leg_boxes",
    "quality_score",
)


def load_research_rule(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("research rule JSON must be an object")
    return payload


def _rule_match_text(field: str, setup: dict[str, Any], rule: dict[str, Any]) -> str | None:
    expected = rule.get(field)
    if expected is None:
        return None
    actual = str(setup.get(field) or "").upper()
    expected_upper = str(expected).upper()
    if actual != expected_upper:
        return f"{field} mismatch: actual={actual or 'MISSING'} expected={expected_upper}"
    return None


def _rule_match_range(field: str, setup: dict[str, Any], rule: dict[str, Any]) -> str | None:
    if field not in rule:
        return None
    bounds = rule.get(field)
    if not isinstance(bounds, dict):
        return f"{field} rule range invalid"
    minimum = bounds.get("min")
    maximum = bounds.get("max")
    raw = setup.get(field)
    if raw is None:
        return f"{field} missing"
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return f"{field} invalid: {raw}"
    if minimum is not None and value < Decimal(str(minimum)):
        return f"{field} below min: {value}<{minimum}"
    if maximum is not None and value > Decimal(str(maximum)):
        return f"{field} above max: {value}>{maximum}"
    return None


def _normalize_range_rule(rule: dict[str, Any], field: str) -> dict[str, Any] | None:
    direct = rule.get(field)
    if isinstance(direct, dict):
        return direct
    numeric_thresholds = rule.get("numeric_thresholds")
    if isinstance(numeric_thresholds, dict):
        numeric_rule = numeric_thresholds.get(field)
        if isinstance(numeric_rule, dict):
            return numeric_rule
    integer_filters = rule.get("integer_filters")
    if isinstance(integer_filters, dict):
        integer_rule = integer_filters.get(field)
        if isinstance(integer_rule, dict):
            if "allowed" in integer_rule:
                allowed = integer_rule.get("allowed")
                if isinstance(allowed, list) and allowed:
                    try:
                        allowed_dec = [Decimal(str(item)) for item in allowed]
                    except (InvalidOperation, ValueError):
                        return {"__invalid__": True}
                    return {"min": min(allowed_dec), "max": max(allowed_dec), "allowed_exact": allowed_dec}
            return integer_rule
    return None


def evaluate_research_rule(setup: dict[str, Any], rule: dict[str, Any]) -> tuple[bool, str | None]:
    for field in RESEARCH_RULE_REQUIRED_FIELDS:
        if setup.get(field) is None:
            return False, f"missing required setup field: {field}"
    for field in ("symbol", "side", "status", "breakout_context", "pullback_quality", "trend_regime", "continuation_execution_class", "entry_distance_bucket"):
        reason = _rule_match_text(field, setup, rule)
        if reason is not None:
            return False, reason
    for field in ("active_leg_boxes", "quality_score"):
        normalized = _normalize_range_rule(rule, field)
        if normalized is None:
            continue
        if normalized.get("__invalid__"):
            return False, f"{field} rule range invalid"
        reason = _rule_match_range(field, setup, {field: normalized})
        if reason is not None:
            return False, reason
        allowed_exact = normalized.get("allowed_exact")
        if isinstance(allowed_exact, list):
            value = Decimal(str(setup.get(field)))
            if value not in allowed_exact:
                return False, f"{field} not in allowed list: {value}"
    return True, None


def connect_candle_db_readonly(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    conn = sqlite3.connect(sqlite_readonly_uri(db_path), uri=True)
    conn.execute("PRAGMA query_only = ON")
    return conn


def connect_state_db_readonly(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    conn = sqlite3.connect(sqlite_readonly_uri(db_path), uri=True)
    conn.execute("PRAGMA query_only = ON")
    conn.row_factory = sqlite3.Row
    return conn


def resolve_state_db_path(args: argparse.Namespace) -> str:
    return str(getattr(args, "state_db_path", None) or args.db_path)


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


def pole_motif_staleness_diagnostics(
    signal: TriangleSignal,
    *,
    current_price: Decimal | float | None,
    latest_candle_close_time: int | None,
) -> dict[str, Any]:
    if not is_demo_pole_motif_signal(signal):
        return {}

    current_price_dec = _dec(current_price) if current_price is not None else None
    entry_distance_percent = None
    if current_price_dec is not None and current_price_dec != 0:
        entry_distance_percent = ((signal.entry_price - current_price_dec) / current_price_dec) * Decimal("100")

    trigger_age_seconds = None
    if latest_candle_close_time is not None:
        trigger_age_seconds = (Decimal(int(latest_candle_close_time)) - Decimal(int(signal.trigger_ts))) / Decimal("1000")

    return {
        "CURRENT_PRICE": current_price_dec,
        "ENTRY_PRICE": signal.entry_price,
        "ENTRY_DISTANCE_PERCENT": entry_distance_percent,
        "TRIGGER_TIMESTAMP": signal.trigger_ts,
        "LATEST_CANDLE_CLOSE_TIME": latest_candle_close_time,
        "TRIGGER_AGE_SECONDS": trigger_age_seconds,
        "ENTRY_MODEL": POLE_MOTIF_ENTRY_MODEL,
    }


def log_signal_detail(
    signal: TriangleSignal,
    profile: PnFProfile,
    last_close: float | None,
    *,
    latest_candle_close_time: int | None = None,
) -> None:
    payload = {
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
    if is_p2_survivor_signal(signal):
        payload.update({"strategy_id": P2_SURVIVOR_STRATEGY_ID, "candidate_id": P2_SURVIVOR_CANDIDATE_ID})
    payload.update(
        pole_motif_staleness_diagnostics(
            signal,
            current_price=last_close,
            latest_candle_close_time=latest_candle_close_time,
        )
    )
    console(
        "SIGNAL_DETAIL",
        signal.symbol,
        compact_visibility_payload(payload),
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
            "break_even_armed": "INTEGER NOT NULL DEFAULT 0",
            "break_even_trigger_price": "REAL",
            "active_stop_price": "REAL",
            "strategy_id": "TEXT",
            "candidate_id": "TEXT",
            "stop_algo_id": "TEXT",
            "tp_algo_id": "TEXT",
            "protective_orders_status": "TEXT",
            "protective_orders_raw_response": "TEXT",
            "protective_orders_error": "TEXT",
            "exit_ts": "INTEGER",
            "realized_pnl": "REAL",
            "realized_pnl_pct": "REAL",
            "close_reason": "TEXT",
            "setup_id": "TEXT",
            "intent_id": "TEXT",
        },
    )
    ensure_columns(
        conn,
        "live_signals_binance",
        {
            "strategy_id": "TEXT",
            "candidate_id": "TEXT",
        },
    )
    conn.commit()



def init_execution_intents_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_intents (
            intent_id TEXT PRIMARY KEY,
            setup_id TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry TEXT NOT NULL,
            stop TEXT NOT NULL,
            tp1 TEXT NOT NULL,
            tp2 TEXT NOT NULL,
            rr1 TEXT,
            rr2 TEXT,
            reference_ts INTEGER NOT NULL,
            created_ts INTEGER NOT NULL,
            intent_status TEXT NOT NULL CHECK(intent_status IN ('NEW', 'READY', 'CANCELLED'))
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_intents_status_created
        ON execution_intents(intent_status, created_ts)
        """
    )
    conn.commit()



EXECUTION_INTENT_INSPECT_COLUMNS = (
    "intent_id",
    "setup_id",
    "symbol",
    "side",
    "entry",
    "stop",
    "tp1",
    "tp2",
    "rr1",
    "rr2",
    "reference_ts",
    "created_ts",
    "intent_status",
)


def execution_intents_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'execution_intents'"
    ).fetchone()
    return row is not None


def inspect_execution_intents_once(args: argparse.Namespace) -> None:
    state_db_path = resolve_state_db_path(args)
    try:
        conn = connect_state_db_readonly(state_db_path)
    except sqlite3.OperationalError:
        print("INTENTS_TOTAL 0")
        print("INTENTS_NEW 0")
        print("INTENTS_READY 0")
        print("INTENTS_CANCELLED 0")
        return

    with closing(conn):
        if not execution_intents_table_exists(conn):
            print("INTENTS_TOTAL 0")
            print("INTENTS_NEW 0")
            print("INTENTS_READY 0")
            print("INTENTS_CANCELLED 0")
            return

        status_counts = {status: 0 for status in EXECUTION_INTENT_STATUSES}
        rows = conn.execute(
            f"""
            SELECT {", ".join(EXECUTION_INTENT_INSPECT_COLUMNS)}
            FROM execution_intents
            ORDER BY created_ts, intent_id
            """
        ).fetchall()
        for row in rows:
            status = row["intent_status"]
            if status in status_counts:
                status_counts[status] += 1

        print(f"INTENTS_TOTAL {len(rows)}")
        print(f"INTENTS_NEW {status_counts[EXECUTION_INTENT_STATUS_NEW]}")
        print(f"INTENTS_READY {status_counts[EXECUTION_INTENT_STATUS_READY]}")
        print(f"INTENTS_CANCELLED {status_counts[EXECUTION_INTENT_STATUS_CANCELLED]}")
        for row in rows:
            payload = {column: row[column] for column in EXECUTION_INTENT_INSPECT_COLUMNS}
            print(f"INTENT_ROW {json.dumps(payload, sort_keys=True)}")

def execution_intent_id(setup_id: str) -> str:
    digest = hashlib.sha256(setup_id.encode("utf-8")).hexdigest()[:16]
    return f"intent-{digest}"


def mexc_dry_run_seed_intents() -> list[SetupExecutionIntent]:
    return [
        SetupExecutionIntent(
            setup_id=f"test-mexc-dry-run-{symbol.lower()}",
            symbol=f"MEXC_FUT:{symbol}",
            side="LONG",
            entry=Decimal("100"),
            stop=Decimal("99"),
            tp1=Decimal("102"),
            tp2=Decimal("103"),
            rr1=Decimal("2"),
            rr2=Decimal("3"),
            reference_ts=MEXC_DRY_RUN_SEED_REFERENCE_TS + idx,
        )
        for idx, symbol in enumerate(MEXC_DRY_RUN_SEED_SYMBOLS)
    ]


def seed_mexc_dry_run_intents_once(args: argparse.Namespace) -> None:
    if not (bool(getattr(args, "seed_mexc_dry_run_intents", False)) and bool(getattr(args, "allow_test_seed", False))):
        raise SystemExit("--seed-mexc-dry-run-intents requires --allow-test-seed")

    state_db_path = resolve_state_db_path(args)
    seeded = 0
    with closing(sqlite3.connect(state_db_path)) as conn:
        init_execution_intents_table(conn)
        for intent in mexc_dry_run_seed_intents():
            intent_id = execution_intent_id(intent.setup_id)
            if create_execution_intent(conn, intent, created_ts=MEXC_DRY_RUN_SEED_CREATED_TS):
                seeded += 1
                console(
                    "MEXC_DRY_RUN_INTENT_SEEDED",
                    "",
                    {"intent_id": intent_id, "setup_id": intent.setup_id, "symbol": intent.symbol, "intent_status": EXECUTION_INTENT_STATUS_NEW},
                )
            else:
                console(
                    "MEXC_DRY_RUN_INTENT_ALREADY_EXISTS",
                    "",
                    {"intent_id": intent_id, "setup_id": intent.setup_id, "symbol": intent.symbol},
                )
    console("MEXC_DRY_RUN_INTENTS_SEEDED", str(seeded))


def create_execution_intent(
    conn: sqlite3.Connection,
    intent: SetupExecutionIntent,
    *,
    created_ts: int | None = None,
) -> bool:
    observed_ts = int(time.time() if created_ts is None else created_ts)
    try:
        conn.execute(
            """
            INSERT INTO execution_intents(
                intent_id, setup_id, symbol, side, entry, stop, tp1, tp2, rr1, rr2,
                reference_ts, created_ts, intent_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                execution_intent_id(intent.setup_id),
                intent.setup_id,
                intent.symbol,
                intent.side,
                str(intent.entry),
                str(intent.stop),
                str(intent.tp1),
                str(intent.tp2),
                str(intent.rr1) if intent.rr1 is not None else None,
                str(intent.rr2) if intent.rr2 is not None else None,
                intent.reference_ts,
                observed_ts,
                EXECUTION_INTENT_STATUS_NEW,
            ),
        )
    except sqlite3.IntegrityError:
        conn.rollback()
        return False
    conn.commit()
    return True


def execution_intent_to_signal(row: sqlite3.Row | dict[str, Any]) -> TriangleSignal:
    raw = dict(row)
    return TriangleSignal(
        symbol=str(raw["symbol"]),
        pattern="execution_intent",
        side=str(raw["side"]).upper(),
        trigger_ts=int(raw["reference_ts"]),
        entry_price=_dec(raw["entry"]),
        stop_price=_dec(raw["stop"]),
        tp1_price=_dec(raw["tp1"]),
        tp2_price=_dec(raw["tp2"]),
        trigger_column_idx=0,
        support_level=_dec(raw["stop"]),
        resistance_level=_dec(raw["entry"]),
        break_distance_boxes=Decimal("0"),
        pattern_quality=f"EXECUTION_INTENT|setup_id={raw['setup_id']}|intent_id={raw['intent_id']}",
    )


def load_execution_intents(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        f"""
        SELECT {", ".join(EXECUTION_INTENT_INSPECT_COLUMNS)}
        FROM execution_intents
        ORDER BY created_ts, intent_id
        """
    ).fetchall()


def setup_already_executed(conn: sqlite3.Connection, setup_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM live_trades_binance WHERE setup_id = ? AND dry_run = 0 LIMIT 1",
        (setup_id,),
    ).fetchone()
    return row is not None


def reject_execution_intent(row: sqlite3.Row | dict[str, Any], reason: str) -> None:
    raw = dict(row)
    console(
        "EXECUTION_INTENT_REJECTED",
        "",
        {"intent_id": raw.get("intent_id"), "setup_id": raw.get("setup_id"), "symbol": raw.get("symbol"), "reason": reason},
    )


def record_execution_intent_trade(
    conn: sqlite3.Connection,
    signal: TriangleSignal,
    *,
    intent_id: str,
    setup_id: str,
    notional_usdt: Decimal,
    order: dict[str, Any],
    response: dict[str, Any],
) -> None:
    order_id, client_order_id = extract_order_ids(order, response)
    exchange_order_id = order_id or client_order_id
    conn.execute(
        """
        INSERT INTO live_trades_binance(
            created_at, symbol, pattern, strategy_id, candidate_id, side, trigger_timestamp, entry_price,
            stop_price, tp1_price, tp2_price, notional_usdt, decision, status,
            block_reason, dry_run, exchange_order_id, raw_order_response, notes,
            break_even_armed, break_even_trigger_price, active_stop_price,
            entry_order_status, protective_orders_status, setup_id, intent_id
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_iso(),
            signal.symbol,
            signal.pattern,
            None,
            setup_id,
            signal.side,
            signal.trigger_ts,
            float(signal.entry_price),
            float(signal.stop_price),
            float(signal.tp1_price),
            float(signal.tp2_price),
            float(notional_usdt),
            "ORDER_SENT",
            "ORDER_SENT",
            None,
            0,
            exchange_order_id,
            json.dumps({"order_request": order, "order_response": response}, sort_keys=True, default=str),
            "execution intent demo entry order submitted; protective orders pending entry fill/future lifecycle",
            0,
            None,
            float(signal.stop_price),
            str(response.get("status") or "NEW"),
            "PENDING_ENTRY_FILL",
            setup_id,
            intent_id,
        ),
    )
    conn.execute(
        "UPDATE execution_intents SET intent_status = ? WHERE intent_id = ? AND intent_status = ?",
        (EXECUTION_INTENT_STATUS_READY, intent_id, EXECUTION_INTENT_STATUS_NEW),
    )
    conn.commit()


def init_executed_setup_candidates_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS executed_setup_candidates (
            setup_id TEXT PRIMARY KEY,
            first_seen_ts INTEGER NOT NULL,
            last_seen_ts INTEGER NOT NULL
        )
        """
    )
    conn.commit()


def mark_setup_candidate_seen(conn: sqlite3.Connection, setup_id: str, *, seen_ts: int | None = None) -> bool:
    observed_ts = int(time.time() if seen_ts is None else seen_ts)
    existing = conn.execute(
        "SELECT setup_id FROM executed_setup_candidates WHERE setup_id = ?",
        (setup_id,),
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO executed_setup_candidates(setup_id, first_seen_ts, last_seen_ts) VALUES (?, ?, ?)",
            (setup_id, observed_ts, observed_ts),
        )
        conn.commit()
        return True
    conn.execute(
        "UPDATE executed_setup_candidates SET last_seen_ts = ? WHERE setup_id = ?",
        (observed_ts, setup_id),
    )
    conn.commit()
    return False


def is_setup_symbol_supported_for_execution(symbol: str, execution_venue: str | None) -> bool:
    if execution_venue == "BINANCE_DEMO":
        return symbol in BINANCE_DEMO_SETUP_SYMBOLS
    if execution_venue == "MEXC_FUT":
        return symbol in MEXC_FUTURES_ALLOWED_SYMBOLS or symbol in MEXC_FUTURES_ALLOWED_VENUE_SYMBOLS
    return True


@dataclass(frozen=True)
class MexcFuturesOrderPlan:
    symbol: str
    entry: Decimal
    stop: Decimal
    tp1: Decimal
    tp2: Decimal
    quantity: Decimal
    risk_usdt: Decimal
    notional_usdt: Decimal
    leverage: Decimal


class MexcFuturesExecutionClient:
    """Fail-closed MEXC Futures client skeleton for the execution-intent path."""

    def __init__(self, api_key: str | None, api_secret: str | None, *, base_url: str = MEXC_FUTURES_BASE_URL) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def get_open_positions(self) -> list[dict[str, Any]]:
        raise RuntimeError("MEXC live position sync is not enabled in Phase A dry-run mode")

    def submit_order(self, order: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("MEXC live order submission requires explicit Phase B implementation")


class MexcFuturesPublicClient:
    """Read-only public MEXC Futures market-data client."""

    def __init__(self, *, base_url: str = MEXC_FUTURES_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")

    def _request_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"MEXC HTTP {exc.code}: {raw}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"MEXC request failed: {exc}") from exc
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"MEXC non-JSON response: {raw[:500]}") from exc

    def get_contract_details(self) -> Any:
        return self._request_json("/api/v1/contract/detail")


def normalize_mexc_futures_symbol(symbol: str) -> str:
    return symbol.split(":", 1)[-1].upper()


def is_mexc_futures_symbol_allowed(symbol: str) -> bool:
    return normalize_mexc_futures_symbol(symbol) in MEXC_FUTURES_ALLOWED_SYMBOLS


def _first_present(row: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in row and row.get(name) not in (None, ""):
            return row.get(name)
    return None


def _mexc_display_symbol(value: Any) -> str:
    return str(value or "").replace("_", "").upper()


def parse_mexc_contract_specs(response: Any, target_symbols: tuple[str, ...] = MEXC_FUTURES_INSPECT_SYMBOLS) -> list[dict[str, Any]]:
    if not isinstance(response, dict):
        raise RuntimeError(f"MEXC contract detail response must be an object: {response!r}")
    data = response.get("data")
    if isinstance(data, dict):
        rows = [data]
    elif isinstance(data, list):
        rows = data
    else:
        raise RuntimeError(f"MEXC contract detail response missing data list/object: {response!r}")
    by_symbol = {_mexc_display_symbol(row.get("symbol")): row for row in rows if isinstance(row, dict)}
    specs: list[dict[str, Any]] = []
    for symbol in target_symbols:
        row = by_symbol.get(symbol)
        supported = symbol in MEXC_FUTURES_ALLOWED_SYMBOLS and row is not None
        if row is None:
            specs.append({"symbol": symbol, "supported": False, "unsupported_reason": "MISSING_FROM_EXCHANGE_RESPONSE"})
            continue
        specs.append(
            {
                "symbol": symbol,
                "exchange_symbol": row.get("symbol"),
                "base_asset": _first_present(row, ("baseCoin", "baseAsset", "base_currency", "baseCurrency")),
                "quote_asset": _first_present(row, ("quoteCoin", "quoteAsset", "quote_currency", "quoteCurrency", "settleCoin")),
                "tick_size": _first_present(row, ("priceUnit", "tickSize", "priceTick")),
                "price_precision": _first_present(row, ("priceScale", "pricePrecision")),
                "quantity_precision": _first_present(row, ("volScale", "quantityPrecision", "volumePrecision")),
                "minimum_quantity": _first_present(row, ("minVol", "minQty", "minVolume")),
                "minimum_notional": _first_present(row, ("minNotional", "minNominalValue", "minNominal")),
                "contract_size": _first_present(row, ("contractSize", "contractUnit", "multiplier")),
                "max_leverage": _first_present(row, ("maxLeverage", "max_leverage")),
                "supported_order_types": _first_present(row, ("orderTypes", "supportOrderTypes")),
                "status": _first_present(row, ("state", "status", "contractStatus")),
                "supported": supported,
                "unsupported_reason": None if supported else "NOT_ENABLED_FOR_MEXC_EXECUTION",
            }
        )
    return specs


def inspect_mexc_contracts_once(args: argparse.Namespace) -> None:
    response = MexcFuturesPublicClient(base_url=getattr(args, "mexc_futures_base_url", MEXC_FUTURES_BASE_URL)).get_contract_details()
    specs = parse_mexc_contract_specs(response)
    found = sum(1 for spec in specs if spec.get("unsupported_reason") != "MISSING_FROM_EXCHANGE_RESPONSE")
    supported = sum(1 for spec in specs if spec.get("supported") is True)
    unsupported = len(specs) - supported
    for spec in specs:
        print(f"MEXC_CONTRACT_SPEC {json.dumps(spec, sort_keys=True, default=str)}", flush=True)
    print(f"MEXC_CONTRACTS_FOUND {found}", flush=True)
    print(f"MEXC_CONTRACTS_SUPPORTED {supported}", flush=True)
    print(f"MEXC_CONTRACTS_UNSUPPORTED {unsupported}", flush=True)


def count_open_mexc_execution_trades(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM live_trades_binance
        WHERE pattern = 'execution_intent'
          AND symbol LIKE 'MEXC_FUT:%'
          AND status IN ('ORDER_SENT','POSITION_OPEN','POSITION_OPEN_UNPROTECTED','EXIT_PENDING')
        """
    ).fetchone()
    return int(row[0] if row is not None else 0)


def mexc_setup_already_executed(conn: sqlite3.Connection, setup_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM live_trades_binance WHERE setup_id = ? AND symbol LIKE 'MEXC_FUT:%' LIMIT 1",
        (setup_id,),
    ).fetchone()
    return row is not None


def calculate_mexc_futures_position_size(signal: TriangleSignal) -> tuple[MexcFuturesOrderPlan | None, str | None]:
    risk_reason = validate_risk_levels(signal)
    if risk_reason is not None:
        return None, risk_reason
    if signal.side != "LONG":
        return None, "SHORT execution intents are blocked"
    stop_distance = signal.entry_price - signal.stop_price
    if stop_distance <= 0:
        return None, "invalid stop distance"
    risk_quantity = MEXC_FUTURES_RISK_PER_TRADE_USDT / stop_distance
    max_notional = MEXC_FUTURES_MAX_BANKROLL_USDT * MEXC_FUTURES_DEFAULT_LEVERAGE
    max_quantity = max_notional / signal.entry_price
    quantity = min(risk_quantity, max_quantity)
    notional = quantity * signal.entry_price
    risk_usdt = quantity * stop_distance
    if quantity <= 0 or notional <= 0:
        return None, "calculated quantity is non-positive"
    if notional > max_notional:
        return None, "5x leverage cap exceeded"
    if risk_usdt > MEXC_FUTURES_RISK_PER_TRADE_USDT:
        return None, "0.20 USDT risk cap exceeded"
    return (
        MexcFuturesOrderPlan(
            symbol=f"MEXC_FUT:{normalize_mexc_futures_symbol(signal.symbol)}",
            entry=signal.entry_price,
            stop=signal.stop_price,
            tp1=signal.tp1_price,
            tp2=signal.tp2_price,
            quantity=quantity,
            risk_usdt=risk_usdt,
            notional_usdt=notional,
            leverage=MEXC_FUTURES_DEFAULT_LEVERAGE,
        ),
        None,
    )


def mexc_order_from_plan(row: sqlite3.Row | dict[str, Any], plan: MexcFuturesOrderPlan) -> dict[str, Any]:
    return {
        "venue": "MEXC_FUT",
        "symbol": normalize_mexc_futures_symbol(plan.symbol),
        "side": "BUY",
        "positionSide": "LONG",
        "type": "LIMIT",
        "entry": str(plan.entry),
        "stop": str(plan.stop),
        "tp1": str(plan.tp1),
        "tp2": str(plan.tp2),
        "quantity": str(plan.quantity),
        "notional_usdt": str(plan.notional_usdt),
        "risk_usdt": str(plan.risk_usdt),
        "leverage": str(plan.leverage),
        "clientOrderId": f"pnf-mexc-{str(row['intent_id']).replace('intent-', '')}"[:32],
    }

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


def log_candle_query(runtime_symbol: str, primary_query_symbol: str, fallback_query_symbol: str | None = None) -> None:
    console(
        "CANDLE_QUERY",
        "",
        {
            "runtime_symbol": runtime_symbol,
            "normalized_symbol": binance_symbol(runtime_symbol),
            "primary_query_symbol": primary_query_symbol,
            "fallback_query_symbol": fallback_query_symbol,
            "table": "candles",
            "interval": "1m",
        },
    )


def log_candle_result(symbol: str, candles: list[Candle], *, runtime_symbol: str | None = None) -> None:
    first_candle = candles[0] if candles else None
    last_candle = candles[-1] if candles else None
    console(
        "CANDLE_RESULT",
        "",
        {
            "runtime_symbol": runtime_symbol,
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
    target_symbols = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}
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


def candle_table_has_open(conn: sqlite3.Connection) -> bool:
    return "open" in {str(row[1]) for row in conn.execute("PRAGMA table_info(candles)").fetchall()}


def load_candles(conn: sqlite3.Connection, symbol: str, limit: int) -> list[Candle]:
    primary_query_symbol = symbol
    fallback_query_symbol = binance_symbol(symbol)
    use_fallback = primary_query_symbol != fallback_query_symbol
    log_candle_query(symbol, primary_query_symbol, fallback_query_symbol if use_fallback else None)
    has_open = candle_table_has_open(conn)
    select_fields = "close_time, close, high, low, open" if has_open else "close_time, close, high, low"
    rows = conn.execute(
        f"""
        SELECT {select_fields}
        FROM candles
        WHERE symbol = ? AND interval = '1m'
        ORDER BY close_time DESC
        LIMIT ?
        """,
        (primary_query_symbol, limit),
    ).fetchall()
    query_used = primary_query_symbol
    if not rows and use_fallback:
        rows = conn.execute(
            f"""
            SELECT {select_fields}
            FROM candles
            WHERE symbol = ? AND interval = '1m'
            ORDER BY close_time DESC
            LIMIT ?
            """,
            (fallback_query_symbol, limit),
        ).fetchall()
        query_used = fallback_query_symbol
    candles = [
        Candle(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]) if len(row) > 4 and row[4] is not None else None)
        for row in reversed(rows)
    ]
    log_candle_result(query_used, candles, runtime_symbol=symbol)
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
    if box_size <= 0:
        return None

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
    if box_size <= 0:
        return None

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


def pole_direction_for_pattern(pattern_name: str) -> str:
    pattern = str(pattern_name).upper()
    if pattern == "LOW_POLE":
        return "LONG"
    if pattern == "HIGH_POLE":
        return "SHORT"
    raise ValueError(f"unsupported pole motif pattern: {pattern_name}")


def pole_motif_setup_key(
    symbol: str,
    profile: PnFProfile,
    pattern_name: str,
    pole_idx: int,
    reversal_idx: int,
    confirmation_idx: int,
) -> str:
    return (
        f"{symbol}|{profile.name}|{pattern_name}|{pole_idx}|{reversal_idx}|{confirmation_idx}|"
        f"{POLE_MOTIF_ENTRY_MODEL}|SL{POLE_MOTIF_STOP_BOXES:g}|"
        f"T{POLE_MOTIF_TARGET_R:g}|BE{POLE_MOTIF_BREAK_EVEN_TRIGGER_R:g}"
    )


def pole_motif_price_levels(direction: str, entry: Decimal, box_size: Decimal) -> tuple[Decimal, Decimal, Decimal]:
    risk = POLE_MOTIF_STOP_BOXES * box_size
    if direction == "LONG":
        return entry - risk, entry + (risk * POLE_MOTIF_TARGET_R), entry + (risk * POLE_MOTIF_BREAK_EVEN_TRIGGER_R)
    return entry + risk, entry - (risk * POLE_MOTIF_TARGET_R), entry - (risk * POLE_MOTIF_BREAK_EVEN_TRIGGER_R)


def build_pole_motif_signal(
    *,
    symbol: str,
    profile: PnFProfile,
    pattern: dict[str, Any],
    entry_candle: Candle,
    confirmation_idx: int,
) -> PoleMotifSignal:
    pattern_name = str(pattern["pattern_name"]).upper()
    direction = pole_direction_for_pattern(pattern_name)
    entry_source = entry_candle.open if entry_candle.open is not None else entry_candle.close
    entry = _dec(entry_source)
    box_size = _dec(profile.box_size)
    stop, target, be_trigger = pole_motif_price_levels(direction, entry, box_size)
    pole_idx = int(pattern["pole_column_index"])
    reversal_idx = int(pattern["reversal_column_index"])
    return PoleMotifSignal(
        symbol=symbol,
        direction=direction,
        entry=entry,
        stop=stop,
        target=target,
        be_trigger=be_trigger,
        trigger_ts=int(entry_candle.close_time),
        pattern_name=pattern_name,
        pole_column_index=pole_idx,
        reversal_column_index=reversal_idx,
        confirmation_column_index=confirmation_idx,
        setup_key=pole_motif_setup_key(symbol, profile, pattern_name, pole_idx, reversal_idx, confirmation_idx),
    )


def _column_boxes(col: Any, box_size: Decimal) -> Decimal | None:
    if box_size <= 0:
        return None
    return (abs(_column_top(col) - _column_bottom(col)) / box_size) + Decimal("1")


def _bucket_relative_pole_size(value: Decimal | None) -> str:
    if value is None:
        return "MISSING_RELATIVE_SIZE"
    if value < Decimal("0.75"):
        return "BELOW_RECENT_AVG_<0_75X"
    if value <= Decimal("1.25"):
        return P2_SURVIVOR_RELATIVE_POLE_SIZE
    return "ABOVE_RECENT_AVG_>1_25X"


def _bucket_p2_reversal_boxes(value: Decimal | None) -> str:
    if value is None:
        return "MISSING_REVERSAL_BOXES"
    if value <= Decimal("3"):
        return "SMALL_REVERSAL_<=3_BOXES"
    if value <= Decimal("6"):
        return P2_SURVIVOR_REVERSAL_BOXES
    return "LARGE_REVERSAL_>=7_BOXES"


def _relative_pole_size_bucket(columns_by_idx: dict[int, Any], pole_idx: int, pole_boxes: Decimal | None, box_size: Decimal) -> tuple[Decimal | None, str]:
    if pole_boxes is None:
        return None, _bucket_relative_pole_size(None)
    recent = [
        _column_boxes(columns_by_idx[idx], box_size)
        for idx in range(max(0, pole_idx - RECENT_COLUMN_LOOKBACK), pole_idx)
        if idx in columns_by_idx
    ]
    usable = [value for value in recent if value is not None]
    if not usable:
        return None, _bucket_relative_pole_size(None)
    relative = pole_boxes / (sum(usable, Decimal("0")) / Decimal(len(usable)))
    return relative, _bucket_relative_pole_size(relative)


def p2_survivor_setup_key(symbol: str, profile: PnFProfile, pole_idx: int, reversal_idx: int, confirmation_idx: int) -> str:
    return f"{symbol}|{profile.name}|{P2_SURVIVOR_STRATEGY_ID}|{P2_SURVIVOR_CANDIDATE_ID}|{pole_idx}|{reversal_idx}|{confirmation_idx}"


def detect_latest_p2_survivor_demo_signal(symbol: str, profile: PnFProfile, candles: list[Candle]) -> TriangleSignal | None:
    """Detect frozen causal P+2 survivor CAND-000053 for demo-only forwarding.

    This intentionally does not use the rejected historical core-motif filters
    (`opposing_pole_distance_columns` / `enhanced_by_opposing_pole`).  The setup
    exists when a LOW_POLE has a reversal column and the next confirmation
    column exists; only P+2-knowable survivor buckets are checked.
    """
    engine, _latest_ts = _replay_close_confirmed_pnf(profile, candles)
    if len(engine.columns) < 3:
        return None
    box_size = _dec(profile.box_size)
    if box_size <= 0:
        return None
    patterns = detect_pole_patterns(engine.columns, box_size=float(profile.box_size))
    by_idx = {int(getattr(col, "idx")): col for col in engine.columns}
    latest: TriangleSignal | None = None
    for pattern in patterns:
        if str(pattern.get("pattern_name", "")).upper() != "LOW_POLE":
            continue
        pole_idx_raw = pattern.get("pole_column_index")
        reversal_idx_raw = pattern.get("reversal_column_index")
        if pole_idx_raw is None or reversal_idx_raw is None:
            continue
        pole_idx = int(pole_idx_raw)
        reversal_idx = int(reversal_idx_raw)
        confirmation_idx = reversal_idx + 1
        pole = by_idx.get(pole_idx)
        reversal = by_idx.get(reversal_idx)
        confirmation = by_idx.get(confirmation_idx)
        if pole is None or reversal is None or confirmation is None:
            continue
        if (_column_kind(pole), _column_kind(reversal)) != ("O", "X"):
            continue
        pole_boxes = _column_boxes(pole, box_size)
        reversal_boxes = _column_boxes(reversal, box_size)
        relative_size, relative_bucket = _relative_pole_size_bucket(by_idx, pole_idx, pole_boxes, box_size)
        reversal_bucket = _bucket_p2_reversal_boxes(reversal_boxes)
        if relative_bucket != P2_SURVIVOR_RELATIVE_POLE_SIZE or reversal_bucket != P2_SURVIVOR_REVERSAL_BOXES:
            continue
        if getattr(confirmation, "start_ts", None) is None:
            continue
        entry_after_ts = int(getattr(confirmation, "start_ts"))
        entry_candle = next((c for c in candles if c.close_time > entry_after_ts), None)
        if entry_candle is None:
            continue
        entry_source = entry_candle.open if entry_candle.open is not None else entry_candle.close
        entry = _dec(entry_source)
        stop = entry - (POLE_MOTIF_STOP_BOXES * box_size)
        target = entry + ((POLE_MOTIF_STOP_BOXES * box_size) * POLE_MOTIF_TARGET_R)
        be_trigger = entry + ((POLE_MOTIF_STOP_BOXES * box_size) * POLE_MOTIF_BREAK_EVEN_TRIGGER_R)
        setup_key = p2_survivor_setup_key(symbol, profile, pole_idx, reversal_idx, confirmation_idx)
        candidate = TriangleSignal(
            symbol=symbol,
            pattern=P2_SURVIVOR_PATTERN,
            side="LONG",
            trigger_ts=int(entry_candle.close_time),
            entry_price=entry,
            stop_price=stop,
            tp1_price=be_trigger,
            tp2_price=target,
            trigger_column_idx=confirmation_idx,
            support_level=min(entry, stop),
            resistance_level=max(entry, stop),
            break_distance_boxes=POLE_MOTIF_STOP_BOXES,
            pattern_quality=(
                f"{P2_SURVIVOR_STRATEGY_ID}|candidate_id={P2_SURVIVOR_CANDIDATE_ID}|"
                f"status={P2_SURVIVOR_STATUS}|relative_pole_size={relative_bucket}|"
                f"reversal_boxes={reversal_bucket}|relative_pole_size_value={relative_size}|"
                f"pole_boxes={pole_boxes}|reversal_box_count={reversal_boxes}|setup={setup_key}"
            ),
        )
        if latest is None or candidate.trigger_column_idx > latest.trigger_column_idx:
            latest = candidate
    return latest


def detect_latest_pole_motif_demo_signal(symbol: str, profile: PnFProfile, candles: list[Candle]) -> TriangleSignal | None:
    engine, _latest_ts = _replay_close_confirmed_pnf(profile, candles)
    if len(engine.columns) < 3:
        return None
    patterns = detect_pole_patterns(engine.columns, box_size=float(profile.box_size))
    core = [
        pattern
        for pattern in patterns
        if pattern.get("opposing_pole_distance_columns") == 3
        and pattern.get("enhanced_by_opposing_pole") is False
        and pattern.get("reversal_column_index") is not None
    ]
    if not core:
        return None

    by_idx = {int(getattr(col, "idx")): col for col in engine.columns}
    latest: PoleMotifSignal | None = None
    for pattern in core:
        reversal_idx = int(pattern["reversal_column_index"])
        confirmation_idx = reversal_idx + 1
        confirmation = by_idx.get(confirmation_idx)
        if confirmation is None or getattr(confirmation, "start_ts", None) is None:
            continue
        entry_after_ts = int(getattr(confirmation, "start_ts"))
        entry_candle = next((c for c in candles if c.close_time > entry_after_ts), None)
        if entry_candle is None:
            continue
        candidate = build_pole_motif_signal(
            symbol=symbol,
            profile=profile,
            pattern=pattern,
            entry_candle=entry_candle,
            confirmation_idx=confirmation_idx,
        )
        if latest is None or candidate.reversal_column_index > latest.reversal_column_index:
            latest = candidate
    return latest.to_triangle_signal() if latest is not None else None


def is_demo_pole_motif_signal(signal: TriangleSignal) -> bool:
    return signal.pattern in DEMO_POLE_MOTIF_PATTERNS


def is_p2_survivor_signal(signal: TriangleSignal) -> bool:
    return signal.pattern == P2_SURVIVOR_PATTERN


def is_break_even_managed_signal(signal: TriangleSignal) -> bool:
    return is_demo_pole_motif_signal(signal) or is_p2_survivor_signal(signal)


def strategy_id_for_signal(signal: TriangleSignal) -> str | None:
    if is_p2_survivor_signal(signal):
        return P2_SURVIVOR_STRATEGY_ID
    return None


def candidate_id_for_signal(signal: TriangleSignal) -> str | None:
    if is_p2_survivor_signal(signal):
        return P2_SURVIVOR_CANDIDATE_ID
    return None


def be_trigger_price_for_signal(signal: TriangleSignal) -> Decimal | None:
    if is_break_even_managed_signal(signal):
        return signal.tp1_price
    return None


def is_demo_double_signal(signal: TriangleSignal) -> bool:
    return signal.pattern in DEMO_DOUBLE_PATTERNS


def append_demo_signal_note(signal: TriangleSignal, notes: str | None) -> str | None:
    prefix = None
    if is_demo_double_signal(signal):
        prefix = "DEMO_DOUBLE_SMOKE_TEST"
    elif is_p2_survivor_signal(signal):
        prefix = f"strategy_id={P2_SURVIVOR_STRATEGY_ID}; candidate_id={P2_SURVIVOR_CANDIDATE_ID}; {P2_SURVIVOR_STATUS}"
    if prefix is None:
        return notes
    return prefix if not notes else f"{prefix}; {notes}"


# Backward-compatible name retained for existing call sites/tests.
def append_demo_double_note(signal: TriangleSignal, notes: str | None) -> str | None:
    return append_demo_signal_note(signal, notes)


def has_existing_open_trade(conn: sqlite3.Connection, symbol: str) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM live_trades_binance WHERE symbol = ? AND status IN ({','.join('?' for _ in OPEN_TRADE_STATUSES)}) LIMIT 1",
        (symbol, *sorted(OPEN_TRADE_STATUSES)),
    ).fetchone()
    return row is not None


def _prices_match_for_duplicate_setup(left: Any, right: Decimal) -> bool:
    try:
        left_dec = _dec(left)
    except ValueError:
        return False
    return abs(left_dec - right) <= DUPLICATE_SETUP_PRICE_TOLERANCE


def has_recent_duplicate_setup_trade(conn: sqlite3.Connection, signal: TriangleSignal) -> bool:
    cutoff = datetime.now(timezone.utc).timestamp() - DUPLICATE_SETUP_COOLDOWN_SECONDS
    rows = conn.execute(
        """
        SELECT created_at, entry_price, stop_price
        FROM live_trades_binance
        WHERE symbol = ? AND pattern = ? AND side = ? AND dry_run = 0
        ORDER BY id DESC
        """,
        (signal.symbol, signal.pattern, signal.side),
    ).fetchall()
    for created_at, entry_price, stop_price in rows:
        created = parse_utc_timestamp(str(created_at))
        if created is None or created.timestamp() < cutoff:
            continue
        entry_matches = _prices_match_for_duplicate_setup(entry_price, signal.entry_price)
        stop_matches = _prices_match_for_duplicate_setup(stop_price, signal.stop_price)
        if entry_matches and stop_matches:
            return True
    return False


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
            created_at, symbol, pattern, strategy_id, candidate_id, side, trigger_timestamp, entry_price,
            stop_price, tp1_price, tp2_price, notional_usdt, decision,
            block_reason, dry_run, exchange_order_id, raw_order_response, notes
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_iso(),
            signal.symbol,
            signal.pattern,
            strategy_id_for_signal(signal),
            candidate_id_for_signal(signal),
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
            created_at, symbol, pattern, strategy_id, candidate_id, side, trigger_timestamp, entry_price,
            stop_price, tp1_price, tp2_price, notional_usdt, decision, status,
            block_reason, dry_run, exchange_order_id, raw_order_response, notes,
            break_even_armed, break_even_trigger_price, active_stop_price
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_iso(),
            signal.symbol,
            signal.pattern,
            strategy_id_for_signal(signal),
            candidate_id_for_signal(signal),
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
            0,
            float(be_trigger_price_for_signal(signal)) if be_trigger_price_for_signal(signal) is not None else None,
            float(signal.stop_price),
        ),
    )
    conn.commit()


def extract_algo_order_id(response: dict[str, Any] | None) -> str | None:
    if not isinstance(response, dict):
        return None
    for key in ("algoId", "orderId", "clientAlgoId", "clientOrderId"):
        raw = response.get(key)
        if raw not in (None, ""):
            return str(raw)
    return None


def build_protective_algo_orders(
    *,
    trade_id: int,
    signal: TriangleSignal,
    active_stop_price: Decimal | None = None,
    spec: SymbolSpec | None = None,
    working_type: str = "MARK_PRICE",
    position_side: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    exit_side = "SELL" if signal.side == "LONG" else "BUY"
    if position_side is None:
        position_side = "LONG" if signal.side == "LONG" else "SHORT"
    stop_price = active_stop_price if active_stop_price is not None else signal.stop_price
    tp_price = signal.tp2_price
    if spec is not None:
        stop_price = quantize_nearest(stop_price, spec.tick_size).quantize(spec.tick_size.normalize())
        tp_price = quantize_nearest(tp_price, spec.tick_size).quantize(spec.tick_size.normalize())
    base = {
        "algoType": "CONDITIONAL",
        "symbol": binance_symbol(signal.symbol),
        "side": exit_side,
        "closePosition": "true",
        "workingType": working_type,
    }
    if position_side:
        base["positionSide"] = position_side
    stop_order = {
        **base,
        "type": "STOP_MARKET",
        "triggerPrice": format_decimal_for_step(stop_price, spec.tick_size) if spec is not None else str(stop_price),
        "clientAlgoId": f"pnf-sl-{trade_id}"[:36],
    }
    tp_order = {
        **base,
        "type": "TAKE_PROFIT_MARKET",
        "triggerPrice": format_decimal_for_step(tp_price, spec.tick_size) if spec is not None else str(tp_price),
        "clientAlgoId": f"pnf-tp-{trade_id}"[:36],
    }
    return stop_order, tp_order


def extract_entry_position_side(raw_order_response: str | dict[str, Any] | None) -> str | None:
    """Return the accepted entry order positionSide, when Binance supplied one."""
    if raw_order_response in (None, ""):
        return None
    raw: Any = raw_order_response
    if isinstance(raw_order_response, str):
        try:
            raw = json.loads(raw_order_response)
        except json.JSONDecodeError:
            return None
    if not isinstance(raw, dict):
        return None
    candidates: list[Any] = [raw]
    for key in ("order_response", "order_status"):
        value = raw.get(key)
        if isinstance(value, dict):
            candidates.append(value)
    for candidate in candidates:
        position_side = str(candidate.get("positionSide") or "").upper()
        if position_side:
            return position_side
    return None


def protective_position_side_for_trade(signal: TriangleSignal, raw_order_response: str | dict[str, Any] | None) -> str:
    entry_position_side = extract_entry_position_side(raw_order_response)
    if entry_position_side == "BOTH":
        return "BOTH"
    if entry_position_side in {"LONG", "SHORT"}:
        return entry_position_side
    return "LONG" if signal.side == "LONG" else "SHORT"


def protective_triggers_valid_for_mark(
    *,
    side: str,
    mark_price: Decimal,
    stop_trigger: Decimal,
    tp_trigger: Decimal,
) -> bool:
    side_upper = side.upper()
    if side_upper == "LONG":
        return mark_price > stop_trigger and mark_price < tp_trigger
    if side_upper == "SHORT":
        return mark_price < stop_trigger and mark_price > tp_trigger
    raise ValueError(f"unsupported protective side: {side}")


def emergency_close_stop_violated(*, side: str, mark_price: Decimal, stop_trigger: Decimal) -> bool:
    side_upper = side.upper()
    if side_upper == "LONG":
        return mark_price <= stop_trigger
    if side_upper == "SHORT":
        return mark_price >= stop_trigger
    raise ValueError(f"unsupported emergency close side: {side}")


def build_emergency_market_close_order(
    *,
    trade_id: int,
    symbol: str,
    side: str,
    quantity: Decimal,
    spec: SymbolSpec,
    position_side: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    if quantity <= 0:
        return None, "emergency close quantity is non-positive"
    quantity = quantize_down(quantity, spec.step_size)
    if not aligned(quantity, spec.step_size):
        return None, "emergency close quantity precision invalid"
    if quantity < spec.min_qty:
        return None, f"emergency close quantity below minQty: quantity={quantity} minQty={spec.min_qty}"
    order = {
        "symbol": binance_symbol(symbol),
        "side": "SELL" if side.upper() == "LONG" else "BUY",
        "type": "MARKET",
        "quantity": format_decimal_for_step(quantity, spec.step_size),
        "reduceOnly": "true",
        "newClientOrderId": f"pnf-emerg-{trade_id}"[:36],
    }
    if position_side:
        order["positionSide"] = position_side
    return order, None


def emergency_close_unprotected_if_stop_violated(
    conn: sqlite3.Connection,
    client: BinanceFuturesClient,
    trade_id: int,
    signal: TriangleSignal,
) -> bool:
    row = conn.execute(
        """
        SELECT status, protective_orders_status, active_stop_price, raw_order_response, executed_qty
        FROM live_trades_binance
        WHERE id = ?
        """,
        (int(trade_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"trade_id={trade_id} not found for emergency close")
    status, protective_status, active_stop, raw_order_response, executed_qty = row
    if status == "EMERGENCY_CLOSE_SENT" or protective_status == "EMERGENCY_CLOSE_SENT":
        console("EMERGENCY_CLOSE_TRIGGERED", f"{signal.symbol} trade_id={trade_id} skipped; already sent")
        return True
    if status != "POSITION_OPEN_UNPROTECTED":
        return False

    stop_trigger = _dec(active_stop) if active_stop not in (None, "") else signal.stop_price
    mark_price = client.get_mark_price(binance_symbol(signal.symbol))
    if not emergency_close_stop_violated(side=signal.side, mark_price=mark_price, stop_trigger=stop_trigger):
        return False

    payload = {
        "symbol": signal.symbol,
        "trade_id": int(trade_id),
        "mark_price": str(mark_price),
        "stop_trigger": str(stop_trigger),
        "side": signal.side,
    }
    console("EMERGENCY_CLOSE_TRIGGERED", f"{signal.symbol} trade_id={trade_id}", payload)
    try:
        if executed_qty in (None, ""):
            raise RuntimeError("missing executed quantity for emergency close")
        mode_response = client.get_position_mode()
        if not position_mode_is_unambiguous(mode_response):
            raise RuntimeError(f"one-way position mode required for emergency reduceOnly close: {mode_response}")
        spec = client.get_symbol_spec(binance_symbol(signal.symbol))
        close_order, reason = build_emergency_market_close_order(
            trade_id=int(trade_id),
            symbol=signal.symbol,
            side=signal.side,
            quantity=_dec(executed_qty),
            spec=spec,
            position_side=protective_position_side_for_trade(signal, raw_order_response),
        )
        if reason is not None:
            raise RuntimeError(reason)
        close_response = client.submit_order(close_order or {})
    except Exception as exc:
        cursor = conn.execute(
            """
            UPDATE live_trades_binance
            SET status = 'POSITION_OPEN_UNPROTECTED', notes = ?
            WHERE id = ?
            """,
            (f"emergency close failed; still unprotected: {exc}", int(trade_id)),
        )
        conn.commit()
        console("EMERGENCY_CLOSE_FAILED", f"{signal.symbol} trade_id={trade_id}", {**payload, "error": str(exc)})
        return True

    conn.execute(
        """
        UPDATE live_trades_binance
        SET status = 'EMERGENCY_CLOSE_SENT', protective_orders_status = 'EMERGENCY_CLOSE_SENT',
            protective_orders_raw_response = ?, protective_orders_error = NULL, notes = ?
        WHERE id = ?
        """,
        (
            json.dumps({"close_order": close_order, "close_response": close_response, **payload}, sort_keys=True, default=str),
            "emergency reduce-only MARKET close sent; protective attach skipped",
            int(trade_id),
        ),
    )
    conn.commit()
    console("EMERGENCY_CLOSE_ORDER_SENT", f"{signal.symbol} trade_id={trade_id}", {"close_order": close_order, "close_response": close_response})
    return True


def mark_protective_attach_blocked(
    conn: sqlite3.Connection,
    *,
    trade_id: int,
    status: str,
    error: str,
    raw_response: dict[str, Any],
) -> None:
    conn.execute(
        """
        UPDATE live_trades_binance
        SET status = ?, protective_orders_status = ?, protective_orders_raw_response = ?, protective_orders_error = ?
        WHERE id = ?
        """,
        (
            "POSITION_OPEN_UNPROTECTED",
            status,
            json.dumps(raw_response, sort_keys=True, default=str),
            error,
            int(trade_id),
        ),
    )
    conn.commit()


def attach_protective_algo_orders(
    conn: sqlite3.Connection,
    client: BinanceFuturesClient,
    trade_id: int,
    signal: TriangleSignal,
    *,
    dry_run: bool = False,
) -> None:
    row = conn.execute(
        """
        SELECT stop_algo_id, tp_algo_id, protective_orders_status, active_stop_price, raw_order_response
        FROM live_trades_binance
        WHERE id = ?
        """,
        (int(trade_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"trade_id={trade_id} not found for protective order attach")
    stop_algo_id, tp_algo_id, protective_status, active_stop, raw_order_response = row
    if stop_algo_id and tp_algo_id:
        if protective_status != "ATTACHED":
            conn.execute(
                """
                UPDATE live_trades_binance
                SET protective_orders_status = ?, protective_orders_error = NULL
                WHERE id = ?
                """,
                ("ATTACHED", int(trade_id)),
            )
            conn.commit()
        console("PROTECTIVE_ATTACH_ATTEMPT", f"{signal.symbol} trade_id={trade_id} skipped; already attached")
        return
    if protective_status == "ATTACHED":
        console("PROTECTIVE_ATTACH_ATTEMPT", f"{signal.symbol} trade_id={trade_id} skipped; already attached")
        return

    active_stop_dec = _dec(active_stop) if active_stop not in (None, "") else None
    stop_order: dict[str, Any] | None = None
    tp_order: dict[str, Any] | None = None
    stop_response: dict[str, Any] | None = None
    tp_response: dict[str, Any] | None = None
    stop_id = str(stop_algo_id) if stop_algo_id not in (None, "") else None
    tp_id = str(tp_algo_id) if tp_algo_id not in (None, "") else None
    try:
        spec = client.get_symbol_spec(binance_symbol(signal.symbol))
        stop_order, tp_order = build_protective_algo_orders(
            trade_id=int(trade_id),
            signal=signal,
            active_stop_price=active_stop_dec,
            spec=spec,
            position_side=protective_position_side_for_trade(signal, raw_order_response),
        )
        console(
            "PROTECTIVE_TRIGGER_PRICE_QUANTIZED",
            f"{signal.symbol} trade_id={trade_id}",
            {
                "tick_size": str(spec.tick_size),
                "stop_trigger_price": stop_order["triggerPrice"],
                "tp_trigger_price": tp_order["triggerPrice"],
            },
        )
        try:
            mark_price = client.get_mark_price(binance_symbol(signal.symbol))
        except Exception as exc:
            payload = {
                "symbol": signal.symbol,
                "trade_id": int(trade_id),
                "stop_trigger": stop_order["triggerPrice"],
                "tp_trigger": tp_order["triggerPrice"],
                "side": signal.side,
                "error": str(exc),
            }
            mark_protective_attach_blocked(
                conn,
                trade_id=int(trade_id),
                status="MARK_PRICE_UNAVAILABLE",
                error=f"PROTECTIVE_ORDER_MARK_PRICE_UNAVAILABLE: {exc}",
                raw_response={"stop_order": stop_order, "tp_order": tp_order, "mark_price_error": str(exc)},
            )
            console("PROTECTIVE_ORDER_MARK_PRICE_UNAVAILABLE", f"{signal.symbol} trade_id={trade_id} UNPROTECTED", payload)
            return
        stop_trigger = _dec(stop_order["triggerPrice"])
        tp_trigger = _dec(tp_order["triggerPrice"])
        if not protective_triggers_valid_for_mark(
            side=signal.side,
            mark_price=mark_price,
            stop_trigger=stop_trigger,
            tp_trigger=tp_trigger,
        ):
            payload = {
                "symbol": signal.symbol,
                "trade_id": int(trade_id),
                "mark_price": str(mark_price),
                "stop_trigger": str(stop_trigger),
                "tp_trigger": str(tp_trigger),
                "side": signal.side,
            }
            mark_protective_attach_blocked(
                conn,
                trade_id=int(trade_id),
                status="BLOCKED_IMMEDIATE_TRIGGER",
                error="PROTECTIVE_ORDER_BLOCKED_IMMEDIATE_TRIGGER",
                raw_response={"stop_order": stop_order, "tp_order": tp_order, **payload},
            )
            console("PROTECTIVE_ORDER_BLOCKED_IMMEDIATE_TRIGGER", f"{signal.symbol} trade_id={trade_id} UNPROTECTED", payload)
            return
        console(
            "PROTECTIVE_ATTACH_ATTEMPT",
            f"{signal.symbol} trade_id={trade_id}",
            {"stop_order": stop_order, "tp_order": tp_order, "dry_run": dry_run},
        )

        if dry_run:
            conn.execute(
                """
                UPDATE live_trades_binance
                SET protective_orders_status = ?, protective_orders_raw_response = ?, protective_orders_error = NULL
                WHERE id = ?
                """,
                (
                    "DRY_RUN",
                    json.dumps({"stop_order": stop_order, "tp_order": tp_order}, sort_keys=True, default=str),
                    int(trade_id),
                ),
            )
            conn.commit()
            return

        if stop_id is None:
            stop_response = client.submit_algo_order(stop_order)
            stop_id = extract_algo_order_id(stop_response) or stop_order["clientAlgoId"]
        if tp_id is None:
            tp_response = client.submit_algo_order(tp_order)
            tp_id = extract_algo_order_id(tp_response) or tp_order["clientAlgoId"]
        conn.execute(
            """
            UPDATE live_trades_binance
            SET stop_algo_id = ?, tp_algo_id = ?, protective_orders_status = ?,
                protective_orders_raw_response = ?, protective_orders_error = NULL
            WHERE id = ?
            """,
            (
                stop_id,
                tp_id,
                "ATTACHED",
                json.dumps(
                    {
                        "stop_order": stop_order,
                        "stop_response": stop_response,
                        "tp_order": tp_order,
                        "tp_response": tp_response,
                    },
                    sort_keys=True,
                    default=str,
                ),
                int(trade_id),
            ),
        )
        conn.commit()
        console("PROTECTIVE_ATTACH_SUCCESS", f"{signal.symbol} trade_id={trade_id}", {"stop_algo_id": stop_id, "tp_algo_id": tp_id})
    except Exception as exc:
        conn.execute(
            """
            UPDATE live_trades_binance
            SET stop_algo_id = COALESCE(?, stop_algo_id),
                tp_algo_id = COALESCE(?, tp_algo_id),
                protective_orders_status = ?, protective_orders_raw_response = ?,
                protective_orders_error = ?
            WHERE id = ?
            """,
            (
                stop_id,
                tp_id,
                "ATTACH_FAILED",
                json.dumps(
                    {
                        "stop_order": stop_order,
                        "stop_response": stop_response,
                        "tp_order": tp_order,
                        "tp_response": tp_response,
                    },
                    sort_keys=True,
                    default=str,
                ),
                str(exc),
                int(trade_id),
            ),
        )
        conn.commit()
        console("PROTECTIVE_ATTACH_FAILED", f"{signal.symbol} trade_id={trade_id} UNPROTECTED", {"error": str(exc)})



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
    client: BinanceFuturesClient | None = None,
    signal: TriangleSignal,
    order_request: dict[str, Any] | None,
    order_response: dict[str, Any] | None,
    status_response: dict[str, Any],
    trades_response: Any,
    lifecycle: dict[str, Any],
    live_enabled: bool = True,
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
    if entry_status == "FILLED":
        if live_enabled and client is not None:
            attach_protective_algo_orders(conn, client, int(trade_id), signal, dry_run=False)
        console("ORDER_FILLED", f"{signal.symbol} {signal.pattern}", lifecycle)
        if is_p2_survivor_signal(signal):
            console(
                "POSITION_OPEN",
                f"{signal.symbol} {signal.pattern}",
                {"strategy_id": P2_SURVIVOR_STRATEGY_ID, "candidate_id": P2_SURVIVOR_CANDIDATE_ID, **lifecycle},
            )
        if verbose_market_logs:
            log_position_open_detail(signal, lifecycle)


def parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def pending_age_minutes(created_at: str | None) -> float | None:
    created = parse_utc_timestamp(created_at)
    if created is None:
        return None
    return (datetime.now(timezone.utc) - created).total_seconds() / 60


def mark_entry_order_terminal(
    conn: sqlite3.Connection,
    trade_id: int,
    *,
    status: str,
    status_response: dict[str, Any],
    trades_response: Any,
    raw_order_response: dict[str, Any],
    notes: str,
    block_reason: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE live_trades_binance
        SET status = ?, entry_order_status = ?, block_reason = COALESCE(?, block_reason),
            order_status_response = ?, user_trades_response = ?, raw_order_response = ?, notes = ?
        WHERE id = ?
        """,
        (
            status,
            status,
            block_reason,
            json.dumps(status_response, sort_keys=True, default=str),
            json.dumps(trades_response, sort_keys=True, default=str),
            json.dumps(raw_order_response, sort_keys=True, default=str),
            notes,
            int(trade_id),
        ),
    )
    conn.commit()


def trade_row_to_signal(row: sqlite3.Row | dict[str, Any]) -> TriangleSignal:
    raw = dict(row)
    return TriangleSignal(
        symbol=str(raw["symbol"]),
        pattern=str(raw["pattern"]),
        side=str(raw["side"]).upper(),
        trigger_ts=int(raw["trigger_timestamp"]),
        entry_price=_dec(raw["entry_price"]),
        stop_price=_dec(raw["stop_price"]),
        tp1_price=_dec(raw["tp1_price"]),
        tp2_price=_dec(raw["tp2_price"]),
        trigger_column_idx=0,
        support_level=Decimal("0"),
        resistance_level=Decimal("0"),
        break_distance_boxes=Decimal("0"),
        pattern_quality="PERSISTED_ENTRY_ORDER",
    )


def poll_pending_entry_orders(
    conn: sqlite3.Connection,
    client: BinanceFuturesClient,
    *,
    live_enabled: bool,
    verbose_market_logs: bool = False,
    max_pending_entry_minutes: int | None = None,
) -> None:
    if not live_enabled:
        return
    rows = conn.execute(
        """
        SELECT id, created_at, symbol, pattern, side, trigger_timestamp, entry_price, stop_price, tp1_price, tp2_price,
               raw_order_response
        FROM live_trades_binance
        WHERE status = 'ORDER_SENT'
        """
    ).fetchall()
    for row in rows:
        trade_id, created_at, symbol, pattern, side, trigger_ts, entry, stop, tp1, tp2, raw_order_response = row
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
            entry_status = str(lifecycle.get("entry_order_status") or "")
            age_minutes = pending_age_minutes(str(created_at))
            is_expired = (
                max_pending_entry_minutes is not None
                and age_minutes is not None
                and age_minutes > max_pending_entry_minutes
            )
            if is_expired and entry_status in {"CANCELED", "EXPIRED", "REJECTED"}:
                merged_raw = {
                    "order_request": order_request,
                    "order_response": order_response,
                    "order_status": status_response,
                    "user_trades": trades_response,
                }
                mark_entry_order_terminal(
                    conn,
                    int(trade_id),
                    status=entry_status,
                    status_response=status_response,
                    trades_response=trades_response,
                    raw_order_response=merged_raw,
                    notes=f"entry order terminal on exchange before stale cancellation; status={entry_status}",
                )
                console("ORDER_STATUS", f"{symbol} trade_id={trade_id}", lifecycle)
                continue
            if is_expired and entry_status == "PARTIALLY_FILLED":
                console(
                    "ORDER_EXPIRED_MANUAL_REVIEW",
                    f"{symbol} trade_id={trade_id}",
                    {"reason": "MAX_PENDING_ENTRY_AGE_EXCEEDED", "age_minutes": age_minutes, **lifecycle},
                )
                continue
            if is_expired and entry_status == "NEW":
                order_id, client_order_id = extract_order_ids(order_request, order_response)
                cancel_response = client.cancel_order(
                    binance_symbol(signal.symbol),
                    order_id=order_id,
                    orig_client_order_id=client_order_id,
                )
                merged_raw = {
                    "order_request": order_request,
                    "order_response": order_response,
                    "order_status": status_response,
                    "user_trades": trades_response,
                    "cancel_response": cancel_response,
                    "cancel_reason": "MAX_PENDING_ENTRY_AGE_EXCEEDED",
                }
                mark_entry_order_terminal(
                    conn,
                    int(trade_id),
                    status="ENTRY_EXPIRED",
                    status_response=status_response,
                    trades_response=trades_response,
                    raw_order_response=merged_raw,
                    notes=json.dumps(
                        {
                            "reason": "MAX_PENDING_ENTRY_AGE_EXCEEDED",
                            "age_minutes": age_minutes,
                            "cancel_response": cancel_response,
                        },
                        sort_keys=True,
                        default=str,
                    ),
                    block_reason="MAX_PENDING_ENTRY_AGE_EXCEEDED",
                )
                console("ORDER_CANCELLED", f"{symbol} trade_id={trade_id}", {"reason": "MAX_PENDING_ENTRY_AGE_EXCEEDED", "cancel_response": cancel_response})
                console("ORDER_EXPIRED", f"{symbol} trade_id={trade_id}", {"age_minutes": age_minutes})
                continue
            apply_entry_lifecycle(
                conn,
                int(trade_id),
                client=client,
                signal=signal,
                order_request=order_request,
                order_response=order_response,
                status_response=status_response,
                trades_response=trades_response,
                lifecycle=lifecycle,
                live_enabled=live_enabled,
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
    console("ORDER_SUBMITTED", f"{signal.symbol} {signal.pattern}", {"order": order, "response": raw_response})
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
        client=client,
        signal=signal,
        order_request=order,
        order_response=raw_response,
        status_response=status_response,
        trades_response=trades_response,
        lifecycle=lifecycle,
        live_enabled=True,
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


def quantize_nearest(value: Decimal, unit: Decimal) -> Decimal:
    if unit <= 0:
        raise ValueError("precision unit must be positive")
    return (value / unit).to_integral_value(rounding=ROUND_HALF_UP) * unit


def format_decimal_for_step(value: Decimal, unit: Decimal) -> str:
    return format(value.quantize(unit.normalize()), "f")


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


def validate_risk_levels(signal: TriangleSignal) -> str | None:
    if signal.side == "LONG" and signal.stop_price < signal.entry_price < signal.tp1_price < signal.tp2_price:
        return None
    if signal.side == "SHORT" and signal.stop_price > signal.entry_price > signal.tp1_price > signal.tp2_price:
        return None
    return "invalid risk levels"


def build_entry_order(
    signal: TriangleSignal,
    spec: SymbolSpec,
    notional_usdt: Decimal,
    *,
    max_notional_usdt: Decimal = MAX_NOTIONAL_USDT,
) -> tuple[dict[str, Any] | None, str | None]:
    risk_reason = validate_risk_levels(signal)
    if risk_reason is not None:
        return None, risk_reason
    if notional_usdt > max_notional_usdt:
        return None, f"notional exceeds effective cap: requested={notional_usdt} cap={max_notional_usdt}"
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
    if actual_notional > max_notional_usdt:
        return None, f"rounded notional exceeds effective cap: actual={actual_notional} cap={max_notional_usdt}"
    if actual_notional < spec.min_notional:
        cap_label = "1 USDT cap" if max_notional_usdt == MAX_NOTIONAL_USDT else f"effective cap {max_notional_usdt}"
        return None, f"min order notional cannot support {cap_label}: actual={actual_notional} minNotional={spec.min_notional}"
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


def _position_risk_rows(position_response: Any) -> list[dict[str, Any]]:
    rows: Any = position_response
    if isinstance(position_response, dict):
        rows = position_response.get("positions", position_response.get("data", position_response))
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def parse_binance_open_positions(position_response: Any, runtime_symbol: str) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for row in _position_risk_rows(position_response):
        try:
            position_amt = _dec(row.get("positionAmt", "0"))
        except ValueError:
            continue
        if position_amt == 0:
            continue
        position_side = str(row.get("positionSide") or "").upper()
        side = position_side if position_side in {"LONG", "SHORT"} else ("LONG" if position_amt > 0 else "SHORT")
        positions.append(
            {
                "symbol": runtime_symbol,
                "side": side,
                "qty": abs(position_amt),
                "entry_price": _dec(row.get("entryPrice", "0")),
                "avg_fill_price": None,
                "stop_price": None,
                "tp2_price": None,
                "status": "BINANCE_POSITION_OPEN",
                "raw_position": row,
            }
        )
    return positions


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _quantity_from_raw_order_response(raw_order_response: Any) -> Decimal | None:
    try:
        parsed = json.loads(raw_order_response or "{}")
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    request = parsed.get("order_request") or parsed.get("would_submit_order")
    if not isinstance(request, dict) or request.get("quantity") in (None, ""):
        return None
    try:
        return _dec(request.get("quantity"))
    except ValueError:
        return None


def load_local_open_trades_for_reconciliation(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(conn, "live_trades_binance"):
        return []
    placeholders = ",".join("?" for _ in sorted(OPEN_TRADE_STATUSES))
    symbol_placeholders = ",".join("?" for _ in sorted(ALLOWED_SYMBOLS))
    rows = conn.execute(
        f"""
        SELECT id, symbol, side, entry_price, avg_fill_price, executed_qty, stop_price, tp2_price, status, raw_order_response
        FROM live_trades_binance
        WHERE status IN ({placeholders}) AND symbol IN ({symbol_placeholders})
        ORDER BY symbol, side, id
        """,
        (*sorted(OPEN_TRADE_STATUSES), *sorted(ALLOWED_SYMBOLS)),
    ).fetchall()
    trades: list[dict[str, Any]] = []
    for row in rows:
        qty = (
            _dec(row["executed_qty"])
            if row["executed_qty"] not in (None, "")
            else _quantity_from_raw_order_response(row["raw_order_response"])
        )
        trades.append(
            {
                "id": int(row["id"]),
                "symbol": str(row["symbol"]),
                "side": str(row["side"]).upper(),
                "qty": qty,
                "entry_price": _dec(row["entry_price"]),
                "avg_fill_price": _dec(row["avg_fill_price"]) if row["avg_fill_price"] not in (None, "") else None,
                "stop_price": _dec(row["stop_price"]),
                "tp2_price": _dec(row["tp2_price"]),
                "status": str(row["status"]),
            }
        )
    return trades


def _decimal_payload(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def _position_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("symbol")), str(row.get("side")).upper())


def _qty_distance(binance_qty: Decimal | None, local_qty: Decimal | None) -> tuple[int, Decimal]:
    if isinstance(binance_qty, Decimal) and isinstance(local_qty, Decimal):
        return (0, abs(binance_qty - local_qty))
    return (1, Decimal("0"))


def _closest_qty_local_trade(
    binance_position: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda trade: (
            _qty_distance(binance_position.get("qty"), trade.get("qty")),
            int(trade.get("id", 0)),
        ),
    )


def _prices_diverge_materially(local_price: Decimal | None, binance_entry: Decimal | None) -> bool:
    if not isinstance(local_price, Decimal) or not isinstance(binance_entry, Decimal):
        return False
    tolerance = max(abs(binance_entry) * RECONCILE_PRICE_MISMATCH_BPS / Decimal("10000"), RECONCILE_MIN_PRICE_MISMATCH)
    return abs(local_price - binance_entry) > tolerance


def _matching_warnings(binance_position: dict[str, Any], local_trade: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    binance_qty = binance_position.get("qty")
    local_qty = local_trade.get("qty")
    if isinstance(binance_qty, Decimal) and isinstance(local_qty, Decimal) and binance_qty != local_qty:
        warnings.append("QTY_MISMATCH")
    if local_trade.get("status") != "POSITION_OPEN":
        warnings.append("STATUS_MISMATCH")
    local_price = local_trade.get("avg_fill_price") if local_trade.get("avg_fill_price") is not None else local_trade.get("entry_price")
    if _prices_diverge_materially(local_price, binance_position.get("entry_price")):
        warnings.append("ENTRY_PRICE_MISMATCH")
    return warnings


def _primary_reconcile_status(warnings: list[str]) -> str:
    for status in ("STATUS_MISMATCH", "QTY_MISMATCH", "SIDE_MISMATCH", "DUPLICATE_LOCAL_OPEN_ROWS", "ENTRY_PRICE_MISMATCH"):
        if status in warnings:
            return status
    return "MATCHED"


def _duplicate_local_symbol_warnings(local_trades: list[dict[str, Any]]) -> dict[str, list[str]]:
    counts: dict[str, int] = {}
    for trade in local_trades:
        counts[str(trade.get("symbol"))] = counts.get(str(trade.get("symbol")), 0) + 1
    return {symbol: ["DUPLICATE_LOCAL_OPEN_ROWS"] for symbol, count in counts.items() if count > 1}


def _binance_payload(
    position: dict[str, Any],
    *,
    match: dict[str, Any] | None,
    reconcile_status: str,
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "source": "BINANCE",
        "reconcile_status": reconcile_status,
        "matched_local_trade_id": match.get("id") if match is not None else None,
        "symbol": position["symbol"],
        "side": position["side"],
        "qty": _decimal_payload(position.get("qty")),
        "entry_price": _decimal_payload(position.get("entry_price")),
        "avg_fill_price": _decimal_payload(position.get("avg_fill_price")),
        "stop_price": _decimal_payload(position.get("stop_price")),
        "tp2_price": _decimal_payload(position.get("tp2_price")),
        "status": position.get("status"),
        "has_matching_local_state_row": match is not None,
        "mismatch_warnings": warnings,
    }


def _local_payload(
    trade: dict[str, Any],
    *,
    match: dict[str, Any] | None,
    reconcile_status: str,
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "source": "LOCAL",
        "reconcile_status": reconcile_status,
        "local_trade_id": trade.get("id"),
        "symbol": trade["symbol"],
        "side": trade["side"],
        "qty": _decimal_payload(trade.get("qty")),
        "entry_price": _decimal_payload(trade.get("entry_price")),
        "avg_fill_price": _decimal_payload(trade.get("avg_fill_price")),
        "stop_price": _decimal_payload(trade.get("stop_price")),
        "tp2_price": _decimal_payload(trade.get("tp2_price")),
        "status": trade.get("status"),
        "has_matching_binance_position": match is not None,
        "mismatch_warnings": warnings,
    }


def build_position_reconciliation_logs(
    binance_positions: list[dict[str, Any]],
    local_trades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    local_by_symbol: dict[str, list[dict[str, Any]]] = {}
    local_by_symbol_side: dict[tuple[str, str], list[dict[str, Any]]] = {}
    binance_by_symbol: dict[str, list[dict[str, Any]]] = {}
    binance_by_symbol_side: dict[tuple[str, str], list[dict[str, Any]]] = {}
    duplicate_warnings = _duplicate_local_symbol_warnings(local_trades)
    logs: list[dict[str, Any]] = []
    matched_local_ids: set[int] = set()

    for trade in local_trades:
        local_by_symbol.setdefault(str(trade.get("symbol")), []).append(trade)
        local_by_symbol_side.setdefault(_position_key(trade), []).append(trade)
    for position in binance_positions:
        binance_by_symbol.setdefault(str(position.get("symbol")), []).append(position)
        binance_by_symbol_side.setdefault(_position_key(position), []).append(position)

    for position in sorted(
        binance_positions,
        key=lambda item: (_position_key(item), str(item.get("qty"))),
    ):
        same_side_candidates = [
            trade
            for trade in local_by_symbol_side.get(_position_key(position), [])
            if int(trade.get("id", 0)) not in matched_local_ids
        ]
        match = _closest_qty_local_trade(position, same_side_candidates)
        if match is not None:
            matched_local_ids.add(int(match.get("id", 0)))
            warnings = _matching_warnings(position, match) + duplicate_warnings.get(str(position.get("symbol")), [])
            reconcile_status = _primary_reconcile_status(warnings)
            logs.append(_binance_payload(position, match=match, reconcile_status=reconcile_status, warnings=warnings))
            logs.append(_local_payload(match, match=position, reconcile_status=reconcile_status, warnings=warnings))
            continue

        same_symbol_local = local_by_symbol.get(str(position.get("symbol")), [])
        if same_symbol_local:
            warnings = ["SIDE_MISMATCH"] + duplicate_warnings.get(str(position.get("symbol")), [])
            reconcile_status = "SIDE_MISMATCH"
        else:
            warnings = ["BINANCE_ONLY"]
            reconcile_status = "BINANCE_ONLY"
        logs.append(_binance_payload(position, match=None, reconcile_status=reconcile_status, warnings=warnings))

    for trade in sorted(
        local_trades,
        key=lambda item: (_position_key(item), int(item.get("id", 0))),
    ):
        trade_id = int(trade.get("id", 0))
        if trade_id in matched_local_ids:
            continue
        same_symbol_binance = binance_by_symbol.get(str(trade.get("symbol")), [])
        same_side_binance = binance_by_symbol_side.get(_position_key(trade), [])
        warnings = duplicate_warnings.get(str(trade.get("symbol")), []).copy()
        if same_symbol_binance and not same_side_binance:
            warnings.insert(0, "SIDE_MISMATCH")
            reconcile_status = "SIDE_MISMATCH"
        else:
            warnings.insert(0, "LOCAL_ONLY")
            reconcile_status = "LOCAL_ONLY"
        logs.append(_local_payload(trade, match=None, reconcile_status=reconcile_status, warnings=warnings))
    return logs


def run_position_reconciliation_report(conn: sqlite3.Connection, client: BinanceFuturesClient) -> None:
    if not client.has_credentials:
        console("RECONCILE_POSITION", "blocked", {"reconcile_status": "API_CREDENTIALS_MISSING", "mismatch_warnings": ["API_CREDENTIALS_MISSING"]})
        return

    binance_positions: list[dict[str, Any]] = []
    for runtime_symbol in sorted(ALLOWED_SYMBOLS):
        response = client.get_position_risk(binance_symbol(runtime_symbol))
        symbol_positions = parse_binance_open_positions(response, runtime_symbol)
        binance_positions.extend(symbol_positions)
        if not symbol_positions:
            console(
                "RECONCILE_POSITION",
                runtime_symbol,
                {
                    "source": "BINANCE",
                    "reconcile_status": "NO_BINANCE_POSITION",
                    "symbol": runtime_symbol,
                    "side": None,
                    "qty": 0.0,
                    "entry_price": None,
                    "avg_fill_price": None,
                    "stop_price": None,
                    "tp2_price": None,
                    "status": "NO_BINANCE_POSITION",
                    "has_matching_local_state_row": False,
                    "mismatch_warnings": [],
                },
            )

    local_trades = load_local_open_trades_for_reconciliation(conn)
    if not table_exists(conn, "live_trades_binance"):
        console(
            "RECONCILE_POSITION",
            "local state table missing",
            {"source": "LOCAL", "reconcile_status": "LOCAL_STATE_TABLE_MISSING", "mismatch_warnings": ["LOCAL_STATE_TABLE_MISSING"]},
        )
    for payload in build_position_reconciliation_logs(binance_positions, local_trades):
        console("RECONCILE_POSITION", str(payload.get("symbol", "")), payload)


def run_reconciliation_once(args: argparse.Namespace) -> None:
    demo = bool(getattr(args, "demo", False))
    api_key_env, api_secret_env = binance_env_names(demo)
    client = BinanceFuturesClient(
        os.environ.get(api_key_env),
        os.environ.get(api_secret_env),
        base_url=binance_base_url(demo),
    )
    if bool(getattr(args, "loop", False)):
        console(
            "RECONCILE_POSITION",
            "--loop ignored; reconciliation runs once",
            {"reconcile_status": "LOOP_IGNORED", "mismatch_warnings": []},
        )
    state_db_path = resolve_state_db_path(args)
    with closing(connect_state_db_readonly(state_db_path)) as state_conn:
        log_db_info(state_db_path, state_conn)
        run_position_reconciliation_report(state_conn, client)


def export_trade_journal(conn: sqlite3.Connection, csv_path: str | os.PathLike[str]) -> int:
    """Export persisted Binance live trade rows as a human-readable CSV."""
    if not table_exists(conn, "live_trades_binance"):
        raise RuntimeError("live_trades_binance table does not exist in state DB")

    existing_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(live_trades_binance)").fetchall()}
    source_columns = ["id" if column == "trade_id" else column for column in TRADE_JOURNAL_COLUMNS]
    selected_columns = [column for column in source_columns if column in existing_columns]
    select_clause = ", ".join(selected_columns) if selected_columns else "id"
    rows = conn.execute(
        f"""
        SELECT {select_clause}
        FROM live_trades_binance
        ORDER BY id ASC
        """
    ).fetchall()

    output_path = Path(csv_path)
    if output_path.parent and str(output_path.parent) != ".":
        output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(TRADE_JOURNAL_COLUMNS))
        writer.writeheader()
        for row in rows:
            raw = dict(row)
            journal_row: dict[str, Any] = {}
            for column in TRADE_JOURNAL_COLUMNS:
                source_column = "id" if column == "trade_id" else column
                journal_row[column] = raw.get(source_column, "")
            writer.writerow(journal_row)
    return len(rows)


def run_trade_journal_export_once(args: argparse.Namespace) -> None:
    state_db_path = resolve_state_db_path(args)
    with closing(connect_state_db_readonly(state_db_path)) as state_conn:
        log_db_info(state_db_path, state_conn)
        row_count = export_trade_journal(state_conn, args.export_trade_journal)
    console(
        "TRADE_JOURNAL_EXPORT",
        str(args.export_trade_journal),
        {"rows": row_count, "state_db_path": str(Path(state_db_path).expanduser().absolute())},
    )


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


def effective_notional_cap(*, requested_notional_usdt: Decimal, demo: bool, live_enabled: bool, demo_max_notional_usdt: Decimal, allow_demo_cap_override: bool = False) -> Decimal:
    demo_cap_override_allowed = demo and (live_enabled or allow_demo_cap_override)
    if demo_cap_override_allowed and requested_notional_usdt > MAX_NOTIONAL_USDT and demo_max_notional_usdt >= requested_notional_usdt:
        return demo_max_notional_usdt
    return MAX_NOTIONAL_USDT


def validate_guards(
    conn: sqlite3.Connection,
    client: BinanceFuturesClient,
    signal: TriangleSignal,
    *,
    notional_usdt: Decimal,
    live_enabled: bool,
    demo: bool = False,
    allow_demo_doubles: bool = False,
    allow_demo_pole_motif: bool = False,
    allow_demo_p2_survivor_v1: bool = False,
    demo_max_notional_usdt: Decimal = MAX_NOTIONAL_USDT,
    allow_demo_cap_override: bool = False,
) -> tuple[SymbolSpec | None, dict[str, Any] | None, str | None]:
    if signal.symbol not in ALLOWED_SYMBOLS:
        return None, None, "symbol outside live allowlist"
    demo_doubles_enabled = bool(demo and live_enabled and allow_demo_doubles)
    demo_pole_motif_enabled = bool(demo and live_enabled and allow_demo_pole_motif)
    demo_p2_survivor_enabled = bool(demo and allow_demo_p2_survivor_v1)
    if (
        signal.pattern not in ALLOWED_PATTERNS
        and not (demo_doubles_enabled and signal.pattern in DEMO_DOUBLE_PATTERNS)
        and not (demo_pole_motif_enabled and signal.pattern in DEMO_POLE_MOTIF_PATTERNS)
        and not (demo_p2_survivor_enabled and is_p2_survivor_signal(signal))
    ):
        return None, None, "pattern outside live allowlist"
    if signal.pattern in CATAPULT_SIGNAL_NAMES:
        return None, None, "catapult patterns are log-only"
    risk_reason = validate_risk_levels(signal)
    if risk_reason is not None:
        return None, None, risk_reason
    if signal_exists(conn, signal):
        return None, None, "duplicate signal for same symbol/pattern/trigger timestamp"
    if has_recent_duplicate_setup_trade(conn, signal):
        return None, None, "DUPLICATE_SETUP_COOLDOWN"
    if has_existing_open_trade(conn, signal.symbol):
        return None, None, "existing open live trade on symbol"
    max_notional_usdt = effective_notional_cap(
        requested_notional_usdt=notional_usdt,
        demo=demo,
        live_enabled=live_enabled,
        demo_max_notional_usdt=demo_max_notional_usdt,
        allow_demo_cap_override=allow_demo_cap_override,
    )
    if notional_usdt > max_notional_usdt:
        return None, None, f"notional exceeds effective cap: requested={notional_usdt} cap={max_notional_usdt}"
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
    order, reason = build_entry_order(signal, spec, notional_usdt, max_notional_usdt=max_notional_usdt)
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


def build_forced_demo_signal(symbol: str, side: str, candle: Candle) -> TriangleSignal:
    entry = _dec(candle.close)
    side_upper = str(side).upper()
    if side_upper == "LONG":
        stop = entry * Decimal("0.99")
        tp1 = entry * Decimal("1.01")
        tp2 = entry * Decimal("1.02")
        pattern = "bullish_triangle"
    elif side_upper == "SHORT":
        stop = entry * Decimal("1.01")
        tp1 = entry * Decimal("0.99")
        tp2 = entry * Decimal("0.98")
        pattern = "bearish_triangle"
    else:
        raise ValueError(f"invalid force demo side: {side}")
    return TriangleSignal(
        symbol=symbol,
        pattern=pattern,
        side=side_upper,
        trigger_ts=int(candle.close_time),
        entry_price=entry,
        stop_price=stop,
        tp1_price=tp1,
        tp2_price=tp2,
        trigger_column_idx=-1,
        support_level=min(entry, stop),
        resistance_level=max(entry, stop),
        break_distance_boxes=Decimal("0"),
        pattern_quality="FORCED_DEMO_SELF_TEST",
    )


def hit_target_price(high: Any, low: Any, target: Decimal, side: str) -> bool:
    return _dec(high) >= target if side == "LONG" else _dec(low) <= target


def hit_stop_price(high: Any, low: Any, stop: Decimal, side: str) -> bool:
    return _dec(low) <= stop if side == "LONG" else _dec(high) >= stop


def update_open_trade_exits(
    state_conn: sqlite3.Connection,
    candle_conn_or_client: sqlite3.Connection | BinanceFuturesClient,
    client: BinanceFuturesClient | None = None,
    *,
    live_enabled: bool,
    verbose_market_logs: bool = False,
) -> None:
    # Backward-compatible two-argument form uses the same connection for tests.
    candle_conn = state_conn if client is None else candle_conn_or_client
    active_client = candle_conn_or_client if client is None else client
    rows = state_conn.execute(
        """
        SELECT id, symbol, side, trigger_timestamp, entry_price, stop_price, tp1_price, tp2_price,
               raw_order_response, executed_qty, pattern, break_even_armed, break_even_trigger_price, active_stop_price
        FROM live_trades_binance
        WHERE status IN ('POSITION_OPEN','EXIT_PENDING')
        """
    ).fetchall()
    for row in rows:
        (
            trade_id,
            symbol,
            side,
            entry_time,
            entry,
            stop,
            _tp1,
            tp2,
            raw_order_response,
            executed_qty,
            pattern,
            break_even_armed,
            break_even_trigger,
            active_stop,
        ) = row
        candles = candle_conn.execute(
            """
            SELECT close_time, high, low
            FROM candles
            WHERE symbol IN (?, ?) AND interval = '1m' AND close_time > ?
            ORDER BY close_time ASC
            """,
            (binance_symbol(symbol), symbol, int(entry_time)),
        ).fetchall()
        entry_dec = _dec(entry)
        initial_stop = _dec(stop)
        current_stop = _dec(active_stop) if active_stop not in (None, "") else initial_stop
        target = _dec(tp2)
        is_pole_motif = str(pattern) in DEMO_POLE_MOTIF_PATTERNS
        is_p2_survivor = str(pattern) == P2_SURVIVOR_PATTERN
        armed = bool(break_even_armed)
        be_trigger = _dec(break_even_trigger) if break_even_trigger not in (None, "") else None
        for close_time, high, low in candles:
            exit_price = None
            exit_reason = None

            target_hit = hit_target_price(high, low, target, side)
            stop_hit = hit_stop_price(high, low, current_stop, side)
            trigger_hit = bool((is_pole_motif or is_p2_survivor) and not armed and be_trigger is not None and hit_target_price(high, low, be_trigger, side))

            if target_hit and stop_hit:
                exit_price = current_stop
                exit_reason = "STOP"
            elif target_hit:
                exit_price = target
                exit_reason = "TARGET"
            elif stop_hit:
                exit_price = current_stop
                exit_reason = "STOP"
            elif trigger_hit:
                armed = True
                current_stop = entry_dec
                state_conn.execute(
                    """
                    UPDATE live_trades_binance
                    SET break_even_armed = 1, active_stop_price = ?, notes = ?
                    WHERE id = ? AND status IN ('POSITION_OPEN','EXIT_PENDING')
                    """,
                    (float(entry_dec), "break-even armed after +2R; active stop moved to entry", int(trade_id)),
                )
                state_conn.commit()
                console("BE_ARMED", f"{symbol} trade_id={trade_id}", {"be_trigger": float(be_trigger), "active_stop": float(entry_dec)})
                if hit_stop_price(high, low, current_stop, side):
                    exit_price = current_stop
                    exit_reason = "STOP"
                else:
                    continue

            if exit_price is None:
                continue

            denom = abs(float(entry_dec) - float(initial_stop))
            realized_r = 0.0 if denom == 0 else (
                (float(exit_price) - float(entry_dec)) / denom if side == "LONG" else (float(entry_dec) - float(exit_price)) / denom
            )
            if exit_reason == "TARGET":
                console("TARGET_HIT", f"{symbol} trade_id={trade_id}", {"exit_price": float(exit_price), "realized_r": realized_r})
            else:
                console("STOP_HIT", f"{symbol} trade_id={trade_id}", {"exit_price": float(exit_price), "realized_r": realized_r, "break_even_armed": armed})

            if not live_enabled:
                state_conn.execute(
                    """
                    UPDATE live_trades_binance
                    SET status = 'POSITION_CLOSED', exit_time = ?, exit_price = ?, realized_r = ?, active_stop_price = ?
                    WHERE id = ?
                    """,
                    (int(close_time), float(exit_price), realized_r, float(current_stop), int(trade_id)),
                )
                state_conn.commit()
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

            if not active_client.has_credentials:
                console("ORDER_FAILED", f"{symbol} trade_id={trade_id} close blocked", {"reason": "API credentials missing"})
                break
            try:
                mode_response = active_client.get_position_mode()
                if not position_mode_is_unambiguous(mode_response):
                    raise RuntimeError(f"one-way position mode required for reduceOnly exits: {mode_response}")
                spec = active_client.get_symbol_spec(binance_symbol(symbol))
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
                raw_close_response = active_client.submit_order(close_order or {})
                requested_exit = _dec(close_order["price"]) if close_order is not None else exit_price
            except Exception as exc:
                state_conn.execute(
                    "UPDATE live_trades_binance SET status = 'EXIT_PENDING', notes = ? WHERE id = ?",
                    (f"reduce-only exit failed closed: {exc}", int(trade_id)),
                )
                state_conn.commit()
                console("ORDER_FAILED", f"{symbol} trade_id={trade_id} reduce-only close", {"error": str(exc)})
                break

            state_conn.execute(
                """
                UPDATE live_trades_binance
                SET status = 'POSITION_CLOSED', exit_time = ?, exit_price = ?, realized_r = ?, active_stop_price = ?, notes = ?
                WHERE id = ?
                """,
                (
                    int(close_time),
                    float(exit_price),
                    realized_r,
                    float(current_stop),
                    f"local closed-candle exit; reduce-only close response={json.dumps(raw_close_response, sort_keys=True)}",
                    int(trade_id),
                ),
            )
            state_conn.commit()
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


def mexc_execution_mode_label(mode_name: str) -> tuple[str, str, bool]:
    normalized = mode_name.upper()
    if normalized == "PAPER":
        return "MEXC_PAPER_PHASE_A", "PAPER", True
    if normalized == "LIVE":
        return "MEXC_LIVE_REQUESTED_PHASE_A_FAIL_CLOSED", "DRY_RUN", True
    return "MEXC_DRY_RUN_PHASE_A", "DRY_RUN", True


def log_startup(args: argparse.Namespace) -> None:
    dry_run = not (os.environ.get("LIVE_TRADING_ENABLED") == "1" and not args.dry_run)
    details = {
        "venue": "DEMO" if bool(getattr(args, 'demo', False)) else "PRODUCTION",
        "execution": "DRY_RUN" if dry_run else "LIVE",
        "base_url": binance_base_url(bool(getattr(args, 'demo', False))),
        "dry_run": dry_run,
        "api_key_env": binance_env_names(bool(getattr(args, 'demo', False)))[0],
        "enable_demo_doubles": bool(getattr(args, "enable_demo_doubles", False)),
        "enable_demo_pole_motif": bool(getattr(args, "enable_demo_pole_motif", False)),
        "state_db_path": resolve_state_db_path(args),
        "demo_max_notional_usdt": getattr(args, "demo_max_notional_usdt", str(MAX_NOTIONAL_USDT)),
    }
    mode = execution_mode_label(demo=bool(getattr(args, 'demo', False)), dry_run=dry_run)
    if bool(getattr(args, "execute_mexc_intents", False)):
        mexc_mode, mexc_execution, mexc_dry_run = mexc_execution_mode_label(str(getattr(args, "mexc_demo_or_live_mode_name_if_supported", "DRY_RUN")))
        mode = mexc_mode
        details.update(
            {
                "venue": "MEXC_FUT",
                "execution": mexc_execution,
                "base_url": "MEXC_FUTURES_BASE_URL",
                "dry_run": mexc_dry_run,
                "api_key_env": MEXC_FUTURES_API_KEY_ENV,
            }
        )
    console(
        "STARTUP",
        f"mode={mode}",
        details,
    )
    if getattr(args, "db_path", None):
        log_db_info(args.db_path)
    log_db_info(resolve_state_db_path(args))


def process_once(args: argparse.Namespace, *, iteration: int | None = None) -> None:
    demo = bool(getattr(args, "demo", False))
    enable_demo_doubles = bool(getattr(args, "enable_demo_doubles", False))
    enable_demo_pole_motif = bool(getattr(args, "enable_demo_pole_motif", False))
    enable_demo_p2_survivor_v1 = bool(getattr(args, "enable_demo_p2_survivor_v1", False))
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
    demo_max_notional_usdt = _dec(getattr(args, "demo_max_notional_usdt", str(MAX_NOTIONAL_USDT)))
    state_db_path = resolve_state_db_path(args)
    research_rule = load_research_rule(getattr(args, "research_rule_json", None))

    if research_rule is not None and not (demo or args.dry_run):
        raise RuntimeError("--research-rule-json requires --demo or --dry-run (forward testing only)")
    if enable_demo_pole_motif and not demo:
        raise RuntimeError("--enable-demo-pole-motif requires --demo; pole motif execution is demo-only")
    if enable_demo_p2_survivor_v1 and not demo:
        raise RuntimeError("--enable-demo-p2-survivor-v1 requires --demo; P2 survivor execution is demo-only")

    if bool(getattr(args, "reconcile_positions", False)):
        raise RuntimeError("--reconcile-positions must be handled by main() before process_once()")

    if args.self_test_signal:
        with closing(sqlite3.connect(state_db_path)) as state_conn:
            log_db_info(state_db_path, state_conn)
            init_live_tables(state_conn)
            if os.environ.get("LIVE_TRADING_ENABLED") == "1" and not args.dry_run:
                console("BLOCKED", "self-test refuses to run unless --dry-run is supplied when LIVE_TRADING_ENABLED=1")
                return
            process_self_test_signal(state_conn, client, notional_usdt=notional_usdt)
            return

    with closing(connect_candle_db_readonly(args.db_path)) as candle_conn, closing(sqlite3.connect(state_db_path)) as state_conn:
        log_db_info(args.db_path, candle_conn)
        log_db_info(state_db_path, state_conn)
        if verbose_market_logs:
            log_raw_candle_symbol_max_times(candle_conn)
        init_live_tables(state_conn)
        if bool(getattr(args, "force_demo_order", False)):
            if not demo:
                raise RuntimeError("--force-demo-order requires --demo")
            force_live_allowed = live_enabled and client.has_credentials
            if not (args.dry_run or force_live_allowed):
                raise RuntimeError("--force-demo-order requires --dry-run, or LIVE_TRADING_ENABLED=1 with demo API credentials")
            force_symbol = str(getattr(args, "force_demo_symbol", "BINANCE_FUT:SOLUSDT"))
            force_side = str(getattr(args, "force_demo_side", "LONG")).upper()
            force_notional = _dec(getattr(args, "force_demo_notional_usdt", str(notional_usdt)))
            forced_demo_cap_arg = getattr(args, "force_demo_max_notional_usdt", None)
            forced_demo_cap = _dec(forced_demo_cap_arg) if forced_demo_cap_arg is not None else demo_max_notional_usdt
            candles = load_candles(candle_conn, force_symbol, 1)
            if not candles:
                raise RuntimeError(f"--force-demo-order could not load latest candle for {force_symbol}")
            console("FORCE_DEMO_ORDER_START", force_symbol, {"side": force_side, "notional_usdt": str(force_notional), "forced_demo_cap_usdt": str(forced_demo_cap)})
            signal = build_forced_demo_signal(force_symbol, force_side, candles[-1])
            _spec, order, block_reason = validate_guards(
                state_conn, client, signal, notional_usdt=force_notional, live_enabled=live_enabled, demo=demo, allow_demo_doubles=False, demo_max_notional_usdt=forced_demo_cap
                , allow_demo_cap_override=forced_demo_cap_arg is not None
            )
            if block_reason is not None:
                record_signal(state_conn, signal, decision="BLOCKED", block_reason=block_reason, dry_run=dry_run, notional_usdt=force_notional, notes="forced demo self-test")
                raise RuntimeError(f"FORCE_DEMO_ORDER blocked: {block_reason}")
            console("FORCE_DEMO_ORDER_BUILD", force_symbol, {"entry": str(signal.entry_price), "stop": str(signal.stop_price), "tp1": str(signal.tp1_price), "tp2": str(signal.tp2_price), "forced_demo_cap_usdt": str(forced_demo_cap), "order": order})
            if dry_run:
                record_signal(state_conn, signal, decision="FORCE_DEMO_DRY_RUN", block_reason=None, dry_run=True, notional_usdt=force_notional, raw_order_response={"would_submit_order": order}, notes="forced demo self-test")
                console("FORCE_DEMO_ORDER_DRY_RUN", force_symbol, {"order": order})
            else:
                if order is None:
                    raise RuntimeError("FORCE_DEMO_ORDER could not build order")
                raw_response = client.submit_order(order)
                record_signal(state_conn, signal, decision="FORCE_DEMO_SENT", block_reason=None, dry_run=False, notional_usdt=force_notional, raw_order_response={"order_request": order, "order_response": raw_response}, notes="forced demo self-test")
                console("FORCE_DEMO_ORDER_SENT", force_symbol, {"order": order, "response": raw_response})
                console("FORCE_DEMO_ORDER_SENT", "protective algo TP/SL attach is not part of forced self-test; entry-only self-test")
            console("FORCE_DEMO_ORDER_DONE", force_symbol)
            return

        settings = load_settings(Path(args.settings))
        poll_pending_entry_orders(
            state_conn,
            client,
            live_enabled=live_enabled,
            verbose_market_logs=verbose_market_logs,
            max_pending_entry_minutes=getattr(args, "max_pending_entry_minutes", None),
        )
        update_open_trade_exits(state_conn, candle_conn, client, live_enabled=live_enabled, verbose_market_logs=verbose_market_logs)
        configured_symbols = [s for s in settings.get("symbols", []) if s in ALLOWED_SYMBOLS]
        for symbol in configured_symbols:
            console("SCAN", symbol)
            profile = get_profile(settings, symbol)
            if profile is None:
                console("BLOCKED", f"{symbol} missing PnF profile")
                continue
            candles = load_candles(candle_conn, symbol, args.history_bars)
            if not candles:
                console("BLOCKED", f"{symbol} has no 1m candles")
                continue
            log_market_runtime_compare(symbol, profile, candles)
            if verbose_market_logs:
                log_market_snapshot(symbol, profile, candles)
            latest_close_time = latest_candle_close_time(candles)
            last_processed = get_last_processed_close_time(state_conn, symbol)
            if latest_close_time is None or last_processed == latest_close_time:
                console("NO_SIGNAL", symbol)
                continue
            signal = detect_latest_strict_triangle(symbol, profile, candles)
            allow_demo_doubles = bool(enable_demo_doubles and demo and live_enabled)
            allow_demo_pole_motif = bool(enable_demo_pole_motif and demo and live_enabled)
            allow_demo_p2_survivor_v1 = bool(enable_demo_p2_survivor_v1 and demo)
            if signal is None and allow_demo_doubles:
                signal = detect_latest_strict_double(symbol, profile, candles)
            if signal is None and allow_demo_pole_motif:
                signal = detect_latest_pole_motif_demo_signal(symbol, profile, candles)
            if signal is None and allow_demo_p2_survivor_v1:
                signal = detect_latest_p2_survivor_demo_signal(symbol, profile, candles)
            set_last_processed_close_time(state_conn, symbol, latest_close_time)
            if signal is None:
                console("NO_SIGNAL", symbol)
                continue

            if is_demo_pole_motif_signal(signal) or is_p2_survivor_signal(signal):
                pole_motif_diagnostics = pole_motif_staleness_diagnostics(
                    signal,
                    current_price=candles[-1].close if candles else None,
                    latest_candle_close_time=latest_close_time,
                )
                console(
                    "SETUP_DETECTED",
                    f"{signal.symbol} {signal.pattern} {signal.side}",
                    compact_visibility_payload(
                        {
                            "strategy_id": strategy_id_for_signal(signal),
                            "candidate_id": candidate_id_for_signal(signal),
                            "status": P2_SURVIVOR_STATUS if is_p2_survivor_signal(signal) else None,
                            "entry_model": POLE_MOTIF_ENTRY_MODEL,
                            "entry": signal.entry_price,
                            "stop": signal.stop_price,
                            "target": signal.tp2_price,
                            "be_trigger": signal.tp1_price,
                            "pattern_quality": signal.pattern_quality,
                            **pole_motif_diagnostics,
                        }
                    ),
                )
            console("SIGNAL", f"{signal.symbol} {signal.pattern} {signal.side}", signal.__dict__)
            if verbose_market_logs or is_demo_pole_motif_signal(signal) or is_p2_survivor_signal(signal):
                log_signal_detail(
                    signal,
                    profile,
                    candles[-1].close if candles else None,
                    latest_candle_close_time=latest_close_time,
                )
            _spec, order, block_reason = validate_guards(
                state_conn, client, signal, notional_usdt=notional_usdt, live_enabled=live_enabled, demo=demo, allow_demo_doubles=allow_demo_doubles, allow_demo_pole_motif=allow_demo_pole_motif, allow_demo_p2_survivor_v1=allow_demo_p2_survivor_v1, demo_max_notional_usdt=demo_max_notional_usdt
            )
            if verbose_market_logs and order is not None:
                log_order_detail(signal.symbol, order)
            if block_reason is not None:
                record_signal(
                    state_conn,
                    signal,
                    decision="BLOCKED",
                    block_reason=block_reason,
                    dry_run=dry_run,
                    notional_usdt=notional_usdt,
                    notes=append_demo_double_note(signal, "fail-closed guard"),
                )
                console("BLOCKED", f"{signal.symbol} {signal.pattern}", {"reason": block_reason})
                continue

            if research_rule is not None:
                setup = dict(research_rule.get("setup", {}))
                setup.setdefault("symbol", signal.symbol)
                setup.setdefault("side", signal.side)
                matched, reject_reason = evaluate_research_rule(setup, research_rule)
                rule_payload = {
                    "symbol": setup.get("symbol"),
                    "side": setup.get("side"),
                    "status": setup.get("status"),
                    "breakout_context": setup.get("breakout_context"),
                    "pullback_quality": setup.get("pullback_quality"),
                    "trend_regime": setup.get("trend_regime"),
                    "continuation_execution_class": setup.get("continuation_execution_class"),
                    "entry_distance_bucket": setup.get("entry_distance_bucket"),
                    "active_leg_boxes": setup.get("active_leg_boxes"),
                    "quality_score": setup.get("quality_score"),
                    "rule_id": research_rule.get("rule_id"),
                    "rule_match": matched,
                    "reject_reason": reject_reason,
                }
                console("RULE_MATCHED" if matched else "RULE_REJECTED", signal.symbol, rule_payload)
                if not matched:
                    continue

            if dry_run:
                record_signal(
                    state_conn,
                    signal,
                    decision="DRY_RUN",
                    block_reason=None,
                    dry_run=True,
                    notional_usdt=notional_usdt,
                    raw_order_response={"would_submit_order": order},
                    notes=append_demo_double_note(signal, "LIVE_TRADING_ENABLED is not 1 or --dry-run was supplied"),
                )
                record_trade(
                    state_conn,
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
                    record_signal(state_conn, signal, decision="BLOCKED", block_reason=reason, dry_run=False, notional_usdt=notional_usdt, notes=append_demo_double_note(signal, json.dumps(position_response)))
                    console("BLOCKED", f"{signal.symbol} {signal.pattern}", {"reason": reason})
                    continue
                lifecycle_result = record_submitted_entry_order(
                    state_conn,
                    client,
                    signal,
                    order=order or {},
                    notional_usdt=notional_usdt,
                    verbose_market_logs=verbose_market_logs,
                )
            except Exception as exc:
                raw_response = {"error": str(exc)}
                record_signal(state_conn, signal, decision="ORDER_FAILED", block_reason=str(exc), dry_run=False, notional_usdt=notional_usdt, raw_order_response={"order_request": order, "order_response": raw_response}, notes=append_demo_double_note(signal, "API/order error; fail closed"))
                record_trade(state_conn, signal, notional_usdt=notional_usdt, exchange_order_id=None, status="ORDER_FAILED", dry_run=False, decision="ORDER_FAILED", block_reason=str(exc), raw_order_response={"order_request": order, "order_response": raw_response}, notes=append_demo_double_note(signal, "API/order error; fail closed"))
                console("ORDER_FAILED", f"{signal.symbol} {signal.pattern}", raw_response)
                continue

            console("ORDER_SENT", f"{signal.symbol} {signal.pattern}", lifecycle_result)


def process_setup_execution_once(args: argparse.Namespace, *, iteration: int | None = None) -> None:
    loop_iteration = 1 if iteration is None else iteration
    console("LOOP_BEGIN", f"iteration={loop_iteration}")
    console("SETUP_CONSUMER_MODE", "ENABLED")
    execution_venue = "BINANCE_DEMO" if bool(getattr(args, "demo", False)) else None
    setup_db_path = getattr(args, "strategy_setups_db", None)
    if not setup_db_path:
        raise RuntimeError("--strategy-setups-db is required when --consume-strategy-setups is supplied")
    state_db_path = getattr(args, "state_db_path", None)
    if not state_db_path:
        raise RuntimeError("--state-db-path is required when --consume-strategy-setups is supplied")

    with closing(connect_strategy_setups_db_readonly(setup_db_path)) as setup_conn:
        rows = load_executable_strategy_setups(setup_conn)

    console("SETUP_ROWS_FOUND", str(len(rows)))
    accepted = 0
    rejected = 0
    intents_created = 0
    intents_skipped = 0
    with closing(sqlite3.connect(state_db_path)) as state_conn:
        init_executed_setup_candidates_table(state_conn)
        init_execution_intents_table(state_conn)
        for row in rows:
            intent, reject_reason = setup_row_to_execution_intent(row)
            row_payload = {
                "setup_id": row["setup_id"] if "setup_id" in row.keys() else None,
                "symbol": row["symbol"] if "symbol" in row.keys() else None,
                "reason": reject_reason,
            }
            if intent is None:
                rejected += 1
                console("SETUP_EXECUTION_REJECTED", "", row_payload)
                continue
            if not is_setup_symbol_supported_for_execution(intent.symbol, execution_venue):
                rejected += 1
                console(
                    "SETUP_EXECUTION_REJECTED",
                    "",
                    {"setup_id": intent.setup_id, "symbol": intent.symbol, "reason": UNSUPPORTED_EXECUTION_VENUE},
                )
                continue
            accepted += 1
            is_first_seen = mark_setup_candidate_seen(state_conn, intent.setup_id)
            if is_first_seen:
                console("SETUP_EXECUTION_CANDIDATE", "", setup_execution_payload(intent))
            else:
                console("SETUP_ALREADY_SEEN", "", {"setup_id": intent.setup_id, "symbol": intent.symbol})
            if create_execution_intent(state_conn, intent):
                intents_created += 1
                console("EXECUTION_INTENT_CREATED", "", {
                    "setup_id": intent.setup_id,
                    "symbol": intent.symbol,
                    "side": intent.side,
                    "entry": str(intent.entry),
                    "stop": str(intent.stop),
                })
            else:
                intents_skipped += 1
                console("EXECUTION_INTENT_ALREADY_EXISTS", "", {"setup_id": intent.setup_id, "symbol": intent.symbol})
    console("SETUP_ROWS_ACCEPTED", str(accepted))
    console("INTENTS_CREATED", str(intents_created))
    console("INTENTS_SKIPPED", str(intents_skipped))
    console("SETUP_ROWS_REJECTED", str(rejected))
    if accepted == 0:
        console("NO_EXECUTABLE_SETUPS", "")



def position_size_is_zero(position_response: Any) -> bool:
    rows = _position_risk_rows(position_response)
    if not rows:
        return True
    for row in rows:
        try:
            if _dec(row.get("positionAmt", "0")) != 0:
                return False
        except (AttributeError, ValueError):
            return False
    return True


def _trade_time(trade: dict[str, Any]) -> int:
    for key in ("time", "updateTime"):
        if trade.get(key) not in (None, ""):
            try:
                return int(trade[key])
            except (TypeError, ValueError):
                return 0
    return 0


def _trade_order_id_matches(trade: dict[str, Any], order_id: str) -> bool:
    raw_order_id = trade.get("orderId", trade.get("orderID"))
    return raw_order_id not in (None, "") and str(raw_order_id) == str(order_id)


def _trade_is_exit_side(trade: dict[str, Any], side: str) -> bool:
    raw_side = str(trade.get("side") or "").upper()
    if not raw_side:
        return True
    expected = "SELL" if side.upper() == "LONG" else "BUY"
    return raw_side == expected


def _filled_trade_metrics(user_trades: Any, fallback_exit_price: Decimal) -> tuple[Decimal, Decimal | None, int | None]:
    if not isinstance(user_trades, list):
        return Decimal("0"), None, None
    pnl = Decimal("0")
    notional_qty = Decimal("0")
    notional_px_qty = Decimal("0")
    latest_ts: int | None = None
    for trade in user_trades:
        if not isinstance(trade, dict):
            continue
        if trade.get("realizedPnl") not in (None, ""):
            try:
                pnl += _dec(trade.get("realizedPnl"))
            except ValueError:
                pass
        try:
            qty = abs(_dec(trade.get("qty", trade.get("quantity", "0"))))
            price = _dec(trade.get("price"))
        except ValueError:
            qty = Decimal("0")
            price = fallback_exit_price
        if qty > 0:
            notional_qty += qty
            notional_px_qty += price * qty
        ts = _trade_time(trade)
        if ts and (latest_ts is None or ts > latest_ts):
            latest_ts = ts
    exit_price = (notional_px_qty / notional_qty) if notional_qty > 0 else None
    return pnl, exit_price, latest_ts


def _valid_protective_trades(user_trades: Any, *, order_id: str, entry_fill_time: int, side: str) -> list[dict[str, Any]]:
    if not isinstance(user_trades, list):
        return []
    valid: list[dict[str, Any]] = []
    for trade in user_trades:
        if not isinstance(trade, dict):
            continue
        trade_time = _trade_time(trade)
        if trade_time < entry_fill_time:
            continue
        if not _trade_order_id_matches(trade, order_id):
            continue
        if not _trade_is_exit_side(trade, side):
            continue
        valid.append(trade)
    return valid


def _algo_order_status(client: BinanceFuturesClient, symbol: str, order_id: str | None) -> str | None:
    if order_id in (None, ""):
        return None
    try:
        response = client.get_algo_order(symbol, algo_id=str(order_id))
    except Exception:
        return None
    return str(response.get("status") or response.get("orderStatus") or "").upper()


def _protective_closure_candidate(
    client: BinanceFuturesClient,
    symbol: str,
    *,
    order_id: str | None,
    entry_fill_time: int | None,
    side: str,
) -> tuple[bool, list[dict[str, Any]], int | None, str]:
    """Return whether one stored protective order causally closed this trade.

    Deterministic ambiguity rules:
    - Only the single stored protective order id for this trade is eligible; duplicate
      TP/STOP orders not persisted on the row are ignored instead of guessed.
    - A FILLED/FINISHED algo status is insufficient by itself.  At least one user
      trade for the same stored order must have an execution timestamp at or after
      the entry fill timestamp.  Older fills are treated as stale and fall back to
      MANUAL classification.
    - If both stored TP and STOP independently pass this causality check, the close
      is ambiguous and falls back to MANUAL rather than choosing a winner.
    """
    if order_id in (None, ""):
        return False, [], None, "missing_protective_order_id"
    if entry_fill_time is None:
        return False, [], None, "missing_entry_fill_time"
    status = _algo_order_status(client, symbol, order_id)
    if status not in {"FILLED", "FINISHED"}:
        return False, [], None, f"protective_status_not_filled:{status or 'MISSING'}"
    trades = client.get_user_trades(symbol, order_id=str(order_id), start_time=entry_fill_time, limit=1000)
    valid_trades = _valid_protective_trades(trades, order_id=str(order_id), entry_fill_time=entry_fill_time, side=side)
    if not valid_trades:
        return False, [], None, "no_causal_protective_execution"
    fill_time = max(_trade_time(trade) for trade in valid_trades)
    return True, valid_trades, fill_time, "causal_protective_execution"


def _bounded_manual_trades(
    client: BinanceFuturesClient,
    symbol: str,
    *,
    entry_fill_time: int | None,
    side: str,
    executed_qty: Decimal,
) -> tuple[list[dict[str, Any]], int | None, str]:
    if entry_fill_time is None:
        return [], None, "missing_entry_fill_time"
    trades = client.get_user_trades(symbol, start_time=entry_fill_time, end_time=int(time.time() * 1000), limit=1000)
    if not isinstance(trades, list):
        return [], None, "manual_user_trades_unavailable"
    selected: list[dict[str, Any]] = []
    cumulative_qty = Decimal("0")
    for trade in sorted((trade for trade in trades if isinstance(trade, dict)), key=_trade_time):
        trade_time = _trade_time(trade)
        if trade_time < entry_fill_time:
            continue
        if not _trade_is_exit_side(trade, side):
            continue
        if trade.get("realizedPnl") in (None, ""):
            continue
        selected.append(trade)
        try:
            cumulative_qty += abs(_dec(trade.get("qty", trade.get("quantity", "0"))))
        except ValueError:
            pass
        if executed_qty > 0 and cumulative_qty >= executed_qty:
            break
    close_time = max((_trade_time(trade) for trade in selected), default=None)
    return selected, close_time, "bounded_manual_exit_trades"


def _classification_log(event: str, *, trade_id: int, symbol: str, entry_fill_time: int | None, protective_fill_time: int | None, close_time: int | None, classification_reason: str) -> None:
    console(
        event,
        f"{symbol} trade_id={trade_id}",
        {
            "trade_id": trade_id,
            "symbol": symbol,
            "entry_fill_time": entry_fill_time,
            "protective_fill_time": protective_fill_time,
            "close_time": close_time,
            "classification_reason": classification_reason,
        },
    )


def sync_closed_execution_positions(conn: sqlite3.Connection, client: BinanceFuturesClient) -> None:
    rows = conn.execute(
        """
        SELECT id, symbol, side, entry_price, avg_fill_price, executed_qty, entry_order_update_time,
               stop_algo_id, tp_algo_id
        FROM live_trades_binance
        WHERE status IN ('POSITION_OPEN','POSITION_OPEN_UNPROTECTED')
          AND setup_id IS NOT NULL AND setup_id != ''
          AND intent_id IS NOT NULL AND intent_id != ''
        ORDER BY id ASC
        """
    ).fetchall()
    console("OPEN_POSITION_CLOSURE_SYNC_FOUND", str(len(rows)))
    for row in rows:
        trade_id = int(row["id"])
        symbol = str(row["symbol"])
        exchange_symbol = binance_symbol(symbol)
        response = client.get_position_risk(exchange_symbol)
        if not position_size_is_zero(response):
            continue
        entry_fill_time = int(row["entry_order_update_time"]) if row["entry_order_update_time"] not in (None, "") else None
        side = str(row["side"]).upper()
        executed_qty = _dec(row["executed_qty"] if row["executed_qty"] not in (None, "") else "0")
        stop_valid, stop_trades, stop_fill_time, stop_reason = _protective_closure_candidate(
            client,
            exchange_symbol,
            order_id=row["stop_algo_id"],
            entry_fill_time=entry_fill_time,
            side=side,
        )
        tp_valid, tp_trades, tp_fill_time, tp_reason = _protective_closure_candidate(
            client,
            exchange_symbol,
            order_id=row["tp_algo_id"],
            entry_fill_time=entry_fill_time,
            side=side,
        )
        fallback_exit = _dec(row["avg_fill_price"] if row["avg_fill_price"] not in (None, "") else row["entry_price"])
        if tp_valid and not stop_valid:
            status = close_reason = "POSITION_CLOSED_TP"
            trades = tp_trades
            protective_fill_time = tp_fill_time
            classification_reason = "tp_causal_protective_execution"
            log_event = "CLOSE_CLASSIFICATION_TP"
        elif stop_valid and not tp_valid:
            status = close_reason = "POSITION_CLOSED_STOP"
            trades = stop_trades
            protective_fill_time = stop_fill_time
            classification_reason = "stop_causal_protective_execution"
            log_event = "CLOSE_CLASSIFICATION_STOP"
        else:
            status = close_reason = "POSITION_CLOSED_MANUAL"
            trades, manual_close_time, manual_reason = _bounded_manual_trades(
                client,
                exchange_symbol,
                entry_fill_time=entry_fill_time,
                side=side,
                executed_qty=executed_qty,
            )
            protective_fill_time = None
            if tp_valid and stop_valid:
                classification_reason = "ambiguous_tp_and_stop_causal_executions"
            else:
                classification_reason = f"manual_fallback:tp={tp_reason};stop={stop_reason};manual={manual_reason}"
            log_event = "CLOSE_CLASSIFICATION_MANUAL"
        pnl, exit_price, exit_ts = _filled_trade_metrics(trades, fallback_exit)
        if exit_price is None:
            exit_price = fallback_exit
        close_time = exit_ts if exit_ts is not None else (protective_fill_time if protective_fill_time is not None else None)
        _classification_log(
            log_event,
            trade_id=trade_id,
            symbol=symbol,
            entry_fill_time=entry_fill_time,
            protective_fill_time=protective_fill_time,
            close_time=close_time,
            classification_reason=classification_reason,
        )
        entry_price = _dec(row["avg_fill_price"] if row["avg_fill_price"] not in (None, "") else row["entry_price"])
        denominator = entry_price * executed_qty
        pnl_pct = (pnl / denominator * Decimal("100")) if denominator != 0 else Decimal("0")
        cursor = conn.execute(
            """
            UPDATE live_trades_binance
            SET status = ?, exit_time = ?, exit_ts = ?, exit_price = ?, realized_pnl = ?,
                realized_pnl_pct = ?, close_reason = ?, notes = ?
            WHERE id = ? AND status IN ('POSITION_OPEN','POSITION_OPEN_UNPROTECTED')
            """,
            (
                status,
                close_time,
                close_time,
                float(exit_price),
                float(pnl),
                float(pnl_pct),
                close_reason,
                f"execution trade sync: {close_reason}; {classification_reason}",
                trade_id,
            ),
        )
        if cursor.rowcount:
            conn.commit()
            console(close_reason, f"{symbol} trade_id={trade_id}", {"exit_price": str(exit_price), "realized_pnl": str(pnl), "realized_pnl_pct": str(pnl_pct), "exit_ts": close_time})


def process_execution_intents_once(args: argparse.Namespace, *, iteration: int | None = None) -> None:
    loop_iteration = 1 if iteration is None else iteration
    console("LOOP_BEGIN", f"iteration={loop_iteration}")
    console("EXECUTION_INTENT_EXECUTOR_MODE", "ENABLED")
    state_db_path = resolve_state_db_path(args)
    demo = bool(getattr(args, "demo", False))
    dry_run_flag = bool(getattr(args, "dry_run", False))
    live_enabled = os.environ.get("LIVE_TRADING_ENABLED") == "1"
    api_key_env, api_secret_env = binance_env_names(demo)
    notional_usdt = _dec(getattr(args, "notional_usdt", str(DEFAULT_NOTIONAL_USDT)))
    client = BinanceFuturesClient(
        os.environ.get(api_key_env),
        os.environ.get(api_secret_env),
        base_url=binance_base_url(demo),
    )
    with closing(sqlite3.connect(state_db_path)) as conn:
        conn.row_factory = sqlite3.Row
        init_live_tables(conn)
        init_execution_intents_table(conn)
        rows = load_execution_intents(conn)
        console("EXECUTION_INTENTS_FOUND", str(len(rows)))
        sent = 0
        rejected = 0
        failed = 0
        for row in rows:
            if not demo:
                rejected += 1
                reject_execution_intent(row, "requires --demo; mainnet execution-intent path is disabled")
                continue
            if dry_run_flag:
                rejected += 1
                reject_execution_intent(row, "blocked by --dry-run")
                continue
            if not live_enabled:
                rejected += 1
                reject_execution_intent(row, "LIVE_TRADING_ENABLED is not 1")
                continue
            if row["intent_status"] != EXECUTION_INTENT_STATUS_NEW:
                rejected += 1
                reject_execution_intent(row, f"intent_status is not NEW: {row['intent_status']}")
                continue
            signal = execution_intent_to_signal(row)
            if signal.side != "LONG":
                rejected += 1
                reject_execution_intent(row, "SHORT execution intents are blocked")
                continue
            if not is_setup_symbol_supported_for_execution(signal.symbol, "BINANCE_DEMO"):
                rejected += 1
                reject_execution_intent(row, UNSUPPORTED_EXECUTION_VENUE)
                continue
            risk_reason = validate_risk_levels(signal)
            if risk_reason is not None:
                rejected += 1
                reject_execution_intent(row, risk_reason)
                continue
            if has_existing_open_trade(conn, signal.symbol):
                rejected += 1
                reject_execution_intent(row, "existing open live trade on symbol")
                continue
            if setup_already_executed(conn, row["setup_id"]):
                rejected += 1
                reject_execution_intent(row, "setup_id already executed")
                continue
            if not client.has_credentials:
                rejected += 1
                reject_execution_intent(row, "API credentials missing")
                continue
            try:
                spec = client.get_symbol_spec(binance_symbol(signal.symbol))
                order, reason = build_entry_order(
                    signal,
                    spec,
                    notional_usdt,
                    max_notional_usdt=EXECUTION_INTENT_DEMO_MAX_NOTIONAL_USDT,
                )
                if reason is not None or order is None:
                    rejected += 1
                    reject_execution_intent(row, reason or "order build failed")
                    continue
                order["newClientOrderId"] = f"pnf-intent-{str(row['intent_id']).replace('intent-', '')}"[:36]
                log_order_detail(signal.symbol, order)
                response = client.submit_order(order)
                record_execution_intent_trade(
                    conn,
                    signal,
                    intent_id=row["intent_id"],
                    setup_id=row["setup_id"],
                    notional_usdt=notional_usdt,
                    order=order,
                    response=response,
                )
                sent += 1
                console("EXECUTION_INTENT_ORDER_SENT", "", {"intent_id": row["intent_id"], "setup_id": row["setup_id"], "symbol": signal.symbol, "response": response})
            except Exception as exc:
                failed += 1
                conn.rollback()
                console("EXECUTION_INTENT_ORDER_FAILED", "", {"intent_id": row["intent_id"], "setup_id": row["setup_id"], "symbol": row["symbol"], "error": str(exc)})
        console("EXECUTION_INTENTS_SENT", str(sent))
        console("EXECUTION_INTENTS_REJECTED", str(rejected))
        console("EXECUTION_INTENTS_FAILED", str(failed))


def record_mexc_execution_intent_trade(
    conn: sqlite3.Connection,
    signal: TriangleSignal,
    *,
    intent_id: str,
    setup_id: str,
    plan: MexcFuturesOrderPlan,
    order: dict[str, Any],
    response: dict[str, Any],
    dry_run: bool,
) -> None:
    conn.execute(
        """
        INSERT INTO live_trades_binance(
            created_at, symbol, pattern, strategy_id, candidate_id, side, trigger_timestamp, entry_price,
            stop_price, tp1_price, tp2_price, notional_usdt, decision, status,
            block_reason, dry_run, exchange_order_id, raw_order_response, notes,
            break_even_armed, active_stop_price, entry_order_status, protective_orders_status, setup_id, intent_id
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_iso(),
            plan.symbol,
            "execution_intent",
            "MEXC_FUT",
            setup_id,
            signal.side,
            signal.trigger_ts,
            float(plan.entry),
            float(plan.stop),
            float(plan.tp1),
            float(plan.tp2),
            float(plan.notional_usdt),
            "DRY_RUN" if dry_run else "ORDER_SENT",
            "DRY_RUN" if dry_run else "ORDER_SENT",
            None,
            int(dry_run),
            str(response.get("orderId") or response.get("data") or "") or None,
            json.dumps({"order_request": order, "order_response": response, "venue": "MEXC_FUT"}, sort_keys=True, default=str),
            "MEXC Futures execution intent dry-run; Phase B live submit remains fail-closed" if dry_run else "MEXC Futures execution intent order submitted; protective TP/SL pending fill sync",
            0,
            float(plan.stop),
            "DRY_RUN" if dry_run else str(response.get("status") or "NEW"),
            "DRY_RUN" if dry_run else "PENDING_ENTRY_FILL",
            setup_id,
            intent_id,
        ),
    )
    if dry_run:
        conn.execute(
            "UPDATE execution_intents SET intent_status = ? WHERE intent_id = ? AND intent_status = ?",
            (EXECUTION_INTENT_STATUS_READY, intent_id, EXECUTION_INTENT_STATUS_NEW),
        )
    conn.commit()


def process_mexc_execution_intents_once(args: argparse.Namespace, *, iteration: int | None = None) -> None:
    loop_iteration = 1 if iteration is None else iteration
    console("LOOP_BEGIN", f"iteration={loop_iteration}")
    console("MEXC_EXECUTION_INTENT_EXECUTOR_MODE", "DRY_RUN_PHASE_A")
    state_db_path = resolve_state_db_path(args)
    live_requested = str(getattr(args, "mexc_demo_or_live_mode_name_if_supported", "")).upper() == "LIVE"
    live_enabled = live_requested and os.environ.get("LIVE_TRADING_ENABLED") == "1"
    client = MexcFuturesExecutionClient(
        os.environ.get(MEXC_FUTURES_API_KEY_ENV),
        os.environ.get(MEXC_FUTURES_API_SECRET_ENV),
        base_url=MEXC_FUTURES_BASE_URL,
    )
    with closing(sqlite3.connect(state_db_path)) as conn:
        conn.row_factory = sqlite3.Row
        init_live_tables(conn)
        init_execution_intents_table(conn)
        rows = load_execution_intents(conn)
        sent = 0
        rejected = 0
        for row in rows:
            def reject(reason: str) -> None:
                nonlocal rejected
                rejected += 1
                console("MEXC_INTENT_REJECTED", "", {"intent_id": row["intent_id"], "setup_id": row["setup_id"], "symbol": row["symbol"], "reason": reason})

            if row["intent_status"] != EXECUTION_INTENT_STATUS_NEW:
                reject(f"intent_status is not NEW: {row['intent_status']}")
                continue
            signal = execution_intent_to_signal(row)
            if not is_mexc_futures_symbol_allowed(signal.symbol):
                reject(UNSUPPORTED_EXECUTION_VENUE)
                continue
            signal = TriangleSignal(**{**signal.__dict__, "symbol": f"MEXC_FUT:{normalize_mexc_futures_symbol(signal.symbol)}"})
            if signal.side != "LONG":
                reject("SHORT execution intents are blocked")
                continue
            if count_open_mexc_execution_trades(conn) >= MEXC_FUTURES_MAX_OPEN_POSITIONS:
                reject("max open MEXC positions reached")
                continue
            if mexc_setup_already_executed(conn, row["setup_id"]):
                reject("setup_id already executed on MEXC")
                continue
            if not client.has_credentials:
                reject("API credentials missing")
                continue
            plan, reason = calculate_mexc_futures_position_size(signal)
            if reason is not None or plan is None:
                reject(reason or "position sizing failed")
                continue
            console("MEXC_POSITION_SIZE", "", {"intent_id": row["intent_id"], "symbol": plan.symbol, "quantity": str(plan.quantity), "notional_usdt": str(plan.notional_usdt), "risk_usdt": str(plan.risk_usdt)})
            console("MEXC_RISK_CHECK", "", {"risk_cap_usdt": str(MEXC_FUTURES_RISK_PER_TRADE_USDT), "bankroll_cap_usdt": str(MEXC_FUTURES_MAX_BANKROLL_USDT), "leverage": str(MEXC_FUTURES_DEFAULT_LEVERAGE)})
            order = mexc_order_from_plan(row, plan)
            console("MEXC_INTENT_ACCEPTED", "", {"intent_id": row["intent_id"], "setup_id": row["setup_id"], "symbol": plan.symbol})
            if not live_enabled:
                record_mexc_execution_intent_trade(conn, signal, intent_id=row["intent_id"], setup_id=row["setup_id"], plan=plan, order=order, response={"dry_run": True}, dry_run=True)
                sent += 1
                console("MEXC_ORDER_DRY_RUN", "", {"intent_id": row["intent_id"], "setup_id": row["setup_id"], "symbol": plan.symbol, "order": order})
                continue
            reject("MEXC live order submission is fail-closed until Phase B is explicitly implemented")
        console("MEXC_INTENTS_ACCEPTED", str(sent))
        console("MEXC_INTENTS_REJECTED", str(rejected))


def process_sync_mexc_trades_once(args: argparse.Namespace, *, iteration: int | None = None) -> None:
    loop_iteration = 1 if iteration is None else iteration
    console("LOOP_BEGIN", f"iteration={loop_iteration}")
    console("MEXC_TRADE_SYNC_MODE", "DRY_RUN_PHASE_A")
    raise RuntimeError("--sync-mexc-trades is fail-closed until MEXC Phase B fill/protective-order sync is implemented")


def process_sync_execution_trades_once(args: argparse.Namespace, *, iteration: int | None = None) -> None:
    loop_iteration = 1 if iteration is None else iteration
    console("LOOP_BEGIN", f"iteration={loop_iteration}")
    console("EXECUTION_TRADE_SYNC_MODE", "ENABLED")
    state_db_path = resolve_state_db_path(args)
    demo = bool(getattr(args, "demo", False))
    dry_run_flag = bool(getattr(args, "dry_run", False))
    live_enabled = os.environ.get("LIVE_TRADING_ENABLED") == "1"
    if not demo:
        raise RuntimeError("--sync-execution-trades requires --demo; mainnet sync is disabled")
    if dry_run_flag:
        raise RuntimeError("--sync-execution-trades is blocked by --dry-run")
    if not live_enabled:
        raise RuntimeError("--sync-execution-trades requires LIVE_TRADING_ENABLED=1")

    api_key_env, api_secret_env = binance_env_names(demo)
    client = BinanceFuturesClient(
        os.environ.get(api_key_env),
        os.environ.get(api_secret_env),
        base_url=binance_base_url(demo),
    )
    if not client.has_credentials:
        raise RuntimeError("--sync-execution-trades requires Binance Demo API credentials")

    with closing(sqlite3.connect(state_db_path)) as conn:
        conn.row_factory = sqlite3.Row
        init_live_tables(conn)
        unprotected_rows = conn.execute(
            """
            SELECT id, created_at, symbol, pattern, side, trigger_timestamp, entry_price, stop_price,
                   tp1_price, tp2_price, raw_order_response, entry_order_status, setup_id, intent_id,
                   stop_algo_id, tp_algo_id, protective_orders_status, active_stop_price, executed_qty
            FROM live_trades_binance
            WHERE status = 'POSITION_OPEN_UNPROTECTED'
              AND setup_id IS NOT NULL AND setup_id != ''
              AND intent_id IS NOT NULL AND intent_id != ''
            ORDER BY id ASC
            """
        ).fetchall()
        console("UNPROTECTED_TRADES_SYNC_FOUND", str(len(unprotected_rows)))
        for row in unprotected_rows:
            trade_id = int(row["id"])
            signal = trade_row_to_signal(row)
            if emergency_close_unprotected_if_stop_violated(conn, client, trade_id, signal):
                continue
            attach_protective_algo_orders(conn, client, trade_id, signal, dry_run=False)
            protected = conn.execute(
                "SELECT protective_orders_status FROM live_trades_binance WHERE id = ?",
                (trade_id,),
            ).fetchone()
            if protected is not None and protected[0] == "ATTACHED":
                conn.execute(
                    "UPDATE live_trades_binance SET status = ?, notes = ? WHERE id = ?",
                    ("POSITION_OPEN", "execution trade sync: protective orders attached on retry", trade_id),
                )
                conn.commit()
                console("PROTECTIVE_ORDERS_ATTACHED", f"{signal.symbol} trade_id={trade_id} retry")
            else:
                conn.execute(
                    "UPDATE live_trades_binance SET status = ?, notes = ? WHERE id = ?",
                    ("POSITION_OPEN_UNPROTECTED", "execution trade sync: protective order retry failed; still unprotected", trade_id),
                )
                conn.commit()
                console("PROTECTIVE_ORDER_FAILED", f"{signal.symbol} trade_id={trade_id} still UNPROTECTED")

        rows = conn.execute(
            """
            SELECT id, created_at, symbol, pattern, side, trigger_timestamp, entry_price, stop_price,
                   tp1_price, tp2_price, raw_order_response, entry_order_status, setup_id, intent_id,
                   stop_algo_id, tp_algo_id, protective_orders_status
            FROM live_trades_binance
            WHERE status = 'ORDER_SENT'
              AND COALESCE(entry_order_status, 'NEW') IN ('NEW', 'PARTIALLY_FILLED', 'FILLED')
              AND setup_id IS NOT NULL AND setup_id != ''
              AND intent_id IS NOT NULL AND intent_id != ''
            ORDER BY id ASC
            """
        ).fetchall()
        console("EXECUTION_TRADES_SYNC_FOUND", str(len(rows)))
        for row in rows:
            trade_id = int(row["id"])
            signal = trade_row_to_signal(row)
            raw = json.loads(row["raw_order_response"] or "{}") if row["raw_order_response"] else {}
            order_request = raw.get("order_request") if isinstance(raw, dict) else None
            order_response = raw.get("order_response") if isinstance(raw, dict) else None
            try:
                status_response, trades_response, lifecycle = poll_entry_order_status(client, signal, order_request, order_response)
                entry_status = str(lifecycle.get("entry_order_status") or "")
                merged_raw = {
                    "order_request": order_request,
                    "order_response": order_response,
                    "order_status": status_response,
                    "user_trades": trades_response,
                }
                if entry_status != "FILLED":
                    conn.execute(
                        """
                        UPDATE live_trades_binance
                        SET entry_order_status = ?, executed_qty = ?, avg_fill_price = ?,
                            entry_order_update_time = ?, order_status_response = ?,
                            user_trades_response = ?, raw_order_response = ?, notes = ?
                        WHERE id = ?
                        """,
                        (
                            lifecycle.get("entry_order_status"),
                            lifecycle.get("executed_qty"),
                            lifecycle.get("avg_fill_price"),
                            lifecycle.get("entry_order_update_time"),
                            json.dumps(status_response, sort_keys=True, default=str),
                            json.dumps(trades_response, sort_keys=True, default=str),
                            json.dumps(merged_raw, sort_keys=True, default=str),
                            f"execution trade sync: entry not filled; status={entry_status}",
                            trade_id,
                        ),
                    )
                    conn.commit()
                    console("ENTRY_NOT_FILLED", f"{signal.symbol} trade_id={trade_id}", lifecycle)
                    continue

                conn.execute(
                    """
                    UPDATE live_trades_binance
                    SET status = 'POSITION_OPEN_UNPROTECTED', entry_order_status = ?, avg_fill_price = ?, executed_qty = ?,
                        entry_order_update_time = ?, entry_commission = ?, entry_commission_asset = ?,
                        entry_slippage = ?, entry_slippage_bps = ?, fees = ?, order_status_response = ?,
                        user_trades_response = ?, raw_order_response = ?, notes = ?
                    WHERE id = ?
                    """,
                    (
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
                        json.dumps(merged_raw, sort_keys=True, default=str),
                        "execution trade sync: entry filled; emergency/protective decision pending",
                        trade_id,
                    ),
                )
                conn.commit()
                if emergency_close_unprotected_if_stop_violated(conn, client, trade_id, signal):
                    continue
                attach_protective_algo_orders(conn, client, trade_id, signal, dry_run=False)
                protected = conn.execute(
                    "SELECT protective_orders_status FROM live_trades_binance WHERE id = ?",
                    (trade_id,),
                ).fetchone()
                if protected is not None and protected[0] == "ATTACHED":
                    conn.execute(
                        "UPDATE live_trades_binance SET status = ?, notes = ? WHERE id = ?",
                        ("POSITION_OPEN", "execution trade sync: protective orders attached", trade_id),
                    )
                    conn.commit()
                    console("PROTECTIVE_ORDERS_ATTACHED", f"{signal.symbol} trade_id={trade_id}", lifecycle)
                else:
                    conn.execute(
                        "UPDATE live_trades_binance SET status = ?, notes = ? WHERE id = ?",
                        ("POSITION_OPEN_UNPROTECTED", "execution trade sync: entry filled but protective order attachment failed", trade_id),
                    )
                    conn.commit()
                    console("PROTECTIVE_ORDER_FAILED", f"{signal.symbol} trade_id={trade_id} UNPROTECTED", lifecycle)
            except Exception as exc:
                conn.execute(
                    "UPDATE live_trades_binance SET notes = ? WHERE id = ?",
                    (f"execution trade sync failed before protective attach decision; will retry: {exc}", trade_id),
                )
                conn.commit()
                console("ENTRY_SYNC_FAILED", f"{signal.symbol} trade_id={trade_id}", {"error": str(exc)})

        sync_closed_execution_positions(conn, client)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forward-validate strict PnF triangles with guarded Binance USD-M futures micro-orders")
    parser.add_argument("--db-path", help="Path to existing market_data.db; opened read-only and used only as the candle source")
    parser.add_argument("--state-db-path", help="Path to trader-owned state DB for Binance live signal/trade/state tables")
    parser.add_argument("--settings", help="Path to settings.json with PnF profiles")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run mode even if LIVE_TRADING_ENABLED=1")
    parser.add_argument("--demo", action="store_true", help="Use Binance Demo USD-M Futures at https://demo-fapi.binance.com with demo API credentials")
    parser.add_argument("--enable-demo-doubles", action="store_true", help="Enable strict double top/bottom smoke-test execution only for DEMO LIVE mode")
    parser.add_argument("--enable-demo-pole-motif", action="store_true", help="Enable validated Pole Motif forward execution only for Binance DEMO LIVE mode")
    parser.add_argument("--enable-demo-p2-survivor-v1", action="store_true", help="Enable frozen P2_SURVIVOR_V1 / CAND-000053 path only in --demo mode")
    parser.add_argument("--notional-usdt", default=str(DEFAULT_NOTIONAL_USDT), help="Fixed order notional; capped by the effective notional limit")
    parser.add_argument("--demo-max-notional-usdt", default=str(MAX_NOTIONAL_USDT), help="Demo-live-only cap; requested notional above 1 USDT is allowed only in --demo with LIVE_TRADING_ENABLED=1 when this value is >= --notional-usdt")
    parser.add_argument("--history-bars", type=int, default=5000, help="Number of recent 1m candles used to reconstruct close-confirmed PnF state")
    parser.add_argument("--loop", action="store_true", help="Run continuously instead of one pass")
    parser.add_argument("--poll-seconds", type=float, default=30.0, help="Sleep between loop iterations")
    parser.add_argument("--self-test-signal", action="store_true", help="Inject one synthetic allowed dry-run signal through the guarded pipeline")
    parser.add_argument("--force-demo-order", action="store_true", help="Force one demo self-test order using latest candle without waiting for strategy signal")
    parser.add_argument("--force-demo-symbol", default="BINANCE_FUT:SOLUSDT", help="Symbol for forced demo self-test order")
    parser.add_argument("--force-demo-side", default="LONG", choices=("LONG", "SHORT"), help="Side for forced demo self-test order")
    parser.add_argument("--force-demo-notional-usdt", default="1.0", help="Notional USDT for forced demo self-test order")
    parser.add_argument("--force-demo-max-notional-usdt", help="Override notional cap only for --force-demo-order validation (demo mode only)")
    parser.add_argument("--verbose-market-logs", action="store_true", help="Emit compact per-symbol market, signal, order, fill, and exit visibility logs")
    parser.add_argument("--max-pending-entry-minutes", type=int, help="Cancel live entry orders that remain pending longer than this many minutes; disabled by default")
    parser.add_argument("--reconcile-positions", action="store_true", help="Read-only Binance/local open position reconciliation report; exits before scanning or submitting orders")
    parser.add_argument("--export-trade-journal", help="Read-only CSV export path for all rows in live_trades_binance; exits before scanning or submitting orders")
    parser.add_argument("--research-rule-json", help="Path to research_v2 optimizer rule JSON used as pre-order forward-test gate (demo/dry-run only)")
    parser.add_argument("--consume-strategy-setups", action="store_true", help="Read scanner-generated strategy_setups rows and emit setup execution intents; read-only/no orders")
    parser.add_argument("--strategy-setups-db", help="Path to strategy_validation.db containing strategy_setups for --consume-strategy-setups")
    parser.add_argument("--execute-execution-intents", action="store_true", help="Submit NEW execution_intents to Binance Demo under strict guards")
    parser.add_argument("--sync-execution-trades", action="store_true", help="Sync Binance Demo execution-intent entry fills and attach protective TP/SL orders")
    parser.add_argument("--mexc-demo-or-live-mode-name-if-supported", default="DRY_RUN", help="MEXC mode selector; only DRY_RUN/paper is enabled in this repo, LIVE fails closed")
    parser.add_argument("--execute-mexc-intents", action="store_true", help="Process NEW execution_intents for MEXC Futures through Phase A dry-run guards")
    parser.add_argument("--seed-mexc-dry-run-intents", action="store_true", help="Seed deterministic MEXC Futures dry-run test execution_intents into --state-db-path; requires --allow-test-seed")
    parser.add_argument("--allow-test-seed", action="store_true", help="Second confirmation required for --seed-mexc-dry-run-intents")
    parser.add_argument("--sync-mexc-trades", action="store_true", help="Fail-closed placeholder for MEXC Futures fill/protective-order/closure sync")
    parser.add_argument("--inspect-mexc-contracts", action="store_true", help="Read-only public MEXC Futures contract/spec discovery; exits before any order/executor path")
    parser.add_argument("--mexc-futures-base-url", default=MEXC_FUTURES_BASE_URL, help="Base URL for read-only MEXC Futures public API")
    parser.add_argument("--inspect-execution-intents", action="store_true", help="Read-only summary of execution_intents in --state-db-path; exits before Binance initialization or scanning")
    args = parser.parse_args()
    if not args.inspect_mexc_contracts and not args.state_db_path:
        parser.error("the following arguments are required unless --inspect-mexc-contracts is used: --state-db-path")
    if not (args.inspect_mexc_contracts or args.inspect_execution_intents or args.execute_execution_intents or args.sync_execution_trades or args.execute_mexc_intents or args.sync_mexc_trades or args.seed_mexc_dry_run_intents):
        missing = [flag for flag, value in (("--db-path", args.db_path), ("--settings", args.settings)) if not value]
        if missing:
            parser.error(f"the following arguments are required unless a state-db-only utility mode is used: {', '.join(missing)}")
    return args


def main() -> None:
    args = parse_args()
    if bool(getattr(args, "inspect_mexc_contracts", False)):
        inspect_mexc_contracts_once(args)
        return
    if bool(getattr(args, "inspect_execution_intents", False)):
        inspect_execution_intents_once(args)
        return
    if bool(getattr(args, "seed_mexc_dry_run_intents", False)):
        seed_mexc_dry_run_intents_once(args)
        return
    if bool(getattr(args, "execute_execution_intents", False)):
        log_startup(args)
        process_execution_intents_once(args, iteration=1)
        return
    if bool(getattr(args, "sync_execution_trades", False)):
        log_startup(args)
        process_sync_execution_trades_once(args, iteration=1)
        return
    if bool(getattr(args, "execute_mexc_intents", False)):
        log_startup(args)
        process_mexc_execution_intents_once(args, iteration=1)
        return
    if bool(getattr(args, "sync_mexc_trades", False)):
        log_startup(args)
        process_sync_mexc_trades_once(args, iteration=1)
        return

    if getattr(args, "export_trade_journal", None):
        run_trade_journal_export_once(args)
        return

    log_startup(args)

    if bool(getattr(args, "reconcile_positions", False)):
        run_reconciliation_once(args)
        return

    if args.loop:
        iteration = 1
        while True:
            if bool(getattr(args, "consume_strategy_setups", False)):
                process_setup_execution_once(args, iteration=iteration)
            else:
                process_once(args, iteration=iteration)
            iteration += 1
            time.sleep(args.poll_seconds)
    else:
        if bool(getattr(args, "consume_strategy_setups", False)):
            process_setup_execution_once(args, iteration=1)
        else:
            process_once(args, iteration=1)


if __name__ == "__main__":
    main()
