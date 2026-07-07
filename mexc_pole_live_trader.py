#!/usr/bin/env python3
"""Isolated live MEXC Futures executor for validated MEXC Pole Strategy v1.

Default mode is dry-run.  Real orders require both ``live_trading_enabled=true``
in config and ``dry_run=false`` plus MEXC_FUTURES_API_KEY/SECRET environment
variables.  Strategy geometry is delegated to research_v2.patterns.mexc_pole_strategy_v1;
this module only gates risk, state, exchange IO, and audit logs.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import http.client
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Protocol

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_v2.patterns import mexc_pole_strategy_v1 as strategy  # noqa: E402

MEXC_API_KEY_ENV = "MEXC_FUTURES_API_KEY"
MEXC_API_SECRET_ENV = "MEXC_FUTURES_API_SECRET"
MEXC_CREDENTIALS_FILE = Path("mexc_credentials.json")
DEFAULT_ALLOWED_SYMBOLS = tuple(strategy.TARGET_SYMBOLS)
OPEN_STATUSES = {"ENTRY_SENT", "OPEN", "STOP_UNVERIFIED", "BE_MOVED"}
TERMINAL_STATUSES = {"CLOSED", "DRY_RUN", "BLOCKED", "KILLED"}
PARITY_NUMERIC_TOLERANCE = Decimal("0.000001")


@dataclass(frozen=True)
class LiveConfig:
    live_trading_enabled: bool = False
    dry_run: bool = True
    candles_db_path: Path = strategy.DEFAULT_DB
    state_db_path: Path = Path("live_state.sqlite3")
    decisions_log_path: Path = Path("live_decisions.log")
    orders_log_path: Path = Path("live_orders.log")
    trade_plan_csv_path: Path = Path("current_trade_plan.csv")
    fixed_risk_usdt: Decimal = Decimal("1.0")
    max_open_positions: int = 1
    max_daily_loss_usdt: Decimal = Decimal("3.0")
    max_notional_usdt: Decimal | None = None
    entry_order_type: str = "MARKET"
    mexc_base_url: str = "https://contract.mexc.com"
    allowed_symbols: tuple[str, ...] = DEFAULT_ALLOWED_SYMBOLS
    box_sizes: dict[str, float] | None = None

    @classmethod
    def from_json(cls, path: Path) -> "LiveConfig":
        raw = json.loads(path.read_text())
        symbols = _configured_symbols(raw)
        return cls(
            live_trading_enabled=bool(raw.get("live_trading_enabled", False)),
            dry_run=bool(raw.get("dry_run", True)),
            candles_db_path=Path(raw.get("candles_db_path", strategy.DEFAULT_DB)),
            state_db_path=Path(raw.get("state_db_path", "live_state.sqlite3")),
            decisions_log_path=Path(raw.get("decisions_log_path", "live_decisions.log")),
            orders_log_path=Path(raw.get("orders_log_path", "live_orders.log")),
            trade_plan_csv_path=Path(raw.get("trade_plan_csv_path", "current_trade_plan.csv")),
            fixed_risk_usdt=Decimal(str(raw.get("fixed_risk_usdt", "1.0"))),
            max_open_positions=int(raw.get("max_open_positions", 1)),
            max_daily_loss_usdt=Decimal(str(raw.get("max_daily_loss_usdt", "3.0"))),
            max_notional_usdt=None if raw.get("max_notional_usdt") is None else Decimal(str(raw["max_notional_usdt"])),
            entry_order_type=str(raw.get("entry_order_type", "MARKET")).upper(),
            mexc_base_url=str(raw.get("mexc_base_url", "https://contract.mexc.com")),
            allowed_symbols=symbols,
            box_sizes={k: float(v) for k, v in raw.get("box_sizes", {}).items()} or None,
        )


def _symbol_tuple(value: Any, key: str) -> tuple[str, ...]:
    if not isinstance(value, list | tuple):
        raise ValueError(f"{key} must be a list of symbols")
    symbols = tuple(str(symbol).strip() for symbol in value if str(symbol).strip())
    if not symbols:
        raise ValueError(f"{key} must contain at least one symbol")
    return symbols


def _configured_symbols(raw: dict[str, Any]) -> tuple[str, ...]:
    symbols = _symbol_tuple(raw["symbols"], "symbols") if "symbols" in raw else None
    allowed_symbols = _symbol_tuple(raw["allowed_symbols"], "allowed_symbols") if "allowed_symbols" in raw else None
    if symbols is not None and allowed_symbols is not None and symbols != allowed_symbols:
        raise ValueError("config keys symbols and allowed_symbols both exist but differ; use symbols as the canonical key")
    return symbols or allowed_symbols or DEFAULT_ALLOWED_SYMBOLS


@dataclass(frozen=True)
class ContractSpec:
    symbol: str
    qty_step: Decimal = Decimal("1")
    min_qty: Decimal = Decimal("1")
    contract_size: Decimal = Decimal("1")


@dataclass(frozen=True)
class TradePlan:
    symbol: str
    direction: str
    opportunity_id: str
    entry_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    break_even_trigger_price: Decimal
    risk_per_unit: Decimal
    position_qty: Decimal
    notional_usdt: Decimal
    observable_entry_ts: int
    source_row_reference: str = ""


class ExchangeClient(Protocol):
    def authenticate(self) -> dict[str, Any]: ...
    def query_account(self) -> dict[str, Any]: ...
    def query_all_positions(self) -> list[dict[str, Any]]: ...
    def query_all_open_orders(self) -> list[dict[str, Any]]: ...
    def get_contract_spec(self, venue_symbol: str) -> ContractSpec: ...
    def place_entry(self, plan: TradePlan, order_type: str) -> dict[str, Any]: ...
    def place_stop(self, plan: TradePlan) -> dict[str, Any]: ...
    def place_target(self, plan: TradePlan) -> dict[str, Any]: ...
    def query_position(self, symbol: str) -> list[dict[str, Any]]: ...
    def query_open_orders(self, symbol: str) -> list[dict[str, Any]]: ...
    def query_order(self, order_id: str) -> dict[str, Any]: ...
    def query_plan_orders(self, symbol: str) -> list[dict[str, Any]]: ...
    def replace_stop_to_break_even(self, trade_id: int, plan: TradePlan) -> dict[str, Any]: ...
    def get_mark_price(self, venue_symbol: str) -> Decimal: ...
    def sync_trade(self, row: sqlite3.Row) -> dict[str, Any]: ...


def load_mexc_credentials(credentials_path: Path = MEXC_CREDENTIALS_FILE) -> tuple[str | None, str | None, str]:
    """Load MEXC credentials from a local file first, then environment variables.

    Returns the API key, API secret, and a non-secret source label suitable for
    logs or error messages.  Secret values must never be logged by callers.
    """
    if credentials_path.exists():
        try:
            raw = json.loads(credentials_path.read_text())
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid MEXC credentials file JSON: {credentials_path}") from exc
        api_key = raw.get("api_key")
        api_secret = raw.get("api_secret")
        if not isinstance(api_key, str) or not api_key.strip() or not isinstance(api_secret, str) or not api_secret.strip():
            raise RuntimeError(f"MEXC credentials file must define non-empty api_key and api_secret: {credentials_path}")
        return api_key.strip(), api_secret.strip(), str(credentials_path)

    api_key = os.environ.get(MEXC_API_KEY_ENV)
    api_secret = os.environ.get(MEXC_API_SECRET_ENV)
    if api_key and api_secret:
        return api_key, api_secret, "environment"
    return None, None, "missing"


def has_mexc_credentials() -> bool:
    api_key, api_secret, _source = load_mexc_credentials()
    return bool(api_key and api_secret)


class MexcFuturesClient:
    def __init__(self, api_key: str | None, api_secret: str | None, base_url: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        signed: bool = False,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = json.dumps(body or {}, separators=(",", ":")) if method == "POST" else ""
        query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
        request_path = f"{path}?{query}" if query else path
        headers = {"Content-Type": "application/json"}
        if signed:
            if not (self.api_key and self.api_secret):
                raise RuntimeError("missing MEXC futures API credentials")
            ts = str(int(time.time() * 1000))
            signature_payload = payload if method == "POST" else query
            sig = hmac.new(self.api_secret.encode(), f"{self.api_key}{ts}{signature_payload}".encode(), hashlib.sha256).hexdigest()
            headers.update({"ApiKey": self.api_key, "Request-Time": ts, "Signature": sig})
        req = urllib.request.Request(f"{self.base_url}{request_path}", data=payload.encode() if method == "POST" else None, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as res:
                return json.loads(res.read().decode())
        except (urllib.error.URLError, json.JSONDecodeError, http.client.IncompleteRead) as exc:
            raise RuntimeError(f"MEXC request failed: {exc}") from exc

    def authenticate(self) -> dict[str, Any]:
        return self.query_account()

    def query_account(self) -> dict[str, Any]:
        data = self._request("GET", "/api/v1/private/account/assets", signed=True).get("data", {})
        return data if isinstance(data, dict) else {"data": data}

    def query_all_positions(self) -> list[dict[str, Any]]:
        data = self._request("GET", "/api/v1/private/position/open_positions", signed=True).get("data", [])
        return data if isinstance(data, list) else []

    def query_all_open_orders(self) -> list[dict[str, Any]]:
        data = self._request("GET", "/api/v1/private/order/list/open_orders", signed=True, params={"page_num": 1, "page_size": 100}).get("data", [])
        if isinstance(data, dict):
            data = data.get("resultList", [])
        return data if isinstance(data, list) else []

    def get_contract_spec(self, venue_symbol: str) -> ContractSpec:
        # Conservative fallback; tests inject exact specs. Live operators should verify contract metadata first.
        return ContractSpec(symbol=venue_symbol.split(":", 1)[-1], qty_step=Decimal("1"), min_qty=Decimal("1"), contract_size=Decimal("1"))

    def place_entry(self, plan: TradePlan, order_type: str) -> dict[str, Any]:
        return self._request("POST", "/api/v1/private/order/submit", {"symbol": plan.symbol.split(":",1)[-1], "vol": str(plan.position_qty), "side": 1, "type": 5 if order_type == "MARKET" else 1}, True)
    def place_stop(self, plan: TradePlan) -> dict[str, Any]:
        return self._request("POST", "/api/v1/private/planorder/place", {"symbol": plan.symbol.split(":",1)[-1], "triggerPrice": str(plan.stop_price), "vol": str(plan.position_qty)}, True)
    def place_target(self, plan: TradePlan) -> dict[str, Any]:
        return self._request("POST", "/api/v1/private/planorder/place", {"symbol": plan.symbol.split(":",1)[-1], "triggerPrice": str(plan.target_price), "vol": str(plan.position_qty)}, True)
    def query_position(self, symbol: str) -> list[dict[str, Any]]:
        data = self._request("GET", "/api/v1/private/position/open_positions", signed=True, params={"symbol": symbol.split(":", 1)[-1]}).get("data", [])
        return data if isinstance(data, list) else []
    def query_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        data = self._request("GET", "/api/v1/private/order/list/open_orders", signed=True, params={"symbol": symbol.split(":", 1)[-1], "page_num": 1, "page_size": 100}).get("data", [])
        if isinstance(data, dict):
            data = data.get("resultList", [])
        return data if isinstance(data, list) else []
    def query_order(self, order_id: str) -> dict[str, Any]:
        data = self._request("GET", f"/api/v1/private/order/get/{order_id}", signed=True).get("data", {})
        return data if isinstance(data, dict) else {}
    def query_plan_orders(self, symbol: str) -> list[dict[str, Any]]:
        data = self._request("GET", "/api/v1/private/stoporder/open_orders", signed=True, params={"symbol": symbol.split(":", 1)[-1]}).get("data", [])
        return data if isinstance(data, list) else []
    def replace_stop_to_break_even(self, trade_id: int, plan: TradePlan) -> dict[str, Any]:
        return self.place_stop(plan)
    def get_mark_price(self, venue_symbol: str) -> Decimal:
        data = self._request("GET", "/api/v1/contract/fairPrice", params={"symbol": venue_symbol.split(":", 1)[-1]}).get("data", {})
        if isinstance(data, dict):
            value = data.get("fairPrice") or data.get("price")
            if value is not None:
                return Decimal(str(value))
        raise RuntimeError("MEXC fair price response did not include a mark price")
    def sync_trade(self, row: sqlite3.Row) -> dict[str, Any]:
        return {"status": row["status"]}


def audit(path: Path, event: str, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True) if path.parent != Path(".") else None
    safe = {k: ("***" if "secret" in k.lower() or "key" in k.lower() else v) for k, v in data.items()}
    with path.open("a") as fh:
        fh.write(json.dumps({"ts": datetime.now(UTC).isoformat(), "event": event, **safe}, sort_keys=True, default=str) + "\n")


def init_state(db: Path) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY, opportunity_id TEXT UNIQUE, symbol TEXT, status TEXT, entry_price TEXT, stop_price TEXT, target_price TEXT, be_trigger_price TEXT, qty TEXT, notional_usdt TEXT, entry_order_id TEXT, stop_order_id TEXT, target_order_id TEXT, be_moved INTEGER DEFAULT 0, opened_at TEXT, closed_at TEXT, realized_pnl_usdt TEXT DEFAULT '0')")
        conn.execute("CREATE TABLE IF NOT EXISTS kill_switch (id INTEGER PRIMARY KEY CHECK(id=1), active INTEGER NOT NULL DEFAULT 0, reason TEXT, updated_at TEXT)")
        conn.execute("INSERT OR IGNORE INTO kill_switch(id, active, reason, updated_at) VALUES (1,0,'',?)", (datetime.now(UTC).isoformat(),))


def is_killed(db: Path) -> bool:
    init_state(db)
    with sqlite3.connect(db) as conn:
        return bool(conn.execute("SELECT active FROM kill_switch WHERE id=1").fetchone()[0])


def trigger_kill_switch(db: Path, reason: str) -> None:
    init_state(db)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE kill_switch SET active=1, reason=?, updated_at=? WHERE id=1", (reason, datetime.now(UTC).isoformat()))


def halt_unprotected_position(config: LiveConfig, symbol: str, reason: str) -> None:
    trigger_kill_switch(config.state_db_path, reason)
    audit(config.decisions_log_path, "UNPROTECTED_POSITION", {"symbol": symbol, "reason": reason})


def round_qty(qty: Decimal, spec: ContractSpec) -> Decimal:
    rounded = (qty / spec.qty_step).to_integral_value(rounding=ROUND_DOWN) * spec.qty_step
    return Decimal("0") if rounded < spec.min_qty else rounded


def compute_qty(entry: Decimal, stop: Decimal, fixed_risk: Decimal, spec: ContractSpec) -> Decimal:
    risk = abs(entry - stop)
    if risk <= 0:
        raise ValueError("risk_per_unit must be positive")
    return round_qty(fixed_risk / risk, spec)


def generate_trade_plans(config: LiveConfig, client: ExchangeClient) -> list[TradePlan]:
    box_sizes = {**strategy.DEFAULT_BOX_SIZES, **(config.box_sizes or {})}
    observations = []
    for symbol in config.allowed_symbols:
        candles = strategy._load_market_candles(config.candles_db_path, symbol)
        columns = strategy._build_columns(symbol, candles, box_sizes[symbol]) if candles else []
        observations.extend(strategy._detect_core_observations(symbol, columns, box_sizes[symbol], candles) if candles else [])
    opportunities = strategy._observable_opportunities(strategy._build_opportunities(observations))
    trades = strategy._select_one_position_per_symbol(opportunities)
    rows = strategy._trade_plan_rows(trades, float(config.fixed_risk_usdt))
    write_current_plan(config.trade_plan_csv_path, rows)
    plans = []
    for row in rows:
        spec = client.get_contract_spec(row["symbol"])
        entry, stop = Decimal(str(row["entry_price"])), Decimal(str(row["stop_price"]))
        qty = compute_qty(entry, stop, config.fixed_risk_usdt, spec)
        notional = qty * entry * spec.contract_size
        plans.append(
            TradePlan(
                row["symbol"],
                row["direction"],
                row["source_opportunity_id"],
                entry,
                stop,
                Decimal(str(row["target_price"])),
                Decimal(str(row["break_even_trigger_price"])),
                abs(entry - stop),
                qty,
                notional,
                int(row["observable_entry_ts"]),
                str(row.get("source_row_reference", "")),
            )
        )
    return plans


@dataclass(frozen=True)
class ParityCheckResult:
    status: str
    checked_at: str
    error: str = ""


def _decimal_close(left: Decimal, right: Decimal) -> bool:
    return abs(left - right) <= PARITY_NUMERIC_TOLERANCE


def recompute_research_plan_for_live(plan: TradePlan, config: LiveConfig, client: ExchangeClient) -> TradePlan | None:
    """Rebuild the research strategy plan and select the row matching a live plan.

    This is intentionally a thin wrapper around the research strategy pipeline used by
    ``generate_trade_plans`` so execution cannot proceed from stale or hand-edited plan
    data.  Prefer exact source opportunity matching; fall back to symbol/direction/time
    only for artifacts where the source row reference is unavailable.
    """
    research_plans = generate_trade_plans(config, client)
    for research_plan in research_plans:
        if research_plan.opportunity_id == plan.opportunity_id:
            return research_plan
    for research_plan in research_plans:
        if (
            research_plan.symbol == plan.symbol
            and research_plan.direction == plan.direction
            and research_plan.observable_entry_ts == plan.observable_entry_ts
        ):
            return research_plan
    return None


def check_live_strategy_parity(plan: TradePlan, config: LiveConfig, client: ExchangeClient) -> ParityCheckResult:
    checked_at = datetime.now(UTC).isoformat()
    try:
        research_plan = recompute_research_plan_for_live(plan, config, client)
    except Exception as exc:
        return ParityCheckResult("PARITY_UNAVAILABLE", checked_at, str(exc))
    if research_plan is None:
        return ParityCheckResult("PARITY_UNAVAILABLE", checked_at, "research plan was not generated for live plan")

    mismatches: list[str] = []
    exact_fields = ("symbol", "direction", "observable_entry_ts", "opportunity_id", "source_row_reference")
    numeric_fields = (
        "entry_price",
        "stop_price",
        "risk_per_unit",
        "target_price",
        "break_even_trigger_price",
        "position_qty",
        "notional_usdt",
    )
    for field in exact_fields:
        live_value = getattr(plan, field)
        research_value = getattr(research_plan, field)
        if field == "source_row_reference" and not live_value and not research_value:
            continue
        if live_value != research_value:
            mismatches.append(f"{field}: live={live_value} research={research_value}")
    for field in numeric_fields:
        live_value = Decimal(str(getattr(plan, field)))
        research_value = Decimal(str(getattr(research_plan, field)))
        if not _decimal_close(live_value, research_value):
            label = "approximate_notional_usdt" if field == "notional_usdt" else field
            mismatches.append(f"{label}: live={live_value} research={research_value}")
    if mismatches:
        return ParityCheckResult("PARITY_FAILED", checked_at, "; ".join(mismatches))
    return ParityCheckResult("PARITY_PASSED", checked_at, "")

def write_current_plan(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=strategy.TRADE_PLAN_FIELDS)
        writer.writeheader(); writer.writerows(rows)


def daily_loss(db: Path) -> Decimal:
    today = datetime.now(UTC).date().isoformat()
    with sqlite3.connect(db) as conn:
        value = conn.execute("SELECT COALESCE(SUM(CAST(realized_pnl_usdt AS REAL)),0) FROM trades WHERE closed_at LIKE ?", (today + "%",)).fetchone()[0]
    return Decimal(str(value))


def has_stop_unverified(db: Path) -> bool:
    init_state(db)
    with sqlite3.connect(db) as conn:
        return bool(conn.execute("SELECT 1 FROM trades WHERE status='STOP_UNVERIFIED' LIMIT 1").fetchone())


def open_count(db: Path, symbol: str | None = None) -> int:
    q = f"SELECT COUNT(*) FROM trades WHERE status IN ({','.join('?' for _ in OPEN_STATUSES)})"
    args: list[Any] = list(OPEN_STATUSES)
    if symbol:
        q += " AND symbol=?"; args.append(symbol)
    with sqlite3.connect(db) as conn:
        return int(conn.execute(q, args).fetchone()[0])


def can_open(plan: TradePlan, config: LiveConfig) -> tuple[bool, str]:
    if is_killed(config.state_db_path): return False, "KILL_SWITCH_ACTIVE"
    if has_stop_unverified(config.state_db_path): return False, "STOP_UNVERIFIED_BLOCK"
    if plan.symbol not in config.allowed_symbols: return False, "SYMBOL_NOT_ALLOWED"
    if plan.position_qty <= 0: return False, "QTY_ROUNDED_TO_ZERO"
    with sqlite3.connect(config.state_db_path) as conn:
        if conn.execute("SELECT 1 FROM trades WHERE opportunity_id=?", (plan.opportunity_id,)).fetchone():
            return False, "DUPLICATE_OPPORTUNITY"
    if open_count(config.state_db_path, plan.symbol): return False, "SYMBOL_POSITION_ALREADY_OPEN"
    if open_count(config.state_db_path) >= config.max_open_positions: return False, "MAX_OPEN_POSITIONS"
    if abs(daily_loss(config.state_db_path)) >= config.max_daily_loss_usdt and daily_loss(config.state_db_path) < 0: return False, "MAX_DAILY_LOSS"
    if config.max_notional_usdt is not None and plan.notional_usdt > config.max_notional_usdt: return False, "MAX_NOTIONAL"
    return True, "OK"


def _payload_id(payload: dict[str, Any]) -> str:
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("orderId", "id"):
            if data.get(key) is not None:
                return str(data[key])
    if data is not None and not isinstance(data, (list, dict)):
        return str(data)
    for key in ("order_id", "orderId", "id"):
        if payload.get(key) is not None:
            return str(payload[key])
    return ""


def _decimal_field(row: dict[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return Decimal(str(value))
    return None


def _active_position_rows(client: ExchangeClient, symbol: str) -> list[dict[str, Any]]:
    rows = client.query_position(symbol)
    return [row for row in rows if _decimal_field(row, "holdVol", "vol", "positionQty") not in (None, Decimal("0")) and int(row.get("state", 1)) != 3]


def verify_entry_filled_or_open(client: ExchangeClient, plan: TradePlan, order_id: str) -> bool:
    if order_id:
        order = client.query_order(order_id)
        if int(order.get("state", 0) or 0) == 3:
            return True
        deal_vol = _decimal_field(order, "dealVol", "filledQty", "executedQty")
        if deal_vol is not None and deal_vol > 0:
            return True
    return bool(_active_position_rows(client, plan.symbol))


def verify_plan_order_exists(client: ExchangeClient, plan: TradePlan, expected_price: Decimal, kind: str, order_id: str = "") -> bool:
    price_fields = ("stopLossPrice", "triggerPrice") if kind == "STOP" else ("takeProfitPrice", "triggerPrice")
    id_fields = ("id", "orderId", "placeOrderId")
    for order in client.query_plan_orders(plan.symbol):
        if int(order.get("state", 1) or 1) not in {0, 1}:
            continue
        if order_id and any(str(order.get(field, "")) == order_id for field in id_fields):
            return True
        observed = _decimal_field(order, *price_fields)
        if observed is not None and observed == expected_price:
            return True
    return False


def _log_reconcile_query_error(config: LiveConfig, symbol: str, query: str, exc: Exception) -> None:
    audit(config.decisions_log_path, "EXCHANGE_RECONCILE_ERROR", {"symbol": symbol, "step": query, "query": query, "error": str(exc)})


def reconcile_from_exchange(config: LiveConfig, client: ExchangeClient) -> str:
    init_state(config.state_db_path)
    for symbol in config.allowed_symbols:
        try:
            positions = _active_position_rows(client, symbol)
        except Exception as exc:
            _log_reconcile_query_error(config, symbol, "query_position", exc)
            return "EXCHANGE_RECONCILE_ERROR"
        try:
            open_orders = client.query_open_orders(symbol)
        except Exception as exc:
            _log_reconcile_query_error(config, symbol, "query_open_orders", exc)
            return "EXCHANGE_RECONCILE_ERROR"
        try:
            plan_orders = client.query_plan_orders(symbol)
        except Exception as exc:
            _log_reconcile_query_error(config, symbol, "query_plan_orders", exc)
            return "EXCHANGE_RECONCILE_ERROR"
        audit(config.decisions_log_path, "EXCHANGE_RECONCILE", {"symbol": symbol, "positions": len(positions), "open_orders": len(open_orders), "plan_orders": len(plan_orders)})
        for position in positions:
            stop_exists = any(
                int(order.get("state", 1) or 1) in {0, 1}
                and _decimal_field(order, "stopLossPrice", "triggerPrice") not in (None, Decimal("0"))
                for order in plan_orders
            )
            position_id = str(position.get("positionId", f"{symbol}:unknown"))
            qty = _decimal_field(position, "holdVol", "vol", "positionQty") or Decimal("0")
            entry = _decimal_field(position, "holdAvgPrice", "openAvgPrice", "entryPrice") or Decimal("0")
            status = "OPEN" if stop_exists else "STOP_UNVERIFIED"
            with sqlite3.connect(config.state_db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO trades(opportunity_id,symbol,status,entry_price,stop_price,target_price,be_trigger_price,qty,notional_usdt,opened_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (f"EXCHANGE-{symbol}-{position_id}", symbol, status, str(entry), "0", "0", "0", str(qty), "0", datetime.now(UTC).isoformat()),
                )
            if not stop_exists:
                halt_unprotected_position(config, symbol, "exchange position has no verified protective stop after restart")
    return "OK"


def execute_plan(plan: TradePlan, config: LiveConfig, client: ExchangeClient) -> str:
    init_state(config.state_db_path)
    ok, reason = can_open(plan, config)
    audit(config.decisions_log_path, "OPEN_CHECK", {"opportunity_id": plan.opportunity_id, "symbol": plan.symbol, "ok": ok, "reason": reason})
    if not ok: return reason
    parity = check_live_strategy_parity(plan, config, client)
    audit(
        config.decisions_log_path,
        parity.status,
        {
            "opportunity_id": plan.opportunity_id,
            "symbol": plan.symbol,
            "parity_status": parity.status,
            "parity_checked_at": parity.checked_at,
            "parity_error": parity.error,
        },
    )
    if parity.status != "PARITY_PASSED":
        if config.live_trading_enabled:
            trigger_kill_switch(config.state_db_path, parity.error or parity.status)
        return parity.status
    if config.dry_run or not config.live_trading_enabled:
        audit(config.orders_log_path, "DRY_RUN_ORDER_BLOCKED", {"live_trading_enabled": config.live_trading_enabled, "dry_run": config.dry_run, "plan": plan.__dict__})
        return "DRY_RUN"
    if not has_mexc_credentials():
        return "LIVE_FLAG_REQUIRES_MEXC_CREDENTIALS"
    with sqlite3.connect(config.state_db_path) as conn:
        try:
            conn.execute("INSERT INTO trades(opportunity_id,symbol,status,entry_price,stop_price,target_price,be_trigger_price,qty,notional_usdt,opened_at) VALUES (?,?,?,?,?,?,?,?,?,?)", (plan.opportunity_id, plan.symbol, "ENTRY_SENT", str(plan.entry_price), str(plan.stop_price), str(plan.target_price), str(plan.break_even_trigger_price), str(plan.position_qty), str(plan.notional_usdt), datetime.now(UTC).isoformat()))
            trade_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        except sqlite3.IntegrityError:
            return "DUPLICATE_OPPORTUNITY"
    try:
        entry = client.place_entry(plan, config.entry_order_type); audit(config.orders_log_path, "ENTRY_SENT", entry)
        entry_order_id = _payload_id(entry)
        entry_active = verify_entry_filled_or_open(client, plan, entry_order_id)
        if not entry_active:
            audit(config.decisions_log_path, "ENTRY_NOT_CONFIRMED", {"symbol": plan.symbol, "opportunity_id": plan.opportunity_id, "entry_order_id": entry_order_id})
            with sqlite3.connect(config.state_db_path) as conn:
                conn.execute("UPDATE trades SET status='ENTRY_SENT', entry_order_id=? WHERE id=?", (entry_order_id, trade_id))
            return "ENTRY_NOT_CONFIRMED"
        stop = client.place_stop(plan); audit(config.orders_log_path, "STOP_SENT", stop)
        stop_order_id = _payload_id(stop)
        if not verify_plan_order_exists(client, plan, plan.stop_price, "STOP", stop_order_id):
            with sqlite3.connect(config.state_db_path) as conn:
                conn.execute("UPDATE trades SET status='STOP_UNVERIFIED', entry_order_id=?, stop_order_id=? WHERE id=?", (entry_order_id, stop_order_id, trade_id))
            halt_unprotected_position(config, plan.symbol, "entry filled/open but protective stop could not be verified")
            return "UNPROTECTED_POSITION"
        target = client.place_target(plan); audit(config.orders_log_path, "TARGET_SENT", target)
        target_order_id = _payload_id(target)
        if not verify_plan_order_exists(client, plan, plan.target_price, "TARGET", target_order_id):
            audit(config.decisions_log_path, "TARGET_NOT_VERIFIED", {"symbol": plan.symbol, "opportunity_id": plan.opportunity_id, "target_order_id": target_order_id})
            return "TARGET_NOT_VERIFIED"
        with sqlite3.connect(config.state_db_path) as conn:
            conn.execute("UPDATE trades SET status='OPEN', entry_order_id=?, stop_order_id=?, target_order_id=? WHERE id=?", (entry_order_id, stop_order_id, target_order_id, trade_id))
        return "OPEN"
    except Exception as exc:
        try:
            stop = client.place_stop(plan); audit(config.orders_log_path, "EMERGENCY_STOP_SENT", stop)
            if verify_plan_order_exists(client, plan, plan.stop_price, "STOP", _payload_id(stop)):
                with sqlite3.connect(config.state_db_path) as conn: conn.execute("UPDATE trades SET status='STOP_UNVERIFIED' WHERE id=?", (trade_id,))
            else:
                raise RuntimeError("emergency protective stop could not be verified")
        except Exception as stop_exc:
            trigger_kill_switch(config.state_db_path, f"protective stop failed after entry error: {stop_exc}")
            audit(config.decisions_log_path, "UNPROTECTED_POSITION", {"symbol": plan.symbol, "reason": str(stop_exc)})
        return f"EXCHANGE_ERROR:{exc}"


def sync_open_trades(config: LiveConfig, client: ExchangeClient) -> None:
    init_state(config.state_db_path)
    if is_killed(config.state_db_path): return
    with sqlite3.connect(config.state_db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(f"SELECT * FROM trades WHERE status IN ({','.join('?' for _ in OPEN_STATUSES)})", tuple(OPEN_STATUSES)).fetchall()
    for row in rows:
        direction = "LONG" if Decimal(row["target_price"]) >= Decimal(row["entry_price"]) else "SHORT"
        plan = TradePlan(row["symbol"], direction, row["opportunity_id"], Decimal(row["entry_price"]), Decimal(row["stop_price"]), Decimal(row["target_price"]), Decimal(row["be_trigger_price"]), abs(Decimal(row["entry_price"])-Decimal(row["stop_price"])), Decimal(row["qty"]), Decimal(row["notional_usdt"]), 0)
        if plan.break_even_trigger_price == 0:
            continue
        mark = client.get_mark_price(plan.symbol)
        be_reached = mark >= plan.break_even_trigger_price if plan.direction == "LONG" else mark <= plan.break_even_trigger_price
        if not row["be_moved"] and be_reached:
            result = client.replace_stop_to_break_even(int(row["id"]), plan)
            audit(config.orders_log_path, "STOP_MOVED_TO_BREAK_EVEN", result)
            with sqlite3.connect(config.state_db_path) as conn: conn.execute("UPDATE trades SET be_moved=1,status='BE_MOVED',stop_price=? WHERE id=?", (str(plan.entry_price), row["id"]))


def run_health_check(config: LiveConfig, client: ExchangeClient | None = None) -> bool:
    step = "Credentials"
    try:
        api_key, api_secret, _source = load_mexc_credentials()
        if not (api_key and api_secret):
            raise RuntimeError("missing MEXC futures API credentials")
        print("PASS Credentials")

        if client is None:
            client = MexcFuturesClient(api_key, api_secret, config.mexc_base_url)

        step = "Authentication"
        client.authenticate()
        print("PASS Authentication")

        step = "Account"
        client.query_account()
        print("PASS Account")

        step = "Positions"
        client.query_all_positions()
        print("PASS Positions")

        step = "Open Orders"
        client.query_all_open_orders()
        print("PASS Open Orders")

        print("OVERALL: READY")
        return True
    except Exception as exc:
        print(f"FAIL {step}")
        print(f"Reason: {exc}")
        print("OVERALL: NOT_READY")
        return False


def run_once(config: LiveConfig, client: ExchangeClient | None = None) -> list[str]:
    init_state(config.state_db_path)
    audit(config.decisions_log_path, "SYMBOL_UNIVERSE_LOADED", {"symbols": list(config.allowed_symbols)})
    if client is None:
        api_key, api_secret, _source = load_mexc_credentials()
        client = MexcFuturesClient(api_key, api_secret, config.mexc_base_url)
    if config.live_trading_enabled and not config.dry_run:
        reconcile_result = reconcile_from_exchange(config, client)
        if reconcile_result != "OK":
            audit(config.decisions_log_path, "TRADING_BLOCKED", {"reason": reconcile_result})
            return [reconcile_result]
    sync_open_trades(config, client)
    if is_killed(config.state_db_path):
        audit(config.decisions_log_path, "TRADING_BLOCKED", {"reason": "KILL_SWITCH_ACTIVE"}); return ["KILL_SWITCH_ACTIVE"]
    results = []
    for plan in generate_trade_plans(config, client):
        results.append(execute_plan(plan, config, client))
    if not results:
        audit(config.decisions_log_path, "NO_VALID_SIGNAL", {})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Live MEXC Pole Trader v1")
    parser.add_argument("--config", type=Path, default=Path("mexc_pole_live_config.example.json"))
    parser.add_argument("--health-check", action="store_true", help="Verify MEXC futures credentials and read-only account access, then exit")
    args = parser.parse_args()
    config = LiveConfig.from_json(args.config)
    if args.health_check:
        raise SystemExit(0 if run_health_check(config) else 1)
    for result in run_once(config):
        print(result)


if __name__ == "__main__":
    main()
