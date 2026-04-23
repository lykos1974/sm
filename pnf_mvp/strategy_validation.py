"""
strategy_validation.py

PnF Strategy Validation Store
=============================

Optimized execution model
-------------------------
- same schema
- same activation / resolution logic
- reduced DB overhead:
  - in-memory cache for pending trades per symbol
  - deferred / batched commits
  - no full pending SELECT on every candle

Behavioral intent
-----------------
This file preserves the original trade logic and DB layout while making
historical backfill materially faster.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Optional


RESOLUTION_PENDING = "PENDING"
RESOLUTION_STOPPED = "STOPPED"
RESOLUTION_TP2 = "TP2"
RESOLUTION_TP1_PARTIAL_THEN_BE = "TP1_PARTIAL_THEN_BE"
RESOLUTION_AMBIGUOUS = "AMBIGUOUS"

ACTIVATION_PENDING = "PENDING"
ACTIVATION_ACTIVE = "ACTIVE"

FEES_RATE = 0.0002

# === BE EXPERIMENT CONFIG ===
BE_MODE = True
BE_TRIGGER_R = 1.5

DEFAULT_COMMIT_EVERY = 1000


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


class StrategyValidationStore:
    def __init__(
        self,
        db_path: str = "strategy_validation.db",
        allow_multiple_trades_per_symbol: Optional[bool] = None,
        commit_every: int = DEFAULT_COMMIT_EVERY,
    ):
        self.db_path = str(Path(db_path))
        self.allow_multiple_trades_per_symbol = (
            self._load_allow_multiple_from_settings()
            if allow_multiple_trades_per_symbol is None
            else bool(allow_multiple_trades_per_symbol)
        )
        self._commit_every = max(1, int(commit_every))
        self._dirty_writes = 0

        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

        self._pending_by_symbol: dict[str, list[dict[str, Any]]] = {}
        self._pending_loaded_symbols: set[str] = set()
        self._perf_lock = threading.Lock()
        self._perf: dict[str, Any] = {
            "update_pending": defaultdict(
                lambda: {
                    "call_count": 0,
                    "elapsed_s": 0.0,
                    "trades_scanned": 0,
                    "trades_updated": 0,
                    "trades_resolved": 0,
                    "trades_activated": 0,
                    "tp1_hits": 0,
                    "tp2_hits": 0,
                    "stop_hits": 0,
                    "ambiguous_hits": 0,
                    "sql_update_count": 0,
                    "sql_insert_count": 0,
                    "sql_select_count": 0,
                    "current_pending_count": 0,
                    "max_pending_count": 0,
                }
            ),
            "register_setup": {
                "call_count": 0,
                "elapsed_s": 0.0,
                "successful_inserts": 0,
                "duplicate_noop_inserts": 0,
                "sql_statement_count": 0,
                "sql_update_count": 0,
                "sql_insert_count": 0,
                "sql_select_count": 0,
            },
        }

    def _perf_counter(self, category: str, symbol: Optional[str] = None) -> dict[str, Any]:
        if category == "update_pending":
            if symbol is None:
                raise ValueError("symbol is required for update_pending perf counters")
            return self._perf["update_pending"][symbol]
        return self._perf["register_setup"]

    def _perf_inc(
        self,
        category: str,
        key: str,
        amount: float | int = 1,
        symbol: Optional[str] = None,
    ) -> None:
        with self._perf_lock:
            counter = self._perf_counter(category, symbol)
            counter[key] = counter.get(key, 0) + amount

    def _count_sql(self, category: str, sql: str, symbol: Optional[str] = None) -> None:
        stmt = str(sql or "").lstrip().upper()
        self._perf_inc(category, "sql_statement_count", 1, symbol=symbol)
        if stmt.startswith("SELECT"):
            self._perf_inc(category, "sql_select_count", 1, symbol=symbol)
        elif stmt.startswith("INSERT"):
            self._perf_inc(category, "sql_insert_count", 1, symbol=symbol)
        elif stmt.startswith("UPDATE"):
            self._perf_inc(category, "sql_update_count", 1, symbol=symbol)

    def _execute_counted(
        self,
        category: str,
        sql: str,
        params: Any = None,
        symbol: Optional[str] = None,
    ) -> sqlite3.Cursor:
        self._count_sql(category, sql, symbol=symbol)
        if params is None:
            return self._conn.execute(sql)
        return self._conn.execute(sql, params)

    def get_perf_snapshot(self) -> Dict[str, Any]:
        with self._perf_lock:
            update_pending = {k: dict(v) for k, v in self._perf["update_pending"].items()}
            register_setup = dict(self._perf["register_setup"])
        return {"update_pending": update_pending, "register_setup": register_setup}

    def _load_allow_multiple_from_settings(self) -> bool:
        settings_path = Path("settings.json")
        if not settings_path.exists():
            return False
        try:
            with settings_path.open("r", encoding="utf-8") as f:
                settings = json.load(f)
            return bool(settings.get("allow_multiple_trades_per_symbol", False))
        except Exception:
            return False

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(str(r["name"]) == column_name for r in rows)

    def _ensure_column(self, table_name: str, column_name: str, column_def: str) -> None:
        if not self._column_exists(table_name, column_name):
            with self._conn:
                self._conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")

    def _init_schema(self):
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_setups (
                    setup_id TEXT PRIMARY KEY,
                    created_ts INTEGER NOT NULL,
                    updated_ts INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    side TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reference_ts INTEGER NOT NULL,
                    bars_observed INTEGER NOT NULL DEFAULT 0,

                    trend_state TEXT,
                    trend_regime TEXT,
                    immediate_slope TEXT,
                    breakout_context TEXT,
                    is_extended_move INTEGER,
                    active_leg_boxes INTEGER,

                    current_column_index INTEGER,
                    current_column_kind TEXT,
                    current_column_top REAL,
                    current_column_bottom REAL,

                    support_level REAL,
                    resistance_level REAL,

                    zone_low REAL,
                    zone_high REAL,
                    ideal_entry REAL,
                    invalidation REAL,
                    risk REAL,
                    tp1 REAL,
                    tp2 REAL,
                    rr1 REAL,
                    rr2 REAL,

                    pullback_quality TEXT,
                    risk_quality TEXT,
                    reward_quality TEXT,
                    quality_score REAL,
                    quality_grade TEXT,
                    reason TEXT,
                    reject_reason TEXT,

                    activation_status TEXT NOT NULL DEFAULT 'PENDING',
                    activated_ts INTEGER,
                    activated_price REAL,

                    tp1_hit INTEGER NOT NULL DEFAULT 0,
                    tp1_hit_ts INTEGER,
                    tp1_price REAL,

                    max_favorable_excursion REAL,
                    max_adverse_excursion REAL,

                    resolution_status TEXT NOT NULL,
                    resolved_ts INTEGER,
                    resolved_price REAL,
                    resolution_note TEXT,

                    first_outcome_ts INTEGER,
                    last_outcome_ts INTEGER,

                    snapshot_path TEXT,

                    raw_setup_json TEXT,
                    raw_structure_json TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_strategy_setups_pending
                ON strategy_setups(symbol, resolution_status, reference_ts)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_strategy_setups_status
                ON strategy_setups(status, symbol, created_ts)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_strategy_setups_activation
                ON strategy_setups(symbol, activation_status, resolution_status, reference_ts)
                """
            )

        self._ensure_column("strategy_setups", "activation_status", "TEXT NOT NULL DEFAULT 'PENDING'")
        self._ensure_column("strategy_setups", "activated_ts", "INTEGER")
        self._ensure_column("strategy_setups", "activated_price", "REAL")
        self._ensure_column("strategy_setups", "tp1_hit", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("strategy_setups", "tp1_hit_ts", "INTEGER")
        self._ensure_column("strategy_setups", "tp1_price", "REAL")
        self._ensure_column("strategy_setups", "current_column_index", "INTEGER")
        self._ensure_column("strategy_setups", "snapshot_path", "TEXT")

    def _make_setup_id(self, symbol: str, setup: Dict[str, Any], structure_state: Dict[str, Any], reference_ts: int) -> str:
        payload = {
            "symbol": symbol,
            "strategy": setup.get("strategy"),
            "side": setup.get("side"),
            "status": setup.get("status"),
            "reference_ts": int(reference_ts),
            "current_column_index": _safe_int(structure_state.get("current_column_index")),
            "current_column_kind": structure_state.get("current_column_kind"),
            "current_column_top": _safe_float(structure_state.get("current_column_top")),
            "current_column_bottom": _safe_float(structure_state.get("current_column_bottom")),
            "support_level": _safe_float(structure_state.get("support_level")),
            "resistance_level": _safe_float(structure_state.get("resistance_level")),
            "ideal_entry": _safe_float(setup.get("ideal_entry")),
            "invalidation": _safe_float(setup.get("invalidation")),
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _mark_dirty(self, units: int = 1) -> None:
        self._dirty_writes += max(1, int(units))
        if self._dirty_writes >= self._commit_every:
            self._conn.commit()
            self._dirty_writes = 0

    def flush(self) -> None:
        with self._lock:
            if self._dirty_writes:
                self._conn.commit()
                self._dirty_writes = 0

    def close(self) -> None:
        with self._lock:
            self.flush()
            self._conn.close()

    def _row_to_pending_dict(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        return dict(row)

    def _ensure_pending_loaded(self, symbol: str, perf_category: str = "update_pending") -> None:
        if symbol in self._pending_loaded_symbols:
            return
        rows = self._execute_counted(
            perf_category,
            """
            SELECT *
            FROM strategy_setups
            WHERE symbol = ?
              AND resolution_status = ?
            ORDER BY reference_ts ASC
            """,
            (symbol, RESOLUTION_PENDING),
            symbol=symbol,
        ).fetchall()
        self._pending_by_symbol[symbol] = [self._row_to_pending_dict(r) for r in rows]
        self._pending_loaded_symbols.add(symbol)

    def has_open_trade_for_symbol(self, symbol: str) -> bool:
        with self._lock:
            self._ensure_pending_loaded(symbol, perf_category="register_setup")
            return bool(self._pending_by_symbol.get(symbol))

    def register_setup(
        self,
        symbol: str,
        setup: Dict[str, Any],
        structure_state: Dict[str, Any],
        reference_ts: int,
        snapshot_path: Optional[str] = None,
        active_column_index: Optional[int] = None,
    ) -> Optional[str]:
        now_ts = int(reference_ts)
        started = time.perf_counter()

        with self._lock:
            self._perf_inc("register_setup", "call_count", 1)
            self._ensure_pending_loaded(symbol, perf_category="register_setup")

            if not self.allow_multiple_trades_per_symbol and self._pending_by_symbol.get(symbol):
                elapsed = time.perf_counter() - started
                self._perf_inc("register_setup", "elapsed_s", elapsed)
                return None

            setup_id = self._make_setup_id(symbol, setup, structure_state, now_ts)

            stored_active_column_index = (
                _safe_int(active_column_index)
                if active_column_index is not None
                else _safe_int(structure_state.get("current_column_index"))
            )

            row = {
                "setup_id": setup_id,
                "created_ts": now_ts,
                "updated_ts": now_ts,
                "symbol": symbol,
                "strategy": str(setup.get("strategy") or ""),
                "side": str(setup.get("side") or ""),
                "status": str(setup.get("status") or ""),
                "reference_ts": now_ts,
                "trend_state": structure_state.get("trend_state"),
                "trend_regime": structure_state.get("trend_regime"),
                "immediate_slope": structure_state.get("immediate_slope"),
                "breakout_context": structure_state.get("breakout_context"),
                "is_extended_move": 1 if bool(structure_state.get("is_extended_move", False)) else 0,
                "active_leg_boxes": _safe_int(structure_state.get("active_leg_boxes")),
                "current_column_index": stored_active_column_index,
                "current_column_kind": structure_state.get("current_column_kind"),
                "current_column_top": _safe_float(structure_state.get("current_column_top")),
                "current_column_bottom": _safe_float(structure_state.get("current_column_bottom")),
                "support_level": _safe_float(structure_state.get("support_level")),
                "resistance_level": _safe_float(structure_state.get("resistance_level")),
                "zone_low": _safe_float(setup.get("zone_low")),
                "zone_high": _safe_float(setup.get("zone_high")),
                "ideal_entry": _safe_float(setup.get("ideal_entry")),
                "invalidation": _safe_float(setup.get("invalidation")),
                "risk": _safe_float(setup.get("risk")),
                "tp1": _safe_float(setup.get("tp1")),
                "tp2": _safe_float(setup.get("tp2")),
                "rr1": _safe_float(setup.get("rr1")),
                "rr2": _safe_float(setup.get("rr2")),
                "pullback_quality": setup.get("pullback_quality"),
                "risk_quality": setup.get("risk_quality"),
                "reward_quality": setup.get("reward_quality"),
                "quality_score": _safe_float(setup.get("quality_score")),
                "quality_grade": setup.get("quality_grade"),
                "reason": setup.get("reason"),
                "reject_reason": setup.get("reject_reason"),
                "activation_status": ACTIVATION_PENDING,
                "activated_ts": None,
                "activated_price": None,
                "tp1_hit": 0,
                "tp1_hit_ts": None,
                "tp1_price": None,
                "max_favorable_excursion": None,
                "max_adverse_excursion": None,
                "resolution_status": RESOLUTION_PENDING,
                "resolved_ts": None,
                "resolved_price": None,
                "resolution_note": None,
                "first_outcome_ts": None,
                "last_outcome_ts": None,
                "snapshot_path": snapshot_path,
                "raw_setup_json": json.dumps(setup, ensure_ascii=False, sort_keys=True),
                "raw_structure_json": json.dumps(structure_state, ensure_ascii=False, sort_keys=True),
            }

            cur = self._execute_counted(
                "register_setup",
                """
                INSERT OR IGNORE INTO strategy_setups (
                    setup_id, created_ts, updated_ts, symbol, strategy, side, status,
                    reference_ts, bars_observed,
                    trend_state, trend_regime, immediate_slope, breakout_context,
                    is_extended_move, active_leg_boxes,
                    current_column_index, current_column_kind, current_column_top, current_column_bottom,
                    support_level, resistance_level,
                    zone_low, zone_high, ideal_entry, invalidation, risk, tp1, tp2, rr1, rr2,
                    pullback_quality, risk_quality, reward_quality, quality_score, quality_grade,
                    reason, reject_reason,
                    activation_status, activated_ts, activated_price,
                    tp1_hit, tp1_hit_ts, tp1_price,
                    max_favorable_excursion, max_adverse_excursion,
                    resolution_status, resolved_ts, resolved_price, resolution_note,
                    first_outcome_ts, last_outcome_ts,
                    snapshot_path,
                    raw_setup_json, raw_structure_json
                ) VALUES (
                    :setup_id, :created_ts, :updated_ts, :symbol, :strategy, :side, :status,
                    :reference_ts, 0,
                    :trend_state, :trend_regime, :immediate_slope, :breakout_context,
                    :is_extended_move, :active_leg_boxes,
                    :current_column_index, :current_column_kind, :current_column_top, :current_column_bottom,
                    :support_level, :resistance_level,
                    :zone_low, :zone_high, :ideal_entry, :invalidation, :risk, :tp1, :tp2, :rr1, :rr2,
                    :pullback_quality, :risk_quality, :reward_quality, :quality_score, :quality_grade,
                    :reason, :reject_reason,
                    :activation_status, :activated_ts, :activated_price,
                    :tp1_hit, :tp1_hit_ts, :tp1_price,
                    :max_favorable_excursion, :max_adverse_excursion,
                    :resolution_status, :resolved_ts, :resolved_price, :resolution_note,
                    :first_outcome_ts, :last_outcome_ts,
                    :snapshot_path,
                    :raw_setup_json, :raw_structure_json
                )
                """,
                row,
            )
            if cur.rowcount and cur.rowcount > 0:
                self._pending_by_symbol.setdefault(symbol, []).append(dict(row))
                self._mark_dirty()
                self._perf_inc("register_setup", "successful_inserts", 1)
            else:
                self._perf_inc("register_setup", "duplicate_noop_inserts", 1)
            elapsed = time.perf_counter() - started
            self._perf_inc("register_setup", "elapsed_s", elapsed)
            return setup_id

    def _should_activate(self, side: str, close_price: float, ideal_entry: Optional[float]) -> bool:
        if ideal_entry is None:
            return False
        side = str(side or "").upper()
        if side == "LONG":
            return float(close_price) <= float(ideal_entry)
        if side == "SHORT":
            return float(close_price) >= float(ideal_entry)
        return False

    def _breakeven_price(self, side: str, entry_price: float) -> float:
        side = str(side or "").upper()
        if side == "LONG":
            return float(entry_price) * (1.0 + FEES_RATE)
        return float(entry_price) * (1.0 - FEES_RATE)

    def _resolve_long_before_tp1(self, low_price, high_price, close_price, invalidation, tp1, tp2):
        hit_stop = invalidation is not None and low_price <= invalidation
        hit_tp1 = tp1 is not None and high_price >= tp1
        hit_tp2 = tp2 is not None and high_price >= tp2

        if hit_stop and (hit_tp1 or hit_tp2):
            return RESOLUTION_AMBIGUOUS, close_price, "same_candle_stop_and_target_hit", False, False
        if hit_stop:
            return RESOLUTION_STOPPED, invalidation, "stop_hit_before_tp1", False, False
        if hit_tp2:
            return RESOLUTION_TP2, tp2, "tp1_then_tp2_same_or_later_candle", True, True
        if hit_tp1:
            return None, None, "tp1_partial_hit", True, False
        return None, None, None, False, False

    def _resolve_short_before_tp1(self, low_price, high_price, close_price, invalidation, tp1, tp2):
        hit_stop = invalidation is not None and high_price >= invalidation
        hit_tp1 = tp1 is not None and low_price <= tp1
        hit_tp2 = tp2 is not None and low_price <= tp2

        if hit_stop and (hit_tp1 or hit_tp2):
            return RESOLUTION_AMBIGUOUS, close_price, "same_candle_stop_and_target_hit", False, False
        if hit_stop:
            return RESOLUTION_STOPPED, invalidation, "stop_hit_before_tp1", False, False
        if hit_tp2:
            return RESOLUTION_TP2, tp2, "tp1_then_tp2_same_or_later_candle", True, True
        if hit_tp1:
            return None, None, "tp1_partial_hit", True, False
        return None, None, None, False, False

    def _resolve_long_after_tp1(self, low_price, high_price, close_price, be_price, tp2):
        hit_be = low_price <= be_price
        hit_tp2 = tp2 is not None and high_price >= tp2
        if hit_tp2:
            return RESOLUTION_TP2, tp2, "tp2_hit_after_tp1"
        if hit_be:
            return RESOLUTION_TP1_PARTIAL_THEN_BE, be_price, "tp1_partial_then_breakeven"
        return None, None, None

    def _resolve_short_after_tp1(self, low_price, high_price, close_price, be_price, tp2):
        hit_be = high_price >= be_price
        hit_tp2 = tp2 is not None and low_price <= tp2
        if hit_tp2:
            return RESOLUTION_TP2, tp2, "tp2_hit_after_tp1"
        if hit_be:
            return RESOLUTION_TP1_PARTIAL_THEN_BE, be_price, "tp1_partial_then_breakeven"
        return None, None, None

    def update_pending_with_candle(
        self,
        symbol: str,
        close_ts: int,
        high_price: float,
        low_price: float,
        close_price: float,
    ):
        started = time.perf_counter()
        close_ts = int(close_ts)
        high_price = float(high_price)
        low_price = float(low_price)
        close_price = float(close_price)

        with self._lock:
            self._perf_inc("update_pending", "call_count", 1, symbol=symbol)
            self._ensure_pending_loaded(symbol, perf_category="update_pending")
            pending = self._pending_by_symbol.get(symbol, [])
            pending_count = len(pending)
            self._perf_inc("update_pending", "current_pending_count", pending_count, symbol=symbol)
            with self._perf_lock:
                counter = self._perf_counter("update_pending", symbol=symbol)
                counter["max_pending_count"] = max(counter.get("max_pending_count", 0), pending_count)
            if not pending:
                self._perf_inc("update_pending", "elapsed_s", time.perf_counter() - started, symbol=symbol)
                return

            still_pending: list[dict[str, Any]] = []

            for row in pending:
                self._perf_inc("update_pending", "trades_scanned", 1, symbol=symbol)
                if int(row.get("reference_ts") or 0) >= close_ts:
                    still_pending.append(row)
                    continue

                bars_observed = int(row.get("bars_observed") or 0) + 1
                side = str(row.get("side") or "").upper()
                ideal_entry = _safe_float(row.get("ideal_entry"))
                invalidation = _safe_float(row.get("invalidation"))
                tp1 = _safe_float(row.get("tp1"))
                tp2 = _safe_float(row.get("tp2"))
                activation_status = str(row.get("activation_status") or ACTIVATION_PENDING).upper()
                tp1_hit = bool(int(row.get("tp1_hit") or 0))

                max_fav = _safe_float(row.get("max_favorable_excursion"))
                max_adv = _safe_float(row.get("max_adverse_excursion"))
                first_outcome_ts = row.get("first_outcome_ts") if row.get("first_outcome_ts") is not None else close_ts
                last_outcome_ts = close_ts

                if activation_status == ACTIVATION_PENDING:
                    if self._should_activate(side=side, close_price=close_price, ideal_entry=ideal_entry):
                        activated_price = ideal_entry if ideal_entry is not None else close_price
                        self._execute_counted(
                            "update_pending",
                            """
                            UPDATE strategy_setups
                            SET updated_ts = ?,
                                bars_observed = ?,
                                activation_status = ?,
                                activated_ts = ?,
                                activated_price = ?,
                                first_outcome_ts = ?,
                                last_outcome_ts = ?
                            WHERE setup_id = ?
                            """,
                            (
                                close_ts,
                                bars_observed,
                                ACTIVATION_ACTIVE,
                                close_ts,
                                activated_price,
                                first_outcome_ts,
                                last_outcome_ts,
                                row["setup_id"],
                            ),
                            symbol=symbol,
                        )
                        row["updated_ts"] = close_ts
                        row["bars_observed"] = bars_observed
                        row["activation_status"] = ACTIVATION_ACTIVE
                        row["activated_ts"] = close_ts
                        row["activated_price"] = activated_price
                        row["first_outcome_ts"] = first_outcome_ts
                        row["last_outcome_ts"] = last_outcome_ts
                        still_pending.append(row)
                        self._mark_dirty()
                        self._perf_inc("update_pending", "trades_updated", 1, symbol=symbol)
                        self._perf_inc("update_pending", "trades_activated", 1, symbol=symbol)
                        continue

                    self._execute_counted(
                        "update_pending",
                        """
                        UPDATE strategy_setups
                        SET updated_ts = ?,
                            bars_observed = ?,
                            first_outcome_ts = ?,
                            last_outcome_ts = ?
                        WHERE setup_id = ?
                        """,
                        (close_ts, bars_observed, first_outcome_ts, last_outcome_ts, row["setup_id"]),
                        symbol=symbol,
                    )
                    row["updated_ts"] = close_ts
                    row["bars_observed"] = bars_observed
                    row["first_outcome_ts"] = first_outcome_ts
                    row["last_outcome_ts"] = last_outcome_ts
                    still_pending.append(row)
                    self._mark_dirty()
                    self._perf_inc("update_pending", "trades_updated", 1, symbol=symbol)
                    continue

                if activation_status != ACTIVATION_ACTIVE:
                    still_pending.append(row)
                    continue

                entry_price = _safe_float(row.get("activated_price"))
                if entry_price is None:
                    entry_price = ideal_entry

                if entry_price is not None:
                    if side == "LONG":
                        favorable = high_price - entry_price
                        adverse = entry_price - low_price
                    else:
                        favorable = entry_price - low_price
                        adverse = high_price - entry_price
                    favorable = max(0.0, favorable)
                    adverse = max(0.0, adverse)
                    max_fav = favorable if max_fav is None else max(max_fav, favorable)
                    max_adv = adverse if max_adv is None else max(max_adv, adverse)

                resolution_status = None
                resolved_price = None
                resolution_note = None

                if not tp1_hit:
                    if BE_MODE and entry_price is not None:
                        risk = abs(entry_price - invalidation) if invalidation is not None else None
                        if risk and risk > 0:
                            if side == "LONG":
                                trigger = entry_price + BE_TRIGGER_R * risk
                                if high_price >= trigger:
                                    tp1_hit = True
                                    tp1 = trigger
                            elif side == "SHORT":
                                trigger = entry_price - BE_TRIGGER_R * risk
                                if low_price <= trigger:
                                    tp1_hit = True
                                    tp1 = trigger

                    if side == "LONG":
                        resolution_status, resolved_price, resolution_note, mark_tp1_hit, direct_tp2 = self._resolve_long_before_tp1(
                            low_price, high_price, close_price, invalidation, tp1, tp2
                        )
                    elif side == "SHORT":
                        resolution_status, resolved_price, resolution_note, mark_tp1_hit, direct_tp2 = self._resolve_short_before_tp1(
                            low_price, high_price, close_price, invalidation, tp1, tp2
                        )
                    else:
                        mark_tp1_hit = False

                    if mark_tp1_hit and resolution_status is None:
                        self._execute_counted(
                            "update_pending",
                            """
                            UPDATE strategy_setups
                            SET updated_ts = ?,
                                bars_observed = ?,
                                tp1_hit = 1,
                                tp1_hit_ts = ?,
                                tp1_price = ?,
                                max_favorable_excursion = ?,
                                max_adverse_excursion = ?,
                                first_outcome_ts = ?,
                                last_outcome_ts = ?
                            WHERE setup_id = ?
                            """,
                            (close_ts, bars_observed, close_ts, tp1, max_fav, max_adv, first_outcome_ts, last_outcome_ts, row["setup_id"]),
                            symbol=symbol,
                        )
                        row["updated_ts"] = close_ts
                        row["bars_observed"] = bars_observed
                        row["tp1_hit"] = 1
                        row["tp1_hit_ts"] = close_ts
                        row["tp1_price"] = tp1
                        row["max_favorable_excursion"] = max_fav
                        row["max_adverse_excursion"] = max_adv
                        row["first_outcome_ts"] = first_outcome_ts
                        row["last_outcome_ts"] = last_outcome_ts
                        still_pending.append(row)
                        self._mark_dirty()
                        self._perf_inc("update_pending", "trades_updated", 1, symbol=symbol)
                        self._perf_inc("update_pending", "tp1_hits", 1, symbol=symbol)
                        continue

                    if mark_tp1_hit and resolution_status is not None:
                        self._execute_counted(
                            "update_pending",
                            """
                            UPDATE strategy_setups
                            SET updated_ts = ?,
                                bars_observed = ?,
                                tp1_hit = 1,
                                tp1_hit_ts = ?,
                                tp1_price = ?,
                                max_favorable_excursion = ?,
                                max_adverse_excursion = ?,
                                resolution_status = ?,
                                resolved_ts = ?,
                                resolved_price = ?,
                                resolution_note = ?,
                                first_outcome_ts = ?,
                                last_outcome_ts = ?
                            WHERE setup_id = ?
                            """,
                            (
                                close_ts,
                                bars_observed,
                                close_ts,
                                tp1,
                                max_fav,
                                max_adv,
                                resolution_status,
                                close_ts,
                                resolved_price,
                                resolution_note,
                                first_outcome_ts,
                                last_outcome_ts,
                                row["setup_id"],
                            ),
                            symbol=symbol,
                        )
                        row["updated_ts"] = close_ts
                        row["bars_observed"] = bars_observed
                        row["tp1_hit"] = 1
                        row["tp1_hit_ts"] = close_ts
                        row["tp1_price"] = tp1
                        row["max_favorable_excursion"] = max_fav
                        row["max_adverse_excursion"] = max_adv
                        row["resolution_status"] = resolution_status
                        row["resolved_ts"] = close_ts
                        row["resolved_price"] = resolved_price
                        row["resolution_note"] = resolution_note
                        row["first_outcome_ts"] = first_outcome_ts
                        row["last_outcome_ts"] = last_outcome_ts
                        self._mark_dirty()
                        self._perf_inc("update_pending", "trades_updated", 1, symbol=symbol)
                        self._perf_inc("update_pending", "trades_resolved", 1, symbol=symbol)
                        self._perf_inc("update_pending", "tp1_hits", 1, symbol=symbol)
                        if resolution_status == RESOLUTION_TP2:
                            self._perf_inc("update_pending", "tp2_hits", 1, symbol=symbol)
                        elif resolution_status == RESOLUTION_STOPPED:
                            self._perf_inc("update_pending", "stop_hits", 1, symbol=symbol)
                        elif resolution_status == RESOLUTION_AMBIGUOUS:
                            self._perf_inc("update_pending", "ambiguous_hits", 1, symbol=symbol)
                        continue
                else:
                    if entry_price is None:
                        still_pending.append(row)
                        continue
                    be_price = self._breakeven_price(side, entry_price)

                    if side == "LONG":
                        resolution_status, resolved_price, resolution_note = self._resolve_long_after_tp1(
                            low_price, high_price, close_price, be_price, tp2
                        )
                    elif side == "SHORT":
                        resolution_status, resolved_price, resolution_note = self._resolve_short_after_tp1(
                            low_price, high_price, close_price, be_price, tp2
                        )

                if resolution_status is None:
                    self._execute_counted(
                        "update_pending",
                        """
                        UPDATE strategy_setups
                        SET updated_ts = ?,
                            bars_observed = ?,
                            max_favorable_excursion = ?,
                            max_adverse_excursion = ?,
                            first_outcome_ts = ?,
                            last_outcome_ts = ?
                        WHERE setup_id = ?
                        """,
                        (close_ts, bars_observed, max_fav, max_adv, first_outcome_ts, last_outcome_ts, row["setup_id"]),
                        symbol=symbol,
                    )
                    row["updated_ts"] = close_ts
                    row["bars_observed"] = bars_observed
                    row["max_favorable_excursion"] = max_fav
                    row["max_adverse_excursion"] = max_adv
                    row["first_outcome_ts"] = first_outcome_ts
                    row["last_outcome_ts"] = last_outcome_ts
                    still_pending.append(row)
                    self._mark_dirty()
                    self._perf_inc("update_pending", "trades_updated", 1, symbol=symbol)
                else:
                    self._execute_counted(
                        "update_pending",
                        """
                        UPDATE strategy_setups
                        SET updated_ts = ?,
                            bars_observed = ?,
                            max_favorable_excursion = ?,
                            max_adverse_excursion = ?,
                            resolution_status = ?,
                            resolved_ts = ?,
                            resolved_price = ?,
                            resolution_note = ?,
                            first_outcome_ts = ?,
                            last_outcome_ts = ?
                        WHERE setup_id = ?
                        """,
                        (
                            close_ts,
                            bars_observed,
                            max_fav,
                            max_adv,
                            resolution_status,
                            close_ts,
                            resolved_price,
                            resolution_note,
                            first_outcome_ts,
                            last_outcome_ts,
                            row["setup_id"],
                        ),
                        symbol=symbol,
                    )
                    row["updated_ts"] = close_ts
                    row["bars_observed"] = bars_observed
                    row["max_favorable_excursion"] = max_fav
                    row["max_adverse_excursion"] = max_adv
                    row["resolution_status"] = resolution_status
                    row["resolved_ts"] = close_ts
                    row["resolved_price"] = resolved_price
                    row["resolution_note"] = resolution_note
                    row["first_outcome_ts"] = first_outcome_ts
                    row["last_outcome_ts"] = last_outcome_ts
                    self._mark_dirty()
                    self._perf_inc("update_pending", "trades_updated", 1, symbol=symbol)
                    self._perf_inc("update_pending", "trades_resolved", 1, symbol=symbol)
                    if resolution_status == RESOLUTION_TP2:
                        self._perf_inc("update_pending", "tp2_hits", 1, symbol=symbol)
                    elif resolution_status == RESOLUTION_STOPPED:
                        self._perf_inc("update_pending", "stop_hits", 1, symbol=symbol)
                    elif resolution_status == RESOLUTION_AMBIGUOUS:
                        self._perf_inc("update_pending", "ambiguous_hits", 1, symbol=symbol)

            self._pending_by_symbol[symbol] = [r for r in still_pending if str(r.get("resolution_status") or "").upper() == RESOLUTION_PENDING]
            self._perf_inc("update_pending", "elapsed_s", time.perf_counter() - started, symbol=symbol)
