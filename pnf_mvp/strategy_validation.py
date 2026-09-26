"""
strategy_validation.py

PnF Strategy Validation Store
=============================

Historical validation execution model
-------------------------------------
- one atomic transaction per candle across every eligible setup
- replay-safe persisted candle watermarks
- structured, registration-time-frozen tick provenance
- explicit activation-candle branches for unresolved OHLC chronology
- in-memory caching without intra-candle commits
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import threading
import time
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Optional


RESOLUTION_PENDING = "PENDING"
RESOLUTION_STOPPED = "STOPPED"
RESOLUTION_TP2 = "TP2"
RESOLUTION_TP1_PARTIAL_THEN_BE = "TP1_PARTIAL_THEN_BE"
RESOLUTION_AMBIGUOUS = "AMBIGUOUS"
RESOLUTION_EXPIRED = "EXPIRED"

ACTIVATION_PENDING = "PENDING"
ACTIVATION_ACTIVE = "ACTIVE"

FEES_RATE = 0.0002

# === BE EXPERIMENT CONFIG ===
BE_MODE = True
BE_TRIGGER_R = 1.5

DEFAULT_COMMIT_EVERY = 1000
PENDING_EXPIRY_CANDLES = 3
HISTORICAL_ACTIVATION_MODEL = "historical_ohlc_one_tick_trade_through_v1"
VALIDATION_STATE_VERSION = 2
TP1_PARTIAL_FRACTION = 0.5
STRUCTURED_TICK_FIELDS = (
    "provider",
    "venue",
    "instrument_type",
    "native_symbol",
    "source_symbol",
    "provenance_timestamp",
    "provenance_version",
)
SYMBOL_IDENTITY_FIELDS = (
    "provider",
    "venue",
    "instrument_type",
    "native_symbol",
    "source_symbol",
)


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


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 3)


def _require_finite_positive_tick(value: Any, context: str) -> float:
    try:
        tick_size = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{context} requires a finite positive tick_size") from exc
    if not math.isfinite(tick_size) or tick_size <= 0:
        raise ValueError(f"{context} requires a finite positive tick_size")
    return tick_size


def _normalize_identity_allowlist(allowlist: Any) -> dict[str, dict[str, str]]:
    if allowlist is None:
        return {}
    if not isinstance(allowlist, dict):
        raise ValueError("symbol identity allowlist must be an object")
    normalized_allowlist: dict[str, dict[str, str]] = {}
    for symbol, identity in allowlist.items():
        symbol = str(symbol).strip()
        if not symbol or not isinstance(identity, dict):
            raise ValueError("symbol identity allowlist entries must be named objects")
        normalized_identity: dict[str, str] = {}
        for field in SYMBOL_IDENTITY_FIELDS:
            value = str(identity.get(field) or "").strip()
            if not value:
                raise ValueError(
                    f"symbol identity allowlist for {symbol} requires field {field}"
                )
            normalized_identity[field] = value
        if normalized_identity["source_symbol"] != symbol:
            raise ValueError(
                f"symbol identity allowlist key/source mismatch for {symbol}"
            )
        normalized_allowlist[symbol] = normalized_identity
    return normalized_allowlist


def _normalize_tick_provenance(
    symbol: str,
    provenance: Any,
    allowed_identity: Optional[Dict[str, str]] = None,
) -> dict[str, Any]:
    if allowed_identity is None:
        raise ValueError(f"unknown configured symbol rejected: {symbol}")
    if not isinstance(provenance, dict):
        raise ValueError(f"tick provenance for {symbol} must be an object")
    normalized: dict[str, Any] = {}
    for field in STRUCTURED_TICK_FIELDS:
        value = str(provenance.get(field) or "").strip()
        if not value:
            raise ValueError(
                f"tick provenance for {symbol} requires structured field {field}"
            )
        normalized[field] = value
    mismatched = [
        field
        for field in SYMBOL_IDENTITY_FIELDS
        if normalized[field] != allowed_identity[field]
    ]
    if mismatched:
        raise ValueError(
            "tick provenance symbol identity mismatch: "
            f"registered={symbol} fields={','.join(mismatched)}"
        )
    normalized["tick_size"] = _require_finite_positive_tick(
        provenance.get("tick_size"), f"tick provenance for {symbol}"
    )
    normalized["source"] = str(provenance.get("source") or "").strip()
    return normalized


class StrategyValidationStore:
    def __init__(
        self,
        db_path: str = "strategy_validation.db",
        allow_multiple_trades_per_symbol: Optional[bool] = None,
        commit_every: int = DEFAULT_COMMIT_EVERY,
        symbol_tick_provenance: Optional[Dict[str, Dict[str, Any]]] = None,
        symbol_identity_allowlist: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        self.db_path = str(Path(db_path))
        self.allow_multiple_trades_per_symbol = (
            self._load_allow_multiple_from_settings()
            if allow_multiple_trades_per_symbol is None
            else bool(allow_multiple_trades_per_symbol)
        )
        self._commit_every = max(1, int(commit_every))
        self._dirty_writes = 0
        self._candle_transaction_active = False
        self._symbol_identity_allowlist = _normalize_identity_allowlist(
            symbol_identity_allowlist
        )
        self._symbol_tick_provenance: dict[str, dict[str, Any]] = {}
        for symbol, provenance in dict(symbol_tick_provenance or {}).items():
            symbol = str(symbol)
            self._symbol_tick_provenance[str(symbol)] = _normalize_tick_provenance(
                symbol,
                provenance,
                self._symbol_identity_allowlist.get(symbol),
            )

        self._lock = threading.RLock()
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
                    "pending_count_total": 0,
                    "max_pending_count": 0,
                    "update_pending_sql_updates_total": 0,
                    "update_pending_event_updates": 0,
                    "update_pending_event_activation": 0,
                    "update_pending_event_tp1_hit": 0,
                    "update_pending_event_final_resolution": 0,
                    "update_pending_event_stop_loss": 0,
                    "update_pending_event_break_even": 0,
                    "update_pending_event_timeout_expiry": 0,
                    "update_pending_progress_updates": 0,
                    "update_pending_progress_unresolved_active": 0,
                    "update_pending_progress_pending_not_activated": 0,
                    "update_pending_only_timestamp_updates": 0,
                    "update_pending_excursion_updates": 0,
                    "update_pending_noop_candidate_updates": 0,
                    "noop_skipped_count": 0,
                    "lifecycle_update_count": 0,
                    "commit_count": 0,
                    "commit_elapsed_s": 0.0,
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
                "commit_count": 0,
                "commit_elapsed_s": 0.0,
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

    def _perf_set(self, category: str, key: str, value: Any, symbol: Optional[str] = None) -> None:
        with self._perf_lock:
            counter = self._perf_counter(category, symbol)
            counter[key] = value

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
                    activation_tick_size REAL,
                    activation_tick_source TEXT,
                    activation_tick_provider TEXT,
                    activation_tick_venue TEXT,
                    activation_tick_instrument_type TEXT,
                    activation_tick_native_symbol TEXT,
                    activation_tick_source_symbol TEXT,
                    activation_tick_provenance_timestamp TEXT,
                    activation_tick_provenance_version TEXT,
                    last_evaluated_candle_ts INTEGER,
                    validation_state_version INTEGER,
                    branch_active INTEGER NOT NULL DEFAULT 0,

                    tp1_hit INTEGER NOT NULL DEFAULT 0,
                    tp1_hit_ts INTEGER,
                    tp1_price REAL,

                    max_favorable_excursion REAL,
                    max_adverse_excursion REAL,

                    resolution_status TEXT NOT NULL,
                    resolved_ts INTEGER,
                    resolved_price REAL,
                    resolution_note TEXT,
                    ambiguous_pessimistic_r REAL,
                    ambiguous_optimistic_r REAL,

                    first_outcome_ts INTEGER,
                    last_outcome_ts INTEGER,

                    snapshot_path TEXT,

                    raw_setup_json TEXT,
                    raw_structure_json TEXT
                )
                """
            )

        # Legacy columns must exist before indexes reference them.
        self._ensure_column("strategy_setups", "activation_status", "TEXT NOT NULL DEFAULT 'PENDING'")
        self._ensure_column("strategy_setups", "activated_ts", "INTEGER")
        self._ensure_column("strategy_setups", "activated_price", "REAL")
        self._ensure_column("strategy_setups", "tp1_hit", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("strategy_setups", "tp1_hit_ts", "INTEGER")
        self._ensure_column("strategy_setups", "tp1_price", "REAL")
        self._ensure_column("strategy_setups", "current_column_index", "INTEGER")
        self._ensure_column("strategy_setups", "snapshot_path", "TEXT")
        self._ensure_column("strategy_setups", "activation_tick_size", "REAL")
        self._ensure_column("strategy_setups", "activation_tick_source", "TEXT")
        self._ensure_column("strategy_setups", "activation_tick_provider", "TEXT")
        self._ensure_column("strategy_setups", "activation_tick_venue", "TEXT")
        self._ensure_column("strategy_setups", "activation_tick_instrument_type", "TEXT")
        self._ensure_column("strategy_setups", "activation_tick_native_symbol", "TEXT")
        self._ensure_column("strategy_setups", "activation_tick_source_symbol", "TEXT")
        self._ensure_column("strategy_setups", "activation_tick_provenance_timestamp", "TEXT")
        self._ensure_column("strategy_setups", "activation_tick_provenance_version", "TEXT")
        self._ensure_column("strategy_setups", "last_evaluated_candle_ts", "INTEGER")
        self._ensure_column("strategy_setups", "validation_state_version", "INTEGER")
        self._ensure_column("strategy_setups", "branch_active", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("strategy_setups", "ambiguous_pessimistic_r", "REAL")
        self._ensure_column("strategy_setups", "ambiguous_optimistic_r", "REAL")

        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_setup_branches (
                    setup_id TEXT NOT NULL,
                    branch_key TEXT NOT NULL,
                    branch_status TEXT NOT NULL,
                    resolution_status TEXT,
                    created_ts INTEGER NOT NULL,
                    resolved_ts INTEGER,
                    resolved_price REAL,
                    r_lower REAL,
                    r_upper REAL,
                    tp1_hit INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (setup_id, branch_key),
                    FOREIGN KEY (setup_id) REFERENCES strategy_setups(setup_id)
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

    def _record_commit(self, category: str = "update_pending", symbol: Optional[str] = None) -> None:
        started = time.perf_counter()
        self._conn.commit()
        elapsed = time.perf_counter() - started
        self._perf_inc(category, "commit_count", 1, symbol=symbol)
        self._perf_inc(category, "commit_elapsed_s", elapsed, symbol=symbol)

    def _mark_dirty(
        self,
        units: int = 1,
        category: str = "update_pending",
        symbol: Optional[str] = None,
    ) -> None:
        self._dirty_writes += max(1, int(units))
        if self._dirty_writes >= self._commit_every and not self._candle_transaction_active:
            self._record_commit(category=category, symbol=symbol)
            self._dirty_writes = 0

    def flush(self) -> None:
        with self._lock:
            if self._dirty_writes:
                self._record_commit(category="register_setup")
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
              AND (
                    resolution_status = ?
                    OR (resolution_status = ? AND branch_active = 1)
                  )
            ORDER BY reference_ts ASC
            """,
            (symbol, RESOLUTION_PENDING, RESOLUTION_AMBIGUOUS),
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
        profile = {
            "candidate_gate_elapsed_ms": 0,
            "setup_id_generation_elapsed_ms": 0,
            "duplicate_detection_elapsed_ms": 0,
            "insert_elapsed_ms": 0,
            "cache_append_elapsed_ms": 0,
            "commit_elapsed_ms": 0,
        }
        setup_id: Optional[str] = None

        def _print_register_summary(*, inserted: bool = False, duplicate: bool = False, skipped_gate: bool = False) -> None:
            print(
                "REGISTER_SETUP_SUMMARY "
                f"symbol={symbol} "
                f"setup_id={setup_id or 'None'} "
                f"inserted={str(inserted).lower()} "
                f"duplicate={str(duplicate).lower()} "
                f"skipped_gate={str(skipped_gate).lower()} "
                f"candidate_gate_elapsed_ms={profile['candidate_gate_elapsed_ms']} "
                f"setup_id_generation_elapsed_ms={profile['setup_id_generation_elapsed_ms']} "
                f"duplicate_detection_elapsed_ms={profile['duplicate_detection_elapsed_ms']} "
                f"insert_elapsed_ms={profile['insert_elapsed_ms']} "
                f"cache_append_elapsed_ms={profile['cache_append_elapsed_ms']} "
                f"commit_elapsed_ms={profile['commit_elapsed_ms']} "
                f"total_elapsed_ms={_elapsed_ms(started)}",
                flush=True,
            )

        with self._lock:
            self._perf_inc("register_setup", "call_count", 1)
            gate_started = time.perf_counter()
            self._ensure_pending_loaded(symbol, perf_category="register_setup")

            setup_status = str(setup.get("status") or "").upper()
            duplicate_started = time.perf_counter()
            pending_candidate_exists = any(
                str(row.get("status") or "").upper() == "CANDIDATE"
                for row in self._pending_by_symbol.get(symbol, [])
            )
            profile["duplicate_detection_elapsed_ms"] += _elapsed_ms(duplicate_started)
            if (
                not self.allow_multiple_trades_per_symbol
                and setup_status == "CANDIDATE"
                and pending_candidate_exists
            ):
                profile["candidate_gate_elapsed_ms"] += _elapsed_ms(gate_started)
                elapsed = time.perf_counter() - started
                self._perf_inc("register_setup", "elapsed_s", elapsed)
                _print_register_summary(skipped_gate=True)
                return None
            profile["candidate_gate_elapsed_ms"] += _elapsed_ms(gate_started)

            setup_id_started = time.perf_counter()
            setup_id = self._make_setup_id(symbol, setup, structure_state, now_ts)
            profile["setup_id_generation_elapsed_ms"] += _elapsed_ms(setup_id_started)

            stored_active_column_index = (
                _safe_int(active_column_index)
                if active_column_index is not None
                else _safe_int(structure_state.get("current_column_index"))
            )
            frozen_tick = self._symbol_tick_provenance.get(symbol)
            if frozen_tick is None:
                raise ValueError(
                    f"setup registration requires explicit tick provenance for symbol {symbol}"
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
                "activation_tick_size": frozen_tick["tick_size"],
                "activation_tick_source": frozen_tick["source"],
                "activation_tick_provider": frozen_tick["provider"],
                "activation_tick_venue": frozen_tick["venue"],
                "activation_tick_instrument_type": frozen_tick["instrument_type"],
                "activation_tick_native_symbol": frozen_tick["native_symbol"],
                "activation_tick_source_symbol": frozen_tick["source_symbol"],
                "activation_tick_provenance_timestamp": frozen_tick["provenance_timestamp"],
                "activation_tick_provenance_version": frozen_tick["provenance_version"],
                "last_evaluated_candle_ts": None,
                "validation_state_version": VALIDATION_STATE_VERSION,
                "branch_active": 0,
                "tp1_hit": 0,
                "tp1_hit_ts": None,
                "tp1_price": None,
                "max_favorable_excursion": None,
                "max_adverse_excursion": None,
                "resolution_status": RESOLUTION_PENDING,
                "resolved_ts": None,
                "resolved_price": None,
                "resolution_note": None,
                "ambiguous_pessimistic_r": None,
                "ambiguous_optimistic_r": None,
                "first_outcome_ts": None,
                "last_outcome_ts": None,
                "snapshot_path": snapshot_path,
                "raw_setup_json": json.dumps(setup, ensure_ascii=False, sort_keys=True),
                "raw_structure_json": json.dumps(structure_state, ensure_ascii=False, sort_keys=True),
            }

            insert_started = time.perf_counter()
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
                    activation_tick_size, activation_tick_source,
                    activation_tick_provider, activation_tick_venue,
                    activation_tick_instrument_type, activation_tick_native_symbol,
                    activation_tick_source_symbol, activation_tick_provenance_timestamp,
                    activation_tick_provenance_version,
                    last_evaluated_candle_ts, validation_state_version, branch_active,
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
                    :activation_tick_size, :activation_tick_source,
                    :activation_tick_provider, :activation_tick_venue,
                    :activation_tick_instrument_type, :activation_tick_native_symbol,
                    :activation_tick_source_symbol, :activation_tick_provenance_timestamp,
                    :activation_tick_provenance_version,
                    :last_evaluated_candle_ts, :validation_state_version, :branch_active,
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
            profile["insert_elapsed_ms"] += _elapsed_ms(insert_started)
            if cur.rowcount and cur.rowcount > 0:
                cache_started = time.perf_counter()
                self._pending_by_symbol.setdefault(symbol, []).append(dict(row))
                profile["cache_append_elapsed_ms"] += _elapsed_ms(cache_started)
                commit_before = self._perf_counter("register_setup").get("commit_elapsed_s", 0.0)
                self._mark_dirty(category="register_setup")
                commit_after = self._perf_counter("register_setup").get("commit_elapsed_s", 0.0)
                profile["commit_elapsed_ms"] += round(max(0.0, commit_after - commit_before) * 1000, 3)
                self._perf_inc("register_setup", "successful_inserts", 1)
                inserted = True
                duplicate = False
            else:
                self._perf_inc("register_setup", "duplicate_noop_inserts", 1)
                inserted = False
                duplicate = True
            elapsed = time.perf_counter() - started
            self._perf_inc("register_setup", "elapsed_s", elapsed)
            _print_register_summary(inserted=inserted, duplicate=duplicate)
            return setup_id

    def _should_activate(
        self,
        side: str,
        high_price: float,
        low_price: float,
        ideal_entry: Optional[float],
        tick_size: float,
    ) -> bool:
        if ideal_entry is None:
            return False
        try:
            entry = Decimal(str(ideal_entry))
            tick = Decimal(str(tick_size))
            high = Decimal(str(high_price))
            low = Decimal(str(low_price))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("historical activation requires an explicit positive tick_size") from exc
        if not tick.is_finite() or tick <= 0:
            raise ValueError("historical activation requires an explicit positive tick_size")
        side = str(side or "").upper()
        if side == "LONG":
            return low <= entry - tick
        if side == "SHORT":
            return high >= entry + tick
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
        # Historical OHLC cannot order intrabar touches; approved policy is BE-first.
        if hit_be and hit_tp2:
            return RESOLUTION_TP1_PARTIAL_THEN_BE, be_price, "same_candle_be_and_tp2_be_first"
        if hit_be:
            return RESOLUTION_TP1_PARTIAL_THEN_BE, be_price, "tp1_partial_then_breakeven"
        if hit_tp2:
            return RESOLUTION_TP2, tp2, "tp2_hit_after_tp1"
        return None, None, None

    def _resolve_short_after_tp1(self, low_price, high_price, close_price, be_price, tp2):
        hit_be = high_price >= be_price
        hit_tp2 = tp2 is not None and low_price <= tp2
        # Historical OHLC cannot order intrabar touches; approved policy is BE-first.
        if hit_be and hit_tp2:
            return RESOLUTION_TP1_PARTIAL_THEN_BE, be_price, "same_candle_be_and_tp2_be_first"
        if hit_be:
            return RESOLUTION_TP1_PARTIAL_THEN_BE, be_price, "tp1_partial_then_breakeven"
        if hit_tp2:
            return RESOLUTION_TP2, tp2, "tp2_hit_after_tp1"
        return None, None, None

    def _frozen_provenance_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        setup_id = str(row.get("setup_id") or "")
        symbol = str(row.get("symbol") or "")
        if _safe_int(row.get("validation_state_version")) != VALIDATION_STATE_VERSION:
            if row.get("last_evaluated_candle_ts") is None:
                raise ValueError(
                    f"legacy nonterminal setup {setup_id} has NULL watermark and must remain unchanged"
                )
            raise ValueError(f"legacy nonterminal setup {setup_id} has incomplete validation state")
        values = {
            "provider": row.get("activation_tick_provider"),
            "venue": row.get("activation_tick_venue"),
            "instrument_type": row.get("activation_tick_instrument_type"),
            "native_symbol": row.get("activation_tick_native_symbol"),
            "source_symbol": row.get("activation_tick_source_symbol"),
            "provenance_timestamp": row.get("activation_tick_provenance_timestamp"),
            "provenance_version": row.get("activation_tick_provenance_version"),
            "tick_size": row.get("activation_tick_size"),
            "source": row.get("activation_tick_source"),
        }
        try:
            return _normalize_tick_provenance(
                symbol,
                values,
                self._symbol_identity_allowlist.get(symbol),
            )
        except ValueError as exc:
            raise ValueError(f"legacy incomplete provenance for setup {setup_id}: {exc}") from exc

    @staticmethod
    def _r_for_price(
        side: str,
        entry_price: Optional[float],
        invalidation: Optional[float],
        price: Optional[float],
    ) -> Optional[float]:
        if entry_price is None or invalidation is None or price is None:
            return None
        risk = abs(float(entry_price) - float(invalidation))
        if risk <= 0:
            return None
        if str(side or "").upper() == "LONG":
            return (float(price) - float(entry_price)) / risk
        if str(side or "").upper() == "SHORT":
            return (float(entry_price) - float(price)) / risk
        return None

    def _completed_outcome_r(
        self,
        row: dict[str, Any],
        resolution_status: str,
        resolved_price: Optional[float],
        tp1_hit: bool,
        tp1_price: Optional[float],
    ) -> Optional[float]:
        side = str(row.get("side") or "").upper()
        entry = _safe_float(row.get("activated_price")) or _safe_float(row.get("ideal_entry"))
        invalidation = _safe_float(row.get("invalidation"))
        status = str(resolution_status or "").upper()
        exit_r = self._r_for_price(side, entry, invalidation, resolved_price)
        if status == RESOLUTION_STOPPED:
            return -1.0
        if status == RESOLUTION_TP1_PARTIAL_THEN_BE:
            first_r = self._r_for_price(side, entry, invalidation, tp1_price)
            if first_r is None or exit_r is None:
                return None
            return TP1_PARTIAL_FRACTION * first_r + (1.0 - TP1_PARTIAL_FRACTION) * exit_r
        if status == RESOLUTION_TP2 and tp1_hit:
            first_r = self._r_for_price(side, entry, invalidation, tp1_price)
            if first_r is None or exit_r is None:
                return None
            return TP1_PARTIAL_FRACTION * first_r + (1.0 - TP1_PARTIAL_FRACTION) * exit_r
        return exit_r

    def _persist_setup_state(self, symbol: str, row: dict[str, Any]) -> None:
        self._execute_counted(
            "update_pending",
            """
            UPDATE strategy_setups
            SET updated_ts=:updated_ts,
                bars_observed=:bars_observed,
                activation_status=:activation_status,
                activated_ts=:activated_ts,
                activated_price=:activated_price,
                tp1_hit=:tp1_hit,
                tp1_hit_ts=:tp1_hit_ts,
                tp1_price=:tp1_price,
                max_favorable_excursion=:max_favorable_excursion,
                max_adverse_excursion=:max_adverse_excursion,
                resolution_status=:resolution_status,
                resolved_ts=:resolved_ts,
                resolved_price=:resolved_price,
                resolution_note=:resolution_note,
                ambiguous_pessimistic_r=:ambiguous_pessimistic_r,
                ambiguous_optimistic_r=:ambiguous_optimistic_r,
                last_evaluated_candle_ts=:last_evaluated_candle_ts,
                first_outcome_ts=:first_outcome_ts,
                last_outcome_ts=:last_outcome_ts,
                branch_active=:branch_active
            WHERE setup_id=:setup_id
            """,
            row,
            symbol=symbol,
        )

    def _candle_transaction_boundary(self, name: str) -> None:
        """Test seam for deterministic crash injection; production is a no-op."""
        return None

    def _persist_branch(
        self,
        symbol: str,
        *,
        setup_id: str,
        branch_key: str,
        branch_status: str,
        resolution_status: Optional[str],
        created_ts: int,
        resolved_ts: Optional[int],
        resolved_price: Optional[float],
        r_lower: Optional[float],
        r_upper: Optional[float],
        tp1_hit: bool,
    ) -> None:
        self._execute_counted(
            "update_pending",
            """
            INSERT INTO strategy_setup_branches (
                setup_id, branch_key, branch_status, resolution_status,
                created_ts, resolved_ts, resolved_price, r_lower, r_upper, tp1_hit
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(setup_id, branch_key) DO UPDATE SET
                branch_status=excluded.branch_status,
                resolution_status=excluded.resolution_status,
                resolved_ts=excluded.resolved_ts,
                resolved_price=excluded.resolved_price,
                r_lower=excluded.r_lower,
                r_upper=excluded.r_upper,
                tp1_hit=excluded.tp1_hit
            """,
            (
                setup_id,
                branch_key,
                branch_status,
                resolution_status,
                created_ts,
                resolved_ts,
                resolved_price,
                r_lower,
                r_upper,
                1 if tp1_hit else 0,
            ),
            symbol=symbol,
        )

    def _apply_later_candle(
        self,
        row: dict[str, Any],
        close_ts: int,
        high_price: float,
        low_price: float,
        close_price: float,
    ) -> tuple[dict[str, Any], Optional[tuple[Optional[float], Optional[float]]]]:
        side = str(row.get("side") or "").upper()
        entry = _safe_float(row.get("activated_price")) or _safe_float(row.get("ideal_entry"))
        invalidation = _safe_float(row.get("invalidation"))
        configured_tp1 = _safe_float(row.get("tp1"))
        tp2 = _safe_float(row.get("tp2"))
        tp1_hit = bool(int(row.get("tp1_hit") or 0))
        effective_tp1 = _safe_float(row.get("tp1_price")) if tp1_hit else configured_tp1
        resolution_status = None
        resolved_price = None
        resolution_note = None
        mark_tp1_hit = False

        if entry is not None:
            if side == "LONG":
                fav = max(0.0, high_price - entry)
                adv = max(0.0, entry - low_price)
            else:
                fav = max(0.0, entry - low_price)
                adv = max(0.0, high_price - entry)
            prior_fav = _safe_float(row.get("max_favorable_excursion"))
            prior_adv = _safe_float(row.get("max_adverse_excursion"))
            row["max_favorable_excursion"] = fav if prior_fav is None else max(prior_fav, fav)
            row["max_adverse_excursion"] = adv if prior_adv is None else max(prior_adv, adv)

        if not tp1_hit:
            if BE_MODE and entry is not None and invalidation is not None:
                risk = abs(entry - invalidation)
                if risk > 0:
                    trigger = (
                        entry + BE_TRIGGER_R * risk
                        if side == "LONG"
                        else entry - BE_TRIGGER_R * risk
                    )
                    trigger_hit = high_price >= trigger if side == "LONG" else low_price <= trigger
                    if trigger_hit:
                        effective_tp1 = trigger
            if side == "LONG":
                resolution_status, resolved_price, resolution_note, mark_tp1_hit, _ = (
                    self._resolve_long_before_tp1(
                        low_price, high_price, close_price, invalidation, effective_tp1, tp2
                    )
                )
            elif side == "SHORT":
                resolution_status, resolved_price, resolution_note, mark_tp1_hit, _ = (
                    self._resolve_short_before_tp1(
                        low_price, high_price, close_price, invalidation, effective_tp1, tp2
                    )
                )
            if mark_tp1_hit:
                row["tp1_hit"] = 1
                row["tp1_hit_ts"] = close_ts
                row["tp1_price"] = effective_tp1
                tp1_hit = True
        elif entry is not None:
            be_price = self._breakeven_price(side, entry)
            if side == "LONG":
                resolution_status, resolved_price, resolution_note = self._resolve_long_after_tp1(
                    low_price, high_price, close_price, be_price, tp2
                )
            elif side == "SHORT":
                resolution_status, resolved_price, resolution_note = self._resolve_short_after_tp1(
                    low_price, high_price, close_price, be_price, tp2
                )

        bounds = None
        if resolution_status == RESOLUTION_AMBIGUOUS:
            optimistic_status = None
            optimistic_price = None
            optimistic_tp1_hit = False
            if side == "LONG":
                if tp2 is not None and high_price >= tp2:
                    optimistic_status = RESOLUTION_TP2
                    optimistic_price = tp2
                    optimistic_tp1_hit = True
                elif effective_tp1 is not None and high_price >= effective_tp1:
                    optimistic_status = RESOLUTION_TP1_PARTIAL_THEN_BE
                    optimistic_price = self._breakeven_price(side, entry)
                    optimistic_tp1_hit = True
            else:
                if tp2 is not None and low_price <= tp2:
                    optimistic_status = RESOLUTION_TP2
                    optimistic_price = tp2
                    optimistic_tp1_hit = True
                elif effective_tp1 is not None and low_price <= effective_tp1:
                    optimistic_status = RESOLUTION_TP1_PARTIAL_THEN_BE
                    optimistic_price = self._breakeven_price(side, entry)
                    optimistic_tp1_hit = True
            optimistic_r = self._completed_outcome_r(
                row,
                optimistic_status,
                optimistic_price,
                optimistic_tp1_hit,
                effective_tp1,
            )
            bounds = (-1.0, optimistic_r)
            row["ambiguous_pessimistic_r"] = bounds[0]
            row["ambiguous_optimistic_r"] = bounds[1]
            resolved_price = None
        elif resolution_status is not None:
            exact_r = self._completed_outcome_r(
                row,
                resolution_status,
                resolved_price,
                tp1_hit,
                _safe_float(row.get("tp1_price")),
            )
            bounds = (exact_r, exact_r)

        row["updated_ts"] = close_ts
        row["bars_observed"] = int(row.get("bars_observed") or 0) + 1
        row["last_evaluated_candle_ts"] = close_ts
        row["first_outcome_ts"] = row.get("first_outcome_ts") or close_ts
        row["last_outcome_ts"] = close_ts
        if resolution_status is not None:
            row["resolution_status"] = resolution_status
            row["resolved_ts"] = close_ts
            row["resolved_price"] = resolved_price
            row["resolution_note"] = resolution_note
        return row, bounds

    def update_pending_with_candle(
        self,
        symbol: str,
        close_ts: int,
        high_price: float,
        low_price: float,
        close_price: float,
        tick_size: Optional[float] = None,
        tick_size_source: Optional[str] = None,
    ):
        started = time.perf_counter()
        close_ts = int(close_ts)
        high_price = float(high_price)
        low_price = float(low_price)
        close_price = float(close_price)
        override_requested = tick_size is not None or tick_size_source is not None
        normalized_override_tick = None
        normalized_override_source = None
        if override_requested:
            if symbol not in self._symbol_tick_provenance:
                raise ValueError(f"unknown-symbol override rejected for {symbol}")
            normalized_override_tick = _require_finite_positive_tick(
                tick_size, "historical activation override"
            )
            normalized_override_source = str(tick_size_source or "").strip()
            if not normalized_override_source:
                raise ValueError(
                    "historical activation override requires a non-empty tick_size_source"
                )

        with self._lock:
            self._perf_inc("update_pending", "call_count", 1, symbol=symbol)
            self._ensure_pending_loaded(symbol, perf_category="update_pending")
            pending = self._pending_by_symbol.get(symbol, [])
            self._perf_set("update_pending", "current_pending_count", len(pending), symbol=symbol)
            self._perf_inc("update_pending", "pending_count_total", len(pending), symbol=symbol)
            if not pending:
                return

            provenance_by_setup: dict[str, dict[str, Any]] = {}
            for row in pending:
                frozen = self._frozen_provenance_from_row(row)
                setup_id = str(row.get("setup_id") or "")
                if override_requested and (
                    normalized_override_tick != frozen["tick_size"]
                    or normalized_override_source != frozen["source"]
                ):
                    raise ValueError(
                        f"historical activation override does not match frozen provenance for setup {setup_id}"
                    )
                provenance_by_setup[setup_id] = frozen

            eligible = []
            skipped = []
            for original_row in pending:
                watermark = _safe_int(original_row.get("last_evaluated_candle_ts"))
                if int(original_row.get("reference_ts") or 0) >= close_ts or (
                    watermark is not None and close_ts <= watermark
                ):
                    skipped.append(original_row)
                else:
                    eligible.append(original_row)
            self._perf_inc("update_pending", "trades_scanned", len(pending), symbol=symbol)
            if not eligible:
                self._perf_inc("update_pending", "noop_skipped_count", len(skipped), symbol=symbol)
                self._perf_inc("update_pending", "elapsed_s", time.perf_counter() - started, symbol=symbol)
                return

            if self._conn.in_transaction:
                self._record_commit(category="update_pending", symbol=symbol)
                self._dirty_writes = 0

            next_pending = [dict(row) for row in skipped]
            committed_events: list[str] = []
            savepoint = "validation_candle"
            self._conn.execute(f"SAVEPOINT {savepoint}")
            self._candle_transaction_active = True
            released = False
            try:
                self._candle_transaction_boundary("after_savepoint")
                for original_row in eligible:
                    row = dict(original_row)
                    setup_id = str(row["setup_id"])
                    side = str(row.get("side") or "").upper()
                    ideal_entry = _safe_float(row.get("ideal_entry"))
                    invalidation = _safe_float(row.get("invalidation"))
                    tp1 = _safe_float(row.get("tp1"))
                    tp2 = _safe_float(row.get("tp2"))
                    activation_status = str(row.get("activation_status") or ACTIVATION_PENDING).upper()
                    tick = provenance_by_setup[setup_id]["tick_size"]

                    if activation_status == ACTIVATION_PENDING:
                        bars = int(row.get("bars_observed") or 0) + 1
                        activated = self._should_activate(
                            side, high_price, low_price, ideal_entry, tick
                        )
                        row["updated_ts"] = close_ts
                        row["bars_observed"] = bars
                        row["last_evaluated_candle_ts"] = close_ts
                        if not activated:
                            if bars >= PENDING_EXPIRY_CANDLES:
                                row["resolution_status"] = RESOLUTION_EXPIRED
                                row["resolved_ts"] = close_ts
                                row["resolution_note"] = "pending_not_activated_within_three_candles"
                                committed_events.append("expired")
                            else:
                                next_pending.append(row)
                                committed_events.append("pending")
                            self._persist_setup_state(symbol, row)
                            continue

                        row["activation_status"] = ACTIVATION_ACTIVE
                        row["activated_ts"] = close_ts
                        row["activated_price"] = ideal_entry
                        row["first_outcome_ts"] = row.get("first_outcome_ts") or close_ts
                        row["last_outcome_ts"] = close_ts
                        committed_events.append("activated")
                        stop_hit = invalidation is not None and (
                            (side == "LONG" and low_price <= invalidation)
                            or (side == "SHORT" and high_price >= invalidation)
                        )
                        tp1_hit_on_candle = tp1 is not None and (
                            (side == "LONG" and high_price >= tp1)
                            or (side == "SHORT" and low_price <= tp1)
                        )
                        tp2_hit_on_candle = tp2 is not None and (
                            (side == "LONG" and high_price >= tp2)
                            or (side == "SHORT" and low_price <= tp2)
                        )
                        if stop_hit:
                            row["resolution_status"] = RESOLUTION_STOPPED
                            row["resolved_ts"] = close_ts
                            row["resolved_price"] = invalidation
                            row["resolution_note"] = "activation_candle_stop_touch"
                            committed_events.append("resolved_stop")
                        elif tp1_hit_on_candle or tp2_hit_on_candle:
                            target_status = RESOLUTION_TP2 if tp2_hit_on_candle else "TP1"
                            target_price = tp2 if tp2_hit_on_candle else tp1
                            if tp2_hit_on_candle:
                                target_r = self._completed_outcome_r(
                                    row, RESOLUTION_TP2, target_price, True, tp1
                                )
                            else:
                                target_r = self._r_for_price(
                                    side, ideal_entry, invalidation, target_price
                                )
                            row["resolution_status"] = RESOLUTION_AMBIGUOUS
                            row["resolved_ts"] = None
                            row["resolved_price"] = None
                            row["resolution_note"] = "activation_candle_target_order_unknown:branched"
                            row["ambiguous_pessimistic_r"] = None
                            row["ambiguous_optimistic_r"] = None
                            row["branch_active"] = 1
                            self._persist_branch(
                                symbol,
                                setup_id=setup_id,
                                branch_key="TARGET_TERMINAL",
                                branch_status="COMPLETED",
                                resolution_status=target_status,
                                created_ts=close_ts,
                                resolved_ts=close_ts,
                                resolved_price=target_price,
                                r_lower=target_r,
                                r_upper=target_r,
                                tp1_hit=tp2_hit_on_candle,
                            )
                            self._persist_branch(
                                symbol,
                                setup_id=setup_id,
                                branch_key="STILL_ACTIVE",
                                branch_status="ACTIVE",
                                resolution_status=None,
                                created_ts=close_ts,
                                resolved_ts=None,
                                resolved_price=None,
                                r_lower=None,
                                r_upper=None,
                                tp1_hit=False,
                            )
                            next_pending.append(row)
                            committed_events.append("branched")
                        else:
                            row["resolution_status"] = RESOLUTION_PENDING
                            next_pending.append(row)
                        self._persist_setup_state(symbol, row)
                        continue

                    is_branched = bool(int(row.get("branch_active") or 0))
                    working = dict(row)
                    if is_branched:
                        working["resolution_status"] = RESOLUTION_PENDING
                        working["resolved_ts"] = None
                        working["resolved_price"] = None
                    working, outcome_bounds = self._apply_later_candle(
                        working, close_ts, high_price, low_price, close_price
                    )
                    if is_branched:
                        if outcome_bounds is None:
                            working["resolution_status"] = RESOLUTION_AMBIGUOUS
                            working["resolved_ts"] = None
                            working["resolved_price"] = None
                            working["resolution_note"] = "activation_candle_target_order_unknown:active_branch_open"
                            working["branch_active"] = 1
                            self._persist_branch(
                                symbol,
                                setup_id=setup_id,
                                branch_key="STILL_ACTIVE",
                                branch_status="ACTIVE",
                                resolution_status=None,
                                created_ts=int(row.get("activated_ts") or close_ts),
                                resolved_ts=None,
                                resolved_price=None,
                                r_lower=None,
                                r_upper=None,
                                tp1_hit=bool(int(working.get("tp1_hit") or 0)),
                            )
                            next_pending.append(working)
                        else:
                            active_status = str(working.get("resolution_status") or "")
                            self._persist_branch(
                                symbol,
                                setup_id=setup_id,
                                branch_key="STILL_ACTIVE",
                                branch_status="COMPLETED",
                                resolution_status=active_status,
                                created_ts=int(row.get("activated_ts") or close_ts),
                                resolved_ts=close_ts,
                                resolved_price=_safe_float(working.get("resolved_price")),
                                r_lower=outcome_bounds[0],
                                r_upper=outcome_bounds[1],
                                tp1_hit=bool(int(working.get("tp1_hit") or 0)),
                            )
                            branch_bounds = self._execute_counted(
                                "update_pending",
                                """
                                SELECT r_lower, r_upper
                                FROM strategy_setup_branches
                                WHERE setup_id = ? AND branch_status = 'COMPLETED'
                                ORDER BY branch_key
                                """,
                                (setup_id,),
                                symbol=symbol,
                            ).fetchall()
                            lowers = [_safe_float(item["r_lower"]) for item in branch_bounds]
                            uppers = [_safe_float(item["r_upper"]) for item in branch_bounds]
                            working["resolution_status"] = RESOLUTION_AMBIGUOUS
                            working["resolved_ts"] = close_ts
                            working["resolved_price"] = None
                            working["resolution_note"] = "activation_candle_target_order_unknown:branches_completed"
                            working["ambiguous_pessimistic_r"] = min(
                                value for value in lowers if value is not None
                            )
                            working["ambiguous_optimistic_r"] = max(
                                value for value in uppers if value is not None
                            )
                            working["branch_active"] = 0
                            committed_events.append("branched_resolved")
                    elif str(working.get("resolution_status") or "") == RESOLUTION_PENDING:
                        next_pending.append(working)
                    else:
                        committed_events.append("resolved")
                    self._persist_setup_state(symbol, working)

                self._candle_transaction_boundary("before_release")
                self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                released = True
                self._candle_transaction_boundary("after_release")
            except BaseException:
                if not released:
                    self._conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                self._pending_by_symbol.pop(symbol, None)
                self._pending_loaded_symbols.discard(symbol)
                raise
            finally:
                self._candle_transaction_active = False

            self._record_commit(category="update_pending", symbol=symbol)
            self._dirty_writes = 0
            self._pending_by_symbol[symbol] = next_pending
            self._pending_loaded_symbols.add(symbol)
            self._perf_inc("update_pending", "trades_updated", len(eligible), symbol=symbol)
            self._perf_inc("update_pending", "update_pending_sql_updates_total", len(eligible), symbol=symbol)
            self._perf_inc("update_pending", "elapsed_s", time.perf_counter() - started, symbol=symbol)
            for event in committed_events:
                if event == "pending":
                    self._perf_inc(
                        "update_pending",
                        "update_pending_progress_pending_not_activated",
                        1,
                        symbol=symbol,
                    )
                elif event == "activated":
                    self._perf_inc("update_pending", "trades_activated", 1, symbol=symbol)
                    self._perf_inc("update_pending", "lifecycle_update_count", 1, symbol=symbol)
                    self._perf_inc(
                        "update_pending", "update_pending_event_activation", 1, symbol=symbol
                    )
                elif event in ("resolved", "resolved_stop", "branched_resolved", "expired"):
                    self._perf_inc("update_pending", "trades_resolved", 1, symbol=symbol)
                if event == "resolved_stop":
                    self._perf_inc("update_pending", "stop_hits", 1, symbol=symbol)
            print(
                "VALIDATION_FUNCTION_SUMMARY "
                f"symbol={symbol} rows_scanned={len(pending)} "
                f"rows_updated={len(eligible)} rows_skipped={len(skipped)} "
                f"total_elapsed_ms={_elapsed_ms(started)}",
                flush=True,
            )
