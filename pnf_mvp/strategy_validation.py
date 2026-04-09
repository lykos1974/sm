"""
strategy_validation.py

PnF Strategy Validation Store
=============================

Execution model
---------------
- activation is close-confirmed
- backtest fill price is ideal_entry
- TP/SL resolution uses candle high/low
- no time-based expiry
- default execution constraint: one open trade per symbol
- optional multiple-trades mode via settings.json:
    "allow_multiple_trades_per_symbol": true

Partial-exit model
------------------
- TP1 closes 50% of the position
- after TP1, remaining 50% stop moves to breakeven + fees
- fees buffer = 0.02%
- final outcomes:
    STOPPED
    TP2
    TP1_PARTIAL_THEN_BE
    AMBIGUOUS

Important rules
---------------
- TP2 always implies TP1 happened first
- same-candle TP1 + TP2 is treated as sequential TP1 -> TP2
- same-candle STOP + TP1/TP2 remains AMBIGUOUS

Snapshot support
----------------
This file supports storing:
- active_column_index
- snapshot_path
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
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
    ):
        self.db_path = str(Path(db_path))
        self.allow_multiple_trades_per_symbol = (
            self._load_allow_multiple_from_settings()
            if allow_multiple_trades_per_symbol is None
            else bool(allow_multiple_trades_per_symbol)
        )

        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

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

    def has_open_trade_for_symbol(self, symbol: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1
            FROM strategy_setups
            WHERE symbol = ?
              AND resolution_status = ?
            LIMIT 1
            """,
            (symbol, RESOLUTION_PENDING),
        ).fetchone()
        return row is not None

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

        with self._lock:
            if not self.allow_multiple_trades_per_symbol and self.has_open_trade_for_symbol(symbol):
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
                "snapshot_path": snapshot_path,
                "raw_setup_json": json.dumps(setup, ensure_ascii=False, sort_keys=True),
                "raw_structure_json": json.dumps(structure_state, ensure_ascii=False, sort_keys=True),
                "resolution_status": RESOLUTION_PENDING,
            }

            self._conn.execute(
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
                    :activation_status, NULL, NULL,
                    0, NULL, NULL,
                    NULL, NULL,
                    :resolution_status, NULL, NULL, NULL,
                    NULL, NULL,
                    :snapshot_path,
                    :raw_setup_json, :raw_structure_json
                )
                """,
                row,
            )
            self._conn.commit()

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
        close_ts = int(close_ts)
        high_price = float(high_price)
        low_price = float(low_price)
        close_price = float(close_price)

        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM strategy_setups
                WHERE symbol = ?
                  AND resolution_status = ?
                  AND reference_ts < ?
                ORDER BY reference_ts ASC
                """,
                (symbol, RESOLUTION_PENDING, close_ts),
            ).fetchall()

            for row in rows:
                bars_observed = int(row["bars_observed"] or 0) + 1
                side = str(row["side"] or "").upper()
                ideal_entry = _safe_float(row["ideal_entry"])
                invalidation = _safe_float(row["invalidation"])
                tp1 = _safe_float(row["tp1"])
                tp2 = _safe_float(row["tp2"])
                activation_status = str(row["activation_status"] or ACTIVATION_PENDING).upper()
                tp1_hit = bool(int(row["tp1_hit"] or 0))

                max_fav = _safe_float(row["max_favorable_excursion"])
                max_adv = _safe_float(row["max_adverse_excursion"])
                first_outcome_ts = row["first_outcome_ts"] if row["first_outcome_ts"] is not None else close_ts
                last_outcome_ts = close_ts

                if activation_status == ACTIVATION_PENDING:
                    if self._should_activate(side=side, close_price=close_price, ideal_entry=ideal_entry):
                        activated_price = ideal_entry if ideal_entry is not None else close_price
                        self._conn.execute(
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
                            (close_ts, bars_observed, ACTIVATION_ACTIVE, close_ts, activated_price, first_outcome_ts, last_outcome_ts, row["setup_id"]),
                        )
                        continue

                    self._conn.execute(
                        """
                        UPDATE strategy_setups
                        SET updated_ts = ?,
                            bars_observed = ?,
                            first_outcome_ts = ?,
                            last_outcome_ts = ?
                        WHERE setup_id = ?
                        """,
                        (close_ts, bars_observed, first_outcome_ts, last_outcome_ts, row["setup_id"]),
                    )
                    continue

                if activation_status != ACTIVATION_ACTIVE:
                    continue

                entry_price = _safe_float(row["activated_price"])
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
                    # --- BE ARM BEFORE TP1 ---
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
                        self._conn.execute(
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
                        )
                        continue

                    if mark_tp1_hit and resolution_status is not None:
                        self._conn.execute(
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
                        )
                        continue
                else:
                    if entry_price is None:
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
                    self._conn.execute(
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
                    )
                else:
                    self._conn.execute(
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
                        (close_ts, bars_observed, max_fav, max_adv, resolution_status, close_ts, resolved_price, resolution_note, first_outcome_ts, last_outcome_ts, row["setup_id"]),
                    )

            self._conn.commit()
