#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
PNF_MVP_DIR = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_DIR) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_DIR))

from pnf_engine import PnFEngine, PnFProfile  # noqa: E402
from strategy_engine import evaluate_pullback_retest_long, evaluate_pullback_retest_short  # noqa: E402
from structure_engine import build_structure_state  # noqa: E402

VALIDATION_ELIGIBLE_STATUSES = {"CANDIDATE", "WATCH"}
DEFAULT_FUNNEL_CSV_PATH = "exports/shadow_research_scanner.csv"

FUNNEL_FIELD_ORDER = [
    "symbol",
    "reference_ts",
    "side",
    "status",
    "strategy",
    "reason",
    "reject_reason",
    "quality_score",
    "quality_grade",
    "trend_state",
    "trend_regime",
    "immediate_slope",
    "breakout_context",
    "market_state",
    "latest_signal_name",
    "is_extended_move",
    "active_leg_boxes",
    "zone_low",
    "zone_high",
    "ideal_entry",
    "invalidation",
    "risk",
    "tp1",
    "tp2",
    "rr1",
    "rr2",
    "decision_version",
    "decision_path",
    "watch_flags",
    "reject_flags",
    "promotion_checklist_pass_count",
    "promotion_checklist_failed_items",
    "entry_to_support_boxes",
    "invalidation_distance_boxes",
    "pullback_position_bucket",
    "breakout_context_rank",
    "extension_risk_score",
    "is_baseline_profile_match",
    "shadow_v4_version",
    "shadow_v4_status",
    "shadow_v4_reason",
    "shadow_v4_flags",
    "shadow_v4_rule_hit",
    "shadow_v4_status_delta",
    "shadow_v4_registration_eligible",
    "shadow_continuation_candidate",
    "shadow_continuation_trigger",
    "shadow_entry_price",
    "shadow_stop_price",
    "shadow_continuation_candidate_id",
    "shadow_krausz_short_candidate",
    "shadow_krausz_short_entry",
    "shadow_krausz_short_stop",
    "shadow_krausz_short_tp1",
    "shadow_krausz_short_tp2",
    "shadow_krausz_bounce_short_candidate",
    "shadow_krausz_bounce_short_entry",
    "shadow_krausz_bounce_short_stop",
    "shadow_krausz_bounce_short_tp1",
    "shadow_krausz_bounce_short_tp2",
    "bars_since_breakdown",
    "bounce_depth_boxes",
    "reclaim_fraction",
    "rejection_speed_bars",
    "bounce_column_height",
    "failed_below_breakdown",
    "shadow_reversal_long_candidate",
    "shadow_reversal_long_entry",
    "shadow_reversal_long_stop",
    "shadow_reversal_long_tp1",
    "shadow_reversal_long_tp2",
    "shadow_triple_top_breakout",
    "shadow_triple_bottom_breakdown",
    "shadow_double_top_breakout",
    "shadow_double_bottom_breakdown",
    "shadow_7col_bullish_continuation",
    "shadow_7col_bearish_continuation",
    "shadow_5col_bullish_compression_break",
    "shadow_5col_bearish_compression_break",
    "shadow_bullish_catapult",
    "shadow_bearish_catapult",
    "catapult_has_prior_triple_signal",
    "catapult_has_reaction_column",
    "catapult_has_followup_double_signal",
    "catapult_is_canonical_candidate",
    "shadow_bullish_triangle",
    "shadow_bearish_triangle",
    "shadow_bullish_signal_reversal",
    "shadow_bearish_signal_reversal",
    "shadow_shakeout",
    "pattern_width_columns",
    "pattern_support_level",
    "pattern_resistance_level",
    "pattern_break_distance_boxes",
    "pattern_quality",
    "catapult_support_level",
    "catapult_origin_width",
    "catapult_rebound_columns",
    "catapult_total_columns",
    "catapult_break_distance_boxes",
    "catapult_rebound_failed",
    "catapult_pattern_quality",
    "triple_top_resistance_level",
    "triple_bottom_support_level",
    "triple_pattern_width_columns",
    "breakout_distance_boxes",
    "breakdown_distance_boxes",
    "prior_test_count",
    "pattern_compaction_hint",
    "breakout_column_height_boxes",
    "pattern_is_compact_preferred",
    "pattern_is_broad_warning",
    "early_trend_candidate_flag",
    "blocked_by_existing_open_trade",
    "blocked_by_watch_cap",
    "registered_to_validation",
]


@dataclass(frozen=True)
class ShadowEventState:
    column_count: int
    current_column_kind: str | None
    current_column_top: float | None
    current_column_bottom: float | None
    latest_signal_name: str | None
    breakout_context: str | None
    active_leg_boxes: int | None


@dataclass
class ShadowReversalLongState:
    breakdown_ts: int
    breakdown_level: float
    lowest_low_after_breakdown: float
    has_extended: bool
    has_reclaimed: bool
    initial_active_leg_boxes: int | None


@dataclass(frozen=True)
class CachedStructureSnapshot:
    structure: Dict[str, Any]
    latest_signal_name: str | None
    market_state: str
    latest_all_time_o_breakdown_idx: int | None
    latest_all_time_o_breakdown_level: float | None


class ScannerStructureCache:
    """Incremental structure-state builder for this scanner.

    The public structure engine computes several values by repeatedly scanning
    the full PnF column history. That is fine for interactive snapshots but too
    expensive for an event scanner over ~1M candles. This cache mirrors
    ``build_structure_state`` semantics using only the current/previous columns
    plus small rolling summaries of completed columns.
    """

    EARLY_MIN_COLUMNS = 4
    EXTENSION_BOXES_THRESHOLD = 4
    RECENT_COLUMNS_FOR_BIAS = 5
    REGIME_BIAS_THRESHOLD = 1

    def __init__(self, symbol: str, profile: PnFProfile) -> None:
        self.symbol = symbol
        self.profile = profile
        self.box_size = float(profile.box_size)
        self._last_column_count = 0
        self._completed_x_highs: List[float] = []
        self._completed_o_lows: List[float] = []
        self._completed_kinds: List[str] = []
        self._latest_all_time_o_breakdown_idx: int | None = None
        self._latest_all_time_o_breakdown_level: float | None = None
        self._min_o_low_seen: float | None = None

    @staticmethod
    def _kind(col: Any) -> str:
        return str(getattr(col, "kind", ""))

    @staticmethod
    def _top(col: Any) -> float:
        return float(getattr(col, "top", 0.0))

    @staticmethod
    def _bottom(col: Any) -> float:
        return float(getattr(col, "bottom", 0.0))

    def _ingest_newly_completed_columns(self, columns: List[Any]) -> None:
        # A column becomes "meaningful" for structure_engine when it is no
        # longer the current column (i.e. columns[:-1]). Ingest each such
        # column exactly once when a reversal appends a new current column.
        completed_count = max(0, len(columns) - 1)
        while self._last_column_count < completed_count:
            col = columns[self._last_column_count]
            kind = self._kind(col)
            self._completed_kinds.append(kind)
            if kind == "X":
                self._completed_x_highs.append(self._top(col))
            elif kind == "O":
                low = self._bottom(col)
                if self._min_o_low_seen is None or low <= self._min_o_low_seen:
                    self._latest_all_time_o_breakdown_idx = int(getattr(col, "idx"))
                    self._latest_all_time_o_breakdown_level = low
                    self._min_o_low_seen = low
                self._completed_o_lows.append(low)
            self._last_column_count += 1

    def _refresh_current_o_breakdown(self, current: Any) -> tuple[int | None, float | None]:
        idx = int(getattr(current, "idx"))
        low = self._bottom(current)
        if self._kind(current) == "O" and (self._min_o_low_seen is None or low <= self._min_o_low_seen):
            return idx, low
        return self._latest_all_time_o_breakdown_idx, self._latest_all_time_o_breakdown_level

    def _latest_signal_name(self, current: Any | None) -> str | None:
        if current is None:
            return None
        kind = self._kind(current)
        if kind == "X" and self._completed_x_highs and self._top(current) > self._completed_x_highs[-1]:
            return "BUY"
        if kind == "O" and self._completed_o_lows and self._bottom(current) < self._completed_o_lows[-1]:
            return "SELL"
        return None

    def _market_state(self, columns: List[Any], current: Any, latest_signal_name: str | None) -> str:
        if len(columns) < 2:
            return "EARLY"
        if latest_signal_name == "BUY":
            return "BULLISH_BREAKOUT"
        if latest_signal_name == "SELL":
            return "BEARISH_BREAKDOWN"
        kind = self._kind(current)
        bullish_trend = kind == "X" and bool(self._completed_x_highs) and self._top(current) > self._completed_x_highs[-1]
        bearish_trend = kind == "O" and bool(self._completed_o_lows) and self._bottom(current) < self._completed_o_lows[-1]
        if bullish_trend:
            return "BULLISH_TREND"
        if bearish_trend:
            return "BEARISH_TREND"
        return "RANGE" if len(columns) >= 4 else "NEUTRAL"

    def _swing_direction(self, columns: List[Any]) -> str:
        x_highs = self._completed_x_highs[-2:]
        o_lows = self._completed_o_lows[-2:]
        up = len(x_highs) >= 2 and x_highs[-1] > x_highs[-2]
        down = len(o_lows) >= 2 and o_lows[-1] < o_lows[-2]
        if up and not down:
            return "UP"
        if down and not up:
            return "DOWN"
        if len(columns) >= 2:
            last_completed_kind = self._kind(columns[-2])
            if last_completed_kind == "X":
                return "UP"
            if last_completed_kind == "O":
                return "DOWN"
        return "NEUTRAL"

    def _trend_regime(self, columns: List[Any], market_state: str, swing_direction: str, current: Any) -> str:
        if len(columns) < self.EARLY_MIN_COLUMNS:
            return "EARLY_REGIME"
        x_highs = self._completed_x_highs[-2:]
        o_lows = self._completed_o_lows[-2:]
        bullish_structure = len(x_highs) >= 2 and x_highs[-1] > x_highs[-2]
        bearish_structure = len(o_lows) >= 2 and o_lows[-1] < o_lows[-2]
        ms = market_state.upper()
        bias = sum(1 if k == "X" else -1 if k == "O" else 0 for k in self._completed_kinds[-self.RECENT_COLUMNS_FOR_BIAS :])
        if "BULLISH" in ms:
            return "BULLISH_REGIME"
        if "BEARISH" in ms:
            return "BEARISH_REGIME"
        if bullish_structure and swing_direction == "UP":
            return "BULLISH_REGIME"
        if bearish_structure and swing_direction == "DOWN":
            return "BEARISH_REGIME"
        if bias >= self.REGIME_BIAS_THRESHOLD and swing_direction == "UP":
            return "BULLISH_REGIME"
        if bias <= -self.REGIME_BIAS_THRESHOLD and swing_direction == "DOWN":
            return "BEARISH_REGIME"
        last_x = self._completed_x_highs[-1] if self._completed_x_highs else None
        last_o = self._completed_o_lows[-1] if self._completed_o_lows else None
        if last_x is not None and last_o is not None:
            if self._top(current) >= last_x and self._bottom(current) > last_o:
                return "BULLISH_REGIME"
            if self._bottom(current) <= last_o and self._top(current) < last_x:
                return "BEARISH_REGIME"
        return "RANGE_REGIME"

    def snapshot(self, columns: List[Any], last_price: float | None) -> CachedStructureSnapshot:
        self._ingest_newly_completed_columns(columns)
        if not columns:
            structure = build_structure_state(self.symbol, self.profile, columns, None, "EARLY", last_price)
            return CachedStructureSnapshot(structure, None, "EARLY", None, None)

        current = columns[-1]
        latest_signal_name = self._latest_signal_name(current)
        market_state = self._market_state(columns, current, latest_signal_name)
        swing_direction = self._swing_direction(columns)
        trend_regime = self._trend_regime(columns, market_state, swing_direction, current)

        if len(columns) < self.EARLY_MIN_COLUMNS:
            trend_state = "EARLY"
        else:
            x_highs = self._completed_x_highs[-2:]
            o_lows = self._completed_o_lows[-2:]
            bullish_structure = len(x_highs) >= 2 and len(o_lows) >= 2 and x_highs[-1] > x_highs[-2] and o_lows[-1] > o_lows[-2]
            bearish_structure = len(x_highs) >= 2 and len(o_lows) >= 2 and x_highs[-1] < x_highs[-2] and o_lows[-1] < o_lows[-2]
            if bullish_structure:
                trend_state = "BULLISH"
            elif bearish_structure:
                trend_state = "BEARISH"
            elif trend_regime == "BULLISH_REGIME" and swing_direction == "UP":
                trend_state = "BULLISH"
            elif trend_regime == "BEARISH_REGIME" and swing_direction == "DOWN":
                trend_state = "BEARISH"
            elif "BULLISH" in market_state.upper():
                trend_state = "BULLISH"
            elif "BEARISH" in market_state.upper():
                trend_state = "BEARISH"
            else:
                trend_state = "RANGE"

        current_kind = self._kind(current)
        if current_kind == "X":
            immediate_slope = "BULLISH_PUSH" if trend_state == "BULLISH" or trend_regime == "BULLISH_REGIME" else "BULLISH_REBOUND"
        elif current_kind == "O":
            if trend_state == "BULLISH" or trend_regime == "BULLISH_REGIME":
                immediate_slope = "BEARISH_PULLBACK"
            elif trend_state == "BEARISH" or trend_regime == "BEARISH_REGIME":
                immediate_slope = "BEARISH_PUSH"
            else:
                immediate_slope = "BEARISH_PULLBACK"
        else:
            immediate_slope = "FLAT"

        active_leg_boxes = int(round(abs(self._top(current) - self._bottom(current)) / self.box_size)) if self.box_size > 0 else 0
        is_extended_move = active_leg_boxes >= self.EXTENSION_BOXES_THRESHOLD
        prev_x_high = self._completed_x_highs[-1] if self._completed_x_highs else None
        prev_o_low = self._completed_o_lows[-1] if self._completed_o_lows else None
        if len(columns) < 3:
            breakout_context = "NONE"
        elif is_extended_move:
            breakout_context = "LATE_EXTENSION"
        elif trend_regime == "BULLISH_REGIME" and current_kind == "X" and prev_x_high is not None and self._top(current) > prev_x_high:
            breakout_context = "FRESH_BULLISH_BREAKOUT"
        elif trend_regime == "BULLISH_REGIME" and current_kind == "O":
            breakout_context = "POST_BREAKOUT_PULLBACK"
        elif trend_regime == "BEARISH_REGIME" and current_kind == "O" and prev_o_low is not None and self._bottom(current) < prev_o_low:
            breakout_context = "FRESH_BEARISH_BREAKDOWN"
        elif trend_regime == "BEARISH_REGIME" and current_kind == "X":
            breakout_context = "POST_BREAKDOWN_REBOUND"
        else:
            breakout_context = "NONE"

        impulse_boxes = None
        pullback_boxes = None
        impulse_to_pullback_ratio = None
        if breakout_context == "POST_BREAKOUT_PULLBACK" and current_kind == "O" and immediate_slope == "BEARISH_PULLBACK":
            impulse_boxes = abs(prev_x_high - self._bottom(columns[-2])) / self.box_size if prev_x_high is not None and len(columns) >= 2 and self.box_size > 0 else None
            pullback_boxes = active_leg_boxes if self.box_size > 0 else None
            impulse_to_pullback_ratio = float(impulse_boxes) / float(pullback_boxes) if impulse_boxes is not None and pullback_boxes and pullback_boxes > 0 else None

        support_level = self._completed_o_lows[-1] if self._completed_o_lows else None
        resistance_level = self._completed_x_highs[-1] if self._completed_x_highs else None
        structure = {
            "symbol": self.symbol,
            "trend_state": trend_state,
            "trend_regime": trend_regime,
            "immediate_slope": immediate_slope,
            "swing_direction": swing_direction,
            "support_level": support_level,
            "resistance_level": resistance_level,
            "breakout_context": breakout_context,
            "is_extended_move": is_extended_move,
            "active_leg_boxes": active_leg_boxes,
            "impulse_boxes": impulse_boxes,
            "pullback_boxes": pullback_boxes,
            "impulse_to_pullback_ratio": impulse_to_pullback_ratio,
            "last_meaningful_x_high": resistance_level,
            "last_meaningful_o_low": support_level,
            "current_column_kind": current_kind,
            "current_column_top": self._top(current),
            "current_column_bottom": self._bottom(current),
            "latest_signal_name": latest_signal_name,
            "market_state": market_state,
            "last_price": last_price,
            "notes": [],
        }
        structure["notes"] = [
            f"Trend: {trend_state}",
            f"Trend regime: {trend_regime}",
            f"Immediate slope: {immediate_slope}",
            *( [f"Support identified at {support_level}"] if support_level is not None else [] ),
            *( [f"Resistance identified at {resistance_level}"] if resistance_level is not None else [] ),
            *( [f"Breakout context: {breakout_context}"] if breakout_context != "NONE" else [] ),
            *( ["Move classified as extended"] if is_extended_move else [] ),
        ]
        breakdown_idx, breakdown_level = self._refresh_current_o_breakdown(current)
        return CachedStructureSnapshot(structure, latest_signal_name, market_state, breakdown_idx, breakdown_level)


def load_settings(settings_path: str) -> dict:
    with open(settings_path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_settings_relative_path(settings_path: str, raw_path: str) -> str:
    path = Path(raw_path)
    if path.is_absolute():
        return str(path)
    settings_parent = Path(settings_path).resolve().parent
    candidate = settings_parent / path
    if candidate.exists():
        return str(candidate)
    return str((REPO_ROOT / path).resolve())


def build_profiles(settings: dict) -> Dict[str, PnFProfile]:
    profiles: Dict[str, PnFProfile] = {}
    for symbol in settings["symbols"]:
        p = settings["profiles"][symbol]
        profiles[symbol] = PnFProfile(
            name=symbol,
            box_size=float(p["box_size"]),
            reversal_boxes=int(p["reversal_boxes"]),
        )
    return profiles


def split_symbols(settings: dict, symbols_arg: str | None) -> List[str]:
    if not symbols_arg:
        return list(settings["symbols"])
    wanted = [s.strip() for s in symbols_arg.split(",") if s.strip()]
    return [s for s in settings["symbols"] if s in wanted]


def load_all_closed_candles(database_path: str, symbol: str, sample_limit: int | None = None) -> List[dict]:
    uri = f"file:{Path(database_path).resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        if sample_limit is None:
            cur = conn.execute(
                """
                SELECT close_time, close, high, low
                FROM candles
                WHERE symbol = ? AND interval = '1m'
                ORDER BY open_time ASC
                """,
                (symbol,),
            )
            candles = [dict(r) for r in cur.fetchall()]
        else:
            cur = conn.execute(
                """
                SELECT close_time, close, high, low
                FROM candles
                WHERE symbol = ? AND interval = '1m'
                ORDER BY open_time DESC
                LIMIT ?
                """,
                (symbol, int(sample_limit)),
            )
            candles = list(reversed([dict(r) for r in cur.fetchall()]))
    finally:
        conn.close()
    return candles[:-1] if len(candles) > 1 else []


def _coerce_bool_int(value: Any) -> int:
    return 1 if bool(value) else 0


def _current_column_parts(engine: PnFEngine) -> tuple[int, str | None, float | None, float | None]:
    current_col = engine.columns[-1] if engine.columns else None
    return (
        len(engine.columns),
        str(getattr(current_col, "kind", "")).upper() if current_col is not None else None,
        float(getattr(current_col, "top")) if current_col is not None else None,
        float(getattr(current_col, "bottom")) if current_col is not None else None,
    )


def _event_state_from_engine(engine: PnFEngine, structure: Dict[str, Any] | None = None) -> ShadowEventState:
    column_count, current_column_kind, current_column_top, current_column_bottom = _current_column_parts(engine)
    active_leg_boxes = None
    if structure is not None and structure.get("active_leg_boxes") is not None:
        active_leg_boxes = int(structure["active_leg_boxes"])
    return ShadowEventState(
        column_count=column_count,
        current_column_kind=current_column_kind,
        current_column_top=current_column_top,
        current_column_bottom=current_column_bottom,
        latest_signal_name=engine.latest_signal_name(),
        breakout_context=str(structure.get("breakout_context")) if structure and structure.get("breakout_context") is not None else None,
        active_leg_boxes=active_leg_boxes,
    )


def _basic_event_state_from_engine(engine: PnFEngine) -> tuple[int, str | None, float | None, float | None, str | None]:
    column_count, current_column_kind, current_column_top, current_column_bottom = _current_column_parts(engine)
    return (column_count, current_column_kind, current_column_top, current_column_bottom, engine.latest_signal_name())



def _empty_shadow_triple_pattern_fields() -> Dict[str, Any]:
    return {
        "shadow_triple_top_breakout": 0,
        "shadow_triple_bottom_breakdown": 0,
        "triple_top_resistance_level": None,
        "triple_bottom_support_level": None,
        "triple_pattern_width_columns": None,
        "breakout_distance_boxes": None,
        "breakdown_distance_boxes": None,
        "prior_test_count": None,
        "pattern_compaction_hint": None,
        "breakout_column_height_boxes": None,
        "pattern_is_compact_preferred": None,
        "pattern_is_broad_warning": None,
    }


def _empty_shadow_bearish_catapult_fields() -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    fields.update({
        "shadow_bearish_catapult": 0,
        "catapult_support_level": None,
        "catapult_origin_width": None,
        "catapult_rebound_columns": None,
        "catapult_total_columns": None,
        "catapult_break_distance_boxes": None,
        "catapult_rebound_failed": None,
        "catapult_pattern_quality": None,
    })
    return fields


def _empty_shadow_core_pattern_fields() -> Dict[str, Any]:
    return {
        "shadow_double_top_breakout": 0,
        "shadow_double_bottom_breakdown": 0,
        "shadow_7col_bullish_continuation": 0,
        "shadow_7col_bearish_continuation": 0,
        "shadow_5col_bullish_compression_break": 0,
        "shadow_5col_bearish_compression_break": 0,
        "shadow_bullish_catapult": 0,
        "shadow_bullish_triangle": 0,
        "shadow_bearish_triangle": 0,
        "shadow_bullish_signal_reversal": 0,
        "shadow_bearish_signal_reversal": 0,
        "shadow_shakeout": 0,
        "pattern_width_columns": None,
        "pattern_support_level": None,
        "pattern_resistance_level": None,
        "pattern_break_distance_boxes": None,
        "pattern_quality": None,
        "catapult_has_prior_triple_signal": 0,
        "catapult_has_reaction_column": 0,
        "catapult_has_followup_double_signal": 0,
        "catapult_is_canonical_candidate": 0,
    }


def _core_pattern_flag_names() -> tuple[str, ...]:
    return (
        "shadow_double_top_breakout",
        "shadow_double_bottom_breakdown",
        "shadow_7col_bullish_continuation",
        "shadow_7col_bearish_continuation",
        "shadow_5col_bullish_compression_break",
        "shadow_5col_bearish_compression_break",
        "shadow_bullish_catapult",
        "shadow_bullish_triangle",
        "shadow_bearish_triangle",
        "shadow_bullish_signal_reversal",
        "shadow_bearish_signal_reversal",
        "shadow_shakeout",
    )


def _has_shadow_core_pattern_flag(fields: Dict[str, Any]) -> bool:
    return any(int(fields.get(name) or 0) == 1 for name in _core_pattern_flag_names())


def _first_shadow_core_pattern_flag(fields: Dict[str, Any]) -> str | None:
    for name in _core_pattern_flag_names():
        if int(fields.get(name) or 0) == 1:
            return name
    return None


def _column_kind(column: Any) -> str:
    return str(getattr(column, "kind", "")).upper()


def _column_top(column: Any) -> float:
    return float(getattr(column, "top", 0.0))


def _column_bottom(column: Any) -> float:
    return float(getattr(column, "bottom", 0.0))


def _consecutive_indices(columns: List[Any]) -> bool:
    if not columns:
        return False
    indices = [int(getattr(col, "idx", -1)) for col in columns]
    return indices == list(range(indices[0], indices[0] + len(indices)))


def _pattern_break_distance(break_level: float, reference_level: float, box_size: float, direction: str) -> float | None:
    if box_size <= 0:
        return None
    if direction == "UP":
        return round((break_level - reference_level) / box_size, 4)
    return round((reference_level - break_level) / box_size, 4)


def _emit_core_pattern(
    *,
    flag_name: str,
    width_columns: int,
    support_level: float | None,
    resistance_level: float | None,
    break_distance_boxes: float | None,
    quality: str,
) -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    fields[flag_name] = 1
    fields["pattern_width_columns"] = width_columns
    fields["pattern_support_level"] = support_level
    fields["pattern_resistance_level"] = resistance_level
    fields["pattern_break_distance_boxes"] = break_distance_boxes
    fields["pattern_quality"] = quality
    return fields


def _strict_double_top_breakout_fields(*, structure: Dict[str, Any], columns: List[Any], profile: PnFProfile) -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    if len(columns) < 3 or float(profile.box_size) <= 0:
        return fields
    current = columns[-1]
    if _column_kind(current) != "X" or str(structure.get("latest_signal_name") or "").upper() != "BUY":
        return fields
    sequence = columns[-3:]
    if not _consecutive_indices(sequence) or [_column_kind(col) for col in sequence] != ["X", "O", "X"]:
        return fields
    first_x, pullback_o, breakout_x = sequence
    resistance_level = _column_top(first_x)
    current_top = _column_top(breakout_x)
    if current_top <= resistance_level:
        return fields
    return _emit_core_pattern(
        flag_name="shadow_double_top_breakout",
        width_columns=3,
        support_level=_column_bottom(pullback_o),
        resistance_level=resistance_level,
        break_distance_boxes=_pattern_break_distance(current_top, resistance_level, float(profile.box_size), "UP"),
        quality="STRICT_CONSECUTIVE_3_COL_DOUBLE_TOP",
    )


def _strict_double_bottom_breakdown_fields(*, structure: Dict[str, Any], columns: List[Any], profile: PnFProfile) -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    if len(columns) < 3 or float(profile.box_size) <= 0:
        return fields
    current = columns[-1]
    if _column_kind(current) != "O" or str(structure.get("latest_signal_name") or "").upper() != "SELL":
        return fields
    sequence = columns[-3:]
    if not _consecutive_indices(sequence) or [_column_kind(col) for col in sequence] != ["O", "X", "O"]:
        return fields
    first_o, rebound_x, breakdown_o = sequence
    support_level = _column_bottom(first_o)
    current_bottom = _column_bottom(breakdown_o)
    if current_bottom >= support_level:
        return fields
    return _emit_core_pattern(
        flag_name="shadow_double_bottom_breakdown",
        width_columns=3,
        support_level=support_level,
        resistance_level=_column_top(rebound_x),
        break_distance_boxes=_pattern_break_distance(current_bottom, support_level, float(profile.box_size), "DOWN"),
        quality="STRICT_CONSECUTIVE_3_COL_DOUBLE_BOTTOM",
    )



def _catapult_canonical_diagnostics(sequence: List[Any], direction: str, box_size: float) -> Dict[str, int]:
    """Return textbook catapult component diagnostics for a 7-column terminal sequence.

    A canonical catapult is a prior triple signal, one reaction column, then a
    follow-up double signal in the same direction. These diagnostics are
    intentionally informational only and do not change existing catapult flags.
    """
    diagnostics = {
        "catapult_has_prior_triple_signal": 0,
        "catapult_has_reaction_column": 0,
        "catapult_has_followup_double_signal": 0,
        "catapult_is_canonical_candidate": 0,
    }
    if len(sequence) != 7 or box_size <= 0:
        return diagnostics

    if direction == "UP":
        expected_kinds = ["X", "O", "X", "O", "X", "O", "X"]
        if [_column_kind(col) for col in sequence] != expected_kinds:
            return diagnostics
        first_test, _first_reaction, second_test, _second_reaction, prior_breakout, reaction, followup = sequence
        resistance_level = _column_top(first_test)
        tolerance = 0.25 * box_size
        prior_triple_signal = (
            _column_top(second_test) == resistance_level
            and _column_bottom(prior_breakout) <= resistance_level + tolerance
            and _column_top(prior_breakout) > resistance_level
        )
        reaction_column = _column_kind(reaction) == "O"
        followup_double_signal = _column_top(followup) > _column_top(prior_breakout)
    elif direction == "DOWN":
        expected_kinds = ["O", "X", "O", "X", "O", "X", "O"]
        if [_column_kind(col) for col in sequence] != expected_kinds:
            return diagnostics
        first_test, _first_reaction, second_test, _second_reaction, prior_breakdown, reaction, followup = sequence
        support_level = _column_bottom(first_test)
        tolerance = 0.25 * box_size
        prior_triple_signal = (
            _column_bottom(second_test) == support_level
            and _column_top(prior_breakdown) >= support_level - tolerance
            and _column_bottom(prior_breakdown) < support_level
        )
        reaction_column = _column_kind(reaction) == "X"
        followup_double_signal = _column_bottom(followup) < _column_bottom(prior_breakdown)
    else:
        return diagnostics

    diagnostics["catapult_has_prior_triple_signal"] = int(prior_triple_signal)
    diagnostics["catapult_has_reaction_column"] = int(reaction_column)
    diagnostics["catapult_has_followup_double_signal"] = int(followup_double_signal)
    diagnostics["catapult_is_canonical_candidate"] = int(prior_triple_signal and reaction_column and followup_double_signal)
    return diagnostics


def _generic_7col_continuation_fields(*, structure: Dict[str, Any], columns: List[Any], profile: PnFProfile) -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    if len(columns) < 7 or float(profile.box_size) <= 0:
        return fields
    latest_signal_name = str(structure.get("latest_signal_name") or "").upper()
    sequence = columns[-7:]
    if not _consecutive_indices(sequence):
        return fields
    kinds = [_column_kind(col) for col in sequence]
    if kinds == ["X", "O", "X", "O", "X", "O", "X"] and latest_signal_name == "BUY":
        prior_x_high = max(_column_top(col) for col in sequence[:-1] if _column_kind(col) == "X")
        current_top = _column_top(sequence[-1])
        if current_top <= prior_x_high:
            return fields
        return _emit_core_pattern(
            flag_name="shadow_7col_bullish_continuation",
            width_columns=7,
            support_level=_column_bottom(sequence[-2]),
            resistance_level=prior_x_high,
            break_distance_boxes=_pattern_break_distance(current_top, prior_x_high, float(profile.box_size), "UP"),
            quality="GENERIC_7_COL_BULLISH_CONTINUATION",
        )
    if kinds == ["O", "X", "O", "X", "O", "X", "O"] and latest_signal_name == "SELL":
        prior_o_low = min(_column_bottom(col) for col in sequence[:-1] if _column_kind(col) == "O")
        current_bottom = _column_bottom(sequence[-1])
        if current_bottom >= prior_o_low:
            return fields
        return _emit_core_pattern(
            flag_name="shadow_7col_bearish_continuation",
            width_columns=7,
            support_level=prior_o_low,
            resistance_level=_column_top(sequence[-2]),
            break_distance_boxes=_pattern_break_distance(current_bottom, prior_o_low, float(profile.box_size), "DOWN"),
            quality="GENERIC_7_COL_BEARISH_CONTINUATION",
        )
    return fields


def _generic_5col_compression_break_fields(*, structure: Dict[str, Any], columns: List[Any], profile: PnFProfile) -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    if len(columns) < 5 or float(profile.box_size) <= 0:
        return fields
    latest_signal_name = str(structure.get("latest_signal_name") or "").upper()
    sequence = columns[-5:]
    if not _consecutive_indices(sequence):
        return fields
    kinds = [_column_kind(col) for col in sequence]
    if kinds == ["X", "O", "X", "O", "X"] and latest_signal_name == "BUY":
        first_x, first_o, lower_high_x, higher_low_o, breakout_x = sequence
        resistance_level = _column_top(lower_high_x)
        if _column_top(lower_high_x) >= _column_top(first_x):
            return fields
        if _column_bottom(higher_low_o) <= _column_bottom(first_o):
            return fields
        if _column_top(breakout_x) <= resistance_level:
            return fields
        return _emit_core_pattern(
            flag_name="shadow_5col_bullish_compression_break",
            width_columns=5,
            support_level=_column_bottom(higher_low_o),
            resistance_level=resistance_level,
            break_distance_boxes=_pattern_break_distance(_column_top(breakout_x), resistance_level, float(profile.box_size), "UP"),
            quality="GENERIC_5_COL_BULLISH_COMPRESSION_BREAK",
        )
    if kinds == ["O", "X", "O", "X", "O"] and latest_signal_name == "SELL":
        first_o, first_x, higher_low_o, lower_high_x, breakdown_o = sequence
        support_level = _column_bottom(higher_low_o)
        if _column_bottom(higher_low_o) <= _column_bottom(first_o):
            return fields
        if _column_top(lower_high_x) >= _column_top(first_x):
            return fields
        if _column_bottom(breakdown_o) >= support_level:
            return fields
        return _emit_core_pattern(
            flag_name="shadow_5col_bearish_compression_break",
            width_columns=5,
            support_level=support_level,
            resistance_level=_column_top(lower_high_x),
            break_distance_boxes=_pattern_break_distance(_column_bottom(breakdown_o), support_level, float(profile.box_size), "DOWN"),
            quality="GENERIC_5_COL_BEARISH_COMPRESSION_BREAK",
        )
    return fields

def _strict_bullish_catapult_fields(*, structure: Dict[str, Any], columns: List[Any], profile: PnFProfile) -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    if len(columns) < 7 or float(profile.box_size) <= 0:
        return fields
    current = columns[-1]
    if _column_kind(current) != "X" or str(structure.get("latest_signal_name") or "").upper() != "BUY":
        return fields
    sequence = columns[-7:]
    if not _consecutive_indices(sequence) or [_column_kind(col) for col in sequence] != ["X", "O", "X", "O", "X", "O", "X"]:
        return fields
    first_test, _, second_test, _, first_breakout, pullback, second_breakout = sequence
    resistance_level = _column_top(first_test)
    if _column_top(second_test) != resistance_level:
        return fields
    first_breakout_top = _column_top(first_breakout)
    second_breakout_top = _column_top(second_breakout)
    first_breakout_bottom = _column_bottom(first_breakout)
    second_breakout_bottom = _column_bottom(second_breakout)
    resistance_tolerance = 0.25 * float(profile.box_size)
    if first_breakout_bottom > resistance_level + resistance_tolerance or first_breakout_top <= resistance_level:
        return fields
    if second_breakout_bottom > resistance_level + resistance_tolerance or second_breakout_top <= resistance_level:
        return fields
    if (_column_height_boxes(first_breakout, float(profile.box_size)) or 0) < 2:
        return fields
    if (_column_height_boxes(second_breakout, float(profile.box_size)) or 0) < 2:
        return fields
    fields = _emit_core_pattern(
        flag_name="shadow_bullish_catapult",
        width_columns=7,
        support_level=_column_bottom(pullback),
        resistance_level=resistance_level,
        break_distance_boxes=_pattern_break_distance(second_breakout_top, resistance_level, float(profile.box_size), "UP"),
        quality="STRICT_CONSECUTIVE_7_COL_BULLISH_CATAPULT",
    )
    fields.update(_catapult_canonical_diagnostics(sequence, "UP", float(profile.box_size)))
    return fields


def _strict_triangle_fields(*, structure: Dict[str, Any], columns: List[Any], profile: PnFProfile) -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    if len(columns) < 5 or float(profile.box_size) <= 0:
        return fields
    latest_signal_name = str(structure.get("latest_signal_name") or "").upper()
    sequence = columns[-5:]
    if not _consecutive_indices(sequence):
        return fields
    kinds = [_column_kind(col) for col in sequence]
    if kinds == ["X", "O", "X", "O", "X"] and latest_signal_name == "BUY":
        first_x, first_o, lower_high_x, higher_low_o, breakout_x = sequence
        resistance_level = _column_top(lower_high_x)
        if _column_top(lower_high_x) >= _column_top(first_x):
            return fields
        if _column_bottom(higher_low_o) <= _column_bottom(first_o):
            return fields
        if _column_top(breakout_x) <= resistance_level:
            return fields
        return _emit_core_pattern(
            flag_name="shadow_bullish_triangle",
            width_columns=5,
            support_level=_column_bottom(higher_low_o),
            resistance_level=resistance_level,
            break_distance_boxes=_pattern_break_distance(_column_top(breakout_x), resistance_level, float(profile.box_size), "UP"),
            quality="STRICT_CONSECUTIVE_5_COL_TRIANGLE_UP_BREAK",
        )
    if kinds == ["O", "X", "O", "X", "O"] and latest_signal_name == "SELL":
        first_o, first_x, higher_low_o, lower_high_x, breakdown_o = sequence
        support_level = _column_bottom(higher_low_o)
        if _column_bottom(higher_low_o) <= _column_bottom(first_o):
            return fields
        if _column_top(lower_high_x) >= _column_top(first_x):
            return fields
        if _column_bottom(breakdown_o) >= support_level:
            return fields
        return _emit_core_pattern(
            flag_name="shadow_bearish_triangle",
            width_columns=5,
            support_level=support_level,
            resistance_level=_column_top(lower_high_x),
            break_distance_boxes=_pattern_break_distance(_column_bottom(breakdown_o), support_level, float(profile.box_size), "DOWN"),
            quality="STRICT_CONSECUTIVE_5_COL_TRIANGLE_DOWN_BREAK",
        )
    return fields


def _strict_signal_reversal_fields(*, structure: Dict[str, Any], columns: List[Any], profile: PnFProfile) -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    if len(columns) < 6 or float(profile.box_size) <= 0:
        return fields
    latest_signal_name = str(structure.get("latest_signal_name") or "").upper()
    sequence = columns[-6:]
    if not _consecutive_indices(sequence):
        return fields
    kinds = [_column_kind(col) for col in sequence]
    if kinds == ["O", "X", "O", "X", "O", "X"] and latest_signal_name == "BUY":
        first_o = sequence[0]
        first_x = sequence[1]
        lower_low_o = sequence[2]
        lower_high_x = sequence[3]
        confirming_o = sequence[4]
        breakout_x = sequence[5]
        if _column_bottom(lower_low_o) >= _column_bottom(first_o):
            return fields
        if _column_top(lower_high_x) >= _column_top(first_x):
            return fields
        resistance_level = _column_top(lower_high_x)
        if _column_top(breakout_x) <= resistance_level:
            return fields
        return _emit_core_pattern(
            flag_name="shadow_bearish_signal_reversal",
            width_columns=6,
            support_level=_column_bottom(confirming_o),
            resistance_level=resistance_level,
            break_distance_boxes=_pattern_break_distance(_column_top(breakout_x), resistance_level, float(profile.box_size), "UP"),
            quality="STRICT_CONSECUTIVE_6_COL_BEARISH_SIGNAL_REVERSED",
        )
    if kinds == ["X", "O", "X", "O", "X", "O"] and latest_signal_name == "SELL":
        first_x = sequence[0]
        first_o = sequence[1]
        higher_high_x = sequence[2]
        higher_low_o = sequence[3]
        confirming_x = sequence[4]
        breakdown_o = sequence[5]
        if _column_top(higher_high_x) <= _column_top(first_x):
            return fields
        if _column_bottom(higher_low_o) <= _column_bottom(first_o):
            return fields
        support_level = _column_bottom(higher_low_o)
        if _column_bottom(breakdown_o) >= support_level:
            return fields
        return _emit_core_pattern(
            flag_name="shadow_bullish_signal_reversal",
            width_columns=6,
            support_level=support_level,
            resistance_level=_column_top(confirming_x),
            break_distance_boxes=_pattern_break_distance(_column_bottom(breakdown_o), support_level, float(profile.box_size), "DOWN"),
            quality="STRICT_CONSECUTIVE_6_COL_BULLISH_SIGNAL_REVERSED",
        )
    return fields

def _strict_shakeout_fields(*, structure: Dict[str, Any], columns: List[Any], profile: PnFProfile) -> Dict[str, Any]:
    fields = _empty_shadow_core_pattern_fields()
    if len(columns) < 5 or float(profile.box_size) <= 0:
        return fields
    current = columns[-1]
    if _column_kind(current) != "X" or str(structure.get("latest_signal_name") or "").upper() != "BUY":
        return fields
    sequence = columns[-5:]
    if not _consecutive_indices(sequence) or [_column_kind(col) for col in sequence] != ["X", "O", "X", "O", "X"]:
        return fields
    first_top_x, first_o, second_top_x, shakeout_o, recovery_x = sequence
    if _column_top(second_top_x) != _column_top(first_top_x):
        return fields
    if _column_bottom(shakeout_o) >= _column_bottom(first_o):
        return fields
    resistance_level = _column_top(second_top_x)
    if _column_top(recovery_x) <= resistance_level:
        return fields
    return _emit_core_pattern(
        flag_name="shadow_shakeout",
        width_columns=5,
        support_level=_column_bottom(first_o),
        resistance_level=resistance_level,
        break_distance_boxes=_pattern_break_distance(_column_top(recovery_x), resistance_level, float(profile.box_size), "UP"),
        quality="STRICT_CONSECUTIVE_5_COL_SHAKEOUT",
    )


def _core_pattern_event_level(flag: str, fields: Dict[str, Any]) -> float | None:
    up_break_flags = {
        "shadow_double_top_breakout",
        "shadow_7col_bullish_continuation",
        "shadow_5col_bullish_compression_break",
        "shadow_bullish_catapult",
        "shadow_bullish_triangle",
        "shadow_bearish_signal_reversal",
        "shadow_shakeout",
    }
    level = fields.get("pattern_resistance_level") if flag in up_break_flags else fields.get("pattern_support_level")
    return float(level) if isinstance(level, (int, float)) else None


def _compute_shadow_core_pattern_field_sets(
    *,
    structure: Dict[str, Any],
    columns: List[Any],
    profile: PnFProfile,
    emitted_core_pattern_keys: set[tuple[str, float | None, int]] | None = None,
) -> List[Dict[str, Any]]:
    """Detect diagnostic-only core PnF structural patterns on the current event.

    All detectors inspect only the current consecutive terminal column sequence
    required by the textbook pattern. They produce structural flags and audit
    diagnostics only; no entry, stop, target, validation, or strategy fields are
    derived here. More specific patterns are allowed to co-exist with their
    underlying double-top/double-bottom signal so pattern counts remain auditable.
    """
    detectors = (
        _strict_double_top_breakout_fields,
        _strict_double_bottom_breakdown_fields,
        _generic_7col_continuation_fields,
        _generic_5col_compression_break_fields,
        _strict_bullish_catapult_fields,
        _strict_triangle_fields,
        _strict_signal_reversal_fields,
        _strict_shakeout_fields,
    )
    field_sets: List[Dict[str, Any]] = []
    current_idx = int(getattr(columns[-1], "idx", len(columns) - 1)) if columns else -1
    for detector in detectors:
        fields = detector(structure=structure, columns=columns, profile=profile)
        flag = _first_shadow_core_pattern_flag(fields)
        if flag is None:
            continue
        key = (flag, _core_pattern_event_level(flag, fields), current_idx)
        if emitted_core_pattern_keys is not None:
            if key in emitted_core_pattern_keys:
                continue
            emitted_core_pattern_keys.add(key)
        field_sets.append(fields)
    return field_sets


def _compute_shadow_core_pattern_fields(
    *,
    structure: Dict[str, Any],
    columns: List[Any],
    profile: PnFProfile,
    emitted_core_pattern_keys: set[tuple[str, float | None, int]] | None = None,
) -> Dict[str, Any]:
    field_sets = _compute_shadow_core_pattern_field_sets(
        structure=structure,
        columns=columns,
        profile=profile,
        emitted_core_pattern_keys=emitted_core_pattern_keys,
    )
    return field_sets[0] if field_sets else _empty_shadow_core_pattern_fields()


def _pattern_compaction_hint(width_columns: int | None) -> str | None:
    if width_columns is None:
        return None
    if width_columns <= 5:
        return "COMPACT"
    if width_columns <= 9:
        return "BALANCED"
    return "BROAD"


def _exact_prior_tests(prior_columns: List[Any], level_attr: str, candidate_level: float) -> List[Any]:
    return [col for col in prior_columns if float(getattr(col, level_attr, 0.0)) == candidate_level]


def _column_height_boxes(column: Any, box_size: float) -> int | None:
    if box_size <= 0:
        return None
    return int(round(abs(float(getattr(column, "top", 0.0)) - float(getattr(column, "bottom", 0.0))) / box_size))


def _apply_triple_pattern_compaction_diagnostics(out: Dict[str, Any], width_columns: int | None) -> None:
    compaction_hint = _pattern_compaction_hint(width_columns)
    out["pattern_compaction_hint"] = compaction_hint
    out["pattern_is_compact_preferred"] = 1 if compaction_hint in {"COMPACT", "BALANCED"} else 0
    out["pattern_is_broad_warning"] = 1 if compaction_hint == "BROAD" else 0


def _compute_shadow_triple_pattern_fields(
    *,
    structure: Dict[str, Any],
    columns: List[Any],
    profile: PnFProfile,
    emitted_triple_top_levels: set[float] | None = None,
    emitted_triple_bottom_levels: set[float] | None = None,
    recent_window_columns: int = 21,
) -> Dict[str, Any]:
    """Classify sparse v2 triple-top/bottom PnF structural events.

    This helper is intentionally diagnostic-only. It uses the scanner's
    close-updated PnF columns and the existing BUY/SELL signal semantics as
    the breakout/breakdown confirmation, then inspects only a bounded local
    column window so event processing remains O(1) per structural change.
    v2 emits fresh defended-level breaks only: prior defenses must be exact
    same-level tests, the current break column is excluded from defense counts,
    and caller-provided per-symbol emitted-level state suppresses repeats.
    """
    out = _empty_shadow_triple_pattern_fields()
    if len(columns) < 5:
        return out

    box_size = float(profile.box_size)
    if box_size <= 0:
        return out

    current = columns[-1]
    current_kind = str(getattr(current, "kind", "")).upper()
    latest_signal_name = str(structure.get("latest_signal_name") or "").upper()
    local_columns = columns[-recent_window_columns:]
    current_idx = int(getattr(current, "idx", len(columns) - 1))
    breakout_column_height_boxes = _column_height_boxes(current, box_size)

    if current_kind == "X" and latest_signal_name == "BUY":
        out["breakout_column_height_boxes"] = breakout_column_height_boxes
        if breakout_column_height_boxes is None or breakout_column_height_boxes < 2:
            return out
        prior_x = [col for col in local_columns[:-1] if str(getattr(col, "kind", "")).upper() == "X"]
        current_top = float(getattr(current, "top", 0.0))
        emitted_levels = emitted_triple_top_levels or set()
        candidate_levels = sorted(
            {float(getattr(col, "top", 0.0)) for col in prior_x if float(getattr(col, "top", 0.0)) < current_top},
            reverse=True,
        )
        for resistance_level in candidate_levels:
            if resistance_level in emitted_levels:
                continue
            tests = _exact_prior_tests(prior_x, "top", resistance_level)
            if len(tests) < 2:
                continue
            earliest_idx = min(int(getattr(col, "idx", 0)) for col in tests)
            width_columns = current_idx - earliest_idx + 1
            if width_columns < 5:
                continue
            out["shadow_triple_top_breakout"] = 1
            out["triple_top_resistance_level"] = resistance_level
            out["triple_pattern_width_columns"] = width_columns
            out["breakout_distance_boxes"] = round((current_top - resistance_level) / box_size, 4)
            out["prior_test_count"] = len(tests)
            _apply_triple_pattern_compaction_diagnostics(out, width_columns)
            return out

    if current_kind == "O" and latest_signal_name == "SELL":
        out["breakout_column_height_boxes"] = breakout_column_height_boxes
        if breakout_column_height_boxes is None or breakout_column_height_boxes < 2:
            return out
        prior_o = [col for col in local_columns[:-1] if str(getattr(col, "kind", "")).upper() == "O"]
        current_bottom = float(getattr(current, "bottom", 0.0))
        emitted_levels = emitted_triple_bottom_levels or set()
        candidate_levels = sorted(
            {float(getattr(col, "bottom", 0.0)) for col in prior_o if float(getattr(col, "bottom", 0.0)) > current_bottom}
        )
        for support_level in candidate_levels:
            if support_level in emitted_levels:
                continue
            tests = _exact_prior_tests(prior_o, "bottom", support_level)
            if len(tests) < 2:
                continue
            earliest_idx = min(int(getattr(col, "idx", 0)) for col in tests)
            width_columns = current_idx - earliest_idx + 1
            if width_columns < 5:
                continue
            out["shadow_triple_bottom_breakdown"] = 1
            out["triple_bottom_support_level"] = support_level
            out["triple_pattern_width_columns"] = width_columns
            out["breakdown_distance_boxes"] = round((support_level - current_bottom) / box_size, 4)
            out["prior_test_count"] = len(tests)
            _apply_triple_pattern_compaction_diagnostics(out, width_columns)
            return out

    return out


def _catapult_pattern_quality(total_columns: int | None) -> str | None:
    if total_columns == 7:
        return "STRICT_CONSECUTIVE_7_COL"
    return None


def _strict_bearish_catapult_fields(
    *, structure: Dict[str, Any], columns: List[Any], profile: PnFProfile
) -> Dict[str, Any]:
    """Detect only the textbook seven-consecutive-column bearish catapult.

    The strict sequence is:
    O support test -> X -> O support test -> X -> first O breakdown ->
    single X rebound -> second O breakdown.

    This intentionally avoids carrying state across a broad/local window; the
    current column must be the seventh consecutive column in the catapult.
    """
    fields = _empty_shadow_bearish_catapult_fields()
    if len(columns) < 7:
        return fields

    box_size = float(profile.box_size)
    if box_size <= 0:
        return fields

    current = columns[-1]
    if str(getattr(current, "kind", "")).upper() != "O":
        return fields
    if str(structure.get("latest_signal_name") or "").upper() != "SELL":
        return fields

    sequence = columns[-7:]
    sequence_indices = [int(getattr(col, "idx", -1)) for col in sequence]
    if sequence_indices != list(range(sequence_indices[0], sequence_indices[0] + 7)):
        return fields

    expected_kinds = ["O", "X", "O", "X", "O", "X", "O"]
    actual_kinds = [str(getattr(col, "kind", "")).upper() for col in sequence]
    if actual_kinds != expected_kinds:
        return fields

    first_test, _, second_test, _, first_breakdown, _rebound, second_breakdown = sequence
    support_level = float(getattr(first_test, "bottom", 0.0))
    if float(getattr(second_test, "bottom", 0.0)) != support_level:
        return fields

    first_breakdown_bottom = float(getattr(first_breakdown, "bottom", 0.0))
    second_breakdown_bottom = float(getattr(second_breakdown, "bottom", 0.0))
    first_breakdown_top = float(getattr(first_breakdown, "top", 0.0))
    second_breakdown_top = float(getattr(second_breakdown, "top", 0.0))

    # Both sell columns must be actual triple-bottom breakdowns of the same
    # defended support. Requiring the O columns to span the support prevents a
    # drifting continuation leg from being misclassified as a catapult.
    support_tolerance = 0.25 * box_size
    if first_breakdown_top < support_level - support_tolerance or first_breakdown_bottom >= support_level:
        return fields
    if second_breakdown_top < support_level - support_tolerance or second_breakdown_bottom >= support_level:
        return fields

    first_breakdown_height = _column_height_boxes(first_breakdown, box_size)
    second_breakdown_height = _column_height_boxes(second_breakdown, box_size)
    if first_breakdown_height is None or first_breakdown_height < 2:
        return fields
    if second_breakdown_height is None or second_breakdown_height < 2:
        return fields

    fields.update(
        {
            "shadow_bearish_catapult": 1,
            "pattern_width_columns": 7,
            "pattern_support_level": support_level,
            "pattern_resistance_level": float(getattr(_rebound, "top", 0.0)),
            "pattern_break_distance_boxes": round((support_level - second_breakdown_bottom) / box_size, 4),
            "pattern_quality": "STRICT_CONSECUTIVE_7_COL_BEARISH_CATAPULT",
            "catapult_support_level": support_level,
            "catapult_origin_width": 5,
            "catapult_rebound_columns": 1,
            "catapult_total_columns": 7,
            "catapult_break_distance_boxes": round((support_level - second_breakdown_bottom) / box_size, 4),
            "catapult_rebound_failed": 1,
            "catapult_pattern_quality": _catapult_pattern_quality(7),
        }
    )
    fields.update(_catapult_canonical_diagnostics(sequence, "DOWN", box_size))
    return fields


def update_shadow_bearish_catapult_states(
    *,
    active_states: Dict[float, Any],
    emitted_levels: set[float],
    triple_pattern_fields: Dict[str, Any],
    structure: Dict[str, Any],
    columns: List[Any],
    profile: PnFProfile,
    max_total_columns: int = 7,
) -> Dict[str, Any]:
    """Emit strict bearish catapults without broad/local-window state.

    The legacy implementation tracked a prior triple-bottom breakdown through a
    broad local window and waited for a later weak rebound failure. The research
    definition is now stricter: only the current seven consecutive PnF columns
    may participate in the catapult, so any carried active state is discarded.
    """
    del triple_pattern_fields, max_total_columns
    active_states.clear()

    fields = _strict_bearish_catapult_fields(structure=structure, columns=columns, profile=profile)
    support_level = fields.get("catapult_support_level")
    if int(fields.get("shadow_bearish_catapult") or 0) != 1 or support_level is None:
        return fields
    if float(support_level) in emitted_levels:
        return _empty_shadow_bearish_catapult_fields()

    emitted_levels.add(float(support_level))
    return fields


def _compute_shadow_continuation_fields(
    *, setup: Dict[str, Any], structure: Dict[str, Any], columns: List[Any], profile: PnFProfile
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "shadow_continuation_candidate": 0,
        "shadow_continuation_trigger": 0,
        "shadow_entry_price": None,
        "shadow_stop_price": None,
        "shadow_continuation_candidate_id": None,
    }
    if str(setup.get("side") or "").upper() != "LONG":
        return out
    if str(structure.get("breakout_context") or "").upper() != "LATE_EXTENSION":
        return out
    if str(structure.get("trend_regime") or "").upper() != "BULLISH_REGIME":
        return out
    active_leg_boxes = structure.get("active_leg_boxes")
    if active_leg_boxes is None or int(active_leg_boxes) < 4:
        return out
    if not columns:
        return out
    box_size = float(profile.box_size)
    current_col = columns[-1]
    if str(getattr(current_col, "kind", "")).upper() == "O":
        pullback_depth_boxes = int(
            round(abs(float(getattr(current_col, "top", 0.0)) - float(getattr(current_col, "bottom", 0.0))) / box_size)
        )
        if pullback_depth_boxes <= 4:
            out["shadow_continuation_candidate"] = 1
    if len(columns) < 3:
        return out
    new_x = columns[-1]
    prev_o = columns[-2]
    if str(getattr(new_x, "kind", "")).upper() != "X" or str(getattr(prev_o, "kind", "")).upper() != "O":
        return out
    prev_x = None
    for col in reversed(columns[:-2]):
        if str(getattr(col, "kind", "")).upper() == "X":
            prev_x = col
            break
    if prev_x is None:
        return out
    prev_o_depth_boxes = int(
        round(abs(float(getattr(prev_o, "top", 0.0)) - float(getattr(prev_o, "bottom", 0.0))) / box_size)
    )
    if prev_o_depth_boxes > 4:
        return out
    if float(getattr(new_x, "top", 0.0)) <= float(getattr(prev_x, "top", 0.0)):
        return out
    out["shadow_continuation_trigger"] = 1
    out["shadow_entry_price"] = float(getattr(prev_x, "top", 0.0))
    out["shadow_stop_price"] = float(getattr(prev_o, "bottom", 0.0))
    return out


def _compute_shadow_krausz_short_fields(*, structure: Dict[str, Any], columns: List[Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "shadow_krausz_short_candidate": 0,
        "shadow_krausz_short_entry": None,
        "shadow_krausz_short_stop": None,
        "shadow_krausz_short_tp1": None,
        "shadow_krausz_short_tp2": None,
    }
    if str(structure.get("latest_signal_name") or "").upper() != "SELL":
        return out
    if str(structure.get("breakout_context") or "").upper() != "FRESH_BEARISH_BREAKDOWN":
        return out
    if not columns:
        return out
    current_col = columns[-1]
    if str(getattr(current_col, "kind", "")).upper() != "O":
        return out
    entry = float(getattr(current_col, "bottom", 0.0))
    stop = None
    for col in reversed(columns[:-1]):
        if str(getattr(col, "kind", "")).upper() == "X":
            stop = float(getattr(col, "top", 0.0))
            break
    if stop is None:
        resistance_level = structure.get("resistance_level")
        if isinstance(resistance_level, (int, float)):
            stop = float(resistance_level)
    if stop is None:
        return out
    risk = stop - entry
    if risk <= 0:
        return out
    out["shadow_krausz_short_candidate"] = 1
    out["shadow_krausz_short_entry"] = entry
    out["shadow_krausz_short_stop"] = stop
    out["shadow_krausz_short_tp1"] = entry - (2.0 * risk)
    out["shadow_krausz_short_tp2"] = entry - (3.0 * risk)
    return out


def _bars_between(start_ts: int | None, end_ts: int | None) -> int | None:
    if start_ts is None or end_ts is None:
        return None
    return max(0, int(round((int(end_ts) - int(start_ts)) / 60000.0)))


def _previous_o_bottom_before(columns: List[Any], column_idx: int) -> float | None:
    for col in reversed(columns[:column_idx]):
        if str(getattr(col, "kind", "")).upper() == "O":
            return float(getattr(col, "bottom", 0.0))
    return None


def _compute_shadow_krausz_bounce_short_fields(
    *,
    structure: Dict[str, Any],
    columns: List[Any],
    profile: PnFProfile,
    max_bounce_boxes: int = 2,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "shadow_krausz_bounce_short_candidate": 0,
        "shadow_krausz_bounce_short_entry": None,
        "shadow_krausz_bounce_short_stop": None,
        "shadow_krausz_bounce_short_tp1": None,
        "shadow_krausz_bounce_short_tp2": None,
        "bars_since_breakdown": None,
        "bounce_depth_boxes": None,
        "reclaim_fraction": None,
        "rejection_speed_bars": None,
        "bounce_column_height": None,
        "failed_below_breakdown": None,
    }
    if str(structure.get("latest_signal_name") or "").upper() != "SELL":
        return out
    if "BREAKDOWN" not in str(structure.get("breakout_context") or "").upper():
        return out
    if len(columns) < 4:
        return out
    resumed_o = columns[-1]
    bounce_x = columns[-2]
    breakdown_o = columns[-3]
    prior_x = columns[-4]
    if str(getattr(resumed_o, "kind", "")).upper() != "O":
        return out
    if str(getattr(bounce_x, "kind", "")).upper() != "X":
        return out
    if str(getattr(breakdown_o, "kind", "")).upper() != "O":
        return out
    if str(getattr(prior_x, "kind", "")).upper() != "X":
        return out
    bounce_size_boxes = int(round(abs(float(getattr(bounce_x, "top", 0.0)) - float(getattr(bounce_x, "bottom", 0.0)))))
    if bounce_size_boxes > max_bounce_boxes:
        return out
    breakdown_level = float(getattr(breakdown_o, "bottom", 0.0))
    failure_reclaim_level = float(getattr(prior_x, "top", 0.0))
    bounce_high = float(getattr(bounce_x, "top", 0.0))
    resumed_low = float(getattr(resumed_o, "bottom", 0.0))
    if bounce_high >= failure_reclaim_level:
        return out
    if resumed_low >= breakdown_level:
        return out
    entry = breakdown_level
    stop = bounce_high
    risk = stop - entry
    if risk <= 0:
        return out
    out["shadow_krausz_bounce_short_candidate"] = 1
    out["shadow_krausz_bounce_short_entry"] = entry
    out["shadow_krausz_bounce_short_stop"] = stop
    out["shadow_krausz_bounce_short_tp1"] = entry - (2.0 * risk)
    out["shadow_krausz_bounce_short_tp2"] = entry - (3.0 * risk)

    box_size = float(profile.box_size)
    initial_breakdown_level = _previous_o_bottom_before(columns, int(getattr(breakdown_o, "idx", 0)))
    if initial_breakdown_level is None:
        initial_breakdown_level = breakdown_level
    if box_size > 0:
        out["bounce_column_height"] = int(
            round(abs(bounce_high - float(getattr(bounce_x, "bottom", 0.0))) / box_size)
        )
        out["bounce_depth_boxes"] = max(0.0, (bounce_high - breakdown_level) / box_size)
    breakdown_range = initial_breakdown_level - breakdown_level
    if breakdown_range > 0:
        out["reclaim_fraction"] = (bounce_high - breakdown_level) / breakdown_range
    else:
        out["reclaim_fraction"] = None
    out["bars_since_breakdown"] = _bars_between(
        int(getattr(breakdown_o, "start_ts", 0)),
        int(getattr(resumed_o, "start_ts", 0)),
    )
    out["rejection_speed_bars"] = _bars_between(
        int(getattr(bounce_x, "end_ts", 0)),
        int(getattr(resumed_o, "start_ts", 0)),
    )
    out["failed_below_breakdown"] = 1 if bounce_high <= initial_breakdown_level else 0
    return out


def _empty_shadow_reversal_long_fields() -> Dict[str, Any]:
    return {
        "shadow_reversal_long_candidate": 0,
        "shadow_reversal_long_entry": None,
        "shadow_reversal_long_stop": None,
        "shadow_reversal_long_tp1": None,
        "shadow_reversal_long_tp2": None,
    }


def _shadow_reversal_long_fields_from_state(state: ShadowReversalLongState | None) -> Dict[str, Any]:
    out = _empty_shadow_reversal_long_fields()
    if state is None or not state.has_reclaimed:
        return out
    entry = float(state.breakdown_level)
    stop = float(state.lowest_low_after_breakdown)
    risk = entry - stop
    if risk <= 0:
        return out
    out["shadow_reversal_long_candidate"] = 1
    out["shadow_reversal_long_entry"] = entry
    out["shadow_reversal_long_stop"] = stop
    out["shadow_reversal_long_tp1"] = entry + (2.0 * risk)
    out["shadow_reversal_long_tp2"] = entry + (3.0 * risk)
    return out


def _current_structure_low(structure: Dict[str, Any], fallback_price: float) -> float:
    current_bottom = structure.get("current_column_bottom")
    if isinstance(current_bottom, (int, float)):
        return min(float(current_bottom), float(fallback_price))
    return float(fallback_price)


def _structure_has_reclaimed_breakdown(structure: Dict[str, Any], breakdown_level: float) -> bool:
    current_kind = str(structure.get("current_column_kind") or "").upper()
    current_top = structure.get("current_column_top")
    if (
        current_kind == "X"
        and isinstance(current_top, (int, float))
        and float(current_top) > float(breakdown_level)
    ):
        return True
    return False


def update_shadow_reversal_long_state(
    *,
    state: ShadowReversalLongState | None,
    reference_ts: int,
    close_price: float,
    structure: Dict[str, Any],
) -> tuple[ShadowReversalLongState | None, Dict[str, Any]]:
    candidate_fields = _empty_shadow_reversal_long_fields()
    latest_signal_name = str(structure.get("latest_signal_name") or "").upper()
    current_kind = str(structure.get("current_column_kind") or "").upper()
    active_leg_boxes = structure.get("active_leg_boxes")
    active_leg_boxes_value = int(active_leg_boxes) if isinstance(active_leg_boxes, (int, float)) else None

    if state is None and latest_signal_name == "SELL" and current_kind == "O":
        current_bottom = structure.get("current_column_bottom")
        if isinstance(current_bottom, (int, float)):
            breakdown_level = float(current_bottom)
            return (
                ShadowReversalLongState(
                    breakdown_ts=int(reference_ts),
                    breakdown_level=breakdown_level,
                    lowest_low_after_breakdown=breakdown_level,
                    has_extended=False,
                    has_reclaimed=False,
                    initial_active_leg_boxes=active_leg_boxes_value,
                ),
                candidate_fields,
            )
        return state, candidate_fields

    if state is None or state.has_reclaimed:
        return state, candidate_fields

    state.lowest_low_after_breakdown = min(
        float(state.lowest_low_after_breakdown),
        _current_structure_low(structure, close_price),
    )

    initial_active_leg_boxes = state.initial_active_leg_boxes
    if active_leg_boxes_value is not None:
        extension_threshold = 4
        if initial_active_leg_boxes is not None:
            extension_threshold = max(extension_threshold, initial_active_leg_boxes + 1)
        if active_leg_boxes_value >= extension_threshold:
            state.has_extended = True

    if state.has_extended and _structure_has_reclaimed_breakdown(structure, state.breakdown_level):
        state.has_reclaimed = True
        candidate_fields = _shadow_reversal_long_fields_from_state(state)
        if int(candidate_fields.get("shadow_reversal_long_candidate") or 0) == 1:
            return None, candidate_fields

    return state, candidate_fields


def evaluate_setups(symbol: str, profile: PnFProfile, engine: PnFEngine, structure: Dict[str, Any]) -> List[dict]:
    setup_long = evaluate_pullback_retest_long(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )
    setup_short = evaluate_pullback_retest_short(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )
    return [s for s in (setup_long, setup_short) if s]


def build_funnel_row(
    *,
    symbol: str,
    reference_ts: int,
    setup: Dict[str, Any],
    structure: Dict[str, Any],
    columns: List[Any],
    profile: PnFProfile,
    shadow_reversal_long_fields: Dict[str, Any] | None = None,
    emitted_triple_top_levels: set[float] | None = None,
    emitted_triple_bottom_levels: set[float] | None = None,
) -> Dict[str, Any]:
    shadow_v4_status = setup.get("shadow_v4_status", setup.get("status"))
    shadow_v4_watch_flags = str(setup.get("watch_flags") or "").strip()
    shadow_v4_reject_flags = str(setup.get("reject_flags") or "").strip()
    shadow_v4_flags = setup.get("shadow_v4_flags")
    if shadow_v4_flags is None:
        shadow_v4_flags = "|".join([x for x in (shadow_v4_watch_flags, shadow_v4_reject_flags) if x])
    shadow_v4_registration_eligible = setup.get("shadow_v4_registration_eligible")
    if shadow_v4_registration_eligible is None:
        shadow_v4_registration_eligible = 1 if str(shadow_v4_status or "").upper() in VALIDATION_ELIGIBLE_STATUSES else 0
    row = {
        "symbol": symbol,
        "reference_ts": int(reference_ts),
        "side": setup.get("side"),
        "status": setup.get("status"),
        "strategy": setup.get("strategy"),
        "reason": setup.get("reason"),
        "reject_reason": setup.get("reject_reason"),
        "quality_score": setup.get("quality_score"),
        "quality_grade": setup.get("quality_grade"),
        "trend_state": structure.get("trend_state"),
        "trend_regime": structure.get("trend_regime"),
        "immediate_slope": structure.get("immediate_slope"),
        "breakout_context": structure.get("breakout_context"),
        "market_state": structure.get("market_state"),
        "latest_signal_name": structure.get("latest_signal_name"),
        "is_extended_move": _coerce_bool_int(structure.get("is_extended_move", False)),
        "active_leg_boxes": structure.get("active_leg_boxes"),
        "zone_low": setup.get("zone_low"),
        "zone_high": setup.get("zone_high"),
        "ideal_entry": setup.get("ideal_entry"),
        "invalidation": setup.get("invalidation"),
        "risk": setup.get("risk"),
        "tp1": setup.get("tp1"),
        "tp2": setup.get("tp2"),
        "rr1": setup.get("rr1"),
        "rr2": setup.get("rr2"),
        "decision_version": setup.get("decision_version"),
        "decision_path": setup.get("decision_path"),
        "watch_flags": setup.get("watch_flags"),
        "reject_flags": setup.get("reject_flags"),
        "promotion_checklist_pass_count": setup.get("promotion_checklist_pass_count"),
        "promotion_checklist_failed_items": setup.get("promotion_checklist_failed_items"),
        "entry_to_support_boxes": setup.get("entry_to_support_boxes"),
        "invalidation_distance_boxes": setup.get("invalidation_distance_boxes"),
        "pullback_position_bucket": setup.get("pullback_position_bucket"),
        "breakout_context_rank": setup.get("breakout_context_rank"),
        "extension_risk_score": setup.get("extension_risk_score"),
        "is_baseline_profile_match": setup.get("is_baseline_profile_match"),
        "shadow_v4_version": setup.get("shadow_v4_version", setup.get("decision_version")),
        "shadow_v4_status": shadow_v4_status,
        "shadow_v4_reason": setup.get("shadow_v4_reason", setup.get("reason")),
        "shadow_v4_flags": shadow_v4_flags,
        "shadow_v4_rule_hit": setup.get("shadow_v4_rule_hit", setup.get("decision_path")),
        "shadow_v4_status_delta": setup.get("shadow_v4_status_delta"),
        "shadow_v4_registration_eligible": shadow_v4_registration_eligible,
        "shadow_continuation_candidate": 0,
        "shadow_continuation_trigger": 0,
        "shadow_entry_price": None,
        "shadow_stop_price": None,
        "shadow_continuation_candidate_id": None,
        "shadow_krausz_short_candidate": 0,
        "shadow_krausz_short_entry": None,
        "shadow_krausz_short_stop": None,
        "shadow_krausz_short_tp1": None,
        "shadow_krausz_short_tp2": None,
        "shadow_krausz_bounce_short_candidate": 0,
        "shadow_krausz_bounce_short_entry": None,
        "shadow_krausz_bounce_short_stop": None,
        "shadow_krausz_bounce_short_tp1": None,
        "shadow_krausz_bounce_short_tp2": None,
        "bars_since_breakdown": None,
        "bounce_depth_boxes": None,
        "reclaim_fraction": None,
        "rejection_speed_bars": None,
        "bounce_column_height": None,
        "failed_below_breakdown": None,
        "shadow_reversal_long_candidate": 0,
        "shadow_reversal_long_entry": None,
        "shadow_reversal_long_stop": None,
        "shadow_reversal_long_tp1": None,
        "shadow_reversal_long_tp2": None,
        "shadow_triple_top_breakout": 0,
        "shadow_triple_bottom_breakdown": 0,
        "shadow_double_top_breakout": 0,
        "shadow_double_bottom_breakdown": 0,
        "shadow_7col_bullish_continuation": 0,
        "shadow_7col_bearish_continuation": 0,
        "shadow_5col_bullish_compression_break": 0,
        "shadow_5col_bearish_compression_break": 0,
        "shadow_bullish_catapult": 0,
        "shadow_bearish_catapult": 0,
        "shadow_bullish_triangle": 0,
        "shadow_bearish_triangle": 0,
        "shadow_bullish_signal_reversal": 0,
        "shadow_bearish_signal_reversal": 0,
        "shadow_shakeout": 0,
        "pattern_width_columns": None,
        "pattern_support_level": None,
        "pattern_resistance_level": None,
        "pattern_break_distance_boxes": None,
        "pattern_quality": None,
        "catapult_support_level": None,
        "catapult_origin_width": None,
        "catapult_rebound_columns": None,
        "catapult_total_columns": None,
        "catapult_break_distance_boxes": None,
        "catapult_rebound_failed": None,
        "catapult_pattern_quality": None,
        "catapult_has_prior_triple_signal": 0,
        "catapult_has_reaction_column": 0,
        "catapult_has_followup_double_signal": 0,
        "catapult_is_canonical_candidate": 0,
        "triple_top_resistance_level": None,
        "triple_bottom_support_level": None,
        "triple_pattern_width_columns": None,
        "breakout_distance_boxes": None,
        "breakdown_distance_boxes": None,
        "prior_test_count": None,
        "pattern_compaction_hint": None,
        "breakout_column_height_boxes": None,
        "pattern_is_compact_preferred": None,
        "pattern_is_broad_warning": None,
        "early_trend_candidate_flag": setup.get("early_trend_candidate_flag"),
        "blocked_by_existing_open_trade": 0,
        "blocked_by_watch_cap": 0,
        "registered_to_validation": 0,
    }
    row.update(_compute_shadow_continuation_fields(setup=setup, structure=structure, columns=columns, profile=profile))
    row.update(_compute_shadow_krausz_short_fields(structure=structure, columns=columns))
    row.update(_compute_shadow_krausz_bounce_short_fields(structure=structure, columns=columns, profile=profile))
    row.update(
        _compute_shadow_triple_pattern_fields(
            structure=structure,
            columns=columns,
            profile=profile,
            emitted_triple_top_levels=emitted_triple_top_levels,
            emitted_triple_bottom_levels=emitted_triple_bottom_levels,
        )
    )
    if shadow_reversal_long_fields is not None:
        row.update(shadow_reversal_long_fields)
    return row



def _has_shadow_triple_pattern_flag(row: Dict[str, Any]) -> bool:
    return int(row.get("shadow_triple_top_breakout") or 0) == 1 or int(row.get("shadow_triple_bottom_breakdown") or 0) == 1


def _shadow_triple_pattern_event_key(symbol: str, columns: List[Any], fields: Dict[str, Any]) -> tuple[str, int, str] | None:
    if not _has_shadow_triple_pattern_flag(fields) or not columns:
        return None
    current = columns[-1]
    pattern_name = "TRIPLE_TOP" if int(fields.get("shadow_triple_top_breakout") or 0) == 1 else "TRIPLE_BOTTOM"
    return (symbol, int(getattr(current, "idx", len(columns) - 1)), pattern_name)


def build_structural_event_row(
    *, symbol: str, reference_ts: int, structure: Dict[str, Any], fields: Dict[str, Any]
) -> Dict[str, Any]:
    row = {field: None for field in FUNNEL_FIELD_ORDER}
    reason = _first_shadow_core_pattern_flag(fields)
    if reason is None:
        if int(fields.get("shadow_bearish_catapult") or 0) == 1:
            reason = "shadow_bearish_catapult"
        elif int(fields.get("shadow_triple_top_breakout") or 0) == 1:
            reason = "shadow_triple_top_breakout"
        else:
            reason = "shadow_triple_bottom_breakdown"
    long_event_reasons = {
        "shadow_triple_top_breakout",
        "shadow_double_top_breakout",
        "shadow_7col_bullish_continuation",
        "shadow_5col_bullish_compression_break",
        "shadow_bullish_catapult",
        "shadow_bullish_triangle",
        "shadow_bearish_signal_reversal",
        "shadow_shakeout",
    }
    side = "LONG" if reason in long_event_reasons else "SHORT"
    row.update(
        {
            "symbol": symbol,
            "reference_ts": int(reference_ts),
            "side": side,
            "status": "STRUCTURAL_EVENT",
            "strategy": "PNF_STRUCTURE",
            "reason": reason,
            "trend_state": structure.get("trend_state"),
            "trend_regime": structure.get("trend_regime"),
            "immediate_slope": structure.get("immediate_slope"),
            "breakout_context": structure.get("breakout_context"),
            "market_state": structure.get("market_state"),
            "latest_signal_name": structure.get("latest_signal_name"),
            "is_extended_move": _coerce_bool_int(structure.get("is_extended_move", False)),
            "active_leg_boxes": structure.get("active_leg_boxes"),
            "shadow_continuation_candidate": 0,
            "shadow_continuation_trigger": 0,
            "shadow_krausz_short_candidate": 0,
            "shadow_krausz_bounce_short_candidate": 0,
            "shadow_reversal_long_candidate": 0,
            "blocked_by_existing_open_trade": 0,
            "blocked_by_watch_cap": 0,
            "registered_to_validation": 0,
        }
    )
    row.update(fields)
    return row


def _has_shadow_candidate_flag(row: Dict[str, Any]) -> bool:
    for key, value in row.items():
        if key.startswith("shadow_") and key.endswith("_candidate") and int(value or 0) == 1:
            return True
    return False


def write_funnel_csv(rows: List[Dict[str, Any]], csv_path: str) -> str:
    out_path = Path(csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FUNNEL_FIELD_ORDER, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return str(out_path.resolve())


def process_symbol(symbol: str, profile: PnFProfile, candles: List[dict]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    engine = PnFEngine(profile)
    structure_cache = ScannerStructureCache(symbol, profile)
    rows: List[Dict[str, Any]] = []
    previous_basic_state: tuple[int, str | None, float | None, float | None, str | None] | None = None
    previous_event_state: ShadowEventState | None = None
    shadow_continuation_pending_candidate_id: str | None = None
    shadow_reversal_long_state: ShadowReversalLongState | None = None
    emitted_triple_top_levels: set[float] = set()
    emitted_triple_bottom_levels: set[float] = set()
    active_bearish_catapult_states: Dict[float, Any] = {}
    emitted_bearish_catapult_levels: set[float] = set()
    emitted_core_pattern_keys: set[tuple[str, float | None, int]] = set()
    t_symbol = time.perf_counter()
    counters: Dict[str, Any] = {
        "candles_processed": 0,
        "events_processed": 0,
        "events_skipped": 0,
        "candidates_generated": 0,
        "structural_events_generated": 0,
        "pnf_update_s": 0.0,
        "event_detection_s": 0.0,
        "structure_build_s": 0.0,
        "shadow_eval_s": 0.0,
    }

    for candle in candles:
        close_ts = int(candle["close_time"])
        close_price = float(candle["close"])
        t0 = time.perf_counter()
        engine.update_from_price(close_ts, close_price)
        counters["pnf_update_s"] += time.perf_counter() - t0
        counters["candles_processed"] += 1

        t0 = time.perf_counter()
        basic_state = _basic_event_state_from_engine(engine)
        counters["event_detection_s"] += time.perf_counter() - t0
        if basic_state == previous_basic_state:
            if shadow_reversal_long_state is not None:
                shadow_reversal_long_state.lowest_low_after_breakdown = min(
                    float(shadow_reversal_long_state.lowest_low_after_breakdown),
                    close_price,
                )
            counters["events_skipped"] += 1
            continue
        previous_basic_state = basic_state

        t0 = time.perf_counter()
        snapshot = structure_cache.snapshot(engine.columns, getattr(engine, "last_price", None))
        structure = snapshot.structure
        active_leg_boxes = structure.get("active_leg_boxes")
        event_state = ShadowEventState(
            column_count=len(engine.columns),
            current_column_kind=str(structure.get("current_column_kind")) if structure.get("current_column_kind") is not None else None,
            current_column_top=float(structure["current_column_top"]) if structure.get("current_column_top") is not None else None,
            current_column_bottom=float(structure["current_column_bottom"]) if structure.get("current_column_bottom") is not None else None,
            latest_signal_name=snapshot.latest_signal_name,
            breakout_context=str(structure.get("breakout_context")) if structure.get("breakout_context") is not None else None,
            active_leg_boxes=int(active_leg_boxes) if active_leg_boxes is not None else None,
        )
        counters["structure_build_s"] += time.perf_counter() - t0
        if event_state == previous_event_state:
            counters["events_skipped"] += 1
            continue
        previous_event_state = event_state
        counters["events_processed"] += 1

        t0 = time.perf_counter()
        shadow_reversal_long_state, shadow_reversal_long_fields = update_shadow_reversal_long_state(
            state=shadow_reversal_long_state,
            reference_ts=close_ts,
            close_price=close_price,
            structure=structure,
        )
        triple_pattern_fields = _compute_shadow_triple_pattern_fields(
            structure=structure,
            columns=engine.columns,
            profile=profile,
            emitted_triple_top_levels=emitted_triple_top_levels,
            emitted_triple_bottom_levels=emitted_triple_bottom_levels,
        )
        bearish_catapult_fields = update_shadow_bearish_catapult_states(
            active_states=active_bearish_catapult_states,
            emitted_levels=emitted_bearish_catapult_levels,
            triple_pattern_fields=triple_pattern_fields,
            structure=structure,
            columns=engine.columns,
            profile=profile,
        )
        core_pattern_field_sets = _compute_shadow_core_pattern_field_sets(
            structure=structure,
            columns=engine.columns,
            profile=profile,
            emitted_core_pattern_keys=emitted_core_pattern_keys,
        )
        for core_pattern_fields in core_pattern_field_sets:
            rows.append(
                build_structural_event_row(
                    symbol=symbol,
                    reference_ts=close_ts,
                    structure=structure,
                    fields=core_pattern_fields,
                )
            )
            counters["structural_events_generated"] += 1
        if _has_shadow_triple_pattern_flag(triple_pattern_fields):
            rows.append(
                build_structural_event_row(
                    symbol=symbol,
                    reference_ts=close_ts,
                    structure=structure,
                    fields=triple_pattern_fields,
                )
            )
            if int(triple_pattern_fields.get("shadow_triple_top_breakout") or 0) == 1:
                emitted_triple_top_levels.add(float(triple_pattern_fields["triple_top_resistance_level"]))
            if int(triple_pattern_fields.get("shadow_triple_bottom_breakdown") or 0) == 1:
                emitted_triple_bottom_levels.add(float(triple_pattern_fields["triple_bottom_support_level"]))
            counters["structural_events_generated"] += 1
        if int(bearish_catapult_fields.get("shadow_bearish_catapult") or 0) == 1:
            rows.append(
                build_structural_event_row(
                    symbol=symbol,
                    reference_ts=close_ts,
                    structure=structure,
                    fields=bearish_catapult_fields,
                )
            )
            counters["structural_events_generated"] += 1
        setups = evaluate_setups(symbol, profile, engine, structure)
        for setup in setups:
            funnel_row = build_funnel_row(
                symbol=symbol,
                reference_ts=close_ts,
                setup=setup,
                structure=structure,
                columns=engine.columns,
                profile=profile,
                shadow_reversal_long_fields=shadow_reversal_long_fields,
                emitted_triple_top_levels=emitted_triple_top_levels,
                emitted_triple_bottom_levels=emitted_triple_bottom_levels,
            )
            if str(setup.get("side") or "").upper() == "LONG" and str(structure.get("breakout_context") or "").upper() == "LATE_EXTENSION":
                current_col = engine.columns[-1] if engine.columns else None
                current_col_kind = str(getattr(current_col, "kind", "")).upper() if current_col is not None else ""
                current_col_idx = getattr(current_col, "idx", None) if current_col is not None else None
                if int(funnel_row.get("shadow_continuation_candidate") or 0) == 1 and current_col_kind == "O" and current_col_idx is not None:
                    shadow_continuation_pending_candidate_id = f"{symbol}:{int(current_col_idx)}"
                    funnel_row["shadow_continuation_candidate_id"] = shadow_continuation_pending_candidate_id
                if int(funnel_row.get("shadow_continuation_trigger") or 0) == 1:
                    prev_o = engine.columns[-2] if len(engine.columns) >= 2 else None
                    prev_o_idx = getattr(prev_o, "idx", None) if prev_o is not None else None
                    expected_candidate_id = f"{symbol}:{int(prev_o_idx)}" if prev_o_idx is not None else None
                    if shadow_continuation_pending_candidate_id is None or expected_candidate_id != shadow_continuation_pending_candidate_id:
                        funnel_row["shadow_continuation_trigger"] = 0
                        funnel_row["shadow_entry_price"] = None
                        funnel_row["shadow_stop_price"] = None
                    else:
                        funnel_row["shadow_continuation_candidate_id"] = shadow_continuation_pending_candidate_id
                        shadow_continuation_pending_candidate_id = None
            if _has_shadow_candidate_flag(funnel_row):
                rows.append(funnel_row)
                counters["candidates_generated"] += 1
        counters["shadow_eval_s"] += time.perf_counter() - t0

    counters["elapsed_s"] = time.perf_counter() - t_symbol
    candles_processed = int(counters["candles_processed"])
    counters["event_ratio"] = (float(counters["events_processed"]) / float(candles_processed)) if candles_processed else 0.0
    return rows, counters


def main() -> None:
    parser = argparse.ArgumentParser(description="Candidate-only shadow research scanner with event-driven PnF execution")
    parser.add_argument("--settings", default="pnf_mvp/settings.research_clean.json")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--funnel-csv", default=DEFAULT_FUNNEL_CSV_PATH)
    parser.add_argument("--sample-limit", type=int, default=None)
    args = parser.parse_args()

    settings = load_settings(args.settings)
    database_path = resolve_settings_relative_path(args.settings, settings["database_path"])
    profiles = build_profiles(settings)
    symbols = split_symbols(settings, args.symbols)

    all_rows: List[Dict[str, Any]] = []
    totals = {
        "candles_processed": 0,
        "events_processed": 0,
        "events_skipped": 0,
        "candidates_generated": 0,
        "structural_events_generated": 0,
        "elapsed_s": 0.0,
        "candle_load_s": 0.0,
        "pnf_update_s": 0.0,
        "event_detection_s": 0.0,
        "structure_build_s": 0.0,
        "shadow_eval_s": 0.0,
        "output_write_s": 0.0,
    }
    for symbol in symbols:
        t_load = time.perf_counter()
        candles = load_all_closed_candles(database_path, symbol, args.sample_limit)
        candle_load_s = time.perf_counter() - t_load
        rows, counters = process_symbol(symbol, profiles[symbol], candles)
        counters["candle_load_s"] = candle_load_s
        counters["output_write_s"] = 0.0
        all_rows.extend(rows)
        for key in totals:
            totals[key] += counters[key]
        print(
            f"[EVENT] symbol={symbol} elapsed_s={counters['elapsed_s']:.3f} "
            f"candle_load_s={counters['candle_load_s']:.3f} "
            f"pnf_update_s={counters['pnf_update_s']:.3f} "
            f"event_detection_s={counters['event_detection_s']:.3f} "
            f"structure_build_s={counters['structure_build_s']:.3f} "
            f"shadow_eval_s={counters['shadow_eval_s']:.3f} "
            f"output_write_s={counters['output_write_s']:.3f} "
            f"candles_processed={counters['candles_processed']} "
            f"events_processed={counters['events_processed']} "
            f"events_skipped={counters['events_skipped']} "
            f"candidates_generated={counters['candidates_generated']} "
            f"structural_events_generated={counters['structural_events_generated']} "
            f"event_ratio={counters['event_ratio']:.6f}"
        )

    t_write = time.perf_counter()
    output_path = write_funnel_csv(all_rows, args.funnel_csv)
    totals["output_write_s"] = time.perf_counter() - t_write
    total_candles = int(totals["candles_processed"])
    event_ratio = (float(totals["events_processed"]) / float(total_candles)) if total_candles else 0.0
    print(
        f"[EVENT_TOTAL] elapsed_s={totals['elapsed_s']:.3f} "
        f"candle_load_s={totals['candle_load_s']:.3f} "
        f"pnf_update_s={totals['pnf_update_s']:.3f} "
        f"event_detection_s={totals['event_detection_s']:.3f} "
        f"structure_build_s={totals['structure_build_s']:.3f} "
        f"shadow_eval_s={totals['shadow_eval_s']:.3f} "
        f"output_write_s={totals['output_write_s']:.3f} "
        f"candles_processed={totals['candles_processed']} "
        f"events_processed={totals['events_processed']} "
        f"events_skipped={totals['events_skipped']} "
        f"candidates_generated={totals['candidates_generated']} "
        f"structural_events_generated={totals['structural_events_generated']} "
        f"event_ratio={event_ratio:.6f}"
    )
    print(f"Wrote {len(all_rows)} shadow research rows to {output_path}")


if __name__ == "__main__":
    main()
