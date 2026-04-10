"""
structure_engine.py

PnF Structure Engine v3
=======================

Purpose
-------
Structural truth layer between:
- raw PnF generation
- future strategy / trade-plan logic

v3 goals
--------
- Keep v2 improvements for immediate_slope
- Improve trend classification so obvious bullish/bearish regimes
  are not misclassified as RANGE
- Add trend_regime as a softer / more practical trend field

Important
---------
This module does NOT create entries, stops, targets, alerts, or drawings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

TREND_BULLISH = "BULLISH"
TREND_BEARISH = "BEARISH"
TREND_RANGE = "RANGE"
TREND_EARLY = "EARLY"

REGIME_BULLISH = "BULLISH_REGIME"
REGIME_BEARISH = "BEARISH_REGIME"
REGIME_RANGE = "RANGE_REGIME"
REGIME_EARLY = "EARLY_REGIME"

SLOPE_BULLISH_PUSH = "BULLISH_PUSH"
SLOPE_BEARISH_PULLBACK = "BEARISH_PULLBACK"
SLOPE_BEARISH_PUSH = "BEARISH_PUSH"
SLOPE_BULLISH_REBOUND = "BULLISH_REBOUND"
SLOPE_FLAT = "FLAT"

SWING_UP = "UP"
SWING_DOWN = "DOWN"
SWING_NEUTRAL = "NEUTRAL"

BREAKOUT_NONE = "NONE"
BREAKOUT_FRESH_BULLISH = "FRESH_BULLISH_BREAKOUT"
BREAKOUT_FRESH_BEARISH = "FRESH_BEARISH_BREAKDOWN"
BREAKOUT_POST_BULLISH_PULLBACK = "POST_BREAKOUT_PULLBACK"
BREAKOUT_POST_BEARISH_REBOUND = "POST_BREAKDOWN_REBOUND"
BREAKOUT_LATE_EXTENSION = "LATE_EXTENSION"


@dataclass(frozen=True)
class StructureConfig:
    early_min_columns: int = 4
    extension_boxes_threshold: int = 4
    recent_columns_for_bias: int = 5
    regime_bias_threshold: int = 1


def _col_kind(col: Any) -> str:
    return getattr(col, "kind", "")


def _col_top(col: Any) -> float:
    return float(getattr(col, "top", 0.0))


def _col_bottom(col: Any) -> float:
    return float(getattr(col, "bottom", 0.0))


def _last_of_kind(columns: List[Any], kind: str, before_index: Optional[int] = None) -> Optional[Any]:
    end = len(columns) if before_index is None else max(0, before_index)
    for i in range(end - 1, -1, -1):
        if _col_kind(columns[i]) == kind:
            return columns[i]
    return None


def _meaningful_x_columns(columns: List[Any]) -> List[Any]:
    return [c for c in columns[:-1] if _col_kind(c) == "X"]


def _meaningful_o_columns(columns: List[Any]) -> List[Any]:
    return [c for c in columns[:-1] if _col_kind(c) == "O"]


def _meaningful_x_highs(columns: List[Any]) -> List[float]:
    return [_col_top(c) for c in _meaningful_x_columns(columns)]


def _meaningful_o_lows(columns: List[Any]) -> List[float]:
    return [_col_bottom(c) for c in _meaningful_o_columns(columns)]


def _last_meaningful_x_high(columns: List[Any]) -> Optional[float]:
    highs = _meaningful_x_highs(columns)
    return highs[-1] if highs else None


def _last_meaningful_o_low(columns: List[Any]) -> Optional[float]:
    lows = _meaningful_o_lows(columns)
    return lows[-1] if lows else None


def _last_two_meaningful_x_highs(columns: List[Any]) -> List[float]:
    return _meaningful_x_highs(columns)[-2:]


def _last_two_meaningful_o_lows(columns: List[Any]) -> List[float]:
    return _meaningful_o_lows(columns)[-2:]


def _active_leg_boxes(columns: List[Any], box_size: float) -> int:
    if not columns or box_size <= 0:
        return 0
    current = columns[-1]
    span = abs(_col_top(current) - _col_bottom(current))
    return int(round(span / box_size))


def _column_span_boxes(col: Optional[Any], box_size: float) -> Optional[float]:
    if col is None or box_size <= 0:
        return None
    span = abs(_col_top(col) - _col_bottom(col))
    return span / box_size


def _safe_ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None:
        return None
    if denominator <= 0:
        return None
    return float(numerator) / float(denominator)


def _detect_swing_direction(columns: List[Any]) -> str:
    x_highs = _last_two_meaningful_x_highs(columns)
    o_lows = _last_two_meaningful_o_lows(columns)

    up = len(x_highs) >= 2 and x_highs[-1] > x_highs[-2]
    down = len(o_lows) >= 2 and o_lows[-1] < o_lows[-2]

    if up and not down:
        return SWING_UP
    if down and not up:
        return SWING_DOWN

    if len(columns) >= 2:
        last_completed = columns[-2]
        if _col_kind(last_completed) == "X":
            return SWING_UP
        if _col_kind(last_completed) == "O":
            return SWING_DOWN

    return SWING_NEUTRAL


def _recent_direction_bias(columns: List[Any], window: int) -> int:
    """
    Positive => bullish bias
    Negative => bearish bias
    Uses the last completed columns only.
    """
    recent = columns[:-1][-window:]
    score = 0
    for col in recent:
        if _col_kind(col) == "X":
            score += 1
        elif _col_kind(col) == "O":
            score -= 1
    return score


def _detect_trend_regime(
    columns: List[Any],
    market_state: str,
    swing_direction: str,
    config: StructureConfig,
) -> str:
    if len(columns) < config.early_min_columns:
        return REGIME_EARLY

    x_highs = _last_two_meaningful_x_highs(columns)
    o_lows = _last_two_meaningful_o_lows(columns)

    bullish_structure = len(x_highs) >= 2 and x_highs[-1] > x_highs[-2]
    bearish_structure = len(o_lows) >= 2 and o_lows[-1] < o_lows[-2]

    ms = (market_state or "").upper()
    bias = _recent_direction_bias(columns, config.recent_columns_for_bias)

    if "BULLISH" in ms:
        return REGIME_BULLISH
    if "BEARISH" in ms:
        return REGIME_BEARISH

    if bullish_structure and swing_direction == SWING_UP:
        return REGIME_BULLISH
    if bearish_structure and swing_direction == SWING_DOWN:
        return REGIME_BEARISH

    if bias >= config.regime_bias_threshold and swing_direction == SWING_UP:
        return REGIME_BULLISH
    if bias <= -config.regime_bias_threshold and swing_direction == SWING_DOWN:
        return REGIME_BEARISH

    # fallback: compare current location to last meaningful S/R
    last_x = _last_meaningful_x_high(columns)
    last_o = _last_meaningful_o_low(columns)
    current = columns[-1]
    if last_x is not None and last_o is not None:
        if _col_top(current) >= last_x and _col_bottom(current) > last_o:
            return REGIME_BULLISH
        if _col_bottom(current) <= last_o and _col_top(current) < last_x:
            return REGIME_BEARISH

    return REGIME_RANGE


def _detect_trend_state(
    columns: List[Any],
    market_state: str,
    swing_direction: str,
    trend_regime: str,
    config: StructureConfig,
) -> str:
    if len(columns) < config.early_min_columns:
        return TREND_EARLY

    x_highs = _last_two_meaningful_x_highs(columns)
    o_lows = _last_two_meaningful_o_lows(columns)

    bullish_structure = len(x_highs) >= 2 and len(o_lows) >= 2 and x_highs[-1] > x_highs[-2] and o_lows[-1] > o_lows[-2]
    bearish_structure = len(x_highs) >= 2 and len(o_lows) >= 2 and x_highs[-1] < x_highs[-2] and o_lows[-1] < o_lows[-2]

    if bullish_structure:
        return TREND_BULLISH
    if bearish_structure:
        return TREND_BEARISH

    # softer fallback: regime + swing alignment
    if trend_regime == REGIME_BULLISH and swing_direction == SWING_UP:
        return TREND_BULLISH
    if trend_regime == REGIME_BEARISH and swing_direction == SWING_DOWN:
        return TREND_BEARISH

    ms = (market_state or "").upper()
    if "BULLISH" in ms:
        return TREND_BULLISH
    if "BEARISH" in ms:
        return TREND_BEARISH

    return TREND_RANGE


def _detect_immediate_slope(columns: List[Any], trend_regime: str, trend_state: str) -> str:
    if not columns:
        return SLOPE_FLAT

    current_kind = _col_kind(columns[-1])

    if current_kind == "X":
        if trend_state == TREND_BULLISH or trend_regime == REGIME_BULLISH:
            return SLOPE_BULLISH_PUSH
        if trend_state == TREND_BEARISH or trend_regime == REGIME_BEARISH:
            return SLOPE_BULLISH_REBOUND
        return SLOPE_BULLISH_REBOUND

    if current_kind == "O":
        if trend_state == TREND_BULLISH or trend_regime == REGIME_BULLISH:
            return SLOPE_BEARISH_PULLBACK
        if trend_state == TREND_BEARISH or trend_regime == REGIME_BEARISH:
            return SLOPE_BEARISH_PUSH
        return SLOPE_BEARISH_PULLBACK

    return SLOPE_FLAT


def _detect_breakout_context(
    columns: List[Any],
    trend_regime: str,
    immediate_slope: str,
    box_size: float,
    config: StructureConfig,
) -> str:
    if len(columns) < 3:
        return BREAKOUT_NONE

    active_boxes = _active_leg_boxes(columns, box_size)
    if active_boxes >= config.extension_boxes_threshold:
        return BREAKOUT_LATE_EXTENSION

    current = columns[-1]
    prev_x = _last_of_kind(columns, "X", before_index=len(columns) - 1)
    prev_o = _last_of_kind(columns, "O", before_index=len(columns) - 1)

    if trend_regime == REGIME_BULLISH:
        if _col_kind(current) == "X" and prev_x is not None and _col_top(current) > _col_top(prev_x):
            return BREAKOUT_FRESH_BULLISH
        if _col_kind(current) == "O":
            return BREAKOUT_POST_BULLISH_PULLBACK

    if trend_regime == REGIME_BEARISH:
        if _col_kind(current) == "O" and prev_o is not None and _col_bottom(current) < _col_bottom(prev_o):
            return BREAKOUT_FRESH_BEARISH
        if _col_kind(current) == "X":
            return BREAKOUT_POST_BEARISH_REBOUND

    return BREAKOUT_NONE


def _build_notes(
    trend_state: str,
    trend_regime: str,
    immediate_slope: str,
    breakout_context: str,
    support_level: Optional[float],
    resistance_level: Optional[float],
    is_extended_move: bool,
) -> List[str]:
    notes: List[str] = []
    notes.append(f"Trend: {trend_state}")
    notes.append(f"Trend regime: {trend_regime}")
    notes.append(f"Immediate slope: {immediate_slope}")
    if support_level is not None:
        notes.append(f"Support identified at {support_level}")
    if resistance_level is not None:
        notes.append(f"Resistance identified at {resistance_level}")
    if breakout_context != BREAKOUT_NONE:
        notes.append(f"Breakout context: {breakout_context}")
    if is_extended_move:
        notes.append("Move classified as extended")
    return notes


def build_structure_state(
    symbol: str,
    profile: Any,
    columns: List[Any],
    latest_signal_name: Optional[str],
    market_state: str,
    last_price: Optional[float],
    config: Optional[StructureConfig] = None,
) -> Dict[str, Any]:
    cfg = config or StructureConfig()
    box_size = float(getattr(profile, "box_size", 0.0) or 0.0)

    if not columns:
        return {
            "symbol": symbol,
            "trend_state": TREND_EARLY,
            "trend_regime": REGIME_EARLY,
            "immediate_slope": SLOPE_FLAT,
            "swing_direction": SWING_NEUTRAL,
            "support_level": None,
            "resistance_level": None,
            "breakout_context": BREAKOUT_NONE,
            "is_extended_move": False,
            "active_leg_boxes": 0,
            "impulse_boxes": None,
            "pullback_boxes": None,
            "impulse_to_pullback_ratio": None,
            "last_meaningful_x_high": None,
            "last_meaningful_o_low": None,
            "current_column_kind": None,
            "current_column_top": None,
            "current_column_bottom": None,
            "latest_signal_name": latest_signal_name,
            "market_state": market_state,
            "last_price": last_price,
            "notes": ["No columns available"],
        }

    swing_direction = _detect_swing_direction(columns)
    trend_regime = _detect_trend_regime(columns, market_state, swing_direction, cfg)
    trend_state = _detect_trend_state(columns, market_state, swing_direction, trend_regime, cfg)
    immediate_slope = _detect_immediate_slope(columns, trend_regime, trend_state)
    support_level = _last_meaningful_o_low(columns)
    resistance_level = _last_meaningful_x_high(columns)
    active_leg_boxes = _active_leg_boxes(columns, box_size)
    is_extended_move = active_leg_boxes >= cfg.extension_boxes_threshold
    breakout_context = _detect_breakout_context(
        columns=columns,
        trend_regime=trend_regime,
        immediate_slope=immediate_slope,
        box_size=box_size,
        config=cfg,
    )

    current = columns[-1]
    prev_x = _last_of_kind(columns, "X", before_index=len(columns) - 1)

    impulse_boxes = None
    pullback_boxes = None
    impulse_to_pullback_ratio = None

    if (
        breakout_context == BREAKOUT_POST_BULLISH_PULLBACK
        and _col_kind(current) == "O"
        and immediate_slope == SLOPE_BEARISH_PULLBACK
    ):
        impulse_boxes = _column_span_boxes(prev_x, box_size)
        pullback_boxes = _column_span_boxes(current, box_size)
        impulse_to_pullback_ratio = _safe_ratio(impulse_boxes, pullback_boxes)

    return {
        "symbol": symbol,
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
        "last_meaningful_x_high": _last_meaningful_x_high(columns),
        "last_meaningful_o_low": _last_meaningful_o_low(columns),
        "current_column_kind": _col_kind(current),
        "current_column_top": _col_top(current),
        "current_column_bottom": _col_bottom(current),
        "latest_signal_name": latest_signal_name,
        "market_state": market_state,
        "last_price": last_price,
        "notes": _build_notes(
            trend_state=trend_state,
            trend_regime=trend_regime,
            immediate_slope=immediate_slope,
            breakout_context=breakout_context,
            support_level=support_level,
            resistance_level=resistance_level,
            is_extended_move=is_extended_move,
        ),
    }


def build_structure_state_debug_text(state: Dict[str, Any]) -> str:
    lines = [
        f"symbol={state.get('symbol')}",
        f"trend_state={state.get('trend_state')}",
        f"trend_regime={state.get('trend_regime')}",
        f"immediate_slope={state.get('immediate_slope')}",
        f"swing_direction={state.get('swing_direction')}",
        f"support_level={state.get('support_level')}",
        f"resistance_level={state.get('resistance_level')}",
        f"breakout_context={state.get('breakout_context')}",
        f"is_extended_move={state.get('is_extended_move')}",
        f"active_leg_boxes={state.get('active_leg_boxes')}",
        f"current_column_kind={state.get('current_column_kind')}",
        f"current_column_top={state.get('current_column_top')}",
        f"current_column_bottom={state.get('current_column_bottom')}",
    ]
    notes = state.get("notes", [])
    if notes:
        lines.append("notes=" + " | ".join(str(x) for x in notes))
    return "\\n".join(lines)
