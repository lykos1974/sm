"""
strategy_engine.py

Long profitable baseline + experimental short reversal branch
=============================================================

Important
---------
- Long side keeps the currently profitable baseline behavior.
- Short side is NOT a continuation mirror.
- Short side is an experimental FAILED BREAKOUT / REVERSAL branch.
- Compatible with:
    evaluate_pullback_retest_long(...)
    evaluate_pullback_retest_short(...)

Philosophy
----------
LONG:
- keep the profitable filtered baseline
- only LONG healthy, 2-leg, non-extended, post-breakout pullback promotes

SHORT:
- separate logic
- only promote bearish reversal after a bullish failure / bull-trap style structure
- extended structure is ALLOWED / desired for shorts here
- this is intentionally research mode
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


STATUS_REJECT = "REJECT"
STATUS_WATCH = "WATCH"
STATUS_CANDIDATE = "CANDIDATE"

PULLBACK_SHALLOW = "SHALLOW"
PULLBACK_HEALTHY = "HEALTHY"
PULLBACK_DEEP = "DEEP"
PULLBACK_BROKEN = "BROKEN"

RISK_TIGHT = "TIGHT"
RISK_NORMAL = "NORMAL"
RISK_WIDE = "WIDE"

REWARD_POOR = "POOR"
REWARD_OK = "OK"
REWARD_GOOD = "GOOD"
REWARD_STRONG = "STRONG"

BREAKOUT_NONE = "NONE"
BREAKOUT_FRESH_BULLISH = "FRESH_BULLISH_BREAKOUT"
BREAKOUT_FRESH_BEARISH = "FRESH_BEARISH_BREAKDOWN"
BREAKOUT_POST_BULLISH_PULLBACK = "POST_BREAKOUT_PULLBACK"
BREAKOUT_POST_BEARISH_REBOUND = "POST_BREAKDOWN_REBOUND"
BREAKOUT_LATE_EXTENSION = "LATE_EXTENSION"
DECISION_VERSION = "v4_diag"
CONTINUATION_EXECUTION_V1_VERSION = "continuation_execution_v1_research"

ENTRY_DISTANCE_BELOW_BREAKOUT = "BELOW_BREAKOUT"
ENTRY_DISTANCE_IMMEDIATE = "IMMEDIATE"
ENTRY_DISTANCE_EARLY = "EARLY"
ENTRY_DISTANCE_LATE = "LATE"
ENTRY_DISTANCE_EXTENDED = "EXTENDED"
ENTRY_DISTANCE_UNKNOWN = "UNKNOWN"

CONT_EXEC_BASELINE_DIAGNOSTIC = "BASELINE_DIAGNOSTIC"
CONT_EXEC_CONTINUATION_PRIMARY = "CONTINUATION_PRIMARY"
CONT_EXEC_CONTINUATION_ACCEPTABLE = "CONTINUATION_ACCEPTABLE"
CONT_EXEC_CONTINUATION_LATE_WATCH = "CONTINUATION_LATE_WATCH"
CONT_EXEC_CONTINUATION_EXTENDED_REJECT = "CONTINUATION_EXTENDED_REJECT"
CONT_EXEC_PULLBACK_CONFIRMED = "PULLBACK_CONFIRMED"
CONT_EXEC_PULLBACK_WATCH = "PULLBACK_WATCH"
CONT_EXEC_REJECT = "REJECT"


def _box_size(profile: Any) -> float:
    return float(getattr(profile, "box_size", 0.0) or 0.0)


def _current_column(columns: List[Any]) -> Optional[Any]:
    return columns[-1] if columns else None


def _col_top(col: Any) -> float:
    return float(getattr(col, "top", 0.0))


def _col_bottom(col: Any) -> float:
    return float(getattr(col, "bottom", 0.0))


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None



def _is_continuation_execution_v1_enabled(profile: Any, structure_state: Dict[str, Any]) -> bool:
    return bool(
        getattr(profile, "continuation_execution_v1_enabled", False)
        or structure_state.get("continuation_execution_v1_enabled", False)
        or structure_state.get("execution_model") == CONTINUATION_EXECUTION_V1_VERSION
    )


def _clamp_score(value: float) -> float:
    return float(max(0.0, min(100.0, value)))


def _classify_entry_distance_bucket(entry_distance_boxes: Optional[float]) -> str:
    if entry_distance_boxes is None:
        return ENTRY_DISTANCE_UNKNOWN
    if entry_distance_boxes < 0.0:
        return ENTRY_DISTANCE_BELOW_BREAKOUT
    if entry_distance_boxes <= 1.0:
        return ENTRY_DISTANCE_IMMEDIATE
    if entry_distance_boxes <= 2.0:
        return ENTRY_DISTANCE_EARLY
    if entry_distance_boxes <= 3.0:
        return ENTRY_DISTANCE_LATE
    return ENTRY_DISTANCE_EXTENDED


def _normalize_pattern_family(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    normalized = text.replace("-", "_").replace(" ", "_")
    if "triangle" in normalized:
        if "bear" in normalized or "sell" in normalized or "down" in normalized:
            return "bearish_triangle"
        if "bull" in normalized or "buy" in normalized or "up" in normalized:
            return "bullish_triangle"
        return "triangle"
    if "double_top" in normalized or normalized in {"buy", "double_top_breakout"}:
        return "double_top_breakout"
    if "double_bottom" in normalized or normalized in {"sell", "double_bottom_breakdown"}:
        return "double_bottom_breakdown"
    return normalized


def _extract_pattern_family(structure_state: Dict[str, Any]) -> Optional[str]:
    for key in (
        "pattern_family",
        "pattern_name",
        "latest_pattern_name",
        "latest_signal_type",
        "latest_signal_name",
    ):
        family = _normalize_pattern_family(structure_state.get(key))
        if family:
            return family
    return None


def _is_aligned_triangle(pattern_family: Optional[str], side: str) -> bool:
    if side == "LONG":
        return pattern_family == "bullish_triangle"
    if side == "SHORT":
        return pattern_family == "bearish_triangle"
    return False


def _aligned_trend_regime(side: str, trend_regime: Optional[str]) -> bool:
    if side == "LONG":
        return trend_regime == "BULLISH_REGIME"
    if side == "SHORT":
        return trend_regime == "BEARISH_REGIME"
    return False


def _opposing_trend_regime(side: str, trend_regime: Optional[str]) -> bool:
    if side == "LONG":
        return trend_regime == "BEARISH_REGIME"
    if side == "SHORT":
        return trend_regime == "BULLISH_REGIME"
    return False


def _compute_extension_penalty(
    *,
    entry_distance_bucket: str,
    breakout_context: Optional[str],
    is_extended: bool,
    active_leg_boxes: int,
) -> float:
    penalty = 0.0
    if entry_distance_bucket == ENTRY_DISTANCE_LATE:
        penalty += 35.0
    elif entry_distance_bucket == ENTRY_DISTANCE_EXTENDED:
        penalty += 70.0
    if is_extended:
        penalty += 25.0
    if breakout_context == BREAKOUT_LATE_EXTENSION:
        penalty += 35.0
    if active_leg_boxes >= 4:
        penalty += 20.0
    elif active_leg_boxes == 3:
        penalty += 10.0
    return _clamp_score(penalty)


def _distance_policy_action(entry_distance_bucket: str, pattern_family: Optional[str], side: str) -> str:
    if entry_distance_bucket == ENTRY_DISTANCE_IMMEDIATE:
        return "ALLOW_PRIMARY"
    if entry_distance_bucket == ENTRY_DISTANCE_EARLY:
        return "ALLOW_ACCEPTABLE"
    if entry_distance_bucket == ENTRY_DISTANCE_LATE:
        if _is_aligned_triangle(pattern_family, side):
            return "DOWNGRADE_WATCH_TRIANGLE_EXCEPTION"
        return "DOWNGRADE_WATCH"
    if entry_distance_bucket == ENTRY_DISTANCE_EXTENDED:
        return "REJECT_EXTENDED"
    if entry_distance_bucket == ENTRY_DISTANCE_BELOW_BREAKOUT:
        return "PULLBACK_CONTEXT"
    return "INSUFFICIENT_DISTANCE_DATA"


def _continuation_execution_class(
    *,
    entry_distance_bucket: str,
    continuation_quality_score: float,
    extension_penalty: float,
    breakout_context: Optional[str],
    pullback_quality: Optional[str],
) -> str:
    if entry_distance_bucket == ENTRY_DISTANCE_EXTENDED or extension_penalty >= 80.0:
        return CONT_EXEC_CONTINUATION_EXTENDED_REJECT
    if entry_distance_bucket == ENTRY_DISTANCE_LATE:
        return CONT_EXEC_CONTINUATION_LATE_WATCH
    if entry_distance_bucket == ENTRY_DISTANCE_IMMEDIATE and continuation_quality_score >= 70.0:
        return CONT_EXEC_CONTINUATION_PRIMARY
    if entry_distance_bucket == ENTRY_DISTANCE_EARLY and continuation_quality_score >= 60.0:
        return CONT_EXEC_CONTINUATION_ACCEPTABLE
    if breakout_context in (BREAKOUT_POST_BULLISH_PULLBACK, BREAKOUT_POST_BEARISH_REBOUND):
        if pullback_quality == PULLBACK_HEALTHY and continuation_quality_score >= 68.0 and extension_penalty < 35.0:
            return CONT_EXEC_PULLBACK_CONFIRMED
        return CONT_EXEC_PULLBACK_WATCH
    if continuation_quality_score < 35.0:
        return CONT_EXEC_REJECT
    return CONT_EXEC_BASELINE_DIAGNOSTIC


def _compute_entry_distance_boxes(
    *,
    side: str,
    entry_price: Optional[float],
    breakout_level: Optional[float],
    box_size: float,
) -> Optional[float]:
    if entry_price is None or breakout_level is None or box_size <= 0:
        return None
    if side == "LONG":
        return (float(entry_price) - float(breakout_level)) / float(box_size)
    if side == "SHORT":
        return (float(breakout_level) - float(entry_price)) / float(box_size)
    return None


def _compute_continuation_quality_score(
    *,
    side: str,
    entry_distance_bucket: str,
    breakout_context: Optional[str],
    trend_regime: Optional[str],
    is_extended: bool,
    active_leg_boxes: int,
    pattern_family: Optional[str],
    pullback_quality: Optional[str],
    risk_quality: Optional[str],
    reward_quality: Optional[str],
) -> float:
    score = 50.0

    if entry_distance_bucket == ENTRY_DISTANCE_IMMEDIATE:
        score += 18.0
    elif entry_distance_bucket == ENTRY_DISTANCE_EARLY:
        score += 8.0
    elif entry_distance_bucket == ENTRY_DISTANCE_LATE:
        score -= 22.0
    elif entry_distance_bucket == ENTRY_DISTANCE_EXTENDED:
        score -= 45.0
    elif entry_distance_bucket == ENTRY_DISTANCE_BELOW_BREAKOUT:
        score += 0.0 if breakout_context in (BREAKOUT_POST_BULLISH_PULLBACK, BREAKOUT_POST_BEARISH_REBOUND) else -10.0
    else:
        score -= 15.0

    if breakout_context in (BREAKOUT_FRESH_BULLISH, BREAKOUT_FRESH_BEARISH):
        score += 14.0
    elif breakout_context in (BREAKOUT_POST_BULLISH_PULLBACK, BREAKOUT_POST_BEARISH_REBOUND):
        score += 4.0
    elif breakout_context == BREAKOUT_LATE_EXTENSION:
        score -= 30.0
    elif breakout_context == BREAKOUT_NONE:
        score -= 10.0

    if _aligned_trend_regime(side, trend_regime):
        score += 10.0
    elif _opposing_trend_regime(side, trend_regime):
        score -= 15.0
    elif trend_regime in ("RANGE_REGIME", "EARLY_REGIME"):
        score -= 5.0

    if is_extended:
        score -= 25.0
    if breakout_context == BREAKOUT_LATE_EXTENSION:
        score -= 30.0
    if active_leg_boxes >= 4:
        score -= 20.0
    elif active_leg_boxes == 3:
        score -= 8.0
    elif active_leg_boxes <= 2:
        score += 6.0

    if _is_aligned_triangle(pattern_family, side):
        score += 8.0
    elif pattern_family is None:
        score -= 2.0

    if pullback_quality == PULLBACK_HEALTHY:
        score += 8.0
    elif pullback_quality == PULLBACK_SHALLOW:
        score -= 4.0
    elif pullback_quality == PULLBACK_DEEP:
        score -= 12.0
    elif pullback_quality == PULLBACK_BROKEN:
        score -= 40.0

    if risk_quality == RISK_TIGHT:
        score += 8.0
    elif risk_quality == RISK_NORMAL:
        score += 3.0
    elif risk_quality == RISK_WIDE:
        score -= 15.0

    if reward_quality == REWARD_STRONG:
        score += 8.0
    elif reward_quality == REWARD_GOOD:
        score += 5.0
    elif reward_quality == REWARD_POOR:
        score -= 30.0

    return _clamp_score(score)


def _compute_continuation_diagnostics(
    *,
    side: str,
    profile: Any,
    structure_state: Dict[str, Any],
    entry_price: Optional[float],
    breakout_level: Optional[float],
    pullback_quality: Optional[str] = None,
    risk_quality: Optional[str] = None,
    reward_quality: Optional[str] = None,
) -> Dict[str, Any]:
    box = _box_size(profile)
    breakout_context = structure_state.get("breakout_context")
    trend_regime = structure_state.get("trend_regime")
    is_extended = bool(structure_state.get("is_extended_move", False))
    active_leg_boxes = int(structure_state.get("active_leg_boxes") or 0)
    pattern_family = _extract_pattern_family(structure_state)
    entry_distance_boxes = _compute_entry_distance_boxes(
        side=side,
        entry_price=entry_price,
        breakout_level=breakout_level,
        box_size=box,
    )
    entry_distance_bucket = _classify_entry_distance_bucket(entry_distance_boxes)
    extension_penalty = _compute_extension_penalty(
        entry_distance_bucket=entry_distance_bucket,
        breakout_context=breakout_context,
        is_extended=is_extended,
        active_leg_boxes=active_leg_boxes,
    )
    continuation_quality_score = _compute_continuation_quality_score(
        side=side,
        entry_distance_bucket=entry_distance_bucket,
        breakout_context=breakout_context,
        trend_regime=trend_regime,
        is_extended=is_extended,
        active_leg_boxes=active_leg_boxes,
        pattern_family=pattern_family,
        pullback_quality=pullback_quality,
        risk_quality=risk_quality,
        reward_quality=reward_quality,
    )
    return {
        "entry_distance_boxes": entry_distance_boxes,
        "entry_distance_bucket": entry_distance_bucket,
        "breakout_level": breakout_level,
        "continuation_quality_score": continuation_quality_score,
        "extension_penalty": extension_penalty,
        "continuation_execution_class": _continuation_execution_class(
            entry_distance_bucket=entry_distance_bucket,
            continuation_quality_score=continuation_quality_score,
            extension_penalty=extension_penalty,
            breakout_context=breakout_context,
            pullback_quality=pullback_quality,
        ),
        "pattern_family": pattern_family,
        "distance_policy_action": _distance_policy_action(entry_distance_bucket, pattern_family, side),
    }

def _pullback_position_long(support: float, resistance: float, current_bottom: float) -> float:
    span = max(float(resistance) - float(support), 1e-9)
    return (float(current_bottom) - float(support)) / span


def _rebound_position_short(support: float, resistance: float, current_top: float) -> float:
    span = max(float(resistance) - float(support), 1e-9)
    return (float(current_top) - float(support)) / span


def _classify_risk_quality(risk: float, profile: Any) -> str:
    box = _box_size(profile)
    if box <= 0 or risk <= 0:
        return RISK_WIDE
    risk_boxes = risk / box
    if risk_boxes <= 2.0:
        return RISK_TIGHT
    if risk_boxes <= 4.0:
        return RISK_NORMAL
    return RISK_WIDE


def _classify_reward_quality(rr1: float, rr2: float) -> str:
    if rr1 < 2.0:
        return REWARD_POOR
    if rr1 < 2.5:
        return REWARD_OK
    if rr2 >= 3.0:
        return REWARD_STRONG
    return REWARD_GOOD


def _classify_long_pullback_quality(support: float, resistance: float, current_bottom: float) -> str:
    pos = _pullback_position_long(support, resistance, current_bottom)
    if current_bottom <= support + 1e-12:
        return PULLBACK_BROKEN
    if pos < 0.10:
        return PULLBACK_SHALLOW
    if pos <= 0.75:
        return PULLBACK_HEALTHY
    return PULLBACK_DEEP


def _classify_short_rebound_quality(support: float, resistance: float, current_top: float) -> str:
    pos = _rebound_position_short(support, resistance, current_top)
    if current_top >= resistance - 1e-12:
        return PULLBACK_BROKEN
    if pos < 0.10:
        return PULLBACK_SHALLOW
    if pos <= 0.75:
        return PULLBACK_HEALTHY
    return PULLBACK_DEEP


def _compute_strength_score(
    *,
    pullback_quality: str,
    pullback_position: Optional[float],
    breakout_context: Optional[str],
    trend_regime: Optional[str],
    risk_quality: str,
    is_extended: bool,
    rr1: float,
    rr2: float,
) -> int:
    score = 0

    if pullback_quality == PULLBACK_HEALTHY:
        score += 25
    elif pullback_quality == PULLBACK_DEEP:
        score += 15
    elif pullback_quality == PULLBACK_SHALLOW:
        score += 5
    elif pullback_quality == PULLBACK_BROKEN:
        score -= 25

    if pullback_position is not None:
        if 0.20 <= pullback_position <= 0.60:
            score += 20
        elif 0.60 < pullback_position <= 0.85:
            score += 10
        else:
            score -= 10

    if breakout_context in (BREAKOUT_POST_BULLISH_PULLBACK, BREAKOUT_POST_BEARISH_REBOUND):
        score += 25
    elif breakout_context in (BREAKOUT_FRESH_BULLISH, BREAKOUT_FRESH_BEARISH):
        score += 15
    elif breakout_context == BREAKOUT_LATE_EXTENSION:
        score -= 20
    elif breakout_context == BREAKOUT_NONE:
        score -= 5

    if trend_regime in ("BULLISH_REGIME", "BEARISH_REGIME"):
        score += 10

    if risk_quality == RISK_TIGHT:
        score += 15
    elif risk_quality == RISK_NORMAL:
        score += 5
    else:
        score -= 10

    if is_extended:
        score -= 15

    if rr1 >= 2.5:
        score += 10
    elif rr1 < 2.0:
        score -= 20

    if rr2 >= 3.0:
        score += 5

    return max(0, min(100, score))


def _quality_grade(score: int) -> str:
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    return "D"


def _compute_continuation_strength_v1_long(
    *,
    breakout_context: Optional[str],
    trend_regime: Optional[str],
    pullback_quality: Optional[str],
    is_extended: bool,
    impulse_boxes: Optional[float],
    pullback_boxes: Optional[float],
    impulse_to_pullback_ratio: Optional[float],
) -> Optional[float]:
    is_long_continuation_context = breakout_context in (BREAKOUT_POST_BULLISH_PULLBACK, BREAKOUT_FRESH_BULLISH)
    if not is_long_continuation_context:
        return None

    score = 50.0

    if impulse_boxes is not None:
        score += min(20.0, max(0.0, impulse_boxes) * 3.0)

    if pullback_boxes is not None:
        pb = max(0.0, pullback_boxes)
        if pb <= 1.0:
            score += 12.0
        elif pb <= 2.0:
            score += 6.0
        elif pb <= 3.0:
            score += 0.0
        elif pb <= 4.0:
            score -= 8.0
        else:
            score -= 15.0

    if impulse_to_pullback_ratio is not None:
        ratio = impulse_to_pullback_ratio
        if ratio >= 3.0:
            score += 18.0
        elif ratio >= 2.0:
            score += 10.0
        elif ratio >= 1.3:
            score += 3.0
        elif ratio >= 1.0:
            score -= 6.0
        else:
            score -= 16.0

    if breakout_context == BREAKOUT_POST_BULLISH_PULLBACK:
        score += 10.0
    elif breakout_context == BREAKOUT_FRESH_BULLISH:
        score += 4.0
    elif breakout_context == BREAKOUT_LATE_EXTENSION:
        score -= 12.0

    if pullback_quality == PULLBACK_HEALTHY:
        score += 8.0
    elif pullback_quality == PULLBACK_SHALLOW:
        score -= 2.0
    elif pullback_quality == PULLBACK_DEEP:
        score -= 8.0
    elif pullback_quality == PULLBACK_BROKEN:
        score -= 20.0

    if trend_regime == "BULLISH_REGIME":
        score += 8.0
    elif trend_regime == "RANGE_REGIME":
        score -= 5.0
    elif trend_regime == "BEARISH_REGIME":
        score -= 10.0

    if is_extended:
        score -= 8.0

    return float(max(0.0, min(100.0, score)))


def _derive_cs_geometry_component(
    *,
    pullback_quality: Optional[str],
    impulse_to_pullback_ratio: Optional[float],
    impulse_boxes: Optional[float],
    pullback_boxes: Optional[float],
) -> str:
    if pullback_quality == PULLBACK_BROKEN:
        return "BROKEN_STRUCTURE"
    if impulse_to_pullback_ratio is not None:
        ratio = float(impulse_to_pullback_ratio)
        if ratio >= 3.0:
            return "STRONG_IMPULSE_SHALLOW_PULLBACK"
        if ratio >= 2.0:
            return "GOOD_IMPULSE_BALANCED_PULLBACK"
        if ratio >= 1.3:
            return "MODEST_IMPULSE_BALANCED_PULLBACK"
        if ratio >= 1.0:
            return "WEAK_IMPULSE_DEEPER_PULLBACK"
        return "PULLBACK_DOMINANT_GEOMETRY"
    if impulse_boxes is not None and pullback_boxes is not None:
        if float(pullback_boxes) <= 0:
            return "NO_PULLBACK_MEASURED"
        ratio = float(impulse_boxes) / max(float(pullback_boxes), 1e-9)
        if ratio >= 2.0:
            return "IMPULSE_DOMINANT_GEOMETRY"
        return "PULLBACK_HEAVY_GEOMETRY"
    if pullback_quality:
        return f"{pullback_quality}_GEOMETRY"
    return "UNCLASSIFIED_GEOMETRY"


def _derive_cs_profile_tag(
    *,
    side: str,
    breakout_context: Optional[str],
    trend_regime: Optional[str],
    is_extended: bool,
) -> str:
    tag = f"{side}_"
    if breakout_context:
        tag += str(breakout_context)
    else:
        tag += "NO_BREAKOUT_CONTEXT"
    if trend_regime:
        tag += f"__{trend_regime}"
    else:
        tag += "__NO_TREND_REGIME"
    if is_extended:
        tag += "__EXTENDED"
    else:
        tag += "__NON_EXTENDED"
    return tag


def _pullback_position_bucket(position: Optional[float]) -> Optional[str]:
    if position is None:
        return None
    if position < 0.0:
        return "BELOW_RANGE"
    if position < 0.10:
        return "0_10"
    if position < 0.20:
        return "10_20"
    if position <= 0.60:
        return "20_60"
    if position <= 0.75:
        return "60_75"
    if position <= 0.85:
        return "75_85"
    if position <= 1.0:
        return "85_100"
    return "ABOVE_RANGE"


def _breakout_context_rank(context: Optional[str], side: str) -> int:
    if side == "LONG":
        mapping = {
            BREAKOUT_POST_BULLISH_PULLBACK: 5,
            BREAKOUT_FRESH_BULLISH: 4,
            BREAKOUT_NONE: 2,
            BREAKOUT_LATE_EXTENSION: 1,
            BREAKOUT_FRESH_BEARISH: 0,
            BREAKOUT_POST_BEARISH_REBOUND: 0,
        }
    else:
        mapping = {
            BREAKOUT_POST_BEARISH_REBOUND: 5,
            BREAKOUT_FRESH_BEARISH: 4,
            BREAKOUT_NONE: 2,
            BREAKOUT_LATE_EXTENSION: 3,
            BREAKOUT_FRESH_BULLISH: 1,
            BREAKOUT_POST_BULLISH_PULLBACK: 1,
        }
    return int(mapping.get(context, 0))


def _compute_early_trend_diagnostics(
    *,
    trend_regime: Optional[str],
    breakout_context: Optional[str],
    active_leg_boxes: int,
    pullback_position: Optional[float],
    is_extended: bool,
) -> Dict[str, Any]:
    leg_stage = "EARLY" if active_leg_boxes <= 1 else ("MATURE" if active_leg_boxes == 2 else "LATE")
    pullback_stage = "UNKNOWN"
    if pullback_position is not None:
        if pullback_position < 0.20:
            pullback_stage = "SHALLOW_EARLY"
        elif pullback_position <= 0.75:
            pullback_stage = "HEALTHY_BALANCED"
        else:
            pullback_stage = "DEEP_LATE"
    context_bias = "UNCLASSIFIED"
    if breakout_context in (BREAKOUT_FRESH_BULLISH, BREAKOUT_FRESH_BEARISH):
        context_bias = "FRESH_BREAKOUT"
    elif breakout_context in (BREAKOUT_POST_BULLISH_PULLBACK, BREAKOUT_POST_BEARISH_REBOUND):
        context_bias = "POST_BREAKOUT_CONTINUATION"
    elif breakout_context == BREAKOUT_LATE_EXTENSION:
        context_bias = "LATE_EXTENSION"
    elif breakout_context == BREAKOUT_NONE:
        context_bias = "NO_BREAKOUT"
    regime_bias = trend_regime or "NO_REGIME"
    score = 0
    if leg_stage == "EARLY":
        score += 20
    elif leg_stage == "MATURE":
        score += 10
    else:
        score -= 10
    if pullback_stage == "HEALTHY_BALANCED":
        score += 20
    elif pullback_stage == "DEEP_LATE":
        score -= 10
    if context_bias == "POST_BREAKOUT_CONTINUATION":
        score += 15
    elif context_bias == "FRESH_BREAKOUT":
        score += 8
    elif context_bias == "LATE_EXTENSION":
        score -= 20
    if is_extended:
        score -= 15
    return {
        "early_trend_diag_leg_stage": leg_stage,
        "early_trend_diag_pullback_stage": pullback_stage,
        "early_trend_diag_context_bias": context_bias,
        "early_trend_diag_regime_bias": regime_bias,
        "early_trend_diag_score": float(max(0, min(100, 50 + score))),
    }


def _base_result(
    *,
    symbol: str,
    side: str,
    status: str,
    reason: str,
    reject_reason: Optional[str],
    breakout_context: Optional[str],
    zone_low: Optional[float] = None,
    zone_high: Optional[float] = None,
    ideal_entry: Optional[float] = None,
    invalidation: Optional[float] = None,
    risk: Optional[float] = None,
    tp1: Optional[float] = None,
    tp2: Optional[float] = None,
    rr1: Optional[float] = None,
    rr2: Optional[float] = None,
    pullback_quality: Optional[str] = None,
    risk_quality: Optional[str] = None,
    reward_quality: Optional[str] = None,
    quality_score: Optional[float] = None,
    quality_grade: Optional[str] = None,
    continuation_strength_v1: Optional[float] = None,
    cs_geometry_component: Optional[str] = None,
    cs_profile_tag: Optional[str] = None,
    decision_version: Optional[str] = None,
    decision_path: Optional[str] = None,
    watch_flags: Optional[str] = None,
    reject_flags: Optional[str] = None,
    promotion_checklist_pass_count: Optional[int] = None,
    promotion_checklist_failed_items: Optional[str] = None,
    entry_to_support_boxes: Optional[float] = None,
    invalidation_distance_boxes: Optional[float] = None,
    pullback_position_bucket: Optional[str] = None,
    breakout_context_rank: Optional[int] = None,
    extension_risk_score: Optional[float] = None,
    is_baseline_profile_match: Optional[int] = None,
    early_trend_diag_leg_stage: Optional[str] = None,
    early_trend_diag_pullback_stage: Optional[str] = None,
    early_trend_diag_context_bias: Optional[str] = None,
    early_trend_diag_regime_bias: Optional[str] = None,
    early_trend_diag_score: Optional[float] = None,
    entry_distance_boxes: Optional[float] = None,
    entry_distance_bucket: Optional[str] = None,
    breakout_level: Optional[float] = None,
    continuation_quality_score: Optional[float] = None,
    extension_penalty: Optional[float] = None,
    continuation_execution_class: Optional[str] = None,
    pattern_family: Optional[str] = None,
    distance_policy_action: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "strategy": "pullback_retest",
        "side": side,
        "status": status,
        "symbol": symbol,
        "zone_low": zone_low,
        "zone_high": zone_high,
        "ideal_entry": ideal_entry,
        "invalidation": invalidation,
        "risk": risk,
        "tp1": tp1,
        "tp2": tp2,
        "rr1": rr1,
        "rr2": rr2,
        "pullback_quality": pullback_quality,
        "risk_quality": risk_quality,
        "reward_quality": reward_quality,
        "quality_score": quality_score,
        "quality_grade": quality_grade,
        "continuation_strength_v1": continuation_strength_v1,
        "cs_geometry_component": cs_geometry_component,
        "cs_profile_tag": cs_profile_tag,
        "decision_version": decision_version,
        "decision_path": decision_path,
        "watch_flags": watch_flags,
        "reject_flags": reject_flags,
        "promotion_checklist_pass_count": promotion_checklist_pass_count,
        "promotion_checklist_failed_items": promotion_checklist_failed_items,
        "entry_to_support_boxes": entry_to_support_boxes,
        "invalidation_distance_boxes": invalidation_distance_boxes,
        "pullback_position_bucket": pullback_position_bucket,
        "breakout_context_rank": breakout_context_rank,
        "extension_risk_score": extension_risk_score,
        "is_baseline_profile_match": is_baseline_profile_match,
        "early_trend_diag_leg_stage": early_trend_diag_leg_stage,
        "early_trend_diag_pullback_stage": early_trend_diag_pullback_stage,
        "early_trend_diag_context_bias": early_trend_diag_context_bias,
        "early_trend_diag_regime_bias": early_trend_diag_regime_bias,
        "early_trend_diag_score": early_trend_diag_score,
        "entry_distance_boxes": entry_distance_boxes,
        "entry_distance_bucket": entry_distance_bucket,
        "breakout_level": breakout_level,
        "continuation_quality_score": continuation_quality_score,
        "extension_penalty": extension_penalty,
        "continuation_execution_class": continuation_execution_class,
        "pattern_family": pattern_family,
        "distance_policy_action": distance_policy_action,
        "breakout_context": breakout_context,
        "reject_reason": reject_reason,
        "reason": reason,
    }


def evaluate_pullback_retest_long(
    symbol: str,
    profile: Any,
    columns: List[Any],
    structure_state: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if not structure_state:
        return None

    box = _box_size(profile)
    if box <= 0:
        return None

    trend = structure_state.get("trend_state")
    trend_regime = structure_state.get("trend_regime")
    slope = structure_state.get("immediate_slope")
    support = _safe_float(structure_state.get("support_level"))
    resistance = _safe_float(structure_state.get("resistance_level"))
    breakout_context = structure_state.get("breakout_context")
    is_extended = bool(structure_state.get("is_extended_move", False))
    active_leg_boxes = int(structure_state.get("active_leg_boxes") or 0)
    current_kind = structure_state.get("current_column_kind")
    current_bottom = _safe_float(structure_state.get("current_column_bottom"))
    impulse_boxes = _safe_float(structure_state.get("impulse_boxes"))
    pullback_boxes = _safe_float(structure_state.get("pullback_boxes"))
    impulse_to_pullback_ratio = _safe_float(structure_state.get("impulse_to_pullback_ratio"))

    current = _current_column(columns)
    if current is None:
        return None

    current_top = _safe_float(structure_state.get("current_column_top"))
    if current_top is None:
        current_top = _col_top(current)
    preliminary_diag = _compute_continuation_diagnostics(
        side="LONG",
        profile=profile,
        structure_state=structure_state,
        entry_price=current_top if current_kind == "X" else None,
        breakout_level=resistance,
    )

    if trend != "BULLISH":
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Bullish trend state not present", reject_reason="trend_not_bullish", breakout_context=breakout_context, **preliminary_diag)
    if support is None or resistance is None:
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Support / resistance unavailable", reject_reason="missing_structure_levels", breakout_context=breakout_context, **preliminary_diag)
    if resistance <= support:
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Invalid structure range", reject_reason="invalid_structure_range", breakout_context=breakout_context, **preliminary_diag)

    if (
        _is_continuation_execution_v1_enabled(profile, structure_state)
        and current_kind == "X"
        and breakout_context == BREAKOUT_FRESH_BULLISH
        and trend_regime == "BULLISH_REGIME"
    ):
        ideal_entry = current_top
        breakout_level = resistance
        invalidation = breakout_level - box
        risk = ideal_entry - invalidation
        if risk <= 0:
            return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
                reason="Continuation v1 long has non-positive risk distance", reject_reason="non_positive_risk",
                breakout_context=breakout_context, zone_low=breakout_level, zone_high=ideal_entry,
                ideal_entry=ideal_entry, invalidation=invalidation, **preliminary_diag)
        tp1 = ideal_entry + 2.0 * risk
        tp2 = ideal_entry + 3.0 * risk
        rr1 = (tp1 - ideal_entry) / risk
        rr2 = (tp2 - ideal_entry) / risk
        risk_quality = _classify_risk_quality(risk, profile)
        reward_quality = _classify_reward_quality(rr1, rr2)
        diag = _compute_continuation_diagnostics(
            side="LONG",
            profile=profile,
            structure_state=structure_state,
            entry_price=ideal_entry,
            breakout_level=breakout_level,
            risk_quality=risk_quality,
            reward_quality=reward_quality,
        )
        score = float(diag["continuation_quality_score"] or 0.0)
        grade = _quality_grade(int(round(score)))
        watch_flags: List[str] = []
        reject_flags: List[str] = []
        decision_path = "long_eval->continuation_execution_v1"
        bucket = str(diag.get("entry_distance_bucket") or ENTRY_DISTANCE_UNKNOWN)
        penalty = float(diag.get("extension_penalty") or 0.0)
        pattern_family = diag.get("pattern_family")
        if reward_quality == REWARD_POOR:
            status = STATUS_REJECT
            reason = "Continuation v1 long reward-to-risk below minimum threshold"
            reject_reason = "rr_too_low"
            reject_flags.append("rr_too_low")
            decision_path += "->reject_rr_too_low"
        elif bucket == ENTRY_DISTANCE_EXTENDED or penalty >= 80.0 or breakout_context == BREAKOUT_LATE_EXTENSION or is_extended:
            status = STATUS_REJECT
            reason = "Continuation v1 long rejected extended continuation"
            reject_reason = "continuation_extended"
            reject_flags.append("continuation_extended")
            decision_path += "->reject_extended_continuation"
        elif bucket == ENTRY_DISTANCE_LATE:
            status = STATUS_WATCH
            reason = "Continuation v1 long is late; kept as WATCH for research"
            reject_reason = None
            watch_flags.append("late_entry_distance")
            if _is_aligned_triangle(pattern_family, "LONG"):
                watch_flags.append("triangle_late_exception")
            decision_path += "->watch_late_entry_distance"
        elif score >= 68.0 and bucket in (ENTRY_DISTANCE_IMMEDIATE, ENTRY_DISTANCE_EARLY) and risk_quality != RISK_WIDE:
            status = STATUS_CANDIDATE
            reason = "Continuation v1 bullish early continuation research candidate"
            reject_reason = None
            decision_path += "->candidate_continuation_v1"
        elif score >= 45.0:
            status = STATUS_WATCH
            reason = "Continuation v1 long exists but quality is not yet strong enough"
            reject_reason = None
            watch_flags.append("continuation_quality_not_strong_enough")
            decision_path += "->watch_quality_not_strong_enough"
        else:
            status = STATUS_REJECT
            reason = "Continuation v1 long quality is insufficient"
            reject_reason = "continuation_quality_too_low"
            reject_flags.append("continuation_quality_too_low")
            decision_path += "->reject_quality_too_low"
        return _base_result(
            symbol=symbol, side="LONG", status=status, reason=reason, reject_reason=reject_reason,
            breakout_context=breakout_context, zone_low=breakout_level, zone_high=ideal_entry,
            ideal_entry=ideal_entry, invalidation=invalidation, risk=risk, tp1=tp1, tp2=tp2,
            rr1=rr1, rr2=rr2, risk_quality=risk_quality, reward_quality=reward_quality,
            quality_score=score, quality_grade=grade, decision_version=CONTINUATION_EXECUTION_V1_VERSION,
            decision_path=decision_path, watch_flags="|".join(watch_flags), reject_flags="|".join(reject_flags),
            invalidation_distance_boxes=risk / box if box > 0 else None,
            breakout_context_rank=_breakout_context_rank(breakout_context, "LONG"),
            extension_risk_score=penalty, is_baseline_profile_match=0, **diag
        )

    if slope != "BEARISH_PULLBACK":
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="No bearish pullback active for long retest", reject_reason="wrong_immediate_slope", breakout_context=breakout_context, **preliminary_diag)
    if current_kind != "O":
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Current column is not an O pullback column", reject_reason="wrong_current_column_kind", breakout_context=breakout_context, **preliminary_diag)

    if current_bottom is None:
        current_bottom = _col_bottom(current)

    zone_low = support + box
    zone_high = min(resistance, current_bottom + box)
    if zone_high < zone_low:
        zone_high = zone_low

    ideal_entry = zone_low
    invalidation = support - box
    risk = ideal_entry - invalidation

    if risk <= 0:
        pullback_diag = _compute_continuation_diagnostics(
            side="LONG", profile=profile, structure_state=structure_state,
            entry_price=ideal_entry, breakout_level=resistance,
        )
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Non-positive risk distance", reject_reason="non_positive_risk", breakout_context=breakout_context,
            zone_low=zone_low, zone_high=zone_high, ideal_entry=ideal_entry, invalidation=invalidation, **pullback_diag)

    tp1 = ideal_entry + 2.0 * risk
    tp2 = ideal_entry + 3.0 * risk
    rr1 = (tp1 - ideal_entry) / risk
    rr2 = (tp2 - ideal_entry) / risk

    pullback_position = _pullback_position_long(support, resistance, current_bottom)
    pullback_quality = _classify_long_pullback_quality(support, resistance, current_bottom)
    risk_quality = _classify_risk_quality(risk, profile)
    reward_quality = _classify_reward_quality(rr1, rr2)
    invalidation_distance_boxes = risk / box if box > 0 else None

    strength = _compute_strength_score(
        pullback_quality=pullback_quality,
        pullback_position=pullback_position,
        breakout_context=breakout_context,
        trend_regime=trend_regime,
        risk_quality=risk_quality,
        is_extended=is_extended,
        rr1=rr1,
        rr2=rr2,
    )
    grade = _quality_grade(strength)
    continuation_strength_v1 = _compute_continuation_strength_v1_long(
        breakout_context=breakout_context,
        trend_regime=trend_regime,
        pullback_quality=pullback_quality,
        is_extended=is_extended,
        impulse_boxes=impulse_boxes,
        pullback_boxes=pullback_boxes,
        impulse_to_pullback_ratio=impulse_to_pullback_ratio,
    )
    cs_geometry_component = _derive_cs_geometry_component(
        pullback_quality=pullback_quality,
        impulse_to_pullback_ratio=impulse_to_pullback_ratio,
        impulse_boxes=impulse_boxes,
        pullback_boxes=pullback_boxes,
    )
    cs_profile_tag = _derive_cs_profile_tag(
        side="LONG",
        breakout_context=breakout_context,
        trend_regime=trend_regime,
        is_extended=is_extended,
    )
    pullback_position_bucket = _pullback_position_bucket(pullback_position)
    early_trend_diag = _compute_early_trend_diagnostics(
        trend_regime=trend_regime,
        breakout_context=breakout_context,
        active_leg_boxes=active_leg_boxes,
        pullback_position=pullback_position,
        is_extended=is_extended,
    )
    breakout_rank = _breakout_context_rank(breakout_context, "LONG")
    entry_to_support_boxes = (ideal_entry - support) / box if box > 0 else None
    extension_risk_score = float(100 if (breakout_context == BREAKOUT_LATE_EXTENSION or is_extended) else 0)
    is_baseline_profile_match = int(
        pullback_quality == PULLBACK_HEALTHY
        and active_leg_boxes == 2
        and breakout_context == BREAKOUT_POST_BULLISH_PULLBACK
        and (not is_extended)
    )
    watch_flags: List[str] = []
    reject_flags: List[str] = []
    decision_path = "long_eval"

    if pullback_quality == PULLBACK_BROKEN:
        status = STATUS_REJECT
        reason = "Pullback broke support"
        reject_reason = "broken_pullback"
        reject_flags.append("broken_pullback")
        decision_path += "->reject_broken_pullback"
    elif reward_quality == REWARD_POOR:
        status = STATUS_REJECT
        reason = "Reward-to-risk below minimum threshold"
        reject_reason = "rr_too_low"
        reject_flags.append("rr_too_low")
        decision_path += "->reject_rr_too_low"
    elif breakout_context == BREAKOUT_LATE_EXTENSION or is_extended:
        status = STATUS_WATCH
        reason = "Bullish setup exists but extended structure is restricted to WATCH by promotion policy"
        reject_reason = None
        watch_flags.append("extended_policy_restriction")
        decision_path += "->watch_extended_policy"
    elif active_leg_boxes >= 3:
        status = STATUS_WATCH
        reason = "Bullish setup exists but late leg count is restricted to WATCH by promotion policy"
        reject_reason = None
        watch_flags.append("late_leg_policy_restriction")
        decision_path += "->watch_late_leg_policy"
    elif pullback_quality == PULLBACK_DEEP:
        status = STATUS_WATCH
        reason = "Bullish setup exists but deep pullback is restricted to WATCH by promotion policy"
        reject_reason = None
        watch_flags.append("deep_pullback_policy_restriction")
        decision_path += "->watch_deep_pullback_policy"
    elif strength >= 65 and pullback_quality == PULLBACK_HEALTHY and active_leg_boxes == 2 and breakout_context == BREAKOUT_POST_BULLISH_PULLBACK:
        status = STATUS_CANDIDATE
        reason = "Bullish pullback near support with acceptable close-confirmed risk profile"
        reject_reason = None
        decision_path += "->candidate_baseline_promoted"
    elif strength >= 35:
        status = STATUS_WATCH
        reason = "Bullish setup exists but quality is not yet strong enough"
        reject_reason = None
        watch_flags.append("quality_not_strong_enough")
        decision_path += "->watch_quality_not_strong_enough"
    else:
        status = STATUS_REJECT
        reason = "Bullish setup quality is insufficient"
        reject_reason = "quality_too_low"
        reject_flags.append("quality_too_low")
        decision_path += "->reject_quality_too_low"

    if status in (STATUS_WATCH, STATUS_CANDIDATE):
        if (
            trend_regime == "BULLISH_REGIME"
            and (not is_extended)
            and active_leg_boxes == 3
            and pullback_position_bucket in ("20_60", "75_85")
        ):
            status = STATUS_CANDIDATE
            reason = "promoted_by_structure_edge_filter"
            decision_path += "->override_candidate_structure_edge_filter"

    pullback_diag = _compute_continuation_diagnostics(
        side="LONG",
        profile=profile,
        structure_state=structure_state,
        entry_price=ideal_entry,
        breakout_level=resistance,
        pullback_quality=pullback_quality,
        risk_quality=risk_quality,
        reward_quality=reward_quality,
    )

    checklist_failed_items = [
        item
        for item, passed in (
            ("pullback_healthy", pullback_quality == PULLBACK_HEALTHY),
            ("active_leg_boxes_eq_2", active_leg_boxes == 2),
            ("post_breakout_pullback_context", breakout_context == BREAKOUT_POST_BULLISH_PULLBACK),
            ("non_extended", not is_extended),
        )
        if not passed
    ]
    promotion_checklist_pass_count = 4 - len(checklist_failed_items)

    return _base_result(
        symbol=symbol, side="LONG", status=status, reason=reason, reject_reason=reject_reason,
        breakout_context=breakout_context, zone_low=zone_low, zone_high=zone_high,
        ideal_entry=ideal_entry, invalidation=invalidation, risk=risk, tp1=tp1, tp2=tp2,
        rr1=rr1, rr2=rr2, pullback_quality=pullback_quality, risk_quality=risk_quality,
        reward_quality=reward_quality, quality_score=float(strength), quality_grade=grade,
        continuation_strength_v1=continuation_strength_v1,
        cs_geometry_component=cs_geometry_component,
        cs_profile_tag=cs_profile_tag,
        decision_version=DECISION_VERSION,
        decision_path=decision_path,
        watch_flags="|".join(watch_flags),
        reject_flags="|".join(reject_flags),
        promotion_checklist_pass_count=promotion_checklist_pass_count,
        promotion_checklist_failed_items="|".join(checklist_failed_items),
        entry_to_support_boxes=entry_to_support_boxes,
        invalidation_distance_boxes=invalidation_distance_boxes,
        pullback_position_bucket=pullback_position_bucket,
        breakout_context_rank=breakout_rank,
        extension_risk_score=extension_risk_score,
        is_baseline_profile_match=is_baseline_profile_match,
        early_trend_diag_leg_stage=early_trend_diag["early_trend_diag_leg_stage"],
        early_trend_diag_pullback_stage=early_trend_diag["early_trend_diag_pullback_stage"],
        early_trend_diag_context_bias=early_trend_diag["early_trend_diag_context_bias"],
        early_trend_diag_regime_bias=early_trend_diag["early_trend_diag_regime_bias"],
        early_trend_diag_score=early_trend_diag["early_trend_diag_score"],
        **pullback_diag,
    )


def evaluate_pullback_retest_short(
    symbol: str,
    profile: Any,
    columns: List[Any],
    structure_state: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if not structure_state:
        return None

    box = _box_size(profile)
    if box <= 0:
        return None

    trend = structure_state.get("trend_state")
    trend_regime = structure_state.get("trend_regime")
    slope = structure_state.get("immediate_slope")
    support = _safe_float(structure_state.get("support_level"))
    resistance = _safe_float(structure_state.get("resistance_level"))
    breakout_context = structure_state.get("breakout_context")
    is_extended = bool(structure_state.get("is_extended_move", False))
    active_leg_boxes = int(structure_state.get("active_leg_boxes") or 0)
    current_kind = structure_state.get("current_column_kind")
    current_top = _safe_float(structure_state.get("current_column_top"))
    latest_signal_name = str(structure_state.get("latest_signal_name") or "")
    market_state = str(structure_state.get("market_state") or "")
    impulse_boxes = _safe_float(structure_state.get("impulse_boxes"))
    pullback_boxes = _safe_float(structure_state.get("pullback_boxes"))
    impulse_to_pullback_ratio = _safe_float(structure_state.get("impulse_to_pullback_ratio"))

    current = _current_column(columns)
    if current is None:
        return None

    current_bottom = _safe_float(structure_state.get("current_column_bottom"))
    if current_bottom is None:
        current_bottom = _col_bottom(current)
    preliminary_diag = _compute_continuation_diagnostics(
        side="SHORT",
        profile=profile,
        structure_state=structure_state,
        entry_price=current_bottom if current_kind == "O" else None,
        breakout_level=support,
    )

    if trend not in ("BULLISH", "BEARISH"):
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Trend state unavailable", reject_reason="missing_trend_state", breakout_context=breakout_context, **preliminary_diag)
    if support is None or resistance is None:
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Support / resistance unavailable", reject_reason="missing_structure_levels", breakout_context=breakout_context, **preliminary_diag)
    if resistance <= support:
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Invalid structure range", reject_reason="invalid_structure_range", breakout_context=breakout_context, **preliminary_diag)

    if (
        _is_continuation_execution_v1_enabled(profile, structure_state)
        and current_kind == "O"
        and breakout_context == BREAKOUT_FRESH_BEARISH
        and trend_regime == "BEARISH_REGIME"
    ):
        ideal_entry = current_bottom
        breakout_level = support
        invalidation = breakout_level + box
        risk = invalidation - ideal_entry
        if risk <= 0:
            return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
                reason="Continuation v1 short has non-positive risk distance", reject_reason="non_positive_risk",
                breakout_context=breakout_context, zone_low=ideal_entry, zone_high=breakout_level,
                ideal_entry=ideal_entry, invalidation=invalidation, **preliminary_diag)
        tp1 = ideal_entry - 2.0 * risk
        tp2 = ideal_entry - 3.0 * risk
        rr1 = (ideal_entry - tp1) / risk
        rr2 = (ideal_entry - tp2) / risk
        risk_quality = _classify_risk_quality(risk, profile)
        reward_quality = _classify_reward_quality(rr1, rr2)
        diag = _compute_continuation_diagnostics(
            side="SHORT",
            profile=profile,
            structure_state=structure_state,
            entry_price=ideal_entry,
            breakout_level=breakout_level,
            risk_quality=risk_quality,
            reward_quality=reward_quality,
        )
        score = float(diag["continuation_quality_score"] or 0.0)
        grade = _quality_grade(int(round(score)))
        watch_flags: List[str] = []
        reject_flags: List[str] = []
        decision_path = "short_eval->continuation_execution_v1"
        bucket = str(diag.get("entry_distance_bucket") or ENTRY_DISTANCE_UNKNOWN)
        penalty = float(diag.get("extension_penalty") or 0.0)
        pattern_family = diag.get("pattern_family")
        if reward_quality == REWARD_POOR:
            status = STATUS_REJECT
            reason = "Continuation v1 short reward-to-risk below minimum threshold"
            reject_reason = "rr_too_low"
            reject_flags.append("rr_too_low")
            decision_path += "->reject_rr_too_low"
        elif bucket == ENTRY_DISTANCE_EXTENDED or penalty >= 80.0 or breakout_context == BREAKOUT_LATE_EXTENSION or is_extended:
            status = STATUS_REJECT
            reason = "Continuation v1 short rejected extended continuation"
            reject_reason = "continuation_extended"
            reject_flags.append("continuation_extended")
            decision_path += "->reject_extended_continuation"
        elif bucket == ENTRY_DISTANCE_LATE:
            status = STATUS_WATCH
            reason = "Continuation v1 short is late; kept as WATCH for research"
            reject_reason = None
            watch_flags.append("late_entry_distance")
            if _is_aligned_triangle(pattern_family, "SHORT"):
                watch_flags.append("triangle_late_exception")
            decision_path += "->watch_late_entry_distance"
        elif score >= 68.0 and bucket in (ENTRY_DISTANCE_IMMEDIATE, ENTRY_DISTANCE_EARLY) and risk_quality != RISK_WIDE:
            status = STATUS_CANDIDATE
            reason = "Continuation v1 bearish early continuation research candidate"
            reject_reason = None
            decision_path += "->candidate_continuation_v1"
        elif score >= 45.0:
            status = STATUS_WATCH
            reason = "Continuation v1 short exists but quality is not yet strong enough"
            reject_reason = None
            watch_flags.append("continuation_quality_not_strong_enough")
            decision_path += "->watch_quality_not_strong_enough"
        else:
            status = STATUS_REJECT
            reason = "Continuation v1 short quality is insufficient"
            reject_reason = "continuation_quality_too_low"
            reject_flags.append("continuation_quality_too_low")
            decision_path += "->reject_quality_too_low"
        return _base_result(
            symbol=symbol, side="SHORT", status=status, reason=reason, reject_reason=reject_reason,
            breakout_context=breakout_context, zone_low=ideal_entry, zone_high=breakout_level,
            ideal_entry=ideal_entry, invalidation=invalidation, risk=risk, tp1=tp1, tp2=tp2,
            rr1=rr1, rr2=rr2, risk_quality=risk_quality, reward_quality=reward_quality,
            quality_score=score, quality_grade=grade, decision_version=CONTINUATION_EXECUTION_V1_VERSION,
            decision_path=decision_path, watch_flags="|".join(watch_flags), reject_flags="|".join(reject_flags),
            invalidation_distance_boxes=risk / box if box > 0 else None,
            breakout_context_rank=_breakout_context_rank(breakout_context, "SHORT"),
            extension_risk_score=penalty, is_baseline_profile_match=0, **diag
        )

    if current_kind != "O":
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Short reversal branch requires O column after failure", reject_reason="wrong_current_column_kind", breakout_context=breakout_context, **preliminary_diag)
    if slope not in ("BEARISH_PULLBACK", "BEARISH"):
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Short reversal branch requires bearish downslope after failure", reject_reason="wrong_immediate_slope", breakout_context=breakout_context, **preliminary_diag)

    bullish_failure_context = (
        trend == "BULLISH"
        or breakout_context in (BREAKOUT_POST_BULLISH_PULLBACK, BREAKOUT_LATE_EXTENSION, BREAKOUT_FRESH_BULLISH)
        or is_extended
        or market_state == "RANGE"
        or "TOP" in latest_signal_name.upper()
    )
    if not bullish_failure_context:
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_WATCH,
            reason="Short reversal candidate exists but no clear bullish failure context yet", reject_reason=None, breakout_context=breakout_context, **preliminary_diag)

    if current_top is None:
        current_top = _col_top(current)

    zone_high = current_top
    zone_low = max(support, current_bottom)
    if zone_low > zone_high:
        zone_low = zone_high

    ideal_entry = current_bottom
    invalidation = resistance + box
    risk = invalidation - ideal_entry

    if risk <= 0:
        rebound_diag = _compute_continuation_diagnostics(
            side="SHORT", profile=profile, structure_state=structure_state,
            entry_price=ideal_entry, breakout_level=support,
        )
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Non-positive risk distance", reject_reason="non_positive_risk", breakout_context=breakout_context,
            zone_low=zone_low, zone_high=zone_high, ideal_entry=ideal_entry, invalidation=invalidation, **rebound_diag)

    tp1 = ideal_entry - 2.0 * risk
    tp2 = ideal_entry - 3.0 * risk
    rr1 = (ideal_entry - tp1) / risk
    rr2 = (ideal_entry - tp2) / risk

    rebound_position = _rebound_position_short(support, resistance, current_top)
    rebound_quality = _classify_short_rebound_quality(support, resistance, current_top)
    risk_quality = _classify_risk_quality(risk, profile)
    reward_quality = _classify_reward_quality(rr1, rr2)
    invalidation_distance_boxes = risk / box if box > 0 else None

    strength = _compute_strength_score(
        pullback_quality=rebound_quality,
        pullback_position=rebound_position,
        breakout_context=breakout_context,
        trend_regime=trend_regime,
        risk_quality=risk_quality,
        is_extended=False,
        rr1=rr1,
        rr2=rr2,
    )

    if is_extended:
        strength = min(100, strength + 10)
    if active_leg_boxes in (2, 3):
        strength = min(100, strength + 5)
    if market_state == "RANGE":
        strength = min(100, strength + 5)

    grade = _quality_grade(strength)
    cs_geometry_component = _derive_cs_geometry_component(
        pullback_quality=rebound_quality,
        impulse_to_pullback_ratio=impulse_to_pullback_ratio,
        impulse_boxes=impulse_boxes,
        pullback_boxes=pullback_boxes,
    )
    cs_profile_tag = _derive_cs_profile_tag(
        side="SHORT",
        breakout_context=breakout_context,
        trend_regime=trend_regime,
        is_extended=is_extended,
    )
    pullback_position_bucket = _pullback_position_bucket(rebound_position)
    early_trend_diag = _compute_early_trend_diagnostics(
        trend_regime=trend_regime,
        breakout_context=breakout_context,
        active_leg_boxes=active_leg_boxes,
        pullback_position=rebound_position,
        is_extended=is_extended,
    )
    breakout_rank = _breakout_context_rank(breakout_context, "SHORT")
    entry_to_support_boxes = (ideal_entry - support) / box if box > 0 else None
    extension_risk_score = float(100 if is_extended else 0)
    is_baseline_profile_match = 0
    watch_flags: List[str] = []
    reject_flags: List[str] = []
    decision_path = "short_eval"

    if rebound_quality == PULLBACK_BROKEN:
        status = STATUS_REJECT
        reason = "Failure structure is invalid; reversal short broke too far"
        reject_reason = "broken_reversal_context"
        reject_flags.append("broken_reversal_context")
        decision_path += "->reject_broken_reversal_context"
    elif reward_quality == REWARD_POOR:
        status = STATUS_REJECT
        reason = "Reward-to-risk below minimum threshold"
        reject_reason = "rr_too_low"
        reject_flags.append("rr_too_low")
        decision_path += "->reject_rr_too_low"
    elif active_leg_boxes < 2:
        status = STATUS_WATCH
        reason = "Short reversal exists but needs more mature failure structure"
        reject_reason = None
        watch_flags.append("immature_failure_structure")
        decision_path += "->watch_immature_failure_structure"
    elif rebound_quality == PULLBACK_SHALLOW:
        status = STATUS_WATCH
        reason = "Short reversal exists but failure pullback is still shallow"
        reject_reason = None
        watch_flags.append("shallow_failure_pullback")
        decision_path += "->watch_shallow_failure_pullback"
    elif risk_quality == RISK_WIDE:
        status = STATUS_WATCH
        reason = "Short reversal exists but risk is still wide"
        reject_reason = None
        watch_flags.append("risk_wide")
        decision_path += "->watch_risk_wide"
    elif strength >= 70 and active_leg_boxes in (2, 3):
        status = STATUS_CANDIDATE
        reason = "Failed bullish move / reversal short near resistance with acceptable risk profile"
        reject_reason = None
        decision_path += "->candidate_reversal_promoted"
    elif strength >= 45:
        status = STATUS_WATCH
        reason = "Short reversal exists but quality is not yet strong enough"
        reject_reason = None
        watch_flags.append("quality_not_strong_enough")
        decision_path += "->watch_quality_not_strong_enough"
    else:
        status = STATUS_REJECT
        reason = "Short reversal quality is insufficient"
        reject_reason = "quality_too_low"
        reject_flags.append("quality_too_low")
        decision_path += "->reject_quality_too_low"

    rebound_diag = _compute_continuation_diagnostics(
        side="SHORT",
        profile=profile,
        structure_state=structure_state,
        entry_price=ideal_entry,
        breakout_level=support,
        pullback_quality=rebound_quality,
        risk_quality=risk_quality,
        reward_quality=reward_quality,
    )

    return _base_result(
        symbol=symbol, side="SHORT", status=status, reason=reason, reject_reason=reject_reason,
        breakout_context=breakout_context, zone_low=zone_low, zone_high=zone_high,
        ideal_entry=ideal_entry, invalidation=invalidation, risk=risk, tp1=tp1, tp2=tp2,
        rr1=rr1, rr2=rr2, pullback_quality=rebound_quality, risk_quality=risk_quality,
        reward_quality=reward_quality, quality_score=float(strength), quality_grade=grade,
        continuation_strength_v1=None, cs_geometry_component=cs_geometry_component, cs_profile_tag=cs_profile_tag,
        decision_version=DECISION_VERSION,
        decision_path=decision_path,
        watch_flags="|".join(watch_flags),
        reject_flags="|".join(reject_flags),
        promotion_checklist_pass_count=None,
        promotion_checklist_failed_items="",
        entry_to_support_boxes=entry_to_support_boxes,
        invalidation_distance_boxes=invalidation_distance_boxes,
        pullback_position_bucket=pullback_position_bucket,
        breakout_context_rank=breakout_rank,
        extension_risk_score=extension_risk_score,
        is_baseline_profile_match=is_baseline_profile_match,
        early_trend_diag_leg_stage=early_trend_diag["early_trend_diag_leg_stage"],
        early_trend_diag_pullback_stage=early_trend_diag["early_trend_diag_pullback_stage"],
        early_trend_diag_context_bias=early_trend_diag["early_trend_diag_context_bias"],
        early_trend_diag_regime_bias=early_trend_diag["early_trend_diag_regime_bias"],
        early_trend_diag_score=early_trend_diag["early_trend_diag_score"],
        **rebound_diag,
    )
