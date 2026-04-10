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

    if trend != "BULLISH":
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Bullish trend state not present", reject_reason="trend_not_bullish", breakout_context=breakout_context)
    if support is None or resistance is None:
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Support / resistance unavailable", reject_reason="missing_structure_levels", breakout_context=breakout_context)
    if resistance <= support:
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Invalid structure range", reject_reason="invalid_structure_range", breakout_context=breakout_context)
    if slope != "BEARISH_PULLBACK":
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="No bearish pullback active for long retest", reject_reason="wrong_immediate_slope", breakout_context=breakout_context)
    if current_kind != "O":
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Current column is not an O pullback column", reject_reason="wrong_current_column_kind", breakout_context=breakout_context)

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
        return _base_result(symbol=symbol, side="LONG", status=STATUS_REJECT,
            reason="Non-positive risk distance", reject_reason="non_positive_risk", breakout_context=breakout_context,
            zone_low=zone_low, zone_high=zone_high, ideal_entry=ideal_entry, invalidation=invalidation)

    tp1 = ideal_entry + 2.0 * risk
    tp2 = ideal_entry + 3.0 * risk
    rr1 = (tp1 - ideal_entry) / risk
    rr2 = (tp2 - ideal_entry) / risk

    pullback_position = _pullback_position_long(support, resistance, current_bottom)
    pullback_quality = _classify_long_pullback_quality(support, resistance, current_bottom)
    risk_quality = _classify_risk_quality(risk, profile)
    reward_quality = _classify_reward_quality(rr1, rr2)

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

    if pullback_quality == PULLBACK_BROKEN:
        status = STATUS_REJECT
        reason = "Pullback broke support"
        reject_reason = "broken_pullback"
    elif reward_quality == REWARD_POOR:
        status = STATUS_REJECT
        reason = "Reward-to-risk below minimum threshold"
        reject_reason = "rr_too_low"
    elif breakout_context == BREAKOUT_LATE_EXTENSION or is_extended:
        status = STATUS_WATCH
        reason = "Bullish setup exists but extended structure is restricted to WATCH by promotion policy"
        reject_reason = None
    elif active_leg_boxes >= 3:
        status = STATUS_WATCH
        reason = "Bullish setup exists but late leg count is restricted to WATCH by promotion policy"
        reject_reason = None
    elif pullback_quality == PULLBACK_DEEP:
        status = STATUS_WATCH
        reason = "Bullish setup exists but deep pullback is restricted to WATCH by promotion policy"
        reject_reason = None
    elif strength >= 65 and pullback_quality == PULLBACK_HEALTHY and active_leg_boxes == 2 and breakout_context == BREAKOUT_POST_BULLISH_PULLBACK:
        status = STATUS_CANDIDATE
        reason = "Bullish pullback near support with acceptable close-confirmed risk profile"
        reject_reason = None
    elif strength >= 35:
        status = STATUS_WATCH
        reason = "Bullish setup exists but quality is not yet strong enough"
        reject_reason = None
    else:
        status = STATUS_REJECT
        reason = "Bullish setup quality is insufficient"
        reject_reason = "quality_too_low"

    return _base_result(
        symbol=symbol, side="LONG", status=status, reason=reason, reject_reason=reject_reason,
        breakout_context=breakout_context, zone_low=zone_low, zone_high=zone_high,
        ideal_entry=ideal_entry, invalidation=invalidation, risk=risk, tp1=tp1, tp2=tp2,
        rr1=rr1, rr2=rr2, pullback_quality=pullback_quality, risk_quality=risk_quality,
        reward_quality=reward_quality, quality_score=float(strength), quality_grade=grade,
        continuation_strength_v1=continuation_strength_v1,
        cs_geometry_component=cs_geometry_component,
        cs_profile_tag=cs_profile_tag,
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

    if trend not in ("BULLISH", "BEARISH"):
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Trend state unavailable", reject_reason="missing_trend_state", breakout_context=breakout_context)
    if support is None or resistance is None:
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Support / resistance unavailable", reject_reason="missing_structure_levels", breakout_context=breakout_context)
    if resistance <= support:
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Invalid structure range", reject_reason="invalid_structure_range", breakout_context=breakout_context)
    if current_kind != "O":
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Short reversal branch requires O column after failure", reject_reason="wrong_current_column_kind", breakout_context=breakout_context)
    if slope not in ("BEARISH_PULLBACK", "BEARISH"):
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Short reversal branch requires bearish downslope after failure", reject_reason="wrong_immediate_slope", breakout_context=breakout_context)

    bullish_failure_context = (
        trend == "BULLISH"
        or breakout_context in (BREAKOUT_POST_BULLISH_PULLBACK, BREAKOUT_LATE_EXTENSION, BREAKOUT_FRESH_BULLISH)
        or is_extended
        or market_state == "RANGE"
        or "TOP" in latest_signal_name.upper()
    )
    if not bullish_failure_context:
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_WATCH,
            reason="Short reversal candidate exists but no clear bullish failure context yet", reject_reason=None, breakout_context=breakout_context)

    if current_top is None:
        current_top = _col_top(current)

    current_bottom = _col_bottom(current)
    zone_high = current_top
    zone_low = max(support, current_bottom)
    if zone_low > zone_high:
        zone_low = zone_high

    ideal_entry = current_bottom
    invalidation = resistance + box
    risk = invalidation - ideal_entry

    if risk <= 0:
        return _base_result(symbol=symbol, side="SHORT", status=STATUS_REJECT,
            reason="Non-positive risk distance", reject_reason="non_positive_risk", breakout_context=breakout_context,
            zone_low=zone_low, zone_high=zone_high, ideal_entry=ideal_entry, invalidation=invalidation)

    tp1 = ideal_entry - 2.0 * risk
    tp2 = ideal_entry - 3.0 * risk
    rr1 = (ideal_entry - tp1) / risk
    rr2 = (ideal_entry - tp2) / risk

    rebound_position = _rebound_position_short(support, resistance, current_top)
    rebound_quality = _classify_short_rebound_quality(support, resistance, current_top)
    risk_quality = _classify_risk_quality(risk, profile)
    reward_quality = _classify_reward_quality(rr1, rr2)

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

    if rebound_quality == PULLBACK_BROKEN:
        status = STATUS_REJECT
        reason = "Failure structure is invalid; reversal short broke too far"
        reject_reason = "broken_reversal_context"
    elif reward_quality == REWARD_POOR:
        status = STATUS_REJECT
        reason = "Reward-to-risk below minimum threshold"
        reject_reason = "rr_too_low"
    elif active_leg_boxes < 2:
        status = STATUS_WATCH
        reason = "Short reversal exists but needs more mature failure structure"
        reject_reason = None
    elif rebound_quality == PULLBACK_SHALLOW:
        status = STATUS_WATCH
        reason = "Short reversal exists but failure pullback is still shallow"
        reject_reason = None
    elif risk_quality == RISK_WIDE:
        status = STATUS_WATCH
        reason = "Short reversal exists but risk is still wide"
        reject_reason = None
    elif strength >= 70 and active_leg_boxes in (2, 3):
        status = STATUS_CANDIDATE
        reason = "Failed bullish move / reversal short near resistance with acceptable risk profile"
        reject_reason = None
    elif strength >= 45:
        status = STATUS_WATCH
        reason = "Short reversal exists but quality is not yet strong enough"
        reject_reason = None
    else:
        status = STATUS_REJECT
        reason = "Short reversal quality is insufficient"
        reject_reason = "quality_too_low"

    return _base_result(
        symbol=symbol, side="SHORT", status=status, reason=reason, reject_reason=reject_reason,
        breakout_context=breakout_context, zone_low=zone_low, zone_high=zone_high,
        ideal_entry=ideal_entry, invalidation=invalidation, risk=risk, tp1=tp1, tp2=tp2,
        rr1=rr1, rr2=rr2, pullback_quality=rebound_quality, risk_quality=risk_quality,
        reward_quality=reward_quality, quality_score=float(strength), quality_grade=grade,
        continuation_strength_v1=None, cs_geometry_component=cs_geometry_component, cs_profile_tag=cs_profile_tag,
    )
