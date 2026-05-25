from __future__ import annotations

from typing import Any, Dict, List


def _box_count(top: float, bottom: float, box_size: float) -> int:
    return int(round((top - bottom) / box_size)) + 1


def _previous_same(columns, idx: int, kind: str):
    for i in range(idx - 1, -1, -1):
        if columns[i].kind == kind:
            return columns[i]
    return None


def _classify_status(retrace_ratio: float, max_early_retrace_ratio: float) -> str:
    if retrace_ratio > max_early_retrace_ratio:
        return "OVERRETRACE_POLE"
    return "EARLY_50_RETRACE"


def _base_pattern(
    *,
    pattern_name: str,
    pole,
    reversal,
    pole_boxes: int,
    retrace_boxes: int,
    retrace_ratio: float,
    breakout_excess_boxes: int,
    direction_bias: str,
    max_early_retrace_ratio: float,
) -> Dict[str, Any]:
    return {
        "pattern_name": pattern_name,
        "status": _classify_status(retrace_ratio, max_early_retrace_ratio),
        "pole_column_index": pole.idx,
        "reversal_column_index": reversal.idx,
        "pole_boxes": pole_boxes,
        "retrace_boxes": retrace_boxes,
        "retrace_ratio": round(retrace_ratio, 4),
        "breakout_excess_boxes": breakout_excess_boxes,
        "direction_bias": direction_bias,
        "risk_note": "Diagnostic reversal warning after >50% retrace of pole column.",
        "is_diagnostic_only": True,
        "opposing_pole_nearby": False,
        "opposing_pole_role": "NONE",
        "opposing_pole_partner_index": None,
        "opposing_pole_distance_columns": None,
        "enhanced_by_opposing_pole": False,
    }


def _mark_opposing_poles(patterns: List[Dict[str, Any]], max_distance_columns: int) -> None:
    for i, first in enumerate(patterns):
        for second in patterns[i + 1 :]:
            if first["pattern_name"] == second["pattern_name"]:
                continue
            distance = second["pole_column_index"] - first["pole_column_index"]
            if distance <= 0 or distance > max_distance_columns:
                continue
            first["opposing_pole_nearby"] = True
            if first["opposing_pole_role"] == "NONE":
                first["opposing_pole_role"] = "FIRST_POLE"
                first["opposing_pole_partner_index"] = second["pole_column_index"]
                first["opposing_pole_distance_columns"] = distance

            second["opposing_pole_nearby"] = True
            second["opposing_pole_role"] = "SECOND_POLE"
            second["opposing_pole_partner_index"] = first["pole_column_index"]
            second["opposing_pole_distance_columns"] = distance
            second["enhanced_by_opposing_pole"] = True
            break


def detect_pole_patterns(
    columns,
    box_size: float,
    min_breakout_excess_boxes: int = 3,
    min_pole_boxes_exclusive: int = 5,
    max_early_retrace_ratio: float = 1.0,
    max_opposing_distance_columns: int = 4,
) -> List[Dict[str, Any]]:
    """Return diagnostic pole patterns from completed adjacent columns only."""
    out: List[Dict[str, Any]] = []
    if len(columns) < 2:
        return out

    for i in range(1, len(columns)):
        pole = columns[i - 1]
        reversal = columns[i]
        if reversal.idx != pole.idx + 1:
            continue

        pole_boxes = _box_count(pole.top, pole.bottom, box_size)
        retrace_boxes = _box_count(reversal.top, reversal.bottom, box_size)
        retrace_ratio = (retrace_boxes / pole_boxes) if pole_boxes > 0 else 0.0

        if pole.kind == "X" and reversal.kind == "O":
            prev_x = _previous_same(columns, i - 1, "X")
            if prev_x is None:
                continue
            breakout_excess_boxes = _box_count(pole.top, prev_x.top, box_size) - 1
            if pole_boxes <= min_pole_boxes_exclusive:
                continue
            if breakout_excess_boxes < min_breakout_excess_boxes:
                continue
            if retrace_ratio <= 0.5:
                continue

            out.append(
                _base_pattern(
                    pattern_name="HIGH_POLE",
                    pole=pole,
                    reversal=reversal,
                    pole_boxes=pole_boxes,
                    retrace_boxes=retrace_boxes,
                    retrace_ratio=retrace_ratio,
                    breakout_excess_boxes=breakout_excess_boxes,
                    direction_bias="BEARISH",
                    max_early_retrace_ratio=max_early_retrace_ratio,
                )
            )

        if pole.kind == "O" and reversal.kind == "X":
            prev_o = _previous_same(columns, i - 1, "O")
            if prev_o is None:
                continue
            breakout_excess_boxes = _box_count(prev_o.bottom, pole.bottom, box_size) - 1
            if pole_boxes <= min_pole_boxes_exclusive:
                continue
            if breakout_excess_boxes < min_breakout_excess_boxes:
                continue
            if retrace_ratio <= 0.5:
                continue

            out.append(
                _base_pattern(
                    pattern_name="LOW_POLE",
                    pole=pole,
                    reversal=reversal,
                    pole_boxes=pole_boxes,
                    retrace_boxes=retrace_boxes,
                    retrace_ratio=retrace_ratio,
                    breakout_excess_boxes=breakout_excess_boxes,
                    direction_bias="BULLISH",
                    max_early_retrace_ratio=max_early_retrace_ratio,
                )
            )

    _mark_opposing_poles(out, max_opposing_distance_columns)
    return out
