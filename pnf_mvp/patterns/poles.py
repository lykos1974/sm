from __future__ import annotations

from typing import Any, Dict, List, Optional


def _box_count(top: float, bottom: float, box_size: float) -> int:
    return int(round((top - bottom) / box_size)) + 1


def _price_to_boxes(distance: float, box_size: float) -> int:
    return int(round(distance / box_size))


def _previous_same(columns, idx: int, kind: str):
    for i in range(idx - 1, -1, -1):
        if columns[i].kind == kind:
            return columns[i]
    return None


def detect_pole_patterns(
    columns,
    box_size: float,
    min_breakout_excess_boxes: int = 3,
    min_pole_boxes_exclusive: int = 5,
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

        # High pole: X then O retrace > 50%
        if pole.kind == "X" and reversal.kind == "O":
            prev_x = _previous_same(columns, i - 1, "X")
            if prev_x is None:
                continue
            breakout_excess_boxes = _price_to_boxes(pole.top - prev_x.top, box_size)
            pole_boxes = _box_count(pole.top, pole.bottom, box_size)
            retrace_boxes = _price_to_boxes(pole.top - reversal.bottom, box_size)
            retrace_ratio = (retrace_boxes / pole_boxes) if pole_boxes > 0 else 0.0

            if pole_boxes <= min_pole_boxes_exclusive:
                continue
            if breakout_excess_boxes < min_breakout_excess_boxes:
                continue
            if retrace_ratio <= 0.5:
                continue

            out.append(
                {
                    "pattern_name": "HIGH_POLE",
                    "status": "EARLY_50_RETRACE",
                    "pole_column_index": pole.idx,
                    "reversal_column_index": reversal.idx,
                    "pole_boxes": pole_boxes,
                    "retrace_boxes": retrace_boxes,
                    "retrace_ratio": round(retrace_ratio, 4),
                    "breakout_excess_boxes": breakout_excess_boxes,
                    "direction_bias": "BEARISH",
                    "risk_note": "Diagnostic reversal warning after >50% retrace of X pole.",
                    "is_diagnostic_only": True,
                }
            )

        # Low pole: O then X retrace > 50%
        if pole.kind == "O" and reversal.kind == "X":
            prev_o = _previous_same(columns, i - 1, "O")
            if prev_o is None:
                continue
            breakout_excess_boxes = _price_to_boxes(prev_o.bottom - pole.bottom, box_size)
            pole_boxes = _box_count(pole.top, pole.bottom, box_size)
            retrace_boxes = _price_to_boxes(reversal.top - pole.bottom, box_size)
            retrace_ratio = (retrace_boxes / pole_boxes) if pole_boxes > 0 else 0.0

            if pole_boxes <= min_pole_boxes_exclusive:
                continue
            if breakout_excess_boxes < min_breakout_excess_boxes:
                continue
            if retrace_ratio <= 0.5:
                continue

            out.append(
                {
                    "pattern_name": "LOW_POLE",
                    "status": "EARLY_50_RETRACE",
                    "pole_column_index": pole.idx,
                    "reversal_column_index": reversal.idx,
                    "pole_boxes": pole_boxes,
                    "retrace_boxes": retrace_boxes,
                    "retrace_ratio": round(retrace_ratio, 4),
                    "breakout_excess_boxes": breakout_excess_boxes,
                    "direction_bias": "BULLISH",
                    "risk_note": "Diagnostic reversal warning after >50% retrace of O pole.",
                    "is_diagnostic_only": True,
                }
            )

    return out
