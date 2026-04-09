from typing import Optional


def previous_same_type_column(columns, kind: str):
    """
    Return the previous column of the same type, excluding the current last column.
    """
    if len(columns) < 2:
        return None

    for i in range(len(columns) - 2, -1, -1):
        if columns[i].kind == kind:
            return columns[i]

    return None


def detect_double_top_breakout_signal(columns, emitted_signal_keys, signal_ts: int):
    if not columns:
        return None

    c = columns[-1]
    if c.kind != "X":
        return None

    prev_x = previous_same_type_column(columns, "X")
    if prev_x is None:
        return None

    if c.top > prev_x.top:
        key = ("DOUBLE_TOP_BREAKOUT", c.idx)
        if key in emitted_signal_keys:
            return None

        emitted_signal_keys.add(key)
        return {
            "type": "DOUBLE_TOP_BREAKOUT",
            "trigger": c.top,
            "column_idx": c.idx,
            "previous_same_type_idx": prev_x.idx,
            "comparison_level": prev_x.top,
            "note": f"X column {c.idx} exceeded previous X high {prev_x.top:.2f}",
            "timestamp": signal_ts,
        }

    return None


def detect_double_bottom_breakdown_signal(columns, emitted_signal_keys, signal_ts: int):
    if not columns:
        return None

    c = columns[-1]
    if c.kind != "O":
        return None

    prev_o = previous_same_type_column(columns, "O")
    if prev_o is None:
        return None

    if c.bottom < prev_o.bottom:
        key = ("DOUBLE_BOTTOM_BREAKDOWN", c.idx)
        if key in emitted_signal_keys:
            return None

        emitted_signal_keys.add(key)
        return {
            "type": "DOUBLE_BOTTOM_BREAKDOWN",
            "trigger": c.bottom,
            "column_idx": c.idx,
            "previous_same_type_idx": prev_o.idx,
            "comparison_level": prev_o.bottom,
            "note": f"O column {c.idx} broke previous O low {prev_o.bottom:.2f}",
            "timestamp": signal_ts,
        }

    return None


def has_double_top_breakout(columns) -> bool:
    if not columns:
        return False

    c = columns[-1]
    if c.kind != "X":
        return False

    prev_x = previous_same_type_column(columns, "X")
    return prev_x is not None and c.top > prev_x.top


def has_double_bottom_breakdown(columns) -> bool:
    if not columns:
        return False

    c = columns[-1]
    if c.kind != "O":
        return False

    prev_o = previous_same_type_column(columns, "O")
    return prev_o is not None and c.bottom < prev_o.bottom
