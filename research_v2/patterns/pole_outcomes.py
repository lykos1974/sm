from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from typing import Any


@dataclass
class CsvColumn:
    idx: int
    kind: str
    top: float
    bottom: float


def extract_box_size_from_profile_name(profile_name: str) -> float | None:
    match = re.search(r"_bs([0-9]+(?:\.[0-9]+)?)_rev", profile_name)
    if not match:
        return None
    return float(match.group(1))


def box_move(distance: float, box_size: float) -> int:
    if box_size <= 0:
        raise ValueError("box_size must be > 0")
    return max(0, int(round(distance / box_size)))


def load_columns_csv(path) -> tuple[list[CsvColumn], float | None]:
    columns: list[CsvColumn] = []
    inferred_box_size: float | None = None
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if inferred_box_size is None and row.get("profile_name"):
                inferred_box_size = extract_box_size_from_profile_name(row["profile_name"])
            columns.append(
                CsvColumn(
                    idx=int(row["idx"]),
                    kind=row["kind"].strip().upper(),
                    top=float(row["top"]),
                    bottom=float(row["bottom"]),
                )
            )
    return columns, inferred_box_size


def load_poles_csv(path) -> list[dict[str, Any]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _classify_outcome(pattern_name: str, events: list[tuple[int, str]]) -> str:
    if pattern_name == "HIGH_POLE":
        cont, fail = "BEARISH_CONTINUATION", "FAILED_REVERSAL"
    else:
        cont, fail = "BULLISH_CONTINUATION", "FAILED_REVERSAL"

    if not events:
        return "SIDEWAYS"
    events.sort(key=lambda e: e[0])
    if events[0][1] == "CONT":
        return cont
    if events[0][1] == "FAIL":
        return fail
    return "SIDEWAYS"


def label_pole_outcomes(
    poles: list[dict[str, Any]],
    columns: list[CsvColumn],
    *,
    box_size: float,
    future_columns: int,
    continuation_threshold_boxes: int,
    invalidation_threshold_boxes: int,
) -> list[dict[str, Any]]:
    by_idx = {c.idx: c for c in columns}
    max_idx = max(by_idx) if by_idx else -1
    out = []
    for pole in poles:
        row = dict(pole)
        pattern_name = row["pattern_name"].strip().upper()
        pole_idx = int(row["pole_column_index"])
        rev_idx = int(row["reversal_column_index"])
        pole_col = by_idx.get(pole_idx)
        rev_col = by_idx.get(rev_idx)
        if pole_col is None or rev_col is None:
            row["outcome_class"] = "INSUFFICIENT_DATA"
            row["future_columns_observed"] = 0
            out.append(row)
            continue

        future_idxs = list(range(rev_idx + 1, min(max_idx, rev_idx + future_columns) + 1))
        future = [by_idx[i] for i in future_idxs if i in by_idx]
        row["future_columns_observed"] = len(future)
        if len(future) == 0:
            row["outcome_class"] = "INSUFFICIENT_DATA"
            row["max_favorable_boxes"] = 0
            row["max_adverse_boxes"] = 0
            row["net_move_boxes"] = 0
            row["time_to_max_favorable"] = ""
            row["time_to_max_adverse"] = ""
            out.append(row)
            continue

        fav_series: list[int] = []
        adv_series: list[int] = []
        for col in future:
            if pattern_name == "HIGH_POLE":
                # Structural post-reversal anchors:
                # continuation is bearish expansion on O-columns from reversal bottom,
                # invalidation is bullish expansion on X-columns from reversal top.
                favorable = box_move(rev_col.bottom - col.bottom, box_size) if col.kind == "O" else 0
                adverse = box_move(col.top - rev_col.top, box_size) if col.kind == "X" else 0
            else:
                # LOW_POLE mirror:
                # continuation is bullish expansion on X-columns from reversal top,
                # invalidation is bearish expansion on O-columns from reversal bottom.
                favorable = box_move(col.top - rev_col.top, box_size) if col.kind == "X" else 0
                adverse = box_move(rev_col.bottom - col.bottom, box_size) if col.kind == "O" else 0
            fav_series.append(favorable)
            adv_series.append(adverse)

        max_fav = max(fav_series)
        max_adv = max(adv_series)
        t_fav = fav_series.index(max_fav) + 1
        t_adv = adv_series.index(max_adv) + 1

        events: list[tuple[int, str]] = []
        for i, (fav, adv) in enumerate(zip(fav_series, adv_series), start=1):
            # Strictly temporal classification: check thresholds in directional invalidation-first order
            # per pole type at each observed future column, then stop on first event.
            if pattern_name == "HIGH_POLE":
                if adv >= invalidation_threshold_boxes:
                    events.append((i, "FAIL"))
                    break
                if fav >= continuation_threshold_boxes:
                    events.append((i, "CONT"))
                    break
            else:
                if adv >= invalidation_threshold_boxes:
                    events.append((i, "FAIL"))
                    break
                if fav >= continuation_threshold_boxes:
                    events.append((i, "CONT"))
                    break

        row["max_favorable_boxes"] = max_fav
        row["max_adverse_boxes"] = max_adv
        row["net_move_boxes"] = max_fav - max_adv
        row["time_to_max_favorable"] = t_fav
        row["time_to_max_adverse"] = t_adv
        row["outcome_class"] = _classify_outcome(pattern_name, events)
        out.append(row)
    return out
