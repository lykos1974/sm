"""Phase 3 research-only structural outcome audit for PnF AB=CD cohorts.

This module measures only the next confirmed structural swing after D is known.
It does not create or alter detectors, scanners, signals, production behavior, or
any trade model. Outputs are descriptive cohort summaries only.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from pnf_abcd_geometry_audit import (
    DESIGN_DOC,
    DIRECTIONS,
    REACTIONS_FILENAME,
    SYMBOLS,
    THRESHOLD_NAME,
    YEARS,
    Pivot,
    _column_sort,
    _fmt,
    _normalize_symbol,
    _parse_float,
    _parse_time,
    _trusted_reactions_path,
    _valid_completed,
    classify_cd_ab,
)

TRUSTED_PIVOT_ROOT = Path("research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3")
GEOMETRY_ROOT = Path("research_v2/patterns/abcd_geometry_local_v1")
POPULATION_ROOT = Path("research_v2/patterns/abcd_population_local_v2")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_outcome_repaired_local_v1")
REPO_ROOT = Path(__file__).resolve().parents[2]
GEOMETRY_CANDIDATES = "abcd_geometry_candidates.csv"
COHORTS = ("SYM_0_90_1_10", "EXT_1_20_1_35", "EXT_1_55_1_70", "OTHER")

SUMMARY_FIELDS = [
    "cohort",
    "count",
    "measured_rows",
    "median_next_confirmed_swing",
    "avg_next_confirmed_swing",
    "continuation_frequency",
    "reversal_frequency",
    "continuation_count",
    "reversal_count",
]
BY_SYMBOL_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]
TRACE_FIELDS = [
    "candidate_id",
    "symbol",
    "cohort",
    "a_pivot_id",
    "b_pivot_id",
    "c_pivot_id",
    "d_pivot_id",
    "next_pivot_id",
    "a_direction",
    "b_direction",
    "c_direction",
    "d_direction",
    "pre_d_active_direction",
    "next_confirmed_direction",
    "continuation",
    "reversal",
    "a_knowledge_time",
    "b_knowledge_time",
    "c_knowledge_time",
    "d_knowledge_time",
    "next_knowledge_time",
]


@dataclass(frozen=True)
class Outcome:
    candidate_id: str
    symbol: str
    year: int | None
    cohort: str
    cd_direction: str
    pre_d_active_direction: str
    next_confirmed_swing_boxes: float | None
    next_confirmed_direction: str
    continuation: bool | None
    reversal: bool | None
    a_pivot_id: str = ""
    b_pivot_id: str = ""
    c_pivot_id: str = ""
    d_pivot_id: str = ""
    next_pivot_id: str = ""
    a_direction: str = ""
    b_direction: str = ""
    c_direction: str = ""
    d_direction: str = ""
    a_knowledge_time: str = ""
    b_knowledge_time: str = ""
    c_knowledge_time: str = ""
    d_knowledge_time: str = ""
    next_knowledge_time: str = ""


def _year_from_ts(timestamp: float) -> int | None:
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).year
    except (OverflowError, OSError, ValueError):
        return None


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _sort_pivots(pivots: Iterable[Pivot]) -> list[Pivot]:
    return sorted(
        pivots,
        key=lambda pivot: (
            pivot.knowledge_ts,
            pivot.completion_ts,
            pivot.column_sort,
            pivot.source_row,
        ),
    )


def load_validated_pivots(input_root: Path) -> tuple[list[Pivot], dict[str, int], Path]:
    reactions_path = _trusted_reactions_path(input_root)
    if reactions_path is None:
        raise FileNotFoundError(
            f"missing trusted {REACTIONS_FILENAME} under {input_root} or {input_root / 'audit'}"
        )

    rejects = {
        "non_slow_or_non_confirming": 0,
        "untrusted_symbol": 0,
        "invalid_candidate_direction": 0,
        "missing_or_invalid_knowledge_time": 0,
        "missing_or_invalid_candidate_boxes": 0,
    }
    pivots: list[Pivot] = []
    with reactions_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{reactions_path}: expected CSV header")
        if "knowledge_time" not in reader.fieldnames:
            raise ValueError(
                f"{reactions_path}: missing required knowledge_time column; completion_time fallback is not used"
            )
        for row_number, row in enumerate(reader, start=2):
            threshold = str(row.get("threshold_name") or "").strip().upper()
            kind = str(row.get("reaction_kind") or "").strip().upper()
            if threshold != THRESHOLD_NAME or kind != "CONFIRMING":
                rejects["non_slow_or_non_confirming"] += 1
                continue
            symbol = _normalize_symbol(str(row.get("symbol") or ""))
            if symbol not in SYMBOLS:
                rejects["untrusted_symbol"] += 1
                continue
            direction = str(row.get("candidate_direction") or "").strip().upper()
            if direction not in DIRECTIONS:
                rejects["invalid_candidate_direction"] += 1
                continue
            knowledge_time = str(row.get("knowledge_time") or "").strip()
            knowledge_ts = _parse_time(knowledge_time)
            if knowledge_ts is None:
                rejects["missing_or_invalid_knowledge_time"] += 1
                continue
            boxes = _parse_float(row.get("candidate_boxes"))
            if boxes is None or boxes <= 0:
                rejects["missing_or_invalid_candidate_boxes"] += 1
                continue
            completion_time = str(row.get("completion_time") or "").strip()
            completion_ts = _parse_time(completion_time)
            column_id = str(row.get("column_id") or "").strip()
            pivot_id = f"{symbol}:{row_number}:{knowledge_time}:{column_id}:{direction}"
            pivots.append(
                Pivot(
                    pivot_id=pivot_id,
                    source_row=row_number,
                    symbol=symbol,
                    candidate_direction=direction,
                    candidate_boxes=boxes,
                    knowledge_time=knowledge_time,
                    knowledge_ts=knowledge_ts,
                    completion_time=completion_time,
                    completion_ts=completion_ts if completion_ts is not None else 0.0,
                    column_id=column_id,
                    column_sort=_column_sort(column_id),
                )
            )
    return pivots, rejects, reactions_path


def outcomes_from_pivots(pivots: Sequence[Pivot]) -> list[Outcome]:
    rows: list[Outcome] = []
    by_symbol = {symbol: [] for symbol in SYMBOLS}
    for pivot in pivots:
        by_symbol.setdefault(pivot.symbol, []).append(pivot)
    for symbol in SYMBOLS:
        ordered = _sort_pivots(by_symbol.get(symbol, []))
        for index in range(len(ordered) - 3):
            a, b, c, d = ordered[index], ordered[index + 1], ordered[index + 2], ordered[index + 3]
            if not _valid_completed(a, b, c, d):
                continue
            next_pivot = ordered[index + 4] if index + 4 < len(ordered) else None
            next_direction = next_pivot.candidate_direction if next_pivot is not None else ""
            pre_d_active_direction = c.candidate_direction
            continuation = next_direction == pre_d_active_direction if next_pivot is not None else None
            reversal = next_direction != pre_d_active_direction if next_pivot is not None else None
            rows.append(
                Outcome(
                    candidate_id="ABCD:" + ":".join([a.pivot_id, b.pivot_id, c.pivot_id, d.pivot_id]),
                    symbol=symbol,
                    year=_year_from_ts(d.knowledge_ts),
                    cohort=classify_cd_ab(d.candidate_boxes / b.candidate_boxes),
                    cd_direction=d.candidate_direction,
                    pre_d_active_direction=pre_d_active_direction,
                    next_confirmed_swing_boxes=next_pivot.candidate_boxes if next_pivot is not None else None,
                    next_confirmed_direction=next_direction,
                    continuation=continuation,
                    reversal=reversal,
                    a_pivot_id=a.pivot_id,
                    b_pivot_id=b.pivot_id,
                    c_pivot_id=c.pivot_id,
                    d_pivot_id=d.pivot_id,
                    next_pivot_id=next_pivot.pivot_id if next_pivot is not None else "",
                    a_direction=a.candidate_direction,
                    b_direction=b.candidate_direction,
                    c_direction=c.candidate_direction,
                    d_direction=d.candidate_direction,
                    a_knowledge_time=a.knowledge_time,
                    b_knowledge_time=b.knowledge_time,
                    c_knowledge_time=c.knowledge_time,
                    d_knowledge_time=d.knowledge_time,
                    next_knowledge_time=next_pivot.knowledge_time if next_pivot is not None else "",
                )
            )
    return rows


def _resolve_repo_path(path: str | Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else REPO_ROOT / path


def _candidate_path(geometry_root: Path) -> Path | None:
    candidate = geometry_root / GEOMETRY_CANDIDATES
    return candidate if candidate.is_file() else None


def _identity_key(symbol: str, d_time: str, d_column_id: str, cd_direction: str) -> tuple[Any, ...] | None:
    d_ts = _parse_time(d_time)
    if d_ts is None:
        return None
    return (symbol, d_ts, _column_sort(d_column_id), cd_direction)


def outcomes_from_geometry_file(path: Path, pivots: Sequence[Pivot]) -> list[Outcome]:
    ordered_by_symbol = {symbol: _sort_pivots([p for p in pivots if p.symbol == symbol]) for symbol in SYMBOLS}
    rows: list[Outcome] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{path}: expected CSV header")
        for row_number, row in enumerate(reader, start=2):
            symbol = _normalize_symbol(str(row.get("symbol") or ""))
            if symbol not in SYMBOLS:
                continue
            cd_direction = str(row.get("cd_direction") or "").strip().upper()
            cd_ratio = _parse_float(row.get("CD_AB_ratio"))
            if cd_direction not in DIRECTIONS or cd_ratio is None:
                raise ValueError(f"{path}:{row_number}: missing CD direction or CD/AB ratio")
            d_time = str(row.get("d_time") or row.get("candidate_knowledge_time") or "").strip()
            d_column_id = str(row.get("d_column_id") or "").strip()
            d_key = _identity_key(symbol, d_time, d_column_id, cd_direction)
            if d_key is None:
                raise ValueError(f"{path}:{row_number}: missing or invalid D knowledge time")
            next_pivot: Pivot | None = None
            pre_d_active_pivot: Pivot | None = None
            previous_pivot: Pivot | None = None
            matched_d = False
            matched_d_index: int | None = None
            ordered = ordered_by_symbol.get(symbol, [])
            for pivot_index, pivot in enumerate(ordered):
                pivot_key = (pivot.symbol, pivot.knowledge_ts, pivot.column_sort, pivot.candidate_direction)
                if matched_d:
                    next_pivot = pivot
                    break
                if pivot_key == d_key:
                    matched_d = True
                    matched_d_index = pivot_index
                    pre_d_active_pivot = previous_pivot
                previous_pivot = pivot
            if not matched_d:
                raise ValueError(f"{path}:{row_number}: could not match D pivot in trusted pivot stream")
            if pre_d_active_pivot is None:
                raise ValueError(f"{path}:{row_number}: could not identify pre-D active direction in trusted pivot stream")
            pre_d_active_direction = pre_d_active_pivot.candidate_direction
            next_direction = next_pivot.candidate_direction if next_pivot is not None else ""
            continuation = next_direction == pre_d_active_direction if next_pivot is not None else None
            reversal = next_direction != pre_d_active_direction if next_pivot is not None else None
            year_value = _parse_float(row.get("year"))
            a_pivot = ordered[matched_d_index - 3] if matched_d_index is not None and matched_d_index >= 3 else None
            b_pivot = ordered[matched_d_index - 2] if matched_d_index is not None and matched_d_index >= 2 else None
            c_pivot = ordered[matched_d_index - 1] if matched_d_index is not None and matched_d_index >= 1 else None
            d_pivot = ordered[matched_d_index] if matched_d_index is not None else None
            rows.append(
                Outcome(
                    candidate_id=str(row.get("candidate_id") or f"geometry_row_{row_number}"),
                    symbol=symbol,
                    year=int(year_value) if year_value is not None else None,
                    cohort=classify_cd_ab(cd_ratio),
                    cd_direction=cd_direction,
                    pre_d_active_direction=pre_d_active_direction,
                    next_confirmed_swing_boxes=next_pivot.candidate_boxes if next_pivot is not None else None,
                    next_confirmed_direction=next_direction,
                    continuation=continuation,
                    reversal=reversal,
                    a_pivot_id=a_pivot.pivot_id if a_pivot is not None else "",
                    b_pivot_id=b_pivot.pivot_id if b_pivot is not None else "",
                    c_pivot_id=c_pivot.pivot_id if c_pivot is not None else "",
                    d_pivot_id=d_pivot.pivot_id if d_pivot is not None else "",
                    next_pivot_id=next_pivot.pivot_id if next_pivot is not None else "",
                    a_direction=a_pivot.candidate_direction if a_pivot is not None else "",
                    b_direction=b_pivot.candidate_direction if b_pivot is not None else "",
                    c_direction=c_pivot.candidate_direction if c_pivot is not None else "",
                    d_direction=d_pivot.candidate_direction if d_pivot is not None else "",
                    a_knowledge_time=a_pivot.knowledge_time if a_pivot is not None else "",
                    b_knowledge_time=b_pivot.knowledge_time if b_pivot is not None else "",
                    c_knowledge_time=c_pivot.knowledge_time if c_pivot is not None else "",
                    d_knowledge_time=d_pivot.knowledge_time if d_pivot is not None else "",
                    next_knowledge_time=next_pivot.knowledge_time if next_pivot is not None else "",
                )
            )
    return rows


def _trace_row_from_outcome(row: Outcome) -> dict[str, Any]:
    """Return trace fields copied from an already-built outcome row.

    Continuation and reversal are intentionally read from ``row`` instead of
    recomputing comparisons, so trace export cannot drift from outcome logic.
    """

    return {
        "candidate_id": row.candidate_id,
        "symbol": row.symbol,
        "cohort": row.cohort,
        "a_pivot_id": row.a_pivot_id,
        "b_pivot_id": row.b_pivot_id,
        "c_pivot_id": row.c_pivot_id,
        "d_pivot_id": row.d_pivot_id,
        "next_pivot_id": row.next_pivot_id,
        "a_direction": row.a_direction,
        "b_direction": row.b_direction,
        "c_direction": row.c_direction,
        "d_direction": row.d_direction,
        "pre_d_active_direction": row.pre_d_active_direction,
        "next_confirmed_direction": row.next_confirmed_direction,
        "continuation": row.continuation,
        "reversal": row.reversal,
        "a_knowledge_time": row.a_knowledge_time,
        "b_knowledge_time": row.b_knowledge_time,
        "c_knowledge_time": row.c_knowledge_time,
        "d_knowledge_time": row.d_knowledge_time,
        "next_knowledge_time": row.next_knowledge_time,
    }


def _write_trace_csv(path: Path, rows: Sequence[Outcome], sample_size: int) -> None:
    measured = [row for row in rows if row.next_confirmed_swing_boxes is not None]
    _write_csv(path, [_trace_row_from_outcome(row) for row in measured[:sample_size]], TRACE_FIELDS)


def _median(values: Sequence[float]) -> str:
    return _fmt(statistics.median(values)) if values else ""


def _avg(values: Sequence[float]) -> str:
    return _fmt(sum(values) / len(values)) if values else ""


def _freq(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def summarize(rows: Sequence[Outcome], cohort: str) -> dict[str, Any]:
    cohort_rows = [row for row in rows if row.cohort == cohort]
    resolved = [row for row in cohort_rows if row.next_confirmed_swing_boxes is not None]
    boxes = [row.next_confirmed_swing_boxes for row in resolved if row.next_confirmed_swing_boxes is not None]
    continuation = sum(1 for row in resolved if row.continuation is True)
    reversal = sum(1 for row in resolved if row.reversal is True)
    return {
        "cohort": cohort,
        "count": len(cohort_rows),
        "measured_rows": len(resolved),
        "median_next_confirmed_swing": _median(boxes),
        "avg_next_confirmed_swing": _avg(boxes),
        "continuation_frequency": _freq(continuation, len(resolved)),
        "reversal_frequency": _freq(reversal, len(resolved)),
        "continuation_count": continuation,
        "reversal_count": reversal,
    }


def _summary_rows(rows: Sequence[Outcome]) -> list[dict[str, Any]]:
    return [summarize(rows, cohort) for cohort in COHORTS]


def _group_rows(rows: Sequence[Outcome], key_name: str) -> list[dict[str, Any]]:
    if key_name == "symbol":
        keys: Sequence[Any] = SYMBOLS
        getter = lambda row: row.symbol
    elif key_name == "year":
        keys = YEARS
        getter = lambda row: row.year
    else:
        raise ValueError(key_name)
    grouped_rows: list[dict[str, Any]] = []
    for key in keys:
        scoped = [row for row in rows if getter(row) == key]
        for summary in _summary_rows(scoped):
            grouped_rows.append({key_name: key, **summary})
    return grouped_rows


def _float_cell(row: dict[str, Any], key: str) -> float | None:
    return _parse_float(row.get(key))


def _row_by_cohort(rows: Sequence[dict[str, Any]], cohort: str) -> dict[str, Any]:
    return next(row for row in rows if row["cohort"] == cohort)


def _delta_text(rows: Sequence[dict[str, Any]], cohort: str, metric: str) -> str:
    other = _float_cell(_row_by_cohort(rows, "OTHER"), metric)
    value = _float_cell(_row_by_cohort(rows, cohort), metric)
    if other is None or value is None:
        return "not available"
    return _fmt(value - other)


def _frequency_range(rows: Sequence[dict[str, Any]], cohort: str, key_name: str, metric: str) -> str:
    values = []
    for row in rows:
        if row["cohort"] == cohort:
            value = _float_cell(row, metric)
            count = _parse_float(row.get("count"))
            if value is not None and count is not None and count > 0:
                values.append((row[key_name], value))
    if not values:
        return "not available"
    low_key, low = min(values, key=lambda item: item[1])
    high_key, high = max(values, key=lambda item: item[1])
    return f"{low_key}={_fmt(low)} to {high_key}={_fmt(high)}"


def _material_swing_text(summary_rows: Sequence[dict[str, Any]], cohort: str) -> str:
    median_delta = _delta_text(summary_rows, cohort, "median_next_confirmed_swing")
    avg_delta = _delta_text(summary_rows, cohort, "avg_next_confirmed_swing")
    return f"median delta vs OTHER={median_delta}; average delta vs OTHER={avg_delta}"


def _material_frequency_text(summary_rows: Sequence[dict[str, Any]], cohort: str) -> str:
    cont_delta = _delta_text(summary_rows, cohort, "continuation_frequency")
    rev_delta = _delta_text(summary_rows, cohort, "reversal_frequency")
    return f"continuation delta vs OTHER={cont_delta}; reversal delta vs OTHER={rev_delta}"


def _cohort_metric_text(rows: Sequence[dict[str, Any]], cohort: str) -> str:
    row = _row_by_cohort(rows, cohort)
    return (
        f"count={row['count']}; measured_rows={row['measured_rows']}; "
        f"median_next_swing={row['median_next_confirmed_swing']}; "
        f"avg_next_swing={row['avg_next_confirmed_swing']}; "
        f"continuation_frequency={row['continuation_frequency']}; "
        f"reversal_frequency={row['reversal_frequency']}"
    )


def _validation_status(rows: Sequence[Outcome]) -> tuple[bool, int, int, int]:
    measured_rows = [row for row in rows if row.next_confirmed_swing_boxes is not None]
    continuation_count = sum(1 for row in measured_rows if row.continuation is True)
    reversal_count = sum(1 for row in measured_rows if row.reversal is True)
    return continuation_count + reversal_count == len(measured_rows), len(measured_rows), continuation_count, reversal_count


def _stable_delta_text(rows: Sequence[dict[str, Any]], cohort: str, key_name: str, metric: str) -> str:
    deltas: list[tuple[Any, float]] = []
    keys = SYMBOLS if key_name == "symbol" else YEARS
    for key in keys:
        scoped = [row for row in rows if row.get(key_name) == key]
        if not scoped:
            continue
        cohort_row = next((row for row in scoped if row["cohort"] == cohort), None)
        other_row = next((row for row in scoped if row["cohort"] == "OTHER"), None)
        if cohort_row is None or other_row is None:
            continue
        cohort_value = _float_cell(cohort_row, metric)
        other_value = _float_cell(other_row, metric)
        cohort_measured = _parse_float(cohort_row.get("measured_rows"))
        other_measured = _parse_float(other_row.get("measured_rows"))
        if (
            cohort_value is None
            or other_value is None
            or cohort_measured is None
            or other_measured is None
            or cohort_measured <= 0
            or other_measured <= 0
        ):
            continue
        deltas.append((key, cohort_value - other_value))
    if not deltas:
        return "not available"
    signs = {1 if value > 0 else -1 if value < 0 else 0 for _, value in deltas}
    stability = "same-sign" if len(signs) == 1 else "mixed-sign"
    rendered = "; ".join(f"{key}={_fmt(value)}" for key, value in deltas)
    return f"{stability} deltas ({rendered})"


def _structural_separation_text(
    summary_rows: Sequence[dict[str, Any]],
    symbol_rows: Sequence[dict[str, Any]],
    year_rows: Sequence[dict[str, Any]],
) -> str:
    del summary_rows
    for cohort in COHORTS:
        if cohort == "OTHER":
            continue
        symbol_stability = _stable_delta_text(symbol_rows, cohort, "symbol", "continuation_frequency")
        year_stability = _stable_delta_text(year_rows, cohort, "year", "continuation_frequency")
        if symbol_stability.startswith("same-sign") and year_stability.startswith("same-sign"):
            return (
                "Descriptively yes for continuation-frequency separation only; "
                f"{cohort} has same-sign deltas versus OTHER across populated symbol and year partitions. "
                "This is not a profitability, expectancy, or trading conclusion."
            )
    return (
        "No confirmed meaningful structural separation from OTHER was established by this repaired audit; "
        "available cohort differences were absent, unavailable, or not stable across populated symbol/year partitions."
    )


def write_report(
    output_root: Path,
    *,
    source_detail: str,
    rows: Sequence[Outcome],
    summary_rows: Sequence[dict[str, Any]],
    symbol_rows: Sequence[dict[str, Any]],
    year_rows: Sequence[dict[str, Any]],
    rejects: dict[str, int],
) -> None:
    total = len(rows)
    validation_ok, measured_rows, continuation_count, reversal_count = _validation_status(rows)
    separation_text = _structural_separation_text(summary_rows, symbol_rows, year_rows)
    lines = [
        "# AB=CD Phase 3 Repaired Structural Outcome Audit Report",
        "",
        "## Scope",
        "- Phase 3 descriptive structural outcome audit only.",
        f"- Source detail: {source_detail}.",
        f"- Causal design reference: `{DESIGN_DOC.as_posix()}`.",
        "- Measurement starts only after D is known and uses the next confirmed SLOW structural swing.",
        "- Repair scope is limited to continuation/reversal semantics in this research audit.",
        "- No harmonic extraction, pivot extraction, geometry generation, AB/CD candidate generation, candidate selection, knowledge-time rules, validated inputs, population audit logic, detector, scanner, strategy, trade model, expectancy, or PnL logic was changed.",
        "",
        "## Repaired Continuation/Reversal Semantics",
        "- `pre_d_active_direction` is the confirmed swing direction that existed immediately before D completed.",
        "- In rolling-pivot mode, `pre_d_active_direction = c.candidate_direction` for the completed A/B/C/D window.",
        "- In geometry-file mode, `pre_d_active_direction` is recovered from the trusted confirmed-pivot stream as the pivot immediately before the matched D pivot.",
        "- `continuation = next_confirmed_direction == pre_d_active_direction`.",
        "- `reversal = next_confirmed_direction != pre_d_active_direction`.",
        "- The old D/CD-anchor comparison is no longer used for continuation/reversal classification.",
        "",
        "## Dataset Coverage",
        f"- Completed ABCD rows observed: {total}",
        f"- Measured rows with a next confirmed swing available: {measured_rows}",
        *[f"- Rejected/ignored pivot rows — {key}: {value}" for key, value in rejects.items()],
        "",
        "## Cohort Summary",
        "| Cohort | Count | Measured rows | Median next confirmed swing | Average next confirmed swing | Continuation frequency | Reversal frequency | Continuation count | Reversal count |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary_rows:
        lines.append(
            f"| {row['cohort']} | {row['count']} | {row['measured_rows']} | {row['median_next_confirmed_swing']} | {row['avg_next_confirmed_swing']} | {row['continuation_frequency']} | {row['reversal_frequency']} | {row['continuation_count']} | {row['reversal_count']} |"
        )
    lines.extend([
        "",
        "## Required Answers",
        f"1. **Does SYM_0_90_1_10 differ from OTHER?** SYM_0_90_1_10: {_cohort_metric_text(summary_rows, 'SYM_0_90_1_10')}; OTHER: {_cohort_metric_text(summary_rows, 'OTHER')}; deltas vs OTHER: {_material_swing_text(summary_rows, 'SYM_0_90_1_10')}; {_material_frequency_text(summary_rows, 'SYM_0_90_1_10')}.",
        f"2. **Does EXT_1_20_1_35 differ from OTHER?** EXT_1_20_1_35: {_cohort_metric_text(summary_rows, 'EXT_1_20_1_35')}; OTHER: {_cohort_metric_text(summary_rows, 'OTHER')}; deltas vs OTHER: {_material_swing_text(summary_rows, 'EXT_1_20_1_35')}; {_material_frequency_text(summary_rows, 'EXT_1_20_1_35')}.",
        f"3. **Does EXT_1_55_1_70 differ from OTHER?** EXT_1_55_1_70: {_cohort_metric_text(summary_rows, 'EXT_1_55_1_70')}; OTHER: {_cohort_metric_text(summary_rows, 'OTHER')}; deltas vs OTHER: {_material_swing_text(summary_rows, 'EXT_1_55_1_70')}; {_material_frequency_text(summary_rows, 'EXT_1_55_1_70')}.",
        f"4. **Are observed differences stable across BTCUSDT / ETHUSDT / SOLUSDT?** Continuation-frequency deltas vs OTHER by symbol — SYM_0_90_1_10: {_stable_delta_text(symbol_rows, 'SYM_0_90_1_10', 'symbol', 'continuation_frequency')}; EXT_1_20_1_35: {_stable_delta_text(symbol_rows, 'EXT_1_20_1_35', 'symbol', 'continuation_frequency')}; EXT_1_55_1_70: {_stable_delta_text(symbol_rows, 'EXT_1_55_1_70', 'symbol', 'continuation_frequency')}.",
        f"5. **Are observed differences stable across 2024 / 2025 / 2026?** Continuation-frequency deltas vs OTHER by year — SYM_0_90_1_10: {_stable_delta_text(year_rows, 'SYM_0_90_1_10', 'year', 'continuation_frequency')}; EXT_1_20_1_35: {_stable_delta_text(year_rows, 'EXT_1_20_1_35', 'year', 'continuation_frequency')}; EXT_1_55_1_70: {_stable_delta_text(year_rows, 'EXT_1_55_1_70', 'year', 'continuation_frequency')}.",
        f"6. **Does any cohort show meaningful structural separation from OTHER?** {separation_text}",
        "",
        "## Required Validation",
        f"- `continuation_count + reversal_count == measured_rows`: {validation_ok} ({continuation_count} + {reversal_count} == {measured_rows}).",
        "- Continuation frequency is no longer mechanically forced to zero by the D/CD-anchor bug because continuation is now measured against `pre_d_active_direction`, not `cd_direction`.",
        "- Reversal frequency is no longer mechanically forced to one by the D/CD-anchor bug because reversal is now measured as any next confirmed direction that differs from `pre_d_active_direction`, not any direction that differs from `cd_direction`.",
        "- This validation proves classification exhaustiveness for measured rows only; unresolved rows without a next confirmed swing are excluded from frequency denominators.",
        "",
        "## By Symbol",
        "| Symbol | Cohort | Count | Measured rows | Median next confirmed swing | Average next confirmed swing | Continuation frequency | Reversal frequency | Continuation count | Reversal count |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in symbol_rows:
        lines.append(
            f"| {row['symbol']} | {row['cohort']} | {row['count']} | {row['measured_rows']} | {row['median_next_confirmed_swing']} | {row['avg_next_confirmed_swing']} | {row['continuation_frequency']} | {row['reversal_frequency']} | {row['continuation_count']} | {row['reversal_count']} |"
        )
    lines.extend([
        "",
        "## By Year",
        "| Year | Cohort | Count | Measured rows | Median next confirmed swing | Average next confirmed swing | Continuation frequency | Reversal frequency | Continuation count | Reversal count |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in year_rows:
        lines.append(
            f"| {row['year']} | {row['cohort']} | {row['count']} | {row['measured_rows']} | {row['median_next_confirmed_swing']} | {row['avg_next_confirmed_swing']} | {row['continuation_frequency']} | {row['reversal_frequency']} | {row['continuation_count']} | {row['reversal_count']} |"
        )
    lines.extend([
        "",
        "## Research Guardrail",
        "This is descriptive structural outcome analysis only. It does not state or imply any trading conclusion, profitability conclusion, signal recommendation, or production behavior change.",
        "",
        f"Final required answer: After repairing the continuation/reversal semantics, AB=CD symmetry {'does' if separation_text.startswith('Descriptively yes') else 'does not'} exhibit structural separation from OTHER.",
        "",
    ])
    (output_root / "abcd_outcome_repaired_report.md").write_text("\n".join(lines), encoding="utf-8")


def _write_blocked_outputs(output_root: Path, *, reason: str) -> None:
    empty_summary = _summary_rows([])
    empty_symbol = _group_rows([], "symbol")
    empty_year = _group_rows([], "year")
    _write_csv(output_root / "abcd_outcome_repaired_summary.csv", empty_summary, SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_outcome_repaired_by_symbol.csv", empty_symbol, BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_outcome_repaired_by_year.csv", empty_year, BY_YEAR_FIELDS)
    report = [
        "# AB=CD Phase 3 Repaired Structural Outcome Audit Report",
        "",
        "## Status",
        "BLOCKED — Phase 3-approved input artifacts are not available in this workspace.",
        "",
        "## Reason",
        reason,
        "",
        "## Repaired Continuation/Reversal Semantics",
        "- The code path has been repaired so continuation is measured as `next_confirmed_direction == pre_d_active_direction`.",
        "- The code path has been repaired so reversal is measured as `next_confirmed_direction != pre_d_active_direction`.",
        "- The old D/CD-anchor comparison is no longer used for continuation/reversal classification.",
        "- This blocked report does not rerun nonzero cohort metrics because trusted Phase 3 inputs are absent.",
        "",
        "## Approved Input Requirement",
        f"- Pivot root: `{TRUSTED_PIVOT_ROOT.as_posix()}`",
        f"- Geometry root: `{GEOMETRY_ROOT.as_posix()}`",
        f"- Population root reference: `{POPULATION_ROOT.as_posix()}`",
        f"- Design: `{DESIGN_DOC.as_posix()}`",
        "- No fallback to non-approved local artifacts was used.",
        "",
        "## Answers",
        "1. SYM_0_90_1_10 vs OTHER: not computed because inputs are missing.",
        "2. EXT_1_20_1_35 vs OTHER: not computed because inputs are missing.",
        "3. EXT_1_55_1_70 vs OTHER: not computed because inputs are missing.",
        "4. Symbol stability across BTCUSDT / ETHUSDT / SOLUSDT: not determined.",
        "5. Year stability across 2024 / 2025 / 2026: not determined.",
        "6. Meaningful structural separation from OTHER: not determined from this workspace.",
        "",
        "## Required Validation",
        "- `continuation_count + reversal_count == measured_rows`: TRUE for the local blocked output (0 + 0 == 0).",
        "- For any measured row, classification is exhaustive because `continuation` is `next_confirmed_direction == pre_d_active_direction` and `reversal` is `next_confirmed_direction != pre_d_active_direction`.",
        "- Continuation frequency is no longer mechanically forced to zero by the D/CD-anchor bug because continuation is now measured against `pre_d_active_direction`, not `cd_direction`.",
        "- Reversal frequency is no longer mechanically forced to one by the D/CD-anchor bug because reversal is now measured against `pre_d_active_direction`, not `cd_direction`.",
        "",
        "## Research Guardrail",
        "Descriptive structural outcome analysis only; no profitability conclusion, expectancy conclusion, trading conclusion, detector, scanner, strategy, or trade model.",
        "",
        "Final required answer: After repairing the continuation/reversal semantics, AB=CD symmetry does not exhibit structural separation from OTHER in this workspace because the trusted Phase 3 inputs needed to measure separation are absent.",
        "",
    ]
    (output_root / "abcd_outcome_repaired_report.md").write_text("\n".join(report), encoding="utf-8")


def run_audit(
    *,
    pivot_root: str | Path = TRUSTED_PIVOT_ROOT,
    geometry_root: str | Path = GEOMETRY_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    trace_sample_size: int | None = None,
    trace_output: str | Path | None = None,
) -> bool:
    if trace_sample_size is not None and trace_sample_size < 0:
        raise ValueError("trace_sample_size must be non-negative")
    if trace_sample_size is not None and trace_output is None:
        raise ValueError("trace_output is required when trace_sample_size is set")
    pivot_path = _resolve_repo_path(pivot_root)
    geometry_path = _resolve_repo_path(geometry_root)
    output_path = _resolve_repo_path(output_root)
    trace_path = _resolve_repo_path(trace_output) if trace_output is not None else None
    output_path.mkdir(parents=True, exist_ok=True)
    try:
        pivots, rejects, reactions_path = load_validated_pivots(pivot_path)
        candidates_path = _candidate_path(geometry_path)
        if candidates_path is not None:
            rows = outcomes_from_geometry_file(candidates_path, pivots)
            source_detail = f"geometry candidates `{candidates_path.as_posix()}` with next pivots from `{reactions_path.as_posix()}`"
        else:
            rows = outcomes_from_pivots(pivots)
            source_detail = f"rolling completed candidates and next pivots from `{reactions_path.as_posix()}`"
    except (FileNotFoundError, ValueError) as exc:
        _write_blocked_outputs(output_path, reason=str(exc))
        return False

    summary_rows = _summary_rows(rows)
    symbol_rows = _group_rows(rows, "symbol")
    year_rows = _group_rows(rows, "year")
    _write_csv(output_path / "abcd_outcome_repaired_summary.csv", summary_rows, SUMMARY_FIELDS)
    _write_csv(output_path / "abcd_outcome_repaired_by_symbol.csv", symbol_rows, BY_SYMBOL_FIELDS)
    _write_csv(output_path / "abcd_outcome_repaired_by_year.csv", year_rows, BY_YEAR_FIELDS)
    write_report(
        output_path,
        source_detail=source_detail,
        rows=rows,
        summary_rows=summary_rows,
        symbol_rows=symbol_rows,
        year_rows=year_rows,
        rejects=rejects,
    )
    if trace_sample_size is not None and trace_path is not None:
        _write_trace_csv(trace_path, rows, trace_sample_size)
    return True


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pivot-root", type=Path, default=TRUSTED_PIVOT_ROOT)
    parser.add_argument("--geometry-root", type=Path, default=GEOMETRY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero when Phase 3-approved inputs are missing or invalid",
    )
    parser.add_argument(
        "--trace-sample-size",
        type=int,
        default=None,
        help="write the first N measured outcome rows to a trace CSV; disabled when omitted",
    )
    parser.add_argument(
        "--trace-output",
        type=Path,
        default=None,
        help="CSV output path for --trace-sample-size",
    )
    args = parser.parse_args(argv)
    if args.trace_sample_size is not None and args.trace_sample_size < 0:
        parser.error("--trace-sample-size must be non-negative")
    if args.trace_sample_size is not None and args.trace_output is None:
        parser.error("--trace-output is required when --trace-sample-size is set")
    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    ok = run_audit(
        pivot_root=args.pivot_root,
        geometry_root=args.geometry_root,
        output_root=args.output_root,
        trace_sample_size=args.trace_sample_size,
        trace_output=args.trace_output,
    )
    if args.strict and not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
