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
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_outcome_local_v1")
REPO_ROOT = Path(__file__).resolve().parents[2]
GEOMETRY_CANDIDATES = "abcd_geometry_candidates.csv"
COHORTS = ("SYM_0_90_1_10", "EXT_1_20_1_35", "EXT_1_55_1_70", "OTHER")

SUMMARY_FIELDS = [
    "cohort",
    "count",
    "median_next_confirmed_swing",
    "avg_next_confirmed_swing",
    "continuation_frequency",
    "reversal_frequency",
]
BY_SYMBOL_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]


@dataclass(frozen=True)
class Outcome:
    candidate_id: str
    symbol: str
    year: int | None
    cohort: str
    cd_direction: str
    next_confirmed_swing_boxes: float | None
    next_confirmed_direction: str
    same_direction_as_cd: bool | None
    opposite_direction_to_cd: bool | None


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
            same = next_direction == d.candidate_direction if next_pivot is not None else None
            opposite = next_direction != d.candidate_direction if next_pivot is not None else None
            rows.append(
                Outcome(
                    candidate_id="ABCD:" + ":".join([a.pivot_id, b.pivot_id, c.pivot_id, d.pivot_id]),
                    symbol=symbol,
                    year=_year_from_ts(d.knowledge_ts),
                    cohort=classify_cd_ab(d.candidate_boxes / b.candidate_boxes),
                    cd_direction=d.candidate_direction,
                    next_confirmed_swing_boxes=next_pivot.candidate_boxes if next_pivot is not None else None,
                    next_confirmed_direction=next_direction,
                    same_direction_as_cd=same,
                    opposite_direction_to_cd=opposite,
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
            matched_d = False
            for pivot in ordered_by_symbol.get(symbol, []):
                pivot_key = (pivot.symbol, pivot.knowledge_ts, pivot.column_sort, pivot.candidate_direction)
                if matched_d:
                    next_pivot = pivot
                    break
                if pivot_key == d_key:
                    matched_d = True
            if not matched_d:
                raise ValueError(f"{path}:{row_number}: could not match D pivot in trusted pivot stream")
            next_direction = next_pivot.candidate_direction if next_pivot is not None else ""
            same = next_direction == cd_direction if next_pivot is not None else None
            opposite = next_direction != cd_direction if next_pivot is not None else None
            year_value = _parse_float(row.get("year"))
            rows.append(
                Outcome(
                    candidate_id=str(row.get("candidate_id") or f"geometry_row_{row_number}"),
                    symbol=symbol,
                    year=int(year_value) if year_value is not None else None,
                    cohort=classify_cd_ab(cd_ratio),
                    cd_direction=cd_direction,
                    next_confirmed_swing_boxes=next_pivot.candidate_boxes if next_pivot is not None else None,
                    next_confirmed_direction=next_direction,
                    same_direction_as_cd=same,
                    opposite_direction_to_cd=opposite,
                )
            )
    return rows


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
    continuation = sum(1 for row in resolved if row.same_direction_as_cd is True)
    reversal = sum(1 for row in resolved if row.opposite_direction_to_cd is True)
    return {
        "cohort": cohort,
        "count": len(cohort_rows),
        "median_next_confirmed_swing": _median(boxes),
        "avg_next_confirmed_swing": _avg(boxes),
        "continuation_frequency": _freq(continuation, len(resolved)),
        "reversal_frequency": _freq(reversal, len(resolved)),
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
    resolved = sum(1 for row in rows if row.next_confirmed_swing_boxes is not None)
    lines = [
        "# AB=CD Phase 3 Structural Outcome Audit Report",
        "",
        "## Scope",
        "- Phase 3 descriptive structural outcome audit only.",
        f"- Source detail: {source_detail}.",
        f"- Causal design reference: `{DESIGN_DOC.as_posix()}`.",
        "- Measurement starts only after D is known and uses the next confirmed SLOW structural swing.",
        "- No trade model, signal generation, detector changes, scanner changes, or production-code behavior changes were performed.",
        "",
        "## Dataset Coverage",
        f"- Completed ABCD rows measured: {total}",
        f"- Rows with a next confirmed swing available: {resolved}",
        *[f"- Rejected/ignored pivot rows — {key}: {value}" for key, value in rejects.items()],
        "",
        "## Cohort Summary",
        "| Cohort | Count | Median next confirmed swing | Average next confirmed swing | Continuation frequency | Reversal frequency |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in summary_rows:
        lines.append(
            f"| {row['cohort']} | {row['count']} | {row['median_next_confirmed_swing']} | {row['avg_next_confirmed_swing']} | {row['continuation_frequency']} | {row['reversal_frequency']} |"
        )
    lines.extend([
        "",
        "## Required Answers",
        f"1. **Do symmetry-zone ABCDs produce different future swing sizes than OTHER?** {_material_swing_text(summary_rows, 'SYM_0_90_1_10')}.",
        f"2. **Do extension-zone ABCDs produce different future swing sizes than OTHER?** EXT_1_20_1_35: {_material_swing_text(summary_rows, 'EXT_1_20_1_35')}; EXT_1_55_1_70: {_material_swing_text(summary_rows, 'EXT_1_55_1_70')}.",
        f"3. **Do any cohorts show materially different continuation/reversal frequencies?** SYM_0_90_1_10: {_material_frequency_text(summary_rows, 'SYM_0_90_1_10')}; EXT_1_20_1_35: {_material_frequency_text(summary_rows, 'EXT_1_20_1_35')}; EXT_1_55_1_70: {_material_frequency_text(summary_rows, 'EXT_1_55_1_70')}.",
        f"4. **Are differences stable across BTCUSDT / ETHUSDT / SOLUSDT?** Continuation-frequency ranges by symbol — SYM_0_90_1_10 {_frequency_range(symbol_rows, 'SYM_0_90_1_10', 'symbol', 'continuation_frequency')}; EXT_1_20_1_35 {_frequency_range(symbol_rows, 'EXT_1_20_1_35', 'symbol', 'continuation_frequency')}; EXT_1_55_1_70 {_frequency_range(symbol_rows, 'EXT_1_55_1_70', 'symbol', 'continuation_frequency')}; OTHER {_frequency_range(symbol_rows, 'OTHER', 'symbol', 'continuation_frequency')}.",
        f"5. **Are differences stable across 2024 / 2025 / 2026?** Continuation-frequency ranges by year — SYM_0_90_1_10 {_frequency_range(year_rows, 'SYM_0_90_1_10', 'year', 'continuation_frequency')}; EXT_1_20_1_35 {_frequency_range(year_rows, 'EXT_1_20_1_35', 'year', 'continuation_frequency')}; EXT_1_55_1_70 {_frequency_range(year_rows, 'EXT_1_55_1_70', 'year', 'continuation_frequency')}; OTHER {_frequency_range(year_rows, 'OTHER', 'year', 'continuation_frequency')}.",
        "6. **Is there sufficient descriptive separation to justify Phase 4 research?** Treat as descriptive only: Phase 4 is justified only if the deltas above remain separated after symbol/year review; this report does not make any profitability or recommendation claim.",
        "",
        "## By Symbol",
        "| Symbol | Cohort | Count | Median next confirmed swing | Average next confirmed swing | Continuation frequency | Reversal frequency |",
        "|---|---|---:|---:|---:|---:|---:|",
    ])
    for row in symbol_rows:
        lines.append(
            f"| {row['symbol']} | {row['cohort']} | {row['count']} | {row['median_next_confirmed_swing']} | {row['avg_next_confirmed_swing']} | {row['continuation_frequency']} | {row['reversal_frequency']} |"
        )
    lines.extend([
        "",
        "## By Year",
        "| Year | Cohort | Count | Median next confirmed swing | Average next confirmed swing | Continuation frequency | Reversal frequency |",
        "|---|---|---:|---:|---:|---:|---:|",
    ])
    for row in year_rows:
        lines.append(
            f"| {row['year']} | {row['cohort']} | {row['count']} | {row['median_next_confirmed_swing']} | {row['avg_next_confirmed_swing']} | {row['continuation_frequency']} | {row['reversal_frequency']} |"
        )
    lines.extend([
        "",
        "## Research Guardrail",
        "This is descriptive structural outcome analysis only. It does not state or imply any trading conclusion, profitability conclusion, signal recommendation, or production behavior change.",
        "",
    ])
    (output_root / "abcd_outcome_report.md").write_text("\n".join(lines), encoding="utf-8")


def _write_blocked_outputs(output_root: Path, *, reason: str) -> None:
    empty_summary = _summary_rows([])
    empty_symbol = _group_rows([], "symbol")
    empty_year = _group_rows([], "year")
    _write_csv(output_root / "abcd_outcome_summary.csv", empty_summary, SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_outcome_by_symbol.csv", empty_symbol, BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_outcome_by_year.csv", empty_year, BY_YEAR_FIELDS)
    report = [
        "# AB=CD Phase 3 Structural Outcome Audit Report",
        "",
        "## Status",
        "BLOCKED — Phase 3-approved input artifacts are not available in this workspace.",
        "",
        "## Reason",
        reason,
        "",
        "## Approved Input Requirement",
        f"- Pivot root: `{TRUSTED_PIVOT_ROOT.as_posix()}`",
        f"- Geometry root: `{GEOMETRY_ROOT.as_posix()}`",
        f"- Population root reference: `{POPULATION_ROOT.as_posix()}`",
        f"- Design: `{DESIGN_DOC.as_posix()}`",
        "- No fallback to non-approved local artifacts was used.",
        "",
        "## Answers",
        "1. Symmetry-zone future swing-size difference vs OTHER: not computed because inputs are missing.",
        "2. Extension-zone future swing-size difference vs OTHER: not computed because inputs are missing.",
        "3. Continuation/reversal frequency differences: not computed because inputs are missing.",
        "4. Symbol stability: not determined.",
        "5. Year stability: not determined.",
        "6. Phase 4 descriptive-separation justification: not determined from this workspace.",
        "",
        "## Research Guardrail",
        "Descriptive structural outcome analysis only; no profitability conclusion and no recommendation.",
        "",
    ]
    (output_root / "abcd_outcome_report.md").write_text("\n".join(report), encoding="utf-8")


def run_audit(
    *,
    pivot_root: str | Path = TRUSTED_PIVOT_ROOT,
    geometry_root: str | Path = GEOMETRY_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> bool:
    pivot_path = _resolve_repo_path(pivot_root)
    geometry_path = _resolve_repo_path(geometry_root)
    output_path = _resolve_repo_path(output_root)
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
    _write_csv(output_path / "abcd_outcome_summary.csv", summary_rows, SUMMARY_FIELDS)
    _write_csv(output_path / "abcd_outcome_by_symbol.csv", symbol_rows, BY_SYMBOL_FIELDS)
    _write_csv(output_path / "abcd_outcome_by_year.csv", year_rows, BY_YEAR_FIELDS)
    write_report(
        output_path,
        source_detail=source_detail,
        rows=rows,
        summary_rows=summary_rows,
        symbol_rows=symbol_rows,
        year_rows=year_rows,
        rejects=rejects,
    )
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
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    ok = run_audit(
        pivot_root=args.pivot_root,
        geometry_root=args.geometry_root,
        output_root=args.output_root,
    )
    if args.strict and not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
