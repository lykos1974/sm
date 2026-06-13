"""Research-only audit of AB=CD Phase 3 next-pivot outcome distance.

This module does not modify outcome logic, geometry logic, continuation/reversal
semantics, or any trading model. It measures whether the repaired Phase 3
"next confirmed pivot" outcome is structurally distant from D or mechanically the
immediately following confirmed pivot.
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

sys.path.append(str(Path(__file__).resolve().parent))

from pnf_abcd_geometry_audit import (
    DIRECTIONS,
    SYMBOLS,
    YEARS,
    _column_sort,
    _fmt,
    _normalize_symbol,
    _parse_float,
    _parse_time,
    classify_cd_ab,
)
from pnf_abcd_outcome_audit import (
    DEFAULT_OUTPUT_ROOT as REPAIRED_OUTCOME_ROOT,
    GEOMETRY_CANDIDATES,
    GEOMETRY_ROOT,
    TRUSTED_PIVOT_ROOT,
    _candidate_path,
    _resolve_repo_path,
    _sort_pivots,
    load_validated_pivots,
)

DEFAULT_REPAIRED_OUTCOME_ROOT = Path("research_v2/patterns/abcd_outcome_repaired_local_v2")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_outcome_distance_local_v1")
COHORTS = ("ALL", "SYM_0_90_1_10", "EXT_1_20_1_35", "EXT_1_55_1_70", "OTHER")

DISTANCE_FIELDS = [
    "scope",
    "cohort",
    "count",
    "median_column_distance",
    "avg_column_distance",
    "p25_column_distance",
    "p75_column_distance",
    "p90_column_distance",
    "pct_column_distance_1",
    "pct_column_distance_lte_2",
    "pct_column_distance_lte_3",
    "median_time_distance_ms",
    "avg_time_distance_ms",
    "p90_time_distance_ms",
]
BY_SYMBOL_FIELDS = ["symbol", *DISTANCE_FIELDS]
BY_YEAR_FIELDS = ["year", *DISTANCE_FIELDS]
SAMPLE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "cohort",
    "d_column_id",
    "next_column_id",
    "column_distance",
    "d_knowledge_time",
    "next_knowledge_time",
    "time_distance_ms",
    "d_direction",
    "next_confirmed_direction",
    "pre_d_active_direction",
    "continuation",
    "reversal",
]


@dataclass(frozen=True)
class DistanceRow:
    candidate_id: str
    symbol: str
    year: int | None
    cohort: str
    d_column_id: str
    next_column_id: str
    column_distance: int
    d_knowledge_time: str
    next_knowledge_time: str
    time_distance_ms: int
    d_direction: str
    next_confirmed_direction: str
    pre_d_active_direction: str
    continuation: bool
    reversal: bool


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _quantile(values: Sequence[float], pct: float) -> str:
    if not values:
        return ""
    ordered = sorted(values)
    if len(ordered) == 1:
        return _fmt(ordered[0])
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return _fmt(ordered[lower] * (1 - weight) + ordered[upper] * weight)


def _median(values: Sequence[float]) -> str:
    return _fmt(statistics.median(values)) if values else ""


def _avg(values: Sequence[float]) -> str:
    return _fmt(sum(values) / len(values)) if values else ""


def _pct(predicate_count: int, total: int) -> str:
    return _fmt(predicate_count / total) if total else ""


def _identity_key(symbol: str, d_time: str, d_column_id: str, cd_direction: str) -> tuple[Any, ...] | None:
    d_ts = _parse_time(d_time)
    if d_ts is None:
        return None
    return (symbol, d_ts, _column_sort(d_column_id), cd_direction)


def distance_rows_from_geometry(path: Path, pivots: Sequence[Any]) -> list[DistanceRow]:
    ordered_by_symbol = {symbol: _sort_pivots([p for p in pivots if p.symbol == symbol]) for symbol in SYMBOLS}
    rows: list[DistanceRow] = []
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
            previous_pivot = None
            matched_d = None
            next_pivot = None
            ordered = ordered_by_symbol.get(symbol, [])
            for pivot in ordered:
                pivot_key = (pivot.symbol, pivot.knowledge_ts, pivot.column_sort, pivot.candidate_direction)
                if matched_d is not None:
                    next_pivot = pivot
                    break
                if pivot_key == d_key:
                    matched_d = pivot
                    pre_d_active_pivot = previous_pivot
                previous_pivot = pivot
            if matched_d is None:
                raise ValueError(f"{path}:{row_number}: could not match D pivot in trusted pivot stream")
            if next_pivot is None:
                continue
            if pre_d_active_pivot is None:
                raise ValueError(f"{path}:{row_number}: could not identify pre-D active direction")
            d_ts = _parse_time(matched_d.knowledge_time)
            next_ts = _parse_time(next_pivot.knowledge_time)
            if d_ts is None or next_ts is None:
                raise ValueError(f"{path}:{row_number}: invalid D or next knowledge time")
            year_value = _parse_float(row.get("year"))
            pre_d_active_direction = pre_d_active_pivot.candidate_direction
            next_direction = next_pivot.candidate_direction
            rows.append(
                DistanceRow(
                    candidate_id=str(row.get("candidate_id") or f"geometry_row_{row_number}"),
                    symbol=symbol,
                    year=int(year_value) if year_value is not None else None,
                    cohort=classify_cd_ab(cd_ratio),
                    d_column_id=matched_d.column_id,
                    next_column_id=next_pivot.column_id,
                    column_distance=next_pivot.column_sort - matched_d.column_sort,
                    d_knowledge_time=matched_d.knowledge_time,
                    next_knowledge_time=next_pivot.knowledge_time,
                    time_distance_ms=int(round((next_ts - d_ts) * 1000)),
                    d_direction=matched_d.candidate_direction,
                    next_confirmed_direction=next_direction,
                    pre_d_active_direction=pre_d_active_direction,
                    continuation=next_direction == pre_d_active_direction,
                    reversal=next_direction != pre_d_active_direction,
                )
            )
    return rows


def _sample_row(row: DistanceRow) -> dict[str, Any]:
    return {field: getattr(row, field) for field in SAMPLE_FIELDS}


def summarize(rows: Sequence[DistanceRow], cohort: str, scope: str) -> dict[str, Any]:
    scoped = list(rows) if cohort == "ALL" else [row for row in rows if row.cohort == cohort]
    columns = [row.column_distance for row in scoped]
    times = [row.time_distance_ms for row in scoped]
    return {
        "scope": scope,
        "cohort": cohort,
        "count": len(scoped),
        "median_column_distance": _median(columns),
        "avg_column_distance": _avg(columns),
        "p25_column_distance": _quantile(columns, 0.25),
        "p75_column_distance": _quantile(columns, 0.75),
        "p90_column_distance": _quantile(columns, 0.90),
        "pct_column_distance_1": _pct(sum(1 for value in columns if value == 1), len(columns)),
        "pct_column_distance_lte_2": _pct(sum(1 for value in columns if value <= 2), len(columns)),
        "pct_column_distance_lte_3": _pct(sum(1 for value in columns if value <= 3), len(columns)),
        "median_time_distance_ms": _median(times),
        "avg_time_distance_ms": _avg(times),
        "p90_time_distance_ms": _quantile(times, 0.90),
    }


def _summary_rows(rows: Sequence[DistanceRow], scope: str = "all") -> list[dict[str, Any]]:
    return [summarize(rows, cohort, scope) for cohort in COHORTS]


def _group_rows(rows: Sequence[DistanceRow], key_name: str) -> list[dict[str, Any]]:
    keys = SYMBOLS if key_name == "symbol" else YEARS
    result = []
    for key in keys:
        scoped = [row for row in rows if getattr(row, key_name) == key]
        for summary in _summary_rows(scoped, scope=str(key)):
            result.append({key_name: key, **summary})
    return result


def _float_cell(row: dict[str, Any], key: str) -> float | None:
    return _parse_float(row.get(key))


def write_report(output_root: Path, rows: Sequence[DistanceRow], source_detail: str) -> None:
    summary_rows = _summary_rows(rows)
    all_row = next(row for row in summary_rows if row["cohort"] == "ALL")
    pct_one = _float_cell(all_row, "pct_column_distance_1") or 0.0
    pct_lte3 = _float_cell(all_row, "pct_column_distance_lte_3") or 0.0
    median_distance = _float_cell(all_row, "median_column_distance")
    tautological = pct_one >= 0.95 and median_distance == 1
    close_mechanical = pct_lte3 >= 0.95
    lines = [
        "# AB=CD Phase 3 Outcome Distance Audit Report",
        "",
        "## Scope",
        "- Research-only audit of the distance from D to the currently used next confirmed pivot outcome.",
        f"- Source detail: {source_detail}.",
        "- No outcome logic, geometry logic, continuation/reversal semantics, strategy, detector, scanner, entries, exits, stops, targets, expectancy, or PnL logic was changed or created.",
        "",
        "## Required Answers",
        f"1. **Is next confirmed pivot usually the immediate next structural pivot?** {'Yes' if pct_one >= 0.5 else 'No'}; pct_column_distance_1={all_row['pct_column_distance_1']} across {all_row['count']} measured rows.",
        f"2. **Is column_distance almost always 1?** {'Yes' if pct_one >= 0.95 else 'No'}; median_column_distance={all_row['median_column_distance']}, p90_column_distance={all_row['p90_column_distance']}.",
        f"3. **Is the 100% continuation result likely tautological?** {'Yes' if tautological else 'Not proven solely by this audit'}; the current metric uses the first confirmed pivot after D, and this audit {'shows it is almost always exactly one column after D' if tautological else 'does not show an almost-always-one-column distance'}.",
        f"4. **Is current Phase 3 outcome metric meaningful as post-D behavior?** {'No; it is better characterized as a mechanical next-pivot artifact than a later post-D structural behavior measure' if tautological or close_mechanical else 'Partially; distance is not overwhelmingly immediate, but it still only measures the first confirmed pivot'}.",
        "5. **Should future outcome research use a later horizon?** Yes. Later-horizon structural audits should consider next 3 confirmed pivots, next 5 confirmed pivots, max favorable/adverse boxes after D, and first break of B/C structural level.",
        "",
        "## Root Cause Assessment",
        ("- Root cause: the Phase 3 outcome selects the first trusted confirmed pivot after D by construction. Because the trusted pivot stream advances to that pivot immediately after D, continuation/reversal is measuring the next alternating structural confirmation rather than independent later post-D behavior." if tautological else "- Root cause not proven as fully tautological at the 95% one-column threshold, but the metric remains structurally constrained to the first confirmed pivot after D."),
        "",
        "## All Rows Summary",
        "| Cohort | Count | Median column distance | Average column distance | P25 | P75 | P90 | Pct distance = 1 | Pct distance <= 2 | Pct distance <= 3 | Median time ms | Average time ms | P90 time ms |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary_rows:
        lines.append(f"| {row['cohort']} | {row['count']} | {row['median_column_distance']} | {row['avg_column_distance']} | {row['p25_column_distance']} | {row['p75_column_distance']} | {row['p90_column_distance']} | {row['pct_column_distance_1']} | {row['pct_column_distance_lte_2']} | {row['pct_column_distance_lte_3']} | {row['median_time_distance_ms']} | {row['avg_time_distance_ms']} | {row['p90_time_distance_ms']} |")
    lines.extend(["", "## Research Guardrail", "Strictly structural meaning audit only; no trading or profitability conclusion is made.", ""])
    (output_root / "abcd_outcome_distance_report.md").write_text("\n".join(lines), encoding="utf-8")


def _write_blocked_outputs(output_root: Path, reason: str) -> None:
    _write_csv(output_root / "abcd_outcome_distance_summary.csv", _summary_rows([]), DISTANCE_FIELDS)
    _write_csv(output_root / "abcd_outcome_distance_by_symbol.csv", _group_rows([], "symbol"), BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_outcome_distance_by_year.csv", _group_rows([], "year"), BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_outcome_distance_sample.csv", [], SAMPLE_FIELDS)
    report = [
        "# AB=CD Phase 3 Outcome Distance Audit Report",
        "",
        "## Status",
        "BLOCKED — required local Phase 3 input artifacts are not available or valid in this workspace.",
        "",
        "## Reason",
        reason,
        "",
        "## Required Answers",
        "1. Not determined from this workspace.",
        "2. Not determined from this workspace.",
        "3. Not determined from this workspace.",
        "4. Not determined from this workspace.",
        "5. Future outcome research should use later structural horizons once inputs are available.",
        "",
    ]
    (output_root / "abcd_outcome_distance_report.md").write_text("\n".join(report), encoding="utf-8")


def run_audit(
    *,
    pivot_root: str | Path = TRUSTED_PIVOT_ROOT,
    geometry_root: str | Path = GEOMETRY_ROOT,
    repaired_outcome_root: str | Path = DEFAULT_REPAIRED_OUTCOME_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    sample_size: int = 200,
) -> bool:
    del repaired_outcome_root  # Accepted to document the required input set without reading outcome metrics.
    pivot_path = _resolve_repo_path(pivot_root)
    geometry_path = _resolve_repo_path(geometry_root)
    output_path = _resolve_repo_path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)
    try:
        pivots, _rejects, reactions_path = load_validated_pivots(pivot_path)
        candidates_path = _candidate_path(geometry_path)
        if candidates_path is None:
            missing = geometry_path / GEOMETRY_CANDIDATES
            raise FileNotFoundError(f"missing geometry candidates file: {missing}")
        rows = distance_rows_from_geometry(candidates_path, pivots)
    except (FileNotFoundError, ValueError) as exc:
        _write_blocked_outputs(output_path, str(exc))
        return False
    _write_csv(output_path / "abcd_outcome_distance_summary.csv", _summary_rows(rows), DISTANCE_FIELDS)
    _write_csv(output_path / "abcd_outcome_distance_by_symbol.csv", _group_rows(rows, "symbol"), BY_SYMBOL_FIELDS)
    _write_csv(output_path / "abcd_outcome_distance_by_year.csv", _group_rows(rows, "year"), BY_YEAR_FIELDS)
    _write_csv(output_path / "abcd_outcome_distance_sample.csv", [_sample_row(row) for row in rows[:sample_size]], SAMPLE_FIELDS)
    write_report(output_path, rows, f"geometry candidates `{candidates_path.as_posix()}` with next pivots from `{reactions_path.as_posix()}`")
    return True


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pivot-root", type=Path, default=TRUSTED_PIVOT_ROOT)
    parser.add_argument("--geometry-root", type=Path, default=GEOMETRY_ROOT)
    parser.add_argument("--repaired-outcome-root", type=Path, default=DEFAULT_REPAIRED_OUTCOME_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--sample-size", type=int, default=200)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)
    if args.sample_size < 0:
        parser.error("--sample-size must be non-negative")
    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    ok = run_audit(
        pivot_root=args.pivot_root,
        geometry_root=args.geometry_root,
        repaired_outcome_root=args.repaired_outcome_root,
        output_root=args.output_root,
        sample_size=args.sample_size,
    )
    if args.strict and not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
