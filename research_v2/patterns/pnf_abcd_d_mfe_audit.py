"""Research-only bounded post-D reaction audit for completed PnF AB=CD candidates.

This module measures only the bounded reaction immediately after D from
validated reaction chronology. The window starts after the matched D pivot and
stops at the first validated pivot in the original CD direction. It does not
inspect raw datasets, reconstruct ABCDs from raw columns, use FAST artifacts,
create entries/exits/stops/targets, define a strategy, or compute
expectancy/profitability/PnL.
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

from pnf_abcd_geometry_audit import (  # noqa: E402
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
from pnf_abcd_outcome_audit import (  # noqa: E402
    GEOMETRY_CANDIDATES,
    GEOMETRY_ROOT,
    TRUSTED_PIVOT_ROOT,
    _candidate_path,
    _resolve_repo_path,
    _sort_pivots,
    load_validated_pivots,
)

DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_d_mfe_local_v1")
COHORTS = ("ALL", "SYM_0_90_1_10", "EXT_1_20_1_35", "EXT_1_55_1_70", "OTHER")
SAMPLE_SIZE = 200
MEANINGFUL_MEDIAN_REACTION_BOXES = 3.0
MEANINGFUL_PCT_REACTION_GE_5 = 0.25

SUMMARY_FIELDS = [
    "scope",
    "cohort",
    "count",
    "measured_rows",
    "median_first_post_d_reaction_boxes",
    "avg_first_post_d_reaction_boxes",
    "median_max_favorable_before_first_adverse_pivot",
    "avg_max_favorable_before_first_adverse_pivot",
    "median_columns_to_first_adverse_pivot",
    "avg_columns_to_first_adverse_pivot",
    "median_time_to_first_adverse_pivot",
    "avg_time_to_first_adverse_pivot",
    "pct_max_favorable_ge_3_boxes",
    "pct_max_favorable_ge_5_boxes",
    "pct_max_favorable_ge_8_boxes",
    "pct_max_favorable_ge_13_boxes",
    "conclusion",
]
BY_SYMBOL_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]
BY_COHORT_FIELDS = SUMMARY_FIELDS
SAMPLE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "cohort",
    "d_column_id",
    "d_knowledge_time",
    "cd_direction",
    "post_d_reaction_direction",
    "first_post_d_reaction_boxes",
    "max_favorable_before_first_adverse_pivot",
    "first_adverse_column_id",
    "first_adverse_knowledge_time",
    "columns_to_first_adverse_pivot",
    "time_to_first_adverse_pivot",
]


@dataclass(frozen=True)
class BoundedReactionRow:
    candidate_id: str
    symbol: str
    year: int | None
    cohort: str
    d_column_id: str
    d_knowledge_time: str
    cd_direction: str
    post_d_reaction_direction: str
    first_post_d_reaction_boxes: float | None
    max_favorable_before_first_adverse_pivot: float | None
    first_adverse_column_id: str
    first_adverse_knowledge_time: str
    columns_to_first_adverse_pivot: int | None
    time_to_first_adverse_pivot: int | None


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _median(values: Sequence[float]) -> str:
    return _fmt(statistics.median(values)) if values else ""


def _avg(values: Sequence[float]) -> str:
    return _fmt(sum(values) / len(values)) if values else ""


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _opposite(direction: str) -> str:
    if direction == "UP":
        return "DOWN"
    if direction == "DOWN":
        return "UP"
    raise ValueError(f"unsupported direction: {direction}")


def _identity_key(symbol: str, d_time: str, d_column_id: str, cd_direction: str) -> tuple[Any, ...] | None:
    d_ts = _parse_time(d_time)
    if d_ts is None:
        return None
    return (symbol, d_ts, _column_sort(d_column_id), cd_direction)


def bounded_reaction_rows_from_geometry(path: Path, pivots: Sequence[Any]) -> list[BoundedReactionRow]:
    ordered_by_symbol = {symbol: _sort_pivots([p for p in pivots if p.symbol == symbol]) for symbol in SYMBOLS}
    rows: list[BoundedReactionRow] = []
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

            ordered = ordered_by_symbol.get(symbol, [])
            matched_index = None
            for pivot_index, pivot in enumerate(ordered):
                pivot_key = (pivot.symbol, pivot.knowledge_ts, pivot.column_sort, pivot.candidate_direction)
                if pivot_key == d_key:
                    matched_index = pivot_index
                    break
            if matched_index is None:
                raise ValueError(f"{path}:{row_number}: could not match D pivot in trusted pivot stream")

            d_pivot = ordered[matched_index]
            post_d_reaction_direction = _opposite(cd_direction)
            first_post_d_reaction_boxes: float | None = None
            max_favorable: float | None = None
            first_adverse_column_id = ""
            first_adverse_knowledge_time = ""
            columns_to_first_adverse: int | None = None
            time_to_first_adverse: int | None = None

            for pivot in ordered[matched_index + 1 :]:
                if pivot.candidate_direction == cd_direction:
                    first_adverse_column_id = pivot.column_id
                    first_adverse_knowledge_time = pivot.knowledge_time
                    columns_to_first_adverse = pivot.column_sort - d_pivot.column_sort
                    time_to_first_adverse = int(round((pivot.knowledge_ts - d_pivot.knowledge_ts) * 1000))
                    if first_post_d_reaction_boxes is None:
                        first_post_d_reaction_boxes = 0.0
                    if max_favorable is None:
                        max_favorable = 0.0
                    break
                if pivot.candidate_direction == post_d_reaction_direction:
                    if first_post_d_reaction_boxes is None:
                        first_post_d_reaction_boxes = pivot.candidate_boxes
                    max_favorable = pivot.candidate_boxes if max_favorable is None else max(max_favorable, pivot.candidate_boxes)

            year_value = _parse_float(row.get("year"))
            rows.append(
                BoundedReactionRow(
                    candidate_id=str(row.get("candidate_id") or f"geometry_row_{row_number}"),
                    symbol=symbol,
                    year=int(year_value) if year_value is not None else None,
                    cohort=classify_cd_ab(cd_ratio),
                    d_column_id=d_pivot.column_id,
                    d_knowledge_time=d_pivot.knowledge_time,
                    cd_direction=cd_direction,
                    post_d_reaction_direction=post_d_reaction_direction,
                    first_post_d_reaction_boxes=first_post_d_reaction_boxes,
                    max_favorable_before_first_adverse_pivot=max_favorable,
                    first_adverse_column_id=first_adverse_column_id,
                    first_adverse_knowledge_time=first_adverse_knowledge_time,
                    columns_to_first_adverse_pivot=columns_to_first_adverse,
                    time_to_first_adverse_pivot=time_to_first_adverse,
                )
            )
    return rows


def _conclusion(median_reaction: float | None, pct_ge_5: float | None) -> str:
    if median_reaction is None or pct_ge_5 is None:
        return "POST_D_REACTION_NOT_MEANINGFUL"
    if median_reaction >= MEANINGFUL_MEDIAN_REACTION_BOXES and pct_ge_5 >= MEANINGFUL_PCT_REACTION_GE_5:
        return "POST_D_REACTION_MEANINGFUL"
    return "POST_D_REACTION_NOT_MEANINGFUL"


def summarize(rows: Sequence[BoundedReactionRow], cohort: str, scope: str) -> dict[str, Any]:
    scoped = list(rows) if cohort == "ALL" else [row for row in rows if row.cohort == cohort]
    measured = [
        row
        for row in scoped
        if row.max_favorable_before_first_adverse_pivot is not None
        and row.columns_to_first_adverse_pivot is not None
        and row.time_to_first_adverse_pivot is not None
    ]
    first_boxes = [row.first_post_d_reaction_boxes for row in measured if row.first_post_d_reaction_boxes is not None]
    max_favorable = [
        row.max_favorable_before_first_adverse_pivot
        for row in measured
        if row.max_favorable_before_first_adverse_pivot is not None
    ]
    columns = [float(row.columns_to_first_adverse_pivot) for row in measured if row.columns_to_first_adverse_pivot is not None]
    times = [float(row.time_to_first_adverse_pivot) for row in measured if row.time_to_first_adverse_pivot is not None]
    median_reaction = statistics.median(max_favorable) if max_favorable else None
    pct_ge_5 = (sum(1 for value in max_favorable if value >= 5) / len(max_favorable)) if max_favorable else None
    return {
        "scope": scope,
        "cohort": cohort,
        "count": len(scoped),
        "measured_rows": len(measured),
        "median_first_post_d_reaction_boxes": _median(first_boxes),
        "avg_first_post_d_reaction_boxes": _avg(first_boxes),
        "median_max_favorable_before_first_adverse_pivot": _median(max_favorable),
        "avg_max_favorable_before_first_adverse_pivot": _avg(max_favorable),
        "median_columns_to_first_adverse_pivot": _median(columns),
        "avg_columns_to_first_adverse_pivot": _avg(columns),
        "median_time_to_first_adverse_pivot": _median(times),
        "avg_time_to_first_adverse_pivot": _avg(times),
        "pct_max_favorable_ge_3_boxes": _pct(sum(1 for value in max_favorable if value >= 3), len(max_favorable)),
        "pct_max_favorable_ge_5_boxes": _pct(sum(1 for value in max_favorable if value >= 5), len(max_favorable)),
        "pct_max_favorable_ge_8_boxes": _pct(sum(1 for value in max_favorable if value >= 8), len(max_favorable)),
        "pct_max_favorable_ge_13_boxes": _pct(sum(1 for value in max_favorable if value >= 13), len(max_favorable)),
        "conclusion": _conclusion(median_reaction, pct_ge_5),
    }


def _summary_rows(rows: Sequence[BoundedReactionRow], scope: str) -> list[dict[str, Any]]:
    return [summarize(rows, cohort, scope) for cohort in COHORTS]


def _group_rows(rows: Sequence[BoundedReactionRow], key_name: str) -> list[dict[str, Any]]:
    if key_name == "symbol":
        keys: Sequence[Any] = SYMBOLS
        getter = lambda row: row.symbol
    elif key_name == "year":
        keys = YEARS
        getter = lambda row: row.year
    else:
        raise ValueError(key_name)
    grouped: list[dict[str, Any]] = []
    for key in keys:
        scoped = [row for row in rows if getter(row) == key]
        for summary in _summary_rows(scoped, str(key)):
            grouped.append({key_name: key, **summary})
    return grouped


def _fmt_optional(value: float | int | None) -> str:
    return _fmt(float(value)) if value is not None else ""


def _sample_row(row: BoundedReactionRow) -> dict[str, Any]:
    return {
        "candidate_id": row.candidate_id,
        "symbol": row.symbol,
        "year": row.year if row.year is not None else "",
        "cohort": row.cohort,
        "d_column_id": row.d_column_id,
        "d_knowledge_time": row.d_knowledge_time,
        "cd_direction": row.cd_direction,
        "post_d_reaction_direction": row.post_d_reaction_direction,
        "first_post_d_reaction_boxes": _fmt_optional(row.first_post_d_reaction_boxes),
        "max_favorable_before_first_adverse_pivot": _fmt_optional(row.max_favorable_before_first_adverse_pivot),
        "first_adverse_column_id": row.first_adverse_column_id,
        "first_adverse_knowledge_time": row.first_adverse_knowledge_time,
        "columns_to_first_adverse_pivot": _fmt_optional(row.columns_to_first_adverse_pivot),
        "time_to_first_adverse_pivot": _fmt_optional(row.time_to_first_adverse_pivot),
    }


def _stability(rows: Sequence[dict[str, Any]], key_name: str) -> str:
    all_rows = [row for row in rows if row["cohort"] == "ALL" and int(row["measured_rows"]) > 0]
    values = [(row[key_name], _parse_float(row.get("median_max_favorable_before_first_adverse_pivot"))) for row in all_rows]
    values = [(key, value) for key, value in values if value is not None]
    if not values:
        return "Not determined; no bounded rows reached a first adverse pivot."
    low_key, low = min(values, key=lambda item: item[1])
    high_key, high = max(values, key=lambda item: item[1])
    spread = high - low
    if spread <= 2:
        return f"Broadly stable by median bounded favorable boxes ({low_key}={_fmt(low)} to {high_key}={_fmt(high)})."
    if spread <= 5:
        return f"Moderately uneven by median bounded favorable boxes ({low_key}={_fmt(low)} to {high_key}={_fmt(high)})."
    return f"Highly uneven by median bounded favorable boxes ({low_key}={_fmt(low)} to {high_key}={_fmt(high)})."


def write_report(
    output_root: Path,
    *,
    source_detail: str,
    summary_rows: Sequence[dict[str, Any]],
    symbol_rows: Sequence[dict[str, Any]],
    year_rows: Sequence[dict[str, Any]],
) -> None:
    overall = next(row for row in summary_rows if row["cohort"] == "ALL")
    lines = [
        "# AB=CD D-Completion Bounded Reaction Audit Report",
        "",
        "## Scope",
        "- Research only: bounded post-D reaction measurement for AB=CD candidates.",
        f"- Source detail: {source_detail}.",
        "- Uses validated SLOW confirming reaction chronology only.",
        "- Measurement starts after the matched D pivot and stops at the first validated pivot in the original CD direction.",
        "- No unbounded future accumulation or end-of-dataset measurement is performed.",
        "- No local data/dataset inspection, ABCD reconstruction, FAST artifacts, entries, exits, stops, targets, strategy, expectancy, profitability, RR, PnL, or trade model were used.",
        "",
        "## Required Bounded Measures",
        f"1. **Total D-completed candidates measured:** {overall['measured_rows']} of {overall['count']} candidates reached a first adverse pivot.",
        f"2. **Median first_post_d_reaction_boxes:** {overall['median_first_post_d_reaction_boxes']}",
        f"3. **Average first_post_d_reaction_boxes:** {overall['avg_first_post_d_reaction_boxes']}",
        f"4. **Median max_favorable_before_first_adverse_pivot:** {overall['median_max_favorable_before_first_adverse_pivot']}",
        f"5. **Average max_favorable_before_first_adverse_pivot:** {overall['avg_max_favorable_before_first_adverse_pivot']}",
        f"6. **Median columns_to_first_adverse_pivot:** {overall['median_columns_to_first_adverse_pivot']}",
        f"7. **Average columns_to_first_adverse_pivot:** {overall['avg_columns_to_first_adverse_pivot']}",
        f"8. **Median time_to_first_adverse_pivot:** {overall['median_time_to_first_adverse_pivot']} ms",
        f"9. **Average time_to_first_adverse_pivot:** {overall['avg_time_to_first_adverse_pivot']} ms",
        f"10. **% max favorable >= 3 boxes:** {overall['pct_max_favorable_ge_3_boxes']}",
        f"11. **% max favorable >= 5 boxes:** {overall['pct_max_favorable_ge_5_boxes']}",
        f"12. **% max favorable >= 8 boxes:** {overall['pct_max_favorable_ge_8_boxes']}",
        f"13. **% max favorable >= 13 boxes:** {overall['pct_max_favorable_ge_13_boxes']}",
        f"14. **Stability across symbols:** {_stability(symbol_rows, 'symbol')}",
        f"15. **Stability across years:** {_stability(year_rows, 'year')}",
        "16. **Stability across cohorts:** See `abcd_d_mfe_by_cohort.csv`; cohort rows are descriptive only and make no strategy claim.",
        "",
        "## Final Research-Only Conclusion",
        f"- {overall['conclusion']}",
        "",
        "## Guardrail",
        "This file is an audit artifact. It intentionally contains no strategy, no entries, no exits, no RR, no profitability, no PnL, no unbounded future accumulation, and no end-of-dataset measurement.",
        "",
    ]
    (output_root / "abcd_d_mfe_report.md").write_text("\n".join(lines), encoding="utf-8")


def _write_blocked_outputs(output_root: Path, *, reason: str) -> None:
    empty_summary = _summary_rows([], "ALL")
    _write_csv(output_root / "abcd_d_mfe_summary.csv", empty_summary, SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_d_mfe_by_symbol.csv", _group_rows([], "symbol"), BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_d_mfe_by_year.csv", _group_rows([], "year"), BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_d_mfe_by_cohort.csv", empty_summary, BY_COHORT_FIELDS)
    _write_csv(output_root / "abcd_d_mfe_candidates.csv", [], SAMPLE_FIELDS)
    _write_csv(output_root / "abcd_d_mfe_sample.csv", [], SAMPLE_FIELDS)
    report = [
        "# AB=CD D-Completion Bounded Reaction Audit Report",
        "",
        "## Status",
        "BLOCKED — approved geometry candidates or validated reaction chronology are not available.",
        "",
        "## Reason",
        reason,
        "",
        "## Guardrail",
        "No fallback to local datasets, ABCD reconstruction, FAST artifacts, strategy logic, entries/exits, expectancy, profitability, PnL, unbounded future accumulation, or end-of-dataset measurement was used.",
        "",
        "## Final Research-Only Conclusion",
        "- POST_D_REACTION_NOT_MEANINGFUL",
        "",
    ]
    (output_root / "abcd_d_mfe_report.md").write_text("\n".join(report), encoding="utf-8")


def run_audit(*, pivot_root: str | Path = TRUSTED_PIVOT_ROOT, geometry_root: str | Path = GEOMETRY_ROOT, output_root: str | Path = DEFAULT_OUTPUT_ROOT) -> bool:
    output_path = Path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)
    try:
        geometry_path = _candidate_path(_resolve_repo_path(geometry_root))
        if geometry_path is None:
            raise FileNotFoundError(f"missing {GEOMETRY_CANDIDATES} under {geometry_root}")
        pivots, _rejects, reactions_path = load_validated_pivots(_resolve_repo_path(pivot_root))
        rows = bounded_reaction_rows_from_geometry(geometry_path, pivots)
        source_detail = f"geometry `{geometry_path.as_posix()}` with chronology `{reactions_path.as_posix()}`"
    except (FileNotFoundError, ValueError) as exc:
        _write_blocked_outputs(output_path, reason=str(exc))
        return False

    summaries = _summary_rows(rows, "ALL")
    symbol_rows = _group_rows(rows, "symbol")
    year_rows = _group_rows(rows, "year")
    _write_csv(output_path / "abcd_d_mfe_summary.csv", summaries, SUMMARY_FIELDS)
    _write_csv(output_path / "abcd_d_mfe_by_symbol.csv", symbol_rows, BY_SYMBOL_FIELDS)
    _write_csv(output_path / "abcd_d_mfe_by_year.csv", year_rows, BY_YEAR_FIELDS)
    _write_csv(output_path / "abcd_d_mfe_by_cohort.csv", summaries, BY_COHORT_FIELDS)
    candidate_rows = [_sample_row(row) for row in rows]
    measured = [row for row in rows if row.columns_to_first_adverse_pivot is not None]
    _write_csv(output_path / "abcd_d_mfe_candidates.csv", candidate_rows, SAMPLE_FIELDS)
    _write_csv(output_path / "abcd_d_mfe_sample.csv", [_sample_row(row) for row in measured[:SAMPLE_SIZE]], SAMPLE_FIELDS)
    write_report(output_path, source_detail=source_detail, summary_rows=summaries, symbol_rows=symbol_rows, year_rows=year_rows)
    return True


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pivot-root", default=str(TRUSTED_PIVOT_ROOT))
    parser.add_argument("--geometry-root", default=str(GEOMETRY_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    return 0 if run_audit(pivot_root=args.pivot_root, geometry_root=args.geometry_root, output_root=args.output_root) else 1


if __name__ == "__main__":
    raise SystemExit(main())
