"""Research-only Model C retest entry-level feasibility audit.

This module consumes only existing local research artifacts and evaluates
candidate retest-depth thresholds for PRZ_VALID_AND_CONFIRMED_13 Model C
candidates. It does not inspect raw datasets, reconstruct ABCDs, use FAST
artifacts, create executable entries, stops, targets, RR, expectancy,
profitability, PnL, or trade recommendations.
"""

from __future__ import annotations

import argparse
import csv
import statistics
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import SYMBOLS, YEARS, _fmt, _parse_float

FEASIBILITY_INPUT = Path(
    "research_v2/patterns/abcd_retest_feasibility_local_v1/abcd_retest_feasibility_candidates.csv"
)
CONFLUENCE_INPUT = Path(
    "research_v2/patterns/abcd_prz_confirmation_confluence_local_v1/"
    "abcd_prz_confirmation_confluence_candidates.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_retest_entry_level_local_v1")

EXPECTED_COHORT_COUNT = 1281
LEVELS: tuple[tuple[str, float], ...] = (
    ("RETRACE_382", 0.382),
    ("RETRACE_500", 0.500),
    ("RETRACE_618", 0.618),
)
FINAL_DECISIONS = {
    "RETRACE_382": "RETRACE_382_PREFERRED",
    "RETRACE_500": "RETRACE_500_PREFERRED",
    "RETRACE_618": "RETRACE_618_PREFERRED",
    "NONE": "NO_RETEST_LEVEL_PREFERRED",
}

METRIC_FIELDS = [
    "measured_candidates",
    "qualified_count",
    "qualified_pct",
    "continuation_count",
    "continuation_pct",
    "median_retrace_pct_of_first_reaction",
    "median_columns_to_retrace",
    "median_continuation_boxes_after_retrace",
    "avg_continuation_boxes_after_retrace",
]
SUMMARY_FIELDS = ["final_decision", *[f"{level}_{field}" for level, _ in LEVELS for field in METRIC_FIELDS]]
BY_SYMBOL_FIELDS = ["symbol", *[f"{level}_{field}" for level, _ in LEVELS for field in METRIC_FIELDS]]
BY_YEAR_FIELDS = ["year", *[f"{level}_{field}" for level, _ in LEVELS for field in METRIC_FIELDS]]
BY_LEVEL_FIELDS = ["retest_level", "threshold", *METRIC_FIELDS, "availability_continuation_balance_score"]
CANDIDATE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "retrace_pct_of_first_reaction",
    "columns_to_retrace",
    "has_continuation_after_retrace",
    "continuation_boxes_after_retrace",
    *[f"qualifies_{level}" for level, _ in LEVELS],
]


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _require_fields(path: Path, fieldnames: Sequence[str] | None, required: Iterable[str]) -> None:
    if not fieldnames:
        raise ValueError(f"{path}: expected CSV header")
    missing = [field for field in required if field not in fieldnames]
    if missing:
        raise ValueError(f"{path}: missing required fields: {', '.join(missing)}")


def _yes(value: Any) -> bool:
    return str(value or "").strip().upper() in {"1", "TRUE", "YES", "Y"}


def _median(values: Iterable[float]) -> str:
    clean = [value for value in values if value is not None]
    return _fmt(statistics.median(clean)) if clean else ""


def _avg(values: Iterable[float]) -> str:
    clean = [value for value in values if value is not None]
    return _fmt(sum(clean) / len(clean)) if clean else ""


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _load_confluence_cohort(path: Path) -> set[str]:
    cohort: set[str] = set()
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("candidate_id", "PRZ_VALID_AND_CONFIRMED_13"))
        for row_number, row in enumerate(reader, start=2):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if not candidate_id:
                raise ValueError(f"{path}:{row_number}: missing candidate_id")
            if _yes(row.get("PRZ_VALID_AND_CONFIRMED_13")):
                cohort.add(candidate_id)
    if len(cohort) != EXPECTED_COHORT_COUNT:
        raise ValueError(
            "PRZ_VALID_AND_CONFIRMED_13 cohort count changed: "
            f"expected {EXPECTED_COHORT_COUNT}, observed {len(cohort)}"
        )
    return cohort


def _load_candidates(feasibility_input: Path, confluence_input: Path) -> list[dict[str, Any]]:
    cohort = _load_confluence_cohort(confluence_input)
    rows: list[dict[str, Any]] = []
    required = (
        "candidate_id",
        "symbol",
        "year",
        "retrace_pct_of_first_reaction",
        "columns_to_retrace",
        "has_continuation_after_retrace",
        "continuation_boxes_after_retrace",
    )
    seen: set[str] = set()
    with feasibility_input.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(feasibility_input, reader.fieldnames, required)
        for row_number, row in enumerate(reader, start=2):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if not candidate_id:
                raise ValueError(f"{feasibility_input}:{row_number}: missing candidate_id")
            if candidate_id not in cohort:
                continue
            if candidate_id in seen:
                raise ValueError(f"{feasibility_input}:{row_number}: duplicate candidate_id {candidate_id}")
            seen.add(candidate_id)
            rows.append({key: str(value or "") for key, value in row.items()})
    if len(rows) != EXPECTED_COHORT_COUNT:
        raise ValueError(f"feasibility cohort join changed: expected {EXPECTED_COHORT_COUNT}, observed {len(rows)}")
    return rows


def _qualifies(row: dict[str, Any], threshold: float) -> bool:
    retrace_pct = _parse_float(row.get("retrace_pct_of_first_reaction"))
    return retrace_pct is not None and retrace_pct >= threshold


def _has_continuation(row: dict[str, Any]) -> bool:
    return _yes(row.get("has_continuation_after_retrace")) and _parse_float(row.get("continuation_boxes_after_retrace")) is not None


def summarize_level(rows: Sequence[dict[str, Any]], level: str, threshold: float) -> dict[str, Any]:
    qualified = [row for row in rows if _qualifies(row, threshold)]
    continued = [row for row in qualified if _has_continuation(row)]
    continuation_values = [v for row in continued if (v := _parse_float(row.get("continuation_boxes_after_retrace"))) is not None]
    return {
        "retest_level": level,
        "threshold": _fmt(threshold),
        "measured_candidates": len(rows),
        "qualified_count": len(qualified),
        "qualified_pct": _pct(len(qualified), len(rows)),
        "continuation_count": len(continued),
        "continuation_pct": _pct(len(continued), len(qualified)),
        "median_retrace_pct_of_first_reaction": _median(
            v for row in qualified if (v := _parse_float(row.get("retrace_pct_of_first_reaction"))) is not None
        ),
        "median_columns_to_retrace": _median(
            v for row in qualified if (v := _parse_float(row.get("columns_to_retrace"))) is not None
        ),
        "median_continuation_boxes_after_retrace": _median(continuation_values),
        "avg_continuation_boxes_after_retrace": _avg(continuation_values),
    }


def _flatten(level_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for row in level_rows:
        level = str(row["retest_level"])
        for field in METRIC_FIELDS:
            out[f"{level}_{field}"] = row[field]
    return out


def _balance_score(row: dict[str, Any]) -> float:
    availability = _parse_float(row.get("qualified_pct")) or 0.0
    continuation = _parse_float(row.get("continuation_pct")) or 0.0
    if availability <= 0.0 or continuation <= 0.0:
        return 0.0
    return 2.0 * availability * continuation / (availability + continuation)


def choose_decision(by_level_rows: Sequence[dict[str, Any]]) -> str:
    scored = [(row, _balance_score(row)) for row in by_level_rows]
    best_row, best_score = max(scored, key=lambda item: (item[1], _parse_float(item[0].get("qualified_pct")) or 0.0))
    if best_score <= 0.0:
        return FINAL_DECISIONS["NONE"]
    return FINAL_DECISIONS[str(best_row["retest_level"])]


def candidate_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        item = {field: row.get(field, "") for field in CANDIDATE_FIELDS if not field.startswith("qualifies_")}
        for level, threshold in LEVELS:
            item[f"qualifies_{level}"] = "YES" if _qualifies(row, threshold) else "NO"
        out.append(item)
    return out


def _scope_rows(rows: Sequence[dict[str, Any]], field: str, values: Sequence[Any]) -> list[dict[str, Any]]:
    scoped: list[dict[str, Any]] = []
    for value in values:
        subset = [row for row in rows if str(row.get(field)) == str(value)]
        level_rows = [summarize_level(subset, level, threshold) for level, threshold in LEVELS]
        scoped.append({field: str(value), **_flatten(level_rows)})
    return scoped


def write_report(path: Path, summary: dict[str, Any], by_level_rows: Sequence[dict[str, Any]]) -> None:
    level_by_name = {str(row["retest_level"]): row for row in by_level_rows}
    lines = [
        "# AB=CD Retest Entry-Level Audit — Model C",
        "",
        "Research-only structural audit. No raw dataset inspection, ABCD reconstruction, FAST artifacts, executable entries, stops, targets, RR, expectancy, profitability, PnL, or trade recommendation is included.",
        "",
        "## Required Answers",
        f"1. Model C candidates measured: {level_by_name['RETRACE_382']['measured_candidates']}",
    ]
    for number, level in enumerate(("RETRACE_382", "RETRACE_500", "RETRACE_618"), start=2):
        row = level_by_name[level]
        lines.append(f"{number}. {level} qualified: {row['qualified_count']} ({row['qualified_pct']})")
    lines.extend(
        [
            f"5. Continuation % after RETRACE_382 / RETRACE_500 / RETRACE_618: {level_by_name['RETRACE_382']['continuation_pct']} / {level_by_name['RETRACE_500']['continuation_pct']} / {level_by_name['RETRACE_618']['continuation_pct']}",
            f"6. Median continuation boxes after RETRACE_382 / RETRACE_500 / RETRACE_618: {level_by_name['RETRACE_382']['median_continuation_boxes_after_retrace']} / {level_by_name['RETRACE_500']['median_continuation_boxes_after_retrace']} / {level_by_name['RETRACE_618']['median_continuation_boxes_after_retrace']}",
            f"7. Best balance of availability and continuation: {summary['final_decision']}",
            "8. Stability across BTCUSDT / ETHUSDT / SOLUSDT: see abcd_retest_entry_level_by_symbol.csv.",
            "9. Stability across 2024 / 2025 / 2026: see abcd_retest_entry_level_by_year.csv.",
            "",
            "## Final Decision",
            str(summary["final_decision"]),
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(feasibility_input: Path, confluence_input: Path, output_root: Path) -> dict[str, Any]:
    rows = _load_candidates(feasibility_input, confluence_input)
    by_level_rows = [summarize_level(rows, level, threshold) for level, threshold in LEVELS]
    for row in by_level_rows:
        row["availability_continuation_balance_score"] = _fmt(_balance_score(row))
    summary = {"final_decision": choose_decision(by_level_rows), **_flatten(by_level_rows)}
    by_symbol_rows = _scope_rows(rows, "symbol", SYMBOLS)
    by_year_rows = _scope_rows(rows, "year", YEARS)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "abcd_retest_entry_level_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_retest_entry_level_by_symbol.csv", by_symbol_rows, BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_retest_entry_level_by_year.csv", by_year_rows, BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_retest_entry_level_by_level.csv", by_level_rows, BY_LEVEL_FIELDS)
    _write_csv(output_root / "abcd_retest_entry_level_candidates.csv", candidate_rows(rows), CANDIDATE_FIELDS)
    write_report(output_root / "abcd_retest_entry_level_report.md", summary, by_level_rows)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feasibility-input", type=Path, default=FEASIBILITY_INPUT)
    parser.add_argument("--confluence-input", type=Path, default=CONFLUENCE_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    summary = run(args.feasibility_input, args.confluence_input, args.output_root)
    print(summary["final_decision"])


if __name__ == "__main__":
    main()
