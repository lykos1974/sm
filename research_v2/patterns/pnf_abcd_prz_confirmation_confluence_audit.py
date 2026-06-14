"""Research-only AB=CD PRZ + D-confirmation confluence audit.

This module consumes only pre-existing local research artifacts and joins them
strictly by candidate_id. It does not inspect raw datasets, reconstruct ABCDs,
use FAST artifacts, or create entries, exits, stops, targets, RR, expectancy,
profitability, PnL, strategy logic, or a trading model.
"""

from __future__ import annotations

import argparse
import csv
import statistics
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import SYMBOLS, YEARS, _fmt, _parse_float

PRZ_INPUT = Path("research_v2/patterns/abcd_prz_convergence_local_v1/abcd_prz_convergence_candidates.csv")
D_MFE_INPUT = Path("research_v2/patterns/abcd_d_mfe_local_v1/abcd_d_mfe_candidates.csv")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_prz_confirmation_confluence_local_v1")

VALID_PRZ_CLASSES = {"PRZ_TIGHT", "PRZ_ACCEPTABLE"}
CONFIRMATION_THRESHOLD_BOXES = 13.0
SAMPLE_SIZE = 200
DECISION_ADDS = "PRZ_PLUS_CONFIRMATION_ADDS_INFORMATION"
DECISION_DOES_NOT_ADD = "PRZ_PLUS_CONFIRMATION_DOES_NOT_ADD_INFORMATION"

REACTION_METRICS = (
    "first_post_d_reaction_boxes",
    "max_favorable_before_first_adverse_pivot",
    "columns_to_first_adverse_pivot",
)
REQUIRED_PRZ_FIELDS = ("candidate_id", "symbol", "year", "prz_class")
REQUIRED_D_MFE_FIELDS = ("candidate_id", *REACTION_METRICS)

COHORT_ORDER = (
    "ALL",
    "PRZ_TIGHT",
    "PRZ_ACCEPTABLE",
    "PRZ_VALID",
    "CONFIRMED_13",
    "PRZ_VALID_AND_CONFIRMED_13",
)

SUMMARY_FIELDS = [
    "comparison",
    "baseline_cohort",
    "test_cohort",
    "baseline_count",
    "test_count",
    "median_first_post_d_reaction_boxes_uplift",
    "avg_first_post_d_reaction_boxes_uplift",
    "median_max_favorable_before_first_adverse_pivot_uplift",
    "avg_max_favorable_before_first_adverse_pivot_uplift",
    "decision",
]
BY_COHORT_FIELDS = [
    "scope",
    "cohort",
    "count",
    "pct_of_population",
    "median_first_post_d_reaction_boxes",
    "avg_first_post_d_reaction_boxes",
    "median_max_favorable_before_first_adverse_pivot",
    "avg_max_favorable_before_first_adverse_pivot",
    "median_columns_to_first_adverse_pivot",
    "avg_columns_to_first_adverse_pivot",
]
SAMPLE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "prz_class",
    *REACTION_METRICS,
    "in_prz_valid",
    "in_confirmed_13",
    "in_prz_valid_and_confirmed_13",
]


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _median(values: Sequence[float]) -> str:
    return _fmt(statistics.median(values)) if values else ""


def _avg(values: Sequence[float]) -> str:
    return _fmt(sum(values) / len(values)) if values else ""


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _numeric(row: dict[str, Any], field: str) -> float | None:
    return _parse_float(row.get(field))


def _require_fields(path: Path, fieldnames: Sequence[str] | None, required: Iterable[str]) -> None:
    if not fieldnames:
        raise ValueError(f"{path}: expected CSV header")
    missing = [field for field in required if field not in fieldnames]
    if missing:
        raise ValueError(f"{path}: missing required fields: {', '.join(missing)}")


def _load_keyed_csv(path: Path, required_fields: Sequence[str]) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, required_fields)
        for row_number, row in enumerate(reader, start=2):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if not candidate_id:
                raise ValueError(f"{path}:{row_number}: missing candidate_id")
            if candidate_id in rows:
                raise ValueError(f"{path}:{row_number}: duplicate candidate_id {candidate_id}")
            rows[candidate_id] = {key: str(value or "") for key, value in row.items()}
    return rows


def _load_joined_rows(prz_input: Path, d_mfe_input: Path) -> list[dict[str, Any]]:
    prz_rows = _load_keyed_csv(prz_input, REQUIRED_PRZ_FIELDS)
    d_mfe_rows = _load_keyed_csv(d_mfe_input, REQUIRED_D_MFE_FIELDS)
    prz_ids = set(prz_rows)
    d_mfe_ids = set(d_mfe_rows)
    if prz_ids != d_mfe_ids:
        missing_d_mfe = sorted(prz_ids - d_mfe_ids)[:10]
        missing_prz = sorted(d_mfe_ids - prz_ids)[:10]
        raise ValueError(
            "candidate populations do not match for strict candidate_id join; "
            f"prz_count={len(prz_ids)} d_mfe_count={len(d_mfe_ids)} "
            f"missing_from_d_mfe_sample={missing_d_mfe} missing_from_prz_sample={missing_prz}"
        )

    joined: list[dict[str, Any]] = []
    for candidate_id in sorted(prz_ids):
        prz = prz_rows[candidate_id]
        d_mfe = d_mfe_rows[candidate_id]
        prz_class = str(prz.get("prz_class") or "").strip().upper()
        first_reaction = _numeric(d_mfe, "first_post_d_reaction_boxes")
        in_prz_valid = prz_class in VALID_PRZ_CLASSES
        in_confirmed = first_reaction is not None and first_reaction >= CONFIRMATION_THRESHOLD_BOXES
        joined.append(
            {
                **d_mfe,
                "candidate_id": candidate_id,
                "symbol": prz.get("symbol") or d_mfe.get("symbol", ""),
                "year": prz.get("year") or d_mfe.get("year", ""),
                "prz_class": prz_class,
                "in_prz_valid": "YES" if in_prz_valid else "NO",
                "in_confirmed_13": "YES" if in_confirmed else "NO",
                "in_prz_valid_and_confirmed_13": "YES" if in_prz_valid and in_confirmed else "NO",
            }
        )
    return joined


def _cohort_predicates() -> dict[str, Callable[[dict[str, Any]], bool]]:
    return {
        "ALL": lambda row: True,
        "PRZ_TIGHT": lambda row: row.get("prz_class") == "PRZ_TIGHT",
        "PRZ_ACCEPTABLE": lambda row: row.get("prz_class") == "PRZ_ACCEPTABLE",
        "PRZ_VALID": lambda row: row.get("prz_class") in VALID_PRZ_CLASSES,
        "CONFIRMED_13": lambda row: (_numeric(row, "first_post_d_reaction_boxes") or float("-inf")) >= CONFIRMATION_THRESHOLD_BOXES,
        "PRZ_VALID_AND_CONFIRMED_13": lambda row: row.get("prz_class") in VALID_PRZ_CLASSES
        and (_numeric(row, "first_post_d_reaction_boxes") or float("-inf")) >= CONFIRMATION_THRESHOLD_BOXES,
    }


def summarize(rows: Sequence[dict[str, Any]], *, scope: str, cohort: str, population_total: int) -> dict[str, Any]:
    return {
        "scope": scope,
        "cohort": cohort,
        "count": len(rows),
        "pct_of_population": _pct(len(rows), population_total),
        "median_first_post_d_reaction_boxes": _median([v for row in rows if (v := _numeric(row, "first_post_d_reaction_boxes")) is not None]),
        "avg_first_post_d_reaction_boxes": _avg([v for row in rows if (v := _numeric(row, "first_post_d_reaction_boxes")) is not None]),
        "median_max_favorable_before_first_adverse_pivot": _median([v for row in rows if (v := _numeric(row, "max_favorable_before_first_adverse_pivot")) is not None]),
        "avg_max_favorable_before_first_adverse_pivot": _avg([v for row in rows if (v := _numeric(row, "max_favorable_before_first_adverse_pivot")) is not None]),
        "median_columns_to_first_adverse_pivot": _median([v for row in rows if (v := _numeric(row, "columns_to_first_adverse_pivot")) is not None]),
        "avg_columns_to_first_adverse_pivot": _avg([v for row in rows if (v := _numeric(row, "columns_to_first_adverse_pivot")) is not None]),
    }


def _cohort_rows(rows: Sequence[dict[str, Any]], cohort: str) -> list[dict[str, Any]]:
    predicate = _cohort_predicates()[cohort]
    return [row for row in rows if predicate(row)]


def _all_cohort_summaries(rows: Sequence[dict[str, Any]], *, scope: str) -> list[dict[str, Any]]:
    total = len(rows)
    return [summarize(_cohort_rows(rows, cohort), scope=scope, cohort=cohort, population_total=total) for cohort in COHORT_ORDER]


def _to_float_from_summary(row: dict[str, Any], field: str) -> float | None:
    return _parse_float(row.get(field))


def _uplift(test: dict[str, Any], baseline: dict[str, Any], field: str) -> str:
    test_value = _to_float_from_summary(test, field)
    baseline_value = _to_float_from_summary(baseline, field)
    if test_value is None or baseline_value is None:
        return ""
    return _fmt(test_value - baseline_value)


def _decision(summary: dict[str, Any]) -> str:
    uplift_fields = [
        "median_first_post_d_reaction_boxes_uplift",
        "avg_first_post_d_reaction_boxes_uplift",
        "median_max_favorable_before_first_adverse_pivot_uplift",
        "avg_max_favorable_before_first_adverse_pivot_uplift",
    ]
    values = [_parse_float(summary.get(field)) for field in uplift_fields]
    if values and all(value is not None and value > 0 for value in values):
        return DECISION_ADDS
    return DECISION_DOES_NOT_ADD


def _summary_row(by_cohort: Sequence[dict[str, Any]]) -> dict[str, Any]:
    indexed = {row["cohort"]: row for row in by_cohort if row["scope"] == "ALL_SYMBOLS_ALL_YEARS"}
    baseline = indexed["ALL"]
    test = indexed["PRZ_VALID_AND_CONFIRMED_13"]
    row = {
        "comparison": "PRZ_VALID_AND_CONFIRMED_13_vs_ALL",
        "baseline_cohort": "ALL",
        "test_cohort": "PRZ_VALID_AND_CONFIRMED_13",
        "baseline_count": baseline["count"],
        "test_count": test["count"],
        "median_first_post_d_reaction_boxes_uplift": _uplift(test, baseline, "median_first_post_d_reaction_boxes"),
        "avg_first_post_d_reaction_boxes_uplift": _uplift(test, baseline, "avg_first_post_d_reaction_boxes"),
        "median_max_favorable_before_first_adverse_pivot_uplift": _uplift(test, baseline, "median_max_favorable_before_first_adverse_pivot"),
        "avg_max_favorable_before_first_adverse_pivot_uplift": _uplift(test, baseline, "avg_max_favorable_before_first_adverse_pivot"),
    }
    row["decision"] = _decision(row)
    return row


def _markdown_table(rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> str:
    lines = ["| " + " | ".join(fields) + " |", "| " + " | ".join("---" for _ in fields) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(field, "")) for field in fields) + " |")
    return "\n".join(lines)


def _stability_answer(rows: Sequence[dict[str, Any]], scope_prefix: str, summary: dict[str, Any]) -> str:
    relevant = [row for row in rows if row["scope"].startswith(scope_prefix) and row["cohort"] == "PRZ_VALID_AND_CONFIRMED_13"]
    if not relevant:
        return "No scoped PRZ_VALID_AND_CONFIRMED_13 rows available."
    parts = [
        f"{row['scope'].replace(scope_prefix, '')}: count={row['count']}, median_first={row['median_first_post_d_reaction_boxes']}, avg_first={row['avg_first_post_d_reaction_boxes']}"
        for row in relevant
    ]
    return "; ".join(parts) + f". Overall decision: {summary['decision']}."


def write_report(output_root: Path, by_cohort: Sequence[dict[str, Any]], summary: dict[str, Any]) -> None:
    overall = [row for row in by_cohort if row["scope"] == "ALL_SYMBOLS_ALL_YEARS"]
    lines = [
        "# AB=CD PRZ + Confirmation Confluence Audit",
        "",
        "Research-only audit using existing PRZ convergence and bounded D-reaction artifacts only. No raw dataset inspection, ABCD reconstruction, FAST artifacts, strategy, entries, exits, stops, targets, RR, expectancy, profitability, PnL, or trade model are used.",
        "",
        "## Inputs and validations",
        f"- PRZ convergence candidates: `{PRZ_INPUT.as_posix()}`.",
        f"- Bounded D-reaction candidates: `{D_MFE_INPUT.as_posix()}`.",
        "- Candidate populations must match exactly by unique `candidate_id`; otherwise the audit fails hard.",
        "",
        "## Required answers",
        f"1. Total measured candidates: {summary['baseline_count']}",
        "2. Population size for every cohort: see table below.",
        "3. Median and average bounded reaction metrics for every cohort: see table below.",
        f"4. Uplift of PRZ_VALID_AND_CONFIRMED_13 versus ALL: median first reaction {summary['median_first_post_d_reaction_boxes_uplift']}; average first reaction {summary['avg_first_post_d_reaction_boxes_uplift']}; median bounded favorable {summary['median_max_favorable_before_first_adverse_pivot_uplift']}; average bounded favorable {summary['avg_max_favorable_before_first_adverse_pivot_uplift']}.",
        f"5. Is PRZ + confirmation stronger than the overall population? {'Yes' if summary['decision'] == DECISION_ADDS else 'No'}.",
        f"6. Stability across BTCUSDT / ETHUSDT / SOLUSDT: {_stability_answer(by_cohort, 'SYMBOL_', summary)}",
        f"7. Stability across 2024 / 2025 / 2026: {_stability_answer(by_cohort, 'YEAR_', summary)}",
        "",
        "## Overall cohorts",
        _markdown_table(overall, BY_COHORT_FIELDS),
        "",
        "## Core comparison",
        _markdown_table([summary], SUMMARY_FIELDS),
        "",
        f"## Final decision\n\n{summary['decision']}",
        "",
    ]
    (output_root / "abcd_prz_confirmation_confluence_report.md").write_text("\n".join(lines), encoding="utf-8")


def run_audit(prz_input: Path, d_mfe_input: Path, output_root: Path) -> None:
    rows = _load_joined_rows(prz_input, d_mfe_input)
    by_cohort: list[dict[str, Any]] = []
    by_cohort.extend(_all_cohort_summaries(rows, scope="ALL_SYMBOLS_ALL_YEARS"))
    for symbol in SYMBOLS:
        symbol_rows = [row for row in rows if row.get("symbol") == symbol]
        by_cohort.extend(_all_cohort_summaries(symbol_rows, scope=f"SYMBOL_{symbol}"))
    for year in YEARS:
        year_rows = [row for row in rows if str(row.get("year")) == str(year)]
        by_cohort.extend(_all_cohort_summaries(year_rows, scope=f"YEAR_{year}"))

    summary = _summary_row(by_cohort)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "abcd_prz_confirmation_confluence_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_prz_confirmation_confluence_by_cohort.csv", by_cohort, BY_COHORT_FIELDS)
    sample_rows = sorted(rows, key=lambda row: (row.get("symbol", ""), row.get("year", ""), row.get("candidate_id", "")))[:SAMPLE_SIZE]
    _write_csv(output_root / "abcd_prz_confirmation_confluence_sample.csv", sample_rows, SAMPLE_FIELDS)
    write_report(output_root, by_cohort, summary)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prz-input", type=Path, default=PRZ_INPUT)
    parser.add_argument("--d-mfe-input", type=Path, default=D_MFE_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_audit(args.prz_input, args.d_mfe_input, args.output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
