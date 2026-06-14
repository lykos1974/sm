"""Research-only AB=CD entry timing location audit.

This module consumes only pre-existing local research artifacts for the already
validated PRZ_VALID_AND_CONFIRMED_13 cohort. It does not inspect raw datasets,
reconstruct ABCDs, use FAST artifacts, create strategy logic, define stops or
targets, compute RR/expectancy/profitability, or make trade recommendations.
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

sys.path.append(str(Path(__file__).resolve().parent))

from pnf_abcd_geometry_audit import SYMBOLS, YEARS, _fmt, _parse_float  # noqa: E402

CONFLUENCE_INPUT = Path(
    "research_v2/patterns/abcd_prz_confirmation_confluence_local_v1/"
    "abcd_prz_confirmation_confluence_candidates.csv"
)
D_MFE_INPUT = Path("research_v2/patterns/abcd_d_mfe_local_v1/abcd_d_mfe_candidates.csv")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_entry_timing_local_v1")

VALIDATED_COHORT = "PRZ_VALID_AND_CONFIRMED_13"
ENTRY_D = "ENTRY_D"
ENTRY_REACTION_8 = "ENTRY_REACTION_8"
ENTRY_REACTION_13 = "ENTRY_REACTION_13"
ENTRY_COHORTS = (ENTRY_D, ENTRY_REACTION_8, ENTRY_REACTION_13)

REACTION_METRICS = (
    "first_post_d_reaction_boxes",
    "max_favorable_before_first_adverse_pivot",
    "columns_to_first_adverse_pivot",
)
REQUIRED_CONFLUENCE_FIELDS = ("candidate_id", "symbol", "year", VALIDATED_COHORT)
REQUIRED_D_MFE_FIELDS = ("candidate_id", *REACTION_METRICS)

SUMMARY_FIELDS = [
    "scope",
    "cohort",
    "population_count",
    "population_pct",
    "median_first_post_d_reaction_boxes",
    "avg_first_post_d_reaction_boxes",
    "median_max_favorable_before_first_adverse_pivot",
    "avg_max_favorable_before_first_adverse_pivot",
    "median_columns_to_first_adverse_pivot",
    "avg_columns_to_first_adverse_pivot",
]
BY_SYMBOL_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]
CANDIDATE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    *REACTION_METRICS,
    "entry_d",
    "entry_reaction_8",
    "entry_reaction_13",
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


def _load_keyed_csv(path: Path, required_fields: Sequence[str]) -> dict[str, dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"required local artifact not found: {path}")
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


def _is_yes(value: Any) -> bool:
    return str(value or "").strip().upper() in {"YES", "TRUE", "1"}


def _numeric(row: dict[str, Any], field: str) -> float | None:
    return _parse_float(row.get(field))


def _median(values: Sequence[float]) -> str:
    return _fmt(statistics.median(values)) if values else ""


def _avg(values: Sequence[float]) -> str:
    return _fmt(sum(values) / len(values)) if values else ""


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _entry_membership(row: dict[str, Any], cohort: str) -> bool:
    first_reaction = _numeric(row, "first_post_d_reaction_boxes")
    if cohort == ENTRY_D:
        return True
    if cohort == ENTRY_REACTION_8:
        return first_reaction is not None and first_reaction >= 8.0
    if cohort == ENTRY_REACTION_13:
        return first_reaction is not None and first_reaction >= 13.0
    raise ValueError(f"unsupported entry cohort: {cohort}")


def _load_validated_rows(confluence_input: Path, d_mfe_input: Path) -> list[dict[str, Any]]:
    confluence_rows = _load_keyed_csv(confluence_input, REQUIRED_CONFLUENCE_FIELDS)
    d_mfe_rows = _load_keyed_csv(d_mfe_input, REQUIRED_D_MFE_FIELDS)
    missing_from_d_mfe = sorted(set(confluence_rows) - set(d_mfe_rows))[:10]
    if missing_from_d_mfe:
        raise ValueError(f"validated confluence candidates missing from D-MFE artifact: {missing_from_d_mfe}")

    rows: list[dict[str, Any]] = []
    for candidate_id in sorted(confluence_rows):
        confluence = confluence_rows[candidate_id]
        if not _is_yes(confluence.get(VALIDATED_COHORT)):
            continue
        d_mfe = d_mfe_rows[candidate_id]
        row: dict[str, Any] = {
            "candidate_id": candidate_id,
            "symbol": confluence.get("symbol") or d_mfe.get("symbol", ""),
            "year": confluence.get("year") or d_mfe.get("year", ""),
        }
        for metric in REACTION_METRICS:
            row[metric] = d_mfe.get(metric, "")
        row["entry_d"] = "YES"
        row["entry_reaction_8"] = "YES" if _entry_membership(row, ENTRY_REACTION_8) else "NO"
        row["entry_reaction_13"] = "YES" if _entry_membership(row, ENTRY_REACTION_13) else "NO"
        rows.append(row)
    return rows


def summarize(rows: Sequence[dict[str, Any]], *, scope: str, cohort: str, population_total: int) -> dict[str, Any]:
    return {
        "scope": scope,
        "cohort": cohort,
        "population_count": len(rows),
        "population_pct": _pct(len(rows), population_total),
        "median_first_post_d_reaction_boxes": _median(
            [v for row in rows if (v := _numeric(row, "first_post_d_reaction_boxes")) is not None]
        ),
        "avg_first_post_d_reaction_boxes": _avg(
            [v for row in rows if (v := _numeric(row, "first_post_d_reaction_boxes")) is not None]
        ),
        "median_max_favorable_before_first_adverse_pivot": _median(
            [v for row in rows if (v := _numeric(row, "max_favorable_before_first_adverse_pivot")) is not None]
        ),
        "avg_max_favorable_before_first_adverse_pivot": _avg(
            [v for row in rows if (v := _numeric(row, "max_favorable_before_first_adverse_pivot")) is not None]
        ),
        "median_columns_to_first_adverse_pivot": _median(
            [v for row in rows if (v := _numeric(row, "columns_to_first_adverse_pivot")) is not None]
        ),
        "avg_columns_to_first_adverse_pivot": _avg(
            [v for row in rows if (v := _numeric(row, "columns_to_first_adverse_pivot")) is not None]
        ),
    }


def _cohort_rows(rows: Sequence[dict[str, Any]], cohort: str) -> list[dict[str, Any]]:
    return [row for row in rows if _entry_membership(row, cohort)]


def _summaries(rows: Sequence[dict[str, Any]], *, scope: str) -> list[dict[str, Any]]:
    total = len(rows)
    return [summarize(_cohort_rows(rows, cohort), scope=scope, cohort=cohort, population_total=total) for cohort in ENTRY_COHORTS]


def _best_cohort(summary_rows: Sequence[dict[str, Any]], metric: str) -> str:
    scored = [(cohort_row["cohort"], _parse_float(cohort_row.get(metric))) for cohort_row in summary_rows]
    scored = [(cohort, value) for cohort, value in scored if value is not None]
    if not scored:
        return "INSUFFICIENT_DATA"
    return max(scored, key=lambda item: item[1])[0]


def _stability(rows: Sequence[dict[str, Any]], groups: Sequence[Any], group_field: str, metric: str) -> str:
    winners: list[str] = []
    for group in groups:
        group_rows = [row for row in rows if str(row.get(group_field) or "") == str(group)]
        if not group_rows:
            winners.append(f"{group}:NO_DATA")
            continue
        winners.append(f"{group}:{_best_cohort(_summaries(group_rows, scope=str(group)), metric)}")
    unique_winners = {winner.split(":", 1)[1] for winner in winners if not winner.endswith(":NO_DATA")}
    status = "STABLE" if len(unique_winners) == 1 and len(winners) == len(groups) else "MIXED_OR_INCOMPLETE"
    return f"{status} ({'; '.join(winners)})"


def _report(summary_rows: Sequence[dict[str, Any]], rows: Sequence[dict[str, Any]]) -> str:
    by_cohort = {row["cohort"]: row for row in summary_rows}
    best_median = _best_cohort(summary_rows, "median_max_favorable_before_first_adverse_pivot")
    best_avg = _best_cohort(summary_rows, "avg_max_favorable_before_first_adverse_pivot")
    symbol_stability = _stability(rows, SYMBOLS, "symbol", "median_max_favorable_before_first_adverse_pivot")
    year_stability = _stability(rows, YEARS, "year", "median_max_favorable_before_first_adverse_pivot")
    decision = (
        "RESEARCH_ONLY_DISCARD_AS_STRATEGY_INPUT: structural reaction-gated locations are descriptive only; "
        "no entry, exit, stop, target, RR, expectancy, profitability, or recommendation is produced."
    )
    lines = [
        "# AB=CD Entry Timing Location Audit",
        "",
        "Research-only audit for `PRZ_VALID_AND_CONFIRMED_13` using existing local artifacts only.",
        "",
        "## Required answers",
        f"1. Total candidates loaded: {len(rows)}",
        f"2. ENTRY_D population: {by_cohort[ENTRY_D]['population_count']} ({by_cohort[ENTRY_D]['population_pct']})",
        f"3. ENTRY_REACTION_8 population: {by_cohort[ENTRY_REACTION_8]['population_count']} ({by_cohort[ENTRY_REACTION_8]['population_pct']})",
        f"4. ENTRY_REACTION_13 population: {by_cohort[ENTRY_REACTION_13]['population_count']} ({by_cohort[ENTRY_REACTION_13]['population_pct']})",
        f"5. Largest median favorable reaction: {best_median}",
        f"6. Largest average favorable reaction: {best_avg}",
        f"7. Stability across BTCUSDT / ETHUSDT / SOLUSDT: {symbol_stability}",
        f"8. Stability across 2024 / 2025 / 2026: {year_stability}",
        f"9. Final research-only decision: {decision}",
        "",
        "## Guardrails",
        "No entries, exits, stops, targets, RR, expectancy, profitability, trade model, or trade recommendations are created.",
    ]
    return "\n".join(lines) + "\n"


def run(confluence_input: Path, d_mfe_input: Path, output_root: Path) -> None:
    rows = _load_validated_rows(confluence_input, d_mfe_input)
    summary_rows = _summaries(rows, scope="ALL")
    by_symbol = [
        {"symbol": symbol, **summary}
        for symbol in SYMBOLS
        for summary in _summaries([row for row in rows if row.get("symbol") == symbol], scope=symbol)
    ]
    by_year = [
        {"year": year, **summary}
        for year in YEARS
        for summary in _summaries([row for row in rows if str(row.get("year") or "") == str(year)], scope=str(year))
    ]
    _write_csv(output_root / "abcd_entry_timing_summary.csv", summary_rows, SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_entry_timing_by_symbol.csv", by_symbol, BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_entry_timing_by_year.csv", by_year, BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_entry_timing_by_cohort.csv", summary_rows, SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_entry_timing_candidates.csv", rows, CANDIDATE_FIELDS)
    (output_root / "abcd_entry_timing_report.md").write_text(_report(summary_rows, rows), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confluence-input", type=Path, default=CONFLUENCE_INPUT)
    parser.add_argument("--d-mfe-input", type=Path, default=D_MFE_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    run(args.confluence_input, args.d_mfe_input, args.output_root)


if __name__ == "__main__":
    main()
