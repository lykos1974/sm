"""Research-only structural PnL proxy prototype for AB=CD PRZ-confirmed candidates.

This module consumes only existing local research artifacts and joins them by
candidate_id. It does not inspect raw datasets, reconstruct ABCDs, use FAST
artifacts, alter previous audits, create production trading logic, model real
execution, include fees/slippage, or make trade recommendations.
"""

from __future__ import annotations

import argparse
import csv
import statistics
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import SYMBOLS, YEARS, _fmt, _parse_float

CONFLUENCE_INPUT = Path(
    "research_v2/patterns/abcd_prz_confirmation_confluence_local_v1/"
    "abcd_prz_confirmation_confluence_candidates.csv"
)
FEASIBILITY_INPUT = Path(
    "research_v2/patterns/abcd_retest_feasibility_local_v1/abcd_retest_feasibility_candidates.csv"
)
ENTRY_INPUT = Path(
    "research_v2/patterns/abcd_retest_entry_level_local_v1/abcd_retest_entry_level_candidates.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_pnl_prototype_local_v1")

EXPECTED_COHORT_COUNT = 1281
ENTRY_MODELS: tuple[tuple[str, float], ...] = (
    ("ENTRY_RETRACE_382", 0.382),
    ("ENTRY_RETRACE_500", 0.500),
    ("ENTRY_RETRACE_618", 0.618),
)
FINAL_DECISION_YES = "PNL_PROXY_JUSTIFIES_CANDLE_SIMULATION"
FINAL_DECISION_NO = "PNL_PROXY_DOES_NOT_JUSTIFY_CANDLE_SIMULATION"

METRIC_FIELDS = [
    "qualified_count",
    "qualified_pct",
    "median_entry_cost_boxes",
    "median_favorable_after_entry_boxes",
    "win_rate_1R",
    "avg_r_1R",
    "total_r_1R",
    "win_rate_2R",
    "avg_r_2R",
    "total_r_2R",
    "win_rate_3R",
    "avg_r_3R",
    "total_r_3R",
]
BY_ENTRY_MODEL_FIELDS = ["entry_model", "threshold", *METRIC_FIELDS]
SUMMARY_FIELDS = ["cohort", "cohort_count", "final_decision", *[f"{model}_{field}" for model, _ in ENTRY_MODELS for field in METRIC_FIELDS]]
BY_SYMBOL_FIELDS = ["symbol", *[f"{model}_{field}" for model, _ in ENTRY_MODELS for field in METRIC_FIELDS]]
BY_YEAR_FIELDS = ["year", *[f"{model}_{field}" for model, _ in ENTRY_MODELS for field in METRIC_FIELDS]]
CANDIDATE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "first_post_d_reaction_boxes",
    "retrace_pct_of_first_reaction",
    "continuation_boxes_after_retrace",
    *[f"{model}_qualified" for model, _ in ENTRY_MODELS],
    *[f"{model}_entry_cost_boxes" for model, _ in ENTRY_MODELS],
    *[f"{model}_realized_r_1R" for model, _ in ENTRY_MODELS],
    *[f"{model}_realized_r_2R" for model, _ in ENTRY_MODELS],
    *[f"{model}_realized_r_3R" for model, _ in ENTRY_MODELS],
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


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


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


def _load_rows(confluence_input: Path, feasibility_input: Path, entry_input: Path) -> list[dict[str, Any]]:
    confluence = _load_keyed_csv(
        confluence_input,
        ("candidate_id", "symbol", "year", "PRZ_VALID_AND_CONFIRMED_13", "first_post_d_reaction_boxes"),
    )
    feasibility = _load_keyed_csv(feasibility_input, ("candidate_id",))
    entry = _load_keyed_csv(
        entry_input,
        ("candidate_id", "retrace_pct_of_first_reaction", "continuation_boxes_after_retrace"),
    )
    cohort_ids = {candidate_id for candidate_id, row in confluence.items() if _yes(row.get("PRZ_VALID_AND_CONFIRMED_13"))}
    if len(cohort_ids) != EXPECTED_COHORT_COUNT:
        raise ValueError(
            "PRZ_VALID_AND_CONFIRMED_13 cohort count changed: "
            f"expected {EXPECTED_COHORT_COUNT}, observed {len(cohort_ids)}"
        )
    missing_feasibility = sorted(cohort_ids - set(feasibility))[:10]
    missing_entry = sorted(cohort_ids - set(entry))[:10]
    if missing_feasibility or missing_entry:
        raise ValueError(
            "strict candidate_id join failed for local artifacts; "
            f"missing_feasibility_sample={missing_feasibility} missing_entry_sample={missing_entry}"
        )
    rows: list[dict[str, Any]] = []
    for candidate_id in sorted(cohort_ids):
        confluence_row = confluence[candidate_id]
        entry_row = entry[candidate_id]
        rows.append(
            {
                "candidate_id": candidate_id,
                "symbol": confluence_row.get("symbol", ""),
                "year": confluence_row.get("year", ""),
                "first_post_d_reaction_boxes": confluence_row.get("first_post_d_reaction_boxes", ""),
                "retrace_pct_of_first_reaction": entry_row.get("retrace_pct_of_first_reaction", ""),
                "continuation_boxes_after_retrace": entry_row.get("continuation_boxes_after_retrace", ""),
            }
        )
    return rows


def _outcomes(row: dict[str, Any], threshold: float) -> tuple[bool, float | None, float | None, int | None, int | None, int | None]:
    retrace = _parse_float(row.get("retrace_pct_of_first_reaction"))
    first_reaction = _parse_float(row.get("first_post_d_reaction_boxes"))
    favorable = _parse_float(row.get("continuation_boxes_after_retrace"))
    qualified = retrace is not None and retrace >= threshold and first_reaction is not None and favorable is not None
    if not qualified:
        return False, None, favorable, None, None, None
    entry_cost = threshold * first_reaction
    return (
        True,
        entry_cost,
        favorable,
        1 if favorable >= entry_cost else -1,
        2 if favorable >= 2.0 * entry_cost else -1,
        3 if favorable >= 3.0 * entry_cost else -1,
    )


def summarize_model(rows: Sequence[dict[str, Any]], model: str, threshold: float) -> dict[str, Any]:
    qualified: list[tuple[float, float, int, int, int]] = []
    for row in rows:
        is_qualified, entry_cost, favorable, r1, r2, r3 = _outcomes(row, threshold)
        if is_qualified and entry_cost is not None and favorable is not None and r1 is not None and r2 is not None and r3 is not None:
            qualified.append((entry_cost, favorable, r1, r2, r3))
    total = len(qualified)

    def r_metric(index: int, win_value: int) -> tuple[str, str, str]:
        values = [item[index] for item in qualified]
        wins = sum(1 for value in values if value == win_value)
        total_r = sum(values)
        return _pct(wins, total), _fmt(total_r / total) if total else "", _fmt(float(total_r)) if total else ""

    win_1r, avg_1r, total_1r = r_metric(2, 1)
    win_2r, avg_2r, total_2r = r_metric(3, 2)
    win_3r, avg_3r, total_3r = r_metric(4, 3)
    return {
        "entry_model": model,
        "threshold": _fmt(threshold),
        "qualified_count": total,
        "qualified_pct": _pct(total, len(rows)),
        "median_entry_cost_boxes": _median(item[0] for item in qualified),
        "median_favorable_after_entry_boxes": _median(item[1] for item in qualified),
        "win_rate_1R": win_1r,
        "avg_r_1R": avg_1r,
        "total_r_1R": total_1r,
        "win_rate_2R": win_2r,
        "avg_r_2R": avg_2r,
        "total_r_2R": total_2r,
        "win_rate_3R": win_3r,
        "avg_r_3R": avg_3r,
        "total_r_3R": total_3r,
    }


def _flatten(model_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return {f"{row['entry_model']}_{field}": row[field] for row in model_rows for field in METRIC_FIELDS}


def _scope_rows(rows: Sequence[dict[str, Any]], field: str, values: Sequence[Any]) -> list[dict[str, Any]]:
    return [
        {field: str(value), **_flatten([summarize_model([row for row in rows if str(row.get(field)) == str(value)], model, threshold) for model, threshold in ENTRY_MODELS])}
        for value in values
    ]


def candidate_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        item = {field: row.get(field, "") for field in CANDIDATE_FIELDS if "ENTRY_RETRACE" not in field}
        for model, threshold in ENTRY_MODELS:
            is_qualified, entry_cost, _, r1, r2, r3 = _outcomes(row, threshold)
            item[f"{model}_qualified"] = "YES" if is_qualified else "NO"
            item[f"{model}_entry_cost_boxes"] = _fmt(entry_cost) if entry_cost is not None else ""
            item[f"{model}_realized_r_1R"] = r1 if r1 is not None else ""
            item[f"{model}_realized_r_2R"] = r2 if r2 is not None else ""
            item[f"{model}_realized_r_3R"] = r3 if r3 is not None else ""
        out.append(item)
    return out


def _best(rows: Sequence[dict[str, Any]], metric: str) -> dict[str, Any]:
    return max(rows, key=lambda row: (_parse_float(row.get(metric)) if _parse_float(row.get(metric)) is not None else float("-inf"), _parse_float(row.get("qualified_pct")) or 0.0))


def _balance(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return max(rows, key=lambda row: ((_parse_float(row.get("qualified_pct")) or 0.0) * max(_parse_float(row.get("avg_r_1R")) or 0.0, 0.0)))


def choose_decision(by_entry_model_rows: Sequence[dict[str, Any]]) -> str:
    best_1r = _best(by_entry_model_rows, "avg_r_1R")
    best_2r = _best(by_entry_model_rows, "avg_r_2R")
    avg_1r = _parse_float(best_1r.get("avg_r_1R")) or 0.0
    avg_2r = _parse_float(best_2r.get("avg_r_2R")) or 0.0
    qualified_pct = _parse_float(best_1r.get("qualified_pct")) or 0.0
    return FINAL_DECISION_YES if avg_1r > 0.0 and avg_2r > 0.0 and qualified_pct >= 0.25 else FINAL_DECISION_NO


def write_report(path: Path, by_entry_model_rows: Sequence[dict[str, Any]], by_symbol_rows: Sequence[dict[str, Any]], by_year_rows: Sequence[dict[str, Any]], final_decision: str) -> None:
    best_1r = _best(by_entry_model_rows, "avg_r_1R")
    best_2r = _best(by_entry_model_rows, "avg_r_2R")
    best_3r = _best(by_entry_model_rows, "avg_r_3R")
    balance = _balance(by_entry_model_rows)
    lines = [
        "# AB=CD Structural PnL Proxy Prototype Audit",
        "",
        "Research-only proxy PnL audit using existing local artifacts only. No raw dataset inspection, ABCD reconstruction, FAST artifacts, production strategy logic, real execution, candle-level ordering, fees/slippage, or trade recommendation is included.",
        "",
        "## Required Answers",
        f"1. Cohort measured: PRZ_VALID_AND_CONFIRMED_13 candidates, count={EXPECTED_COHORT_COUNT}.",
        f"2. Best entry model by avg_r_1R: {best_1r['entry_model']} (avg_r_1R={best_1r['avg_r_1R']}).",
        f"3. Best entry model by avg_r_2R: {best_2r['entry_model']} (avg_r_2R={best_2r['avg_r_2R']}).",
        f"4. Best entry model by avg_r_3R: {best_3r['entry_model']} (avg_r_3R={best_3r['avg_r_3R']}).",
        f"5. Best balance of population and avg_r: {balance['entry_model']} (qualified_pct={balance['qualified_pct']}, avg_r_1R={balance['avg_r_1R']}).",
        "6. Stability across BTCUSDT / ETHUSDT / SOLUSDT: see abcd_pnl_prototype_by_symbol.csv; treat as stable only if all three symbols retain comparable positive avg_r metrics.",
        "7. Stability across 2024 / 2025 / 2026: see abcd_pnl_prototype_by_year.csv; treat as stable only if all three years retain comparable positive avg_r metrics.",
        f"8. Candle-level simulation justification: {final_decision}.",
        "",
        f"Final decision: {final_decision}",
        "",
        "## Entry Model Summary",
    ]
    for row in by_entry_model_rows:
        lines.append(
            f"- {row['entry_model']}: qualified_count={row['qualified_count']}, qualified_pct={row['qualified_pct']}, avg_r_1R={row['avg_r_1R']}, avg_r_2R={row['avg_r_2R']}, avg_r_3R={row['avg_r_3R']}"
        )
    lines.extend(["", "## Stability Tables", "", "By-symbol and by-year CSV outputs provide the detailed stability breakdowns."])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(confluence_input: Path, feasibility_input: Path, entry_input: Path, output_root: Path) -> str:
    rows = _load_rows(confluence_input, feasibility_input, entry_input)
    by_entry_model_rows = [summarize_model(rows, model, threshold) for model, threshold in ENTRY_MODELS]
    final_decision = choose_decision(by_entry_model_rows)
    summary = {"cohort": "PRZ_VALID_AND_CONFIRMED_13", "cohort_count": len(rows), "final_decision": final_decision, **_flatten(by_entry_model_rows)}
    by_symbol_rows = _scope_rows(rows, "symbol", SYMBOLS)
    by_year_rows = _scope_rows(rows, "year", YEARS)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "abcd_pnl_prototype_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_pnl_prototype_by_entry_model.csv", by_entry_model_rows, BY_ENTRY_MODEL_FIELDS)
    _write_csv(output_root / "abcd_pnl_prototype_by_symbol.csv", by_symbol_rows, BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_pnl_prototype_by_year.csv", by_year_rows, BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_pnl_prototype_candidates.csv", candidate_rows(rows), CANDIDATE_FIELDS)
    write_report(output_root / "abcd_pnl_prototype_report.md", by_entry_model_rows, by_symbol_rows, by_year_rows, final_decision)
    return final_decision


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confluence-input", type=Path, default=CONFLUENCE_INPUT)
    parser.add_argument("--feasibility-input", type=Path, default=FEASIBILITY_INPUT)
    parser.add_argument("--entry-input", type=Path, default=ENTRY_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(run(args.confluence_input, args.feasibility_input, args.entry_input, args.output_root))


if __name__ == "__main__":
    main()
