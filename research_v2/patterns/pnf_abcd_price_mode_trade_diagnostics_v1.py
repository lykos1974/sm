"""Research-only diagnostics for validated AB=CD PRICE_MODE Model C results.

This module consumes only existing local PRICE_MODE / execution-context /
PRZ-confluence artifacts and summarizes how already-classified winners and
losers differ. It intentionally does not inspect raw datasets, replay candles,
compute new trades, alter classification logic, optimize filters, model
fees/slippage/leverage, or make trading recommendations.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import _fmt, _parse_float

PRICE_MODE_INPUT = Path(
    "research_v2/patterns/abcd_price_mode_reality_sim_v1/abcd_price_mode_reality_candidates.csv"
)
EXECUTION_CONTEXT_INPUT = Path(
    "research_v2/patterns/abcd_execution_context_v1/abcd_execution_context_candidates.csv"
)
CONFLUENCE_INPUT = Path(
    "research_v2/patterns/abcd_prz_confirmation_confluence_local_v1/"
    "abcd_prz_confirmation_confluence_candidates.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_price_mode_trade_diagnostics_v1")

TARGETS = (1, 2, 3)
TARGET_FIRST = "TARGET_FIRST"
STOP_FIRST = "STOP_FIRST"
FINAL_SUPPORT = "DIAGNOSTICS_SUPPORT_FILTER_RESEARCH"
FINAL_REJECT = "DIAGNOSTICS_DO_NOT_SUPPORT_FILTER_RESEARCH"
MIN_MEANINGFUL_COHORT_SIZE = 20
MEANINGFUL_WIN_RATE_DELTA = 0.10

SUMMARY_FIELDS = [
    "total_candidates",
    "target_1R_target_first_count",
    "target_1R_stop_first_count",
    "target_1R_other_count",
    "target_1R_win_rate",
    "target_2R_target_first_count",
    "target_2R_stop_first_count",
    "target_2R_other_count",
    "target_2R_win_rate",
    "target_3R_target_first_count",
    "target_3R_stop_first_count",
    "target_3R_other_count",
    "target_3R_win_rate",
    "baseline_2R_win_rate",
    "meaningfully_above_baseline_cohorts",
    "meaningfully_below_baseline_cohorts",
    "clean_winner_count",
    "clean_loser_count",
    "loser_cluster_notes",
    "diagnostic_filter_next_phase_testing",
    "final_decision",
]

COHORT_FIELDS = [
    "cohort_type",
    "cohort_value",
    "candidate_count",
    "target_1R_target_first_count",
    "target_1R_stop_first_count",
    "target_1R_other_count",
    "target_1R_win_rate",
    "target_2R_target_first_count",
    "target_2R_stop_first_count",
    "target_2R_other_count",
    "target_2R_win_rate",
    "target_2R_delta_vs_baseline",
    "target_2R_baseline_flag",
    "target_3R_target_first_count",
    "target_3R_stop_first_count",
    "target_3R_other_count",
    "target_3R_win_rate",
]

SAMPLE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "post_d_reaction_direction",
    "prz_class",
    "first_post_d_reaction_boxes",
    "first_post_d_reaction_boxes_bucket",
    "risk_price",
    "risk_price_bucket",
    "retrace_pct_of_first_reaction",
    "retrace_pct_of_first_reaction_bucket",
    "target_1R_classification",
    "target_2R_classification",
    "target_3R_classification",
    "entry_price",
    "stop_price",
    "target_1R_price",
    "target_2R_price",
    "target_3R_price",
]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _require_fields(path: Path, fieldnames: Sequence[str] | None, required: Iterable[str]) -> None:
    if not fieldnames:
        raise ValueError(f"{path}: expected CSV header")
    missing = [field for field in required if field not in fieldnames]
    if missing:
        raise ValueError(f"{path}: missing required fields: {', '.join(missing)}")


def _read_by_candidate(path: Path, required: Sequence[str]) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("candidate_id", *required))
        for line_number, row in enumerate(reader, start=2):
            candidate_id = _text(row.get("candidate_id"))
            if not candidate_id:
                raise ValueError(f"{path}:{line_number}: missing candidate_id")
            if candidate_id in rows:
                raise ValueError(f"{path}:{line_number}: duplicate candidate_id {candidate_id}")
            rows[candidate_id] = {key: _text(value) for key, value in row.items()}
    return rows


def _bucket(value: Any, edges: Sequence[float], labels: Sequence[str]) -> str:
    parsed = _parse_float(value)
    if parsed is None:
        return "UNKNOWN"
    for edge, label in zip(edges, labels):
        if parsed <= edge:
            return label
    return labels[-1]


def _boxes_bucket(value: Any) -> str:
    return _bucket(value, (13, 20, 30, 50), ("<=13", "13-20", "20-30", "30-50", ">50"))


def _risk_bucket(value: Any) -> str:
    return _bucket(value, (50, 100, 250, 500), ("<=50", "50-100", "100-250", "250-500", ">500"))


def _retrace_bucket(value: Any) -> str:
    return _bucket(value, (0.382, 0.5, 0.618, 0.786), ("<=0.382", "0.382-0.500", "0.500-0.618", "0.618-0.786", ">0.786"))


def _win_rate(rows: Sequence[dict[str, str]], target: int) -> tuple[int, int, int, str]:
    counts = Counter(row.get(f"target_{target}R_classification", "") for row in rows)
    wins = counts[TARGET_FIRST]
    losses = counts[STOP_FIRST]
    other = len(rows) - wins - losses
    denominator = wins + losses
    return wins, losses, other, (_fmt(wins / denominator) if denominator else "")


def _cohort_row(cohort_type: str, cohort_value: str, rows: Sequence[dict[str, str]], baseline_2r: float | None) -> dict[str, Any]:
    out: dict[str, Any] = {"cohort_type": cohort_type, "cohort_value": cohort_value, "candidate_count": len(rows)}
    for target in TARGETS:
        wins, losses, other, rate = _win_rate(rows, target)
        out[f"target_{target}R_target_first_count"] = wins
        out[f"target_{target}R_stop_first_count"] = losses
        out[f"target_{target}R_other_count"] = other
        out[f"target_{target}R_win_rate"] = rate
    rate_2r = _parse_float(out["target_2R_win_rate"])
    if baseline_2r is None or rate_2r is None:
        out["target_2R_delta_vs_baseline"] = ""
        out["target_2R_baseline_flag"] = "INSUFFICIENT"
    else:
        delta = rate_2r - baseline_2r
        out["target_2R_delta_vs_baseline"] = _fmt(delta)
        if len(rows) >= MIN_MEANINGFUL_COHORT_SIZE and delta >= MEANINGFUL_WIN_RATE_DELTA:
            out["target_2R_baseline_flag"] = "MEANINGFULLY_ABOVE_BASELINE"
        elif len(rows) >= MIN_MEANINGFUL_COHORT_SIZE and delta <= -MEANINGFUL_WIN_RATE_DELTA:
            out["target_2R_baseline_flag"] = "MEANINGFULLY_BELOW_BASELINE"
        else:
            out["target_2R_baseline_flag"] = "NEAR_BASELINE_OR_SMALL_SAMPLE"
    return out


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _load_joined(price_path: Path, context_path: Path, confluence_path: Path) -> list[dict[str, str]]:
    price = _read_by_candidate(price_path, [f"target_{target}R_classification" for target in TARGETS])
    context = _read_by_candidate(
        context_path,
        ["symbol", "year", "post_d_reaction_direction", "first_post_d_reaction_boxes", "risk_price", "retrace_pct_of_first_reaction"],
    )
    confluence = _read_by_candidate(confluence_path, ["prz_class"])
    if set(price) - set(context) or set(price) - set(confluence):
        raise ValueError("diagnostic inputs do not support strict candidate_id join")
    rows: list[dict[str, str]] = []
    for candidate_id in sorted(price):
        row = {**context[candidate_id], **confluence[candidate_id], **price[candidate_id]}
        row["candidate_id"] = candidate_id
        row["first_post_d_reaction_boxes_bucket"] = _boxes_bucket(row.get("first_post_d_reaction_boxes"))
        row["risk_price_bucket"] = _risk_bucket(row.get("risk_price"))
        row["retrace_pct_of_first_reaction_bucket"] = _retrace_bucket(row.get("retrace_pct_of_first_reaction"))
        rows.append(row)
    return rows


def _samples(rows: Sequence[dict[str, str]], classification: str) -> list[dict[str, str]]:
    matched = [row for row in rows if all(row.get(f"target_{target}R_classification") == classification for target in TARGETS)]
    return sorted(
        matched,
        key=lambda row: (
            row.get("symbol", ""),
            row.get("year", ""),
            row.get("post_d_reaction_direction", ""),
            row.get("candidate_id", ""),
        ),
    )[:25]


def _cluster_notes(rows: Sequence[dict[str, str]], baseline_2r: float | None) -> str:
    loser_rows = [row for row in rows if row.get("target_2R_classification") == STOP_FIRST]
    if not loser_rows:
        return "No 2R STOP_FIRST losers available for clustering diagnostics."
    parts = []
    for field in ("symbol", "year", "post_d_reaction_direction", "risk_price_bucket", "retrace_pct_of_first_reaction_bucket"):
        value, count = Counter(row.get(field, "UNKNOWN") for row in loser_rows).most_common(1)[0]
        share = count / len(loser_rows)
        parts.append(f"{field}={value} contains {_fmt(share)} of 2R STOP_FIRST rows")
    if baseline_2r is not None:
        parts.append(f"baseline 2R win rate is {_fmt(baseline_2r)}")
    return "; ".join(parts) + "."


def _write_report(path: Path, summary: dict[str, Any], cohorts: Sequence[dict[str, Any]]) -> None:
    best = [row for row in cohorts if row.get("target_2R_baseline_flag") == "MEANINGFULLY_ABOVE_BASELINE"][:10]
    worst = [row for row in cohorts if row.get("target_2R_baseline_flag") == "MEANINGFULLY_BELOW_BASELINE"][:10]
    lines = [
        "# AB=CD PRICE_MODE Trade Diagnostics v1",
        "",
        "Research-only diagnostic pass over already validated PRICE_MODE Model C artifacts.",
        "No raw datasets were inspected, no candles were replayed, no trades were recomputed, and no optimization or trade recommendation is made.",
        "",
        "## 1. Overall 1R/2R/3R win rates",
    ]
    for target in TARGETS:
        lines.append(
            f"- {target}R: win_rate={summary[f'target_{target}R_win_rate']} "
            f"TARGET_FIRST={summary[f'target_{target}R_target_first_count']} "
            f"STOP_FIRST={summary[f'target_{target}R_stop_first_count']} "
            f"other={summary[f'target_{target}R_other_count']}"
        )
    lines += ["", "## 2. Best 2R cohorts"]
    lines += [f"- {row['cohort_type']}={row['cohort_value']}: n={row['candidate_count']}, 2R_win_rate={row['target_2R_win_rate']}, delta={row['target_2R_delta_vs_baseline']}" for row in best] or ["- None meeting the non-optimized diagnostic threshold."]
    lines += ["", "## 3. Worst 2R cohorts"]
    lines += [f"- {row['cohort_type']}={row['cohort_value']}: n={row['candidate_count']}, 2R_win_rate={row['target_2R_win_rate']}, delta={row['target_2R_delta_vs_baseline']}" for row in worst] or ["- None meeting the non-optimized diagnostic threshold."]
    lines += [
        "",
        "## 4. Loser clustering diagnostics",
        f"- {summary['loser_cluster_notes']}",
        "",
        "## 5. Next-phase diagnostic-filter research",
        f"- {summary['diagnostic_filter_next_phase_testing']}",
        "",
        "## Final decision",
        str(summary["final_decision"]),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def run(price_path: Path, context_path: Path, confluence_path: Path, output_root: Path) -> dict[str, Any]:
    rows = _load_joined(price_path, context_path, confluence_path)
    summary: dict[str, Any] = {"total_candidates": len(rows)}
    for target in TARGETS:
        wins, losses, other, rate = _win_rate(rows, target)
        summary[f"target_{target}R_target_first_count"] = wins
        summary[f"target_{target}R_stop_first_count"] = losses
        summary[f"target_{target}R_other_count"] = other
        summary[f"target_{target}R_win_rate"] = rate
    baseline_2r = _parse_float(summary["target_2R_win_rate"])
    summary["baseline_2R_win_rate"] = summary["target_2R_win_rate"]

    cohort_specs = (
        "symbol",
        "year",
        "post_d_reaction_direction",
        "prz_class",
        "first_post_d_reaction_boxes_bucket",
        "risk_price_bucket",
        "retrace_pct_of_first_reaction_bucket",
    )
    cohort_rows: list[dict[str, Any]] = []
    for field in cohort_specs:
        grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in rows:
            grouped[row.get(field) or "UNKNOWN"].append(row)
        for value, grouped_rows in grouped.items():
            cohort_rows.append(_cohort_row(field, value, grouped_rows, baseline_2r))
    cohort_rows.sort(key=lambda row: (row["target_2R_baseline_flag"] != "MEANINGFULLY_ABOVE_BASELINE", -(_parse_float(row["target_2R_delta_vs_baseline"]) or -999), row["cohort_type"], row["cohort_value"]))

    above = [row for row in cohort_rows if row.get("target_2R_baseline_flag") == "MEANINGFULLY_ABOVE_BASELINE"]
    below = [row for row in cohort_rows if row.get("target_2R_baseline_flag") == "MEANINGFULLY_BELOW_BASELINE"]
    summary["meaningfully_above_baseline_cohorts"] = len(above)
    summary["meaningfully_below_baseline_cohorts"] = len(below)
    winners = _samples(rows, TARGET_FIRST)
    losers = _samples(rows, STOP_FIRST)
    summary["clean_winner_count"] = len(winners)
    summary["clean_loser_count"] = len(losers)
    summary["loser_cluster_notes"] = _cluster_notes(rows, baseline_2r)
    summary["diagnostic_filter_next_phase_testing"] = (
        "Yes: at least one sufficiently sized cohort is meaningfully above or below the overall 2R baseline; "
        "carry only these predeclared diagnostics into a separate next-phase test."
        if above or below
        else "No: no sufficiently sized cohort separated meaningfully from the overall 2R baseline."
    )
    summary["final_decision"] = FINAL_SUPPORT if above or below else FINAL_REJECT

    _write_csv(output_root / "abcd_price_mode_trade_diagnostics_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_price_mode_trade_diagnostics_by_cohort.csv", cohort_rows, COHORT_FIELDS)
    _write_csv(output_root / "abcd_price_mode_trade_diagnostics_winners_sample.csv", winners, SAMPLE_FIELDS)
    _write_csv(output_root / "abcd_price_mode_trade_diagnostics_losers_sample.csv", losers, SAMPLE_FIELDS)
    _write_report(output_root / "abcd_price_mode_trade_diagnostics_report.md", summary, cohort_rows)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--price-mode-input", type=Path, default=PRICE_MODE_INPUT)
    parser.add_argument("--execution-context-input", type=Path, default=EXECUTION_CONTEXT_INPUT)
    parser.add_argument("--confluence-input", type=Path, default=CONFLUENCE_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(args.price_mode_input, args.execution_context_input, args.confluence_input, args.output_root)
    print(summary["final_decision"])


if __name__ == "__main__":
    main()
