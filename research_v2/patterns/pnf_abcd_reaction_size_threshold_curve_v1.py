"""Research-only AB=CD PRICE_MODE reaction-size threshold curve.

This module consumes only existing PRICE_MODE result classifications and the
execution-context artifact, then measures how cumulative
``first_post_d_reaction_boxes`` thresholds relate to already-computed
PRICE_MODE target-first rates. It does not inspect raw datasets, replay
candles, recompute trades, change classifications, optimize parameters, or
produce strategy recommendations.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import _fmt, _parse_float

PRICE_MODE_INPUT = Path(
    "research_v2/patterns/abcd_price_mode_reality_sim_v1/abcd_price_mode_reality_candidates.csv"
)
EXECUTION_CONTEXT_INPUT = Path(
    "research_v2/patterns/abcd_execution_context_v1/abcd_execution_context_candidates.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_reaction_size_threshold_curve_v1")

TARGETS = (1, 2, 3)
THRESHOLDS = (5, 8, 10, 12, 15, 18, 20, 25, 30, 35, 40, 50)
TARGET_FIRST = "TARGET_FIRST"
STOP_FIRST = "STOP_FIRST"
MIN_RETAINED_CANDIDATES = 100
MIN_SAMPLE_COLLAPSE_RETAINED_FRACTION = 0.25
FINAL_SUPPORTED = "REACTION_SIZE_THRESHOLD_RESEARCH_SUPPORTED"
FINAL_NOT_SUPPORTED = "REACTION_SIZE_THRESHOLD_RESEARCH_NOT_SUPPORTED"

CURVE_FIELDS = [
    "threshold",
    "candidate_count",
    "retained_fraction",
    "target_1R_target_first_count",
    "target_1R_stop_first_count",
    "target_1R_other_count",
    "target_1R_win_rate",
    "target_1R_delta_vs_overall_baseline",
    "target_2R_target_first_count",
    "target_2R_stop_first_count",
    "target_2R_other_count",
    "target_2R_win_rate",
    "target_2R_delta_vs_overall_baseline",
    "target_3R_target_first_count",
    "target_3R_stop_first_count",
    "target_3R_other_count",
    "target_3R_win_rate",
    "target_3R_delta_vs_overall_baseline",
    "sample_collapse_flag",
]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _require_fields(
    path: Path, fieldnames: Sequence[str] | None, required: Iterable[str]
) -> None:
    if not fieldnames:
        raise ValueError(f"{path}: expected CSV header")
    missing = [field for field in required if field not in fieldnames]
    if missing:
        raise ValueError(f"{path}: missing required fields: {', '.join(missing)}")


def _read_by_candidate(
    path: Path, required: Sequence[str]
) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("candidate_id", *required))
        for line_number, row in enumerate(reader, start=2):
            candidate_id = _text(row.get("candidate_id"))
            if not candidate_id:
                raise ValueError(f"{path}:{line_number}: missing candidate_id")
            if candidate_id in rows:
                raise ValueError(
                    f"{path}:{line_number}: duplicate candidate_id {candidate_id}"
                )
            rows[candidate_id] = {key: _text(value) for key, value in row.items()}
    return rows


def _load_joined(price_input: Path, context_input: Path) -> list[dict[str, str]]:
    price = _read_by_candidate(
        price_input, [f"target_{target}R_classification" for target in TARGETS]
    )
    context = _read_by_candidate(context_input, ["first_post_d_reaction_boxes"])
    missing_context = sorted(set(price) - set(context))
    if missing_context:
        sample = ", ".join(missing_context[:5])
        raise ValueError(
            f"execution-context input missing {len(missing_context)} PRICE_MODE candidate_id values; sample: {sample}"
        )

    rows: list[dict[str, str]] = []
    for candidate_id in sorted(price):
        boxes = _parse_float(context[candidate_id].get("first_post_d_reaction_boxes"))
        if boxes is None:
            continue
        rows.append(
            {
                "candidate_id": candidate_id,
                "first_post_d_reaction_boxes": _fmt(boxes),
                **{
                    f"target_{target}R_classification": price[candidate_id][
                        f"target_{target}R_classification"
                    ]
                    for target in TARGETS
                },
            }
        )
    return rows


def _target_counts(
    rows: Sequence[dict[str, str]], target: int
) -> tuple[int, int, int, float | None]:
    wins = sum(
        1 for row in rows if row.get(f"target_{target}R_classification") == TARGET_FIRST
    )
    losses = sum(
        1 for row in rows if row.get(f"target_{target}R_classification") == STOP_FIRST
    )
    other = len(rows) - wins - losses
    denominator = wins + losses
    return wins, losses, other, (wins / denominator if denominator else None)


def _curve_row(
    threshold: int,
    rows: Sequence[dict[str, str]],
    baseline_rates: dict[int, float | None],
    total: int,
) -> dict[str, Any]:
    retained = [
        row
        for row in rows
        if (_parse_float(row.get("first_post_d_reaction_boxes")) or 0.0) <= threshold
    ]
    retained_fraction = (len(retained) / total) if total else 0.0
    out: dict[str, Any] = {
        "threshold": f"<={threshold}",
        "candidate_count": len(retained),
        "retained_fraction": _fmt(retained_fraction),
        "sample_collapse_flag": (
            "SAMPLE_COLLAPSE"
            if retained_fraction < MIN_SAMPLE_COLLAPSE_RETAINED_FRACTION
            else ""
        ),
    }
    for target in TARGETS:
        wins, losses, other, rate = _target_counts(retained, target)
        baseline = baseline_rates[target]
        out[f"target_{target}R_target_first_count"] = wins
        out[f"target_{target}R_stop_first_count"] = losses
        out[f"target_{target}R_other_count"] = other
        out[f"target_{target}R_win_rate"] = _fmt(rate) if rate is not None else ""
        out[f"target_{target}R_delta_vs_overall_baseline"] = (
            _fmt(rate - baseline) if rate is not None and baseline is not None else ""
        )
    return out


def _write_csv(
    path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def _best_threshold(
    curve: Sequence[dict[str, Any]], target: int
) -> dict[str, Any] | None:
    eligible = [
        row
        for row in curve
        if int(row["candidate_count"]) >= MIN_RETAINED_CANDIDATES
        and _parse_float(row.get(f"target_{target}R_win_rate")) is not None
    ]
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda row: (
            _parse_float(row[f"target_{target}R_win_rate"]) or -1.0,
            int(row["candidate_count"]),
        ),
    )


def _appears_monotonic(curve: Sequence[dict[str, Any]]) -> bool:
    for target in TARGETS:
        rates = [_parse_float(row.get(f"target_{target}R_win_rate")) for row in curve]
        cleaned = [rate for rate in rates if rate is not None]
        if len(cleaned) > 1 and any(
            later < earlier for earlier, later in zip(cleaned, cleaned[1:])
        ):
            return False
    return True


def _sample_collapse_invalidates(
    best_2r: dict[str, Any] | None, best_3r: dict[str, Any] | None
) -> bool:
    best_rows = [row for row in (best_2r, best_3r) if row is not None]
    if not best_rows:
        return True
    return any(
        row.get("sample_collapse_flag") == "SAMPLE_COLLAPSE" for row in best_rows
    )


def _write_report(
    path: Path,
    baseline_rates: dict[int, float | None],
    curve: Sequence[dict[str, Any]],
    final_decision: str,
) -> None:
    best_2r = _best_threshold(curve, 2)
    best_3r = _best_threshold(curve, 3)
    monotonic = _appears_monotonic(curve)
    collapse_invalidates = _sample_collapse_invalidates(best_2r, best_3r)

    def answer_best(row: dict[str, Any] | None, target: int) -> str:
        if row is None:
            return (
                f"No threshold retained at least {MIN_RETAINED_CANDIDATES} candidates."
            )
        return f"{row['threshold']} with {row['candidate_count']} candidates and {row[f'target_{target}R_win_rate']} win rate."

    lines = [
        "# AB=CD Reaction Size Threshold Curve — PRICE_MODE",
        "",
        "Research-only threshold curve over already-computed PRICE_MODE classifications joined to execution-context first_post_d_reaction_boxes. No raw datasets, candle replay, trade recomputation, classification changes, optimization, or strategy recommendations are included.",
        "",
        "## Required Answers",
        f"1. Overall baseline 1R/2R/3R: {_fmt(baseline_rates[1]) if baseline_rates[1] is not None else ''} / {_fmt(baseline_rates[2]) if baseline_rates[2] is not None else ''} / {_fmt(baseline_rates[3]) if baseline_rates[3] is not None else ''}.",
        f"2. Highest 2R win rate with at least {MIN_RETAINED_CANDIDATES} candidates: {answer_best(best_2r, 2)}",
        f"3. Highest 3R win rate with at least {MIN_RETAINED_CANDIDATES} candidates: {answer_best(best_3r, 3)}",
        f"4. Win rate improvement appears monotonic: {'YES' if monotonic else 'NO'}.",
        f"5. Sample collapse invalidates apparent improvements: {'YES' if collapse_invalidates else 'NO'}.",
        "",
        "## Final Decision",
        final_decision,
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(price_input: Path, context_input: Path, output_root: Path) -> dict[str, Any]:
    rows = _load_joined(price_input, context_input)
    baseline_rates = {target: _target_counts(rows, target)[3] for target in TARGETS}
    curve = [
        _curve_row(threshold, rows, baseline_rates, len(rows))
        for threshold in THRESHOLDS
    ]
    best_2r = _best_threshold(curve, 2)
    best_3r = _best_threshold(curve, 3)
    monotonic = _appears_monotonic(curve)
    collapse_invalidates = _sample_collapse_invalidates(best_2r, best_3r)
    has_positive_supported_delta = any(
        (_parse_float(row.get("target_2R_delta_vs_overall_baseline")) or 0.0) > 0.0
        or (_parse_float(row.get("target_3R_delta_vs_overall_baseline")) or 0.0) > 0.0
        for row in (best_2r, best_3r)
        if row is not None
    )
    final_decision = (
        FINAL_SUPPORTED
        if has_positive_supported_delta and monotonic and not collapse_invalidates
        else FINAL_NOT_SUPPORTED
    )
    _write_csv(
        output_root / "abcd_reaction_size_threshold_curve.csv", curve, CURVE_FIELDS
    )
    _write_report(
        output_root / "abcd_reaction_size_threshold_curve_report.md",
        baseline_rates,
        curve,
        final_decision,
    )
    return {
        "baseline_rates": baseline_rates,
        "curve": curve,
        "final_decision": final_decision,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--price-input", type=Path, default=PRICE_MODE_INPUT)
    parser.add_argument("--context-input", type=Path, default=EXECUTION_CONTEXT_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(args.price_input, args.context_input, args.output_root)


if __name__ == "__main__":
    main()
