"""Research-only AB=CD confirmation threshold audit.

This module consumes the pre-existing bounded D-reaction candidate artifact from
``abcd_d_mfe_local_v1`` and evaluates whether early post-D confirmation
thresholds separate stronger bounded reactions from weaker ones.

It intentionally uses only existing bounded D-reaction fields. It does not
inspect raw datasets, reconstruct ABCDs, use FAST artifacts, create a strategy,
or define entries, exits, stops, targets, PnL, expectancy, profitability, or
trading rules.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_INPUT_PATH = Path("research_v2/patterns/abcd_d_mfe_local_v1/abcd_d_mfe_candidates.csv")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_confirmation_threshold_local_v1")
EXPECTED_ROWS = 7823
THRESHOLDS = (3, 5, 8, 13, 21)
QUALITY_ORDER = ("STRONG", "MEDIUM", "WEAK")
DECISION_NEXT = "CONFIRMATION_THRESHOLD_WORTH_NEXT_PHASE"
DECISION_STOP = "CONFIRMATION_THRESHOLD_NOT_USEFUL"

SUMMARY_FIELDS = [
    "threshold_first_post_d_reaction_boxes_gte",
    "total_rows_loaded",
    "measured_rows",
    "unmeasured_rows",
    "qualified_count",
    "qualified_pct",
    "median_max_favorable_before_first_adverse_pivot",
    "avg_max_favorable_before_first_adverse_pivot",
    "strong_count",
    "strong_pct",
    "medium_count",
    "medium_pct",
    "weak_count",
    "weak_pct",
]
BY_SYMBOL_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        parsed = float(_text(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _median(values: Iterable[float]) -> float | None:
    clean = list(values)
    return statistics.median(clean) if clean else None


def _average(values: Iterable[float]) -> float | None:
    clean = list(values)
    return statistics.fmean(clean) if clean else None


def _classify_quality(max_favorable: float | None) -> str:
    if max_favorable is None:
        return "UNMEASURED"
    if max_favorable >= 13:
        return "STRONG"
    if max_favorable >= 8:
        return "MEDIUM"
    return "WEAK"


def _read_candidates(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{path.as_posix()} has no CSV header")
        required = {
            "first_post_d_reaction_boxes",
            "max_favorable_before_first_adverse_pivot",
            "symbol",
            "year",
        }
        missing = sorted(required - set(reader.fieldnames))
        if missing:
            raise ValueError(f"{path.as_posix()} is missing required columns: {', '.join(missing)}")
        rows = list(reader)
    if len(rows) != EXPECTED_ROWS:
        raise ValueError(f"input row count changed: expected {EXPECTED_ROWS}, observed {len(rows)} at {path.as_posix()}")

    candidates: list[dict[str, Any]] = []
    for row in rows:
        first_reaction = _parse_float(row.get("first_post_d_reaction_boxes"))
        max_favorable = _parse_float(row.get("max_favorable_before_first_adverse_pivot"))
        candidates.append(
            {
                **row,
                "symbol": _text(row.get("symbol")),
                "year": _text(row.get("year")),
                "first_post_d_reaction_boxes_value": first_reaction,
                "max_favorable_before_first_adverse_pivot_value": max_favorable,
                "bounded_reaction_quality": _classify_quality(max_favorable),
            }
        )
    return candidates


def _measured_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("bounded_reaction_quality") in QUALITY_ORDER]


def _threshold_summary(rows: Sequence[dict[str, Any]], threshold: int, totals: dict[str, int]) -> dict[str, Any]:
    measured = _measured_rows(rows)
    qualified = [
        row
        for row in measured
        if (row.get("first_post_d_reaction_boxes_value") is not None and row["first_post_d_reaction_boxes_value"] >= threshold)
    ]
    max_favorable_values = [row["max_favorable_before_first_adverse_pivot_value"] for row in qualified]
    counts = {quality: sum(1 for row in qualified if row.get("bounded_reaction_quality") == quality) for quality in QUALITY_ORDER}
    qualified_count = len(qualified)
    return {
        "threshold_first_post_d_reaction_boxes_gte": threshold,
        "total_rows_loaded": totals["total_rows_loaded"],
        "measured_rows": totals["measured_rows"],
        "unmeasured_rows": totals["unmeasured_rows"],
        "qualified_count": qualified_count,
        "qualified_pct": _pct(qualified_count, totals["measured_rows"]),
        "median_max_favorable_before_first_adverse_pivot": _fmt(_median(max_favorable_values)),
        "avg_max_favorable_before_first_adverse_pivot": _fmt(_average(max_favorable_values)),
        "strong_count": counts["STRONG"],
        "strong_pct": _pct(counts["STRONG"], qualified_count),
        "medium_count": counts["MEDIUM"],
        "medium_pct": _pct(counts["MEDIUM"], qualified_count),
        "weak_count": counts["WEAK"],
        "weak_pct": _pct(counts["WEAK"], qualified_count),
    }


def _totals(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    measured = len(_measured_rows(rows))
    return {"total_rows_loaded": len(rows), "measured_rows": measured, "unmeasured_rows": len(rows) - measured}


def summarize_thresholds(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    totals = _totals(rows)
    return [_threshold_summary(rows, threshold, totals) for threshold in THRESHOLDS]


def _group_rows(rows: Sequence[dict[str, Any]], field: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_text(row.get(field))].append(row)
    return dict(grouped)


def _group_threshold_rows(rows: Sequence[dict[str, Any]], field: str, output_field: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for group, group_rows in sorted(_group_rows(rows, field).items(), key=lambda item: item[0]):
        for summary in summarize_thresholds(group_rows):
            output.append({output_field: group, **summary})
    return output


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _as_float(summary: dict[str, Any], field: str) -> float:
    parsed = _parse_float(summary.get(field))
    return parsed if parsed is not None else -1.0


def _best_by_strong(summary_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return max(summary_rows, key=lambda row: (_as_float(row, "strong_pct"), int(row["qualified_count"])))


def _best_by_balance(summary_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return max(
        summary_rows,
        key=lambda row: (_as_float(row, "strong_pct") * _as_float(row, "qualified_pct"), _as_float(row, "strong_pct")),
    )


def _separates_strong_from_weak(summary_rows: Sequence[dict[str, Any]]) -> str:
    baseline = next(row for row in summary_rows if int(row["threshold_first_post_d_reaction_boxes_gte"]) == THRESHOLDS[0])
    best = _best_by_balance(summary_rows)
    return "YES" if _as_float(best, "strong_pct") > _as_float(baseline, "strong_pct") and _as_float(best, "weak_pct") < _as_float(baseline, "weak_pct") else "NO"


def _decision(summary_rows: Sequence[dict[str, Any]]) -> str:
    return DECISION_NEXT if _separates_strong_from_weak(summary_rows) == "YES" else DECISION_STOP


def _threshold_line(row: dict[str, Any]) -> str:
    return (
        f">= {row['threshold_first_post_d_reaction_boxes_gte']}: "
        f"qualified={row['qualified_count']} ({row['qualified_pct']}), "
        f"median bounded reaction={row['median_max_favorable_before_first_adverse_pivot']}, "
        f"avg bounded reaction={row['avg_max_favorable_before_first_adverse_pivot']}, "
        f"STRONG={row['strong_pct']}, MEDIUM={row['medium_pct']}, WEAK={row['weak_pct']}"
    )


def _report(summary_rows: Sequence[dict[str, Any]]) -> str:
    totals = summary_rows[0]
    best_strong = _best_by_strong(summary_rows)
    best_balance = _best_by_balance(summary_rows)
    separates = _separates_strong_from_weak(summary_rows)
    decision = _decision(summary_rows)
    threshold_lines = "\n".join(f"- {_threshold_line(row)}" for row in summary_rows)
    return f"""# AB=CD Confirmation Threshold Audit

Research-only audit using the existing bounded D-reaction candidate artifact. Unmeasured rows are reported separately and excluded from threshold statistics. No raw datasets were inspected, no ABCDs were reconstructed, no FAST artifacts were used, and no strategy, entries, exits, stops, targets, PnL, expectancy, profitability, or trading rules were created.

## Required answers

1. Total rows loaded: {totals['total_rows_loaded']}
2. Measured rows: {totals['measured_rows']}
3. Unmeasured rows: {totals['unmeasured_rows']}
4. Best threshold by STRONG%: >= {best_strong['threshold_first_post_d_reaction_boxes_gte']} with STRONG% {best_strong['strong_pct']} and qualified_count {best_strong['qualified_count']}
5. Best threshold by balance of population size and STRONG%: >= {best_balance['threshold_first_post_d_reaction_boxes_gte']} with STRONG% {best_balance['strong_pct']}, qualified_pct {best_balance['qualified_pct']}, and qualified_count {best_balance['qualified_count']}
6. Does confirmation threshold separate STRONG from WEAK? {separates}
7. Final research-only decision: {decision}

## Threshold summary

{threshold_lines}

## Validation

- Input rows required: {EXPECTED_ROWS}
- Threshold stats denominator: measured rows only
- Unmeasured rows: excluded from threshold stats, not audit failures
"""


def run(input_path: Path = DEFAULT_INPUT_PATH, output_root: Path = DEFAULT_OUTPUT_ROOT) -> list[dict[str, Any]]:
    rows = _read_candidates(input_path)
    summary_rows = summarize_thresholds(rows)
    _write_csv(output_root / "abcd_confirmation_threshold_summary.csv", summary_rows, SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_confirmation_threshold_by_symbol.csv", _group_threshold_rows(rows, "symbol", "symbol"), BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_confirmation_threshold_by_year.csv", _group_threshold_rows(rows, "year", "year"), BY_YEAR_FIELDS)
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "abcd_confirmation_threshold_report.md").write_text(_report(summary_rows), encoding="utf-8")
    return summary_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only AB=CD confirmation threshold audit.")
    parser.add_argument("--input-path", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    run(args.input_path, args.output_root)


if __name__ == "__main__":
    main()
