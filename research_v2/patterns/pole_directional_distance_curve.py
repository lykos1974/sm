from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

from research_v2.patterns.pole_directional_decomposition import _infer_direction, _row_expectancy

NA_VALUES = {"", "na", "none", "null", "nan"}
CONTINUATION_OUTCOMES = {"BULLISH_CONTINUATION", "BEARISH_CONTINUATION"}
FAILURE_OUTCOME = "FAILED_REVERSAL"
DIRECTIONS = ("LONG", "SHORT")
DISTANCE_BUCKETS = ("distance=1", "distance=2", "distance=3", "distance=4", "distance>4", "NA")
SCOPES = (
    "all_observations",
    "enhanced=False",
    "enhanced=True",
    "enhanced=False AND retrace_ratio>1.0",
)
BASE_SCOPE = "all_observations"
FALSE_SCOPE = "enhanced=False"
TRUE_SCOPE = "enhanced=True"
QUALITY_SCOPE = "enhanced=False AND retrace_ratio>1.0"

CURVE_FIELDS = [
    "scope",
    "direction",
    "distance_bucket",
    "sample_size",
    "continuation_pct",
    "failure_pct",
    "expectancy_score",
    "asymmetry_score",
]
FLAG_FIELDS = ["check_name", "result", "details"]


@dataclass(frozen=True)
class PoleRow:
    direction: str
    distance_bucket: str
    enhanced: str
    retrace_ratio: float
    outcome: str
    expectancy: float


def _clean(value: Any, na_token: str = "NA") -> str:
    text = str(value or "").strip()
    return na_token if text.lower() in NA_VALUES else text


def _to_float(value: Any, default: float = 0.0) -> float:
    text = _clean(value, na_token="")
    try:
        return float(text) if text else default
    except ValueError:
        return default


def _safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _distance_bucket(value: Any) -> str:
    text = _clean(value)
    if text == "NA":
        return "NA"
    try:
        distance = int(float(text))
    except ValueError:
        return "NA"
    if distance <= 1:
        return "distance=1"
    if distance == 2:
        return "distance=2"
    if distance == 3:
        return "distance=3"
    if distance == 4:
        return "distance=4"
    return "distance>4"


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict[str, str | int | float]], fields: list[str]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _build_rows(labeled: list[dict[str, str]]) -> tuple[list[PoleRow], dict[str, int]]:
    diagnostics = {
        "rows_missing_direction": 0,
        "explicit_direction_rows": 0,
        "pattern_name_direction_rows": 0,
    }
    rows: list[PoleRow] = []
    for raw in labeled:
        direction, source = _infer_direction(raw)
        if not direction:
            diagnostics["rows_missing_direction"] += 1
            continue
        if source == "pattern_name":
            diagnostics["pattern_name_direction_rows"] += 1
        else:
            diagnostics["explicit_direction_rows"] += 1
        outcome = _clean(raw.get("outcome_class"), na_token="").upper()
        rows.append(
            PoleRow(
                direction=direction,
                distance_bucket=_distance_bucket(raw.get("opposing_pole_distance_columns")),
                enhanced=_clean(raw.get("enhanced_by_opposing_pole")),
                retrace_ratio=_to_float(raw.get("retrace_ratio")),
                outcome=outcome,
                expectancy=_row_expectancy(
                    outcome,
                    _to_float(raw.get("max_favorable_boxes")),
                    _to_float(raw.get("max_adverse_boxes")),
                ),
            )
        )
    return rows, diagnostics


def _scope_rows(rows: list[PoleRow], scope: str) -> list[PoleRow]:
    if scope == BASE_SCOPE:
        return rows
    if scope == FALSE_SCOPE:
        return [row for row in rows if row.enhanced.lower() == "false"]
    if scope == TRUE_SCOPE:
        return [row for row in rows if row.enhanced.lower() == "true"]
    if scope == QUALITY_SCOPE:
        return [row for row in rows if row.enhanced.lower() == "false" and row.retrace_ratio > 1.0]
    raise ValueError(f"unknown scope: {scope}")


def _metrics(rows: list[PoleRow]) -> dict[str, int | float]:
    sample_size = len(rows)
    continuation = sum(row.outcome in CONTINUATION_OUTCOMES for row in rows)
    failure = sum(row.outcome == FAILURE_OUTCOME for row in rows)
    continuation_pct = _safe_div(continuation, sample_size)
    failure_pct = _safe_div(failure, sample_size)
    return {
        "sample_size": sample_size,
        "continuation_pct": round(continuation_pct, 6),
        "failure_pct": round(failure_pct, 6),
        "expectancy_score": round(mean(row.expectancy for row in rows), 6) if rows else 0.0,
        "asymmetry_score": round(continuation_pct - failure_pct, 6),
    }


def _curve(rows: list[PoleRow]) -> list[dict[str, str | int | float]]:
    curve: list[dict[str, str | int | float]] = []
    for scope in SCOPES:
        scoped = _scope_rows(rows, scope)
        for direction in DIRECTIONS:
            directional = [row for row in scoped if row.direction == direction]
            for distance_bucket in DISTANCE_BUCKETS:
                metrics = _metrics([row for row in directional if row.distance_bucket == distance_bucket])
                curve.append({"scope": scope, "direction": direction, "distance_bucket": distance_bucket, **metrics})
    return curve


def _index_curve(curve: list[dict[str, str | int | float]]) -> dict[tuple[str, str, str], dict[str, str | int | float]]:
    return {(str(row["scope"]), str(row["direction"]), str(row["distance_bucket"])): row for row in curve}


def _strongest_bucket(index: dict[tuple[str, str, str], dict[str, str | int | float]], scope: str, direction: str) -> str:
    populated = [
        index[(scope, direction, bucket)]
        for bucket in DISTANCE_BUCKETS
        if int(index[(scope, direction, bucket)]["sample_size"]) > 0
    ]
    if not populated:
        return "NONE"
    strongest = max(populated, key=lambda row: (float(row["expectancy_score"]), float(row["asymmetry_score"]), int(row["sample_size"])))
    return str(strongest["distance_bucket"])


def _is_dominant(index: dict[tuple[str, str, str], dict[str, str | int | float]], scope: str, direction: str, min_sample: int) -> bool:
    target = index[(scope, direction, "distance=3")]
    if int(target["sample_size"]) < min_sample:
        return False
    populated_competitors = [
        index[(scope, direction, bucket)]
        for bucket in DISTANCE_BUCKETS
        if bucket != "distance=3" and int(index[(scope, direction, bucket)]["sample_size"]) >= min_sample
    ]
    return all(float(target["expectancy_score"]) > float(row["expectancy_score"]) for row in populated_competitors)


def _combined_metric(index: dict[tuple[str, str, str], dict[str, str | int | float]], scope: str, bucket: str) -> dict[str, float | int]:
    sides = [index[(scope, direction, bucket)] for direction in DIRECTIONS]
    sample_size = sum(int(row["sample_size"]) for row in sides)
    if not sample_size:
        return {"sample_size": 0, "expectancy_score": 0.0, "continuation_pct": 0.0, "asymmetry_score": 0.0}
    return {
        "sample_size": sample_size,
        "expectancy_score": round(sum(float(row["expectancy_score"]) * int(row["sample_size"]) for row in sides) / sample_size, 6),
        "continuation_pct": round(sum(float(row["continuation_pct"]) * int(row["sample_size"]) for row in sides) / sample_size, 6),
        "asymmetry_score": round(sum(float(row["asymmetry_score"]) * int(row["sample_size"]) for row in sides) / sample_size, 6),
    }


def _combined_dominant(index: dict[tuple[str, str, str], dict[str, str | int | float]], scope: str, min_sample: int) -> bool:
    target = _combined_metric(index, scope, "distance=3")
    if int(target["sample_size"]) < min_sample:
        return False
    competitors = [_combined_metric(index, scope, bucket) for bucket in DISTANCE_BUCKETS if bucket != "distance=3"]
    adequate = [row for row in competitors if int(row["sample_size"]) >= min_sample]
    return all(float(target["expectancy_score"]) > float(row["expectancy_score"]) for row in adequate)


def _is_competitive(target: dict[str, float | int], candidate: dict[str, float | int], threshold: float, min_sample: int) -> bool:
    return int(candidate["sample_size"]) >= min_sample and float(candidate["expectancy_score"]) >= float(target["expectancy_score"]) - threshold


def _detect(curve: list[dict[str, str | int | float]], min_sample: int, competitive_threshold: float) -> tuple[dict[str, str], list[dict[str, str]]]:
    index = _index_curve(curve)
    long_dominant = _is_dominant(index, FALSE_SCOPE, "LONG", min_sample)
    short_dominant = _is_dominant(index, FALSE_SCOPE, "SHORT", min_sample)
    distance_3_unique = _combined_dominant(index, FALSE_SCOPE, min_sample)
    false_target = _combined_metric(index, FALSE_SCOPE, "distance=3")
    true_target = _combined_metric(index, TRUE_SCOPE, "distance=3")
    quality_target = _combined_metric(index, QUALITY_SCOPE, "distance=3")
    enhanced_false_required = (
        int(false_target["sample_size"]) >= min_sample
        and (
            int(true_target["sample_size"]) < min_sample
            or float(false_target["expectancy_score"]) > float(true_target["expectancy_score"])
        )
    )
    retrace_gt_1_quality_boost = (
        int(quality_target["sample_size"]) >= min_sample
        and int(false_target["sample_size"]) >= min_sample
        and float(quality_target["expectancy_score"]) > float(false_target["expectancy_score"])
    )
    competitive = [
        bucket
        for bucket in DISTANCE_BUCKETS
        if bucket != "distance=3"
        and _is_competitive(false_target, _combined_metric(index, FALSE_SCOPE, bucket), competitive_threshold, min_sample)
    ]
    long_bucket = _strongest_bucket(index, FALSE_SCOPE, "LONG")
    short_bucket = _strongest_bucket(index, FALSE_SCOPE, "SHORT")
    motif = "distance=3 AND enhanced=False"
    if retrace_gt_1_quality_boost:
        motif += " AND retrace_ratio>1.0"
    if not distance_3_unique:
        motif = "NO UNIQUE distance=3 motif; retain distance curve as research-only"
    conclusion = {
        "distance_3_unique": "YES" if distance_3_unique else "NO",
        "long_distance_3_dominant": "YES" if long_dominant else "NO",
        "short_distance_3_dominant": "YES" if short_dominant else "NO",
        "any_other_distance_competitive": ", ".join(competitive) or "NONE",
        "enhanced_false_required": "YES" if enhanced_false_required else "NO",
        "retrace_gt_1_quality_boost": "YES" if retrace_gt_1_quality_boost else "NO",
        "strongest_LONG_distance_bucket": long_bucket,
        "strongest_SHORT_distance_bucket": short_bucket,
        "recommended_structural_motif_v1": motif,
    }
    sufficient_dist3 = int(false_target["sample_size"]) >= min_sample
    flags = [
        {"check_name": "distance_3_sample_sufficiency", "result": "OK" if sufficient_dist3 else "WARN", "details": f"enhanced=False distance=3 sample_size={false_target['sample_size']}; minimum={min_sample}"},
        {"check_name": "distance_3_unique", "result": conclusion["distance_3_unique"], "details": f"enhanced=False combined distance=3 expectancy={false_target['expectancy_score']}; competitive={conclusion['any_other_distance_competitive']}"},
        {"check_name": "long_distance_3_dominant", "result": conclusion["long_distance_3_dominant"], "details": f"strongest LONG bucket={long_bucket}"},
        {"check_name": "short_distance_3_dominant", "result": conclusion["short_distance_3_dominant"], "details": f"strongest SHORT bucket={short_bucket}"},
        {"check_name": "any_other_distance_competitive", "result": "YES" if competitive else "NO", "details": conclusion["any_other_distance_competitive"]},
        {"check_name": "enhanced_false_required", "result": conclusion["enhanced_false_required"], "details": f"enhanced=False distance=3 expectancy={false_target['expectancy_score']}; enhanced=True={true_target['expectancy_score']}"},
        {"check_name": "retrace_gt_1_quality_boost", "result": conclusion["retrace_gt_1_quality_boost"], "details": f"enhanced=False distance=3 expectancy={false_target['expectancy_score']}; retrace>1.0={quality_target['expectancy_score']}"},
    ]
    return conclusion, flags


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate directional opposing-pole distance curves (research-only).")
    parser.add_argument("--input-labeled-outcomes-csv", required=True)
    parser.add_argument("--input-directional-decomposition-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--min-sample", type=int, default=20)
    parser.add_argument("--competitive-threshold", type=float, default=0.05)
    args = parser.parse_args()

    labeled_path = Path(args.input_labeled_outcomes_csv)
    decomposition_root = Path(args.input_directional_decomposition_root)
    required_decomposition = decomposition_root / "pole_directional_breakdown.csv"
    if not required_decomposition.exists():
        raise FileNotFoundError(f"directional decomposition output not found: {required_decomposition}")

    labeled = _load_csv(labeled_path)
    decomposition_rows = _load_csv(required_decomposition)
    rows, diagnostics = _build_rows(labeled)
    curve = _curve(rows)
    conclusion, flags = _detect(curve, args.min_sample, args.competitive_threshold)

    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "pole_directional_distance_curve.csv", curve, CURVE_FIELDS)
    _write_csv(output_root / "pole_directional_distance_curve_flags.csv", flags, FLAG_FIELDS)

    with (output_root / "pole_directional_distance_curve_summary.md").open("w") as f:
        f.write("# PnF Pole Directional Distance Curve Validation (Research-Only)\n\n")
        f.write("No strategy changes, execution simulation, or TP/SL logic are included.\n")
        f.write("Direction uses explicit pattern direction columns when available, then falls back to LOW_POLE=LONG and HIGH_POLE=SHORT.\n\n")
        f.write("## Diagnostics\n")
        f.write(f"- labeled rows loaded: {len(labeled)}\n")
        f.write(f"- usable directional rows: {len(rows)}\n")
        f.write(f"- directional decomposition rows loaded: {len(decomposition_rows)}\n")
        for key, value in diagnostics.items():
            f.write(f"- {key.replace('_', ' ')}: {value}\n")
        f.write(f"- minimum sample threshold: {args.min_sample}\n")
        f.write(f"- competitive expectancy threshold: {args.competitive_threshold}\n\n")
        f.write("## Required conclusions\n")
        for key, value in conclusion.items():
            f.write(f"- {key}: {value}\n")
        f.write("\n## Directional distance curves\n")
        for scope in SCOPES:
            f.write(f"\n### {scope}\n")
            for direction in DIRECTIONS:
                f.write(f"\n#### {direction}\n\n")
                f.write("| distance bucket | sample size | continuation pct | failure pct | expectancy score | asymmetry score |\n")
                f.write("|---|---:|---:|---:|---:|---:|\n")
                for row in curve:
                    if row["scope"] == scope and row["direction"] == direction:
                        f.write(f"| {row['distance_bucket']} | {row['sample_size']} | {row['continuation_pct']} | {row['failure_pct']} | {row['expectancy_score']} | {row['asymmetry_score']} |\n")


if __name__ == "__main__":
    main()
