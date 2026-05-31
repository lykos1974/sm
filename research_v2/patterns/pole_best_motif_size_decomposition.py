from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

from research_v2.patterns.pole_directional_decomposition import (
    CHRONOLOGICAL_INDEX_COLUMNS,
    CONTINUATION_OUTCOMES,
    FAILURE_OUTCOME,
    _infer_direction,
    _row_expectancy,
)

NA_VALUES = {"", "na", "none", "null", "nan"}
DIRECTIONS = ("LONG", "SHORT", "BOTH")
SEGMENTS = ("early", "middle", "late")
SIZE_BUCKETS = ("<=8", "9-12", "13-16", "17-20", ">20")
DISTANCE_CURVE_FILES = ("pole_directional_distance_curve.csv", "pole_directional_distance_curve_flags.csv")
CURVE_FIELDS = [
    "direction",
    "pole_size_bucket",
    "sample_size",
    "continuation_pct",
    "failure_pct",
    "expectancy_score",
    "asymmetry_score",
]
SEGMENT_FIELDS = ["segment", *CURVE_FIELDS]
FLAG_FIELDS = ["check_name", "result", "details"]


@dataclass(frozen=True)
class PoleRow:
    chronological_index: int
    row_order: int
    direction: str
    pole_size_bucket: str
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


def _to_optional_float(value: Any) -> float | None:
    text = _clean(value, na_token="")
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _to_optional_int(value: Any) -> int | None:
    text = _clean(value, na_token="")
    try:
        return int(float(text)) if text else None
    except ValueError:
        return None


def _safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _pole_size_bucket(value: Any) -> str:
    pole_boxes = _to_float(value)
    if pole_boxes <= 8:
        return "<=8"
    if pole_boxes <= 12:
        return "9-12"
    if pole_boxes <= 16:
        return "13-16"
    if pole_boxes <= 20:
        return "17-20"
    return ">20"


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict[str, str | int | float]], fields: list[str]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _validate_distance_curve_outputs(root: Path) -> dict[str, int]:
    diagnostics: dict[str, int] = {}
    for filename in DISTANCE_CURVE_FILES:
        path = root / filename
        if not path.exists():
            raise FileNotFoundError(f"Required directional distance curve output not found: {path}")
        diagnostics[f"{path.stem}_rows"] = len(_load_csv(path))
    return diagnostics


def _select_chronological_index_source(rows: list[dict[str, str]]) -> str:
    columns = {column for row in rows for column in row}
    return next((column for column in CHRONOLOGICAL_INDEX_COLUMNS if column in columns), "")


def _build_rows(labeled: list[dict[str, str]]) -> tuple[list[PoleRow], dict[str, int | str]]:
    chronological_source = _select_chronological_index_source(labeled)
    diagnostics: dict[str, int | str] = {
        "chronological_index_source": chronological_source or "row_order_fallback",
        "labeled_rows_loaded": len(labeled),
        "rows_missing_direction": 0,
        "explicit_direction_rows": 0,
        "pattern_name_direction_rows": 0,
        "rows_excluded_outside_best_motif": 0,
        "rows_missing_pole_size": 0,
        "best_motif_rows": 0,
    }
    rows: list[PoleRow] = []
    for row_order, raw in enumerate(labeled):
        direction, source = _infer_direction(raw)
        if not direction:
            diagnostics["rows_missing_direction"] += 1  # type: ignore[operator]
            continue
        if not (
            _clean(raw.get("opposing_pole_distance_columns")) == "3"
            and _clean(raw.get("enhanced_by_opposing_pole")).lower() == "false"
            and _to_float(raw.get("retrace_ratio")) > 1.0
        ):
            diagnostics["rows_excluded_outside_best_motif"] += 1  # type: ignore[operator]
            continue
        pole_boxes = _to_optional_float(raw.get("pole_boxes"))
        if pole_boxes is None:
            diagnostics["rows_missing_pole_size"] += 1  # type: ignore[operator]
            continue
        if source == "pattern_name":
            diagnostics["pattern_name_direction_rows"] += 1  # type: ignore[operator]
        else:
            diagnostics["explicit_direction_rows"] += 1  # type: ignore[operator]
        chronological_index = _to_optional_int(raw.get(chronological_source)) if chronological_source else None
        rows.append(
            PoleRow(
                chronological_index=chronological_index if chronological_index is not None else row_order,
                row_order=row_order,
                direction=direction,
                pole_size_bucket=_pole_size_bucket(pole_boxes),
                outcome=_clean(raw.get("outcome_class"), na_token="").upper(),
                expectancy=_row_expectancy(
                    _clean(raw.get("outcome_class"), na_token="").upper(),
                    _to_float(raw.get("max_favorable_boxes")),
                    _to_float(raw.get("max_adverse_boxes")),
                ),
            )
        )
    diagnostics["best_motif_rows"] = len(rows)
    return sorted(rows, key=lambda row: (row.chronological_index, row.row_order)), diagnostics


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


def _direction_rows(rows: list[PoleRow], direction: str) -> list[PoleRow]:
    return rows if direction == "BOTH" else [row for row in rows if row.direction == direction]


def _curve(rows: list[PoleRow]) -> list[dict[str, str | int | float]]:
    curve: list[dict[str, str | int | float]] = []
    for direction in DIRECTIONS:
        directional = _direction_rows(rows, direction)
        for bucket in SIZE_BUCKETS:
            curve.append({"direction": direction, "pole_size_bucket": bucket, **_metrics([row for row in directional if row.pole_size_bucket == bucket])})
    return curve


def _chronological_segments(rows: list[PoleRow]) -> dict[str, list[PoleRow]]:
    segments = {segment: [] for segment in SEGMENTS}
    for index, row in enumerate(rows):
        segment_index = min((index * 3) // len(rows), 2) if rows else 0
        segments[SEGMENTS[segment_index]].append(row)
    return segments


def _segment_curve(rows: list[PoleRow]) -> list[dict[str, str | int | float]]:
    segments: list[dict[str, str | int | float]] = []
    for segment, segment_rows in _chronological_segments(rows).items():
        for row in _curve(segment_rows):
            segments.append({"segment": segment, **row})
    return segments


def _index_curve(curve: list[dict[str, str | int | float]]) -> dict[tuple[str, str], dict[str, str | int | float]]:
    return {(str(row["direction"]), str(row["pole_size_bucket"])): row for row in curve}


def _aggregate_metrics(index: dict[tuple[str, str], dict[str, str | int | float]], buckets: tuple[str, ...]) -> dict[str, float | int]:
    rows = [index[("BOTH", bucket)] for bucket in buckets]
    sample_size = sum(int(row["sample_size"]) for row in rows)
    if not sample_size:
        return {"sample_size": 0, "continuation_pct": 0.0, "failure_pct": 0.0, "expectancy_score": 0.0, "asymmetry_score": 0.0}
    return {
        "sample_size": sample_size,
        **{
            metric: round(sum(float(row[metric]) * int(row["sample_size"]) for row in rows) / sample_size, 6)
            for metric in ("continuation_pct", "failure_pct", "expectancy_score", "asymmetry_score")
        },
    }


def _is_collapse(row: dict[str, str | int | float], min_sample: int) -> bool:
    return int(row["sample_size"]) >= min_sample and (
        float(row["expectancy_score"]) <= 0.0 or float(row["continuation_pct"]) <= float(row["failure_pct"])
    )


def _detect(
    curve: list[dict[str, str | int | float]],
    segments: list[dict[str, str | int | float]],
    min_sample: int,
    materiality_threshold: float,
    primary_threshold: float,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    index = _index_curve(curve)
    combined = [index[("BOTH", bucket)] for bucket in SIZE_BUCKETS]
    populated = [row for row in combined if int(row["sample_size"]) > 0]
    adequate = [row for row in combined if int(row["sample_size"]) >= min_sample]
    ranking_pool = adequate or populated
    best = max(ranking_pool, key=lambda row: (float(row["expectancy_score"]), float(row["asymmetry_score"]), int(row["sample_size"]))) if ranking_pool else None
    weakest = min(ranking_pool, key=lambda row: (float(row["expectancy_score"]), float(row["asymmetry_score"]), -int(row["sample_size"]))) if ranking_pool else None
    motif_metrics = _aggregate_metrics(index, SIZE_BUCKETS)
    improvement = float(best["expectancy_score"]) - float(motif_metrics["expectancy_score"]) if best else 0.0
    adequate_range = (max(float(row["expectancy_score"]) for row in adequate) - min(float(row["expectancy_score"]) for row in adequate)) if len(adequate) >= 2 else 0.0
    collapses = [str(row["pole_size_bucket"]) for row in adequate if _is_collapse(row, min_sample)]
    small_medium = _aggregate_metrics(index, ("<=8", "9-12", "13-16"))
    large = _aggregate_metrics(index, ("17-20", ">20"))
    large_delta = float(small_medium["expectancy_score"]) - float(large["expectancy_score"])
    large_poles_exhaustion_risk = int(large["sample_size"]) >= min_sample and (
        large_delta >= materiality_threshold or float(large["expectancy_score"]) <= 0.0
    )
    small_medium_poles_dominate = int(small_medium["sample_size"]) >= min_sample and int(large["sample_size"]) >= min_sample and large_delta >= materiality_threshold
    quality_filter = bool(best) and int(best["sample_size"]) >= min_sample and improvement >= materiality_threshold
    primary_driver = len(adequate) >= 2 and adequate_range >= primary_threshold and bool(collapses)
    segment_instability: list[str] = []
    for bucket in SIZE_BUCKETS:
        bucket_segments = [row for row in segments if row["direction"] == "BOTH" and row["pole_size_bucket"] == bucket and int(row["sample_size"]) >= min_sample]
        expectancies = [float(row["expectancy_score"]) for row in bucket_segments]
        if len(expectancies) >= 2 and (min(expectancies) <= 0.0 or max(expectancies) - min(expectancies) >= primary_threshold):
            segment_instability.append(bucket)
    recommended = "distance=3 AND enhanced=False AND retrace_ratio>1.0"
    if quality_filter and best:
        recommended += f" AND pole_size_bucket={best['pole_size_bucket']}"

    conclusions = {
        "best_pole_size_bucket": str(best["pole_size_bucket"]) if best else "NONE",
        "weakest_pole_size_bucket": str(weakest["pole_size_bucket"]) if weakest else "NONE",
        "pole_size_is_primary_driver": "YES" if primary_driver else "NO",
        "pole_size_is_quality_filter": "YES" if quality_filter else "NO",
        "large_poles_exhaustion_risk": "YES" if large_poles_exhaustion_risk else "NO",
        "small_medium_poles_dominate": "YES" if small_medium_poles_dominate else "NO",
        "recommended_structural_motif_v2": recommended,
    }
    flags = [
        {"check_name": "best_pole_size_bucket", "result": conclusions["best_pole_size_bucket"], "details": f"best adequate/populated BOTH bucket; improvement_vs_unfiltered_motif={improvement:.6f}"},
        {"check_name": "weakest_pole_size_bucket", "result": conclusions["weakest_pole_size_bucket"], "details": "weakest adequate/populated BOTH bucket by expectancy and asymmetry"},
        {"check_name": "pole_size_is_primary_driver", "result": conclusions["pole_size_is_primary_driver"], "details": f"adequate_bucket_expectancy_range={adequate_range:.6f}; primary_threshold={primary_threshold:.6f}; collapsed_buckets={','.join(collapses) or 'NONE'}"},
        {"check_name": "pole_size_is_quality_filter", "result": conclusions["pole_size_is_quality_filter"], "details": f"best_bucket_improvement_vs_unfiltered_motif={improvement:.6f}; materiality_threshold={materiality_threshold:.6f}"},
        {"check_name": "sample_sufficient_for_bucket_decomposition", "result": "YES" if adequate else "NO", "details": f"adequate_buckets={','.join(str(row['pole_size_bucket']) for row in adequate) or 'NONE'}; min_sample={min_sample}"},
        {"check_name": "any_bucket_collapses", "result": "YES" if collapses else "NO", "details": f"collapsed_adequate_buckets={','.join(collapses) or 'NONE'}"},
        {"check_name": "any_bucket_segment_instability", "result": "YES" if segment_instability else "NO", "details": f"unstable_adequate_buckets={','.join(segment_instability) or 'NONE'}; primary_threshold={primary_threshold:.6f}"},
        {"check_name": "large_poles_exhaustion_risk", "result": conclusions["large_poles_exhaustion_risk"], "details": f"small_medium_expectancy={float(small_medium['expectancy_score']):.6f}; large_expectancy={float(large['expectancy_score']):.6f}; delta={large_delta:.6f}"},
        {"check_name": "small_medium_poles_dominate", "result": conclusions["small_medium_poles_dominate"], "details": f"small_medium_n={int(small_medium['sample_size'])}; large_n={int(large['sample_size'])}; materiality_threshold={materiality_threshold:.6f}"},
        {"check_name": "recommended_structural_motif_v2", "result": conclusions["recommended_structural_motif_v2"], "details": "research-only structural recommendation; no strategy or execution logic changes"},
    ]
    return conclusions, flags


def main() -> None:
    parser = argparse.ArgumentParser(description="Decompose the best directional-distance pole motif by pole size (research-only).")
    parser.add_argument("--input-labeled-outcomes-csv", required=True)
    parser.add_argument("--input-directional-distance-curve-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--min-sample", type=int, default=5)
    parser.add_argument("--materiality-threshold", type=float, default=0.10)
    parser.add_argument("--primary-threshold", type=float, default=0.35)
    args = parser.parse_args()

    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    distance_diagnostics = _validate_distance_curve_outputs(Path(args.input_directional_distance_curve_root))
    labeled = _load_csv(Path(args.input_labeled_outcomes_csv))
    rows, diagnostics = _build_rows(labeled)
    curve = _curve(rows)
    segments = _segment_curve(rows)
    conclusions, flags = _detect(curve, segments, args.min_sample, args.materiality_threshold, args.primary_threshold)

    _write_csv(output_root / "pole_best_motif_size_curve.csv", curve, CURVE_FIELDS)
    _write_csv(output_root / "pole_best_motif_size_segments.csv", segments, SEGMENT_FIELDS)
    _write_csv(output_root / "pole_best_motif_size_flags.csv", flags, FLAG_FIELDS)

    index = _index_curve(curve)
    with (output_root / "pole_best_motif_size_summary.md").open("w") as f:
        f.write("# PnF Best Pole Motif Size Decomposition (Research-Only)\n\n")
        f.write("No strategy changes, execution simulation, or TP/SL logic are included. Rows are built from labeled outcomes only.\n")
        f.write("Direction uses explicit direction columns when available, then falls back to LOW_POLE=LONG and HIGH_POLE=SHORT.\n")
        f.write("Filtered motif: opposing_pole_distance_columns=3 AND enhanced_by_opposing_pole=False AND retrace_ratio>1.0.\n\n")
        f.write("## Diagnostics\n")
        for key, value in {**diagnostics, **distance_diagnostics}.items():
            f.write(f"- {key.replace('_', ' ')}: {value}\n")
        f.write(f"- minimum sample threshold: {args.min_sample}\n")
        f.write(f"- materiality threshold: {args.materiality_threshold:.6f}\n")
        f.write(f"- primary-driver threshold: {args.primary_threshold:.6f}\n\n")
        f.write("## BOTH-direction pole-size curve\n\n")
        f.write("| pole size bucket | sample size | continuation pct | failure pct | expectancy score | asymmetry score |\n")
        f.write("|---|---:|---:|---:|---:|---:|\n")
        for bucket in SIZE_BUCKETS:
            row = index[("BOTH", bucket)]
            f.write(f"| {bucket} | {row['sample_size']} | {row['continuation_pct']} | {row['failure_pct']} | {row['expectancy_score']} | {row['asymmetry_score']} |\n")
        f.write("\n## Required conclusions\n")
        for key, value in conclusions.items():
            f.write(f"- {key}: {value}\n")
        f.write("\n## Detection details\n")
        for flag in flags:
            f.write(f"- {flag['check_name']}: {flag['result']} ({flag['details']})\n")


if __name__ == "__main__":
    main()
