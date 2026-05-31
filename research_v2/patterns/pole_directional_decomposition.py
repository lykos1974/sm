from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

NA_VALUES = {"", "na", "none", "null", "nan"}
CONTINUATION_OUTCOMES = {"BULLISH_CONTINUATION", "BEARISH_CONTINUATION"}
FAILURE_OUTCOME = "FAILED_REVERSAL"
DIRECTIONS = ("LONG", "SHORT")
SEGMENTS = ("early", "middle", "late")
DIRECTION_COLUMNS = ("pattern_direction", "direction", "side")
CHRONOLOGICAL_INDEX_COLUMNS = ("reversal_column_index", "pole_column_index")
TARGET_SCOPE = "distance_3_enhanced_false"
RETRACE_SCOPE = "distance_3_enhanced_false_retrace_gt_1.0"

BREAKDOWN_FIELDS = [
    "scope",
    "direction",
    "sample_size",
    "continuation_pct",
    "failure_pct",
    "expectancy_score",
    "asymmetry_score",
    "sample_insufficiency",
]
SEGMENT_FIELDS = ["scope", "segment", "direction", *BREAKDOWN_FIELDS[2:]]
STABILITY_FIELDS = [
    "window_id",
    "direction",
    "forward_start_ts",
    "forward_end_ts",
    "sample_size",
    "continuation_pct",
    "failure_pct",
    "expectancy_score",
    "asymmetry_score",
    "sample_insufficiency",
]


@dataclass(frozen=True)
class PoleRow:
    ts: int
    direction: str
    outcome: str
    distance: str
    enhanced: str
    retrace_ratio: float
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


def _to_optional_int(value: Any) -> int | None:
    text = _clean(value, na_token="")
    try:
        return int(float(text)) if text else None
    except ValueError:
        return None


def _safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict[str, str | int | float]], fields: list[str]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _normalize_direction(value: Any) -> str:
    normalized = _clean(value, na_token="").upper()
    if normalized in {"LONG", "BULLISH", "BULL", "UP", "LOW_POLE"}:
        return "LONG"
    if normalized in {"SHORT", "BEARISH", "BEAR", "DOWN", "HIGH_POLE"}:
        return "SHORT"
    return ""


def _infer_direction(row: dict[str, str]) -> tuple[str, str]:
    for column in DIRECTION_COLUMNS:
        direction = _normalize_direction(row.get(column))
        if direction:
            return direction, column

    direction = _normalize_direction(row.get("pattern_name"))
    if direction:
        return direction, "pattern_name"
    return "", ""


def _row_expectancy(outcome: str, max_favorable: float, max_adverse: float) -> float:
    outcome_score = 1.0 if outcome in CONTINUATION_OUTCOMES else (-1.0 if outcome == FAILURE_OUTCOME else 0.0)
    asymmetry = _safe_div(max_favorable - max_adverse, max_favorable + max_adverse)
    return (0.7 * outcome_score) + (0.3 * asymmetry)


def _select_chronological_index_source(rows: list[dict[str, str]]) -> str:
    columns = {column for row in rows for column in row}
    return next((column for column in CHRONOLOGICAL_INDEX_COLUMNS if column in columns), "")


def _build_rows(labeled: list[dict[str, str]]) -> tuple[list[PoleRow], dict[str, int | str]]:
    chronological_source = _select_chronological_index_source(labeled)
    diagnostics: dict[str, int | str] = {
        "chronological_index_source": chronological_source or "NONE",
        "rows_missing_chronological_index": 0,
        "rows_missing_direction": 0,
        "explicit_direction_rows": 0,
        "pattern_name_direction_rows": 0,
    }
    rows: list[PoleRow] = []
    for raw in labeled:
        ts = _to_optional_int(raw.get(chronological_source)) if chronological_source else None
        if ts is None:
            diagnostics["rows_missing_chronological_index"] += 1  # type: ignore[operator]
            continue
        direction, source = _infer_direction(raw)
        if not direction:
            diagnostics["rows_missing_direction"] += 1  # type: ignore[operator]
            continue
        if source == "pattern_name":
            diagnostics["pattern_name_direction_rows"] += 1  # type: ignore[operator]
        else:
            diagnostics["explicit_direction_rows"] += 1  # type: ignore[operator]
        outcome = _clean(raw.get("outcome_class"), na_token="").upper()
        rows.append(
            PoleRow(
                ts=ts,
                direction=direction,
                outcome=outcome,
                distance=_clean(raw.get("opposing_pole_distance_columns")),
                enhanced=_clean(raw.get("enhanced_by_opposing_pole")),
                retrace_ratio=_to_float(raw.get("retrace_ratio")),
                expectancy=_row_expectancy(
                    outcome,
                    _to_float(raw.get("max_favorable_boxes")),
                    _to_float(raw.get("max_adverse_boxes")),
                ),
            )
        )
    return sorted(rows, key=lambda row: row.ts), diagnostics


def _metrics(rows: list[PoleRow], min_sample: int) -> dict[str, str | int | float]:
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
        "sample_insufficiency": "True" if sample_size < min_sample else "False",
    }


def _scope_rows(rows: list[PoleRow], scope: str) -> list[PoleRow]:
    if scope == "all_direction_observations":
        return rows
    motif_rows = [row for row in rows if row.distance == "3" and row.enhanced.lower() == "false"]
    if scope == TARGET_SCOPE:
        return motif_rows
    if scope == RETRACE_SCOPE:
        return [row for row in motif_rows if row.retrace_ratio > 1.0]
    raise ValueError(f"unknown scope: {scope}")


def _chronological_segments(rows: list[PoleRow]) -> dict[str, list[PoleRow]]:
    thirds = max(len(rows) // 3, 1)
    return {
        "early": rows[:thirds],
        "middle": rows[thirds : 2 * thirds],
        "late": rows[2 * thirds :],
    }


def _is_side_collapse(metrics: dict[str, str | int | float]) -> bool:
    return int(metrics["sample_size"]) > 0 and (
        float(metrics["expectancy_score"]) <= 0.0 or float(metrics["continuation_pct"]) <= float(metrics["failure_pct"])
    )


def _is_side_unstable(stability: list[dict[str, str | int | float]], direction: str) -> bool:
    adequate = [
        float(row["expectancy_score"])
        for row in stability
        if row["direction"] == direction and row["sample_insufficiency"] == "False"
    ]
    return len(adequate) >= 2 and (min(adequate) <= 0.0 or max(adequate) - min(adequate) > 0.35)


def _conclusion(
    target_metrics: dict[str, dict[str, str | int | float]],
    stability: list[dict[str, str | int | float]],
    similarity_threshold: float,
) -> dict[str, str]:
    long_expectancy = float(target_metrics["LONG"]["expectancy_score"])
    short_expectancy = float(target_metrics["SHORT"]["expectancy_score"])
    diff = long_expectancy - short_expectancy
    strongest = "LONG" if diff > 0 else ("SHORT" if diff < 0 else "NEITHER")
    weakest = "SHORT" if diff > 0 else ("LONG" if diff < 0 else "NEITHER")
    collapses = {direction: _is_side_collapse(target_metrics[direction]) for direction in DIRECTIONS}
    insufficient = {direction: target_metrics[direction]["sample_insufficiency"] == "True" for direction in DIRECTIONS}
    unstable = {direction: _is_side_unstable(stability, direction) for direction in DIRECTIONS}

    if abs(diff) <= similarity_threshold:
        comparison = "C) Both similar"
    elif diff > 0:
        comparison = "A) LONG expectancy > SHORT expectancy"
    else:
        comparison = "B) SHORT expectancy > LONG expectancy"

    discard = "NONE"
    if collapses["LONG"] and not collapses["SHORT"] and not any(insufficient.values()):
        discard = "LONG"
    elif collapses["SHORT"] and not collapses["LONG"] and not any(insufficient.values()):
        discard = "SHORT"
    if discard != "NONE":
        comparison += f"; D) discard {discard}"

    if discard == "SHORT" and long_expectancy > 0:
        recommendation = "LONG_ONLY"
    elif discard == "LONG" and short_expectancy > 0:
        recommendation = "SHORT_ONLY"
    else:
        recommendation = "BOTH"

    if any(insufficient.values()):
        confidence = "LOW"
    elif discard != "NONE" and not unstable[strongest]:
        confidence = "HIGH"
    elif unstable["LONG"] or unstable["SHORT"] or abs(diff) <= similarity_threshold:
        confidence = "LOW"
    else:
        confidence = "MODERATE"

    return {
        "comparison": comparison,
        "strongest_side": strongest,
        "weakest_side": weakest,
        "confidence": confidence,
        "recommendation": recommendation,
        "directional_dominance": strongest if abs(diff) > similarity_threshold else "NONE",
        "side_collapse": ", ".join(direction for direction in DIRECTIONS if collapses[direction]) or "NONE",
        "side_instability": ", ".join(direction for direction in DIRECTIONS if unstable[direction]) or "NONE",
        "sample_insufficiency": ", ".join(direction for direction in DIRECTIONS if insufficient[direction]) or "NONE",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit opposing-pole motif directional decomposition (research-only).")
    parser.add_argument("--input-labeled-outcomes-csv", required=True)
    parser.add_argument("--input-forward-validation-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--min-sample", type=int, default=20)
    parser.add_argument("--similarity-threshold", type=float, default=0.1)
    args = parser.parse_args()

    labeled = _load_csv(Path(args.input_labeled_outcomes_csv))
    rows, diagnostics = _build_rows(labeled)
    forward_windows_path = Path(args.input_forward_validation_root) / "pole_forward_validation_windows.csv"
    forward_windows = _load_csv(forward_windows_path)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    scopes = ("all_direction_observations", TARGET_SCOPE, RETRACE_SCOPE)
    breakdown: list[dict[str, str | int | float]] = []
    target_metrics: dict[str, dict[str, str | int | float]] = {}
    for scope in scopes:
        for direction in DIRECTIONS:
            metrics = _metrics(_scope_rows([row for row in rows if row.direction == direction], scope), args.min_sample)
            breakdown.append({"scope": scope, "direction": direction, **metrics})
            if scope == TARGET_SCOPE:
                target_metrics[direction] = metrics

    segments: list[dict[str, str | int | float]] = []
    for scope in scopes:
        for segment, chronological_rows in _chronological_segments(rows).items():
            scoped_segment = _scope_rows(chronological_rows, scope)
            for direction in DIRECTIONS:
                segment_rows = [row for row in scoped_segment if row.direction == direction]
                segments.append({"scope": scope, "segment": segment, "direction": direction, **_metrics(segment_rows, args.min_sample)})

    stability: list[dict[str, str | int | float]] = []
    target_rows = _scope_rows(rows, TARGET_SCOPE)
    for window in forward_windows:
        start = _to_optional_int(window.get("forward_start_ts"))
        end = _to_optional_int(window.get("forward_end_ts"))
        if start is None or end is None:
            continue
        for direction in DIRECTIONS:
            window_rows = [row for row in target_rows if row.direction == direction and start <= row.ts <= end]
            stability.append(
                {
                    "window_id": _clean(window.get("window_id"), na_token=""),
                    "direction": direction,
                    "forward_start_ts": start,
                    "forward_end_ts": end,
                    **_metrics(window_rows, args.min_sample),
                }
            )

    conclusion = _conclusion(target_metrics, stability, args.similarity_threshold)
    _write_csv(output_root / "pole_directional_breakdown.csv", breakdown, BREAKDOWN_FIELDS)
    _write_csv(output_root / "pole_directional_segments.csv", segments, SEGMENT_FIELDS)
    _write_csv(output_root / "pole_directional_stability.csv", stability, STABILITY_FIELDS)

    with (output_root / "pole_directional_summary.md").open("w") as f:
        f.write("# PnF Pole Directional Decomposition Audit (Research-Only)\n\n")
        f.write("No strategy changes, execution simulation, or TP/SL logic are included.\n")
        f.write("Direction uses explicit pattern direction columns when available, then falls back to HIGH_POLE=SHORT and LOW_POLE=LONG.\n")
        f.write("Stability reuses the chronological forward-window boundaries emitted by pole_forward_validation_btc_v4.\n\n")
        f.write("## Diagnostics\n")
        f.write(f"- labeled rows loaded: {len(labeled)}\n")
        f.write(f"- usable directional rows: {len(rows)}\n")
        for key, value in diagnostics.items():
            f.write(f"- {key.replace('_', ' ')}: {value}\n")
        f.write(f"- forward windows loaded: {len(forward_windows)}\n")
        f.write(f"- minimum sample threshold: {args.min_sample}\n\n")
        f.write("## distance=3 AND enhanced=False\n")
        for direction in DIRECTIONS:
            f.write(f"- {direction}: {target_metrics[direction]}\n")
        f.write("\n## distance=3 AND enhanced=False AND retrace>1.0\n")
        for row in breakdown:
            if row["scope"] == RETRACE_SCOPE:
                f.write(f"- {row['direction']}: {dict((key, row[key]) for key in BREAKDOWN_FIELDS[2:])}\n")
        f.write("\n## Detection\n")
        for key in ("directional_dominance", "side_collapse", "side_instability", "sample_insufficiency"):
            f.write(f"- {key.replace('_', ' ')}: {conclusion[key]}\n")
        f.write("\n## Required conclusions\n")
        f.write(f"- comparison: {conclusion['comparison']}\n")
        f.write(f"- strongest side: {conclusion['strongest_side']}\n")
        f.write(f"- weakest side: {conclusion['weakest_side']}\n")
        f.write(f"- confidence: {conclusion['confidence']}\n")
        f.write(f"- recommendation: {conclusion['recommendation']}\n")


if __name__ == "__main__":
    main()
