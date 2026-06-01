from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_r_targets import (
    DIRECTIONS,
    EXPECTED_SYMBOLS,
    Observation,
    SymbolLoad,
    _load_symbol,
    _parse_symbol_input,
    _safe_div,
    _target_outcome,
)

MILESTONES = (1.0, 1.5, 2.0, 2.5, 3.0)
TRANSITIONS = (
    (1.0, 1.5), (1.0, 2.0), (1.0, 2.5), (1.0, 3.0),
    (1.5, 2.0), (1.5, 2.5), (1.5, 3.0),
    (2.0, 2.5), (2.0, 3.0),
)
MATRIX_FIELDS = ["direction", "from_r", "to_r", "eligible_rows", "reached_rows", "conditional_probability"]
SYMBOL_FIELDS = ["symbol", *MATRIX_FIELDS]
DIRECTIONAL_FIELDS = ["direction", "milestone_r", "sample_size", "reached_rows", "reach_probability"]


def _round(value: float) -> float:
    return round(value, 6)


def _reached(row: Observation, target: float) -> bool:
    return _target_outcome(row, target)[0]


def _direction_rows(rows: Iterable[Observation], direction: str) -> list[Observation]:
    return [row for row in rows if direction == "BOTH" or row.direction == direction]


def _transition_row(rows: list[Observation], direction: str, from_r: float, to_r: float) -> dict[str, Any]:
    eligible = [row for row in rows if _reached(row, from_r)]
    reached = sum(_reached(row, to_r) for row in eligible)
    return {
        "direction": direction,
        "from_r": from_r,
        "to_r": to_r,
        "eligible_rows": len(eligible),
        "reached_rows": reached,
        "conditional_probability": _round(_safe_div(reached, len(eligible))),
    }


def _matrix(rows: Iterable[Observation]) -> list[dict[str, Any]]:
    source = list(rows)
    result: list[dict[str, Any]] = []
    for direction in DIRECTIONS:
        selected = _direction_rows(source, direction)
        result.extend(_transition_row(selected, direction, from_r, to_r) for from_r, to_r in TRANSITIONS)
    return result


def _directional(rows: Iterable[Observation]) -> list[dict[str, Any]]:
    source = list(rows)
    result: list[dict[str, Any]] = []
    for direction in DIRECTIONS:
        selected = _direction_rows(source, direction)
        for milestone in MILESTONES:
            reached = sum(_reached(row, milestone) for row in selected)
            result.append({
                "direction": direction,
                "milestone_r": milestone,
                "sample_size": len(selected),
                "reached_rows": reached,
                "reach_probability": _round(_safe_div(reached, len(selected))),
            })
    return result


def _lookup(matrix: list[dict[str, Any]], direction: str, from_r: float, to_r: float) -> dict[str, Any]:
    return next(row for row in matrix if row["direction"] == direction and row["from_r"] == from_r and row["to_r"] == to_r)


def _directional_lookup(rows: list[dict[str, Any]], direction: str, milestone: float) -> dict[str, Any]:
    return next(row for row in rows if row["direction"] == direction and row["milestone_r"] == milestone)


def _capture_score(rows: list[dict[str, Any]], milestone: float) -> float:
    row = _directional_lookup(rows, "BOTH", milestone)
    return _round(float(row["reach_probability"]) * milestone)


def _best_level(rows: list[dict[str, Any]], candidates: tuple[float, ...]) -> float:
    return max(candidates, key=lambda milestone: (_capture_score(rows, milestone), milestone))


def _format_r(value: float) -> str:
    return f"{value:g}R"


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_summary(
    path: Path,
    loads: list[SymbolLoad],
    matrix: list[dict[str, Any]],
    directional: list[dict[str, Any]],
) -> None:
    best_tp1 = _best_level(directional, (1.0, 1.5))
    best_tp2 = _best_level(directional, (2.0, 2.5, 3.0))
    after_1r = _lookup(matrix, "BOTH", 1.0, 2.5)
    after_1_5r = _lookup(matrix, "BOTH", 1.5, 2.5)
    long_after_1_5r = _lookup(matrix, "LONG", 1.5, 2.5)
    short_after_1_5r = _lookup(matrix, "SHORT", 1.5, 2.5)
    runner_supported = after_1_5r["eligible_rows"] > 0 and after_1_5r["conditional_probability"] >= 0.5
    reconstructed = sum(load.reconstructed_rows for load in loads)
    fallback = sum(load.fallback_rows for load in loads)

    with path.open("w") as f:
        f.write("# Pole core motif TP progression study\n\n")
        f.write("Research-only conditional continuation audit for the already-validated motif: `opposing_pole_distance_columns = 3` and `enhanced_by_opposing_pole = False`. This is an R-excursion study, not an execution simulation. No strategy logic, execution logic, TP/SL logic, database, or existing pattern definition is changed.\n\n")
        f.write("## Method\n\n")
        f.write("Each motif observation uses the fixed-SL geometry from `pole_core_motif_r_targets.py`: reconstruct the stop from the reversal-column boundary plus one invalidation box when raw PnF columns are supplied, and explicitly retain the same labeled-`retrace_boxes` excursion-only fallback otherwise. A milestone counts as reached only when the existing fixed-SL target outcome marks it reached before the stop.\n\n")
        f.write(f"- symbols: `{', '.join(EXPECTED_SYMBOLS)}`\n")
        f.write(f"- motif observations: `{sum(len(load.observations) for load in loads)}`\n")
        f.write(f"- reconstructed fixed-SL rows: `{reconstructed}`\n")
        f.write(f"- excursion-only fallback rows: `{fallback}`\n\n")
        f.write("## Required conclusions\n\n")
        f.write(f"- best TP1 level: **{_format_r(best_tp1)}** by pooled gross milestone-capture score (`reach_probability × R`); descriptive only, not an execution recommendation.\n")
        f.write(f"- best TP2 level: **{_format_r(best_tp2)}** by pooled gross milestone-capture score (`reach_probability × R`); descriptive only, not an execution recommendation.\n")
        f.write(f"- probability of reaching 2.5R after 1R: **{after_1r['conditional_probability']}** (`{after_1r['reached_rows']}/{after_1r['eligible_rows']}`).\n")
        f.write(f"- probability of reaching 2.5R after 1.5R: **{after_1_5r['conditional_probability']}** (`{after_1_5r['reached_rows']}/{after_1_5r['eligible_rows']}`).\n")
        f.write("- whether BE-after-1R is supported: **NOT ESTABLISHED**. Conditional favorable excursion alone cannot validate a stop-management change; post-1R reversal ordering and execution simulation are intentionally outside this study.\n")
        f.write(f"- whether runner-to-2.5R is supported: **{'SUPPORTED' if runner_supported else 'NOT SUPPORTED'}** by the declared descriptive rule `P(2.5R | 1.5R) >= 0.5` with a non-empty denominator. This does not promote TP logic.\n")
        f.write(f"- LONG vs SHORT differences: `P(2.5R | 1.5R)` is **{long_after_1_5r['conditional_probability']}** for LONG (`{long_after_1_5r['reached_rows']}/{long_after_1_5r['eligible_rows']}`) and **{short_after_1_5r['conditional_probability']}** for SHORT (`{short_after_1_5r['reached_rows']}/{short_after_1_5r['eligible_rows']}`).\n\n")
        f.write("## Conditional continuation matrix\n\n")
        f.write("| direction | from | to | eligible | reached | probability |\n")
        f.write("|---|---:|---:|---:|---:|---:|\n")
        for row in matrix:
            f.write(f"| {row['direction']} | {row['from_r']}R | {row['to_r']}R | {row['eligible_rows']} | {row['reached_rows']} | {row['conditional_probability']} |\n")
        f.write("\n## Required experiment scorecard\n\n")
        f.write("The execution scorecard remains deliberately uncomputed because this task prohibits execution simulation.\n\n")
        for metric in ("candidate_rows_registered", "resolved_rows", "win_rate_non_ambiguous", "avg_realized_r_multiple", "total_realized_r_multiple", "TP1 -> TP2 conversion"):
            f.write(f"- `{metric}`: `NOT_COMPUTED_FOR_R_EXCURSION_STUDY`\n")


def run(symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], output_root: Path) -> None:
    missing = [symbol for symbol in EXPECTED_SYMBOLS if symbol not in symbol_inputs]
    if missing:
        raise ValueError(f"missing required labeled outcome inputs: {', '.join(missing)}")
    unknown = sorted(set(symbol_inputs) - set(EXPECTED_SYMBOLS))
    if unknown:
        raise ValueError(f"unexpected labeled outcome symbols: {', '.join(unknown)}")
    unknown_columns = sorted(set(columns_inputs) - set(EXPECTED_SYMBOLS))
    if unknown_columns:
        raise ValueError(f"unexpected columns symbols: {', '.join(unknown_columns)}")

    output_root.mkdir(parents=True, exist_ok=True)
    loads = [_load_symbol(symbol, symbol_inputs[symbol], columns_inputs.get(symbol)) for symbol in EXPECTED_SYMBOLS]
    observations = [row for load in loads for row in load.observations]
    matrix = _matrix(observations)
    directional = _directional(observations)
    by_symbol = [
        {"symbol": load.symbol, **row}
        for load in loads
        for row in _matrix(load.observations)
    ]
    _write_summary(output_root / "tp_progression_summary.md", loads, matrix, directional)
    _write_csv(output_root / "tp_progression_matrix.csv", MATRIX_FIELDS, matrix)
    _write_csv(output_root / "tp_progression_by_symbol.csv", SYMBOL_FIELDS, by_symbol)
    _write_csv(output_root / "tp_progression_directional.csv", DIRECTIONAL_FIELDS, directional)


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only conditional TP progression study for the validated PnF pole core motif")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", default=[], type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    run(dict(args.symbol_input), dict(args.columns_input), args.output_root)


if __name__ == "__main__":
    main()
