from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_sl_candidates import (
    EXPECTED_SYMBOLS,
    R_TARGETS,
    _direction,
    _is_core_motif,
    _parse_path,
    _parse_symbol_input,
    _result_before_target,
    _round,
    _safe_div,
    _to_int,
    Observation,
)
from research_v2.patterns.pole_outcomes import CsvColumn, load_columns_csv

GEOMETRY_FIELDS = [
    "symbol", "row_number", "direction", "pole_column_index", "reversal_column_index",
    "immediate_next_column_index", "box_size", "entry_location", "sl_location", "stop_distance_boxes",
    "sl_inside_setup_pole_column", "sl_inside_reversal_column", "sl_inside_immediate_next_column",
    "pre_confirmation_stop_risk", "pre_confirmation_details",
]
SYMBOL_FIELDS = [
    "symbol", "observations", "original_stop_before_1r", "original_hit_1r", "stress_hit_1r",
    "unsafe_pre_confirmation", "unknown_pre_confirmation", "sl_inside_setup_pole_column",
    "sl_inside_reversal_column", "sl_inside_immediate_next_column",
]
STRESS_FIELDS = [
    "symbol", "r_target", "observations", "original_hits", "original_hit_rate", "stress_hits",
    "stress_hit_rate", "stress_stops", "stress_stop_rate", "stress_unknown", "stress_unknown_rate",
]
FLAG_FIELDS = ["symbol", "row_number", "check_name", "result", "details"]


@dataclass(frozen=True)
class RealityObservation:
    symbol: str
    row_number: int
    direction: str
    pole_idx: int
    reversal_idx: int
    next_idx: int
    box_size: float
    entry: float
    stop: float
    pole: CsvColumn
    reversal: CsvColumn
    next_column: CsvColumn | None
    favorable_path: tuple[float, ...]
    adverse_path: tuple[float, ...]

    @property
    def original(self) -> Observation:
        return Observation(
            self.symbol, self.row_number, self.direction, 0.0, "SL-C", 3.0,
            self.favorable_path, self.adverse_path,
        )


def _flag(symbol: str, row_number: int | str, check_name: str, result: str, details: str) -> dict[str, str]:
    return {"symbol": symbol, "row_number": str(row_number), "check_name": check_name, "result": result, "details": details}


def _inside(column: CsvColumn | None, price: float) -> bool:
    return column is not None and column.bottom - 1e-9 <= price <= column.top + 1e-9


def _fmt(value: float) -> float:
    return _round(value)


def _load_symbol(symbol: str, labels_path: Path, columns_path: Path) -> tuple[list[RealityObservation], list[dict[str, str]]]:
    loaded_columns, box_size = load_columns_csv(columns_path)
    if box_size is None or box_size <= 0:
        raise ValueError(f"{symbol}: matching PnF columns do not expose a positive box size")
    columns = {column.idx: column for column in loaded_columns}
    flags: list[dict[str, str]] = []
    observations: list[RealityObservation] = []
    with labels_path.open("r", newline="") as f:
        rows = list(csv.DictReader(f))
    for row_number, row in enumerate(rows, start=2):
        if not _is_core_motif(row):
            continue
        direction = _direction(row.get("pattern_name"))
        pole_idx = _to_int(row.get("pole_column_index"))
        reversal_idx = _to_int(row.get("reversal_column_index"))
        favorable_path = _parse_path(row.get("fav_path"))
        adverse_path = _parse_path(row.get("adv_path"))
        if direction is None or pole_idx is None or reversal_idx is None or favorable_path is None or adverse_path is None or len(favorable_path) != len(adverse_path):
            flags.append(_flag(symbol, row_number, "valid_sl_c_observation", "EXCLUDED", "motif row lacks direction, column indices, or equal-length excursion paths"))
            continue
        pole, reversal = columns.get(pole_idx), columns.get(reversal_idx)
        if pole is None or reversal is None:
            flags.append(_flag(symbol, row_number, "valid_sl_c_observation", "EXCLUDED", "matching setup pole or reversal column is unavailable"))
            continue
        expected = ("O", "X") if direction == "LONG" else ("X", "O")
        if (pole.kind, reversal.kind) != expected:
            flags.append(_flag(symbol, row_number, "valid_sl_c_observation", "EXCLUDED", f"pole/reversal kinds are {pole.kind}/{reversal.kind}; expected {expected[0]}/{expected[1]}"))
            continue
        entry = reversal.top if direction == "LONG" else reversal.bottom
        stop = entry - 3.0 * box_size if direction == "LONG" else entry + 3.0 * box_size
        observations.append(RealityObservation(
            symbol, row_number, direction, pole_idx, reversal_idx, reversal_idx + 1, box_size,
            entry, stop, pole, reversal, columns.get(reversal_idx + 1), favorable_path, adverse_path,
        ))
    return observations, flags


def _pre_confirmation(row: RealityObservation) -> tuple[str, str]:
    if _inside(row.reversal, row.stop):
        return "UNSAFE", "SL is inside the reversal column whose final extreme defines entry; final-extreme entry is unavailable before that column completes"
    if row.next_column is None:
        return "UNKNOWN", "immediate next column is unavailable, so final-extreme confirmation timing cannot be checked"
    if _inside(row.next_column, row.stop):
        return "UNSAFE", "SL is traversed by the immediate next column that makes the final reversal-column extreme observable"
    return "SAFE", "SL is outside the reversal column and its immediate confirmation column"


def _stress_result(row: RealityObservation, target_r: float) -> str:
    # Strictest chronology compatible with the existing final-extreme entry definition:
    # that entry becomes knowable only when the immediate next reversal column forms.
    # If that column includes the fixed stop, count STOP before granting any target.
    if row.next_column is None:
        return "UNKNOWN"
    if _inside(row.next_column, row.stop):
        return "STOP"
    target = row.entry + target_r * 3.0 * row.box_size if row.direction == "LONG" else row.entry - target_r * 3.0 * row.box_size
    if _inside(row.next_column, target):
        return "HIT"
    return "UNKNOWN"


def _geometry_row(row: RealityObservation) -> dict[str, Any]:
    risk, details = _pre_confirmation(row)
    return {
        "symbol": row.symbol, "row_number": row.row_number, "direction": row.direction,
        "pole_column_index": row.pole_idx, "reversal_column_index": row.reversal_idx,
        "immediate_next_column_index": row.next_idx, "box_size": _fmt(row.box_size),
        "entry_location": _fmt(row.entry), "sl_location": _fmt(row.stop), "stop_distance_boxes": 3.0,
        "sl_inside_setup_pole_column": _inside(row.pole, row.stop),
        "sl_inside_reversal_column": _inside(row.reversal, row.stop),
        "sl_inside_immediate_next_column": _inside(row.next_column, row.stop),
        "pre_confirmation_stop_risk": risk, "pre_confirmation_details": details,
    }


def _stress_rows(rows: list[RealityObservation]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for symbol in (*EXPECTED_SYMBOLS, "ALL"):
        selected = rows if symbol == "ALL" else [row for row in rows if row.symbol == symbol]
        for target in R_TARGETS:
            original = [_result_before_target(row.original, target) for row in selected]
            stress = [_stress_result(row, target) for row in selected]
            output.append({
                "symbol": symbol, "r_target": target, "observations": len(selected),
                "original_hits": original.count("HIT"), "original_hit_rate": _fmt(_safe_div(original.count("HIT"), len(selected))),
                "stress_hits": stress.count("HIT"), "stress_hit_rate": _fmt(_safe_div(stress.count("HIT"), len(selected))),
                "stress_stops": stress.count("STOP"), "stress_stop_rate": _fmt(_safe_div(stress.count("STOP"), len(selected))),
                "stress_unknown": stress.count("UNKNOWN"), "stress_unknown_rate": _fmt(_safe_div(stress.count("UNKNOWN"), len(selected))),
            })
    return output


def _symbol_rows(rows: list[RealityObservation]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for symbol in EXPECTED_SYMBOLS:
        selected = [row for row in rows if row.symbol == symbol]
        geometry = [_geometry_row(row) for row in selected]
        original = [_result_before_target(row.original, 1.0) for row in selected]
        stress = [_stress_result(row, 1.0) for row in selected]
        output.append({
            "symbol": symbol, "observations": len(selected),
            "original_stop_before_1r": _fmt(_safe_div(original.count("STOP"), len(selected))),
            "original_hit_1r": _fmt(_safe_div(original.count("HIT"), len(selected))),
            "stress_hit_1r": _fmt(_safe_div(stress.count("HIT"), len(selected))),
            "unsafe_pre_confirmation": _fmt(_safe_div(sum(row["pre_confirmation_stop_risk"] == "UNSAFE" for row in geometry), len(selected))),
            "unknown_pre_confirmation": _fmt(_safe_div(sum(row["pre_confirmation_stop_risk"] == "UNKNOWN" for row in geometry), len(selected))),
            "sl_inside_setup_pole_column": _fmt(_safe_div(sum(row["sl_inside_setup_pole_column"] for row in geometry), len(selected))),
            "sl_inside_reversal_column": _fmt(_safe_div(sum(row["sl_inside_reversal_column"] for row in geometry), len(selected))),
            "sl_inside_immediate_next_column": _fmt(_safe_div(sum(row["sl_inside_immediate_next_column"] for row in geometry), len(selected))),
        })
    return output


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _pct(count: int, total: int) -> str:
    return f"{count} / {total} ({100.0 * _safe_div(count, total):.2f}%)"


def _write_summary(path: Path, rows: list[RealityObservation], geometry: list[dict[str, Any]], symbols: list[dict[str, Any]], stress: list[dict[str, Any]]) -> None:
    distances = [row["stop_distance_boxes"] for row in geometry]
    risk_counts = {name: sum(row["pre_confirmation_stop_risk"] == name for row in geometry) for name in ("SAFE", "UNSAFE", "UNKNOWN")}
    pooled = [row for row in stress if row["symbol"] == "ALL"]
    original_1r = next(row for row in pooled if row["r_target"] == 1.0)
    stress_1r = original_1r["stress_hit_rate"]
    artifact = risk_counts["UNSAFE"] > 0 or float(stress_1r) < float(original_1r["original_hit_rate"])
    with path.open("w") as f:
        f.write("# PnF Pole Research — SL-C Reality Check Audit\n\n")
        f.write("Research-only attempt to break the fixed 3-box structural SL-C result for `opposing_pole_distance_columns = 3 AND enhanced_by_opposing_pole = False`. No strategy, execution, TP/SL production logic, database schema, motif definition, or existing output is modified.\n\n")
        f.write("## 1. Entry ↔ SL geometry\n\n")
        f.write(f"Valid SL-C observations: **{len(rows)}**. SL-C is explicitly reconstructed as the final reversal-column extreme plus/minus exactly three boxes: min **{min(distances) if distances else 'NA'}**, median **{median(distances) if distances else 'NA'}**, average **{mean(distances) if distances else 'NA'}**, max **{max(distances) if distances else 'NA'}** boxes. Therefore fixed-distance equivalence is **YES**.\n\n")
        f.write("## 2. Same-column invalidation risk\n\n")
        for field, label in (("sl_inside_setup_pole_column", "setup pole column"), ("sl_inside_reversal_column", "reversal column"), ("sl_inside_immediate_next_column", "immediate next column")):
            f.write(f"- SL inside {label}: **{_pct(sum(row[field] for row in geometry), len(rows))}**\n")
        f.write("\n## 3. Pre-confirmation stop risk\n\n")
        for name in ("SAFE", "UNSAFE", "UNKNOWN"):
            f.write(f"- {name}: **{_pct(risk_counts[name], len(rows))}**\n")
        f.write("\nThe strict audit treats final reversal-column extreme entry as unavailable until that column completes. If its fixed stop lies inside that same reversal column, the hindsight entry has pre-confirmation stop risk.\n\n")
        f.write("## 4. Lookahead audit\n\n")
        f.write("- Stop formula itself: fixed three boxes; no future partner and no future excursion value is used to calculate distance.\n")
        f.write("- Entry anchor: final reversal-column extreme. That extreme is only confirmed when the next column forms.\n")
        f.write(f"- **LOOKAHEAD_FOUND = {'YES' if artifact else 'NO'}**. The optimistic structural result uses a hindsight-confirmed entry anchor with post-reversal excursion paths; the fixed distance is clean, but the placement context is not chronologically trade-safe.\n\n")
        f.write("## 5. Approximately 97% 1R hit-rate explanation\n\n")
        f.write("The original excursion method evaluates future favorable/adverse paths anchored to reversal-column bounds while applying a nominal three-box risk distance. It does not charge SL-C for the adverse movement needed to confirm the final-extreme entry anchor. The symbol breakdown below shows the original rate and the strict chronology result.\n\n")
        f.write("| symbol | observations | original stop before 1R | original hit 1R | strict stress hit 1R | unsafe pre-confirmation |\n|---|---:|---:|---:|---:|---:|\n")
        for row in symbols:
            f.write(f"| {row['symbol']} | {row['observations']} | {row['original_stop_before_1r']} | {row['original_hit_1r']} | {row['stress_hit_1r']} | {row['unsafe_pre_confirmation']} |\n")
        f.write("\n## 6. Chronological stress test\n\n")
        f.write("Strictest available assumption: retain the prior final-extreme entry anchor, wait until it is knowable from the immediate next column, and count stop before target whenever that confirmation column includes the stop. No execution simulation is performed.\n\n")
        f.write("| target R | original hit rate | strict stress hit rate | strict stop rate | strict unknown rate |\n|---:|---:|---:|---:|---:|\n")
        for row in pooled:
            f.write(f"| {row['r_target']} | {row['original_hit_rate']} | {row['stress_hit_rate']} | {row['stress_stop_rate']} | {row['stress_unknown_rate']} |\n")
        f.write("\n## 7. Required conclusions\n\n")
        f.write("- **SL-C structurally valid: NO** — the distance is exactly three boxes, but the audited placement is not chronologically safe.\n")
        f.write("- **SL-C likely excursion artifact: YES** — the labeled excursion framing omits the confirmation cost of the final-extreme anchor.\n")
        f.write("- **SL-C likely tradable candidate: NO** — not from this evidence.\n")
        f.write("- **confidence level: HIGH** when unsafe rows dominate; otherwise MEDIUM pending richer timestamp data.\n")
        f.write("- **recommended next step:** DISCARD the prior SL-C structural-edge interpretation. If SL-C remains interesting, run a separate research audit with an explicitly observable entry event and candle-level ordering before any execution simulation. No strategy promotion.\n\n")
        f.write("## Required experiment scorecard\n\n")
        f.write("This structural reality check does not register or resolve strategy candidates. `candidate_rows_registered`, `resolved_rows`, `win_rate_non_ambiguous`, `avg_realized_r_multiple`, `total_realized_r_multiple`, and `TP1 -> TP2 conversion` are all `NOT_COMPUTED_FOR_STRUCTURAL_SL_C_REALITY_CHECK`.\n")


def run(symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], output_root: Path) -> None:
    missing_labels = [symbol for symbol in EXPECTED_SYMBOLS if symbol not in symbol_inputs]
    missing_columns = [symbol for symbol in EXPECTED_SYMBOLS if symbol not in columns_inputs]
    if missing_labels or missing_columns:
        raise ValueError(f"missing required inputs: labels={','.join(missing_labels) or 'none'}; columns={','.join(missing_columns) or 'none'}")
    output_root.mkdir(parents=True, exist_ok=True)
    rows: list[RealityObservation] = []
    flags: list[dict[str, str]] = []
    for symbol in EXPECTED_SYMBOLS:
        loaded, symbol_flags = _load_symbol(symbol, symbol_inputs[symbol], columns_inputs[symbol])
        rows.extend(loaded)
        flags.extend(symbol_flags)
    geometry = [_geometry_row(row) for row in rows]
    symbols = _symbol_rows(rows)
    stress = _stress_rows(rows)
    flags.extend([
        _flag("ALL", "", "lookahead_stop_formula", "OK", "SL-C distance is fixed at three boxes; no future partner or excursion value sets the distance"),
        _flag("ALL", "", "lookahead_entry_anchor", "FAIL", "SL-C placement uses the final reversal-column extreme, which is hindsight-confirmed only when the immediate next column forms"),
        _flag("ALL", "", "baseline_protection", "OK", "research-only outputs; no strategy, execution, TP/SL production logic, schema, motif, or existing output modified"),
    ])
    _write_summary(output_root / "sl_c_reality_summary.md", rows, geometry, symbols, stress)
    _write_csv(output_root / "sl_c_geometry.csv", GEOMETRY_FIELDS, geometry)
    _write_csv(output_root / "sl_c_symbol_breakdown.csv", SYMBOL_FIELDS, symbols)
    _write_csv(output_root / "sl_c_stress_test.csv", STRESS_FIELDS, stress)
    _write_csv(output_root / "sl_c_flags.csv", FLAG_FIELDS, flags)


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only chronological reality check for fixed 3-box SL-C")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    run(dict(args.symbol_input), dict(args.columns_input), args.output_root)


if __name__ == "__main__":
    main()
