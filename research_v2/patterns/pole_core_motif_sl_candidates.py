from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from research_v2.patterns.pole_outcomes import CsvColumn, load_columns_csv

EXPECTED_SYMBOLS = ("BTC", "ETH", "SOL", "SUI", "TAO", "ENA", "HYPE")
SL_CANDIDATES = ("SL-A", "SL-B", "SL-C")
R_TARGETS = (1.0, 1.5, 2.0, 2.5, 3.0)
SCOPES = ("core_motif", "quality_subset")
DIRECTIONS = ("BOTH", "LONG", "SHORT")
NA_VALUES = {"", "na", "none", "null", "nan"}
REQUIRED_COLUMNS = (
    "pattern_name",
    "pole_column_index",
    "reversal_column_index",
    "opposing_pole_distance_columns",
    "enhanced_by_opposing_pole",
    "retrace_ratio",
    "fav_path",
    "adv_path",
)
CURVE_FIELDS = [
    "scope", "direction", "sl_candidate", "r_target", "valid_reconstructed_rows", "excluded_rows",
    "average_r_distance_boxes", "median_r_distance_boxes", "stop_before_1r_rate", "hit_rate", "expectancy_r",
]
SYMBOL_FIELDS = ["symbol", *CURVE_FIELDS]
DIRECTIONAL_FIELDS = CURVE_FIELDS
FLAG_FIELDS = ["symbol", "row_number", "sl_candidate", "check_name", "result", "details"]


@dataclass(frozen=True)
class BaseObservation:
    symbol: str
    row_number: int
    direction: str
    retrace_ratio: float
    favorable_path_boxes: tuple[float, ...]
    adverse_path_boxes: tuple[float, ...]
    row: dict[str, Any]


@dataclass(frozen=True)
class Observation:
    symbol: str
    row_number: int
    direction: str
    retrace_ratio: float
    sl_candidate: str
    stop_distance_boxes: float
    favorable_path_boxes: tuple[float, ...]
    adverse_path_boxes: tuple[float, ...]


@dataclass(frozen=True)
class SymbolLoad:
    symbol: str
    loaded_rows: int
    motif_rows: int
    observations: tuple[Observation, ...]
    eligible_counts: dict[tuple[str, str], int]
    flags: tuple[dict[str, str], ...]


def _clean(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in NA_VALUES else text


def _to_float(value: Any) -> float | None:
    text = _clean(value)
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    number = _to_float(value)
    return int(number) if number is not None and number.is_integer() else None


def _to_bool(value: Any) -> bool | None:
    text = _clean(value).lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _round(value: float) -> float:
    return round(value, 6)


def _safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _parse_symbol_input(value: str) -> tuple[str, Path]:
    symbol, separator, raw_path = value.partition("=")
    symbol, raw_path = symbol.strip().upper(), raw_path.strip()
    if not separator or not symbol or not raw_path:
        raise argparse.ArgumentTypeError("expected SYMBOL=path/to/file.csv")
    return symbol, Path(raw_path)


def _parse_path(value: Any) -> tuple[float, ...] | None:
    text = _clean(value)
    if not text:
        return None
    try:
        values = tuple(float(part.strip()) for part in text.split(",") if part.strip())
    except ValueError:
        return None
    return values or None


def _direction(pattern_name: Any) -> str | None:
    pattern = _clean(pattern_name).upper()
    if pattern == "LOW_POLE":
        return "LONG"
    if pattern == "HIGH_POLE":
        return "SHORT"
    return None


def _is_core_motif(row: dict[str, Any]) -> bool:
    return _to_int(row.get("opposing_pole_distance_columns")) == 3 and _to_bool(row.get("enhanced_by_opposing_pole")) is False


def _integral_positive_boxes(distance: float, box_size: float, label: str) -> tuple[float | None, str]:
    boxes = distance / box_size
    rounded = round(boxes)
    if boxes <= 0:
        return None, f"{label} stop distance is not positive"
    if abs(boxes - rounded) > 1e-6:
        return None, f"{label} stop distance {boxes:g} is not an integral box count"
    return float(rounded), f"reconstructed exact {label} stop distance"


def _column(columns: dict[int, CsvColumn], idx: int | None, label: str) -> tuple[CsvColumn | None, str | None]:
    if idx is None:
        return None, f"labeled {label} is missing"
    if idx not in columns:
        return None, f"labeled {label}={idx} is missing from PnF columns"
    return columns[idx], None


def _sl_a_distance(row: dict[str, Any], columns: dict[int, CsvColumn], box_size: float, direction: str) -> tuple[float | None, str]:
    pole, error = _column(columns, _to_int(row.get("pole_column_index")), "pole_column_index")
    if error:
        return None, error
    reversal, error = _column(columns, _to_int(row.get("reversal_column_index")), "reversal_column_index")
    if error:
        return None, error
    assert pole is not None and reversal is not None
    expected = ("O", "X") if direction == "LONG" else ("X", "O")
    if (pole.kind, reversal.kind) != expected:
        return None, f"pole/reversal kinds are {pole.kind}/{reversal.kind}; expected {expected[0]}/{expected[1]} for {direction}"
    if direction == "LONG":
        distance = reversal.top - (pole.bottom - box_size)
    else:
        distance = (pole.top + box_size) - reversal.bottom
    return _integral_positive_boxes(distance, box_size, "SL-A reversal/pullback")


def _sl_b_distance(row: dict[str, Any], columns: dict[int, CsvColumn], box_size: float, direction: str) -> tuple[float | None, str]:
    pole_idx = _to_int(row.get("pole_column_index"))
    partner_idx = _to_int(row.get("opposing_pole_partner_index"))
    if pole_idx is None:
        return None, "labeled pole_column_index is missing"
    if partner_idx is None:
        return None, "labeled opposing_pole_partner_index is missing; previous opposing pole cannot be identified safely"
    if partner_idx >= pole_idx:
        return None, f"labeled opposing pole partner {partner_idx} is not previous to setup pole {pole_idx}; refusing lookahead geometry"
    partner, error = _column(columns, partner_idx, "opposing_pole_partner_index")
    if error:
        return None, error
    reversal, error = _column(columns, _to_int(row.get("reversal_column_index")), "reversal_column_index")
    if error:
        return None, error
    assert partner is not None and reversal is not None
    expected_partner = "O" if direction == "LONG" else "X"
    expected_reversal = "X" if direction == "LONG" else "O"
    if partner.kind != expected_partner or reversal.kind != expected_reversal:
        return None, f"opposing/reversal kinds are {partner.kind}/{reversal.kind}; expected {expected_partner}/{expected_reversal} for {direction}"
    if direction == "LONG":
        distance = reversal.top - (partner.bottom - box_size)
    else:
        distance = (partner.top + box_size) - reversal.bottom
    return _integral_positive_boxes(distance, box_size, "SL-B previous-opposing-pole")


def _candidate_distance(candidate: str, row: dict[str, Any], columns: dict[int, CsvColumn], box_size: float | None, direction: str) -> tuple[float | None, str]:
    if candidate == "SL-C":
        return 3.0, "fixed 3-box benchmark"
    if box_size is None or box_size <= 0:
        return None, "PnF columns do not expose a safe positive box size"
    if not columns:
        return None, "matching PnF columns CSV is unavailable or empty"
    if candidate == "SL-A":
        return _sl_a_distance(row, columns, box_size, direction)
    return _sl_b_distance(row, columns, box_size, direction)


def _load_symbol(symbol: str, labeled_path: Path, columns_path: Path | None) -> SymbolLoad:
    columns: dict[int, CsvColumn] = {}
    box_size: float | None = None
    flags: list[dict[str, str]] = []
    if columns_path is not None:
        loaded_columns, box_size = load_columns_csv(columns_path)
        columns = {column.idx: column for column in loaded_columns}
    if not columns:
        flags.append(_flag(symbol, "", "ALL", "columns_geometry", "WARN", "matching PnF columns CSV is unavailable or empty; SL-A and SL-B rows will be excluded"))

    with labeled_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        missing = [field for field in REQUIRED_COLUMNS if field not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{symbol}: missing labeled outcome columns: {', '.join(missing)}")
        raw_rows = list(reader)

    base_rows: list[BaseObservation] = []
    for row_number, row in enumerate(raw_rows, start=2):
        if not _is_core_motif(row):
            continue
        direction = _direction(row.get("pattern_name"))
        retrace_ratio = _to_float(row.get("retrace_ratio"))
        favorable_path = _parse_path(row.get("fav_path"))
        adverse_path = _parse_path(row.get("adv_path"))
        if direction is None or retrace_ratio is None or favorable_path is None or adverse_path is None or len(favorable_path) != len(adverse_path):
            flags.append(_flag(symbol, row_number, "ALL", "labeled_path", "EXCLUDED", "core motif row lacks a valid direction, retrace ratio, or equal-length non-empty fav_path/adv_path"))
            continue
        base_rows.append(BaseObservation(symbol, row_number, direction, retrace_ratio, favorable_path, adverse_path, row))

    eligible_counts = {
        (scope, direction): sum(
            _is_core_motif(row)
            and _to_float(row.get("retrace_ratio")) is not None
            and (scope == "core_motif" or (_to_float(row.get("retrace_ratio")) or 0.0) > 1.0)
            and (direction == "BOTH" or _direction(row.get("pattern_name")) == direction)
            for row in raw_rows
        )
        for scope in SCOPES
        for direction in DIRECTIONS
    }
    observations: list[Observation] = []
    excluded = {candidate: 0 for candidate in SL_CANDIDATES}
    for base in base_rows:
        for candidate in SL_CANDIDATES:
            distance, details = _candidate_distance(candidate, base.row, columns, box_size, base.direction)
            if distance is None:
                excluded[candidate] += 1
                flags.append(_flag(symbol, base.row_number, candidate, "sl_geometry", "EXCLUDED", details))
                continue
            observations.append(Observation(base.symbol, base.row_number, base.direction, base.retrace_ratio, candidate, distance, base.favorable_path_boxes, base.adverse_path_boxes))

    for candidate in SL_CANDIDATES:
        valid = sum(row.sl_candidate == candidate for row in observations)
        result = "OK" if valid else "WARN"
        flags.append(_flag(symbol, "", candidate, "reconstruction_coverage", result, f"valid reconstructed rows={valid}; excluded rows={excluded[candidate]}"))
    return SymbolLoad(symbol, len(raw_rows), len([row for row in raw_rows if _is_core_motif(row)]), tuple(observations), eligible_counts, tuple(flags))


def _flag(symbol: str, row_number: int | str, candidate: str, check_name: str, result: str, details: str) -> dict[str, str]:
    return {"symbol": symbol, "row_number": str(row_number), "sl_candidate": candidate, "check_name": check_name, "result": result, "details": details}


def _result_before_target(row: Observation, target_r: float) -> str:
    target_boxes = target_r * row.stop_distance_boxes
    for favorable, adverse in zip(row.favorable_path_boxes, row.adverse_path_boxes):
        if favorable >= target_boxes:
            return "HIT"
        if adverse >= row.stop_distance_boxes:
            return "STOP"
    return "UNRESOLVED"


def _curve_row(rows: list[Observation], *, scope: str, direction: str, candidate: str, target: float, excluded_rows: int) -> dict[str, Any]:
    hits = sum(_result_before_target(row, target) == "HIT" for row in rows)
    stops = sum(_result_before_target(row, target) == "STOP" for row in rows)
    stop_before_1r = sum(_result_before_target(row, 1.0) == "STOP" for row in rows)
    distances = [row.stop_distance_boxes for row in rows]
    return {
        "scope": scope, "direction": direction, "sl_candidate": candidate, "r_target": target,
        "valid_reconstructed_rows": len(rows), "excluded_rows": excluded_rows,
        "average_r_distance_boxes": _round(mean(distances)) if distances else 0.0,
        "median_r_distance_boxes": _round(median(distances)) if distances else 0.0,
        "stop_before_1r_rate": _round(_safe_div(stop_before_1r, len(rows))),
        "hit_rate": _round(_safe_div(hits, len(rows))),
        "expectancy_r": _round(_safe_div(hits * target - stops, len(rows))),
    }


def _scope_rows(rows: Iterable[Observation], scope: str, direction: str, candidate: str) -> list[Observation]:
    return [row for row in rows if row.sl_candidate == candidate and (scope == "core_motif" or row.retrace_ratio > 1.0) and (direction == "BOTH" or row.direction == direction)]


def _curve_for(rows: Iterable[Observation], *, eligible_counts: dict[tuple[str, str], int]) -> list[dict[str, Any]]:
    source = list(rows)
    curve: list[dict[str, Any]] = []
    for scope in SCOPES:
        for direction in DIRECTIONS:
            for candidate in SL_CANDIDATES:
                selected = _scope_rows(source, scope, direction, candidate)
                for target in R_TARGETS:
                    curve.append(_curve_row(selected, scope=scope, direction=direction, candidate=candidate, target=target, excluded_rows=eligible_counts[(scope, direction)] - len(selected)))
    return curve


def _core_pooled(curve: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in curve if row["scope"] == "core_motif" and row["direction"] == "BOTH"]


def _candidate_best_expectancy(curve: Iterable[dict[str, Any]]) -> tuple[str, float, float] | None:
    eligible = [row for row in _core_pooled(curve) if row["valid_reconstructed_rows"]]
    if not eligible:
        return None
    best = max(eligible, key=lambda row: (row["expectancy_r"], row["hit_rate"], -row["r_target"]))
    return best["sl_candidate"], best["r_target"], best["expectancy_r"]


def _stability_candidate(symbol_rows: Iterable[dict[str, Any]]) -> str:
    scores: list[tuple[int, float, str]] = []
    for candidate in SL_CANDIDATES:
        rows = [row for row in symbol_rows if row["scope"] == "core_motif" and row["direction"] == "BOTH" and row["sl_candidate"] == candidate and row["r_target"] == 2.0 and row["valid_reconstructed_rows"]]
        scores.append((len(rows), mean([row["expectancy_r"] for row in rows]) if rows else float("-inf"), candidate))
    valid = [score for score in scores if score[0]]
    return max(valid, key=lambda score: (score[0], score[1]))[2] if valid else "INSUFFICIENT_SAFE_GEOMETRY"


def _metric(curve: Iterable[dict[str, Any]], candidate: str, target: float, field: str) -> float | None:
    row = next((item for item in _core_pooled(curve) if item["sl_candidate"] == candidate and item["r_target"] == target), None)
    return row[field] if row and row["valid_reconstructed_rows"] else None


def _conclusions(curve: list[dict[str, Any]], symbol_rows: list[dict[str, Any]]) -> dict[str, str]:
    best = _candidate_best_expectancy(curve)
    best_text = f"{best[0]} at {best[1]:g}R ({best[2]:g}R expectancy)" if best else "INSUFFICIENT_SAFE_GEOMETRY"
    stability = _stability_candidate(symbol_rows)
    a_stop, b_exp, c_exp = _metric(curve, "SL-A", 1.0, "stop_before_1r_rate"), _metric(curve, "SL-B", 2.0, "expectancy_r"), _metric(curve, "SL-C", 2.0, "expectancy_r")
    tight = "INSUFFICIENT_SAFE_GEOMETRY" if a_stop is None else ("YES" if a_stop > 0.5 else "NO")
    wider = "INSUFFICIENT_SAFE_GEOMETRY" if b_exp is None else ("YES" if b_exp <= 0 else "NO")
    competitive = "INSUFFICIENT_SAFE_GEOMETRY" if c_exp is None else ("YES" if best and c_exp >= best[2] - 0.1 else "NO")
    recommended = best[0] if best else "INSUFFICIENT_SAFE_GEOMETRY"
    return {
        "best SL candidate for expectancy": best_text,
        "best SL candidate for stability": stability,
        "tight SL too fragile": tight,
        "wider SL reduces R too much": wider,
        "fixed 3-box benchmark competitive": competitive,
        "recommended SL candidate for next execution simulation": recommended,
    }


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_summary(path: Path, loads: list[SymbolLoad], curve: list[dict[str, Any]], conclusions: dict[str, str]) -> None:
    with path.open("w") as f:
        f.write("# PnF Pole Core Motif Structural SL Candidate Audit\n\n")
        f.write("Research-only SL hypothesis audit for `opposing_pole_distance_columns = 3 AND enhanced_by_opposing_pole = False`. This is not an execution simulation and does not promote any stop to production. No strategy, execution, TP/SL production logic, database schema, or stable baseline behavior is changed.\n\n")
        f.write("## Reconstruction safety\n\n")
        f.write("SL-A uses the exact pullback pole-column extreme plus one invalidation box. SL-B is reconstructed only when the labeled opposing-pole partner exists and is strictly previous to the setup pole; future partners are excluded as unsafe lookahead geometry. SL-C is an explicit fixed 3-box benchmark. Completed-column `fav_path` and `adv_path` sequences support a structural excursion audit, not intrabar execution claims.\n\n")
        f.write("## Required conclusions\n\n")
        for label, value in conclusions.items():
            f.write(f"- {label}: **{value}**\n")
        f.write("\n## Geometry coverage\n\n")
        f.write("| symbol | loaded labeled rows | core motif rows | SL-A valid | SL-B valid | SL-C valid |\n")
        f.write("|---|---:|---:|---:|---:|---:|\n")
        for load in loads:
            counts = {candidate: sum(row.sl_candidate == candidate for row in load.observations) for candidate in SL_CANDIDATES}
            f.write(f"| {load.symbol} | {load.loaded_rows} | {load.motif_rows} | {counts['SL-A']} | {counts['SL-B']} | {counts['SL-C']} |\n")
        f.write("\n## Pooled core-motif curves\n\n")
        f.write("| SL | target | valid | excluded | avg risk distance boxes | median risk distance boxes | stop before 1R | hit rate | expectancy R |\n")
        f.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in _core_pooled(curve):
            f.write(f"| {row['sl_candidate']} | {row['r_target']} | {row['valid_reconstructed_rows']} | {row['excluded_rows']} | {row['average_r_distance_boxes']} | {row['median_r_distance_boxes']} | {row['stop_before_1r_rate']} | {row['hit_rate']} | {row['expectancy_r']} |\n")
        f.write("\n## Required experiment scorecard\n\n")
        f.write("This structural excursion audit does not register or resolve strategy candidates. `candidate_rows_registered`, `resolved_rows`, `win_rate_non_ambiguous`, `avg_realized_r_multiple`, `total_realized_r_multiple`, and `TP1 -> TP2 conversion` are all `NOT_COMPUTED_FOR_STRUCTURAL_SL_AUDIT`.\n")


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
    eligible_counts = {(scope, direction): sum(load.eligible_counts[(scope, direction)] for load in loads) for scope in SCOPES for direction in DIRECTIONS}
    curve = _curve_for(observations, eligible_counts=eligible_counts)
    directional = [row for row in curve if row["direction"] != "BOTH"]
    symbol_rows: list[dict[str, Any]] = []
    for load in loads:
        for row in _curve_for(load.observations, eligible_counts=load.eligible_counts):
            symbol_rows.append({"symbol": load.symbol, **row})
    flags = [flag for load in loads for flag in load.flags]
    flags.append(_flag("ALL", "", "ALL", "baseline_protection", "OK", "research-only outputs; no strategy, execution, or TP/SL production logic modified"))

    _write_summary(output_root / "sl_candidate_summary.md", loads, curve, _conclusions(curve, symbol_rows))
    _write_csv(output_root / "sl_candidate_curve.csv", CURVE_FIELDS, curve)
    _write_csv(output_root / "sl_candidate_symbol_breakdown.csv", SYMBOL_FIELDS, symbol_rows)
    _write_csv(output_root / "sl_candidate_directional.csv", DIRECTIONAL_FIELDS, directional)
    _write_csv(output_root / "sl_candidate_flags.csv", FLAG_FIELDS, flags)


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only structural SL candidate audit for the PnF pole core motif")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", default=[], type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    run(dict(args.symbol_input), dict(args.columns_input), args.output_root)


if __name__ == "__main__":
    main()
