from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from research_v2.patterns.pole_outcomes import CsvColumn, load_columns_csv

EXPECTED_SYMBOLS = ("BTC", "ETH", "SOL", "SUI", "TAO", "ENA", "HYPE")
R_TARGETS = (1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0)
SCOPES = ("core_motif", "quality_subset")
DIRECTIONS = ("BOTH", "LONG", "SHORT")
NA_VALUES = {"", "na", "none", "null", "nan"}
REQUIRED_COLUMNS = (
    "pattern_name",
    "opposing_pole_distance_columns",
    "enhanced_by_opposing_pole",
    "retrace_ratio",
    "retrace_boxes",
    "max_favorable_boxes",
    "max_adverse_boxes",
)
CURVE_FIELDS = [
    "scope", "direction", "r_target", "sample_size", "hit_rate", "stopped_before_target_rate",
    "average_max_favorable_r", "median_max_favorable_r", "average_max_adverse_r", "target_efficiency",
    "expectancy_r", "best_expectancy_r_target", "hit_rate_collapse_point",
]
SYMBOL_FIELDS = ["symbol", *CURVE_FIELDS]
FLAG_FIELDS = ["symbol", "check_name", "result", "details"]


@dataclass(frozen=True)
class Observation:
    symbol: str
    direction: str
    retrace_ratio: float
    stop_boxes: float
    max_favorable_r: float
    max_adverse_r: float
    favorable_path_r: tuple[float, ...]
    adverse_path_r: tuple[float, ...]
    geometry_mode: str


@dataclass(frozen=True)
class SymbolLoad:
    symbol: str
    labeled_path: Path
    columns_path: Path | None
    observations: tuple[Observation, ...]
    loaded_rows: int
    motif_rows: int
    excluded_rows: int
    reconstructed_rows: int
    fallback_rows: int
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


def _safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _round(value: float) -> float:
    return round(value, 6)


def _parse_symbol_input(value: str) -> tuple[str, Path]:
    symbol, separator, raw_path = value.partition("=")
    symbol, raw_path = symbol.strip().upper(), raw_path.strip()
    if not separator or not symbol or not raw_path:
        raise argparse.ArgumentTypeError("expected SYMBOL=path/to/file.csv")
    return symbol, Path(raw_path)


def _parse_path(value: Any) -> tuple[float, ...]:
    text = _clean(value)
    if not text:
        return ()
    try:
        return tuple(float(part.strip()) for part in text.split(",") if part.strip())
    except ValueError:
        return ()


def _direction(pattern_name: Any) -> str | None:
    pattern = _clean(pattern_name).upper()
    if pattern == "LOW_POLE":
        return "LONG"
    if pattern == "HIGH_POLE":
        return "SHORT"
    return None


def _is_core_motif(row: dict[str, Any]) -> bool:
    return _to_int(row.get("opposing_pole_distance_columns")) == 3 and _to_bool(row.get("enhanced_by_opposing_pole")) is False


def _reconstructed_stop_boxes(
    row: dict[str, Any], columns: dict[int, CsvColumn], box_size: float | None, direction: str
) -> tuple[float | None, str]:
    if box_size is None or box_size <= 0:
        return None, "PnF columns do not expose a safe positive box size"
    reversal_idx = _to_int(row.get("reversal_column_index"))
    if reversal_idx is None or reversal_idx not in columns:
        return None, "labeled reversal_column_index is missing from PnF columns"
    reversal = columns[reversal_idx]
    expected_kind = "X" if direction == "LONG" else "O"
    if reversal.kind != expected_kind:
        return None, f"reversal column kind is {reversal.kind}; expected {expected_kind}"
    # Excursions are labeled from the reversal-column extreme. One extra box places
    # invalidation beyond the pullback column instead of on its boundary.
    stop_boxes = abs(reversal.top - reversal.bottom) / box_size + 1.0
    rounded = round(stop_boxes)
    if stop_boxes <= 0 or abs(stop_boxes - rounded) > 1e-6:
        return None, f"reversal-column stop distance {stop_boxes:g} is not an integral box count"
    return float(rounded), "reconstructed from reversal-column boundary plus one invalidation box"


def _load_symbol(symbol: str, labeled_path: Path, columns_path: Path | None) -> SymbolLoad:
    columns: dict[int, CsvColumn] = {}
    box_size: float | None = None
    flags: list[dict[str, str]] = []
    if columns_path is not None:
        loaded_columns, box_size = load_columns_csv(columns_path)
        columns = {column.idx: column for column in loaded_columns}
        if not columns:
            flags.append({"symbol": symbol, "check_name": "columns_geometry", "result": "WARN", "details": "columns CSV is empty; using excursion-only fallback"})
    else:
        flags.append({"symbol": symbol, "check_name": "columns_geometry", "result": "WARN", "details": "columns CSV not supplied; using excursion-only fallback"})

    with labeled_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        missing = [field for field in REQUIRED_COLUMNS if field not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{symbol}: missing labeled outcome columns: {', '.join(missing)}")
        raw_rows = list(reader)

    observations: list[Observation] = []
    motif_rows = excluded_rows = reconstructed_rows = fallback_rows = 0
    fallback_reasons: dict[str, int] = {}
    for row in raw_rows:
        if not _is_core_motif(row):
            continue
        motif_rows += 1
        direction = _direction(row.get("pattern_name"))
        retrace_ratio = _to_float(row.get("retrace_ratio"))
        fallback_stop = _to_float(row.get("retrace_boxes"))
        favorable = _to_float(row.get("max_favorable_boxes"))
        adverse = _to_float(row.get("max_adverse_boxes"))
        if direction is None or retrace_ratio is None or fallback_stop is None or fallback_stop <= 0 or favorable is None or adverse is None:
            excluded_rows += 1
            continue

        stop_boxes: float | None = None
        geometry_detail = "columns CSV not supplied"
        if columns_path is not None:
            stop_boxes, geometry_detail = _reconstructed_stop_boxes(row, columns, box_size, direction)
        if stop_boxes is None:
            stop_boxes = fallback_stop
            geometry_mode = "EXCURSION_ONLY_FALLBACK"
            fallback_rows += 1
            fallback_reasons[geometry_detail] = fallback_reasons.get(geometry_detail, 0) + 1
        else:
            geometry_mode = "RECONSTRUCTED_PULLBACK_SL"
            reconstructed_rows += 1

        favorable_path = _parse_path(row.get("fav_path"))
        adverse_path = _parse_path(row.get("adv_path"))
        observations.append(
            Observation(
                symbol=symbol,
                direction=direction,
                retrace_ratio=retrace_ratio,
                stop_boxes=stop_boxes,
                max_favorable_r=favorable / stop_boxes,
                max_adverse_r=adverse / stop_boxes,
                favorable_path_r=tuple(value / stop_boxes for value in favorable_path),
                adverse_path_r=tuple(value / stop_boxes for value in adverse_path),
                geometry_mode=geometry_mode,
            )
        )

    if reconstructed_rows:
        flags.append({"symbol": symbol, "check_name": "reconstructed_pullback_sl", "result": "OK", "details": f"{reconstructed_rows} motif rows reconstructed from raw columns"})
    if fallback_rows:
        reason_text = "; ".join(f"{reason} ({count})" for reason, count in sorted(fallback_reasons.items()))
        flags.append({"symbol": symbol, "check_name": "excursion_only_fallback", "result": "WARN", "details": f"{fallback_rows} motif rows use labeled retrace_boxes as stop-distance approximation: {reason_text}"})
    if excluded_rows:
        flags.append({"symbol": symbol, "check_name": "excluded_motif_rows", "result": "WARN", "details": f"{excluded_rows} motif rows excluded because required labeled values were invalid"})
    flags.append({"symbol": symbol, "check_name": "execution_backtest_status", "result": "INFO", "details": "R excursion study only; completed-column paths do not prove executable intrabar entry/stop ordering"})
    return SymbolLoad(symbol, labeled_path, columns_path, tuple(observations), len(raw_rows), motif_rows, excluded_rows, reconstructed_rows, fallback_rows, tuple(flags))


def _scope_rows(rows: Iterable[Observation], scope: str, direction: str = "BOTH") -> list[Observation]:
    return [row for row in rows if (scope == "core_motif" or row.retrace_ratio > 1.0) and (direction == "BOTH" or row.direction == direction)]


def _first_index(values: tuple[float, ...], threshold: float) -> int | None:
    return next((index for index, value in enumerate(values) if value >= threshold), None)


def _target_outcome(row: Observation, target: float) -> tuple[bool, bool]:
    target_index = _first_index(row.favorable_path_r, target)
    stop_index = _first_index(row.adverse_path_r, 1.0)
    if target_index is not None or stop_index is not None:
        hit = target_index is not None and (stop_index is None or target_index < stop_index)
        stopped = stop_index is not None and (target_index is None or stop_index < target_index)
        return hit, stopped
    # Without paths, max excursions can establish reach but not ordering when both
    # thresholds occur. Keep the conservative stop-first approximation explicit.
    stopped = row.max_adverse_r >= 1.0
    return row.max_favorable_r >= target and not stopped, stopped


def _collapse_point(rows: list[Observation]) -> str:
    baseline_hits = _safe_div(sum(_target_outcome(row, 1.5)[0] for row in rows), len(rows))
    for target in R_TARGETS:
        if target <= 1.5:
            continue
        hit_rate = _safe_div(sum(_target_outcome(row, target)[0] for row in rows), len(rows))
        if baseline_hits and (hit_rate < baseline_hits * 0.5 or baseline_hits - hit_rate >= 0.20):
            return f"{target:g}R"
    return "NONE_THROUGH_4R"


def _metrics(rows: list[Observation], target: float, best_target: float | None = None, collapse: str | None = None) -> dict[str, str | int | float]:
    hits = sum(_target_outcome(row, target)[0] for row in rows)
    stops = sum(_target_outcome(row, target)[1] for row in rows)
    n = len(rows)
    hit_rate = _safe_div(hits, n)
    stop_rate = _safe_div(stops, n)
    return {
        "r_target": target,
        "sample_size": n,
        "hit_rate": _round(hit_rate),
        "stopped_before_target_rate": _round(stop_rate),
        "average_max_favorable_r": _round(mean(row.max_favorable_r for row in rows)) if rows else 0.0,
        "median_max_favorable_r": _round(median(row.max_favorable_r for row in rows)) if rows else 0.0,
        "average_max_adverse_r": _round(mean(row.max_adverse_r for row in rows)) if rows else 0.0,
        "target_efficiency": _round(hit_rate * target),
        "expectancy_r": _round(hit_rate * target - stop_rate),
        "best_expectancy_r_target": "" if best_target is None else f"{best_target:g}R",
        "hit_rate_collapse_point": collapse or "",
    }


def _curve_for(rows: list[Observation], *, scope: str, direction: str) -> list[dict[str, str | int | float]]:
    preliminary = [_metrics(rows, target) for target in R_TARGETS]
    best = max(preliminary, key=lambda row: (float(row["expectancy_r"]), -float(row["r_target"]))) if preliminary else None
    best_target = float(best["r_target"]) if best is not None else None
    collapse = _collapse_point(rows)
    return [{"scope": scope, "direction": direction, **_metrics(rows, target, best_target, collapse)} for target in R_TARGETS]


def _maximum_realistic_target(curve: list[dict[str, str | int | float]]) -> str:
    rows = [row for row in curve if float(row["r_target"]) >= 1.5 and float(row["expectancy_r"]) > 0]
    return f"{max(float(row['r_target']) for row in rows):g}R" if rows else "NONE"


def _best_target(curve: list[dict[str, str | int | float]], *, maximum: float | None = None) -> str:
    rows = [row for row in curve if maximum is None or float(row["r_target"]) <= maximum]
    if not rows:
        return "NONE"
    row = max(rows, key=lambda value: (float(value["expectancy_r"]), -float(value["r_target"])))
    return f"{float(row['r_target']):g}R"


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _conclusions(curve: list[dict[str, str | int | float]], symbol_rows: list[dict[str, str | int | float]]) -> dict[str, str]:
    index = {(str(row["scope"]), str(row["direction"]), float(row["r_target"])): row for row in curve}
    core = [index[("core_motif", "BOTH", target)] for target in R_TARGETS]
    quality = [index[("quality_subset", "BOTH", target)] for target in R_TARGETS]
    long = [index[("core_motif", "LONG", target)] for target in R_TARGETS]
    short = [index[("core_motif", "SHORT", target)] for target in R_TARGETS]
    core_max = _maximum_realistic_target(core)
    quality_max = _maximum_realistic_target(quality)
    long_max = _maximum_realistic_target(long)
    short_max = _maximum_realistic_target(short)
    beyond = [row for row in core if float(row["r_target"]) > 1.5 and float(row["expectancy_r"]) > 0]
    collapsed_symbols = sorted({str(row["symbol"]) for row in symbol_rows if row["scope"] == "core_motif" and row["direction"] == "BOTH" and row["hit_rate_collapse_point"] != "NONE_THROUGH_4R" and int(row["sample_size"]) > 0})
    return {
        "is_1_5r_conservative": "YES" if beyond else "NO",
        "edge_survives_beyond_1_5r": "YES" if beyond else "NO",
        "maximum_realistic_r_target": core_max,
        "recommended_tp1": _best_target(core, maximum=1.5),
        "recommended_tp2": core_max,
        "quality_subset_supports_larger_r": "YES" if quality_max != "NONE" and (core_max == "NONE" or float(quality_max[:-1]) > float(core_max[:-1])) else "NO",
        "long_short_differ_in_target_capacity": "YES" if long_max != short_max else "NO",
        "long_maximum_realistic_r_target": long_max,
        "short_maximum_realistic_r_target": short_max,
        "symbols_collapsing_beyond_1_5r": ", ".join(collapsed_symbols) or "NONE",
    }


def _write_summary(path: Path, loads: list[SymbolLoad], curve: list[dict[str, Any]], symbol_rows: list[dict[str, Any]], conclusions: dict[str, str]) -> None:
    core = [row for row in curve if row["scope"] == "core_motif" and row["direction"] == "BOTH"]
    quality = [row for row in curve if row["scope"] == "quality_subset" and row["direction"] == "BOTH"]
    with path.open("w") as f:
        f.write("# Pole Core Motif Fixed-SL Multi-R Target Excursion Study\n\n")
        f.write("## Research-only status\n\n")
        f.write("This is an **R excursion study**, not a full execution backtest or trade simulation. It does not modify production strategy code, live trading logic, or the profitable baseline. Completed PnF-column paths cannot establish intrabar fills or executable ordering.\n\n")
        f.write("The core motif is `opposing_pole_distance_columns = 3 AND enhanced_by_opposing_pole = False`. The quality subset additionally requires `retrace_ratio > 1.0`. `LOW_POLE` is treated as LONG continuation and `HIGH_POLE` as SHORT continuation.\n\n")
        f.write("## Fixed-SL geometry\n\n")
        f.write("When raw columns safely reconstruct the reversal/pullback column, 1R is the distance from its continuation-side extreme to one box beyond its invalidation-side boundary. Otherwise, the report falls back to labeled `retrace_boxes` as the stop-distance denominator and labels those observations `EXCURSION_ONLY_FALLBACK`. Favorable and adverse excursions come from labeled completed-column paths where available.\n\n")
        f.write("## Conclusions\n\n")
        labels = {
            "is_1_5r_conservative": "is 1.5R conservative",
            "edge_survives_beyond_1_5r": "does edge survive beyond 1.5R",
            "maximum_realistic_r_target": "maximum realistic R target",
            "recommended_tp1": "recommended TP1",
            "recommended_tp2": "recommended TP2",
            "quality_subset_supports_larger_r": "whether quality subset supports larger R than core motif",
            "long_short_differ_in_target_capacity": "whether LONG/SHORT differ in target capacity",
            "long_maximum_realistic_r_target": "LONG maximum realistic R target",
            "short_maximum_realistic_r_target": "SHORT maximum realistic R target",
            "symbols_collapsing_beyond_1_5r": "symbols collapsing beyond 1.5R",
        }
        for key, label in labels.items():
            f.write(f"- {label}: **{conclusions[key]}**\n")
        f.write("\n`maximum realistic R target` is the largest tested target at or above 1.5R with positive excursion expectancy (`hit_rate × target - stopped_before_target_rate`). The hit-rate collapse point is the first tested target beyond 1.5R whose hit rate is below half of the 1.5R rate or at least 20 percentage points lower.\n\n")
        f.write("## Pooled target efficiency curve\n\n")
        f.write("| scope | R target | n | hit rate | stopped before target | avg max favorable R | median max favorable R | avg max adverse R | target efficiency | expectancy R |\n")
        f.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in [*core, *quality]:
            f.write(f"| {row['scope']} | {row['r_target']} | {row['sample_size']} | {row['hit_rate']} | {row['stopped_before_target_rate']} | {row['average_max_favorable_r']} | {row['median_max_favorable_r']} | {row['average_max_adverse_r']} | {row['target_efficiency']} | {row['expectancy_r']} |\n")
        f.write("\n## Geometry coverage\n\n")
        f.write("| symbol | loaded rows | motif rows | reconstructed SL rows | fallback rows | excluded motif rows |\n")
        f.write("|---|---:|---:|---:|---:|---:|\n")
        for load in loads:
            f.write(f"| {load.symbol} | {load.loaded_rows} | {load.motif_rows} | {load.reconstructed_rows} | {load.fallback_rows} | {load.excluded_rows} |\n")
        f.write("\n## Required experiment scorecard\n\n")
        f.write("This pole excursion study does not register or resolve strategy candidates. Baseline scorecard metrics remain deliberately uncomputed: `candidate_rows_registered`, `resolved_rows`, `win_rate_non_ambiguous`, `avg_realized_r_multiple`, `total_realized_r_multiple`, and `TP1 -> TP2 conversion` are all `NOT_COMPUTED_FOR_R_EXCURSION_STUDY`.\n")


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
    all_rows = [row for load in loads for row in load.observations]
    curve: list[dict[str, Any]] = []
    for scope in SCOPES:
        for direction in DIRECTIONS:
            curve.extend(_curve_for(_scope_rows(all_rows, scope, direction), scope=scope, direction=direction))

    directional = [row for row in curve if row["direction"] != "BOTH"]
    symbol_rows: list[dict[str, Any]] = []
    for load in loads:
        for scope in SCOPES:
            for direction in DIRECTIONS:
                for row in _curve_for(_scope_rows(load.observations, scope, direction), scope=scope, direction=direction):
                    symbol_rows.append({"symbol": load.symbol, **row})

    flags = [flag for load in loads for flag in load.flags]
    flags.append({"symbol": "ALL", "check_name": "baseline_protection", "result": "OK", "details": "research-only outputs; no strategy or live-trading code modified"})
    if any(load.fallback_rows for load in loads):
        flags.append({"symbol": "ALL", "check_name": "study_classification", "result": "WARN", "details": "fallback geometry present: call results R excursion study, never execution simulation"})
    else:
        flags.append({"symbol": "ALL", "check_name": "study_classification", "result": "INFO", "details": "raw geometry reconstructed, but completed-column paths still make this an R excursion study rather than a full execution backtest"})

    conclusions = _conclusions(curve, symbol_rows)
    _write_summary(output_root / "pole_core_motif_r_targets_summary.md", loads, curve, symbol_rows, conclusions)
    _write_csv(output_root / "pole_core_motif_r_target_curve.csv", CURVE_FIELDS, curve)
    _write_csv(output_root / "pole_core_motif_r_directional.csv", CURVE_FIELDS, directional)
    _write_csv(output_root / "pole_core_motif_r_symbol_breakdown.csv", SYMBOL_FIELDS, symbol_rows)
    _write_csv(output_root / "pole_core_motif_r_flags.csv", FLAG_FIELDS, flags)


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only fixed-SL multi-R excursion study for the PnF pole core motif")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", default=[], type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    run(dict(args.symbol_input), dict(args.columns_input), args.output_root)


if __name__ == "__main__":
    main()
