"""Research-only expectancy and risk-distance audit for NEXT_COLUMN_OPEN_ENTRY."""
from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_entry_timing_audit import (
    Candle,
    EntryTimingObservation,
    _candidate_observation,
    _classify,
    _load_candles,
)
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import (
    _check_symbols,
    _load_columns,
    _parse_candle_symbol,
    _require_paths,
)
from research_v2.patterns.pole_core_motif_sl_candidates import (
    R_TARGETS,
    _direction,
    _is_core_motif,
    _parse_symbol_input,
    _round,
    _to_int,
)

ENTRY_CANDIDATE = "NEXT_COLUMN_OPEN_ENTRY"
COMBINED = "COMBINED"
OUTPUT_NAMES = (
    "next_open_expectancy_summary.md",
    "next_open_expectancy_symbol_breakdown.csv",
    "next_open_expectancy_targets.csv",
    "next_open_expectancy_risk_distribution.csv",
    "next_open_expectancy_manifest.json",
)
UNKNOWN = {"UNKNOWN_MISSING_CANDLES", "UNKNOWN_UNMAPPABLE_COLUMN_TIME", "ANCHOR_NOT_OBSERVABLE"}
SYMBOL_FIELDS = [
    "symbol", "observations", "observable_risk_rows", "median_stop_boxes", "mean_stop_boxes",
    "p90_stop_boxes", "max_stop_boxes", "median_stop_percent", "mean_stop_percent",
]
TARGET_FIELDS = [
    "symbol", "r_target", "observations", "target_first", "stop_first", "ambiguous",
    "not_reached", "unknown", "resolved", "win_rate", "loss_rate", "expected_R",
]
RISK_FIELDS = [
    "symbol", "row_number", "direction", "entry_candidate", "box_size", "entry_price",
    "stop_price", "stop_distance", "stop_distance_boxes", "stop_distance_percent",
    "observable_entry_ts", "geometry_status", "geometry_details",
]


def _percentile_90(values: list[float]) -> float | str:
    """Return the nearest-rank p90 so small synthetic and empirical samples are reproducible."""
    if not values:
        return ""
    return _round(sorted(values)[math.ceil(0.9 * len(values)) - 1])


def _risk_values(observations: list[EntryTimingObservation]) -> tuple[list[float], list[float]]:
    boxes: list[float] = []
    percents: list[float] = []
    for row in observations:
        if row.entry is None or row.stop is None:
            continue
        distance = abs(row.entry - row.stop)
        boxes.append(distance / row.box_size)
        if row.entry != 0:
            percents.append(distance / abs(row.entry) * 100)
    return boxes, percents


def _risk_summary(symbol: str, observations: list[EntryTimingObservation]) -> dict[str, Any]:
    boxes, percents = _risk_values(observations)
    return {
        "symbol": symbol,
        "observations": len(observations),
        "observable_risk_rows": len(boxes),
        "median_stop_boxes": _round(median(boxes)) if boxes else "",
        "mean_stop_boxes": _round(mean(boxes)) if boxes else "",
        "p90_stop_boxes": _percentile_90(boxes),
        "max_stop_boxes": _round(max(boxes)) if boxes else "",
        "median_stop_percent": _round(median(percents)) if percents else "",
        "mean_stop_percent": _round(mean(percents)) if percents else "",
    }


def _target_summary(symbol: str, observations: list[EntryTimingObservation], targets: list[dict[str, Any]], target_r: float) -> dict[str, Any]:
    counts = Counter(row["classification"] for row in targets if row["r_target"] == target_r)
    wins, losses = counts["TARGET_FIRST"], counts["STOP_FIRST"]
    resolved = wins + losses
    return {
        "symbol": symbol,
        "r_target": target_r,
        "observations": len(observations),
        "target_first": wins,
        "stop_first": losses,
        "ambiguous": counts["SAME_CANDLE_AMBIGUOUS"],
        "not_reached": counts["NOT_REACHED"],
        "unknown": sum(counts[name] for name in UNKNOWN),
        "resolved": resolved,
        "win_rate": _round(wins / resolved) if resolved else "",
        "loss_rate": _round(losses / resolved) if resolved else "",
        "expected_R": _round((wins * target_r - losses) / resolved) if resolved else "",
    }


def _risk_row(row: EntryTimingObservation) -> dict[str, Any]:
    distance = None if row.entry is None or row.stop is None else abs(row.entry - row.stop)
    return {
        "symbol": row.symbol,
        "row_number": row.row_number,
        "direction": row.direction,
        "entry_candidate": row.entry_candidate,
        "box_size": _round(row.box_size),
        "entry_price": "" if row.entry is None else _round(row.entry),
        "stop_price": "" if row.stop is None else _round(row.stop),
        "stop_distance": "" if distance is None else _round(distance),
        "stop_distance_boxes": "" if distance is None else _round(distance / row.box_size),
        "stop_distance_percent": "" if distance is None or row.entry == 0 else _round(distance / abs(row.entry) * 100),
        "observable_entry_ts": row.observable_entry_ts or "",
        "geometry_status": row.geometry_status,
        "geometry_details": row.geometry_details,
    }


def _verdict(target_rows: list[dict[str, Any]]) -> tuple[str, str]:
    combined = {row["r_target"]: row for row in target_rows if row["symbol"] == COMBINED}
    one_r, two_r = combined[1.0], combined[2.0]
    if not one_r["resolved"] or one_r["expected_R"] <= 0:
        return "DISCARD", "combined resolved 1R expectancy is unavailable or non-positive"
    positive_two_r_symbols = sum(
        row["symbol"] != COMBINED and row["r_target"] == 2.0 and row["expected_R"] != "" and row["expected_R"] > 0
        for row in target_rows
    )
    if two_r["resolved"] and two_r["expected_R"] > 0 and positive_two_r_symbols >= 2:
        return "HIGH_PRIORITY_RESEARCH", "combined 1R and 2R expectancy are positive and at least two symbols have positive resolved 2R expectancy"
    return "KEEP_AS_RESEARCH", "combined resolved 1R expectancy is positive; broader 2R robustness threshold is not met"


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _load_observations(
    symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], candles_inputs: dict[str, Path], candle_symbols: dict[str, str]
) -> tuple[list[str], list[EntryTimingObservation], dict[str, list[Candle]]]:
    symbols = _check_symbols(symbol_inputs, columns_inputs, candles_inputs)
    _require_paths(symbol_inputs, "symbol-input")
    _require_paths(columns_inputs, "columns-input")
    _require_paths(candles_inputs, "candles-input")
    observations: list[EntryTimingObservation] = []
    candles_by_symbol: dict[str, list[Candle]] = {}
    for symbol in symbols:
        columns, box_size = _load_columns(columns_inputs[symbol])
        if box_size is None or box_size <= 0:
            raise ValueError(f"{symbol}: matching PnF columns do not expose a positive box size in profile_name")
        candles = _load_candles(candles_inputs[symbol], candle_symbols.get(symbol, symbol))
        candles_by_symbol[symbol] = candles
        with symbol_inputs[symbol].open("r", newline="") as handle:
            rows = list(csv.DictReader(handle))
        for row_number, row in enumerate(rows, start=2):
            if not _is_core_motif(row):
                continue
            direction = _direction(row.get("pattern_name"))
            pole_idx, reversal_idx = _to_int(row.get("pole_column_index")), _to_int(row.get("reversal_column_index"))
            pole = columns.get(pole_idx) if pole_idx is not None else None
            reversal = columns.get(reversal_idx) if reversal_idx is not None else None
            expected = ("O", "X") if direction == "LONG" else ("X", "O")
            if direction is None or pole is None or reversal is None or (pole.kind, reversal.kind) != expected:
                continue
            observations.append(_candidate_observation(symbol, row_number, direction, ENTRY_CANDIDATE, pole, reversal, columns.get(reversal.idx + 1), box_size, candles))
    return symbols, observations, candles_by_symbol


def run(
    symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], candles_inputs: dict[str, Path], output_root: Path,
    candle_symbols: dict[str, str] | None = None,
) -> None:
    symbols, observations, candles_by_symbol = _load_observations(symbol_inputs, columns_inputs, candles_inputs, candle_symbols or {})
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing next-open expectancy output(s): {', '.join(existing)}")
    targets = [_classify(row, candles_by_symbol[row.symbol], target_r) for row in observations for target_r in R_TARGETS]
    symbol_rows = [_risk_summary(symbol, [row for row in observations if row.symbol == symbol]) for symbol in symbols]
    symbol_rows.append(_risk_summary(COMBINED, observations))
    target_rows: list[dict[str, Any]] = []
    for symbol in [*symbols, COMBINED]:
        scoped_observations = observations if symbol == COMBINED else [row for row in observations if row.symbol == symbol]
        scoped_targets = targets if symbol == COMBINED else [row for row in targets if row["symbol"] == symbol]
        target_rows.extend(_target_summary(symbol, scoped_observations, scoped_targets, target_r) for target_r in R_TARGETS)
    verdict, reason = _verdict(target_rows)
    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# NEXT_COLUMN_OPEN_ENTRY expectancy and risk-distance audit\n\n")
        handle.write("Research only. This isolated audit evaluates only `NEXT_COLUMN_OPEN_ENTRY`, does not modify production strategy or prior audits, and never outputs `PROMOTE`.\n\n")
        handle.write("Expectancy uses resolved rows only: `TARGET_FIRST` is +target R and `STOP_FIRST` is -1R. Ambiguous, not-reached, and unknown rows remain visible but are excluded from win rate, loss rate, and expected R.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n{reason}.\n\n")
        handle.write("## Risk geometry\n\n| symbol | observations | median stop boxes | mean stop boxes | p90 stop boxes | max stop boxes |\n|---|---:|---:|---:|---:|---:|\n")
        for row in symbol_rows:
            handle.write(f"| {row['symbol']} | {row['observations']} | {row['median_stop_boxes']} | {row['mean_stop_boxes']} | {row['p90_stop_boxes']} | {row['max_stop_boxes']} |\n")
        handle.write("\n## Target progression and expectancy\n\n| symbol | target R | target first | stop first | ambiguous | resolved | win rate | loss rate | expected R |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in target_rows:
            handle.write(f"| {row['symbol']} | {row['r_target']} | {row['target_first']} | {row['stop_first']} | {row['ambiguous']} | {row['resolved']} | {row['win_rate']} | {row['loss_rate']} | {row['expected_R']} |\n")
    _write_csv(output_root / OUTPUT_NAMES[1], SYMBOL_FIELDS, symbol_rows)
    _write_csv(output_root / OUTPUT_NAMES[2], TARGET_FIELDS, target_rows)
    _write_csv(output_root / OUTPUT_NAMES[3], RISK_FIELDS, (_risk_row(row) for row in observations))
    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "stage": "next_open_expectancy_audit",
        "research_only": True,
        "strategy_promotion": False,
        "entry_candidate": ENTRY_CANDIDATE,
        "symbols": symbols,
        "r_targets": list(R_TARGETS),
        "expectancy_assumption": {"winner": "+target_R", "loser": "-1R", "denominator": "TARGET_FIRST + STOP_FIRST"},
        "verdict": verdict,
        "verdict_reason": reason,
        "allowed_verdicts": ["DISCARD", "KEEP_AS_RESEARCH", "HIGH_PRIORITY_RESEARCH"],
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[4]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only NEXT_COLUMN_OPEN_ENTRY expectancy and risk-distance audit")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    try:
        run(dict(args.symbol_input), dict(args.columns_input), dict(args.candles_input), args.output_root, dict(args.candle_symbol))
    except (FileExistsError, OSError, sqlite3.Error, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
