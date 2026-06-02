"""Research-only falsification audit for the surviving NEXT_COLUMN_OPEN_ENTRY candidate.

This module intentionally does not optimize entries, stops, targets, or production strategy.
It reuses the existing observable NEXT_COLUMN_OPEN_ENTRY chronology and fixed 3-box stop
logic to look for symbol/time dependence in the current research edge.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, EntryTimingObservation, _classify
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import (
    COMBINED,
    ENTRY_CANDIDATE,
    UNKNOWN,
    _load_observations,
)
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import R_TARGETS, _parse_symbol_input, _round

OUTPUT_NAMES = (
    "robustness_summary.md",
    "robustness_symbol_breakdown.csv",
    "robustness_half_breakdown.csv",
    "robustness_quartile_breakdown.csv",
    "robustness_flags.csv",
    "robustness_manifest.json",
)
SEGMENT_FIELDS = [
    "scope",
    "symbol",
    "segment",
    "r_target",
    "observations",
    "target_first",
    "stop_first",
    "ambiguous",
    "not_reached",
    "unknown",
    "resolved",
    "win_rate",
    "loss_rate",
    "expected_R",
]
FLAG_FIELDS = ["scope", "symbol", "segment", "r_target", "flag", "details"]
ALLOWED_VERDICTS = ("FRAGILE", "MODERATE", "ROBUST", "INSUFFICIENT_DATA")
MIN_MEANINGFUL_OBSERVATIONS = 5
MIN_RELIABLE_OBSERVATIONS = 10
PRIMARY_TARGET_R = 2.0


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _target_events(observations: list[EntryTimingObservation], candles_by_symbol: dict[str, list[Candle]]) -> list[dict[str, Any]]:
    return [_classify(row, candles_by_symbol[row.symbol], target_r) for row in observations for target_r in R_TARGETS]


def _segment_metrics(
    scope: str,
    symbol: str,
    segment: str,
    observations: list[EntryTimingObservation],
    targets: list[dict[str, Any]],
    target_r: float,
) -> dict[str, Any]:
    selected = [row for row in targets if row["r_target"] == target_r]
    counts = Counter(row["classification"] for row in selected)
    wins = counts["TARGET_FIRST"]
    losses = counts["STOP_FIRST"]
    resolved = wins + losses
    return {
        "scope": scope,
        "symbol": symbol,
        "segment": segment,
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


def _rows_for_segment(
    scope: str,
    symbol: str,
    segment: str,
    observations: list[EntryTimingObservation],
    targets: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [_segment_metrics(scope, symbol, segment, observations, targets, target_r) for target_r in R_TARGETS]


def _split_halves(observations: list[EntryTimingObservation]) -> dict[str, list[EntryTimingObservation]]:
    midpoint = len(observations) // 2
    return {"EARLY_HALF": observations[:midpoint], "LATE_HALF": observations[midpoint:]}


def _split_quartiles(observations: list[EntryTimingObservation]) -> dict[str, list[EntryTimingObservation]]:
    buckets = {"Q1": [], "Q2": [], "Q3": [], "Q4": []}
    names = tuple(buckets)
    total = len(observations)
    if total == 0:
        return buckets
    for index, observation in enumerate(observations):
        buckets[names[min(index * 4 // total, 3)]].append(observation)
    return buckets


def _filter_targets(targets: list[dict[str, Any]], observations: list[EntryTimingObservation]) -> list[dict[str, Any]]:
    keys = {(row.symbol, row.row_number) for row in observations}
    return [row for row in targets if (row["symbol"], row["row_number"]) in keys]


def _sample_flags(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    flags: list[dict[str, Any]] = []
    for row in rows:
        observations = int(row["observations"])
        if observations < MIN_MEANINGFUL_OBSERVATIONS:
            flags.append({
                "scope": row["scope"],
                "symbol": row["symbol"],
                "segment": row["segment"],
                "r_target": row["r_target"],
                "flag": "VERY_LOW_OBSERVATION_COUNT",
                "details": f"{observations} observations; below {MIN_MEANINGFUL_OBSERVATIONS}, inference is not meaningful",
            })
        elif observations < MIN_RELIABLE_OBSERVATIONS:
            flags.append({
                "scope": row["scope"],
                "symbol": row["symbol"],
                "segment": row["segment"],
                "r_target": row["r_target"],
                "flag": "UNRELIABLE_INFERENCE",
                "details": f"{observations} observations; below {MIN_RELIABLE_OBSERVATIONS}, do not overstate the segment result",
            })
    return flags


def _primary(rows: Iterable[dict[str, Any]], scope: str, symbol: str, segment: str) -> dict[str, Any] | None:
    return next((row for row in rows if row["scope"] == scope and row["symbol"] == symbol and row["segment"] == segment and row["r_target"] == PRIMARY_TARGET_R), None)


def _positive(row: dict[str, Any] | None) -> bool:
    return bool(row and row["expected_R"] != "" and row["expected_R"] > 0)


def _combined_stability_rows(symbol_rows: list[dict[str, Any]], half_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    full = _primary(symbol_rows, "SYMBOL", COMBINED, "FULL_SAMPLE")
    stability: list[dict[str, Any]] = []
    if full is None:
        return stability
    for segment in ("EARLY_HALF", "LATE_HALF"):
        row = _primary(half_rows, "HALF", COMBINED, segment)
        if row is None:
            continue
        stability.append({
            "segment": segment,
            "win_rate_delta": "" if full["win_rate"] == "" or row["win_rate"] == "" else _round(row["win_rate"] - full["win_rate"]),
            "expectancy_delta": "" if full["expected_R"] == "" or row["expected_R"] == "" else _round(row["expected_R"] - full["expected_R"]),
        })
    return stability


def _verdict(symbol_rows: list[dict[str, Any]], half_rows: list[dict[str, Any]], quartile_rows: list[dict[str, Any]], symbols: list[str]) -> tuple[str, str]:
    combined_full = _primary(symbol_rows, "SYMBOL", COMBINED, "FULL_SAMPLE")
    combined_half = [_primary(half_rows, "HALF", COMBINED, segment) for segment in ("EARLY_HALF", "LATE_HALF")]
    symbol_primary = [_primary(symbol_rows, "SYMBOL", symbol, "FULL_SAMPLE") for symbol in symbols]
    quartile_primary = [row for row in quartile_rows if row["r_target"] == PRIMARY_TARGET_R and row["scope"] == "QUARTILE"]
    all_primary = [row for row in [combined_full, *combined_half, *symbol_primary, *quartile_primary] if row is not None]
    if not all_primary or any(int(row["observations"]) < MIN_MEANINGFUL_OBSERVATIONS for row in all_primary):
        return "INSUFFICIENT_DATA", "one or more primary 2R symbol/time segments have very low observation counts"
    if not _positive(combined_full):
        return "FRAGILE", "combined full-sample 2R expectancy is unavailable or non-positive under the falsification audit"
    positive_symbols = sum(_positive(row) for row in symbol_primary)
    positive_halves = sum(_positive(row) for row in combined_half)
    positive_quartiles = sum(_positive(row) for row in quartile_primary)
    if positive_symbols <= 1 or positive_halves < 2 or positive_quartiles < max(1, len(quartile_primary) - 1):
        return "FRAGILE", "positive combined expectancy depends on a small subset of symbols or chronology segments"
    unstable_rows = [row for row in [*symbol_primary, *combined_half, *quartile_primary] if row and row["observations"] < MIN_RELIABLE_OBSERVATIONS]
    if unstable_rows or positive_symbols < len(symbols) or positive_quartiles < len(quartile_primary):
        return "MODERATE", "edge remains positive overall but shows notable sample-size or segment instability"
    return "ROBUST", "edge remains positive across most symbols and most time segments without changing entry, stop, or targets"


def _build_rows(
    symbols: list[str], observations: list[EntryTimingObservation], candles_by_symbol: dict[str, list[Candle]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    observations = sorted(observations, key=lambda row: (row.observable_entry_ts or 0, row.symbol, row.row_number))
    targets = _target_events(observations, candles_by_symbol)
    symbol_rows: list[dict[str, Any]] = []
    half_rows: list[dict[str, Any]] = []
    quartile_rows: list[dict[str, Any]] = []
    for symbol in symbols:
        scoped = sorted([row for row in observations if row.symbol == symbol], key=lambda row: row.row_number)
        scoped_targets = _filter_targets(targets, scoped)
        symbol_rows.extend(_rows_for_segment("SYMBOL", symbol, "FULL_SAMPLE", scoped, scoped_targets))
        for segment, rows in _split_halves(scoped).items():
            half_rows.extend(_rows_for_segment("HALF", symbol, segment, rows, _filter_targets(targets, rows)))
        for segment, rows in _split_quartiles(scoped).items():
            quartile_rows.extend(_rows_for_segment("QUARTILE", symbol, segment, rows, _filter_targets(targets, rows)))
    symbol_rows.extend(_rows_for_segment("SYMBOL", COMBINED, "FULL_SAMPLE", observations, targets))
    for segment, rows in _split_halves(observations).items():
        half_rows.extend(_rows_for_segment("HALF", COMBINED, segment, rows, _filter_targets(targets, rows)))
    for segment, rows in _split_quartiles(observations).items():
        quartile_rows.extend(_rows_for_segment("QUARTILE", COMBINED, segment, rows, _filter_targets(targets, rows)))
    flags = _sample_flags([*symbol_rows, *half_rows, *quartile_rows])
    flags.append({
        "scope": "PRODUCTION_ISOLATION",
        "symbol": "ALL",
        "segment": "ALL",
        "r_target": "",
        "flag": "RESEARCH_ONLY",
        "details": "audit reuses existing chronology and writes only new robustness artifacts; no strategy promotion or optimization",
    })
    return symbol_rows, half_rows, quartile_rows, flags


def run(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    output_root: Path,
    candle_symbols: dict[str, str] | None = None,
) -> None:
    symbols, observations, candles_by_symbol = _load_observations(symbol_inputs, columns_inputs, candles_inputs, candle_symbols or {})
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing next-open robustness output(s): {', '.join(existing)}")
    symbol_rows, half_rows, quartile_rows, flags = _build_rows(symbols, observations, candles_by_symbol)
    verdict, reason = _verdict(symbol_rows, half_rows, quartile_rows, symbols)
    stability = _combined_stability_rows(symbol_rows, half_rows)

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# NEXT_COLUMN_OPEN_ENTRY robustness falsification audit\n\n")
        handle.write("Research only. This audit attempts to break the surviving `NEXT_COLUMN_OPEN_ENTRY` edge by segmenting existing observations; it does not optimize entries, stops, targets, or production strategy and never outputs `PROMOTE`.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n{reason}.\n\n")
        handle.write("## Combined stability at 2R\n\n| segment | win rate delta vs full | expectancy delta vs full |\n|---|---:|---:|\n")
        for row in stability:
            handle.write(f"| {row['segment']} | {row['win_rate_delta']} | {row['expectancy_delta']} |\n")
        handle.write("\n## Symbol independence\n\n| symbol | target R | observations | target first | stop first | resolved | win rate | expected R |\n|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in symbol_rows:
            if row["symbol"] == COMBINED:
                continue
            handle.write(f"| {row['symbol']} | {row['r_target']} | {row['observations']} | {row['target_first']} | {row['stop_first']} | {row['resolved']} | {row['win_rate']} | {row['expected_R']} |\n")
        handle.write("\n## Sample sufficiency\n\nSegments below the configured sample thresholds are flagged in `robustness_flags.csv`; flagged segments should not be used to overstate robustness.\n")

    _write_csv(output_root / OUTPUT_NAMES[1], SEGMENT_FIELDS, symbol_rows)
    _write_csv(output_root / OUTPUT_NAMES[2], SEGMENT_FIELDS, half_rows)
    _write_csv(output_root / OUTPUT_NAMES[3], SEGMENT_FIELDS, quartile_rows)
    _write_csv(output_root / OUTPUT_NAMES[4], FLAG_FIELDS, flags)
    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "stage": "next_open_robustness_audit",
        "research_only": True,
        "strategy_promotion": False,
        "optimization": False,
        "entry_candidate": ENTRY_CANDIDATE,
        "stop_logic": "existing fixed 3-box stop",
        "target_logic": "existing R target levels",
        "chronology_logic": "existing NEXT_COLUMN_OPEN_ENTRY observable chronology",
        "symbols": symbols,
        "r_targets": list(R_TARGETS),
        "primary_verdict_target_r": PRIMARY_TARGET_R,
        "minimum_meaningful_observations": MIN_MEANINGFUL_OBSERVATIONS,
        "minimum_reliable_observations": MIN_RELIABLE_OBSERVATIONS,
        "verdict": verdict,
        "verdict_reason": reason,
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "combined_stability_2r": stability,
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[5]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only NEXT_COLUMN_OPEN_ENTRY robustness falsification audit")
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
