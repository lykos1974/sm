"""Research-only observable-entry audit for the fixed three-box SL-C motif."""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import (
    CLASSIFICATIONS, TimedColumn, _check_symbols, _flag, _load_columns,
    _parse_candle_symbol, _require_paths,
)
from research_v2.patterns.pole_core_motif_sl_candidates import R_TARGETS, _direction, _is_core_motif, _parse_symbol_input, _round, _to_int

ENTRY_CANDIDATES = (
    "CURRENT_CONFIRMATION_ENTRY", "REVERSAL_EXTREME_TOUCH_ENTRY",
    "REVERSAL_COLUMN_CLOSE_ENTRY", "NEXT_COLUMN_OPEN_ENTRY",
)
OUTPUT_NAMES = (
    "entry_timing_summary.md", "entry_timing_observations.csv", "entry_timing_targets.csv",
    "entry_timing_candidate_breakdown.csv", "entry_timing_symbol_breakdown.csv",
    "entry_timing_flags.csv", "entry_timing_manifest.json",
)
OBSERVATION_FIELDS = ["symbol", "row_number", "direction", "entry_candidate", "pole_column_index", "reversal_column_index", "confirmation_column_index", "box_size", "entry_location", "sl_location", "risk_boxes", "observable_entry_ts", "replay_includes_anchor_candle", "candles_in_replay", "geometry_status", "geometry_details"]
TARGET_FIELDS = ["symbol", "row_number", "direction", "entry_candidate", "r_target", "entry_location", "sl_location", "target_location", "observable_entry_ts", "first_event_ts", "classification", "details"]
METRIC_FIELDS = ["observations", "target_first", "stop_first", "same_candle_ambiguous", "unknown", "lower_bound_hit_rate", "upper_bound_hit_rate", "median_risk_boxes", "average_risk_boxes"]
CANDIDATE_FIELDS = ["entry_candidate", "r_target", *METRIC_FIELDS, "verdict", "verdict_reason"]
SYMBOL_FIELDS = ["symbol", "entry_candidate", "r_target", *METRIC_FIELDS]
FLAG_FIELDS = ["symbol", "row_number", "check_name", "result", "details"]
UNKNOWN = {"UNKNOWN_MISSING_CANDLES", "UNKNOWN_UNMAPPABLE_COLUMN_TIME", "ANCHOR_NOT_OBSERVABLE"}

@dataclass(frozen=True)
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float

@dataclass(frozen=True)
class EntryTimingObservation:
    symbol: str; row_number: int; direction: str; entry_candidate: str
    pole_idx: int; reversal_idx: int; confirmation_idx: int; box_size: float
    entry: float | None; stop: float | None; observable_entry_ts: int | None
    replay_includes_anchor: bool; candles_in_replay: int; geometry_status: str; geometry_details: str


def _candle_from_row(row: dict[str, Any]) -> Candle:
    ts = row.get("close_time") or row.get("close_ts") or row.get("timestamp") or row.get("ts")
    if ts is None:
        raise ValueError("candles CSV must expose one of: close_time, close_ts, timestamp, ts")
    return Candle(int(ts), float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]))


def _load_candles(path: Path, symbol: str) -> list[Candle]:
    if path.suffix.lower() == ".csv":
        with path.open("r", newline="") as handle:
            rows = [_candle_from_row(row) for row in csv.DictReader(handle)]
    else:
        with sqlite3.connect(str(path)) as connection:
            values = connection.execute("SELECT close_time, open, high, low, close FROM candles WHERE symbol = ? ORDER BY close_time ASC", (symbol,)).fetchall()
        rows = [Candle(int(ts), float(open_), float(high), float(low), float(close)) for ts, open_, high, low, close in values]
    return sorted(rows, key=lambda candle: candle.ts)


def _replay(candles: list[Candle], anchor: int | None, include_anchor: bool) -> list[Candle]:
    if anchor is None:
        return []
    return [c for c in candles if c.ts >= anchor] if include_anchor else [c for c in candles if c.ts > anchor]


def _candidate_observation(symbol: str, row_number: int, direction: str, candidate: str, pole: TimedColumn, reversal: TimedColumn, confirmation: TimedColumn | None, box_size: float, candles: list[Candle]) -> EntryTimingObservation:
    entry: float | None = reversal.top if direction == "LONG" else reversal.bottom
    anchor: int | None = None
    include_anchor = False
    status, details = "OBSERVABLE", ""
    if candidate == "CURRENT_CONFIRMATION_ENTRY":
        if confirmation is None:
            status, details = "ANCHOR_NOT_OBSERVABLE", "immediate next PnF column is unavailable"
        elif confirmation.start_ts is None:
            status, details = "UNKNOWN_UNMAPPABLE_COLUMN_TIME", "confirmation column start_ts is missing or invalid"
        else:
            anchor, details = confirmation.start_ts, "replay starts strictly after confirmation column start_ts"
    elif candidate == "REVERSAL_EXTREME_TOUCH_ENTRY":
        if reversal.start_ts is None or reversal.end_ts is None:
            status, details = "UNKNOWN_UNMAPPABLE_COLUMN_TIME", "reversal column time range is missing or invalid"
        else:
            touches = [c for c in candles if c.ts >= reversal.start_ts and (c.high >= reversal.top if direction == "LONG" else c.low <= reversal.bottom)]
            if not touches:
                status, details = "ANCHOR_NOT_OBSERVABLE", "no candle inside/after reversal column touches its extreme"
            else:
                anchor, details = touches[0].ts, "replay starts strictly after first reversal-extreme touch candle"
    elif candidate == "REVERSAL_COLUMN_CLOSE_ENTRY":
        if reversal.end_ts is None:
            status, details = "UNKNOWN_UNMAPPABLE_COLUMN_TIME", "reversal column end_ts is missing or invalid"
        else:
            matching = next((c for c in candles if c.ts == reversal.end_ts), None)
            if matching is None:
                status, details = "UNKNOWN_MISSING_CANDLES", "reversal end_ts candle is unavailable"
            else:
                entry, anchor, details = matching.close, matching.ts, "entry is reversal end_ts candle close; replay starts strictly after that candle"
    else:
        if confirmation is None:
            status, details = "ANCHOR_NOT_OBSERVABLE", "immediate next PnF column is unavailable"
        elif confirmation.start_ts is None:
            status, details = "UNKNOWN_UNMAPPABLE_COLUMN_TIME", "confirmation column start_ts is missing or invalid"
        else:
            matching = next((c for c in candles if c.ts > confirmation.start_ts), None)
            if matching is None:
                status, details = "UNKNOWN_MISSING_CANDLES", "first candle after confirmation start_ts is unavailable"
            else:
                entry, anchor, include_anchor, details = matching.open, matching.ts, True, "entry is first post-confirmation candle open; replay includes that candle"
    stop = None if entry is None else entry - 3 * box_size if direction == "LONG" else entry + 3 * box_size
    return EntryTimingObservation(symbol, row_number, direction, candidate, pole.idx, reversal.idx, reversal.idx + 1, box_size, entry, stop, anchor, include_anchor, len(_replay(candles, anchor, include_anchor)), status, details)


def _classify(row: EntryTimingObservation, candles: list[Candle], target_r: float) -> dict[str, Any]:
    target = None if row.entry is None else row.entry + 3 * row.box_size * target_r if row.direction == "LONG" else row.entry - 3 * row.box_size * target_r
    base = {"symbol": row.symbol, "row_number": row.row_number, "direction": row.direction, "entry_candidate": row.entry_candidate, "r_target": target_r, "entry_location": "" if row.entry is None else _round(row.entry), "sl_location": "" if row.stop is None else _round(row.stop), "target_location": "" if target is None else _round(target), "observable_entry_ts": row.observable_entry_ts or "", "first_event_ts": ""}
    if row.geometry_status != "OBSERVABLE":
        return {**base, "classification": row.geometry_status, "details": row.geometry_details}
    replay = _replay(candles, row.observable_entry_ts, row.replay_includes_anchor)
    if not candles or not replay:
        return {**base, "classification": "UNKNOWN_MISSING_CANDLES", "details": "no replay candles are available after the observable entry anchor"}
    assert row.stop is not None and target is not None
    for candle in replay:
        hit_target = candle.high >= target if row.direction == "LONG" else candle.low <= target
        hit_stop = candle.low <= row.stop if row.direction == "LONG" else candle.high >= row.stop
        if hit_target and hit_stop:
            return {**base, "first_event_ts": candle.ts, "classification": "SAME_CANDLE_AMBIGUOUS", "details": "target and stop are both inside the first event OHLC candle"}
        if hit_target:
            return {**base, "first_event_ts": candle.ts, "classification": "TARGET_FIRST", "details": "target is reached before any stop candle"}
        if hit_stop:
            return {**base, "first_event_ts": candle.ts, "classification": "STOP_FIRST", "details": "stop is reached before any target candle"}
    return {**base, "classification": "NOT_REACHED", "details": "neither target nor stop is reached by available replay candles"}


def _observation_row(row: EntryTimingObservation) -> dict[str, Any]:
    return {"symbol": row.symbol, "row_number": row.row_number, "direction": row.direction, "entry_candidate": row.entry_candidate, "pole_column_index": row.pole_idx, "reversal_column_index": row.reversal_idx, "confirmation_column_index": row.confirmation_idx, "box_size": _round(row.box_size), "entry_location": "" if row.entry is None else _round(row.entry), "sl_location": "" if row.stop is None else _round(row.stop), "risk_boxes": "" if row.stop is None else _round(abs(row.entry - row.stop) / row.box_size), "observable_entry_ts": row.observable_entry_ts or "", "replay_includes_anchor_candle": row.replay_includes_anchor, "candles_in_replay": row.candles_in_replay, "geometry_status": row.geometry_status, "geometry_details": row.geometry_details}


def _metrics(observations: list[EntryTimingObservation], targets: list[dict[str, Any]], target_r: float) -> dict[str, Any]:
    selected = [row for row in targets if row["r_target"] == target_r]
    counts = Counter(row["classification"] for row in selected)
    risks = [abs(row.entry - row.stop) / row.box_size for row in observations if row.entry is not None and row.stop is not None]
    total = len(observations)
    return {"observations": total, "target_first": counts["TARGET_FIRST"], "stop_first": counts["STOP_FIRST"], "same_candle_ambiguous": counts["SAME_CANDLE_AMBIGUOUS"], "unknown": sum(counts[name] for name in UNKNOWN), "lower_bound_hit_rate": _round(counts["TARGET_FIRST"] / total) if total else 0.0, "upper_bound_hit_rate": _round((counts["TARGET_FIRST"] + counts["SAME_CANDLE_AMBIGUOUS"]) / total) if total else 0.0, "median_risk_boxes": _round(median(risks)) if risks else "", "average_risk_boxes": _round(mean(risks)) if risks else ""}


def _verdict(metrics: dict[str, Any]) -> tuple[str, str]:
    known = metrics["observations"] - metrics["unknown"]
    if known == 0:
        return "INSUFFICIENT_DATA", "no mapped 1R observations are available"
    if metrics["target_first"] <= metrics["stop_first"] + metrics["same_candle_ambiguous"]:
        return "DISCARD_ENTRY_CANDIDATE", "1R target-first rows do not exceed stop-first plus ambiguous rows"
    return "KEEP_AS_RESEARCH_CANDIDATE", "1R target-first rows exceed stop-first plus ambiguous rows; research evidence only"

def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader(); writer.writerows(rows)


def run(symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], candles_inputs: dict[str, Path], output_root: Path, candle_symbols: dict[str, str] | None = None) -> None:
    symbols = _check_symbols(symbol_inputs, columns_inputs, candles_inputs)
    _require_paths(symbol_inputs, "symbol-input"); _require_paths(columns_inputs, "columns-input"); _require_paths(candles_inputs, "candles-input")
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing: raise FileExistsError(f"refusing to overwrite existing entry timing output(s): {', '.join(existing)}")
    candle_symbols = candle_symbols or {}; observations: list[EntryTimingObservation] = []; flags: list[dict[str, str]] = []; candles_by_symbol: dict[str, list[Candle]] = {}
    for symbol in symbols:
        columns, box_size = _load_columns(columns_inputs[symbol]); candles = _load_candles(candles_inputs[symbol], candle_symbols.get(symbol, symbol)); candles_by_symbol[symbol] = candles
        if box_size is None or box_size <= 0: raise ValueError(f"{symbol}: matching PnF columns do not expose a positive box size in profile_name")
        with symbol_inputs[symbol].open("r", newline="") as handle: rows = list(csv.DictReader(handle))
        for row_number, row in enumerate(rows, start=2):
            if not _is_core_motif(row): continue
            direction = _direction(row.get("pattern_name")); pole_idx, reversal_idx = _to_int(row.get("pole_column_index")), _to_int(row.get("reversal_column_index"))
            pole, reversal = columns.get(pole_idx) if pole_idx is not None else None, columns.get(reversal_idx) if reversal_idx is not None else None
            expected = ("O", "X") if direction == "LONG" else ("X", "O")
            if direction is None or pole is None or reversal is None or (pole.kind, reversal.kind) != expected:
                flags.append(_flag(symbol, row_number, "reconstruct_entry_timing_geometry", "EXCLUDED", "motif row lacks usable direction, columns, or expected pole/reversal kinds")); continue
            for candidate in ENTRY_CANDIDATES: observations.append(_candidate_observation(symbol, row_number, direction, candidate, pole, reversal, columns.get(reversal.idx + 1), box_size, candles))
    targets = [_classify(row, candles_by_symbol[row.symbol], r) for row in observations for r in R_TARGETS]
    candidate_rows = []
    for candidate in ENTRY_CANDIDATES:
        candidate_observations = [o for o in observations if o.entry_candidate == candidate]
        candidate_targets = [t for t in targets if t["entry_candidate"] == candidate]
        one_r_metrics = _metrics(candidate_observations, candidate_targets, 1.0)
        verdict, reason = _verdict(one_r_metrics)
        for target_r in R_TARGETS:
            candidate_rows.append({"entry_candidate": candidate, "r_target": target_r, **_metrics(candidate_observations, candidate_targets, target_r), "verdict": verdict, "verdict_reason": reason})
    symbol_rows = [{"symbol": symbol, "entry_candidate": candidate, "r_target": target_r, **_metrics([o for o in observations if o.symbol == symbol and o.entry_candidate == candidate], [t for t in targets if t["symbol"] == symbol and t["entry_candidate"] == candidate], target_r)} for symbol in symbols for candidate in ENTRY_CANDIDATES for target_r in R_TARGETS]
    flags.extend([_flag("ALL", "", "baseline_protection", "OK", "research-only audit; production strategy and existing SL-C chronology outputs remain untouched"), _flag("ALL", "", "promotion_guard", "OK", "entry timing audit never outputs PROMOTE")])
    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# Research-only SL-C entry timing audit\n\nThis is not an execution simulation or strategy promotion. It compares observable entry anchors for the fixed three-box SL-C core motif and never outputs `PROMOTE`.\n\n")
        handle.write("Lower-bound hit rate counts only 1R `TARGET_FIRST`; upper-bound hit rate additionally counts 1R `SAME_CANDLE_AMBIGUOUS`. Both use all reconstructed observations as the denominator.\n\n| entry candidate | observations | target first | stop first | ambiguous | unknown | lower hit rate | upper hit rate | verdict |\n|---|---:|---:|---:|---:|---:|---:|---:|---|\n")
        for row in candidate_rows:
            if row["r_target"] != 1.0: continue
            handle.write(f"| {row['entry_candidate']} | {row['observations']} | {row['target_first']} | {row['stop_first']} | {row['same_candle_ambiguous']} | {row['unknown']} | {row['lower_bound_hit_rate']} | {row['upper_bound_hit_rate']} | **{row['verdict']}** |\n")
    _write_csv(output_root / OUTPUT_NAMES[1], OBSERVATION_FIELDS, (_observation_row(row) for row in observations)); _write_csv(output_root / OUTPUT_NAMES[2], TARGET_FIELDS, targets); _write_csv(output_root / OUTPUT_NAMES[3], CANDIDATE_FIELDS, candidate_rows); _write_csv(output_root / OUTPUT_NAMES[4], SYMBOL_FIELDS, symbol_rows); _write_csv(output_root / OUTPUT_NAMES[5], FLAG_FIELDS, flags)
    manifest = {"created_at_utc": datetime.now(timezone.utc).isoformat(), "stage": "sl_c_entry_timing_audit", "research_only": True, "execution_simulation": False, "strategy_promotion": False, "symbols": symbols, "entry_candidates": list(ENTRY_CANDIDATES), "r_targets": list(R_TARGETS), "artifacts": list(OUTPUT_NAMES[:-1])}
    with (output_root / OUTPUT_NAMES[6]).open("x") as handle: json.dump(manifest, handle, indent=2, sort_keys=True); handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only SL-C observable entry timing audit")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV"); parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV"); parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB"); parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL"); parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    try: run(dict(args.symbol_input), dict(args.columns_input), dict(args.candles_input), args.output_root, dict(args.candle_symbol))
    except (FileExistsError, OSError, sqlite3.Error, ValueError) as exc: parser.error(str(exc))

if __name__ == "__main__": main()
