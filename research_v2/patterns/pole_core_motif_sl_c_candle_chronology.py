from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_sl_candidates import (
    R_TARGETS,
    _direction,
    _is_core_motif,
    _parse_symbol_input,
    _round,
    _to_int,
)
from research_v2.patterns.pole_outcomes import extract_box_size_from_profile_name

CLASSIFICATIONS = (
    "TARGET_FIRST",
    "STOP_FIRST",
    "SAME_CANDLE_AMBIGUOUS",
    "NOT_REACHED",
    "UNKNOWN_MISSING_CANDLES",
    "UNKNOWN_UNMAPPABLE_COLUMN_TIME",
    "ANCHOR_NOT_OBSERVABLE",
)
OUTPUT_NAMES = (
    "sl_c_candle_chronology_summary.md",
    "sl_c_candle_chronology_observations.csv",
    "sl_c_candle_chronology_symbol_breakdown.csv",
    "sl_c_candle_chronology_targets.csv",
    "sl_c_candle_chronology_flags.csv",
    "sl_c_candle_chronology_manifest.json",
)
OBSERVATION_FIELDS = [
    "symbol", "row_number", "direction", "pole_column_index", "reversal_column_index",
    "confirmation_column_index", "box_size", "entry_location", "sl_location",
    "observable_entry_ts", "candles_after_anchor", "geometry_status", "geometry_details",
]
TARGET_FIELDS = [
    "symbol", "row_number", "direction", "r_target", "entry_location", "sl_location",
    "target_location", "observable_entry_ts", "first_event_ts", "classification", "details",
]
BREAKDOWN_FIELDS = ["symbol", "r_target", "observations", *CLASSIFICATIONS]
FLAG_FIELDS = ["symbol", "row_number", "check_name", "result", "details"]


@dataclass(frozen=True)
class TimedColumn:
    idx: int
    kind: str
    top: float
    bottom: float
    start_ts: int | None
    end_ts: int | None


@dataclass(frozen=True)
class Candle:
    ts: int
    high: float
    low: float


@dataclass(frozen=True)
class ChronologyObservation:
    symbol: str
    row_number: int
    direction: str
    pole_idx: int
    reversal_idx: int
    confirmation_idx: int
    box_size: float
    entry: float
    stop: float
    observable_entry_ts: int | None
    candles_after_anchor: int
    geometry_status: str
    geometry_details: str


def _flag(symbol: str, row_number: int | str, check_name: str, result: str, details: str) -> dict[str, str]:
    return {"symbol": symbol, "row_number": str(row_number), "check_name": check_name, "result": result, "details": details}


def _optional_int(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _require_paths(inputs: dict[str, Path], label: str) -> None:
    missing = [f"{symbol}={path}" for symbol, path in inputs.items() if not path.is_file()]
    if missing:
        raise ValueError(f"missing {label} path(s): {', '.join(missing)}")


def _check_symbols(symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], candles_inputs: dict[str, Path]) -> list[str]:
    if not symbol_inputs:
        raise ValueError("at least one --symbol-input SYMBOL=CSV is required")
    symbols = sorted(symbol_inputs)
    for label, supplied in (("--columns-input", columns_inputs), ("--candles-input", candles_inputs)):
        missing = sorted(set(symbols) - set(supplied))
        extra = sorted(set(supplied) - set(symbols))
        if missing or extra:
            raise ValueError(f"{label} symbols must match --symbol-input symbols: missing={','.join(missing) or 'none'}; extra={','.join(extra) or 'none'}")
    return symbols


def _load_columns(path: Path) -> tuple[dict[int, TimedColumn], float | None]:
    columns: dict[int, TimedColumn] = {}
    box_size: float | None = None
    with path.open("r", newline="") as handle:
        for row in csv.DictReader(handle):
            if box_size is None:
                box_size = extract_box_size_from_profile_name(str(row.get("profile_name") or ""))
            column = TimedColumn(
                idx=int(row["idx"]), kind=str(row["kind"]).strip().upper(),
                top=float(row["top"]), bottom=float(row["bottom"]),
                start_ts=_optional_int(row.get("start_ts")), end_ts=_optional_int(row.get("end_ts")),
            )
            columns[column.idx] = column
    return columns, box_size


def _candle_from_row(row: dict[str, Any]) -> Candle:
    ts_value = row.get("close_time") or row.get("close_ts") or row.get("timestamp") or row.get("ts")
    if ts_value is None:
        raise ValueError("candles CSV must expose one of: close_time, close_ts, timestamp, ts")
    return Candle(ts=int(ts_value), high=float(row["high"]), low=float(row["low"]))


def _load_candles_csv(path: Path) -> list[Candle]:
    with path.open("r", newline="") as handle:
        candles = [_candle_from_row(row) for row in csv.DictReader(handle)]
    return sorted(candles, key=lambda candle: candle.ts)


def _load_candles_db(path: Path, symbol: str) -> list[Candle]:
    with sqlite3.connect(str(path)) as connection:
        rows = connection.execute(
            "SELECT close_time, high, low FROM candles WHERE symbol = ? ORDER BY close_time ASC",
            (symbol,),
        ).fetchall()
    return [Candle(ts=int(ts), high=float(high), low=float(low)) for ts, high, low in rows]


def _load_candles(path: Path, symbol: str) -> list[Candle]:
    return _load_candles_csv(path) if path.suffix.lower() == ".csv" else _load_candles_db(path, symbol)


def _load_symbol(symbol: str, labels_path: Path, columns_path: Path, candles_path: Path) -> tuple[list[ChronologyObservation], list[Candle], list[dict[str, str]]]:
    columns, box_size = _load_columns(columns_path)
    if box_size is None or box_size <= 0:
        raise ValueError(f"{symbol}: matching PnF columns do not expose a positive box size in profile_name")
    candles = _load_candles(candles_path, symbol)
    flags: list[dict[str, str]] = []
    observations: list[ChronologyObservation] = []
    with labels_path.open("r", newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row_number, row in enumerate(rows, start=2):
        if not _is_core_motif(row):
            continue
        direction = _direction(row.get("pattern_name"))
        pole_idx, reversal_idx = _to_int(row.get("pole_column_index")), _to_int(row.get("reversal_column_index"))
        if direction is None or pole_idx is None or reversal_idx is None:
            flags.append(_flag(symbol, row_number, "reconstruct_sl_c_geometry", "EXCLUDED", "motif row lacks direction or column indices"))
            continue
        pole, reversal, confirmation = columns.get(pole_idx), columns.get(reversal_idx), columns.get(reversal_idx + 1)
        expected = ("O", "X") if direction == "LONG" else ("X", "O")
        if pole is None or reversal is None:
            flags.append(_flag(symbol, row_number, "reconstruct_sl_c_geometry", "EXCLUDED", "matching setup pole or reversal column is unavailable"))
            continue
        if (pole.kind, reversal.kind) != expected:
            flags.append(_flag(symbol, row_number, "reconstruct_sl_c_geometry", "EXCLUDED", f"pole/reversal kinds are {pole.kind}/{reversal.kind}; expected {expected[0]}/{expected[1]}"))
            continue
        entry = reversal.top if direction == "LONG" else reversal.bottom
        stop = entry - 3.0 * box_size if direction == "LONG" else entry + 3.0 * box_size
        if confirmation is None:
            status, details, anchor = "ANCHOR_NOT_OBSERVABLE", "immediate next PnF column is unavailable", None
        elif confirmation.start_ts is None:
            status, details, anchor = "UNKNOWN_UNMAPPABLE_COLUMN_TIME", "confirmation column start_ts is missing or invalid", None
        else:
            status, details, anchor = "OBSERVABLE", "entry becomes observable after the candle that starts the immediate next PnF column", confirmation.start_ts
        replay_count = sum(candle.ts > anchor for candle in candles) if anchor is not None else 0
        observations.append(ChronologyObservation(
            symbol, row_number, direction, pole_idx, reversal_idx, reversal_idx + 1, box_size,
            entry, stop, anchor, replay_count, status, details,
        ))
    return observations, candles, flags


def _classify(row: ChronologyObservation, candles: list[Candle], target_r: float) -> dict[str, Any]:
    target = row.entry + 3.0 * row.box_size * target_r if row.direction == "LONG" else row.entry - 3.0 * row.box_size * target_r
    base = {
        "symbol": row.symbol, "row_number": row.row_number, "direction": row.direction,
        "r_target": target_r, "entry_location": _round(row.entry), "sl_location": _round(row.stop),
        "target_location": _round(target), "observable_entry_ts": row.observable_entry_ts or "", "first_event_ts": "",
    }
    if row.geometry_status != "OBSERVABLE":
        return {**base, "classification": row.geometry_status, "details": row.geometry_details}
    replay = [candle for candle in candles if candle.ts > row.observable_entry_ts]  # type: ignore[operator]
    if not candles or not replay:
        return {**base, "classification": "UNKNOWN_MISSING_CANDLES", "details": "no candle exists after the observable entry event"}
    for candle in replay:
        stop_hit = candle.low <= row.stop if row.direction == "LONG" else candle.high >= row.stop
        target_hit = candle.high >= target if row.direction == "LONG" else candle.low <= target
        if stop_hit and target_hit:
            return {**base, "first_event_ts": candle.ts, "classification": "SAME_CANDLE_AMBIGUOUS", "details": "stop and target are both inside the same OHLC candle; intrabar order is unknowable"}
        if target_hit:
            return {**base, "first_event_ts": candle.ts, "classification": "TARGET_FIRST", "details": "target reached in an earlier replay candle than stop"}
        if stop_hit:
            return {**base, "first_event_ts": candle.ts, "classification": "STOP_FIRST", "details": "stop reached in an earlier replay candle than target"}
    return {**base, "classification": "NOT_REACHED", "details": "neither stop nor target was reached in available post-anchor candles"}


def _observation_row(row: ChronologyObservation) -> dict[str, Any]:
    values = asdict(row)
    return {
        "symbol": values["symbol"], "row_number": values["row_number"], "direction": values["direction"],
        "pole_column_index": values["pole_idx"], "reversal_column_index": values["reversal_idx"],
        "confirmation_column_index": values["confirmation_idx"], "box_size": values["box_size"],
        "entry_location": _round(values["entry"]), "sl_location": _round(values["stop"]),
        "observable_entry_ts": values["observable_entry_ts"] or "", "candles_after_anchor": values["candles_after_anchor"],
        "geometry_status": values["geometry_status"], "geometry_details": values["geometry_details"],
    }


def _breakdown_rows(symbols: list[str], targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in [*symbols, "ALL"]:
        for target_r in R_TARGETS:
            selected = [row for row in targets if (symbol == "ALL" or row["symbol"] == symbol) and row["r_target"] == target_r]
            counts = Counter(row["classification"] for row in selected)
            rows.append({"symbol": symbol, "r_target": target_r, "observations": len(selected), **{name: counts[name] for name in CLASSIFICATIONS}})
    return rows


def _verdict(targets: list[dict[str, Any]]) -> tuple[str, str]:
    counts = Counter(row["classification"] for row in targets if row["r_target"] == 1.0)
    evidence = counts["TARGET_FIRST"] + counts["STOP_FIRST"] + counts["SAME_CANDLE_AMBIGUOUS"]
    if evidence == 0:
        return "INSUFFICIENT_DATA", "No 1R row has observable target/stop ordering evidence."
    if counts["TARGET_FIRST"] <= counts["STOP_FIRST"] + counts["SAME_CANDLE_AMBIGUOUS"]:
        return "DISCARD_SL_C_TRADABLE_INTERPRETATION", "At 1R, target-first rows do not exceed stop-first plus same-candle ambiguous rows."
    return "KEEP_AS_RESEARCH", "At 1R, target-first rows exceed stop-first plus same-candle ambiguous rows; this is research evidence only."


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_summary(path: Path, symbols: list[str], observations: list[ChronologyObservation], targets: list[dict[str, Any]], verdict: str, reason: str) -> None:
    counts = Counter(row["classification"] for row in targets if row["r_target"] == 1.0)
    with path.open("x") as handle:
        handle.write("# SL-C candle chronology research audit\n\n")
        handle.write("## Scope and chronology rule\n\n")
        handle.write("Research-only candle replay for fixed three-box SL-C geometry. The observable entry event is the candle-close timestamp that starts the immediate next PnF column after the reversal column. Replay starts strictly after that event. If stop and target are both inside one OHLC candle, the result is `SAME_CANDLE_AMBIGUOUS`; no win or loss is forced.\n\n")
        handle.write(f"Symbols supplied: {', '.join(symbols)}. Reconstructed motif rows: {len(observations)}.\n\n")
        handle.write("## 1R ordering evidence\n\n")
        handle.write("| classification | rows |\n|---|---:|\n")
        for name in CLASSIFICATIONS:
            handle.write(f"| {name} | {counts[name]} |\n")
        handle.write("\n## Final verdict\n\n")
        handle.write(f"**{verdict}** — {reason} Never `PROMOTE` from this audit.\n\n")
        handle.write("## Required experiment scorecard\n\n")
        handle.write("This candle chronology audit does not register or resolve strategy candidates. `candidate_rows_registered`, `resolved_rows`, `win_rate_non_ambiguous`, `avg_realized_r_multiple`, `total_realized_r_multiple`, and `TP1 -> TP2 conversion` are all `NOT_COMPUTED_FOR_RESEARCH_ONLY_SL_C_CANDLE_CHRONOLOGY`.\n")


def run(symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], candles_inputs: dict[str, Path], output_root: Path) -> None:
    symbols = _check_symbols(symbol_inputs, columns_inputs, candles_inputs)
    _require_paths(symbol_inputs, "symbol-input")
    _require_paths(columns_inputs, "columns-input")
    _require_paths(candles_inputs, "candles-input")
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing chronology output(s): {', '.join(existing)}")
    observations: list[ChronologyObservation] = []
    candles_by_symbol: dict[str, list[Candle]] = {}
    flags: list[dict[str, str]] = []
    for symbol in symbols:
        loaded, candles, symbol_flags = _load_symbol(symbol, symbol_inputs[symbol], columns_inputs[symbol], candles_inputs[symbol])
        observations.extend(loaded)
        candles_by_symbol[symbol] = candles
        flags.extend(symbol_flags)
    targets = [_classify(row, candles_by_symbol[row.symbol], target_r) for row in observations for target_r in R_TARGETS]
    breakdown = _breakdown_rows(symbols, targets)
    verdict, reason = _verdict(targets)
    flags.extend([
        _flag("ALL", "", "same_candle_rule", "OK", "stop and target in one OHLC candle remain SAME_CANDLE_AMBIGUOUS"),
        _flag("ALL", "", "baseline_protection", "OK", "research-only outputs; no production or existing SL-C reality-check code modified"),
        _flag("ALL", "", "final_verdict", verdict, reason),
    ])
    _write_summary(output_root / OUTPUT_NAMES[0], symbols, observations, targets, verdict, reason)
    _write_csv(output_root / OUTPUT_NAMES[1], OBSERVATION_FIELDS, (_observation_row(row) for row in observations))
    _write_csv(output_root / OUTPUT_NAMES[2], BREAKDOWN_FIELDS, breakdown)
    _write_csv(output_root / OUTPUT_NAMES[3], TARGET_FIELDS, targets)
    _write_csv(output_root / OUTPUT_NAMES[4], FLAG_FIELDS, flags)
    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(), "stage": "sl_c_candle_chronology",
        "research_only": True, "symbols": symbols, "verdict": verdict,
        "inputs": {symbol: {"symbol_input": str(symbol_inputs[symbol]), "columns_input": str(columns_inputs[symbol]), "candles_input": str(candles_inputs[symbol])} for symbol in symbols},
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[5]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only candle chronology audit for fixed three-box SL-C")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    try:
        run(dict(args.symbol_input), dict(args.columns_input), dict(args.candles_input), args.output_root)
    except (FileExistsError, OSError, sqlite3.Error, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
