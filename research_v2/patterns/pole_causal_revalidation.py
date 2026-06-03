"""Research-only causal revalidation for the PnF pole core motif.

This module keeps the fixed core motif definition unchanged:
``opposing_pole_distance_columns == 3`` and
``enhanced_by_opposing_pole == False``.  It changes only the research entry
anchor used for revalidation: the motif is treated as born at the later
opposing-pole discovery column (first pole index + distance + 1), and
``NEXT_COLUMN_OPEN_ENTRY`` is measured after that true birth column.

No production strategy, detector, database schema, or live execution code is
modified by this module.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from research_v2.patterns.pole_be_research_audit import _be_classify
from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, EntryTimingObservation, _classify
from research_v2.patterns.pole_core_motif_execution_reality_audit import _build_opportunities
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import ENTRY_CANDIDATE, UNKNOWN, _load_candles
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _check_symbols, _flag, _load_columns, _parse_candle_symbol, _require_paths
from research_v2.patterns.pole_core_motif_sl_candidates import _direction, _is_core_motif, _parse_symbol_input, _round, _to_int

TARGET_R = 2.5
BREAK_EVEN_TRIGGER_R = 2.0
CORE_DISTANCE = 3
OUTPUT_NAMES = (
    "causal_revalidation_summary.md",
    "causal_revalidation_observations.csv",
    "causal_revalidation_expectancy.csv",
    "causal_revalidation_execution_model.csv",
    "causal_revalidation_break_even.csv",
    "causal_revalidation_flags.csv",
    "causal_revalidation_manifest.json",
)
OBSERVATION_FIELDS = [
    "symbol", "row_number", "direction", "entry_candidate", "pole_column_index", "reversal_column_index",
    "legacy_confirmation_column_index", "motif_birth_column_index", "opposing_pole_column_index", "box_size",
    "entry_price", "stop_price", "observable_entry_ts", "geometry_status", "geometry_details",
]
EXPECTANCY_FIELDS = ["scope", "target_R", "observations", "target_first", "stop_first", "ambiguous", "not_reached", "unknown", "resolved", "win_rate", "expected_R"]
EXECUTION_FIELDS = ["scope", "unique_opportunities", "trades", "wins", "losses", "ambiguous", "not_reached", "unknown", "win_rate", "expectancy", "total_R"]
BE_FIELDS = ["variant", "be_trigger_R", "trades", "wins", "losses", "break_even_exits", "ambiguous", "not_reached", "unknown", "win_rate", "expectancy", "total_R"]
FLAG_FIELDS = ["symbol", "row_number", "check_name", "result", "details"]


@dataclass(frozen=True)
class CausalObservation:
    observation: EntryTimingObservation
    legacy_confirmation_idx: int
    motif_birth_idx: int
    opposing_pole_idx: int


def _pct(numerator: int | float, denominator: int | float) -> float | str:
    return _round(float(numerator) / float(denominator)) if denominator else ""


def _entry_after_birth(
    *,
    symbol: str,
    row_number: int,
    direction: str,
    pole_idx: int,
    reversal_idx: int,
    legacy_confirmation_idx: int,
    opposing_pole_idx: int,
    birth: Any | None,
    box_size: float,
    candles: list[Candle],
) -> CausalObservation:
    status = "OBSERVABLE"
    details = "entry is first post-birth-column candle open; replay includes that candle"
    entry = stop = None
    anchor = None
    include_anchor = False
    motif_birth_idx = opposing_pole_idx + 1

    if birth is None:
        status = "ANCHOR_NOT_OBSERVABLE"
        details = "opposing-pole discovery/birth column is unavailable"
    elif getattr(birth, "start_ts", None) is None:
        status = "UNKNOWN_UNMAPPABLE_COLUMN_TIME"
        details = "opposing-pole discovery/birth column start_ts is missing or invalid"
    else:
        matching = next((c for c in candles if c.ts > int(getattr(birth, "start_ts"))), None)
        if matching is None:
            status = "UNKNOWN_MISSING_CANDLES"
            details = "first candle after opposing-pole discovery/birth column start_ts is unavailable"
        else:
            entry = matching.open
            anchor = matching.ts
            include_anchor = True
            stop = entry - 3 * box_size if direction == "LONG" else entry + 3 * box_size

    obs = EntryTimingObservation(
        symbol=symbol,
        row_number=row_number,
        direction=direction,
        entry_candidate=ENTRY_CANDIDATE,
        pole_idx=pole_idx,
        reversal_idx=reversal_idx,
        confirmation_idx=motif_birth_idx,
        box_size=box_size,
        entry=entry,
        stop=stop,
        observable_entry_ts=anchor,
        replay_includes_anchor=include_anchor,
        candles_in_replay=sum(1 for c in candles if anchor is not None and (c.ts >= anchor if include_anchor else c.ts > anchor)),
        geometry_status=status,
        geometry_details=details,
    )
    return CausalObservation(obs, legacy_confirmation_idx, motif_birth_idx, opposing_pole_idx)


def load_causal_observations(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    candle_symbols: dict[str, str],
    *,
    limit_rows_per_symbol: int | None = None,
) -> tuple[list[str], list[CausalObservation], dict[str, list[Candle]], list[dict[str, str]]]:
    """Load core motif observations with causal P+4 birth-column entries."""
    symbols = _check_symbols(symbol_inputs, columns_inputs, candles_inputs)
    _require_paths(symbol_inputs, "symbol-input")
    _require_paths(columns_inputs, "columns-input")
    _require_paths(candles_inputs, "candles-input")
    observations: list[CausalObservation] = []
    candles_by_symbol: dict[str, list[Candle]] = {}
    flags: list[dict[str, str]] = []

    for symbol in symbols:
        columns, box_size = _load_columns(columns_inputs[symbol])
        if box_size is None or box_size <= 0:
            raise ValueError(f"{symbol}: matching PnF columns do not expose a positive box size in profile_name")
        candles = _load_candles(candles_inputs[symbol], candle_symbols.get(symbol, symbol))
        candles_by_symbol[symbol] = candles
        loaded_for_symbol = 0
        with symbol_inputs[symbol].open("r", newline="") as handle:
            rows = list(csv.DictReader(handle))
        for row_number, row in enumerate(rows, start=2):
            if limit_rows_per_symbol is not None and loaded_for_symbol >= limit_rows_per_symbol:
                break
            if not _is_core_motif(row):
                continue
            if _to_int(row.get("opposing_pole_distance_columns")) != CORE_DISTANCE:
                continue
            direction = _direction(row.get("pattern_name"))
            pole_idx = _to_int(row.get("pole_column_index"))
            reversal_idx = _to_int(row.get("reversal_column_index"))
            if direction is None or pole_idx is None or reversal_idx is None:
                flags.append(_flag(symbol, row_number, "causal_geometry", "EXCLUDED", "core row lacks direction, pole index, or reversal index"))
                continue
            pole = columns.get(pole_idx)
            reversal = columns.get(reversal_idx)
            expected = ("O", "X") if direction == "LONG" else ("X", "O")
            if pole is None or reversal is None or (pole.kind, reversal.kind) != expected:
                flags.append(_flag(symbol, row_number, "causal_geometry", "EXCLUDED", "core row lacks usable pole/reversal columns or expected kinds"))
                continue
            opposing_pole_idx = pole_idx + CORE_DISTANCE
            motif_birth_idx = opposing_pole_idx + 1
            birth = columns.get(motif_birth_idx)
            observations.append(
                _entry_after_birth(
                    symbol=symbol,
                    row_number=row_number,
                    direction=direction,
                    pole_idx=pole_idx,
                    reversal_idx=reversal_idx,
                    legacy_confirmation_idx=reversal_idx + 1,
                    opposing_pole_idx=opposing_pole_idx,
                    birth=birth,
                    box_size=box_size,
                    candles=candles,
                )
            )
            loaded_for_symbol += 1
    return symbols, observations, candles_by_symbol, flags


def _expectancy_rows(observations: list[CausalObservation], candles_by_symbol: dict[str, list[Candle]]) -> list[dict[str, Any]]:
    target_rows = [_classify(row.observation, candles_by_symbol[row.observation.symbol], TARGET_R) for row in observations]
    counts = Counter(row["classification"] for row in target_rows)
    wins, losses = counts["TARGET_FIRST"], counts["STOP_FIRST"]
    resolved = wins + losses
    return [{
        "scope": "ALL",
        "target_R": TARGET_R,
        "observations": len(observations),
        "target_first": wins,
        "stop_first": losses,
        "ambiguous": counts["SAME_CANDLE_AMBIGUOUS"],
        "not_reached": counts["NOT_REACHED"],
        "unknown": sum(counts[name] for name in UNKNOWN),
        "resolved": resolved,
        "win_rate": _pct(wins, resolved),
        "expected_R": _round((wins * TARGET_R - losses) / resolved) if resolved else "",
    }]


def _execution_rows(observations: list[CausalObservation], candles_by_symbol: dict[str, list[Candle]]) -> list[dict[str, Any]]:
    opportunities = _build_opportunities([row.observation for row in observations])
    classifications = [_classify(opp.representative, candles_by_symbol[opp.representative.symbol], TARGET_R)["classification"] for opp in opportunities]
    counts = Counter(classifications)
    wins, losses = counts["TARGET_FIRST"], counts["STOP_FIRST"]
    trades = wins + losses
    total_r = wins * TARGET_R - losses
    return [{
        "scope": "ALL",
        "unique_opportunities": len(opportunities),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "ambiguous": counts["SAME_CANDLE_AMBIGUOUS"],
        "not_reached": counts["NOT_REACHED"],
        "unknown": sum(counts[name] for name in UNKNOWN),
        "win_rate": _pct(wins, trades),
        "expectancy": _round(total_r / trades) if trades else "",
        "total_R": _round(total_r),
    }]


def _be_rows(observations: list[CausalObservation], candles_by_symbol: dict[str, list[Candle]]) -> list[dict[str, Any]]:
    opportunities = _build_opportunities([row.observation for row in observations])
    outcomes = [_be_classify(opp.representative, candles_by_symbol[opp.representative.symbol], BREAK_EVEN_TRIGGER_R) for opp in opportunities]
    counts = Counter(classification for classification, _realized_r, _ts, _details in outcomes)
    wins, losses, be_exits = counts["TARGET_FIRST"], counts["STOP_FIRST"], counts["BREAK_EVEN_EXIT"]
    trades = wins + losses + be_exits
    total_r = sum(realized_r for _classification, realized_r, _ts, _details in outcomes if realized_r is not None)
    return [{
        "variant": "BREAK_EVEN_AFTER_2R",
        "be_trigger_R": BREAK_EVEN_TRIGGER_R,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "break_even_exits": be_exits,
        "ambiguous": counts["SAME_CANDLE_AMBIGUOUS"],
        "not_reached": counts["NOT_REACHED"],
        "unknown": sum(counts[name] for name in UNKNOWN),
        "win_rate": _pct(wins, trades),
        "expectancy": _round(total_r / trades) if trades else "",
        "total_R": _round(total_r),
    }]


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _observation_row(row: CausalObservation) -> dict[str, Any]:
    obs = row.observation
    return {
        "symbol": obs.symbol,
        "row_number": obs.row_number,
        "direction": obs.direction,
        "entry_candidate": obs.entry_candidate,
        "pole_column_index": obs.pole_idx,
        "reversal_column_index": obs.reversal_idx,
        "legacy_confirmation_column_index": row.legacy_confirmation_idx,
        "motif_birth_column_index": row.motif_birth_idx,
        "opposing_pole_column_index": row.opposing_pole_idx,
        "box_size": _round(obs.box_size),
        "entry_price": "" if obs.entry is None else _round(obs.entry),
        "stop_price": "" if obs.stop is None else _round(obs.stop),
        "observable_entry_ts": obs.observable_entry_ts or "",
        "geometry_status": obs.geometry_status,
        "geometry_details": obs.geometry_details,
    }


def run(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    output_root: Path,
    candle_symbols: dict[str, str] | None = None,
    *,
    limit_rows_per_symbol: int | None = None,
) -> dict[str, Any]:
    symbols, observations, candles_by_symbol, flags = load_causal_observations(
        symbol_inputs,
        columns_inputs,
        candles_inputs,
        candle_symbols or {},
        limit_rows_per_symbol=limit_rows_per_symbol,
    )
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing causal revalidation output(s): {', '.join(existing)}")

    expectancy = _expectancy_rows(observations, candles_by_symbol)
    execution = _execution_rows(observations, candles_by_symbol)
    break_even = _be_rows(observations, candles_by_symbol)
    verdict = "EDGE_SURVIVES_CAUSAL_SAMPLE" if break_even and float(break_even[0]["expectancy"] or 0) > 0 else "EDGE_DOES_NOT_SURVIVE_CAUSAL_SAMPLE"
    if int(break_even[0]["trades"] if break_even else 0) == 0:
        verdict = "INSUFFICIENT_CAUSAL_SAMPLE"

    _write_csv(output_root / OUTPUT_NAMES[1], OBSERVATION_FIELDS, [_observation_row(row) for row in observations])
    _write_csv(output_root / OUTPUT_NAMES[2], EXPECTANCY_FIELDS, expectancy)
    _write_csv(output_root / OUTPUT_NAMES[3], EXECUTION_FIELDS, execution)
    _write_csv(output_root / OUTPUT_NAMES[4], BE_FIELDS, break_even)
    _write_csv(output_root / OUTPUT_NAMES[5], FLAG_FIELDS, flags)

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# PnF pole causal revalidation\n\n")
        handle.write("Research only. No strategy, detector, execution, or production code is modified.\n\n")
        handle.write("## Causal definition\n\n")
        handle.write("- Core motif unchanged: `opposing_pole_distance_columns == 3` and `enhanced_by_opposing_pole == False`.\n")
        handle.write("- Motif birth: opposing-pole discovery at `first_pole_index + 4`.\n")
        handle.write("- Entry: `NEXT_COLUMN_OPEN_ENTRY` after the true birth column.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n")
        for title, rows, keys in (
            ("Expectancy", expectancy, ("observations", "resolved", "win_rate", "expected_R")),
            ("Execution model", execution, ("unique_opportunities", "trades", "win_rate", "expectancy", "total_R")),
            ("Break-even after +2R", break_even, ("trades", "wins", "losses", "break_even_exits", "expectancy", "total_R")),
        ):
            handle.write(f"## {title}\n\n")
            row = rows[0] if rows else {}
            for key in keys:
                handle.write(f"- `{key}`: {row.get(key, '')}\n")
            handle.write("\n")

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_causal_revalidation",
        "research_only": True,
        "production_modifications": False,
        "strategy_promotion": False,
        "core_motif": {"opposing_pole_distance_columns": CORE_DISTANCE, "enhanced_by_opposing_pole": False},
        "motif_birth": "opposing_pole_discovery:first_pole_index+4",
        "entry": ENTRY_CANDIDATE,
        "target_R": TARGET_R,
        "break_even_after_R": BREAK_EVEN_TRIGGER_R,
        "symbols": symbols,
        "limit_rows_per_symbol": limit_rows_per_symbol,
        "causal_observations": len(observations),
        "verdict": verdict,
        "expectancy": expectancy[0] if expectancy else {},
        "execution_model": execution[0] if execution else {},
        "break_even": break_even[0] if break_even else {},
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[6]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only causal revalidation for the fixed PnF pole core motif")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--limit-rows-per-symbol", type=int, help="optional small-sample diagnostic cap after core motif filtering")
    args = parser.parse_args()
    try:
        run(
            dict(args.symbol_input),
            dict(args.columns_input),
            dict(args.candles_input),
            args.output_root,
            dict(args.candle_symbol),
            limit_rows_per_symbol=args.limit_rows_per_symbol,
        )
    except (FileExistsError, OSError, sqlite3.Error, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
