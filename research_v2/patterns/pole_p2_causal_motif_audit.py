"""Research-only P+2 causal pole/reversal motif audit.

This module intentionally ignores the later opposing-pole motif fields and tests
whether the earlier, fully observable pole -> reversal -> confirmation structure
has standalone expectancy when traded at P+2.

No production strategy, detector, database schema, or live execution code is
modified by this module.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_be_research_audit import _be_classify
from research_v2.patterns.pole_core_motif_entry_timing_audit import (
    Candle,
    EntryTimingObservation,
    _candidate_observation,
    _classify,
)
from research_v2.patterns.pole_core_motif_execution_reality_audit import _build_opportunities
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import ENTRY_CANDIDATE, UNKNOWN, _load_candles
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import (
    _check_symbols,
    _flag,
    _load_columns,
    _parse_candle_symbol,
    _require_paths,
)
from research_v2.patterns.pole_core_motif_sl_candidates import _direction, _parse_symbol_input, _round, _to_int

TARGET_R = 2.5
BREAK_EVEN_TRIGGER_R = 2.0
EXPECTED_SYMBOLS = ("BTC", "ETH", "SOL", "ENA", "HYPE", "SUI", "TAO")
NON_CAUSAL_TRADES = 460
NON_CAUSAL_EXPECTANCY_R = 1.654
CAUSAL_P4_EXPECTANCY_R = -0.714
IGNORED_FIELDS = ("opposing_pole_distance_columns", "enhanced_by_opposing_pole")
MOTIF_NAME = "CAUSAL_P2_POLE_REVERSAL_CONFIRMATION"
OUTPUT_NAMES = (
    "p2_causal_motif_summary.md",
    "p2_causal_motif_observations.csv",
    "p2_causal_motif_expectancy.csv",
    "p2_causal_motif_execution_model.csv",
    "p2_causal_motif_break_even.csv",
    "p2_causal_motif_comparison.csv",
    "p2_causal_motif_flags.csv",
    "p2_causal_motif_manifest.json",
)
OBSERVATION_FIELDS = [
    "symbol", "row_number", "direction", "motif_name", "entry_candidate", "pole_column_index",
    "reversal_column_index", "confirmation_column_index", "box_size", "entry_price", "stop_price",
    "observable_entry_ts", "geometry_status", "geometry_details",
]
EXPECTANCY_FIELDS = [
    "scope", "target_R", "observations", "target_first", "stop_first", "ambiguous", "not_reached",
    "unknown", "resolved", "win_rate", "expected_R",
]
EXECUTION_FIELDS = [
    "scope", "unique_opportunities", "trades", "wins", "losses", "ambiguous", "not_reached",
    "unknown", "win_rate", "expectancy", "total_R",
]
BE_FIELDS = [
    "variant", "be_trigger_R", "trades", "wins", "losses", "break_even_exits", "ambiguous",
    "not_reached", "unknown", "win_rate", "expectancy", "total_R",
]
COMPARISON_FIELDS = ["baseline", "trade_count", "expectancy", "delta_vs_p2_expectancy"]
FLAG_FIELDS = ["symbol", "row_number", "check_name", "result", "details"]


def _pct(numerator: int | float, denominator: int | float) -> float | str:
    return _round(float(numerator) / float(denominator)) if denominator else ""


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _validate_full_universe(symbols: list[str], require_full_universe: bool) -> None:
    if not require_full_universe:
        return
    expected = set(EXPECTED_SYMBOLS)
    loaded = set(symbols)
    missing = [symbol for symbol in EXPECTED_SYMBOLS if symbol not in loaded]
    unknown = sorted(loaded - expected)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing required seven-market symbols: {', '.join(missing)}")
        if unknown:
            details.append(f"unexpected symbols outside seven-market universe: {', '.join(unknown)}")
        raise ValueError("; ".join(details))


def _is_early_p2_candidate(row: dict[str, Any]) -> bool:
    """Accept pole rows without consulting later opposing-pole fields."""
    return _direction(row.get("pattern_name")) is not None and _to_int(row.get("pole_column_index")) is not None and _to_int(row.get("reversal_column_index")) is not None


def load_p2_observations(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    candle_symbols: dict[str, str],
    *,
    limit_rows_per_symbol: int | None = None,
) -> tuple[list[str], list[EntryTimingObservation], dict[str, list[Candle]], list[dict[str, str]]]:
    """Load pole/reversal/confirmation observations knowable at P+2.

    The later fields ``opposing_pole_distance_columns`` and
    ``enhanced_by_opposing_pole`` are deliberately not read when selecting rows.
    """
    symbols = _check_symbols(symbol_inputs, columns_inputs, candles_inputs)
    _require_paths(symbol_inputs, "symbol-input")
    _require_paths(columns_inputs, "columns-input")
    _require_paths(candles_inputs, "candles-input")
    observations: list[EntryTimingObservation] = []
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
            if not _is_early_p2_candidate(row):
                continue
            direction = _direction(row.get("pattern_name"))
            pole_idx = _to_int(row.get("pole_column_index"))
            reversal_idx = _to_int(row.get("reversal_column_index"))
            assert direction is not None and pole_idx is not None and reversal_idx is not None
            pole = columns.get(pole_idx)
            reversal = columns.get(reversal_idx)
            confirmation = columns.get(reversal_idx + 1)
            expected = ("O", "X") if direction == "LONG" else ("X", "O")
            if pole is None or reversal is None or (pole.kind, reversal.kind) != expected:
                flags.append(_flag(symbol, row_number, "p2_geometry", "EXCLUDED", "row lacks usable pole/reversal columns or expected kinds"))
                continue
            observations.append(_candidate_observation(symbol, row_number, direction, ENTRY_CANDIDATE, pole, reversal, confirmation, box_size, candles))
            loaded_for_symbol += 1
    return symbols, observations, candles_by_symbol, flags


def _target_expectancy_rows(observations: list[EntryTimingObservation], candles_by_symbol: dict[str, list[Candle]]) -> list[dict[str, Any]]:
    target_rows = [_classify(row, candles_by_symbol[row.symbol], TARGET_R) for row in observations]
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


def _execution_rows(observations: list[EntryTimingObservation], candles_by_symbol: dict[str, list[Candle]]) -> list[dict[str, Any]]:
    opportunities = _build_opportunities(observations)
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


def _break_even_rows(observations: list[EntryTimingObservation], candles_by_symbol: dict[str, list[Candle]]) -> list[dict[str, Any]]:
    opportunities = _build_opportunities(observations)
    outcomes = [_be_classify(opp.representative, candles_by_symbol[opp.representative.symbol], BREAK_EVEN_TRIGGER_R) for opp in opportunities]
    counts = Counter(classification for classification, _realized_r, _ts, _details in outcomes)
    wins = counts["TARGET_FIRST"]
    losses = counts["STOP_FIRST"]
    be_exits = counts["BREAK_EVEN_EXIT"]
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
        "win_rate": _pct(wins, wins + losses),
        "expectancy": _round(total_r / trades) if trades else "",
        "total_R": _round(total_r),
    }]


def _comparison_rows(p2_break_even: dict[str, Any]) -> list[dict[str, Any]]:
    p2_expectancy = p2_break_even.get("expectancy", "")
    delta_non_causal = "" if p2_expectancy == "" else _round(NON_CAUSAL_EXPECTANCY_R - float(p2_expectancy))
    delta_p4 = "" if p2_expectancy == "" else _round(CAUSAL_P4_EXPECTANCY_R - float(p2_expectancy))
    return [
        {
            "baseline": "historical_non_causal_core_motif",
            "trade_count": NON_CAUSAL_TRADES,
            "expectancy": NON_CAUSAL_EXPECTANCY_R,
            "delta_vs_p2_expectancy": delta_non_causal,
        },
        {
            "baseline": "causal_P4_true_birth_revalidation",
            "trade_count": "",
            "expectancy": CAUSAL_P4_EXPECTANCY_R,
            "delta_vs_p2_expectancy": delta_p4,
        },
    ]


def _observation_row(row: EntryTimingObservation) -> dict[str, Any]:
    return {
        "symbol": row.symbol,
        "row_number": row.row_number,
        "direction": row.direction,
        "motif_name": MOTIF_NAME,
        "entry_candidate": row.entry_candidate,
        "pole_column_index": row.pole_idx,
        "reversal_column_index": row.reversal_idx,
        "confirmation_column_index": row.confirmation_idx,
        "box_size": _round(row.box_size),
        "entry_price": "" if row.entry is None else _round(row.entry),
        "stop_price": "" if row.stop is None else _round(row.stop),
        "observable_entry_ts": row.observable_entry_ts or "",
        "geometry_status": row.geometry_status,
        "geometry_details": row.geometry_details,
    }


def _verdict(break_even: dict[str, Any]) -> str:
    trades = int(break_even.get("trades") or 0)
    expectancy = break_even.get("expectancy", "")
    if trades == 0 or expectancy == "":
        return "INSUFFICIENT_CAUSAL_P2_SAMPLE"
    if float(expectancy) <= 0:
        return "DISCARD_P2_MOTIF"
    if float(expectancy) < NON_CAUSAL_EXPECTANCY_R:
        return "P2_EDGE_WEAKER_THAN_NON_CAUSAL_CORE"
    return "P2_EDGE_MATCHES_OR_EXCEEDS_NON_CAUSAL_CORE"


def run(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    output_root: Path,
    candle_symbols: dict[str, str] | None = None,
    *,
    limit_rows_per_symbol: int | None = None,
    require_full_universe: bool = True,
) -> dict[str, Any]:
    symbols, observations, candles_by_symbol, flags = load_p2_observations(
        symbol_inputs,
        columns_inputs,
        candles_inputs,
        candle_symbols or {},
        limit_rows_per_symbol=limit_rows_per_symbol,
    )
    _validate_full_universe(symbols, require_full_universe)
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing P+2 causal motif output(s): {', '.join(existing)}")

    expectancy = _target_expectancy_rows(observations, candles_by_symbol)
    execution = _execution_rows(observations, candles_by_symbol)
    break_even = _break_even_rows(observations, candles_by_symbol)
    comparison = _comparison_rows(break_even[0])
    verdict = _verdict(break_even[0])

    _write_csv(output_root / OUTPUT_NAMES[1], OBSERVATION_FIELDS, (_observation_row(row) for row in observations))
    _write_csv(output_root / OUTPUT_NAMES[2], EXPECTANCY_FIELDS, expectancy)
    _write_csv(output_root / OUTPUT_NAMES[3], EXECUTION_FIELDS, execution)
    _write_csv(output_root / OUTPUT_NAMES[4], BE_FIELDS, break_even)
    _write_csv(output_root / OUTPUT_NAMES[5], COMPARISON_FIELDS, comparison)
    _write_csv(output_root / OUTPUT_NAMES[6], FLAG_FIELDS, flags)

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# P+2 causal pole/reversal motif audit\n\n")
        handle.write("Research only. No strategy, detector, execution, production, or live-trader code is modified.\n\n")
        handle.write("## Causal motif definition\n\n")
        handle.write(f"- Motif: `{MOTIF_NAME}`.\n")
        handle.write("- Birth/knowledge time: P+2, after `pole -> reversal -> confirmation` exists.\n")
        handle.write("- Ignored fields: `opposing_pole_distance_columns`, `enhanced_by_opposing_pole`.\n")
        handle.write("- Entry: `NEXT_COLUMN_OPEN_ENTRY` after confirmation column start.\n")
        handle.write("- Execution comparison metric: fixed 2.5R target, fixed three-box stop, break-even after +2R.\n")
        handle.write(f"- Required seven-market symbol set: `{', '.join(EXPECTED_SYMBOLS)}`.\n")
        handle.write(f"- Full seven-market universe loaded: `{set(symbols) == set(EXPECTED_SYMBOLS)}`.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n")
        for title, rows, keys in (
            ("Raw 2.5R target expectancy", expectancy, ("observations", "resolved", "win_rate", "expected_R")),
            ("Deduplicated execution model", execution, ("unique_opportunities", "trades", "win_rate", "expectancy", "total_R")),
            ("Break-even after +2R", break_even, ("trades", "wins", "losses", "break_even_exits", "win_rate", "expectancy", "total_R")),
        ):
            handle.write(f"## {title}\n\n")
            row = rows[0] if rows else {}
            for key in keys:
                handle.write(f"- `{key}`: {row.get(key, '')}\n")
            handle.write("\n")
        handle.write("## Comparison vs historical non-causal core motif\n\n")
        handle.write(f"- Historical non-causal core motif: `{NON_CAUSAL_TRADES}` trades, `{NON_CAUSAL_EXPECTANCY_R}` R/trade.\n")
        handle.write(f"- Causal P+4 true-birth revalidation expectancy: `{CAUSAL_P4_EXPECTANCY_R}` R/trade.\n")
        handle.write(f"- Causal P+2 motif expectancy: `{break_even[0].get('expectancy', '')}` R/trade.\n")
        handle.write(f"- Non-causal minus P+2 expectancy delta: `{comparison[0]['delta_vs_p2_expectancy']}` R/trade.\n")
        handle.write(f"- P+4 minus P+2 expectancy delta: `{comparison[1]['delta_vs_p2_expectancy']}` R/trade.\n")

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_p2_causal_motif_audit",
        "research_only": True,
        "production_modifications": False,
        "live_trader_modifications": False,
        "strategy_promotion": False,
        "motif": MOTIF_NAME,
        "knowable_at": "P+2:pole->reversal->confirmation",
        "ignored_fields": list(IGNORED_FIELDS),
        "entry": ENTRY_CANDIDATE,
        "target_R": TARGET_R,
        "break_even_after_R": BREAK_EVEN_TRIGGER_R,
        "required_symbols": list(EXPECTED_SYMBOLS),
        "full_seven_market_universe": set(symbols) == set(EXPECTED_SYMBOLS),
        "non_causal_core": {"trades": NON_CAUSAL_TRADES, "expectancy_R": NON_CAUSAL_EXPECTANCY_R},
        "causal_P4_true_birth": {"expectancy_R": CAUSAL_P4_EXPECTANCY_R},
        "symbols": symbols,
        "limit_rows_per_symbol": limit_rows_per_symbol,
        "observations": len(observations),
        "verdict": verdict,
        "raw_target_expectancy": expectancy[0] if expectancy else {},
        "execution_model": execution[0] if execution else {},
        "break_even": break_even[0] if break_even else {},
        "comparison": comparison,
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[7]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only causal P+2 pole/reversal motif audit")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--limit-rows-per-symbol", type=int, help="optional small-sample diagnostic cap after P+2 motif filtering")
    parser.add_argument("--allow-partial-universe", action="store_true", help="allow test/diagnostic runs that are not the full BTC/ETH/SOL/ENA/HYPE/SUI/TAO seven-market universe")
    args = parser.parse_args()
    try:
        run(
            dict(args.symbol_input),
            dict(args.columns_input),
            dict(args.candles_input),
            args.output_root,
            dict(args.candle_symbol),
            limit_rows_per_symbol=args.limit_rows_per_symbol,
            require_full_universe=not args.allow_partial_universe,
        )
    except (FileExistsError, OSError, sqlite3.Error, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
