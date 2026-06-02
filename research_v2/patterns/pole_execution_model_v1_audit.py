"""Research-only executable baseline audit for the PnF pole motif model v1.

Execution Model v1 is intentionally minimal-assumption:
- NEXT_COLUMN_OPEN_ENTRY
- fixed three-box stop
- fixed 2.5R target
- single entry, single stop, single target

This module does not optimize parameters or modify production strategy code. It turns the
surviving motif opportunity set into an execution-feasibility ledger with trade stats,
durations, same-symbol concurrency, cross-symbol exposure, and active 1R risk.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, _classify, _replay
from research_v2.patterns.pole_core_motif_execution_reality_audit import Opportunity, _build_opportunities, _ts_to_utc
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import ENTRY_CANDIDATE, UNKNOWN, _load_observations
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import _parse_symbol_input, _round

COMBINED = "COMBINED"
TARGET_R = 2.5
RISK_PER_TRADE_R = 1.0
ALLOWED_VERDICTS = ("EXECUTION_READY", "EXECUTION_COMPLEX", "EXECUTION_UNCERTAIN")
OUTPUT_NAMES = (
    "execution_model_v1_summary.md",
    "execution_model_v1_trade_stats.csv",
    "execution_model_v1_duration_stats.csv",
    "execution_model_v1_concurrency_stats.csv",
    "execution_model_v1_flags.csv",
    "execution_model_v1_manifest.json",
)
TRADE_STAT_FIELDS = [
    "scope", "symbol", "unique_opportunities", "trades", "wins", "losses", "ambiguous", "not_reached", "unknown",
    "win_rate", "expectancy", "total_R",
]
DURATION_FIELDS = ["scope", "symbol", "trades", "median_bars_in_trade", "mean_bars_in_trade", "p90_bars_in_trade", "max_bars_in_trade"]
CONCURRENCY_FIELDS = [
    "scope", "symbol", "maximum_simultaneous_trades", "average_open_trades", "one_position_per_symbol_feasible",
    "median_concurrent_positions", "p90_concurrent_positions", "max_concurrent_positions", "average_active_risk_R", "peak_active_risk_R",
]
FLAG_FIELDS = ["scope", "symbol", "flag", "details"]


@dataclass(frozen=True)
class ExecutedOpportunity:
    opportunity: Opportunity
    classification: str
    realized_r: float | None
    entry_ts: int | None
    exit_ts: int | None
    bars_in_trade: int | None


def _percentile_90(values: list[int | float]) -> float | str:
    if not values:
        return ""
    ordered = sorted(values)
    index = max(0, math.ceil(0.9 * len(ordered)) - 1)
    return _round(float(ordered[index]))


def _realized_r(classification: str) -> float | None:
    if classification == "TARGET_FIRST":
        return TARGET_R
    if classification == "STOP_FIRST":
        return -1.0
    return None


def _bars_until_event(row: Any, candles: list[Candle], first_event_ts: int | str | None) -> int | None:
    if first_event_ts in (None, ""):
        return None
    replay = _replay(candles, row.observable_entry_ts, row.replay_includes_anchor)
    bars = 0
    event_ts = int(first_event_ts)
    for candle in replay:
        bars += 1
        if candle.ts == event_ts:
            return bars
    return None


def _execute_opportunities(opportunities: list[Opportunity], candles_by_symbol: dict[str, list[Candle]]) -> list[ExecutedOpportunity]:
    executed: list[ExecutedOpportunity] = []
    for opp in opportunities:
        rep = opp.representative
        result = _classify(rep, candles_by_symbol[rep.symbol], TARGET_R)
        classification = result["classification"]
        first_event_ts = result.get("first_event_ts") or None
        realized_r = _realized_r(classification)
        executed.append(ExecutedOpportunity(
            opportunity=opp,
            classification=classification,
            realized_r=realized_r,
            entry_ts=rep.observable_entry_ts,
            exit_ts=int(first_event_ts) if first_event_ts not in (None, "") else None,
            bars_in_trade=_bars_until_event(rep, candles_by_symbol[rep.symbol], first_event_ts),
        ))
    return executed


def _trade_stats(scope: str, symbol: str, executed: list[ExecutedOpportunity]) -> dict[str, Any]:
    counts = Counter(row.classification for row in executed)
    wins, losses = counts["TARGET_FIRST"], counts["STOP_FIRST"]
    trades = wins + losses
    total_r = sum(row.realized_r for row in executed if row.realized_r is not None)
    return {
        "scope": scope,
        "symbol": symbol,
        "unique_opportunities": len(executed),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "ambiguous": counts["SAME_CANDLE_AMBIGUOUS"],
        "not_reached": counts["NOT_REACHED"],
        "unknown": sum(counts[name] for name in UNKNOWN),
        "win_rate": _round(wins / trades) if trades else "",
        "expectancy": _round(total_r / trades) if trades else "",
        "total_R": _round(total_r) if trades else "",
    }


def _trade_stat_rows(symbols: list[str], executed: list[ExecutedOpportunity]) -> list[dict[str, Any]]:
    rows = [_trade_stats("SYMBOL", symbol, [row for row in executed if row.opportunity.representative.symbol == symbol]) for symbol in symbols]
    rows.append(_trade_stats("COMBINED", COMBINED, executed))
    return rows


def _duration_stats(scope: str, symbol: str, executed: list[ExecutedOpportunity]) -> dict[str, Any]:
    durations = [row.bars_in_trade for row in executed if row.realized_r is not None and row.bars_in_trade is not None]
    return {
        "scope": scope,
        "symbol": symbol,
        "trades": len(durations),
        "median_bars_in_trade": _round(median(durations)) if durations else "",
        "mean_bars_in_trade": _round(mean(durations)) if durations else "",
        "p90_bars_in_trade": _percentile_90(durations),
        "max_bars_in_trade": max(durations) if durations else "",
    }


def _duration_rows(symbols: list[str], executed: list[ExecutedOpportunity]) -> list[dict[str, Any]]:
    rows = [_duration_stats("SYMBOL", symbol, [row for row in executed if row.opportunity.representative.symbol == symbol]) for symbol in symbols]
    rows.append(_duration_stats("COMBINED", COMBINED, executed))
    return rows


def _active_counts(executed: list[ExecutedOpportunity]) -> list[int]:
    intervals = [(row.entry_ts, row.exit_ts) for row in executed if row.realized_r is not None and row.entry_ts is not None and row.exit_ts is not None]
    timestamps = sorted({ts for interval in intervals for ts in interval if ts is not None})
    return [sum(1 for entry, exit_ in intervals if entry <= ts <= exit_) for ts in timestamps]


def _concurrency_rows(symbols: list[str], executed: list[ExecutedOpportunity]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        scoped = [row for row in executed if row.opportunity.representative.symbol == symbol]
        counts = _active_counts(scoped)
        maximum = max(counts) if counts else 0
        rows.append({
            "scope": "SYMBOL",
            "symbol": symbol,
            "maximum_simultaneous_trades": maximum,
            "average_open_trades": _round(mean(counts)) if counts else 0.0,
            "one_position_per_symbol_feasible": maximum <= 1,
            "median_concurrent_positions": "",
            "p90_concurrent_positions": "",
            "max_concurrent_positions": "",
            "average_active_risk_R": "",
            "peak_active_risk_R": "",
        })
    combined_counts = _active_counts(executed)
    rows.append({
        "scope": "COMBINED",
        "symbol": COMBINED,
        "maximum_simultaneous_trades": "",
        "average_open_trades": "",
        "one_position_per_symbol_feasible": "",
        "median_concurrent_positions": _round(median(combined_counts)) if combined_counts else 0.0,
        "p90_concurrent_positions": _percentile_90(combined_counts),
        "max_concurrent_positions": max(combined_counts) if combined_counts else 0,
        "average_active_risk_R": _round(mean(combined_counts) * RISK_PER_TRADE_R) if combined_counts else 0.0,
        "peak_active_risk_R": _round(max(combined_counts) * RISK_PER_TRADE_R) if combined_counts else 0.0,
    })
    return rows


def _verdict(trade_rows: list[dict[str, Any]], concurrency_rows: list[dict[str, Any]]) -> tuple[str, str]:
    combined = next(row for row in trade_rows if row["symbol"] == COMBINED)
    if combined["trades"] == 0:
        return "EXECUTION_UNCERTAIN", "no resolved fixed-target trades are available"
    if combined["ambiguous"] or combined["not_reached"] or combined["unknown"]:
        return "EXECUTION_UNCERTAIN", "some opportunities lack deterministic fixed-target outcomes"
    blocking = [row["symbol"] for row in concurrency_rows if row["scope"] == "SYMBOL" and row["maximum_simultaneous_trades"] > 1]
    if blocking:
        return "EXECUTION_COMPLEX", f"one-position-per-symbol trading is blocked by overlapping trades in: {';'.join(blocking)}"
    return "EXECUTION_READY", "fixed-entry, fixed-stop, fixed-target model is deterministic and one-position-per-symbol feasible"


def _flags(executed: list[ExecutedOpportunity], verdict: str, reason: str, concurrency_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    flags = [
        {"scope": "ALL", "symbol": COMBINED, "flag": "RESEARCH_ONLY", "details": "audit writes artifacts only; no production or strategy code is modified"},
        {"scope": "ALL", "symbol": COMBINED, "flag": "NO_OPTIMIZATION", "details": "entry, stop, and target are fixed by Execution Model v1"},
        {"scope": "ALL", "symbol": COMBINED, "flag": verdict, "details": reason},
    ]
    counts = Counter(row.classification for row in executed)
    if counts["SAME_CANDLE_AMBIGUOUS"]:
        flags.append({"scope": "ALL", "symbol": COMBINED, "flag": "AMBIGUOUS_OUTCOMES_PRESENT", "details": f"{counts['SAME_CANDLE_AMBIGUOUS']} opportunities hit stop and target in the same candle"})
    if counts["NOT_REACHED"]:
        flags.append({"scope": "ALL", "symbol": COMBINED, "flag": "OPEN_ENDED_OUTCOMES_PRESENT", "details": f"{counts['NOT_REACHED']} opportunities do not hit stop or target in available candles"})
    unknown = sum(counts[name] for name in UNKNOWN)
    if unknown:
        flags.append({"scope": "ALL", "symbol": COMBINED, "flag": "UNKNOWN_OUTCOMES_PRESENT", "details": f"{unknown} opportunities have missing/unmappable execution data"})
    for row in concurrency_rows:
        if row["scope"] == "SYMBOL" and row["maximum_simultaneous_trades"] > 1:
            flags.append({"scope": "SYMBOL", "symbol": row["symbol"], "flag": "ONE_POSITION_PER_SYMBOL_BLOCKED", "details": f"maximum simultaneous trades = {row['maximum_simultaneous_trades']}"})
    return flags


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def run(
    symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], candles_inputs: dict[str, Path], output_root: Path,
    candle_symbols: dict[str, str] | None = None,
) -> None:
    symbols, observations, candles_by_symbol = _load_observations(symbol_inputs, columns_inputs, candles_inputs, candle_symbols or {})
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing execution-model-v1 output(s): {', '.join(existing)}")

    opportunities = _build_opportunities(observations)
    executed = _execute_opportunities(opportunities, candles_by_symbol)
    trade_rows = _trade_stat_rows(symbols, executed)
    duration_rows = _duration_rows(symbols, executed)
    concurrency_rows = _concurrency_rows(symbols, executed)
    verdict, reason = _verdict(trade_rows, concurrency_rows)
    flag_rows = _flags(executed, verdict, reason, concurrency_rows)

    combined_trade = next(row for row in trade_rows if row["symbol"] == COMBINED)
    combined_duration = next(row for row in duration_rows if row["symbol"] == COMBINED)
    combined_concurrency = next(row for row in concurrency_rows if row["symbol"] == COMBINED)
    one_position = all(row["maximum_simultaneous_trades"] <= 1 for row in concurrency_rows if row["scope"] == "SYMBOL")

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# PnF pole motif Execution Model v1 audit\n\n")
        handle.write("Research only. This is the first executable fixed-target baseline: `NEXT_COLUMN_OPEN_ENTRY`, fixed three-box stop, fixed 2.5R target, one entry, one stop, one target. It does not optimize and never outputs `PROMOTE`.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n{reason}.\n\n")
        handle.write("## Actual trade statistics\n\n| metric | value |\n|---|---:|\n")
        for key in ("trades", "wins", "losses", "win_rate", "expectancy", "total_R"):
            handle.write(f"| {key} | {combined_trade[key]} |\n")
        handle.write("\n## Trade duration\n\n| metric | bars |\n|---|---:|\n")
        for key in ("median_bars_in_trade", "mean_bars_in_trade", "p90_bars_in_trade", "max_bars_in_trade"):
            handle.write(f"| {key} | {combined_duration[key]} |\n")
        handle.write("\n## Concurrent trade reality\n\n")
        handle.write(f"One position per symbol feasible: **{one_position}**. See `execution_model_v1_concurrency_stats.csv`.\n\n")
        handle.write("## Cross-symbol exposure and active risk\n\n| metric | value |\n|---|---:|\n")
        for key in ("median_concurrent_positions", "p90_concurrent_positions", "max_concurrent_positions", "average_active_risk_R", "peak_active_risk_R"):
            handle.write(f"| {key} | {combined_concurrency[key]} |\n")
        handle.write("\n## Execution feasibility\n\n")
        handle.write(f"{reason}. This baseline has no TP1, TP2, trailing stop, break-even, scaling, or pyramiding logic.\n")

    _write_csv(output_root / OUTPUT_NAMES[1], TRADE_STAT_FIELDS, trade_rows)
    _write_csv(output_root / OUTPUT_NAMES[2], DURATION_FIELDS, duration_rows)
    _write_csv(output_root / OUTPUT_NAMES[3], CONCURRENCY_FIELDS, concurrency_rows)
    _write_csv(output_root / OUTPUT_NAMES[4], FLAG_FIELDS, flag_rows)
    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_execution_model_v1_audit",
        "research_only": True,
        "strategy_promotion": False,
        "production_modifications": False,
        "optimization_performed": False,
        "entry": ENTRY_CANDIDATE,
        "stop": "fixed_3_box_stop",
        "target_R": TARGET_R,
        "management_rules": {"tp1": False, "tp2": False, "trailing_stop": False, "break_even": False, "scaling": False, "pyramiding": False},
        "symbols": symbols,
        "unique_opportunities": len(opportunities),
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "verdict": verdict,
        "verdict_reason": reason,
        "one_position_per_symbol_feasible": one_position,
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[5]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only PnF pole Execution Model v1 audit")
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
