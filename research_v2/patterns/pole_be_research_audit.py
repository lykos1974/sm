"""Research-only break-even management audit for PnF pole Execution Model v1.

This module compares exactly one management idea against the already-established
Execution Model v1 baseline:
- NEXT_COLUMN_OPEN_ENTRY
- fixed three-box stop
- fixed 2.5R target
- no TP1/TP2/trailing/scaling/pyramiding

It does not modify production strategy code, optimize entries/stops/targets, or output
PROMOTE. Break-even variants only move the stop to the entry price after a fixed favorable
R threshold has already been reached.
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
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, _replay
from research_v2.patterns.pole_core_motif_execution_reality_audit import Opportunity, _build_opportunities
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import ENTRY_CANDIDATE, UNKNOWN, _load_observations
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import _parse_symbol_input, _round

COMBINED = "COMBINED"
TARGET_R = 2.5
BASELINE_VARIANT = "A_NO_BREAK_EVEN"
BE_VARIANTS: tuple[tuple[str, float | None], ...] = (
    (BASELINE_VARIANT, None),
    ("B_BE_AFTER_1R", 1.0),
    ("C_BE_AFTER_1_5R", 1.5),
    ("D_BE_AFTER_2R", 2.0),
)
ALLOWED_VERDICTS = ("KEEP_BASELINE", "BE_IMPROVES", "BE_NEUTRAL", "BE_HURTS")
OUTPUT_NAMES = (
    "be_research_summary.md",
    "be_research_variant_breakdown.csv",
    "be_research_flags.csv",
    "be_research_manifest.json",
)
VARIANT_FIELDS = [
    "variant", "be_trigger_R", "trades", "wins", "losses", "break_even_exits", "ambiguous", "not_reached", "unknown",
    "win_rate", "loss_rate", "BE_rate", "expectancy", "total_R", "expectancy_delta_vs_baseline",
    "drawdown_proxy_reduction", "loss_reduction", "win_destruction_count", "win_destruction_rate", "verdict",
]
FLAG_FIELDS = ["scope", "symbol", "flag", "details"]


@dataclass(frozen=True)
class ManagedOutcome:
    opportunity: Opportunity
    variant: str
    be_trigger_r: float | None
    classification: str
    realized_r: float | None
    first_event_ts: int | None
    details: str


def _target_price(entry: float, risk: float, direction: str) -> float:
    return entry + risk * TARGET_R if direction == "LONG" else entry - risk * TARGET_R


def _trigger_price(entry: float, risk: float, direction: str, trigger_r: float) -> float:
    return entry + risk * trigger_r if direction == "LONG" else entry - risk * trigger_r


def _hit_target(candle: Candle, target: float, direction: str) -> bool:
    return candle.high >= target if direction == "LONG" else candle.low <= target


def _hit_stop(candle: Candle, stop: float, direction: str) -> bool:
    return candle.low <= stop if direction == "LONG" else candle.high >= stop


def _hit_trigger(candle: Candle, trigger: float, direction: str) -> bool:
    return candle.high >= trigger if direction == "LONG" else candle.low <= trigger


def _baseline_classify(rep: Any, candles: list[Candle]) -> tuple[str, float | None, int | None, str]:
    if rep.geometry_status != "OBSERVABLE":
        return rep.geometry_status, None, None, rep.geometry_details
    replay = _replay(candles, rep.observable_entry_ts, rep.replay_includes_anchor)
    if not candles or not replay:
        return "UNKNOWN_MISSING_CANDLES", None, None, "no replay candles are available after the observable entry anchor"
    assert rep.entry is not None and rep.stop is not None
    risk = abs(rep.entry - rep.stop)
    target = _target_price(rep.entry, risk, rep.direction)
    for candle in replay:
        hit_target = _hit_target(candle, target, rep.direction)
        hit_stop = _hit_stop(candle, rep.stop, rep.direction)
        if hit_target and hit_stop:
            return "SAME_CANDLE_AMBIGUOUS", None, candle.ts, "target and stop are both inside the first event OHLC candle"
        if hit_target:
            return "TARGET_FIRST", TARGET_R, candle.ts, "target is reached before any stop candle"
        if hit_stop:
            return "STOP_FIRST", -1.0, candle.ts, "stop is reached before any target candle"
    return "NOT_REACHED", None, None, "neither target nor stop is reached by available replay candles"


def _be_classify(rep: Any, candles: list[Candle], trigger_r: float) -> tuple[str, float | None, int | None, str]:
    if rep.geometry_status != "OBSERVABLE":
        return rep.geometry_status, None, None, rep.geometry_details
    replay = _replay(candles, rep.observable_entry_ts, rep.replay_includes_anchor)
    if not candles or not replay:
        return "UNKNOWN_MISSING_CANDLES", None, None, "no replay candles are available after the observable entry anchor"
    assert rep.entry is not None and rep.stop is not None
    risk = abs(rep.entry - rep.stop)
    target = _target_price(rep.entry, risk, rep.direction)
    trigger = _trigger_price(rep.entry, risk, rep.direction, trigger_r)
    armed = False
    for candle in replay:
        hit_target = _hit_target(candle, target, rep.direction)
        active_stop = rep.entry if armed else rep.stop
        hit_active_stop = _hit_stop(candle, active_stop, rep.direction)
        hit_initial_stop = _hit_stop(candle, rep.stop, rep.direction)
        hit_trigger = _hit_trigger(candle, trigger, rep.direction)

        if armed:
            if hit_target and hit_active_stop:
                return "SAME_CANDLE_AMBIGUOUS", None, candle.ts, "target and armed break-even stop are both inside the first event OHLC candle"
            if hit_target:
                return "TARGET_FIRST", TARGET_R, candle.ts, "target is reached after break-even was armed"
            if hit_active_stop:
                return "BREAK_EVEN_EXIT", 0.0, candle.ts, "armed break-even stop is reached before target"
            continue

        if hit_target and hit_initial_stop:
            return "SAME_CANDLE_AMBIGUOUS", None, candle.ts, "target and initial stop are both inside the first event OHLC candle"
        if hit_target and hit_trigger and _hit_stop(candle, rep.entry, rep.direction):
            return "SAME_CANDLE_AMBIGUOUS", None, candle.ts, "target and newly armed entry-price stop are both inside the same OHLC candle"
        if hit_target:
            return "TARGET_FIRST", TARGET_R, candle.ts, "target is reached before any stop or break-even exit"
        if hit_trigger and hit_initial_stop:
            return "SAME_CANDLE_AMBIGUOUS", None, candle.ts, "break-even trigger and initial stop are both inside the same OHLC candle before stop order state is knowable"
        if hit_initial_stop:
            return "STOP_FIRST", -1.0, candle.ts, "initial stop is reached before break-even trigger"
        if hit_trigger:
            if _hit_stop(candle, rep.entry, rep.direction):
                return "SAME_CANDLE_AMBIGUOUS", None, candle.ts, "break-even trigger and entry-price stop are both inside the same OHLC candle"
            armed = True
    return "NOT_REACHED", None, None, "neither target nor active stop is reached by available replay candles"


def _execute_variant(opportunities: list[Opportunity], candles_by_symbol: dict[str, list[Candle]], variant: str, trigger_r: float | None) -> list[ManagedOutcome]:
    outcomes: list[ManagedOutcome] = []
    for opp in opportunities:
        rep = opp.representative
        if trigger_r is None:
            classification, realized_r, first_event_ts, details = _baseline_classify(rep, candles_by_symbol[rep.symbol])
        else:
            classification, realized_r, first_event_ts, details = _be_classify(rep, candles_by_symbol[rep.symbol], trigger_r)
        outcomes.append(ManagedOutcome(opp, variant, trigger_r, classification, realized_r, first_event_ts, details))
    return outcomes


def _summarize_variant(variant: str, trigger_r: float | None, outcomes: list[ManagedOutcome], baseline: dict[str, Any] | None, baseline_outcomes: list[ManagedOutcome] | None) -> dict[str, Any]:
    counts = Counter(row.classification for row in outcomes)
    wins, losses, be_exits = counts["TARGET_FIRST"], counts["STOP_FIRST"], counts["BREAK_EVEN_EXIT"]
    trades = wins + losses + be_exits
    total_r = sum(row.realized_r for row in outcomes if row.realized_r is not None)
    expectancy = _round(total_r / trades) if trades else ""
    baseline_expectancy = baseline["expectancy"] if baseline else expectancy
    destroyed = 0
    if baseline_outcomes is not None:
        variant_by_id = {row.opportunity.opportunity_id: row for row in outcomes}
        destroyed = sum(
            1 for row in baseline_outcomes
            if row.classification == "TARGET_FIRST" and variant_by_id[row.opportunity.opportunity_id].classification == "BREAK_EVEN_EXIT"
        )
    baseline_wins = baseline["wins"] if baseline else wins
    baseline_losses = baseline["losses"] if baseline else losses
    loss_reduction = baseline_losses - losses if baseline else 0
    drawdown_reduction = _round(loss_reduction / baseline_losses) if baseline_losses else 0.0
    expectancy_delta = _round(float(expectancy) - float(baseline_expectancy)) if trades else ""
    return {
        "variant": variant,
        "be_trigger_R": "" if trigger_r is None else trigger_r,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "break_even_exits": be_exits,
        "ambiguous": counts["SAME_CANDLE_AMBIGUOUS"],
        "not_reached": counts["NOT_REACHED"],
        "unknown": sum(counts[name] for name in UNKNOWN),
        "win_rate": _round(wins / trades) if trades else "",
        "loss_rate": _round(losses / trades) if trades else "",
        "BE_rate": _round(be_exits / trades) if trades else "",
        "expectancy": expectancy,
        "total_R": _round(total_r) if trades else "",
        "expectancy_delta_vs_baseline": expectancy_delta,
        "drawdown_proxy_reduction": drawdown_reduction,
        "loss_reduction": loss_reduction,
        "win_destruction_count": destroyed,
        "win_destruction_rate": _round(destroyed / baseline_wins) if baseline_wins else 0.0,
        "verdict": "",
    }


def _apply_verdicts(rows: list[dict[str, Any]]) -> tuple[str, str]:
    baseline = rows[0]
    be_rows = rows[1:]
    best = max(be_rows, key=lambda row: (float(row["expectancy"]), int(row["loss_reduction"]))) if be_rows else baseline
    for row in rows:
        if row["variant"] == BASELINE_VARIANT:
            row["verdict"] = "KEEP_BASELINE"
            continue
        delta = float(row["expectancy_delta_vs_baseline"])
        loss_reduction = int(row["loss_reduction"])
        if delta >= 0 and loss_reduction > 0:
            row["verdict"] = "BE_IMPROVES"
        elif loss_reduction > 0 and delta >= -0.05:
            row["verdict"] = "BE_NEUTRAL"
        elif delta < -0.05:
            row["verdict"] = "BE_HURTS"
        else:
            row["verdict"] = "KEEP_BASELINE"
    improving = [row for row in be_rows if row["verdict"] == "BE_IMPROVES"]
    neutral = [row for row in be_rows if row["verdict"] == "BE_NEUTRAL"]
    if improving:
        chosen = max(improving, key=lambda row: (float(row["expectancy"]), int(row["loss_reduction"])))
        return "BE_IMPROVES", f"{chosen['variant']} maintains or improves expectancy while reducing losses"
    if neutral:
        chosen = max(neutral, key=lambda row: (int(row["loss_reduction"]), float(row["expectancy"])))
        return "BE_NEUTRAL", f"{chosen['variant']} reduces losses with minimal expectancy damage"
    if any(row["verdict"] == "BE_HURTS" for row in be_rows):
        return "BE_HURTS", f"best BE expectancy variant is {best['variant']} at {best['expectancy']}R versus baseline {baseline['expectancy']}R"
    return "KEEP_BASELINE", "no break-even variant provides evidence of added value"


def _flags(rows: list[dict[str, Any]], verdict: str, reason: str) -> list[dict[str, str]]:
    flags = [
        {"scope": "ALL", "symbol": COMBINED, "flag": "RESEARCH_ONLY", "details": "break-even audit only; entry, stop, and target are unchanged"},
        {"scope": "ALL", "symbol": COMBINED, "flag": "PROMOTION_GUARD", "details": "allowed verdicts exclude PROMOTE"},
        {"scope": "ALL", "symbol": COMBINED, "flag": verdict, "details": reason},
    ]
    for row in rows:
        if row["ambiguous"]:
            flags.append({"scope": "VARIANT", "symbol": row["variant"], "flag": "AMBIGUOUS_OHLC_ORDERING", "details": f"{row['ambiguous']} opportunities have same-candle ambiguity"})
        if row["not_reached"] or row["unknown"]:
            flags.append({"scope": "VARIANT", "symbol": row["variant"], "flag": "UNRESOLVED_OR_UNKNOWN", "details": f"not_reached={row['not_reached']} unknown={row['unknown']}"})
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
        raise FileExistsError(f"refusing to overwrite existing break-even research output(s): {', '.join(existing)}")

    opportunities = _build_opportunities(observations)
    outcomes_by_variant = {
        variant: _execute_variant(opportunities, candles_by_symbol, variant, trigger_r)
        for variant, trigger_r in BE_VARIANTS
    }
    baseline_row = _summarize_variant(BASELINE_VARIANT, None, outcomes_by_variant[BASELINE_VARIANT], None, None)
    rows = [baseline_row]
    for variant, trigger_r in BE_VARIANTS[1:]:
        rows.append(_summarize_variant(variant, trigger_r, outcomes_by_variant[variant], baseline_row, outcomes_by_variant[BASELINE_VARIANT]))
    verdict, reason = _apply_verdicts(rows)
    flag_rows = _flags(rows, verdict, reason)

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# PnF Pole Motif — Break-Even Research Audit v1\n\n")
        handle.write("Research only. This audit keeps `NEXT_COLUMN_OPEN_ENTRY`, the fixed three-box stop, and the fixed 2.5R target unchanged. It only compares no-management baseline behavior against break-even stop movement after +1R, +1.5R, and +2R. It never outputs `PROMOTE`.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n{reason}.\n\n")
        handle.write("## Variant scorecard\n\n")
        handle.write("| variant | trigger R | trades | wins | losses | BE exits | win rate | loss rate | BE rate | expectancy | total R | expectancy delta | loss reduction | win destruction rate | verdict |\n")
        handle.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
        for row in rows:
            handle.write(
                f"| {row['variant']} | {row['be_trigger_R']} | {row['trades']} | {row['wins']} | {row['losses']} | {row['break_even_exits']} | "
                f"{row['win_rate']} | {row['loss_rate']} | {row['BE_rate']} | {row['expectancy']} | {row['total_R']} | "
                f"{row['expectancy_delta_vs_baseline']} | {row['loss_reduction']} | {row['win_destruction_rate']} | **{row['verdict']}** |\n"
            )
        handle.write("\n## Research question answers\n\n")
        handle.write(f"1. Does BE improve expectancy? **{'Yes' if verdict == 'BE_IMPROVES' else 'No'}**. {reason}.\n")
        handle.write(f"2. Does BE reduce losses? Best loss reduction observed: **{max(int(row['loss_reduction']) for row in rows[1:]) if len(rows) > 1 else 0}**.\n")
        handle.write(f"3. Winners destroyed by early BE movement: max **{max(int(row['win_destruction_count']) for row in rows[1:]) if len(rows) > 1 else 0}**.\n")
        best_preserver = max(rows[1:], key=lambda row: (float(row['expectancy']), -int(row['win_destruction_count']))) if len(rows) > 1 else rows[0]
        handle.write(f"4. Trigger preserving the edge best: **{best_preserver['variant']}** by highest BE-variant expectancy.\n")

    _write_csv(output_root / OUTPUT_NAMES[1], VARIANT_FIELDS, rows)
    _write_csv(output_root / OUTPUT_NAMES[2], FLAG_FIELDS, flag_rows)
    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_be_research_audit_v1",
        "research_only": True,
        "strategy_promotion": False,
        "production_modifications": False,
        "optimization_performed": False,
        "entry": ENTRY_CANDIDATE,
        "stop": "fixed_3_box_stop",
        "target_R": TARGET_R,
        "management_rules": {"tp1": False, "tp2": False, "trailing_stop": False, "scaling": False, "pyramiding": False, "break_even_variants": [trigger for _, trigger in BE_VARIANTS]},
        "symbols": symbols,
        "unique_opportunities": len(opportunities),
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "verdict": verdict,
        "verdict_reason": reason,
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[3]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only PnF pole break-even management audit v1")
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
