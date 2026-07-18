"""Research-only NEXT_COLUMN_OPEN_ENTRY pending-limit reality audit.

The existing NEXT_COLUMN_OPEN_ENTRY research baseline records the first candle open after
confirmation as the intended entry price. This audit does not change that baseline. It
asks whether that intended price is executable as a pending limit order: the order is
active for a fixed number of candles, starts a trade only if price trades through the
entry level, and is cancelled after expiry so it can never fill later.
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

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, EntryTimingObservation, _classify
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import ENTRY_CANDIDATE, UNKNOWN, _load_observations
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import _parse_symbol_input, _round

PRIMARY_TARGET_R = 2.5
COMBINED = "COMBINED"
LIMIT_FILL_MODEL = "PENDING_LIMIT_THROUGH_ENTRY_WITH_EXPIRY"
EXPIRY_CANDLES = (1, 2, 3)
ALLOWED_VERDICTS = ("LIMIT_FILL_EDGE_SURVIVES", "LIMIT_FILL_EDGE_WEAKENS", "LIMIT_FILL_EDGE_COLLAPSES", "INSUFFICIENT_DATA")
OUTPUT_NAMES = (
    "next_open_limit_fill_reality_summary.md",
    "next_open_limit_fill_reality_rows.csv",
    "next_open_limit_fill_reality_symbol_breakdown.csv",
    "next_open_limit_fill_reality_manifest.json",
)
ROW_FIELDS = [
    "expiry_candles", "symbol", "row_number", "direction", "entry_candidate", "intended_entry_ts",
    "intended_entry_time_utc", "limit_price", "stop_price", "fill_status", "fill_ts", "fill_time_utc",
    "outcome_classification", "realized_R", "details",
]
SYMBOL_FIELDS = [
    "expiry_candles", "symbol", "observations", "observable_rows", "filled_rows", "cancelled_rows",
    "fill_rate", "resolved_rows", "target_first", "stop_first", "same_candle_fill_stop_conservative",
    "same_candle_fill_target_ambiguous", "ambiguous", "not_reached", "unknown", "win_rate_non_ambiguous",
    "avg_realized_R", "total_realized_R",
]
STOP_CLASSIFICATIONS = {"STOP_FIRST", "SAME_CANDLE_FILL_STOP_CONSERVATIVE"}
AMBIGUOUS_CLASSIFICATIONS = {"SAME_CANDLE_AMBIGUOUS", "SAME_CANDLE_FILL_TARGET_AMBIGUOUS"}


@dataclass(frozen=True)
class LimitFillResult:
    observation: EntryTimingObservation
    expiry_candles: int
    fill_status: str
    fill_ts: int | None
    outcome_classification: str
    realized_r: float | str
    details: str


def _ts_to_utc(ts: int | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(ts / 1000 if ts > 10_000_000_000 else ts, tz=UTC).isoformat()


def _limit_touched(candle: Candle, row: EntryTimingObservation) -> bool:
    if row.entry is None:
        return False
    if row.direction == "LONG":
        return candle.low < row.entry
    if row.direction == "SHORT":
        return candle.high > row.entry
    return candle.low <= row.entry <= candle.high


def _target_price(row: EntryTimingObservation, target_r: float) -> float | None:
    if row.entry is None:
        return None
    if row.direction == "LONG":
        return row.entry + 3 * row.box_size * target_r
    if row.direction == "SHORT":
        return row.entry - 3 * row.box_size * target_r
    return None


def _stop_touched(candle: Candle, row: EntryTimingObservation) -> bool:
    if row.stop is None:
        return False
    return candle.low <= row.stop if row.direction == "LONG" else candle.high >= row.stop


def _target_touched(candle: Candle, row: EntryTimingObservation, target_r: float) -> bool:
    target = _target_price(row, target_r)
    if target is None:
        return False
    return candle.high >= target if row.direction == "LONG" else candle.low <= target


def _post_fill_observation(row: EntryTimingObservation, fill_ts: int, candles: list[Candle]) -> EntryTimingObservation:
    return EntryTimingObservation(
        symbol=row.symbol,
        row_number=row.row_number,
        direction=row.direction,
        entry_candidate=row.entry_candidate,
        pole_idx=row.pole_idx,
        reversal_idx=row.reversal_idx,
        confirmation_idx=row.confirmation_idx,
        box_size=row.box_size,
        entry=row.entry,
        stop=row.stop,
        observable_entry_ts=fill_ts,
        replay_includes_anchor=False,
        candles_in_replay=len([candle for candle in candles if candle.ts > fill_ts]),
        geometry_status="OBSERVABLE",
        geometry_details=f"pending limit filled at ts {fill_ts}; stop/target replay starts after the fill candle",
    )


def _realized_r(classification: str, target_r: float) -> float | str:
    if classification == "TARGET_FIRST":
        return target_r
    if classification in STOP_CLASSIFICATIONS:
        return -1.0
    return ""


def _limit_fill_result(
    row: EntryTimingObservation,
    candles: list[Candle],
    expiry_candles: int,
    target_r: float = PRIMARY_TARGET_R,
) -> LimitFillResult:
    if expiry_candles <= 0:
        raise ValueError("expiry_candles must be positive")
    if row.geometry_status != "OBSERVABLE" or row.observable_entry_ts is None or row.entry is None or row.stop is None:
        return LimitFillResult(row, expiry_candles, row.geometry_status, None, row.geometry_status, "", row.geometry_details)

    eligible = [candle for candle in candles if candle.ts >= row.observable_entry_ts]
    if not eligible:
        return LimitFillResult(row, expiry_candles, "UNKNOWN_MISSING_CANDLES", None, "UNKNOWN_MISSING_CANDLES", "", "no candles exist at or after the intended next-open timestamp")

    active_window = eligible[:expiry_candles]
    first_fill = next((candle for candle in active_window if _limit_touched(candle, row)), None)
    if first_fill is None:
        return LimitFillResult(row, expiry_candles, "CANCELLED_EXPIRED", None, "MISSED_LIMIT_FILL", "", f"pending limit was cancelled after {expiry_candles} candle(s) and cannot fill later")

    stop_on_fill = _stop_touched(first_fill, row)
    target_on_fill = _target_touched(first_fill, row, target_r)
    if stop_on_fill:
        return LimitFillResult(
            row,
            expiry_candles,
            "FILLED",
            first_fill.ts,
            "SAME_CANDLE_FILL_STOP_CONSERVATIVE",
            -1.0,
            "fill candle also contains the stop level; conservative same-candle handling records a stop",
        )
    if target_on_fill:
        return LimitFillResult(
            row,
            expiry_candles,
            "FILLED",
            first_fill.ts,
            "SAME_CANDLE_FILL_TARGET_AMBIGUOUS",
            "",
            "fill candle also contains the target level; OHLC cannot prove target occurred after the limit fill",
        )

    classified = _classify(_post_fill_observation(row, first_fill.ts, candles), candles, target_r)
    classification = classified["classification"]
    return LimitFillResult(row, expiry_candles, "FILLED", first_fill.ts, classification, _realized_r(classification, target_r), classified["details"])


def _rows(results: Iterable[LimitFillResult]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        row = result.observation
        rows.append({
            "expiry_candles": result.expiry_candles,
            "symbol": row.symbol,
            "row_number": row.row_number,
            "direction": row.direction,
            "entry_candidate": row.entry_candidate,
            "intended_entry_ts": row.observable_entry_ts or "",
            "intended_entry_time_utc": _ts_to_utc(row.observable_entry_ts),
            "limit_price": "" if row.entry is None else _round(row.entry),
            "stop_price": "" if row.stop is None else _round(row.stop),
            "fill_status": result.fill_status,
            "fill_ts": result.fill_ts or "",
            "fill_time_utc": _ts_to_utc(result.fill_ts),
            "outcome_classification": result.outcome_classification,
            "realized_R": result.realized_r,
            "details": result.details,
        })
    return rows


def _symbol_summary(expiry_candles: int, symbol: str, results: list[LimitFillResult]) -> dict[str, Any]:
    scoped_by_expiry = [result for result in results if result.expiry_candles == expiry_candles]
    scoped = scoped_by_expiry if symbol == COMBINED else [result for result in scoped_by_expiry if result.observation.symbol == symbol]
    counts = Counter(result.outcome_classification for result in scoped)
    fills = [result for result in scoped if result.fill_status == "FILLED"]
    stop_first = sum(counts[name] for name in STOP_CLASSIFICATIONS)
    realized = [float(result.realized_r) for result in fills if result.realized_r != ""]
    resolved = counts["TARGET_FIRST"] + stop_first
    return {
        "expiry_candles": expiry_candles,
        "symbol": symbol,
        "observations": len(scoped),
        "observable_rows": sum(1 for result in scoped if result.observation.geometry_status == "OBSERVABLE"),
        "filled_rows": len(fills),
        "cancelled_rows": sum(1 for result in scoped if result.fill_status == "CANCELLED_EXPIRED"),
        "fill_rate": _round(len(fills) / len(scoped)) if scoped else "",
        "resolved_rows": resolved,
        "target_first": counts["TARGET_FIRST"],
        "stop_first": stop_first,
        "same_candle_fill_stop_conservative": counts["SAME_CANDLE_FILL_STOP_CONSERVATIVE"],
        "same_candle_fill_target_ambiguous": counts["SAME_CANDLE_FILL_TARGET_AMBIGUOUS"],
        "ambiguous": sum(counts[name] for name in AMBIGUOUS_CLASSIFICATIONS),
        "not_reached": counts["NOT_REACHED"],
        "unknown": sum(counts[name] for name in UNKNOWN) + counts["MISSED_LIMIT_FILL"],
        "win_rate_non_ambiguous": _round(counts["TARGET_FIRST"] / resolved) if resolved else "",
        "avg_realized_R": _round(sum(realized) / len(realized)) if realized else "",
        "total_realized_R": _round(sum(realized)) if realized else "",
    }


def _verdict(summary: dict[str, Any]) -> tuple[str, str]:
    if summary["resolved_rows"] < 10 or summary["avg_realized_R"] == "":
        return "INSUFFICIENT_DATA", "fewer than 10 resolved pending-limit rows"
    if summary["avg_realized_R"] <= 0:
        return "LIMIT_FILL_EDGE_COLLAPSES", "pending-limit resolved expectancy is non-positive"
    if summary["fill_rate"] != "" and summary["fill_rate"] < 0.5:
        return "LIMIT_FILL_EDGE_WEAKENS", "expectancy remains positive but fewer than half of observations fill before expiry"
    return "LIMIT_FILL_EDGE_SURVIVES", "pending-limit expectancy remains positive with at least half of rows filled before expiry"


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def run(symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], candles_inputs: dict[str, Path], output_root: Path, candle_symbols: dict[str, str] | None = None) -> None:
    symbols, observations, candles_by_symbol = _load_observations(symbol_inputs, columns_inputs, candles_inputs, candle_symbols or {})
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing limit-fill output(s): {', '.join(existing)}")

    results = [
        _limit_fill_result(row, candles_by_symbol[row.symbol], expiry_candles)
        for expiry_candles in EXPIRY_CANDLES
        for row in observations
    ]
    row_output = _rows(results)
    summary_rows = [_symbol_summary(expiry_candles, symbol, results) for expiry_candles in EXPIRY_CANDLES for symbol in [*symbols, COMBINED]]
    combined_by_expiry = {expiry: next(row for row in summary_rows if row["symbol"] == COMBINED and row["expiry_candles"] == expiry) for expiry in EXPIRY_CANDLES}
    verdicts = {expiry: _verdict(summary) for expiry, summary in combined_by_expiry.items()}

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# NEXT_COLUMN_OPEN_ENTRY pending-limit reality audit\n\n")
        handle.write("Research only. This audit does not modify live trading logic, protected strategy baseline, optimize parameters, or promote the baseline. It never outputs `PROMOTE`.\n\n")
        handle.write(f"Limit-fill model: `{LIMIT_FILL_MODEL}`. A pending limit starts a trade only when an eligible candle trades through the entry price. If no fill occurs within 1, 2, or 3 candles, the order is cancelled and cannot fill later. Stop/target replay begins after the actual fill candle, with same-candle fill plus stop/target handled conservatively and explicitly.\n\n")
        for expiry in EXPIRY_CANDLES:
            verdict, reason = verdicts[expiry]
            combined = combined_by_expiry[expiry]
            handle.write(f"## Expiry: {expiry} candle(s) — **{verdict}**\n\n{reason}.\n\n")
            handle.write("| metric | value |\n|---|---:|\n")
            for key in ("observations", "filled_rows", "cancelled_rows", "resolved_rows", "win_rate_non_ambiguous", "avg_realized_R", "total_realized_R"):
                handle.write(f"| {key} | {combined[key]} |\n")
            handle.write("\n")

    _write_csv(output_root / OUTPUT_NAMES[1], ROW_FIELDS, row_output)
    _write_csv(output_root / OUTPUT_NAMES[2], SYMBOL_FIELDS, summary_rows)
    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_next_open_limit_fill_reality_audit",
        "research_only": True,
        "live_trading_logic_modified": False,
        "protected_strategy_baseline_modified": False,
        "strategy_promotion": False,
        "optimization_performed": False,
        "entry_candidate": ENTRY_CANDIDATE,
        "limit_fill_model": LIMIT_FILL_MODEL,
        "expiry_candles": list(EXPIRY_CANDLES),
        "primary_target_r": PRIMARY_TARGET_R,
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "verdicts": {str(expiry): {"verdict": verdicts[expiry][0], "verdict_reason": verdicts[expiry][1]} for expiry in EXPIRY_CANDLES},
        "combined_summary_by_expiry": {str(expiry): combined_by_expiry[expiry] for expiry in EXPIRY_CANDLES},
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[3]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only NEXT_COLUMN_OPEN_ENTRY pending-limit reality audit")
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
