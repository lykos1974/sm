"""Research-only diagnostic segmentation audit for the causal P+2 pole motif.

This module segments the already-causal ``pole -> reversal -> confirmation``
P+2 motif to identify where a weak aggregate edge is concentrated.  It is a
research artifact only: it does not import live traders, production strategy
modules, detectors, or mutate schemas.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_be_research_audit import _be_classify
from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, EntryTimingObservation, _candidate_observation
from research_v2.patterns.pole_core_motif_execution_reality_audit import _build_opportunities
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import ENTRY_CANDIDATE, UNKNOWN, _load_candles
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import (
    TimedColumn,
    _check_symbols,
    _flag,
    _load_columns,
    _parse_candle_symbol,
    _require_paths,
)
from research_v2.patterns.pole_core_motif_sl_candidates import _direction, _parse_symbol_input, _round, _to_float, _to_int
from research_v2.patterns.pole_p2_causal_motif_audit import BREAK_EVEN_TRIGGER_R, EXPECTED_SYMBOLS, MOTIF_NAME, TARGET_R, _validate_full_universe

OUTPUT_NAMES = (
    "p2_edge_segmentation_summary.md",
    "p2_edge_segmentation_by_symbol.csv",
    "p2_edge_segmentation_by_direction.csv",
    "p2_edge_segmentation_by_pole_strength.csv",
    "p2_edge_segmentation_by_retrace_quality.csv",
    "p2_edge_segmentation_by_market_family.csv",
    "p2_edge_segmentation_by_time.csv",
    "p2_edge_segmentation_flags.csv",
    "p2_edge_segmentation_manifest.json",
)
ALLOWED_VERDICTS = (
    "EDGE_CONCENTRATED_IN_SEGMENTS",
    "EDGE_BROAD_BUT_WEAK",
    "NO_ACTIONABLE_SEGMENT",
    "INSUFFICIENT_DATA",
)
SUMMARY_FIELDS = [
    "segment_type",
    "segment",
    "observations",
    "unique_opportunities",
    "trades",
    "wins",
    "losses",
    "break_even_exits",
    "win_rate",
    "expectancy",
    "total_R",
]
POLE_FIELDS = ["segment_type", "segment", "pole_metric", *SUMMARY_FIELDS[2:]]
RETRACE_FIELDS = ["segment_type", "segment", "retrace_metric", *SUMMARY_FIELDS[2:]]
TIME_FIELDS = ["segment_type", "segment", "year", "quarter", *SUMMARY_FIELDS[2:]]
FLAG_FIELDS = ["segment_type", "segment", "flag", "details"]
RECENT_COLUMN_LOOKBACK = 10
MIN_DIAGNOSTIC_TRADES = 30
CONCENTRATION_EXPECTANCY_R = 0.15
BASELINE_P2_TRADES = 4023
BASELINE_P2_EXPECTANCY = 0.062516
BASELINE_P2_TOTAL_R = 251.5
UNIVERSE_EXPECTANCY_TOLERANCE = 0.000001
UNIVERSE_TOTAL_R_TOLERANCE = 0.000001


@dataclass(frozen=True)
class SegmentedObservation:
    observation: EntryTimingObservation
    pole_boxes: float | None
    pole_duration_seconds: int | None
    pole_velocity: float | None
    relative_pole_size: float | None
    reversal_boxes: float | None
    retrace_ratio: float | None
    trend_regime: str
    current_pnf_direction: str
    choppiness: str
    breakout_context: str
    market_family: str
    exchange: str
    year: str
    quarter: str


@dataclass(frozen=True)
class SegmentedOutcome:
    observation_count: int
    symbol: str
    direction: str
    classification: str
    realized_r: float | None
    segments: dict[str, str]


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _clean(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "UNKNOWN"


def _column_boxes(column: TimedColumn | None, box_size: float) -> float | None:
    if column is None or box_size <= 0:
        return None
    return _round(abs(column.top - column.bottom) / box_size + 1.0)


def _duration_seconds(column: TimedColumn | None) -> int | None:
    if column is None or column.start_ts is None or column.end_ts is None or column.end_ts < column.start_ts:
        return None
    raw = column.end_ts - column.start_ts
    return int(raw / 1000) if raw > 10_000_000_000 else int(raw)


def _ts_parts(ts: int | None) -> tuple[str, str]:
    if ts is None:
        return "UNKNOWN", "UNKNOWN"
    dt = datetime.fromtimestamp(ts / 1000 if ts > 10_000_000_000 else ts, tz=UTC)
    return str(dt.year), f"Q{((dt.month - 1) // 3) + 1}"


def _exchange_from_path(*paths: Path) -> str:
    text = " ".join(str(path).lower() for path in paths)
    if "binance" in text:
        return "BINANCE"
    if "mexc" in text:
        return "MEXC"
    return "UNKNOWN_EXCHANGE"


def _bucket_pole_boxes(value: float | None) -> str:
    if value is None:
        return "MISSING_POLE_BOXES"
    if value <= 4:
        return "SMALL_<=4_BOXES"
    if value <= 7:
        return "NORMAL_5_7_BOXES"
    return "LARGE_>=8_BOXES"


def _bucket_duration(value: int | None) -> str:
    if value is None:
        return "MISSING_DURATION"
    if value <= 3600:
        return "FAST_<=1H"
    if value <= 14_400:
        return "MEDIUM_1H_4H"
    if value <= 86_400:
        return "SLOW_4H_1D"
    return "VERY_SLOW_>1D"


def _bucket_velocity(value: float | None) -> str:
    if value is None:
        return "MISSING_VELOCITY"
    if value >= 0.002:
        return "HIGH_BOXES_PER_SECOND"
    if value >= 0.0005:
        return "MEDIUM_BOXES_PER_SECOND"
    return "LOW_BOXES_PER_SECOND"


def _bucket_relative_size(value: float | None) -> str:
    if value is None:
        return "MISSING_RELATIVE_SIZE"
    if value < 0.75:
        return "BELOW_RECENT_AVG_<0_75X"
    if value <= 1.25:
        return "NEAR_RECENT_AVG_0_75X_1_25X"
    return "ABOVE_RECENT_AVG_>1_25X"


def _bucket_reversal_boxes(value: float | None) -> str:
    if value is None:
        return "MISSING_REVERSAL_BOXES"
    if value <= 3:
        return "SMALL_REVERSAL_<=3_BOXES"
    if value <= 6:
        return "NORMAL_REVERSAL_4_6_BOXES"
    return "LARGE_REVERSAL_>=7_BOXES"


def _bucket_retrace(value: float | None) -> str:
    if value is None:
        return "MISSING_RETRACE"
    if value < 0.382:
        return "SHALLOW_<0_382"
    if value <= 0.618:
        return "NORMAL_0_382_0_618"
    return "DEEP_>0_618"


def _choppiness(columns: dict[int, TimedColumn], pole_idx: int) -> str:
    recent = [columns[idx] for idx in range(max(0, pole_idx - 6), pole_idx) if idx in columns]
    if len(recent) < 3:
        return "CHOPPINESS_UNKNOWN"
    durations = [_duration_seconds(column) for column in recent]
    short_columns = sum(1 for column in recent if (_column_boxes(column, 1.0) or 0) <= 3)
    fast_columns = sum(1 for value in durations if value is not None and value <= 3600)
    if short_columns >= 4 or fast_columns >= 4:
        return "CHOPPY_RECENT_COLUMNS"
    return "CLEANER_RECENT_COLUMNS"


def _relative_pole_size(columns: dict[int, TimedColumn], pole_idx: int, pole_boxes: float | None, box_size: float) -> float | None:
    if pole_boxes is None:
        return None
    recent = [
        _column_boxes(columns[idx], box_size)
        for idx in range(max(0, pole_idx - RECENT_COLUMN_LOOKBACK), pole_idx)
        if idx in columns
    ]
    usable = [value for value in recent if value is not None]
    if not usable:
        return None
    return _round(pole_boxes / (sum(usable) / len(usable)))


def _row_retrace(row: dict[str, Any], pole_boxes: float | None, reversal_boxes: float | None) -> float | None:
    explicit = _to_float(row.get("retrace_ratio"))
    if explicit is not None:
        return _round(explicit)
    if pole_boxes and reversal_boxes is not None:
        return _round(reversal_boxes / pole_boxes)
    return None


def _is_p2_candidate(row: dict[str, Any]) -> bool:
    return _direction(row.get("pattern_name")) is not None and _to_int(row.get("pole_column_index")) is not None and _to_int(row.get("reversal_column_index")) is not None


def load_segmented_observations(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    candle_symbols: dict[str, str],
    *,
    limit_rows_per_symbol: int | None = None,
) -> tuple[list[str], list[SegmentedObservation], dict[str, list[Candle]], list[dict[str, str]]]:
    symbols = _check_symbols(symbol_inputs, columns_inputs, candles_inputs)
    _require_paths(symbol_inputs, "symbol-input")
    _require_paths(columns_inputs, "columns-input")
    _require_paths(candles_inputs, "candles-input")
    segmented: list[SegmentedObservation] = []
    candles_by_symbol: dict[str, list[Candle]] = {}
    flags: list[dict[str, str]] = []

    for symbol in symbols:
        columns, box_size = _load_columns(columns_inputs[symbol])
        if box_size is None or box_size <= 0:
            raise ValueError(f"{symbol}: matching PnF columns do not expose a positive box size in profile_name")
        candles = _load_candles(candles_inputs[symbol], candle_symbols.get(symbol, symbol))
        candles_by_symbol[symbol] = candles
        exchange = _exchange_from_path(symbol_inputs[symbol], columns_inputs[symbol], candles_inputs[symbol])
        loaded_for_symbol = 0
        with symbol_inputs[symbol].open("r", newline="") as handle:
            rows = list(csv.DictReader(handle))
        for row_number, row in enumerate(rows, start=2):
            if limit_rows_per_symbol is not None and loaded_for_symbol >= limit_rows_per_symbol:
                break
            if not _is_p2_candidate(row):
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
            observation = _candidate_observation(symbol, row_number, direction, ENTRY_CANDIDATE, pole, reversal, confirmation, box_size, candles)
            pole_boxes = _column_boxes(pole, box_size)
            reversal_boxes = _column_boxes(reversal, box_size)
            pole_duration = _duration_seconds(pole)
            pole_velocity = _round(pole_boxes / pole_duration) if pole_boxes is not None and pole_duration else None
            relative_size = _relative_pole_size(columns, pole_idx, pole_boxes, box_size)
            retrace_ratio = _row_retrace(row, pole_boxes, reversal_boxes)
            year, quarter = _ts_parts(observation.observable_entry_ts)
            segmented.append(SegmentedObservation(
                observation=observation,
                pole_boxes=pole_boxes,
                pole_duration_seconds=pole_duration,
                pole_velocity=pole_velocity,
                relative_pole_size=relative_size,
                reversal_boxes=reversal_boxes,
                retrace_ratio=retrace_ratio,
                trend_regime=_clean(row.get("market_regime") or row.get("trend_regime") or row.get("regime")),
                current_pnf_direction=("UP_COLUMN" if confirmation and confirmation.kind == "X" else "DOWN_COLUMN" if confirmation and confirmation.kind == "O" else "UNKNOWN"),
                choppiness=_choppiness(columns, pole_idx),
                breakout_context=_clean(row.get("breakout_context") or row.get("breakdown_context") or row.get("recent_breakout_context")),
                market_family=f"{exchange}:{symbol}",
                exchange=exchange,
                year=year,
                quarter=quarter,
            ))
            loaded_for_symbol += 1
    return symbols, segmented, candles_by_symbol, flags


def _segment_map(row: SegmentedObservation) -> dict[str, str]:
    return {
        "symbol": row.observation.symbol,
        "direction": row.observation.direction,
        "pole_boxes": _bucket_pole_boxes(row.pole_boxes),
        "pole_duration": _bucket_duration(row.pole_duration_seconds),
        "pole_velocity": _bucket_velocity(row.pole_velocity),
        "relative_pole_size": _bucket_relative_size(row.relative_pole_size),
        "reversal_boxes": _bucket_reversal_boxes(row.reversal_boxes),
        "retrace_quality": _bucket_retrace(row.retrace_ratio),
        "trend_regime": row.trend_regime,
        "current_pnf_direction": row.current_pnf_direction,
        "choppiness": row.choppiness,
        "breakout_context": row.breakout_context,
        "market_family": row.market_family,
        "exchange": row.exchange,
        "year": row.year,
        "quarter": row.quarter,
        "year_quarter": f"{row.year}-{row.quarter}",
    }


def _build_segmented_outcomes(segmented: list[SegmentedObservation], candles_by_symbol: dict[str, list[Candle]]) -> list[SegmentedOutcome]:
    by_key = {(row.observation.symbol, row.observation.row_number): row for row in segmented}
    opportunities = _build_opportunities([row.observation for row in segmented])
    outcomes: list[SegmentedOutcome] = []
    for opportunity in opportunities:
        rep = opportunity.representative
        segment_row = by_key[(rep.symbol, rep.row_number)]
        classification, realized_r, _ts, _details = _be_classify(rep, candles_by_symbol[rep.symbol], BREAK_EVEN_TRIGGER_R)
        outcomes.append(SegmentedOutcome(
            observation_count=len(opportunity.observations),
            symbol=rep.symbol,
            direction=rep.direction,
            classification=classification,
            realized_r=realized_r,
            segments=_segment_map(segment_row),
        ))
    return outcomes


def summarize_outcomes(outcomes: Iterable[SegmentedOutcome], segment_type: str, segment_value: str) -> dict[str, Any]:
    rows = list(outcomes)
    counts = Counter(row.classification for row in rows)
    wins = counts["TARGET_FIRST"]
    losses = counts["STOP_FIRST"]
    be_exits = counts["BREAK_EVEN_EXIT"]
    trades = wins + losses + be_exits
    total_r = sum(row.realized_r for row in rows if row.realized_r is not None)
    return {
        "segment_type": segment_type,
        "segment": segment_value,
        "observations": sum(row.observation_count for row in rows),
        "unique_opportunities": len(rows),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "break_even_exits": be_exits,
        "win_rate": _round(wins / (wins + losses)) if wins + losses else "",
        "expectancy": _round(total_r / trades) if trades else "",
        "total_R": _round(total_r),
    }


def _rows_for_segments(outcomes: list[SegmentedOutcome], segment_types: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for segment_type in segment_types:
        grouped: dict[str, list[SegmentedOutcome]] = defaultdict(list)
        for outcome in outcomes:
            grouped[outcome.segments[segment_type]].append(outcome)
        for segment_value in sorted(grouped):
            rows.append(summarize_outcomes(grouped[segment_value], segment_type, segment_value))
    return rows


def _pole_rows(outcomes: list[SegmentedOutcome]) -> list[dict[str, Any]]:
    rows = []
    for row in _rows_for_segments(outcomes, ["pole_boxes", "pole_duration", "pole_velocity", "relative_pole_size"]):
        rows.append({"pole_metric": row["segment_type"], **row})
    return rows


def _retrace_rows(outcomes: list[SegmentedOutcome]) -> list[dict[str, Any]]:
    rows = []
    for row in _rows_for_segments(outcomes, ["reversal_boxes", "retrace_quality"]):
        rows.append({"retrace_metric": row["segment_type"], **row})
    return rows


def _time_rows(outcomes: list[SegmentedOutcome]) -> list[dict[str, Any]]:
    rows = []
    for row in _rows_for_segments(outcomes, ["year", "year_quarter"]):
        year, quarter = (row["segment"].split("-", 1) if row["segment_type"] == "year_quarter" else (row["segment"], ""))
        rows.append({"year": year, "quarter": quarter, **row})
    return rows


def _flag_rows(all_row: dict[str, Any], segment_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    flags: list[dict[str, str]] = []
    aggregate_expectancy = float(all_row["expectancy"]) if all_row.get("expectancy") != "" else 0.0
    for row in segment_rows:
        trades = int(row.get("trades") or 0)
        expectancy = row.get("expectancy")
        if trades and trades < MIN_DIAGNOSTIC_TRADES:
            flags.append({"segment_type": row["segment_type"], "segment": row["segment"], "flag": "LOW_SAMPLE", "details": f"{trades} trades; diagnostic only"})
        if expectancy != "" and trades >= MIN_DIAGNOSTIC_TRADES and float(expectancy) >= max(CONCENTRATION_EXPECTANCY_R, aggregate_expectancy + 0.10):
            flags.append({"segment_type": row["segment_type"], "segment": row["segment"], "flag": "POSITIVE_CONCENTRATION", "details": f"expectancy {expectancy}R over {trades} trades"})
        if expectancy != "" and trades >= MIN_DIAGNOSTIC_TRADES and float(expectancy) < 0:
            flags.append({"segment_type": row["segment_type"], "segment": row["segment"], "flag": "NEGATIVE_SEGMENT", "details": f"expectancy {expectancy}R over {trades} trades"})
    return flags


def _universe_consistency(all_row: dict[str, Any]) -> dict[str, Any]:
    actual_trades = int(all_row.get("trades") or 0)
    actual_expectancy = all_row.get("expectancy")
    actual_total_r = all_row.get("total_R")
    expectancy_delta = "" if actual_expectancy == "" else _round(float(actual_expectancy) - BASELINE_P2_EXPECTANCY)
    total_r_delta = "" if actual_total_r == "" else _round(float(actual_total_r) - BASELINE_P2_TOTAL_R)
    mismatches = []
    if actual_trades != BASELINE_P2_TRADES:
        mismatches.append("trades")
    if expectancy_delta == "" or abs(float(expectancy_delta)) > UNIVERSE_EXPECTANCY_TOLERANCE:
        mismatches.append("expectancy")
    if total_r_delta == "" or abs(float(total_r_delta)) > UNIVERSE_TOTAL_R_TOLERANCE:
        mismatches.append("total_R")
    return {
        "status": "UNIVERSE_MATCH" if not mismatches else "UNIVERSE_MISMATCH",
        "expected": {
            "trades": BASELINE_P2_TRADES,
            "expectancy": BASELINE_P2_EXPECTANCY,
            "total_R": BASELINE_P2_TOTAL_R,
        },
        "actual": {
            "observations": all_row.get("observations", ""),
            "unique_opportunities": all_row.get("unique_opportunities", ""),
            "trades": actual_trades,
            "wins": all_row.get("wins", ""),
            "losses": all_row.get("losses", ""),
            "break_even_exits": all_row.get("break_even_exits", ""),
            "expectancy": actual_expectancy,
            "total_R": actual_total_r,
        },
        "deltas": {
            "trades": actual_trades - BASELINE_P2_TRADES,
            "expectancy": expectancy_delta,
            "total_R": total_r_delta,
        },
        "mismatched_fields": mismatches,
    }


def _universe_flag(universe: dict[str, Any]) -> dict[str, str] | None:
    if universe["status"] != "UNIVERSE_MISMATCH":
        return None
    expected = universe["expected"]
    actual = universe["actual"]
    return {
        "segment_type": "UNIVERSE",
        "segment": "ALL",
        "flag": "UNIVERSE_MISMATCH",
        "details": (
            f"expected trades={expected['trades']}, expectancy={expected['expectancy']}, total_R={expected['total_R']}; "
            f"actual trades={actual['trades']}, expectancy={actual['expectancy']}, total_R={actual['total_R']}"
        ),
    }


def _verdict(all_row: dict[str, Any], flags: list[dict[str, str]]) -> str:
    trades = int(all_row.get("trades") or 0)
    expectancy = all_row.get("expectancy")
    if trades < MIN_DIAGNOSTIC_TRADES or expectancy == "":
        return "INSUFFICIENT_DATA"
    if any(row["flag"] == "POSITIVE_CONCENTRATION" for row in flags):
        return "EDGE_CONCENTRATED_IN_SEGMENTS"
    if float(expectancy) > 0:
        return "EDGE_BROAD_BUT_WEAK"
    return "NO_ACTIONABLE_SEGMENT"


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
    symbols, segmented, candles_by_symbol, load_flags = load_segmented_observations(
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
        raise FileExistsError(f"refusing to overwrite existing P+2 segmentation output(s): {', '.join(existing)}")

    outcomes = _build_segmented_outcomes(segmented, candles_by_symbol)
    all_row = summarize_outcomes(outcomes, "ALL", "ALL")
    by_symbol = _rows_for_segments(outcomes, ["symbol"])
    by_direction = _rows_for_segments(outcomes, ["direction"])
    by_pole = _pole_rows(outcomes)
    by_retrace = _retrace_rows(outcomes)
    by_family = _rows_for_segments(outcomes, ["exchange", "market_family", "trend_regime", "current_pnf_direction", "choppiness", "breakout_context"])
    by_time = _time_rows(outcomes)
    segment_rows = [*by_symbol, *by_direction, *by_pole, *by_retrace, *by_family, *by_time]
    diagnostic_flags = _flag_rows(all_row, segment_rows)
    universe_consistency = _universe_consistency(all_row)
    universe_flag = _universe_flag(universe_consistency)
    flags = [
        {"segment_type": "LOAD", "segment": f"{row['symbol']}:{row['row_number']}", "flag": row["result"], "details": f"{row['check_name']}: {row['details']}"}
        for row in load_flags
    ] + ([universe_flag] if universe_flag is not None else []) + diagnostic_flags
    verdict = _verdict(all_row, flags)

    _write_csv(output_root / OUTPUT_NAMES[1], SUMMARY_FIELDS, by_symbol)
    _write_csv(output_root / OUTPUT_NAMES[2], SUMMARY_FIELDS, by_direction)
    _write_csv(output_root / OUTPUT_NAMES[3], POLE_FIELDS, by_pole)
    _write_csv(output_root / OUTPUT_NAMES[4], RETRACE_FIELDS, by_retrace)
    _write_csv(output_root / OUTPUT_NAMES[5], SUMMARY_FIELDS, by_family)
    _write_csv(output_root / OUTPUT_NAMES[6], TIME_FIELDS, by_time)
    _write_csv(output_root / OUTPUT_NAMES[7], FLAG_FIELDS, flags)

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# P+2 causal edge segmentation audit\n\n")
        handle.write("Research only. No live trader, production strategy, detector, or schema changes are made.\n\n")
        handle.write("This diagnostic segments the causal P+2 pole/reversal/confirmation motif; it is not filter optimization and does not promote any segment.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n")
        handle.write("## Aggregate break-even-managed P+2 result\n\n")
        for key in ("observations", "unique_opportunities", "trades", "wins", "losses", "break_even_exits", "win_rate", "expectancy", "total_R"):
            handle.write(f"- `{key}`: {all_row.get(key, '')}\n")
        handle.write("\n## Mandatory universe consistency check\n\n")
        handle.write(f"- `status`: {universe_consistency['status']}\n")
        for key, value in universe_consistency["expected"].items():
            handle.write(f"- `expected_{key}`: {value}\n")
        for key, value in universe_consistency["deltas"].items():
            handle.write(f"- `delta_{key}`: {value}\n")
        handle.write("\n## Segment files\n\n")
        for name in OUTPUT_NAMES[1:7]:
            handle.write(f"- `{name}`\n")
        handle.write("\n## Allowed verdicts\n\n")
        for allowed in ALLOWED_VERDICTS:
            handle.write(f"- `{allowed}`\n")

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_p2_edge_segmentation_audit",
        "research_only": True,
        "production_modifications": False,
        "live_trader_modifications": False,
        "strategy_promotion": False,
        "motif": MOTIF_NAME,
        "knowable_at": "P+2:pole->reversal->confirmation",
        "entry": ENTRY_CANDIDATE,
        "target_R": TARGET_R,
        "break_even_after_R": BREAK_EVEN_TRIGGER_R,
        "required_symbols": list(EXPECTED_SYMBOLS),
        "full_seven_market_universe": set(symbols) == set(EXPECTED_SYMBOLS),
        "symbols": symbols,
        "limit_rows_per_symbol": limit_rows_per_symbol,
        "aggregate": all_row,
        "universe_consistency": universe_consistency,
        "verdict": verdict,
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "segment_dimensions": {
            "pole_strength": ["pole_boxes", "pole_duration", "pole_velocity", "relative_pole_size"],
            "pullback_reversal_quality": ["reversal_boxes", "retrace_quality"],
            "market_regime": ["trend_regime", "current_pnf_direction", "choppiness", "breakout_context"],
            "symbol_market_family": ["symbol", "exchange", "market_family"],
            "direction": ["direction"],
            "time": ["year", "year_quarter"],
        },
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[8]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only P+2 causal edge segmentation audit")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--limit-rows-per-symbol", type=int, help="optional small-sample diagnostic cap after P+2 motif filtering")
    parser.add_argument("--allow-partial-universe", action="store_true", help="allow test/diagnostic runs outside the full BTC/ETH/SOL/ENA/HYPE/SUI/TAO universe")
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
