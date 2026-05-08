#!/usr/bin/env python3
"""Unified structural PnF pattern outcome analyzer.

Analysis-only utility for comparing structural pattern outcomes emitted by
``experiments/shadow_research_scanner.py``. This script does not modify scanner,
validation, detector, or strategy behavior.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_CSV = "pnf_mvp/exports/core_patterns_v1.csv"
DEFAULT_SETTINGS = "pnf_mvp/settings.research_clean.json"
DEFAULT_MAX_BARS = 5000

OUTCOME_STOP = "STOP"
OUTCOME_TP1 = "TP1"
OUTCOME_TP2 = "TP2"
OUTCOME_NO_EVENT = "NO_EVENT"

R_MULTIPLE = {
    OUTCOME_STOP: -1.0,
    OUTCOME_TP1: 2.0,
    OUTCOME_TP2: 3.0,
    OUTCOME_NO_EVENT: 0.0,
}

CONTEXT_FIELDS = [
    "trend_regime",
    "breakout_context",
    "pattern_quality",
    "pattern_width_columns",
]

OUTPUT_CONTEXT_FIELDS = [
    "trend_state",
    "trend_regime",
    "immediate_slope",
    "breakout_context",
    "market_state",
    "latest_signal_name",
    "is_extended_move",
    "active_leg_boxes",
    "pattern_quality",
    "catapult_pattern_quality",
    "pattern_width_columns",
    "triple_pattern_width_columns",
    "pattern_support_level",
    "pattern_resistance_level",
    "triple_top_resistance_level",
    "triple_bottom_support_level",
    "catapult_support_level",
    "pattern_break_distance_boxes",
    "breakout_distance_boxes",
    "breakdown_distance_boxes",
    "catapult_break_distance_boxes",
    "prior_test_count",
    "pattern_compaction_hint",
    "breakout_column_height_boxes",
    "pattern_is_compact_preferred",
    "pattern_is_broad_warning",
    "early_trend_candidate_flag",
]


@dataclass(frozen=True)
class PatternSpec:
    name: str
    flag_column: str
    side: str
    level_columns: tuple[str, ...]


SUPPORTED_PATTERNS: dict[str, PatternSpec] = {
    "double_top": PatternSpec("double_top", "shadow_double_top_breakout", "LONG", ("pattern_resistance_level",)),
    "double_bottom": PatternSpec("double_bottom", "shadow_double_bottom_breakdown", "SHORT", ("pattern_support_level",)),
    "triple_top": PatternSpec(
        "triple_top", "shadow_triple_top_breakout", "LONG", ("triple_top_resistance_level", "pattern_resistance_level")
    ),
    "triple_bottom": PatternSpec(
        "triple_bottom", "shadow_triple_bottom_breakdown", "SHORT", ("triple_bottom_support_level", "pattern_support_level")
    ),
    "bullish_catapult": PatternSpec("bullish_catapult", "shadow_bullish_catapult", "LONG", ("pattern_resistance_level",)),
    "bearish_catapult": PatternSpec(
        "bearish_catapult", "shadow_bearish_catapult", "SHORT", ("catapult_support_level", "pattern_support_level")
    ),
    "bullish_triangle": PatternSpec(
        "bullish_triangle", "shadow_bullish_triangle", "LONG", ("pattern_resistance_level", "pattern_support_level")
    ),
    "bearish_triangle": PatternSpec(
        "bearish_triangle", "shadow_bearish_triangle", "SHORT", ("pattern_support_level", "pattern_resistance_level")
    ),
    "bullish_signal_reversal": PatternSpec(
        "bullish_signal_reversal", "shadow_bullish_signal_reversal", "LONG", ("pattern_resistance_level", "pattern_support_level")
    ),
    "bearish_signal_reversal": PatternSpec(
        "bearish_signal_reversal", "shadow_bearish_signal_reversal", "SHORT", ("pattern_support_level", "pattern_resistance_level")
    ),
    "shakeout": PatternSpec("shakeout", "shadow_shakeout", "LONG", ("pattern_resistance_level", "pattern_support_level")),
}


@dataclass(frozen=True)
class Candle:
    close_time: int
    high: float
    low: float
    close: float


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def resolve_settings_relative_path(settings_path: Path, path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    return (settings_path.parent / path).resolve()


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def safe_median(values: Iterable[int]) -> float | None:
    values_list = list(values)
    return float(median(values_list)) if values_list else None


def load_settings(settings_path: Path) -> dict[str, Any]:
    with settings_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def symbol_variants(symbol: str) -> list[str]:
    variants = [symbol]
    if ":" in symbol:
        base = symbol.split(":", 1)[1]
        variants.append(base)
    else:
        base = symbol
        variants.append(f"BINANCE_FUT:{symbol}")
    if base.upper().endswith("USDT"):
        variants.append(base[:-4])
    else:
        variants.append(f"{base}USDT")
        variants.append(f"BINANCE_FUT:{base}USDT")
    # Preserve order while de-duplicating.
    return list(dict.fromkeys(variants))


def profile_box_size(settings: dict[str, Any], symbol: str) -> float | None:
    profiles = settings.get("profiles") or {}
    for variant in symbol_variants(symbol):
        profile = profiles.get(variant)
        if isinstance(profile, dict):
            box_size = _to_float(profile.get("box_size"))
            if box_size is not None and box_size > 0:
                return box_size
    return None


def symbol_matches_filter(symbol: str, symbol_filter: str | None) -> bool:
    if not symbol_filter:
        return True
    symbol_values = {item.upper() for item in symbol_variants(symbol)}
    filter_values = {item.upper() for item in symbol_variants(symbol_filter)}
    return bool(symbol_values & filter_values)


def load_pattern_rows(csv_path: Path, spec: PatternSpec, symbol_filter: str | None) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    total_rows = 0
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if spec.flag_column not in (reader.fieldnames or []):
            raise SystemExit(f"input CSV is missing selected pattern flag column: {spec.flag_column}")
        for row in reader:
            total_rows += 1
            if _to_int(row.get(spec.flag_column)) != 1:
                continue
            symbol = str(row.get("symbol") or "").strip()
            if not symbol_matches_filter(symbol, symbol_filter):
                continue
            rows.append(row)
    return rows, total_rows


def db_symbol_for(conn: sqlite3.Connection, symbol: str) -> str:
    for variant in symbol_variants(symbol):
        found = conn.execute("SELECT 1 FROM candles WHERE symbol = ? LIMIT 1", (variant,)).fetchone()
        if found is not None:
            return variant
    return symbol


def reference_close(conn: sqlite3.Connection, symbol: str, reference_ts: int) -> float | None:
    db_symbol = db_symbol_for(conn, symbol)
    row = conn.execute(
        """
        SELECT close
        FROM candles
        WHERE symbol = ?
          AND interval = '1m'
          AND close_time <= ?
        ORDER BY close_time DESC
        LIMIT 1
        """,
        (db_symbol, reference_ts),
    ).fetchone()
    return float(row[0]) if row is not None else None


def load_future_candles(conn: sqlite3.Connection, symbol: str, reference_ts: int, max_bars: int) -> list[Candle]:
    db_symbol = db_symbol_for(conn, symbol)
    rows = conn.execute(
        """
        SELECT close_time, high, low, close
        FROM candles
        WHERE symbol = ?
          AND interval = '1m'
          AND close_time > ?
        ORDER BY close_time ASC
        LIMIT ?
        """,
        (db_symbol, reference_ts, max_bars),
    ).fetchall()
    return [Candle(int(ts), float(high), float(low), float(close)) for ts, high, low, close in rows]


def structural_entry(row: dict[str, Any], spec: PatternSpec, conn: sqlite3.Connection, symbol: str, reference_ts: int) -> float | None:
    for column in spec.level_columns:
        value = _to_float(row.get(column))
        if value is not None:
            return value
    for column in ("close", "price", "reference_price"):
        value = _to_float(row.get(column))
        if value is not None:
            return value
    return reference_close(conn, symbol, reference_ts)


def risk_proxy(row: dict[str, Any], box_size: float) -> float:
    for column in ("breakout_distance_boxes", "breakdown_distance_boxes", "catapult_break_distance_boxes"):
        boxes = _to_float(row.get(column))
        if boxes is not None and boxes > 0:
            return max(boxes, 1.0) * box_size
    return box_size


def classify_outcome(candles: list[Candle], side: str, stop: float, tp1: float, tp2: float) -> tuple[str, int | None, int | None]:
    for idx, candle in enumerate(candles, start=1):
        if side == "LONG":
            hit_stop = candle.low <= stop
            hit_tp2 = candle.high >= tp2
            hit_tp1 = candle.high >= tp1
        else:
            hit_stop = candle.high >= stop
            hit_tp2 = candle.low <= tp2
            hit_tp1 = candle.low <= tp1

        # Deterministic priority: stop first, tp2 second, tp1 third.
        if hit_stop:
            return OUTCOME_STOP, idx, candle.close_time
        if hit_tp2:
            return OUTCOME_TP2, idx, candle.close_time
        if hit_tp1:
            return OUTCOME_TP1, idx, candle.close_time

    return OUTCOME_NO_EVENT, None, None


def build_trade_result(
    *,
    row: dict[str, Any],
    spec: PatternSpec,
    conn: sqlite3.Connection,
    settings: dict[str, Any],
    max_bars: int,
) -> dict[str, Any] | None:
    symbol = str(row.get("symbol") or "").strip()
    reference_ts = _to_int(row.get("reference_ts"))
    if not symbol or reference_ts is None:
        return None

    box_size = profile_box_size(settings, symbol)
    if box_size is None:
        return None

    entry = structural_entry(row, spec, conn, symbol, reference_ts)
    if entry is None:
        return None

    risk = risk_proxy(row, box_size)
    if spec.side == "LONG":
        stop = entry - risk
        tp1 = entry + (2.0 * risk)
        tp2 = entry + (3.0 * risk)
    else:
        stop = entry + risk
        tp1 = entry - (2.0 * risk)
        tp2 = entry - (3.0 * risk)

    candles = load_future_candles(conn, symbol, reference_ts, max_bars)
    outcome, bars_to_event, event_ts = classify_outcome(candles, spec.side, stop, tp1, tp2)
    result: dict[str, Any] = {
        "symbol": symbol,
        "reference_ts": reference_ts,
        "pattern": spec.name,
        "side": spec.side,
        "entry": entry,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "outcome": outcome,
        "bars_to_event": bars_to_event,
        "event_ts": event_ts,
        "r_multiple": R_MULTIPLE[outcome],
    }
    for field in OUTPUT_CONTEXT_FIELDS:
        if field in row:
            result[field] = row.get(field)
    return result


def rate(count: int, total: int) -> float:
    return float(count) / float(total) if total else 0.0


def timing_bucket(bars_to_event: int | None) -> str:
    if bars_to_event is None:
        return "NO_EVENT"
    if bars_to_event <= 50:
        return "<=50"
    if bars_to_event <= 200:
        return "51-200"
    if bars_to_event <= 500:
        return "201-500"
    return "500+"


def print_kv_table(title: str, rows: list[tuple[str, Any]]) -> None:
    print(f"\n{title}")
    width = max([len(key) for key, _value in rows] + [3])
    for key, value in rows:
        print(f"{key.ljust(width)}  {_fmt(value)}")


def print_table(title: str, headers: list[str], rows: list[list[Any]]) -> None:
    print(f"\n{title}")
    str_rows = [[_fmt(cell) for cell in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    print(" | ".join(header.ljust(widths[i]) for i, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in str_rows:
        print(" | ".join(row[i].ljust(widths[i]) for i in range(len(headers))))


def summarize_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    stop_count = sum(1 for row in rows if row["outcome"] == OUTCOME_STOP)
    tp1_count = sum(1 for row in rows if row["outcome"] == OUTCOME_TP1)
    tp2_count = sum(1 for row in rows if row["outcome"] == OUTCOME_TP2)
    return {
        "trades": total,
        "avg_R": (sum(float(row["r_multiple"]) for row in rows) / total) if total else 0.0,
        "tp1_rate": rate(tp1_count, total),
        "tp2_rate": rate(tp2_count, total),
        "stop_rate": rate(stop_count, total),
        "median_bars_to_event": safe_median(int(row["bars_to_event"]) for row in rows if row.get("bars_to_event") is not None),
    }


def write_output_csv(output_path: Path, rows: list[dict[str, Any]]) -> None:
    base_fields = [
        "symbol",
        "reference_ts",
        "pattern",
        "side",
        "entry",
        "stop",
        "tp1",
        "tp2",
        "outcome",
        "bars_to_event",
        "event_ts",
        "r_multiple",
    ]
    extra_fields = [field for field in OUTPUT_CONTEXT_FIELDS if any(field in row for row in rows)]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=base_fields + extra_fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Unified structural PnF pattern outcome analyzer (analysis-only)")
    parser.add_argument("--input-csv", default=DEFAULT_INPUT_CSV)
    parser.add_argument("--settings", default=DEFAULT_SETTINGS)
    parser.add_argument("--pattern", required=True, choices=sorted(SUPPORTED_PATTERNS))
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--max-bars", type=int, default=DEFAULT_MAX_BARS)
    args = parser.parse_args()
    if args.max_bars <= 0:
        raise SystemExit("--max-bars must be positive")

    input_csv = resolve_repo_path(args.input_csv)
    settings_path = resolve_repo_path(args.settings)
    if not input_csv.exists():
        raise SystemExit(f"input CSV not found: {input_csv}")
    if not settings_path.exists():
        raise SystemExit(f"settings file not found: {settings_path}")
    settings = load_settings(settings_path)
    db_path = resolve_repo_path(args.db_path) if args.db_path else resolve_settings_relative_path(settings_path, settings["database_path"])
    if not db_path.exists():
        raise SystemExit(f"database file not found: {db_path}")
    spec = SUPPORTED_PATTERNS[args.pattern]

    rows, total_input_rows = load_pattern_rows(input_csv, spec, args.symbol)
    trade_results: list[dict[str, Any]] = []
    skipped_rows = 0
    conn = sqlite3.connect(str(db_path))
    try:
        for row in rows:
            trade_result = build_trade_result(row=row, spec=spec, conn=conn, settings=settings, max_bars=args.max_bars)
            if trade_result is None:
                skipped_rows += 1
                continue
            trade_results.append(trade_result)
    finally:
        conn.close()

    total = len(trade_results)
    counts = Counter(row["outcome"] for row in trade_results)
    avg_r = sum(float(row["r_multiple"]) for row in trade_results) / total if total else 0.0

    print("=== Unified Structural Pattern Outcome Analyzer ===")
    print(f"input_csv={input_csv}")
    print(f"settings={settings_path}")
    print(f"db_path={db_path}")
    print(f"pattern={spec.name}")
    print(f"side={spec.side}")
    print(f"symbol_filter={args.symbol or 'ALL'}")
    print(f"max_bars={args.max_bars}")
    print(f"input_rows={total_input_rows}")
    print(f"selected_pattern_rows={len(rows)}")
    print(f"skipped_rows={skipped_rows}")

    global_rows = [
        ("total_trades", total),
        ("stop_count", counts[OUTCOME_STOP]),
        ("tp1_count", counts[OUTCOME_TP1]),
        ("tp2_count", counts[OUTCOME_TP2]),
        ("no_event_count", counts[OUTCOME_NO_EVENT]),
        ("stop_rate", rate(counts[OUTCOME_STOP], total)),
        ("tp1_rate", rate(counts[OUTCOME_TP1], total)),
        ("tp2_rate", rate(counts[OUTCOME_TP2], total)),
        ("avg_R_multiple", avg_r),
        ("median_bars_to_event", safe_median(int(row["bars_to_event"]) for row in trade_results if row.get("bars_to_event") is not None)),
        ("median_bars_to_tp1", safe_median(int(row["bars_to_event"]) for row in trade_results if row["outcome"] == OUTCOME_TP1)),
        ("median_bars_to_stop", safe_median(int(row["bars_to_event"]) for row in trade_results if row["outcome"] == OUTCOME_STOP)),
    ]
    print_kv_table("GLOBAL", global_rows)

    bucket_counts = Counter(timing_bucket(row.get("bars_to_event")) for row in trade_results)
    print_table(
        "Timing buckets",
        ["bucket", "trades", "rate"],
        [[bucket, bucket_counts[bucket], rate(bucket_counts[bucket], total)] for bucket in ("<=50", "51-200", "201-500", "500+", "NO_EVENT")],
    )

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trade_results:
        by_symbol[str(row["symbol"])].append(row)
    symbol_rows = []
    for symbol, group_rows in sorted(by_symbol.items()):
        summary = summarize_group(group_rows)
        symbol_rows.append(
            [
                symbol,
                summary["trades"],
                summary["avg_R"],
                summary["tp1_rate"],
                summary["tp2_rate"],
                summary["stop_rate"],
                summary["median_bars_to_event"],
            ]
        )
    print_table(
        "Symbol breakdown",
        ["symbol", "trades", "avg_R", "tp1_rate", "tp2_rate", "stop_rate", "median_bars_to_event"],
        symbol_rows,
    )

    for field in CONTEXT_FIELDS:
        if not any(field in row and str(row.get(field) or "").strip() for row in trade_results):
            continue
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in trade_results:
            value = str(row.get(field) or "").strip() or "(blank)"
            grouped[value].append(row)
        context_rows = []
        for value, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
            summary = summarize_group(group_rows)
            context_rows.append(
                [
                    value,
                    summary["trades"],
                    summary["avg_R"],
                    summary["tp1_rate"],
                    summary["tp2_rate"],
                    summary["stop_rate"],
                    summary["median_bars_to_event"],
                ]
            )
        print_table(
            f"Context breakdown: {field}",
            [field, "trades", "avg_R", "tp1_rate", "tp2_rate", "stop_rate", "median_bars_to_event"],
            context_rows,
        )

    if args.output_csv:
        output_path = resolve_repo_path(args.output_csv)
        write_output_csv(output_path, trade_results)
        print(f"\noutput_csv={output_path}")


if __name__ == "__main__":
    main()
