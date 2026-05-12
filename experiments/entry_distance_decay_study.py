#!/usr/bin/env python3
"""Research/export-only entry-distance decay study for PnF patterns.

Measures how realized structural-pattern outcomes change as the observed entry
price is farther from the original trigger/breakout level. This script only
reads the taxonomy CSV and candle database, then writes CSV/Markdown exports; it
does not modify strategy logic, pattern detection, execution, trader/live code,
or database contents.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Iterable

from pattern_outcome_analysis import (
    DEFAULT_MAX_BARS,
    DEFAULT_SETTINGS,
    OUTCOME_NO_EVENT,
    OUTCOME_STOP,
    OUTCOME_TP1,
    OUTCOME_TP2,
    OUTPUT_CONTEXT_FIELDS,
    R_MULTIPLE,
    SUPPORTED_PATTERNS,
    PatternSpec,
    classify_outcome,
    load_future_candles,
    load_settings,
    profile_box_size,
    rate,
    reference_close,
    resolve_repo_path,
    resolve_settings_relative_path,
    risk_proxy,
    safe_median,
)

DEFAULT_INPUT_CSV = "pnf_mvp/exports/pattern_taxonomy_audit_v2.csv"
DEFAULT_OUTPUT_CSV = "pnf_mvp/exports/entry_distance_decay_v1.csv"
DEFAULT_OUTPUT_MD = "pnf_mvp/exports/entry_distance_decay_v1.md"
BREAKDOWN_FIELDS = ["pattern", "side", "breakout_context", "trend_regime", "pattern_quality"]
ENTRY_PRICE_COLUMNS = ("entry_price", "entry", "close", "reference_price", "price")
PULLBACK_CONTEXT_TOKENS = ("PULLBACK", "RETEST")

CSV_FIELDS = [
    "pattern",
    "side",
    "breakout_context",
    "trend_regime",
    "pattern_quality",
    "entry_distance_bucket",
    "entry_style",
    "pullback_retest_detected",
    "trades",
    "avg_R_multiple",
    "median_R_multiple",
    "stop_rate",
    "tp1_rate",
    "tp2_rate",
    "tp1_tp2_rate",
    "no_event_rate",
    "median_bars_to_event",
    "median_bars_to_tp1",
    "median_bars_to_stop",
    "median_entry_distance_boxes",
    "avg_entry_distance_boxes",
    "immediate_breakout_avg_R_reference",
    "performance_drop_from_breakout",
    "edge_decay_score",
    "execution_survival_rate",
    "source_symbols",
]


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _to_int(value: Any) -> int | None:
    parsed = _to_float(value)
    return int(parsed) if parsed is not None else None


def _round(value: Any, digits: int = 6) -> Any:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return ""
        return round(value, digits)
    return "" if value is None else value


def _median_float(values: Iterable[float]) -> float | None:
    values_list = list(values)
    return float(median(values_list)) if values_list else None


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _present(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "(blank)"


def trigger_level(row: dict[str, Any], spec: PatternSpec) -> float | None:
    """Return the original breakout/breakdown trigger level for a pattern row."""
    for column in spec.level_columns:
        value = _to_float(row.get(column))
        if value is not None:
            return value
    return None


def observed_entry_price(row: dict[str, Any], conn: sqlite3.Connection, symbol: str, reference_ts: int) -> float | None:
    """Return the observed entry/chase price, falling back to candle close."""
    for column in ENTRY_PRICE_COLUMNS:
        value = _to_float(row.get(column))
        if value is not None:
            return value
    return reference_close(conn, symbol, reference_ts)


def distance_boxes(side: str, entry_price: float, breakout_level: float, box_size: float) -> float:
    if side == "LONG":
        return (entry_price - breakout_level) / box_size
    return (breakout_level - entry_price) / box_size


def distance_bucket(distance: float, exact_tolerance: float = 1e-9) -> str:
    if distance < -exact_tolerance:
        return "pullback/retest (<0 boxes)"
    if abs(distance) <= exact_tolerance:
        return "exact breakout (0)"
    if distance <= 1.0:
        return "0-1 boxes late"
    if distance <= 2.0:
        return "1-2 boxes late"
    if distance <= 3.0:
        return "2-3 boxes late"
    if distance <= 5.0:
        return "3-5 boxes late"
    return ">5 boxes late"


def is_pullback_retest(row: dict[str, Any], distance: float) -> bool:
    context = _present(row.get("breakout_context")).upper()
    return distance < 0.0 or any(token in context for token in PULLBACK_CONTEXT_TOKENS)


def entry_style(row: dict[str, Any], distance: float) -> str:
    if is_pullback_retest(row, distance):
        return "pullback/retest-style"
    if abs(distance) <= 1e-9:
        return "immediate breakout"
    if distance <= 2.0:
        return "slightly delayed"
    return "late"


def build_decay_trade_result(
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

    breakout_level = trigger_level(row, spec)
    if breakout_level is None:
        return None

    entry_price = observed_entry_price(row, conn, symbol, reference_ts)
    if entry_price is None:
        return None

    risk = risk_proxy(row, box_size)
    if spec.side == "LONG":
        stop = entry_price - risk
        tp1 = entry_price + (2.0 * risk)
        tp2 = entry_price + (3.0 * risk)
    else:
        stop = entry_price + risk
        tp1 = entry_price - (2.0 * risk)
        tp2 = entry_price - (3.0 * risk)

    candles = load_future_candles(conn, symbol, reference_ts, max_bars)
    outcome, bars_to_event, event_ts = classify_outcome(candles, spec.side, stop, tp1, tp2)
    dist = distance_boxes(spec.side, entry_price, breakout_level, box_size)
    result: dict[str, Any] = {
        "symbol": symbol,
        "reference_ts": reference_ts,
        "pattern": spec.name,
        "side": spec.side,
        "breakout_level": breakout_level,
        "entry_price": entry_price,
        "box_size": box_size,
        "entry_distance_boxes": dist,
        "entry_distance_bucket": distance_bucket(dist),
        "entry_style": entry_style(row, dist),
        "pullback_retest_detected": is_pullback_retest(row, dist),
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


def load_decay_trade_results(input_csv: Path, settings: dict[str, Any], db_path: Path, max_bars: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        source_rows = list(reader)

    available_specs = [spec for spec in SUPPORTED_PATTERNS.values() if spec.flag_column in fieldnames]
    if not available_specs:
        raise SystemExit("input CSV does not contain any supported pattern flag columns")

    trade_results: list[dict[str, Any]] = []
    skipped_by_pattern: dict[str, int] = defaultdict(int)
    conn = sqlite3.connect(str(db_path))
    try:
        for spec in available_specs:
            for row in source_rows:
                if str(row.get(spec.flag_column) or "").strip().lower() not in {"1", "1.0", "true", "yes"}:
                    continue
                trade_result = build_decay_trade_result(row=row, spec=spec, conn=conn, settings=settings, max_bars=max_bars)
                if trade_result is None:
                    skipped_by_pattern[spec.name] += 1
                    continue
                trade_results.append(trade_result)
    finally:
        conn.close()
    return trade_results, dict(skipped_by_pattern)


def group_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    counts = Counter(row["outcome"] for row in rows)
    r_values = [float(row["r_multiple"]) for row in rows]
    distances = [float(row["entry_distance_boxes"]) for row in rows]
    return {
        "trades": total,
        "avg_R_multiple": (sum(r_values) / total) if total else 0.0,
        "median_R_multiple": _median_float(r_values),
        "stop_rate": rate(counts[OUTCOME_STOP], total),
        "tp1_rate": rate(counts[OUTCOME_TP1], total),
        "tp2_rate": rate(counts[OUTCOME_TP2], total),
        "tp1_tp2_rate": rate(counts[OUTCOME_TP1] + counts[OUTCOME_TP2], total),
        "no_event_rate": rate(counts[OUTCOME_NO_EVENT], total),
        "median_bars_to_event": safe_median(int(row["bars_to_event"]) for row in rows if row.get("bars_to_event") is not None),
        "median_bars_to_tp1": safe_median(int(row["bars_to_event"]) for row in rows if row["outcome"] == OUTCOME_TP1),
        "median_bars_to_stop": safe_median(int(row["bars_to_event"]) for row in rows if row["outcome"] == OUTCOME_STOP),
        "median_entry_distance_boxes": _median_float(distances),
        "avg_entry_distance_boxes": (sum(distances) / total) if total else 0.0,
        "execution_survival_rate": 1.0 - rate(counts[OUTCOME_STOP], total),
        "source_symbols": dict(sorted(Counter(str(row["symbol"]) for row in rows).items())),
    }


def immediate_reference_map(trade_results: list[dict[str, Any]]) -> tuple[dict[tuple[str, ...], float], float | None]:
    references: dict[tuple[str, ...], float] = {}
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    global_immediate: list[dict[str, Any]] = []
    for row in trade_results:
        if row["entry_distance_bucket"] != "exact breakout (0)":
            continue
        key = tuple(_present(row.get(field)) for field in BREAKDOWN_FIELDS)
        grouped[key].append(row)
        global_immediate.append(row)
    for key, rows in grouped.items():
        references[key] = float(group_summary(rows)["avg_R_multiple"])
    global_ref = float(group_summary(global_immediate)["avg_R_multiple"]) if global_immediate else None
    return references, global_ref


def summarize_decay(trade_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    immediate_refs, global_immediate_ref = immediate_reference_map(trade_results)
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in trade_results:
        key = tuple(_present(row.get(field)) for field in BREAKDOWN_FIELDS) + (
            str(row["entry_distance_bucket"]),
            str(row["entry_style"]),
            str(bool(row["pullback_retest_detected"])),
        )
        grouped[key].append(row)

    output_rows: list[dict[str, Any]] = []
    for key, rows in grouped.items():
        base_key = key[: len(BREAKDOWN_FIELDS)]
        summary = group_summary(rows)
        reference = immediate_refs.get(base_key, global_immediate_ref)
        avg_r = float(summary["avg_R_multiple"])
        drop = None if reference is None else float(reference) - avg_r
        if reference is None:
            decay_score = None
        else:
            decay_score = _clamp(1.0 - max(0.0, drop or 0.0) / max(1.0, abs(float(reference))))
        record = {
            "pattern": key[0],
            "side": key[1],
            "breakout_context": key[2],
            "trend_regime": key[3],
            "pattern_quality": key[4],
            "entry_distance_bucket": key[5],
            "entry_style": key[6],
            "pullback_retest_detected": key[7],
            "immediate_breakout_avg_R_reference": reference,
            "performance_drop_from_breakout": drop,
            "edge_decay_score": decay_score,
            **summary,
        }
        output_rows.append(record)

    bucket_order = {
        "pullback/retest (<0 boxes)": 0,
        "exact breakout (0)": 1,
        "0-1 boxes late": 2,
        "1-2 boxes late": 3,
        "2-3 boxes late": 4,
        "3-5 boxes late": 5,
        ">5 boxes late": 6,
    }
    output_rows.sort(
        key=lambda row: (
            str(row["pattern"]),
            str(row["side"]),
            str(row["breakout_context"]),
            str(row["trend_regime"]),
            str(row["pattern_quality"]),
            bucket_order.get(str(row["entry_distance_bucket"]), 99),
            -int(row["trades"]),
        )
    )
    return output_rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            output = {field: _round(row.get(field)) for field in CSV_FIELDS}
            output["source_symbols"] = json.dumps(row.get("source_symbols", {}), sort_keys=True)
            writer.writerow(output)


def aggregate_by(trade_results: list[dict[str, Any]], fields: tuple[str, ...]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in trade_results:
        grouped[tuple(_present(row.get(field)) for field in fields)].append(row)
    rows = []
    for key, group_rows in grouped.items():
        rows.append({field: key[idx] for idx, field in enumerate(fields)} | group_summary(group_rows))
    rows.sort(key=lambda row: (float(row["avg_R_multiple"]), int(row["trades"])), reverse=True)
    return rows


def md_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]], limit: int = 10) -> str:
    if not rows:
        return "- None."
    lines = ["| " + " | ".join(label for label, _field in columns) + " |", "|" + "|".join(["---"] * len(columns)) + "|"]
    for row in rows[:limit]:
        values = []
        for _label, field in columns:
            value = row.get(field)
            if isinstance(value, float):
                if field.endswith("rate") or field == "execution_survival_rate":
                    values.append(f"{value:.1%}")
                else:
                    values.append(f"{value:.3f}")
            elif value is None:
                values.append("")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def pattern_decay_profiles(trade_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    style_rows = aggregate_by(trade_results, ("pattern", "side", "entry_style"))
    by_pattern: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in style_rows:
        by_pattern[(str(row["pattern"]), str(row["side"]))][str(row["entry_style"])] = row

    profiles: list[dict[str, Any]] = []
    for (pattern, side), styles in by_pattern.items():
        immediate = styles.get("immediate breakout")
        delayed = styles.get("slightly delayed")
        late = styles.get("late")
        pullback = styles.get("pullback/retest-style")
        reference = immediate or delayed or pullback or late
        ref_avg = float(reference["avg_R_multiple"]) if reference else 0.0
        delayed_avg = float(delayed["avg_R_multiple"]) if delayed else None
        late_avg = float(late["avg_R_multiple"]) if late else None
        pullback_avg = float(pullback["avg_R_multiple"]) if pullback else None
        steep_drop = 0.0 if delayed_avg is None else ref_avg - delayed_avg
        late_drop = 0.0 if late_avg is None else ref_avg - late_avg
        total_trades = sum(int(row["trades"]) for row in styles.values())
        profiles.append(
            {
                "pattern": pattern,
                "side": side,
                "total_trades": total_trades,
                "immediate_avg_R": None if immediate is None else float(immediate["avg_R_multiple"]),
                "delayed_avg_R": delayed_avg,
                "late_avg_R": late_avg,
                "pullback_avg_R": pullback_avg,
                "drop_after_1_2_boxes": steep_drop,
                "late_drop": late_drop,
                "execution_survival_rate": sum(float(row["execution_survival_rate"]) * int(row["trades"]) for row in styles.values()) / max(1, total_trades),
            }
        )
    return profiles


def _fmt_optional(value: Any, digits: int = 3) -> str:
    if value is None or value == "":
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def triangle_vs_double_note(profiles: list[dict[str, Any]]) -> str:
    triangles = [row for row in profiles if "triangle" in str(row["pattern"])]
    doubles = [row for row in profiles if "double" in str(row["pattern"])]
    if not triangles or not doubles:
        return "Insufficient triangle/double overlap to determine relative decay speed."
    tri_drop = sum(float(row["late_drop"]) for row in triangles) / len(triangles)
    dbl_drop = sum(float(row["late_drop"]) for row in doubles) / len(doubles)
    faster = "triangles" if tri_drop > dbl_drop else "doubles"
    return f"Average late-entry drop is {tri_drop:.3f}R for triangles versus {dbl_drop:.3f}R for doubles; in this export, {faster} decay faster."


def pullback_vs_chasing_note(trade_results: list[dict[str, Any]]) -> str:
    styles = {row["entry_style"]: row for row in aggregate_by(trade_results, ("entry_style",))}
    pullback = styles.get("pullback/retest-style")
    chasing_rows = [row for style, row in styles.items() if style in {"slightly delayed", "late"}]
    if pullback is None or not chasing_rows:
        return "Insufficient pullback/retest-style and delayed/late chasing overlap for a direct comparison."
    chase_trades = sum(int(row["trades"]) for row in chasing_rows)
    chase_avg = sum(float(row["avg_R_multiple"]) * int(row["trades"]) for row in chasing_rows) / max(1, chase_trades)
    leader = "pullback/retest-style entries" if float(pullback["avg_R_multiple"]) > chase_avg else "breakout-chasing entries"
    return f"Pullback/retest-style avg_R={float(pullback['avg_R_multiple']):.3f} versus delayed/late chasing avg_R={chase_avg:.3f}; {leader} lead on this sample."


def write_markdown(
    path: Path,
    *,
    trade_results: list[dict[str, Any]],
    decay_rows: list[dict[str, Any]],
    skipped_by_pattern: dict[str, int],
    input_csv: Path,
    db_path: Path,
    max_bars: int,
) -> None:
    global_summary = group_summary(trade_results)
    non_ambiguous = [row for row in trade_results if row["outcome"] != OUTCOME_NO_EVENT]
    non_ambiguous_wins = sum(1 for row in non_ambiguous if row["outcome"] in {OUTCOME_TP1, OUTCOME_TP2})
    tp1_tp2_count = sum(1 for row in trade_results if row["outcome"] in {OUTCOME_TP1, OUTCOME_TP2})
    tp2_count = sum(1 for row in trade_results if row["outcome"] == OUTCOME_TP2)
    style_summary = aggregate_by(trade_results, ("entry_style",))
    bucket_summary = aggregate_by(trade_results, ("entry_distance_bucket",))
    pattern_bucket_summary = aggregate_by(trade_results, ("pattern", "side", "entry_distance_bucket"))
    profiles = pattern_decay_profiles(trade_results)

    immediate = [row for row in pattern_bucket_summary if row["entry_distance_bucket"] == "exact breakout (0)"]
    delayed = [row for row in pattern_bucket_summary if row["entry_distance_bucket"] in {"0-1 boxes late", "1-2 boxes late"}]
    steep = sorted(profiles, key=lambda row: float(row["drop_after_1_2_boxes"]), reverse=True)
    stable = sorted(profiles, key=lambda row: (abs(float(row["late_drop"])), -int(row["total_trades"])))
    fragile = sorted(profiles, key=lambda row: float(row["execution_survival_rate"]))
    robust_live = [
        row
        for row in profiles
        if int(row["total_trades"]) >= 30
        and float(row["execution_survival_rate"]) >= 0.55
        and (row.get("immediate_avg_R") is not None or row.get("delayed_avg_R") is not None)
        and max(float(row.get("immediate_avg_R") or -999), float(row.get("delayed_avg_R") or -999), float(row.get("pullback_avg_R") or -999)) > 0.0
    ]
    robust_live.sort(key=lambda row: (float(row["execution_survival_rate"]), int(row["total_trades"])), reverse=True)

    profile_columns = [
        ("Pattern", "pattern"),
        ("Side", "side"),
        ("Trades", "total_trades"),
        ("Immediate R", "immediate_avg_R"),
        ("Delayed R", "delayed_avg_R"),
        ("Late R", "late_avg_R"),
        ("Pullback R", "pullback_avg_R"),
        ("1-2 box drop", "drop_after_1_2_boxes"),
        ("Survival", "execution_survival_rate"),
    ]
    perf_columns = [
        ("Pattern", "pattern"),
        ("Side", "side"),
        ("Bucket", "entry_distance_bucket"),
        ("Trades", "trades"),
        ("Avg R", "avg_R_multiple"),
        ("Median R", "median_R_multiple"),
        ("Stop", "stop_rate"),
        ("TP1+TP2", "tp1_tp2_rate"),
        ("No event", "no_event_rate"),
    ]

    lines = [
        "# Entry Distance Decay Study v1",
        "",
        "Research/export-only study of how structural PnF pattern edge changes as the observed entry price moves away from the original breakout/breakdown trigger level. Strategy logic, pattern detection, execution, trader/live code, and databases are not modified.",
        "",
        "## Inputs",
        f"- Pattern taxonomy CSV: `{input_csv}`",
        f"- Candle database: `{db_path}`",
        f"- Max future bars per detected setup: `{max_bars}`",
        "",
        "## Required experiment scorecard",
        f"- `candidate_rows_registered`: {len(trade_results)}",
        f"- `resolved_rows`: {len(non_ambiguous)}",
        f"- `win_rate_non_ambiguous`: {rate(non_ambiguous_wins, len(non_ambiguous)):.4f}",
        f"- `avg_realized_r_multiple`: {float(global_summary['avg_R_multiple']):.4f}",
        f"- `total_realized_r_multiple`: {sum(float(row['r_multiple']) for row in trade_results):.4f}",
        f"- `TP1 -> TP2 conversion`: {rate(tp2_count, tp1_tp2_count):.4f}",
        "",
        "## Method",
        "- LONG distance: `(entry_price - breakout_level) / box_size`.",
        "- SHORT distance: `(breakout_level - entry_price) / box_size`.",
        "- `entry_price` uses an explicit entry/close/reference-price column when present, otherwise the 1m candle close at or before `reference_ts`.",
        "- Outcomes reuse the existing structural outcome framework with the observed entry price and the existing risk proxy; this remains an export-only research calculation.",
        "- `edge_decay_score` is 1.0 when a bucket preserves the matching immediate-breakout average R and declines toward 0 as performance drops.",
        "- `execution_survival_rate` is `1 - stop_rate`.",
        "",
        "## Entry timing comparison",
        md_table(style_summary, [("Style", "entry_style"), ("Trades", "trades"), ("Avg R", "avg_R_multiple"), ("Stop", "stop_rate"), ("TP1+TP2", "tp1_tp2_rate"), ("No event", "no_event_rate"), ("Survival", "execution_survival_rate")], 20),
        "",
        "## Distance bucket summary",
        md_table(bucket_summary, [("Bucket", "entry_distance_bucket"), ("Trades", "trades"), ("Avg R", "avg_R_multiple"), ("Median R", "median_R_multiple"), ("Stop", "stop_rate"), ("TP1", "tp1_rate"), ("TP2", "tp2_rate"), ("TP1+TP2", "tp1_tp2_rate"), ("No event", "no_event_rate")], 20),
        "",
        "## Best immediate-entry patterns",
        md_table(immediate, perf_columns, 12),
        "",
        "## Best delayed-entry patterns",
        md_table(delayed, perf_columns, 12),
        "",
        "## Patterns with steep edge decay",
        md_table(steep, profile_columns, 12),
        "",
        "## Patterns with stable edge",
        md_table(stable, profile_columns, 12),
        "",
        "## Which patterns are highly timing-sensitive?",
        md_table(steep, profile_columns, 10),
        "",
        "## Which patterns retain edge even when late?",
        md_table([row for row in stable if row.get("late_avg_R") is not None and float(row["late_avg_R"]) > 0.0], profile_columns, 10),
        "",
        "## Which patterns collapse after 1-2 boxes?",
        md_table([row for row in steep if float(row["drop_after_1_2_boxes"]) > 0.0], profile_columns, 10),
        "",
        "## Which patterns are most execution-fragile?",
        md_table(fragile, profile_columns, 10),
        "",
        "## Which patterns are robust enough for live trading?",
        md_table(robust_live, profile_columns, 10),
        "",
        "## Triangles vs doubles",
        triangle_vs_double_note(profiles),
        "",
        "## Pullback/retest-style entries vs breakout chasing",
        pullback_vs_chasing_note(trade_results),
        "",
        "## Practical live execution implications",
        "- Prefer patterns whose immediate and slightly delayed rows both keep positive average R and acceptable stop rate.",
        "- Treat patterns with a positive immediate R but sharply negative delayed/late R as execution-fragile; these require fast alerting and strict no-chase rules.",
        "- If pullback/retest-style rows outperform delayed/late chasing rows, the practical implication is to wait for retests instead of chasing extended moves.",
        "- Late buckets with high no-event rates imply capital/time drag even when stop rate is not extreme.",
        "",
        "## Skipped rows",
        "Rows can be skipped when required symbol, timestamp, box-size, trigger level, entry price, or candle data are unavailable.",
    ]
    if skipped_by_pattern:
        lines.extend(f"- `{pattern}`: {count}" for pattern, count in sorted(skipped_by_pattern.items()))
    else:
        lines.append("- None.")
    lines.extend([
        "",
        "## CSV schema note",
        f"The detailed grouped export is written to `{DEFAULT_OUTPUT_CSV}` and contains {len(decay_rows)} rows grouped by pattern, side, breakout context, trend regime, pattern quality, distance bucket, and entry style.",
        "",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research/export-only PnF entry distance decay study")
    parser.add_argument("--input-csv", default=DEFAULT_INPUT_CSV)
    parser.add_argument("--settings", default=DEFAULT_SETTINGS)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-csv", default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--output-md", default=DEFAULT_OUTPUT_MD)
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

    trade_results, skipped_by_pattern = load_decay_trade_results(input_csv, settings, db_path, args.max_bars)
    decay_rows = summarize_decay(trade_results)

    output_csv = resolve_repo_path(args.output_csv)
    output_md = resolve_repo_path(args.output_md)
    write_csv(output_csv, decay_rows)
    write_markdown(
        output_md,
        trade_results=trade_results,
        decay_rows=decay_rows,
        skipped_by_pattern=skipped_by_pattern,
        input_csv=input_csv,
        db_path=db_path,
        max_bars=args.max_bars,
    )

    print("=== Entry Distance Decay Study v1 ===")
    print(f"input_csv={input_csv}")
    print(f"db_path={db_path}")
    print(f"trade_results={len(trade_results)}")
    print(f"grouped_rows={len(decay_rows)}")
    print(f"output_csv={output_csv}")
    print(f"output_md={output_md}")


if __name__ == "__main__":
    main()
