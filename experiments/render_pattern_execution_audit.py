#!/usr/bin/env python3
"""Render execution/outcome audits for structural pattern outcome rows.

This script is intentionally analysis/debug-only. It consumes CSV output from
``experiments/pattern_outcome_analysis.py``, reconstructs the surrounding and
forward PnF columns from candle closes, and writes static HTML pages that make
trade execution levels, first touches, and outcome ordering visible.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import math
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
PNF_MVP_DIR = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_DIR) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_DIR))

from pnf_engine import PnFColumn, PnFEngine, PnFProfile  # noqa: E402

DEFAULT_INPUT_CSV = "pnf_mvp/exports/pattern_outcomes.csv"
DEFAULT_SETTINGS = "pnf_mvp/settings.research_clean.json"
DEFAULT_OUTPUT_DIR = "pnf_mvp/exports/pattern_execution_audit"
DEFAULT_MAX_BARS = 5000
COLUMNS_BEFORE_TRIGGER = 10
FORWARD_COLUMNS_AFTER_TRIGGER = 24

OUTCOME_STOP = "STOP"
OUTCOME_TP1 = "TP1"
OUTCOME_TP2 = "TP2"
OUTCOME_NO_EVENT = "NO_EVENT"


@dataclass(frozen=True)
class PatternSpec:
    name: str
    default_side: str
    default_width_columns: int | None
    level_preference: tuple[str, ...]
    level_label: str


SUPPORTED_PATTERNS: dict[str, PatternSpec] = {
    "double_top": PatternSpec("double_top", "LONG", 3, ("pattern_resistance_level",), "resistance"),
    "double_bottom": PatternSpec("double_bottom", "SHORT", 3, ("pattern_support_level",), "support"),
    "triple_top": PatternSpec(
        "triple_top", "LONG", 5, ("triple_top_resistance_level", "pattern_resistance_level"), "resistance"
    ),
    "triple_bottom": PatternSpec(
        "triple_bottom", "SHORT", 5, ("triple_bottom_support_level", "pattern_support_level"), "support"
    ),
    "bullish_catapult": PatternSpec("bullish_catapult", "LONG", 7, ("pattern_resistance_level",), "resistance"),
    "bearish_catapult": PatternSpec(
        "bearish_catapult", "SHORT", 7, ("catapult_support_level", "pattern_support_level"), "support"
    ),
    "bullish_triangle": PatternSpec(
        "bullish_triangle", "LONG", 5, ("pattern_resistance_level", "pattern_support_level"), "pattern level"
    ),
    "bearish_triangle": PatternSpec(
        "bearish_triangle", "SHORT", 5, ("pattern_support_level", "pattern_resistance_level"), "pattern level"
    ),
    "bullish_signal_reversal": PatternSpec(
        "bullish_signal_reversal", "LONG", None, ("pattern_resistance_level", "pattern_support_level"), "pattern level"
    ),
    "bearish_signal_reversal": PatternSpec(
        "bearish_signal_reversal", "SHORT", None, ("pattern_support_level", "pattern_resistance_level"), "pattern level"
    ),
    "shakeout": PatternSpec("shakeout", "LONG", 5, ("pattern_resistance_level", "pattern_support_level"), "pattern level"),
}


@dataclass(frozen=True)
class Candle:
    close_time: int
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class ExecutionTouch:
    name: str
    bars: int
    ts: int
    price: float
    column_idx: int
    priority: int


@dataclass(frozen=True)
class ExecutionAuditRow:
    sample_number: int
    source_row_number: int
    row: dict[str, str]
    symbol: str
    reference_ts: int
    pattern: str
    side: str
    entry: float
    stop: float
    tp1: float
    tp2: float
    outcome: str
    r_multiple: float | None
    bars_to_event: int | None
    event_ts: int | None
    exit_reason: str
    pattern_width_columns: int | None
    pattern_quality: str | None
    highlighted_levels: tuple[tuple[str, float], ...]


@dataclass(frozen=True)
class ReconstructedExecution:
    trigger_idx: int
    columns: list[PnFColumn]
    candles_processed: int
    future_bars_processed: int
    box_size: float
    touches: dict[str, ExecutionTouch]
    exit_touch: ExecutionTouch | None
    sequence: str


@dataclass(frozen=True)
class LoadResult:
    rows: list[ExecutionAuditRow]
    total_input_rows: int
    rows_after_symbol_filter: int
    symbols: Counter[str]
    patterns: Counter[str]


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def resolve_settings_relative_path(settings_path: Path, path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    settings_relative = (settings_path.parent / path).resolve()
    if settings_relative.exists():
        return settings_relative
    return (REPO_ROOT / path).resolve()


def load_settings(settings_path: Path) -> dict[str, Any]:
    with settings_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def build_profiles(settings: dict[str, Any]) -> dict[str, PnFProfile]:
    profiles: dict[str, PnFProfile] = {}
    for symbol, profile_settings in (settings.get("profiles") or {}).items():
        profiles[symbol] = PnFProfile(
            name=symbol,
            box_size=float(profile_settings["box_size"]),
            reversal_boxes=int(profile_settings["reversal_boxes"]),
        )
    return profiles


def parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).strip()))
    except ValueError:
        return None


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(str(value).strip())
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def row_value(row: dict[str, Any], key: str) -> Any | None:
    if key in row:
        return row[key]
    for candidate_key, value in row.items():
        if str(candidate_key).strip().lstrip("\ufeff") == key:
            return value
    return None


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
    return list(dict.fromkeys(variants))


def symbol_matches(symbol: str, filters: set[str] | None) -> bool:
    if not filters:
        return True
    symbol_values = {item.upper() for item in symbol_variants(symbol)}
    for symbol_filter in filters:
        if symbol_values & {item.upper() for item in symbol_variants(symbol_filter)}:
            return True
    return False


def profile_for_symbol(profiles: dict[str, PnFProfile], symbol: str) -> PnFProfile | None:
    for variant in symbol_variants(symbol):
        profile = profiles.get(variant)
        if profile is not None:
            return profile
    return None


def db_symbol_for(conn: sqlite3.Connection, symbol: str) -> str:
    for variant in symbol_variants(symbol):
        found = conn.execute("SELECT 1 FROM candles WHERE symbol = ? LIMIT 1", (variant,)).fetchone()
        if found is not None:
            return variant
    return symbol


def infer_pattern_width(spec: PatternSpec, row: dict[str, Any]) -> int | None:
    for key in ("pattern_width_columns", "triple_pattern_width_columns"):
        value = parse_int(row_value(row, key))
        if value is not None and value > 0:
            return value
    return spec.default_width_columns


def collect_highlighted_levels(spec: PatternSpec, row: dict[str, Any]) -> tuple[tuple[str, float], ...]:
    levels: list[tuple[str, float]] = []
    seen: set[tuple[str, float]] = set()
    keys = list(spec.level_preference) + ["pattern_support_level", "pattern_resistance_level", "catapult_support_level"]
    for key in keys:
        value = parse_float(row_value(row, key))
        if value is None:
            continue
        item = (key, value)
        if item not in seen:
            levels.append(item)
            seen.add(item)
    return tuple(levels)


def first_float(row: dict[str, Any], keys: Iterable[str]) -> float | None:
    for key in keys:
        value = parse_float(row_value(row, key))
        if value is not None:
            return value
    return None


def first_int(row: dict[str, Any], keys: Iterable[str]) -> int | None:
    for key in keys:
        value = parse_int(row_value(row, key))
        if value is not None:
            return value
    return None


def normalize_outcome(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text or OUTCOME_NO_EVENT


def load_execution_rows(input_csv: Path, symbol_filter: set[str] | None, limit: int | None) -> LoadResult:
    rows: list[ExecutionAuditRow] = []
    total_input_rows = 0
    rows_after_symbol_filter = 0
    symbols: Counter[str] = Counter()
    patterns: Counter[str] = Counter()

    with input_csv.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"symbol", "reference_ts", "pattern", "entry", "stop", "tp1", "tp2"}
        fieldnames = {name.strip().lstrip("\ufeff") for name in (reader.fieldnames or [])}
        missing = sorted(required - fieldnames)
        if missing:
            raise ValueError(f"Input outcome CSV is missing required column(s): {', '.join(missing)}")

        for source_row_number, row in enumerate(reader, start=2):
            total_input_rows += 1
            symbol = str(row_value(row, "symbol") or "").strip()
            pattern = str(row_value(row, "pattern") or "").strip()
            if symbol:
                symbols[symbol] += 1
            if pattern:
                patterns[pattern] += 1
            if not symbol or not symbol_matches(symbol, symbol_filter):
                continue
            rows_after_symbol_filter += 1
            if limit is not None and len(rows) >= limit:
                continue
            if pattern not in SUPPORTED_PATTERNS:
                continue
            spec = SUPPORTED_PATTERNS[pattern]
            reference_ts = parse_int(row_value(row, "reference_ts"))
            entry = parse_float(row_value(row, "entry"))
            stop = parse_float(row_value(row, "stop"))
            tp1 = parse_float(row_value(row, "tp1"))
            tp2 = parse_float(row_value(row, "tp2"))
            if reference_ts is None or entry is None or stop is None or tp1 is None or tp2 is None:
                continue
            side = str(row_value(row, "side") or spec.default_side).strip().upper()
            outcome = normalize_outcome(row_value(row, "outcome") or row_value(row, "realized_outcome"))
            rows.append(
                ExecutionAuditRow(
                    sample_number=len(rows) + 1,
                    source_row_number=source_row_number,
                    row=row,
                    symbol=symbol,
                    reference_ts=reference_ts,
                    pattern=pattern,
                    side=side,
                    entry=entry,
                    stop=stop,
                    tp1=tp1,
                    tp2=tp2,
                    outcome=outcome,
                    r_multiple=first_float(row, ("realized_R_multiple", "realized_r_multiple", "r_multiple")),
                    bars_to_event=first_int(row, ("bars_to_event", "realized_bars_to_event")),
                    event_ts=first_int(row, ("event_ts", "exit_ts", "realized_event_ts")),
                    exit_reason=str(row_value(row, "exit_reason") or outcome),
                    pattern_width_columns=infer_pattern_width(spec, row),
                    pattern_quality=str(row_value(row, "pattern_quality") or row_value(row, "catapult_pattern_quality") or "") or None,
                    highlighted_levels=collect_highlighted_levels(spec, row),
                )
            )

    return LoadResult(rows, total_input_rows, rows_after_symbol_filter, symbols, patterns)


def load_past_candles(conn: sqlite3.Connection, symbol: str, end_ts: int) -> list[Candle]:
    db_symbol = db_symbol_for(conn, symbol)
    rows = conn.execute(
        """
        SELECT close_time, high, low, close
        FROM candles
        WHERE symbol = ? AND interval = '1m' AND close_time <= ?
        ORDER BY open_time ASC
        """,
        (db_symbol, int(end_ts)),
    ).fetchall()
    return [Candle(int(ts), float(high), float(low), float(close)) for ts, high, low, close in rows]


def iter_future_candles(conn: sqlite3.Connection, symbol: str, after_ts: int, max_bars: int) -> Iterable[Candle]:
    db_symbol = db_symbol_for(conn, symbol)
    cur = conn.execute(
        """
        SELECT close_time, high, low, close
        FROM candles
        WHERE symbol = ? AND interval = '1m' AND close_time > ?
        ORDER BY open_time ASC
        LIMIT ?
        """,
        (db_symbol, int(after_ts), int(max_bars)),
    )
    for ts, high, low, close in cur:
        yield Candle(int(ts), float(high), float(low), float(close))


def candle_hits(candle: Candle, side: str, audit_row: ExecutionAuditRow) -> list[tuple[str, float, int]]:
    if side == "LONG":
        checks = [
            (OUTCOME_STOP, audit_row.stop, candle.low <= audit_row.stop, 0),
            (OUTCOME_TP2, audit_row.tp2, candle.high >= audit_row.tp2, 1),
            (OUTCOME_TP1, audit_row.tp1, candle.high >= audit_row.tp1, 2),
        ]
    else:
        checks = [
            (OUTCOME_STOP, audit_row.stop, candle.high >= audit_row.stop, 0),
            (OUTCOME_TP2, audit_row.tp2, candle.low <= audit_row.tp2, 1),
            (OUTCOME_TP1, audit_row.tp1, candle.low <= audit_row.tp1, 2),
        ]
    return [(name, price, priority) for name, price, hit, priority in checks if hit]


def outcome_price(audit_row: ExecutionAuditRow) -> float | None:
    if audit_row.outcome == OUTCOME_STOP:
        return audit_row.stop
    if audit_row.outcome == OUTCOME_TP1:
        return audit_row.tp1
    if audit_row.outcome == OUTCOME_TP2:
        return audit_row.tp2
    return None


def build_sequence(touches: dict[str, ExecutionTouch], exit_touch: ExecutionTouch | None) -> str:
    ordered = sorted(touches.values(), key=lambda touch: (touch.bars, touch.priority, touch.name))
    names = ["ENTRY"] + [touch.name for touch in ordered]
    if exit_touch is not None and exit_touch.name not in {touch.name for touch in ordered}:
        names.append(exit_touch.name)
    return " -> ".join(names)


def reconstruct_execution(
    *,
    conn: sqlite3.Connection,
    profile: PnFProfile,
    audit_row: ExecutionAuditRow,
    max_bars: int,
    forward_columns: int,
) -> ReconstructedExecution:
    engine = PnFEngine(profile)
    candles_processed = 0
    for candle in load_past_candles(conn, audit_row.symbol, audit_row.reference_ts):
        engine.update_from_price(candle.close_time, candle.close)
        candles_processed += 1
    if not engine.columns:
        raise RuntimeError(f"No PnF columns reconstructed for {audit_row.symbol} at reference_ts={audit_row.reference_ts}")

    trigger_idx = int(engine.columns[-1].idx)
    target_last_idx = trigger_idx + forward_columns
    touches: dict[str, ExecutionTouch] = {}
    exit_touch: ExecutionTouch | None = None
    future_bars_processed = 0

    for bar_idx, candle in enumerate(iter_future_candles(conn, audit_row.symbol, audit_row.reference_ts, max_bars), start=1):
        engine.update_from_price(candle.close_time, candle.close)
        candles_processed += 1
        future_bars_processed = bar_idx
        current_idx = int(engine.columns[-1].idx)

        for name, price, priority in candle_hits(candle, audit_row.side, audit_row):
            touches.setdefault(name, ExecutionTouch(name, bar_idx, candle.close_time, price, current_idx, priority))

        if exit_touch is None:
            exit_price = outcome_price(audit_row)
            if audit_row.bars_to_event is not None and bar_idx == audit_row.bars_to_event and exit_price is not None:
                exit_touch = ExecutionTouch("EXIT", bar_idx, audit_row.event_ts or candle.close_time, exit_price, current_idx, -1)
            elif audit_row.event_ts is not None and candle.close_time >= audit_row.event_ts and exit_price is not None:
                exit_touch = ExecutionTouch("EXIT", bar_idx, audit_row.event_ts, exit_price, current_idx, -1)

        if current_idx >= target_last_idx and exit_touch is not None and {OUTCOME_TP1, OUTCOME_TP2, OUTCOME_STOP}.issubset(touches):
            break
        if current_idx >= target_last_idx and audit_row.outcome == OUTCOME_NO_EVENT and touches:
            break

    if exit_touch is None and audit_row.outcome in {OUTCOME_STOP, OUTCOME_TP1, OUTCOME_TP2}:
        touch = touches.get(audit_row.outcome)
        if touch is not None:
            exit_touch = ExecutionTouch("EXIT", touch.bars, touch.ts, touch.price, touch.column_idx, -1)

    return ReconstructedExecution(
        trigger_idx=trigger_idx,
        columns=list(engine.columns),
        candles_processed=candles_processed,
        future_bars_processed=future_bars_processed,
        box_size=float(profile.box_size),
        touches=touches,
        exit_touch=exit_touch,
        sequence=build_sequence(touches, exit_touch),
    )


def format_ts(ts: int | None) -> str:
    if ts is None:
        return ""
    seconds = ts / 1000 if ts > 10_000_000_000 else ts
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_price(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.10g}"


def fmt_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.10g}"
    return str(value)


def safe_filename_part(text: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in text).strip("_")


def level_matches(candidate: float, target: float, box_size: float) -> bool:
    tolerance = max(1e-9, abs(box_size) * 1e-9)
    return abs(candidate - target) <= tolerance


def body_indices(audit_row: ExecutionAuditRow, trigger_idx: int) -> set[int]:
    width = audit_row.pattern_width_columns
    if width is None or width <= 0:
        return {trigger_idx}
    return set(range(trigger_idx - width + 1, trigger_idx + 1))


def rendered_window_bounds(audit_row: ExecutionAuditRow, reconstruction: ReconstructedExecution) -> tuple[int, int]:
    columns_by_idx = {int(column.idx): column for column in reconstruction.columns}
    min_idx = min(columns_by_idx)
    max_idx = max(columns_by_idx)
    start_idx = max(min_idx, reconstruction.trigger_idx - COLUMNS_BEFORE_TRIGGER)
    end_idx = min(max_idx, reconstruction.trigger_idx + FORWARD_COLUMNS_AFTER_TRIGGER)
    if audit_row.pattern_width_columns is not None and audit_row.pattern_width_columns > 0:
        body_start_idx = reconstruction.trigger_idx - audit_row.pattern_width_columns + 1
        start_idx = max(min_idx, min(start_idx, body_start_idx))
    event_indices = [touch.column_idx for touch in reconstruction.touches.values()]
    if reconstruction.exit_touch is not None:
        event_indices.append(reconstruction.exit_touch.column_idx)
    if event_indices:
        end_idx = min(max_idx, max(end_idx, *event_indices))
    return start_idx, end_idx


def execution_level_labels(audit_row: ExecutionAuditRow) -> tuple[tuple[str, float], ...]:
    return (("entry", audit_row.entry), ("stop", audit_row.stop), ("tp1", audit_row.tp1), ("tp2", audit_row.tp2))


def badges_for_cell(level: float, column_idx: int, audit_row: ExecutionAuditRow, reconstruction: ReconstructedExecution) -> list[str]:
    badges: list[str] = []
    for touch_name in (OUTCOME_TP1, OUTCOME_TP2, OUTCOME_STOP):
        touch = reconstruction.touches.get(touch_name)
        if touch and touch.column_idx == column_idx and level_matches(level, touch.price, reconstruction.box_size):
            badges.append(f"FIRST {touch_name}")
    if reconstruction.exit_touch and reconstruction.exit_touch.column_idx == column_idx:
        if level_matches(level, reconstruction.exit_touch.price, reconstruction.box_size):
            badges.append("EXIT")
    return badges


def render_column_table(*, audit_row: ExecutionAuditRow, reconstruction: ReconstructedExecution) -> str:
    columns_by_idx = {int(column.idx): column for column in reconstruction.columns}
    start_idx, end_idx = rendered_window_bounds(audit_row, reconstruction)
    visible_columns = [columns_by_idx[idx] for idx in range(start_idx, end_idx + 1) if idx in columns_by_idx]
    body_idx = body_indices(audit_row, reconstruction.trigger_idx)

    all_levels: set[float] = set()
    for column in visible_columns:
        all_levels.update(column.levels(reconstruction.box_size))
    for _label, level in audit_row.highlighted_levels + execution_level_labels(audit_row):
        all_levels.add(round(level, 10))
    if reconstruction.exit_touch is not None:
        all_levels.add(round(reconstruction.exit_touch.price, 10))
    levels = sorted(all_levels, reverse=True)

    header_cells = ["<th class=\"level\">Level</th>"]
    marker_cells = ["<th class=\"level\">Marker</th>"]
    kind_cells = ["<th class=\"level\">Kind</th>"]
    time_cells = ["<th class=\"level\">End time</th>"]
    for column in visible_columns:
        idx = int(column.idx)
        classes = ["colhead"]
        markers: list[str] = []
        if idx in body_idx:
            classes.append("body")
            markers.append("BODY")
        if idx == reconstruction.trigger_idx:
            classes.append("trigger")
            markers.append("TRIGGER")
        if idx > reconstruction.trigger_idx:
            classes.append("forward")
        header_cells.append(f"<th class=\"{' '.join(classes)}\">#{idx}</th>")
        marker_cells.append(f"<td class=\"{' '.join(classes)}\">{html.escape('/'.join(markers))}</td>")
        kind_cells.append(f"<td class=\"{' '.join(classes)}\">{html.escape(str(column.kind))}</td>")
        time_cells.append(f"<td class=\"{' '.join(classes)} small\">{html.escape(format_ts(int(column.end_ts)))}</td>")

    body_rows = [
        f"<tr>{''.join(header_cells)}</tr>",
        f"<tr>{''.join(marker_cells)}</tr>",
        f"<tr>{''.join(kind_cells)}</tr>",
        f"<tr>{''.join(time_cells)}</tr>",
    ]
    for level in levels:
        pattern_labels = [
            label for label, highlighted in audit_row.highlighted_levels if level_matches(level, highlighted, reconstruction.box_size)
        ]
        exec_labels = [label for label, highlighted in execution_level_labels(audit_row) if level_matches(level, highlighted, reconstruction.box_size)]
        row_classes = []
        if pattern_labels:
            row_classes.append("pattern-level")
        if exec_labels:
            row_classes.extend(f"exec-{label}" for label in exec_labels)
        row_label = fmt_price(level)
        labels = pattern_labels + exec_labels
        if labels:
            row_label = f"{row_label} ({', '.join(labels)})"
        row_cells = [f"<th class=\"level {' '.join(row_classes)}\">{html.escape(row_label)}</th>"]
        for column in visible_columns:
            idx = int(column.idx)
            cell_classes = ["box"] + row_classes
            if idx in body_idx:
                cell_classes.append("body")
            if idx == reconstruction.trigger_idx:
                cell_classes.append("trigger")
            if idx > reconstruction.trigger_idx:
                cell_classes.append("forward")
            column_levels = set(column.levels(reconstruction.box_size))
            has_box = any(level_matches(level, column_level, reconstruction.box_size) for column_level in column_levels)
            contents: list[str] = [html.escape(str(column.kind))] if has_box else []
            badges = badges_for_cell(level, idx, audit_row, reconstruction)
            if badges:
                cell_classes.append("event")
                contents.extend(f"<span class=\"badge\">{html.escape(badge)}</span>" for badge in badges)
            row_cells.append(f"<td class=\"{' '.join(cell_classes)}\">{''.join(contents)}</td>")
        body_rows.append(f"<tr>{''.join(row_cells)}</tr>")

    return "\n".join(body_rows)


def render_touch_table(audit_row: ExecutionAuditRow, reconstruction: ReconstructedExecution) -> str:
    rows = []
    for name in (OUTCOME_TP1, OUTCOME_TP2, OUTCOME_STOP):
        touch = reconstruction.touches.get(name)
        rows.append(
            "<tr>"
            f"<th>First touch {name}</th>"
            f"<td>{fmt_value(touch.bars if touch else None)}</td>"
            f"<td>{html.escape(format_ts(touch.ts) if touch else '')}</td>"
            f"<td>{html.escape(fmt_price(touch.price) if touch else '')}</td>"
            f"<td>{fmt_value(touch.column_idx if touch else None)}</td>"
            "</tr>"
        )
    return f"""
<table class=\"meta\">
<tr><th>Touch</th><th>Bars after trigger</th><th>Timestamp</th><th>Level</th><th>PnF column</th></tr>
{''.join(rows)}
</table>
"""


def render_execution_panel(audit_row: ExecutionAuditRow, reconstruction: ReconstructedExecution) -> str:
    bars_to_tp1 = reconstruction.touches.get(OUTCOME_TP1).bars if reconstruction.touches.get(OUTCOME_TP1) else None
    bars_to_tp2 = reconstruction.touches.get(OUTCOME_TP2).bars if reconstruction.touches.get(OUTCOME_TP2) else None
    bars_to_stop = reconstruction.touches.get(OUTCOME_STOP).bars if reconstruction.touches.get(OUTCOME_STOP) else None
    return f"""
<table class=\"meta execution\">
<tr><th>Side</th><td>{html.escape(audit_row.side)}</td></tr>
<tr><th>Entry price</th><td>{html.escape(fmt_price(audit_row.entry))}</td></tr>
<tr><th>Stop price</th><td>{html.escape(fmt_price(audit_row.stop))}</td></tr>
<tr><th>TP1</th><td>{html.escape(fmt_price(audit_row.tp1))}</td></tr>
<tr><th>TP2</th><td>{html.escape(fmt_price(audit_row.tp2))}</td></tr>
<tr><th>Realized outcome</th><td>{html.escape(audit_row.outcome)}</td></tr>
<tr><th>realized_R_multiple</th><td>{html.escape(fmt_value(audit_row.r_multiple))}</td></tr>
<tr><th>bars_to_event</th><td>{html.escape(fmt_value(audit_row.bars_to_event))}</td></tr>
<tr><th>bars_to_tp1</th><td>{html.escape(fmt_value(bars_to_tp1))}</td></tr>
<tr><th>bars_to_tp2</th><td>{html.escape(fmt_value(bars_to_tp2))}</td></tr>
<tr><th>bars_to_stop</th><td>{html.escape(fmt_value(bars_to_stop))}</td></tr>
<tr><th>exit_reason</th><td>{html.escape(audit_row.exit_reason)}</td></tr>
<tr><th>Outcome sequence</th><td><strong>{html.escape(reconstruction.sequence)}</strong></td></tr>
</table>
"""


def render_audit_html(audit_row: ExecutionAuditRow, reconstruction: ReconstructedExecution) -> str:
    spec = SUPPORTED_PATTERNS[audit_row.pattern]
    title = f"Execution Audit {audit_row.sample_number}: {audit_row.pattern} {audit_row.symbol} {format_ts(audit_row.reference_ts)}"
    source_items = "\n".join(
        f"<tr><th>{html.escape(key)}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in audit_row.row.items()
        if value not in (None, "")
    )
    highlighted_levels = ", ".join(f"{label}={fmt_price(level)}" for label, level in audit_row.highlighted_levels) or "n/a"
    return f"""<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>{html.escape(title)}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
table {{ border-collapse: collapse; }}
th, td {{ border: 1px solid #d1d5db; padding: 4px 6px; text-align: center; font-size: 12px; }}
th.level {{ position: sticky; left: 0; background: #f9fafb; text-align: right; z-index: 1; }}
.small {{ font-size: 10px; max-width: 96px; word-break: break-word; }}
.box {{ min-width: 42px; height: 24px; font-family: Consolas, Menlo, monospace; font-weight: 700; }}
.body {{ background: #dbeafe; }}
.trigger {{ background: #ffedd5; outline: 2px solid #ea580c; }}
.forward {{ background-image: linear-gradient(180deg, rgba(240,253,244,.8), rgba(240,253,244,.8)); }}
.pattern-level {{ outline: 2px solid #dc2626; background-color: #fef2f2; }}
.exec-entry {{ outline: 2px solid #2563eb; background-color: #eff6ff; }}
.exec-stop {{ outline: 2px solid #b91c1c; background-color: #fee2e2; }}
.exec-tp1 {{ outline: 2px solid #059669; background-color: #d1fae5; }}
.exec-tp2 {{ outline: 2px solid #047857; background-color: #a7f3d0; }}
.event {{ box-shadow: inset 0 0 0 3px #7c3aed; }}
.badge {{ display: block; margin-top: 2px; padding: 1px 3px; border-radius: 3px; background: #7c3aed; color: white; font-size: 9px; line-height: 1.2; }}
.legend span {{ display: inline-block; margin-right: 16px; margin-bottom: 8px; padding: 4px 8px; border: 1px solid #d1d5db; }}
.meta {{ margin-bottom: 18px; }}
.meta th {{ text-align: left; background: #f9fafb; }}
.meta td {{ text-align: left; }}
.execution th {{ min-width: 180px; }}
.source {{ margin-top: 24px; }}
.table-wrap {{ overflow-x: auto; max-width: 100%; }}
</style>
</head>
<body>
<h1>{html.escape(title)}</h1>
<table class=\"meta\">
<tr><th>Pattern</th><td>{html.escape(audit_row.pattern)}</td></tr>
<tr><th>Symbol</th><td>{html.escape(audit_row.symbol)}</td></tr>
<tr><th>Reference timestamp</th><td>{audit_row.reference_ts} ({html.escape(format_ts(audit_row.reference_ts))})</td></tr>
<tr><th>Pattern width columns</th><td>{html.escape(str(audit_row.pattern_width_columns or ''))}</td></tr>
<tr><th>Pattern quality</th><td>{html.escape(str(audit_row.pattern_quality or ''))}</td></tr>
<tr><th>{html.escape(spec.level_label.title())}</th><td>{html.escape(highlighted_levels)}</td></tr>
<tr><th>Trigger column index</th><td>{reconstruction.trigger_idx}</td></tr>
<tr><th>Candles processed</th><td>{reconstruction.candles_processed}</td></tr>
<tr><th>Future bars processed</th><td>{reconstruction.future_bars_processed}</td></tr>
<tr><th>Source CSV row</th><td>{audit_row.source_row_number}</td></tr>
</table>
<h2>Execution info panel</h2>
{render_execution_panel(audit_row, reconstruction)}
<h2>First touch audit</h2>
{render_touch_table(audit_row, reconstruction)}
<h2>Pattern area + forward price path</h2>
<p class=\"legend\"><span class=\"body\">BODY = exact consecutive-column pattern body inferred from outcome CSV width/default width ending at trigger</span><span class=\"trigger\">TRIGGER = reconstructed current PnF column at reference_ts</span><span class=\"pattern-level\">outlined red row = support/resistance level when available</span><span class=\"exec-entry\">entry row</span><span class=\"exec-stop\">stop row</span><span class=\"exec-tp1\">TP1 row</span><span class=\"exec-tp2\">TP2 row</span><span class=\"event\">purple badge = first touch / actual exit point</span></p>
<div class=\"table-wrap\"><table>
{render_column_table(audit_row=audit_row, reconstruction=reconstruction)}
</table></div>
<h2 class=\"source\">Source outcome CSV fields</h2>
<table class=\"meta\">
{source_items}
</table>
</body>
</html>
"""


def render_index(rows: list[tuple[ExecutionAuditRow, str, str]], load_result: LoadResult) -> str:
    body = []
    for audit_row, filename, status in rows:
        body.append(
            "<tr>"
            f"<td>{audit_row.sample_number}</td>"
            f"<td>{html.escape(audit_row.pattern)}</td>"
            f"<td>{html.escape(audit_row.symbol)}</td>"
            f"<td>{audit_row.reference_ts}<br>{html.escape(format_ts(audit_row.reference_ts))}</td>"
            f"<td>{html.escape(audit_row.side)}</td>"
            f"<td>{html.escape(audit_row.outcome)}</td>"
            f"<td>{html.escape(fmt_value(audit_row.r_multiple))}</td>"
            f"<td>{html.escape(fmt_value(audit_row.bars_to_event))}</td>"
            f"<td>{html.escape(status)}</td>"
            f"<td><a href=\"{html.escape(filename)}\">open</a></td>"
            "</tr>"
        )
    pattern_counts = ", ".join(f"{pattern}={count}" for pattern, count in sorted(load_result.patterns.items())) or "none"
    symbol_counts = ", ".join(f"{symbol}={count}" for symbol, count in sorted(load_result.symbols.items())) or "none"
    return f"""<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>Pattern Execution Audit Index</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; vertical-align: top; }}
th {{ background: #f9fafb; text-align: left; }}
</style>
</head>
<body>
<h1>Pattern Execution Audit Index</h1>
<p>Analysis/debug-only execution renderer for outcome CSV rows generated by <code>experiments/pattern_outcome_analysis.py</code>.</p>
<ul>
<li>Total input rows: {load_result.total_input_rows}</li>
<li>Rows after symbol filter: {load_result.rows_after_symbol_filter}</li>
<li>Pattern counts: {html.escape(pattern_counts)}</li>
<li>Symbol counts: {html.escape(symbol_counts)}</li>
</ul>
<table>
<tr><th>#</th><th>Pattern</th><th>Symbol</th><th>Reference TS</th><th>Side</th><th>Outcome</th><th>R</th><th>Bars to event</th><th>Status</th><th>Audit</th></tr>
{''.join(body)}
</table>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Render execution/outcome audit HTML for structural pattern outcomes")
    parser.add_argument("--input-csv", default=DEFAULT_INPUT_CSV, help="Outcome CSV generated by experiments/pattern_outcome_analysis.py")
    parser.add_argument("--settings", default=DEFAULT_SETTINGS)
    parser.add_argument("--symbol", default=None, help="Optional symbol filter. Comma-separated values are accepted; BTC matches BINANCE_FUT:BTCUSDT.")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of outcome rows to render.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--db-path", default=None, help="Optional database override; defaults to settings database_path.")
    parser.add_argument("--max-bars", type=int, default=DEFAULT_MAX_BARS, help="Maximum forward candles to scan for first touches.")
    parser.add_argument("--forward-columns", type=int, default=FORWARD_COLUMNS_AFTER_TRIGGER, help="Minimum number of forward PnF columns to render after trigger.")
    args = parser.parse_args()
    if args.max_bars <= 0:
        raise SystemExit("--max-bars must be positive")
    if args.forward_columns < 0:
        raise SystemExit("--forward-columns must be non-negative")

    input_csv = resolve_repo_path(args.input_csv)
    settings_path = resolve_repo_path(args.settings)
    output_dir = resolve_repo_path(args.output_dir)
    if not input_csv.exists():
        raise SystemExit(f"input CSV not found: {input_csv}")
    if not settings_path.exists():
        raise SystemExit(f"settings file not found: {settings_path}")

    settings = load_settings(settings_path)
    database_path = resolve_repo_path(args.db_path) if args.db_path else resolve_settings_relative_path(settings_path, settings["database_path"])
    if not database_path.exists():
        raise SystemExit(f"database file not found: {database_path}")
    profiles = build_profiles(settings)
    symbol_filter = {part.strip() for part in args.symbol.split(",") if part.strip()} if args.symbol else None
    load_result = load_execution_rows(input_csv, symbol_filter, args.limit)

    output_dir.mkdir(parents=True, exist_ok=True)
    rendered: list[tuple[ExecutionAuditRow, str, str]] = []
    uri = f"file:{database_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        for audit_row in load_result.rows:
            filename = (
                f"{audit_row.sample_number:04d}_{safe_filename_part(audit_row.pattern)}_"
                f"{safe_filename_part(audit_row.symbol)}_{audit_row.reference_ts}.html"
            )
            output_path = output_dir / filename
            try:
                profile = profile_for_symbol(profiles, audit_row.symbol)
                if profile is None:
                    raise RuntimeError(f"No PnF profile found for {audit_row.symbol}")
                reconstruction = reconstruct_execution(
                    conn=conn,
                    profile=profile,
                    audit_row=audit_row,
                    max_bars=args.max_bars,
                    forward_columns=args.forward_columns,
                )
                output_path.write_text(render_audit_html(audit_row, reconstruction), encoding="utf-8")
                status = "rendered"
            except Exception as exc:
                output_path.write_text(
                    f"<!doctype html><html><body><h1>Render failed</h1><pre>{html.escape(str(exc))}</pre></body></html>",
                    encoding="utf-8",
                )
                status = f"error: {exc}"
            rendered.append((audit_row, filename, status))
    finally:
        conn.close()

    index_path = output_dir / "index.html"
    index_path.write_text(render_index(rendered, load_result), encoding="utf-8")
    print("=== Pattern Execution Audit Renderer ===")
    print(f"input_csv={input_csv}")
    print(f"settings={settings_path}")
    print(f"db_path={database_path}")
    print(f"symbol_filter={args.symbol or 'ALL'}")
    print(f"limit={args.limit if args.limit is not None else 'ALL'}")
    print(f"total_input_rows={load_result.total_input_rows}")
    print(f"rows_after_symbol_filter={load_result.rows_after_symbol_filter}")
    print(f"rendered_rows={len(rendered)}")
    print(f"output_index={index_path}")


if __name__ == "__main__":
    main()
