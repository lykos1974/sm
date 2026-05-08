#!/usr/bin/env python3
"""Render standalone visual audits for structural PnF pattern detections.

This script is intentionally visualization/audit-only. It consumes CSV output
from ``experiments/shadow_research_scanner.py``, rebuilds nearby PnF columns
from historical candle closes, and writes static HTML pages so detected
structural patterns can be checked against textbook PnF geometry.
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

DEFAULT_INPUT_CSV = "pnf_mvp/exports/core_patterns_v1.csv"
DEFAULT_SETTINGS = "pnf_mvp/settings.research_clean.json"
DEFAULT_OUTPUT_DIR = "pnf_mvp/exports/pattern_audit"
COLUMNS_BEFORE_TRIGGER = 10
COLUMNS_AFTER_TRIGGER = 5


@dataclass(frozen=True)
class PatternSpec:
    name: str
    flag_column: str
    default_width_columns: int | None
    level_preference: tuple[str, ...]
    level_label: str


SUPPORTED_PATTERNS: dict[str, PatternSpec] = {
    "double_top": PatternSpec(
        "double_top", "shadow_double_top_breakout", 3, ("pattern_resistance_level",), "resistance"
    ),
    "double_bottom": PatternSpec(
        "double_bottom", "shadow_double_bottom_breakdown", 3, ("pattern_support_level",), "support"
    ),
    "triple_top": PatternSpec(
        "triple_top", "shadow_triple_top_breakout", 5, ("triple_top_resistance_level", "pattern_resistance_level"), "resistance"
    ),
    "triple_bottom": PatternSpec(
        "triple_bottom", "shadow_triple_bottom_breakdown", 5, ("triple_bottom_support_level", "pattern_support_level"), "support"
    ),
    "bullish_catapult": PatternSpec(
        "bullish_catapult", "shadow_bullish_catapult", 7, ("pattern_resistance_level",), "resistance"
    ),
    "bearish_catapult": PatternSpec(
        "bearish_catapult", "shadow_bearish_catapult", 7, ("catapult_support_level", "pattern_support_level"), "support"
    ),
    "bullish_triangle": PatternSpec(
        "bullish_triangle", "shadow_bullish_triangle", 5, ("pattern_resistance_level", "pattern_support_level"), "pattern level"
    ),
    "bearish_triangle": PatternSpec(
        "bearish_triangle", "shadow_bearish_triangle", 5, ("pattern_support_level", "pattern_resistance_level"), "pattern level"
    ),
    "bullish_signal_reversal": PatternSpec(
        "bullish_signal_reversal", "shadow_bullish_signal_reversal", None, ("pattern_resistance_level", "pattern_support_level"), "pattern level"
    ),
    "bearish_signal_reversal": PatternSpec(
        "bearish_signal_reversal", "shadow_bearish_signal_reversal", None, ("pattern_support_level", "pattern_resistance_level"), "pattern level"
    ),
    "shakeout": PatternSpec(
        "shakeout", "shadow_shakeout", 5, ("pattern_support_level", "pattern_resistance_level"), "pattern level"
    ),
}


@dataclass(frozen=True)
class PatternAuditRow:
    sample_number: int
    source_row_number: int
    row: dict[str, str]
    symbol: str
    reference_ts: int
    reason: str | None
    pattern_width_columns: int | None
    pattern_quality: str | None
    highlighted_levels: tuple[tuple[str, float], ...]


@dataclass(frozen=True)
class ReconstructedColumns:
    trigger_idx: int
    columns: list[PnFColumn]
    candles_processed: int
    has_after_context: bool
    box_size: float


@dataclass(frozen=True)
class PatternLoadResult:
    rows: list[PatternAuditRow]
    total_input_rows: int
    rows_before_symbol_filter: int
    rows_after_symbol_filter: int
    symbols: Counter[str]


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
    for symbol in settings["symbols"]:
        profile_settings = settings["profiles"][symbol]
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
        return int(float(str(value)))
    except ValueError:
        return None


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value))
    except ValueError:
        return None


def row_value(row: dict[str, Any], key: str) -> Any | None:
    if key in row:
        return row[key]
    for candidate_key, value in row.items():
        if str(candidate_key).strip().lstrip("\ufeff") == key:
            return value
    return None


def is_flagged(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value == 1
    if isinstance(value, float):
        return math.isfinite(value) and value == 1.0

    text = str(value).strip()
    if not text:
        return False
    try:
        numeric_value = float(text)
    except ValueError:
        return False
    return math.isfinite(numeric_value) and numeric_value == 1.0


def symbol_matches(symbol: str, symbol_filter: set[str] | None) -> bool:
    if symbol_filter is None:
        return True
    normalized_symbol = symbol.upper()
    compact_symbol = normalized_symbol.replace("BINANCE_FUT:", "").replace("USDT", "")
    for requested in symbol_filter:
        normalized_requested = requested.upper()
        if normalized_requested == normalized_symbol:
            return True
        if normalized_requested == compact_symbol:
            return True
        if normalized_requested in normalized_symbol:
            return True
    return False


def infer_pattern_width(spec: PatternSpec, row: dict[str, str]) -> int | None:
    detected_width = parse_int(row_value(row, "pattern_width_columns"))
    if spec.default_width_columns is None:
        return detected_width
    return detected_width or spec.default_width_columns


def collect_highlighted_levels(spec: PatternSpec, row: dict[str, str]) -> tuple[tuple[str, float], ...]:
    levels: list[tuple[str, float]] = []
    seen: set[tuple[str, float]] = set()
    for key in spec.level_preference:
        level = parse_float(row_value(row, key))
        if level is None:
            continue
        label = key.replace("pattern_", "").replace("_level", "").replace("_", " ")
        marker = (label, level)
        if marker not in seen:
            levels.append(marker)
            seen.add(marker)
    return tuple(levels)


def load_pattern_rows(
    input_csv: Path,
    spec: PatternSpec,
    symbol_filter: set[str] | None,
    limit: int | None,
) -> PatternLoadResult:
    rows: list[PatternAuditRow] = []
    total_input_rows = 0
    rows_before_symbol_filter = 0
    rows_after_symbol_filter = 0
    symbols: Counter[str] = Counter()

    with input_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is not None and spec.flag_column not in {name.strip().lstrip("\ufeff") for name in reader.fieldnames}:
            raise ValueError(f"Input CSV does not contain required flag column: {spec.flag_column}")

        for source_row_number, row in enumerate(reader, start=2):
            total_input_rows += 1
            if not is_flagged(row_value(row, spec.flag_column)):
                continue

            rows_before_symbol_filter += 1
            symbol = str(row_value(row, "symbol") or "").strip()
            if symbol:
                symbols[symbol] += 1

            if not symbol or not symbol_matches(symbol, symbol_filter):
                continue

            rows_after_symbol_filter += 1
            if limit is not None and len(rows) >= limit:
                continue

            reference_ts = parse_int(row_value(row, "reference_ts"))
            if reference_ts is None:
                continue

            rows.append(
                PatternAuditRow(
                    sample_number=len(rows) + 1,
                    source_row_number=source_row_number,
                    row=row,
                    symbol=symbol,
                    reference_ts=reference_ts,
                    reason=str(row_value(row, "reason") or "") or None,
                    pattern_width_columns=infer_pattern_width(spec, row),
                    pattern_quality=str(row_value(row, "pattern_quality") or "") or None,
                    highlighted_levels=collect_highlighted_levels(spec, row),
                )
            )

    return PatternLoadResult(
        rows=rows,
        total_input_rows=total_input_rows,
        rows_before_symbol_filter=rows_before_symbol_filter,
        rows_after_symbol_filter=rows_after_symbol_filter,
        symbols=symbols,
    )


def load_candles(database_path: Path, symbol: str, end_ts: int) -> list[dict[str, Any]]:
    uri = f"file:{database_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT close_time, close, high, low
            FROM candles
            WHERE symbol = ? AND interval = '1m' AND close_time <= ?
            ORDER BY open_time ASC
            """,
            (symbol, int(end_ts)),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def iter_future_candles(database_path: Path, symbol: str, after_ts: int) -> Iterable[dict[str, Any]]:
    uri = f"file:{database_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT close_time, close, high, low
            FROM candles
            WHERE symbol = ? AND interval = '1m' AND close_time > ?
            ORDER BY open_time ASC
            """,
            (symbol, int(after_ts)),
        )
        for row in cur:
            yield dict(row)
    finally:
        conn.close()


def reconstruct_columns(
    *,
    database_path: Path,
    profile: PnFProfile,
    symbol: str,
    reference_ts: int,
    after_columns: int = COLUMNS_AFTER_TRIGGER,
) -> ReconstructedColumns:
    engine = PnFEngine(profile)
    candles_processed = 0
    for candle in load_candles(database_path, symbol, reference_ts):
        engine.update_from_price(int(candle["close_time"]), float(candle["close"]))
        candles_processed += 1

    if not engine.columns:
        raise RuntimeError(f"No PnF columns reconstructed for {symbol} at reference_ts={reference_ts}")

    trigger_idx = int(engine.columns[-1].idx)
    target_last_idx = trigger_idx + after_columns
    for candle in iter_future_candles(database_path, symbol, reference_ts):
        if int(engine.columns[-1].idx) >= target_last_idx:
            break
        engine.update_from_price(int(candle["close_time"]), float(candle["close"]))
        candles_processed += 1

    return ReconstructedColumns(
        trigger_idx=trigger_idx,
        columns=list(engine.columns),
        candles_processed=candles_processed,
        has_after_context=int(engine.columns[-1].idx) >= target_last_idx,
        box_size=float(profile.box_size),
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


def safe_filename_part(text: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in text).strip("_")


def body_indices(audit_row: PatternAuditRow, trigger_idx: int) -> set[int]:
    width = audit_row.pattern_width_columns
    if width is None or width <= 0:
        return {trigger_idx}
    return set(range(trigger_idx - width + 1, trigger_idx + 1))


def rendered_window_bounds(audit_row: PatternAuditRow, trigger_idx: int, min_idx: int, max_idx: int) -> tuple[int, int]:
    start_idx = max(min_idx, trigger_idx - COLUMNS_BEFORE_TRIGGER)
    end_idx = min(max_idx, trigger_idx + COLUMNS_AFTER_TRIGGER)
    if audit_row.pattern_width_columns is not None and audit_row.pattern_width_columns > 0:
        body_start_idx = trigger_idx - audit_row.pattern_width_columns + 1
        start_idx = max(min_idx, min(start_idx, body_start_idx))
    return start_idx, end_idx


def level_matches(candidate: float, target: float, box_size: float) -> bool:
    tolerance = max(1e-9, abs(box_size) * 1e-9)
    return abs(candidate - target) <= tolerance


def outcome_text(row: dict[str, str]) -> str:
    preferred = [
        "outcome",
        "validation_outcome",
        "resolved_outcome",
        "final_outcome",
        "exit_reason",
        "status",
        "realized_r_multiple",
        "r_multiple",
    ]
    parts = []
    for key in preferred:
        value = row.get(key)
        if value not in (None, ""):
            parts.append(f"{key}={value}")
    return "; ".join(parts) if parts else "n/a"


def render_column_table(*, audit_row: PatternAuditRow, reconstruction: ReconstructedColumns) -> str:
    columns_by_idx = {int(column.idx): column for column in reconstruction.columns}
    min_idx = min(columns_by_idx)
    max_idx = max(columns_by_idx)
    start_idx, end_idx = rendered_window_bounds(audit_row, reconstruction.trigger_idx, min_idx, max_idx)
    visible_columns = [columns_by_idx[idx] for idx in range(start_idx, end_idx + 1) if idx in columns_by_idx]
    body_idx = body_indices(audit_row, reconstruction.trigger_idx)

    all_levels: set[float] = set()
    for column in visible_columns:
        all_levels.update(column.levels(reconstruction.box_size))
    for _, level in audit_row.highlighted_levels:
        all_levels.add(round(level, 10))
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
        level_labels = [
            label for label, highlighted in audit_row.highlighted_levels if level_matches(level, highlighted, reconstruction.box_size)
        ]
        level_class = " pattern-level" if level_labels else ""
        row_label = fmt_price(level)
        if level_labels:
            row_label = f"{row_label} ({', '.join(level_labels)})"
        row_cells = [f"<th class=\"level{level_class}\">{html.escape(row_label)}</th>"]
        for column in visible_columns:
            idx = int(column.idx)
            cell_classes = ["box"]
            if level_labels:
                cell_classes.append("pattern-level")
            if idx in body_idx:
                cell_classes.append("body")
            if idx == reconstruction.trigger_idx:
                cell_classes.append("trigger")
            column_levels = set(column.levels(reconstruction.box_size))
            has_box = any(level_matches(level, column_level, reconstruction.box_size) for column_level in column_levels)
            glyph = html.escape(str(column.kind)) if has_box else ""
            row_cells.append(f"<td class=\"{' '.join(cell_classes)}\">{glyph}</td>")
        body_rows.append(f"<tr>{''.join(row_cells)}</tr>")

    return "\n".join(body_rows)


def render_audit_html(spec: PatternSpec, audit_row: PatternAuditRow, reconstruction: ReconstructedColumns) -> str:
    title = f"{spec.name} Audit {audit_row.sample_number}: {audit_row.symbol} {format_ts(audit_row.reference_ts)}"
    source_items = "\n".join(
        f"<tr><th>{html.escape(key)}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in audit_row.row.items()
        if value not in (None, "")
    )
    highlighted_levels = ", ".join(f"{label}={fmt_price(level)}" for label, level in audit_row.highlighted_levels) or "n/a"
    after_note = "yes" if reconstruction.has_after_context else "partial/data exhausted"
    return f"""<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>{html.escape(title)}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
table {{ border-collapse: collapse; }}
th, td {{ border: 1px solid #d1d5db; padding: 4px 6px; text-align: center; font-size: 12px; }}
th.level {{ position: sticky; left: 0; background: #f9fafb; text-align: right; }}
.small {{ font-size: 10px; max-width: 96px; word-break: break-word; }}
.box {{ min-width: 34px; height: 20px; font-family: Consolas, Menlo, monospace; font-weight: 700; }}
.body {{ background: #dbeafe; }}
.trigger {{ background: #ffedd5; outline: 2px solid #ea580c; }}
.pattern-level {{ outline: 2px solid #dc2626; background-color: #fef2f2; }}
.legend span {{ display: inline-block; margin-right: 16px; margin-bottom: 8px; padding: 4px 8px; border: 1px solid #d1d5db; }}
.meta th {{ text-align: left; background: #f9fafb; }}
.source {{ margin-top: 24px; }}
</style>
</head>
<body>
<h1>{html.escape(title)}</h1>
<table class=\"meta\">
<tr><th>Pattern</th><td>{html.escape(spec.name)}</td></tr>
<tr><th>Flag column</th><td>{html.escape(spec.flag_column)}</td></tr>
<tr><th>Symbol</th><td>{html.escape(audit_row.symbol)}</td></tr>
<tr><th>Reference timestamp</th><td>{audit_row.reference_ts} ({html.escape(format_ts(audit_row.reference_ts))})</td></tr>
<tr><th>Reason</th><td>{html.escape(str(audit_row.reason or ''))}</td></tr>
<tr><th>Pattern width columns</th><td>{html.escape(str(audit_row.pattern_width_columns or ''))}</td></tr>
<tr><th>Pattern quality</th><td>{html.escape(str(audit_row.pattern_quality or ''))}</td></tr>
<tr><th>{html.escape(spec.level_label.title())}</th><td>{html.escape(highlighted_levels)}</td></tr>
<tr><th>Outcome</th><td>{html.escape(outcome_text(audit_row.row))}</td></tr>
<tr><th>Trigger column index</th><td>{reconstruction.trigger_idx}</td></tr>
<tr><th>After-context complete</th><td>{after_note}</td></tr>
<tr><th>Candles processed</th><td>{reconstruction.candles_processed}</td></tr>
<tr><th>Source CSV row</th><td>{audit_row.source_row_number}</td></tr>
</table>
<p class=\"legend\"><span class=\"body\">BODY = exact consecutive-column pattern body inferred from pattern_width_columns/default width ending at trigger</span><span class=\"trigger\">TRIGGER = reconstructed current PnF column at reference_ts</span><span class=\"pattern-level\">outlined row = support/resistance level when available</span></p>
<table>
{render_column_table(audit_row=audit_row, reconstruction=reconstruction)}
</table>
<h2 class=\"source\">Source CSV fields</h2>
<table class=\"meta\">
{source_items}
</table>
</body>
</html>
"""


def render_index(spec: PatternSpec, rows: list[tuple[PatternAuditRow, str, str]]) -> str:
    body = []
    for audit_row, filename, status in rows:
        highlighted_levels = ", ".join(f"{label}={fmt_price(level)}" for label, level in audit_row.highlighted_levels)
        body.append(
            "<tr>"
            f"<td>{audit_row.sample_number}</td>"
            f"<td>{html.escape(audit_row.symbol)}</td>"
            f"<td>{audit_row.reference_ts}<br>{html.escape(format_ts(audit_row.reference_ts))}</td>"
            f"<td>{html.escape(str(audit_row.reason or ''))}</td>"
            f"<td>{html.escape(str(audit_row.pattern_width_columns or ''))}</td>"
            f"<td>{html.escape(str(audit_row.pattern_quality or ''))}</td>"
            f"<td>{html.escape(highlighted_levels)}</td>"
            f"<td>{html.escape(outcome_text(audit_row.row))}</td>"
            f"<td>{html.escape(status)}</td>"
            f"<td><a href=\"{html.escape(filename)}\">open</a></td>"
            "</tr>"
        )
    return f"""<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>{html.escape(spec.name)} Pattern Audit Index</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; vertical-align: top; }}
th {{ background: #f9fafb; text-align: left; }}
</style>
</head>
<body>
<h1>{html.escape(spec.name)} Pattern Audit Index</h1>
<p>Standalone visual audit output for rows where <code>{html.escape(spec.flag_column)} == 1</code>.</p>
<table>
<tr><th>#</th><th>Symbol</th><th>Reference TS</th><th>Reason</th><th>Width</th><th>Quality</th><th>Levels</th><th>Outcome</th><th>Status</th><th>Audit</th></tr>
{''.join(body)}
</table>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Render visual audit HTML for detected structural PnF patterns")
    parser.add_argument("--input-csv", default=DEFAULT_INPUT_CSV)
    parser.add_argument("--settings", default=DEFAULT_SETTINGS)
    parser.add_argument("--pattern", required=True, choices=sorted(SUPPORTED_PATTERNS))
    parser.add_argument("--symbol", default=None, help="Optional symbol filter. Comma-separated values are accepted; BTC matches BINANCE_FUT:BTCUSDT.")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of pattern rows to render.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    spec = SUPPORTED_PATTERNS[args.pattern]
    input_csv = resolve_repo_path(args.input_csv)
    settings_path = resolve_repo_path(args.settings)
    output_dir = resolve_repo_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    settings = load_settings(settings_path)
    database_path = resolve_settings_relative_path(settings_path, settings["database_path"])
    profiles = build_profiles(settings)
    symbol_filter = {part.strip() for part in args.symbol.split(",") if part.strip()} if args.symbol else None
    load_result = load_pattern_rows(input_csv, spec, symbol_filter, args.limit)
    audit_rows = load_result.rows

    rendered: list[tuple[PatternAuditRow, str, str]] = []
    for audit_row in audit_rows:
        filename = f"{audit_row.sample_number:04d}_{spec.name}_{safe_filename_part(audit_row.symbol)}_{audit_row.reference_ts}.html"
        output_path = output_dir / filename
        try:
            profile = profiles[audit_row.symbol]
            reconstruction = reconstruct_columns(
                database_path=database_path,
                profile=profile,
                symbol=audit_row.symbol,
                reference_ts=audit_row.reference_ts,
            )
            output_path.write_text(render_audit_html(spec, audit_row, reconstruction), encoding="utf-8")
            status = "rendered"
        except Exception as exc:
            output_path.write_text(
                f"<!doctype html><html><body><h1>Render failed</h1><pre>{html.escape(str(exc))}</pre></body></html>",
                encoding="utf-8",
            )
            status = f"error: {exc}"
        rendered.append((audit_row, filename, status))

    index_path = output_dir / "index.html"
    index_path.write_text(render_index(spec, rendered), encoding="utf-8")
    available_symbols = ", ".join(f"{symbol}={count}" for symbol, count in sorted(load_result.symbols.items())) or "none"
    print(f"Total input rows: {load_result.total_input_rows}")
    print(f"Rows matching pattern before symbol filter: {load_result.rows_before_symbol_filter}")
    print(f"Rows matching pattern after symbol filter: {load_result.rows_after_symbol_filter}")
    print(f"Symbols available: {available_symbols}")
    print(f"Loaded {len(audit_rows)} {spec.name} row(s) from {input_csv}")
    print(f"Wrote audit index to {index_path}")


if __name__ == "__main__":
    main()
