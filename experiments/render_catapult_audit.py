#!/usr/bin/env python3
"""Render standalone visual audit pages for shadow bearish catapult rows.

This script is intentionally audit/visualization-only. It rebuilds PnF columns
from historical candle closes and renders sampled `shadow_bearish_catapult == 1`
rows without changing scanner, strategy, or validation behavior.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
PNF_MVP_DIR = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_DIR) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_DIR))

from pnf_engine import PnFColumn, PnFEngine, PnFProfile  # noqa: E402

DEFAULT_INPUT_CSV = "pnf_mvp/exports/catapult_bearish_v1.csv"
DEFAULT_SETTINGS = "pnf_mvp/settings.research_clean.json"
DEFAULT_OUTPUT_DIR = "exports/catapult_audit"
COLUMNS_BEFORE_TRIGGER = 10
COLUMNS_AFTER_TRIGGER = 5


@dataclass(frozen=True)
class CatapultAuditRow:
    sample_number: int
    source_row_number: int
    row: dict[str, str]
    symbol: str
    reference_ts: int
    support_level: float | None
    total_columns: int | None
    origin_width: int | None
    pattern_quality: str | None


@dataclass(frozen=True)
class ReconstructedColumns:
    trigger_idx: int
    columns: list[PnFColumn]
    candles_processed: int
    has_after_context: bool
    box_size: float


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


def is_catapult_row(row: dict[str, str]) -> bool:
    return parse_int(row.get("shadow_bearish_catapult")) == 1


def load_catapult_rows(input_csv: Path, symbol_filter: set[str] | None, limit: int | None) -> list[CatapultAuditRow]:
    rows: list[CatapultAuditRow] = []
    with input_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for source_row_number, row in enumerate(reader, start=2):
            if not is_catapult_row(row):
                continue
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                continue
            if symbol_filter is not None and symbol not in symbol_filter:
                continue
            reference_ts = parse_int(row.get("reference_ts"))
            if reference_ts is None:
                continue
            rows.append(
                CatapultAuditRow(
                    sample_number=len(rows) + 1,
                    source_row_number=source_row_number,
                    row=row,
                    symbol=symbol,
                    reference_ts=reference_ts,
                    support_level=parse_float(row.get("catapult_support_level")),
                    total_columns=parse_int(row.get("catapult_total_columns")),
                    origin_width=parse_int(row.get("catapult_origin_width")),
                    pattern_quality=(row.get("catapult_pattern_quality") or None),
                )
            )
            if limit is not None and len(rows) >= limit:
                break
    return rows


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


def infer_marker_indices(audit_row: CatapultAuditRow, trigger_idx: int, columns_by_idx: dict[int, PnFColumn]) -> tuple[set[int], set[int], int | None]:
    origin_breakdown_idx: int | None = None
    rebound_indices: set[int] = set()
    if audit_row.total_columns is not None and audit_row.origin_width is not None:
        origin_start_idx = trigger_idx - audit_row.total_columns + 1
        origin_breakdown_idx = origin_start_idx + audit_row.origin_width - 1
        rebound_indices = {
            idx
            for idx, column in columns_by_idx.items()
            if origin_breakdown_idx < idx < trigger_idx and str(column.kind).upper() == "X"
        }
    return {trigger_idx}, rebound_indices, origin_breakdown_idx


def rendered_window_bounds(audit_row: CatapultAuditRow, trigger_idx: int, min_idx: int, max_idx: int) -> tuple[int, int]:
    start_idx = max(min_idx, trigger_idx - COLUMNS_BEFORE_TRIGGER)
    end_idx = min(max_idx, trigger_idx + COLUMNS_AFTER_TRIGGER)
    if audit_row.total_columns is not None and audit_row.origin_width is not None:
        origin_start_idx = trigger_idx - audit_row.total_columns + 1
        origin_breakdown_idx = origin_start_idx + audit_row.origin_width - 1
        start_idx = max(min_idx, min(start_idx, origin_start_idx, origin_breakdown_idx))
    return start_idx, end_idx


def render_column_table(
    *,
    audit_row: CatapultAuditRow,
    reconstruction: ReconstructedColumns,
) -> str:
    columns_by_idx = {int(column.idx): column for column in reconstruction.columns}
    trigger_indices, rebound_indices, origin_breakdown_idx = infer_marker_indices(
        audit_row, reconstruction.trigger_idx, columns_by_idx
    )
    min_idx = min(columns_by_idx)
    max_idx = max(columns_by_idx)
    start_idx, end_idx = rendered_window_bounds(audit_row, reconstruction.trigger_idx, min_idx, max_idx)
    visible_columns = [columns_by_idx[idx] for idx in range(start_idx, end_idx + 1) if idx in columns_by_idx]

    all_levels: set[float] = set()
    for column in visible_columns:
        all_levels.update(column.levels(reconstruction.box_size))
    if audit_row.support_level is not None:
        all_levels.add(audit_row.support_level)
    levels = sorted(all_levels, reverse=True)

    header_cells = ["<th class=\"level\">Level</th>"]
    marker_cells = ["<th class=\"level\">Marker</th>"]
    kind_cells = ["<th class=\"level\">Kind</th>"]
    time_cells = ["<th class=\"level\">End time</th>"]
    for column in visible_columns:
        idx = int(column.idx)
        classes = ["colhead"]
        markers: list[str] = []
        if idx == origin_breakdown_idx:
            classes.append("origin")
            markers.append("ORIGIN")
        if idx in rebound_indices:
            classes.append("rebound")
            markers.append("REBOUND")
        if idx in trigger_indices:
            classes.append("trigger")
            markers.append("TRIGGER")
        header_cells.append(f"<th class=\"{' '.join(classes)}\">#{idx}</th>")
        marker_cells.append(f"<td class=\"{' '.join(classes)}\">{html.escape('/'.join(markers))}</td>")
        kind_cells.append(f"<td class=\"{' '.join(classes)}\">{html.escape(str(column.kind))}</td>")
        time_cells.append(f"<td class=\"{' '.join(classes)} small\">{html.escape(format_ts(int(column.end_ts)))}</td>")

    body_rows = [f"<tr>{''.join(header_cells)}</tr>", f"<tr>{''.join(marker_cells)}</tr>", f"<tr>{''.join(kind_cells)}</tr>", f"<tr>{''.join(time_cells)}</tr>"]
    for level in levels:
        support_class = " support" if audit_row.support_level is not None and abs(level - audit_row.support_level) < 1e-9 else ""
        row_cells = [f"<th class=\"level{support_class}\">{html.escape(fmt_price(level))}</th>"]
        for column in visible_columns:
            idx = int(column.idx)
            marker_class = ""
            if idx == origin_breakdown_idx:
                marker_class += " origin"
            if idx in rebound_indices:
                marker_class += " rebound"
            if idx in trigger_indices:
                marker_class += " trigger"
            has_box = level in set(column.levels(reconstruction.box_size))
            glyph = html.escape(str(column.kind)) if has_box else ""
            row_cells.append(f"<td class=\"box{support_class}{marker_class}\">{glyph}</td>")
        body_rows.append(f"<tr>{''.join(row_cells)}</tr>")

    return "\n".join(body_rows)


def render_audit_html(audit_row: CatapultAuditRow, reconstruction: ReconstructedColumns) -> str:
    title = f"Bearish Catapult Audit {audit_row.sample_number}: {audit_row.symbol} {format_ts(audit_row.reference_ts)}"
    source_items = "\n".join(
        f"<tr><th>{html.escape(key)}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in audit_row.row.items()
        if value not in (None, "")
    )
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
.origin {{ background: #fee2e2; }}
.rebound {{ background: #dbeafe; }}
.trigger {{ background: #ffedd5; }}
.support {{ outline: 2px solid #dc2626; background-color: #fef2f2; }}
.legend span {{ display: inline-block; margin-right: 16px; padding: 4px 8px; border: 1px solid #d1d5db; }}
.meta th {{ text-align: left; background: #f9fafb; }}
.source {{ margin-top: 24px; }}
</style>
</head>
<body>
<h1>{html.escape(title)}</h1>
<table class=\"meta\">
<tr><th>Symbol</th><td>{html.escape(audit_row.symbol)}</td></tr>
<tr><th>Reference timestamp</th><td>{audit_row.reference_ts} ({html.escape(format_ts(audit_row.reference_ts))})</td></tr>
<tr><th>Support level</th><td>{html.escape(fmt_price(audit_row.support_level))}</td></tr>
<tr><th>Total columns</th><td>{html.escape(str(audit_row.total_columns or ''))}</td></tr>
<tr><th>Pattern quality</th><td>{html.escape(str(audit_row.pattern_quality or ''))}</td></tr>
<tr><th>Outcome</th><td>{html.escape(outcome_text(audit_row.row))}</td></tr>
<tr><th>Trigger column index</th><td>{reconstruction.trigger_idx}</td></tr>
<tr><th>After-context complete</th><td>{after_note}</td></tr>
<tr><th>Candles processed</th><td>{reconstruction.candles_processed}</td></tr>
</table>
<p class=\"legend\"><span class=\"origin\">ORIGIN = original triple-bottom breakdown column</span><span class=\"rebound\">REBOUND = intervening X column(s)</span><span class=\"trigger\">TRIGGER = second breakdown/catapult column</span><span class=\"support\">outlined row = catapult_support_level</span></p>
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


def render_index(rows: list[tuple[CatapultAuditRow, str, str]]) -> str:
    body = []
    for audit_row, filename, status in rows:
        body.append(
            "<tr>"
            f"<td>{audit_row.sample_number}</td>"
            f"<td>{html.escape(audit_row.symbol)}</td>"
            f"<td>{audit_row.reference_ts}<br>{html.escape(format_ts(audit_row.reference_ts))}</td>"
            f"<td>{html.escape(fmt_price(audit_row.support_level))}</td>"
            f"<td>{html.escape(str(audit_row.pattern_quality or ''))}</td>"
            f"<td>{html.escape(outcome_text(audit_row.row))}</td>"
            f"<td>{html.escape(status)}</td>"
            f"<td><a href=\"{html.escape(filename)}\">open</a></td>"
            "</tr>"
        )
    return f"""<!doctype html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>Bearish Catapult Audit Index</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; vertical-align: top; }}
th {{ background: #f9fafb; text-align: left; }}
</style>
</head>
<body>
<h1>Bearish Catapult Audit Index</h1>
<p>Standalone visual audit output for rows where <code>shadow_bearish_catapult == 1</code>.</p>
<table>
<tr><th>#</th><th>Symbol</th><th>Reference TS</th><th>Support</th><th>Quality</th><th>Outcome</th><th>Status</th><th>Audit</th></tr>
{''.join(body)}
</table>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Render visual audit HTML for detected bearish PnF catapults")
    parser.add_argument("--input-csv", default=DEFAULT_INPUT_CSV)
    parser.add_argument("--settings", default=DEFAULT_SETTINGS)
    parser.add_argument("--symbol", default=None, help="Optional symbol filter. Comma-separated values are accepted.")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of catapult rows to render.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    input_csv = resolve_repo_path(args.input_csv)
    settings_path = resolve_repo_path(args.settings)
    output_dir = resolve_repo_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    settings = load_settings(settings_path)
    database_path = resolve_settings_relative_path(settings_path, settings["database_path"])
    profiles = build_profiles(settings)
    symbol_filter = {part.strip() for part in args.symbol.split(",") if part.strip()} if args.symbol else None
    audit_rows = load_catapult_rows(input_csv, symbol_filter, args.limit)

    rendered: list[tuple[CatapultAuditRow, str, str]] = []
    for audit_row in audit_rows:
        filename = f"{audit_row.sample_number:04d}_{safe_filename_part(audit_row.symbol)}_{audit_row.reference_ts}.html"
        output_path = output_dir / filename
        try:
            profile = profiles[audit_row.symbol]
            reconstruction = reconstruct_columns(
                database_path=database_path,
                profile=profile,
                symbol=audit_row.symbol,
                reference_ts=audit_row.reference_ts,
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

    index_path = output_dir / "index.html"
    index_path.write_text(render_index(rendered), encoding="utf-8")
    print(f"Loaded {len(audit_rows)} bearish catapult row(s) from {input_csv}")
    print(f"Wrote audit index to {index_path}")


if __name__ == "__main__":
    main()
