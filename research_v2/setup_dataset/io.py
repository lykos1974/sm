from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from research_v2.setup_dataset.schema import EXPORT_COLUMNS

SOURCE_TABLE = "strategy_setups"

SOURCE_COLUMNS: tuple[str, ...] = (
    "setup_id",
    "symbol",
    "reference_ts",
    "side",
    "status",
    "strategy",
    "reason",
    "reject_reason",
    "quality_score",
    "quality_grade",
    "trend_state",
    "trend_regime",
    "immediate_slope",
    "breakout_context",
    "pullback_quality",
    "is_extended_move",
    "active_leg_boxes",
    "zone_low",
    "zone_high",
    "ideal_entry",
    "invalidation",
    "risk",
    "tp1",
    "tp2",
    "rr1",
    "rr2",
    "raw_setup_json",
    "raw_structure_json",
)


def _safe_json_loads(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        payload = json.loads(value)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _reference_utc(reference_ts: Any) -> str | None:
    if reference_ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(reference_ts), tz=timezone.utc).isoformat()
    except Exception:
        return None


def fetch_source_columns(conn: sqlite3.Connection, table_name: str = SOURCE_TABLE) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(r[1]) for r in rows}


def fetch_setup_rows(
    db_path: Path,
    symbols: list[str] | None = None,
    reference_ts_from: int | None = None,
    reference_ts_to: int | None = None,
) -> list[dict[str, Any]]:
    """Read setup rows in read-only mode from strategy_setups."""
    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.row_factory = sqlite3.Row
        source_cols = fetch_source_columns(conn)
        selected_cols = [c for c in SOURCE_COLUMNS if c in source_cols]
        if not selected_cols:
            raise RuntimeError(f"No expected source columns found in table '{SOURCE_TABLE}'.")

        where_parts: list[str] = []
        params: list[Any] = []
        if symbols:
            placeholders = ", ".join("?" for _ in symbols)
            where_parts.append(f"symbol IN ({placeholders})")
            params.extend(symbols)
        if reference_ts_from is not None:
            where_parts.append("reference_ts >= ?")
            params.append(int(reference_ts_from))
        if reference_ts_to is not None:
            where_parts.append("reference_ts <= ?")
            params.append(int(reference_ts_to))

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        sql = f"""
            SELECT {', '.join(selected_cols)}
            FROM {SOURCE_TABLE}
            {where_sql}
            ORDER BY reference_ts ASC, symbol ASC, setup_id ASC
        """
        rows = conn.execute(sql, params).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            raw = dict(row)
            raw_setup = _safe_json_loads(raw.get("raw_setup_json"))

            export_row = {
                "setup_id": raw.get("setup_id"),
                "symbol": raw.get("symbol"),
                "reference_ts": raw.get("reference_ts"),
                "reference_utc": _reference_utc(raw.get("reference_ts")),
                "side": raw.get("side"),
                "status": raw.get("status"),
                "strategy": raw.get("strategy"),
                "reason": raw.get("reason"),
                "reject_reason": raw.get("reject_reason"),
                "quality_score": raw.get("quality_score"),
                "quality_grade": raw.get("quality_grade"),
                "trend_state": raw.get("trend_state"),
                "trend_regime": raw.get("trend_regime"),
                "immediate_slope": raw.get("immediate_slope"),
                "breakout_context": raw.get("breakout_context"),
                "pullback_quality": raw.get("pullback_quality") or raw_setup.get("pullback_quality"),
                "market_state": raw_setup.get("market_state"),
                "latest_signal_name": raw_setup.get("latest_signal_name"),
                "is_extended_move": raw.get("is_extended_move"),
                "active_leg_boxes": raw.get("active_leg_boxes"),
                "zone_low": raw.get("zone_low"),
                "zone_high": raw.get("zone_high"),
                "ideal_entry": raw.get("ideal_entry"),
                "invalidation": raw.get("invalidation"),
                "risk": raw.get("risk"),
                "tp1": raw.get("tp1"),
                "tp2": raw.get("tp2"),
                "rr1": raw.get("rr1"),
                "rr2": raw.get("rr2"),
                "raw_setup_json": raw.get("raw_setup_json"),
                "raw_structure_json": raw.get("raw_structure_json"),
            }
            out.append(export_row)
        return out
    finally:
        conn.close()


def write_csv_dataset(rows: Iterable[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(EXPORT_COLUMNS), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_dataset(
    rows: list[dict[str, Any]],
    output_path: Path,
    fmt: str,
) -> str:
    """Write dataset with parquet-first behavior and csv fallback support."""
    normalized = fmt.lower()
    if normalized not in {"parquet", "csv", "auto"}:
        raise ValueError("fmt must be one of: parquet, csv, auto")

    if normalized in {"parquet", "auto"}:
        try:
            import pandas as pd  # type: ignore

            frame = pd.DataFrame(rows, columns=list(EXPORT_COLUMNS))
            output_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(output_path, index=False)
            return "parquet"
        except Exception:
            if normalized == "parquet":
                raise

    csv_path = output_path.with_suffix(".csv")
    write_csv_dataset(rows, csv_path)
    return "csv"
