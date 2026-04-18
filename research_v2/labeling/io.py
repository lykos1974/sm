from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from research_v2.labeling.schema import LABEL_COLUMNS


def read_setup_dataset(input_path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    """Read frozen setup dataset from parquet (preferred) or csv."""
    suffix = input_path.suffix.lower()

    if suffix == ".parquet":
        import pandas as pd  # type: ignore

        frame = pd.read_parquet(input_path)
        if limit is not None:
            frame = frame.head(max(0, int(limit)))
        return frame.to_dict(orient="records")

    if suffix == ".csv":
        with input_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            rows: list[dict[str, Any]] = []
            for row in reader:
                rows.append(dict(row))
                if limit is not None and len(rows) >= int(limit):
                    break
            return rows

    raise ValueError(f"Unsupported input dataset format: {input_path}")


def load_settings(settings_path: Path) -> dict[str, Any]:
    with settings_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Settings file must contain a JSON object: {settings_path}")
    return payload


def resolve_candles_db_path(
    settings_path: Path | None,
    settings_payload: dict[str, Any] | None,
    candles_db_path_override: str | None,
) -> Path:
    if candles_db_path_override:
        candidate = Path(candles_db_path_override)
        return candidate if candidate.is_absolute() else candidate.resolve()

    if settings_path is None or settings_payload is None:
        raise ValueError("Either --candles-db-path or --settings-path with database_path is required")

    raw = settings_payload.get("database_path")
    if not raw:
        raise ValueError("settings file does not include 'database_path'; use --candles-db-path")

    candidate = Path(str(raw))
    if candidate.is_absolute():
        return candidate
    return (settings_path.parent / candidate).resolve()


def fetch_future_candles(
    db_path: Path,
    symbol: str,
    reference_ts: int,
    horizon_minutes: int,
    interval: str = "1m",
) -> list[dict[str, Any]]:
    """Load future candles in read-only mode for one symbol/setup horizon."""
    horizon_seconds = max(1, int(horizon_minutes)) * 60
    max_close_ts = int(reference_ts) + horizon_seconds

    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT close_time, high, low, close
            FROM candles
            WHERE symbol = ?
              AND interval = ?
              AND close_time > ?
              AND close_time <= ?
            ORDER BY open_time ASC
            """,
            (symbol, interval, int(reference_ts), max_close_ts),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def write_labels_csv(rows: Iterable[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(LABEL_COLUMNS), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_labels_dataset(rows: list[dict[str, Any]], output_path: Path, fmt: str) -> str:
    """Write labels dataset with parquet-first behavior and csv fallback support."""
    normalized = fmt.lower()
    if normalized not in {"parquet", "csv", "auto"}:
        raise ValueError("fmt must be one of: parquet, csv, auto")

    if normalized in {"parquet", "auto"}:
        try:
            import pandas as pd  # type: ignore

            frame = pd.DataFrame(rows, columns=list(LABEL_COLUMNS))
            output_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(output_path, index=False)
            return "parquet"
        except Exception:
            if normalized == "parquet":
                raise

    csv_path = output_path.with_suffix(".csv")
    write_labels_csv(rows, csv_path)
    return "csv"
