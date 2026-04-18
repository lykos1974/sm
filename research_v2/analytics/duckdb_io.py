from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def connect_duckdb(database: str = ":memory:"):
    """Create a DuckDB connection (lazy import keeps module importable without duckdb)."""
    try:
        import duckdb  # type: ignore
    except Exception as exc:
        raise RuntimeError("DuckDB engine requires the 'duckdb' package to be installed") from exc

    return duckdb.connect(database=database)


def register_labeled_dataset_view(conn: Any, input_path: Path, view_name: str = "labeled") -> None:
    suffix = input_path.suffix.lower()
    if suffix == ".parquet":
        conn.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet(?)",
            [str(input_path)],
        )
        return

    if suffix == ".csv":
        conn.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_csv_auto(?, HEADER=TRUE)",
            [str(input_path)],
        )
        return

    raise ValueError(f"Unsupported dataset format for DuckDB path: {input_path}")


def query_rows(conn: Any, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, params or [])
    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(col_names, row)) for row in rows]


def write_rows_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
