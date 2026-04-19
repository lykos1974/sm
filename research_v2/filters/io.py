from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def read_setup_dataset(input_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Read setup dataset artifact from parquet/csv preserving row structure."""
    suffix = input_path.suffix.lower()

    if suffix == ".parquet":
        import pandas as pd  # type: ignore

        frame = pd.read_parquet(input_path)
        return frame.to_dict(orient="records"), list(frame.columns)

    if suffix == ".csv":
        with input_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = [dict(row) for row in reader]
            return rows, list(reader.fieldnames or [])

    raise ValueError(f"Unsupported setup dataset format: {input_path}")


def write_setup_dataset(rows: list[dict[str, Any]], columns: list[str], output_path: Path, fmt: str) -> str:
    """Write filtered setup dataset preserving original column shape/order."""
    normalized = fmt.lower()
    if normalized not in {"parquet", "csv", "auto"}:
        raise ValueError("fmt must be one of: parquet, csv, auto")

    if normalized in {"parquet", "auto"}:
        try:
            import pandas as pd  # type: ignore

            frame = pd.DataFrame(rows)
            if columns:
                frame = frame.reindex(columns=columns)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(output_path, index=False)
            return "parquet"
        except Exception:
            if normalized == "parquet":
                raise

    csv_path = output_path.with_suffix(".csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    return "csv"
