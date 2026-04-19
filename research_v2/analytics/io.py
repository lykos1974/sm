from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable

from research_v2.analytics.schema import GROUPED_SUMMARY_COLUMNS


def read_labeled_dataset(input_path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    """Read frozen labeled dataset from parquet (preferred) or csv."""
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

    raise ValueError(f"Unsupported labeled dataset format: {input_path}")


def write_summary_csv(rows: Iterable[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(GROUPED_SUMMARY_COLUMNS), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_report_json(payload: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
