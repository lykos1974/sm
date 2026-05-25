from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
PNF_ROOT = REPO_ROOT / "pnf_mvp"
if str(PNF_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_ROOT))

from patterns.poles import detect_pole_patterns


@dataclass
class CsvColumn:
    idx: int
    kind: str
    top: float
    bottom: float


def _load_columns(path: Path):
    columns = []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            columns.append(
                CsvColumn(
                    idx=int(row["idx"]),
                    kind=row["kind"].strip().upper(),
                    top=float(row["top"]),
                    bottom=float(row["bottom"]),
                )
            )
    return columns


def main():
    parser = argparse.ArgumentParser(description="Audit PnF high/low pole diagnostics")
    parser.add_argument("--input-columns-csv", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--box-size", type=float, default=1.0)
    args = parser.parse_args()

    input_path = Path(args.input_columns_csv)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    columns = _load_columns(input_path)
    patterns = detect_pole_patterns(columns, box_size=args.box_size)

    csv_path = output_root / "pole_patterns.csv"
    with csv_path.open("w", newline="") as f:
        fields = [
            "pattern_name",
            "status",
            "pole_column_index",
            "reversal_column_index",
            "pole_boxes",
            "retrace_boxes",
            "retrace_ratio",
            "breakout_excess_boxes",
            "direction_bias",
            "risk_note",
            "is_diagnostic_only",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(patterns)

    summary_path = output_root / "pole_summary.md"
    high_count = sum(1 for p in patterns if p["pattern_name"] == "HIGH_POLE")
    low_count = sum(1 for p in patterns if p["pattern_name"] == "LOW_POLE")
    with summary_path.open("w") as f:
        f.write("# Pole Pattern Audit\n\n")
        f.write("Poles are reversal diagnostics only and must not be treated as production entries without validation.\n\n")
        f.write(f"- Input columns: {len(columns)}\n")
        f.write(f"- Total pole diagnostics: {len(patterns)}\n")
        f.write(f"- High poles: {high_count}\n")
        f.write(f"- Low poles: {low_count}\n")


if __name__ == "__main__":
    main()
