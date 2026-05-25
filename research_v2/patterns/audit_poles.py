from __future__ import annotations

import argparse
import csv
import statistics
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
    if not path.exists():
        raise FileNotFoundError(
            "Input columns CSV not found. Generate it first with research_v2.patterns.export_pnf_columns."
        )
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
    parser.add_argument("--max-opposing-distance-columns", type=int, default=4)
    parser.add_argument("--max-early-retrace-ratio", type=float, default=1.0)
    parser.add_argument("--min-pole-boxes", type=int, default=6)
    parser.add_argument("--min-breakout-excess-boxes", type=int, default=3)
    args = parser.parse_args()

    input_path = Path(args.input_columns_csv)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    columns = _load_columns(input_path)
    patterns = detect_pole_patterns(
        columns,
        box_size=args.box_size,
        min_breakout_excess_boxes=args.min_breakout_excess_boxes,
        min_pole_boxes_exclusive=args.min_pole_boxes - 1,
        max_early_retrace_ratio=args.max_early_retrace_ratio,
        max_opposing_distance_columns=args.max_opposing_distance_columns,
    )

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
            "opposing_pole_nearby",
            "opposing_pole_role",
            "opposing_pole_partner_index",
            "opposing_pole_distance_columns",
            "enhanced_by_opposing_pole",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(patterns)

    summary_path = output_root / "pole_summary.md"
    high_count = sum(1 for p in patterns if p["pattern_name"] == "HIGH_POLE")
    low_count = sum(1 for p in patterns if p["pattern_name"] == "LOW_POLE")
    early_count = sum(1 for p in patterns if p["status"] == "EARLY_50_RETRACE")
    over_count = sum(1 for p in patterns if p["status"] == "OVERRETRACE_POLE")
    opposing_sequences = sum(1 for p in patterns if p["opposing_pole_role"] == "SECOND_POLE")
    enhanced_count = sum(1 for p in patterns if p["enhanced_by_opposing_pole"])
    ratios = [p["retrace_ratio"] for p in patterns]
    mean_ratio = statistics.mean(ratios) if ratios else 0.0
    median_ratio = statistics.median(ratios) if ratios else 0.0

    with summary_path.open("w") as f:
        f.write("# Pole Pattern Audit\n\n")
        f.write("Poles and opposing poles are reversal diagnostics only. They are not production entries and must be validated against labeled outcomes before any trading use.\n\n")
        f.write(f"- Total columns: {len(columns)}\n")
        f.write(f"- Total poles: {len(patterns)}\n")
        f.write(f"- High poles: {high_count}\n")
        f.write(f"- Low poles: {low_count}\n")
        f.write(f"- Early 50% poles: {early_count}\n")
        f.write(f"- Overretrace poles: {over_count}\n")
        f.write(f"- Opposing pole sequences: {opposing_sequences}\n")
        f.write(f"- Enhanced second poles: {enhanced_count}\n")
        f.write(f"- Mean retrace ratio: {mean_ratio:.4f}\n")
        f.write(f"- Median retrace ratio: {median_ratio:.4f}\n")
        if patterns and over_count / len(patterns) > 0.5:
            f.write("- WARNING: Overretrace poles are dominant in this sample.\n")


if __name__ == "__main__":
    main()
