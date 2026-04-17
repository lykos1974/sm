from __future__ import annotations

import argparse
import csv
from pathlib import Path

ELIGIBLE_STATUSES = {"WATCH", "CANDIDATE"}

FIELD_ORDER = [
    "symbol",
    "reference_ts",
    "status",
    "side",
    "strategy",
    "zone_low",
    "zone_high",
    "ideal_entry",
    "invalidation",
    "risk",
    "tp1",
    "tp2",
    "rr1",
    "rr2",
    "reason",
    "reject_reason",
    "quality_score",
    "quality_grade",
    "pullback_quality",
    "risk_quality",
    "reward_quality",
    "trend_state",
    "trend_regime",
    "immediate_slope",
    "breakout_context",
    "market_state",
    "latest_signal_name",
    "active_leg_boxes",
    "support_level",
    "resistance_level",
    "is_extended_move",
    "current_column_index",
]


def build_simulation_input(source_csv: str, output_csv: str) -> dict:
    src = Path(source_csv)
    if not src.exists():
        raise FileNotFoundError(f"Source CSV not found: {src.resolve()}")

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    eligible_rows = 0

    with src.open("r", encoding="utf-8-sig", newline="") as fin, out.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=FIELD_ORDER)
        writer.writeheader()

        for row in reader:
            total_rows += 1
            status = str(row.get("status") or "").upper()
            if status not in ELIGIBLE_STATUSES:
                continue

            eligible_rows += 1
            out_row = {k: row.get(k, "") for k in FIELD_ORDER}
            writer.writerow(out_row)

    return {
        "source_csv": str(src.resolve()),
        "output_csv": str(out.resolve()),
        "total_rows": total_rows,
        "eligible_rows": eligible_rows,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build lean simulation input CSV from full generated setups CSV")
    p.add_argument("--source-csv", required=True)
    p.add_argument("--output-csv", required=True)
    return p


def main() -> int:
    args = build_parser().parse_args()
    result = build_simulation_input(args.source_csv, args.output_csv)
    print(f"source_csv={result['source_csv']}")
    print(f"output_csv={result['output_csv']}")
    print(f"total_rows={result['total_rows']}")
    print(f"eligible_rows={result['eligible_rows']}")
    print("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
