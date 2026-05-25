from __future__ import annotations

import argparse
import csv
from pathlib import Path

from research_v2.patterns.pole_outcomes import label_pole_outcomes, load_columns_csv, load_poles_csv


def _pct(n: int, d: int) -> float:
    return (100.0 * n / d) if d else 0.0


def _is_continuation(pattern: str, outcome: str) -> bool:
    return (pattern == "HIGH_POLE" and outcome == "BEARISH_CONTINUATION") or (
        pattern == "LOW_POLE" and outcome == "BULLISH_CONTINUATION"
    )


def _bucket_pole_size(v: float) -> str:
    if 6 <= v <= 8:
        return "6-8"
    if 9 <= v <= 12:
        return "9-12"
    if 13 <= v <= 20:
        return "13-20"
    return ">20"


def _bucket_retrace(v: float) -> str:
    if 0.50 <= v < 0.75:
        return "0.50-0.75"
    if 0.75 <= v < 1.00:
        return "0.75-1.00"
    if 1.00 <= v <= 1.50:
        return "1.00-1.50"
    return ">1.50"


def main():
    p = argparse.ArgumentParser(description="Research-only pole outcome labeling")
    p.add_argument("--input-columns-csv", required=True)
    p.add_argument("--input-poles-csv", required=True)
    p.add_argument("--output-root", required=True)
    p.add_argument("--future-columns", type=int, default=20)
    p.add_argument("--continuation-threshold-boxes", type=int, default=3)
    p.add_argument("--reversal-threshold-boxes", type=int, default=3)
    p.add_argument("--invalidation-threshold-boxes", type=int, default=3)
    p.add_argument("--box-size", type=float, default=1.0)
    args = p.parse_args()

    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    columns, inferred_box_size = load_columns_csv(Path(args.input_columns_csv))
    poles = load_poles_csv(Path(args.input_poles_csv))
    box_size = inferred_box_size if inferred_box_size is not None else args.box_size

    labeled = label_pole_outcomes(
        poles,
        columns,
        box_size=box_size,
        future_columns=args.future_columns,
        continuation_threshold_boxes=args.continuation_threshold_boxes,
        invalidation_threshold_boxes=args.invalidation_threshold_boxes,
    )

    out_csv = output_root / "pole_labeled_outcomes.csv"
    with out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(labeled[0].keys()) if labeled else [])
        if labeled:
            writer.writeheader()
            writer.writerows(labeled)

    summary = output_root / "pole_outcome_summary.md"
    with summary.open("w") as f:
        f.write("# Pole Outcome Summary (Research-Only)\n\n")
        f.write("This report is for diagnostics and quantitative research only. No live signaling or execution logic is generated.\n\n")

        for pattern in ("HIGH_POLE", "LOW_POLE"):
            subset = [r for r in labeled if r.get("pattern_name") == pattern]
            n = len(subset)
            cont_name = "BEARISH_CONTINUATION" if pattern == "HIGH_POLE" else "BULLISH_CONTINUATION"
            cont = sum(1 for r in subset if r.get("outcome_class") == cont_name)
            fail = sum(1 for r in subset if r.get("outcome_class") == "FAILED_REVERSAL")
            side = sum(1 for r in subset if r.get("outcome_class") == "SIDEWAYS")
            f.write(f"## {pattern} outcomes\n")
            f.write(f"- Total: {n}\n")
            f.write(f"- Continuation: {cont} ({_pct(cont,n):.2f}%)\n")
            f.write(f"- Failure: {fail} ({_pct(fail,n):.2f}%)\n")
            f.write(f"- Sideways: {side} ({_pct(side,n):.2f}%)\n\n")

        f.write("## Enhanced opposing pole analysis\n")
        for flag in ("True", "False"):
            subset = [r for r in labeled if str(r.get("enhanced_by_opposing_pole")) == flag]
            denom = len(subset)
            cont = sum(1 for r in subset if _is_continuation(r.get("pattern_name", ""), r.get("outcome_class", "")))
            f.write(f"- enhanced_by_opposing_pole={flag}: {cont}/{denom} continuation ({_pct(cont,denom):.2f}%)\n")

        f.write("\n## Pole size analysis\n")
        for bucket in ("6-8", "9-12", "13-20", ">20"):
            subset = [r for r in labeled if _bucket_pole_size(float(r.get("pole_boxes", 0))) == bucket]
            denom = len(subset)
            cont = sum(1 for r in subset if _is_continuation(r.get("pattern_name", ""), r.get("outcome_class", "")))
            f.write(f"- {bucket}: {cont}/{denom} continuation ({_pct(cont,denom):.2f}%)\n")

        f.write("\n## Retrace ratio analysis\n")
        for bucket in ("0.50-0.75", "0.75-1.00", "1.00-1.50", ">1.50"):
            subset = [r for r in labeled if _bucket_retrace(float(r.get("retrace_ratio", 0))) == bucket]
            denom = len(subset)
            cont = sum(1 for r in subset if _is_continuation(r.get("pattern_name", ""), r.get("outcome_class", "")))
            f.write(f"- {bucket}: {cont}/{denom} continuation ({_pct(cont,denom):.2f}%)\n")


if __name__ == "__main__":
    main()
