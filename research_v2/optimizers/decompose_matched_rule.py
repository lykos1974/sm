from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

GROUP_FIELDS: tuple[str, ...] = (
    "symbol",
    "side",
    "status",
    "breakout_context",
    "pullback_quality",
    "trend_regime",
    "continuation_execution_class",
    "entry_distance_bucket",
    "active_leg_boxes",
    "quality_score",
)


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _u(value: Any) -> str:
    return _norm(value).upper()


def _group_key(field: str, value: Any) -> str:
    val = _norm(value)
    return val if val else "<EMPTY>"


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _sum_mean(values: Iterable[float]) -> tuple[int, float, float]:
    vals = list(values)
    if not vals:
        return 0, 0.0, 0.0
    total = sum(vals)
    return len(vals), total, total / len(vals)


def decompose_matched_rows(*, matched_rows_csv: str, output_root: str, rule_id: str | None = None) -> dict[str, Any]:
    input_path = Path(matched_rows_csv).resolve()
    rows = _load_rows(input_path)
    if not rows:
        raise ValueError("Matched rows CSV is empty.")

    out_root = Path(output_root).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    statuses = Counter(_u(r.get("resolution_status")) or "<EMPTY>" for r in rows)
    r_values = [_safe_float(r.get("realized_r_multiple")) for r in rows]
    r_values_clean = [v for v in r_values if v is not None]

    grouped: list[dict[str, Any]] = []
    for field in GROUP_FIELDS:
        buckets: defaultdict[str, list[float]] = defaultdict(list)
        for row in rows:
            r = _safe_float(row.get("realized_r_multiple"))
            if r is None:
                continue
            buckets[_group_key(field, row.get(field))].append(r)
        for bucket, vals in sorted(buckets.items(), key=lambda kv: (-(sum(kv[1])), kv[0])):
            count, total, mean = _sum_mean(vals)
            grouped.append(
                {
                    "dimension": field,
                    "bucket": bucket,
                    "count": count,
                    "sum_realized_r_multiple": round(total, 8),
                    "mean_realized_r_multiple": round(mean, 8),
                }
            )

    def _rows_with_res(status: str) -> list[dict[str, Any]]:
        return [r for r in rows if _u(r.get("resolution_status")) == status]

    tp2_rows = _rows_with_res("TP2")
    stopped_rows = _rows_with_res("STOPPED")

    contributors = [
        r
        for r in grouped
        if r["dimension"] != "quality_score"
    ]
    positive = [r for r in contributors if r["sum_realized_r_multiple"] > 0]
    negative = [r for r in contributors if r["sum_realized_r_multiple"] < 0]
    positive.sort(key=lambda r: (-r["sum_realized_r_multiple"], -r["count"], r["dimension"], r["bucket"]))
    negative.sort(key=lambda r: (r["sum_realized_r_multiple"], -r["count"], r["dimension"], r["bucket"]))

    symbol_totals: defaultdict[str, float] = defaultdict(float)
    for row in rows:
        r = _safe_float(row.get("realized_r_multiple"))
        if r is not None:
            symbol_totals[_group_key("symbol", row.get("symbol"))] += r
    total_r = sum(symbol_totals.values())
    abs_total_r = sum(abs(v) for v in symbol_totals.values())

    warnings: list[str] = []
    if abs_total_r > 0 and symbol_totals:
        top_symbol, top_r = max(symbol_totals.items(), key=lambda kv: abs(kv[1]))
        concentration = abs(top_r) / abs_total_r
        if concentration >= 0.7:
            warnings.append(
                f"Edge concentration warning: symbol '{top_symbol}' contributes {top_r:.4f} net R and {concentration:.1%} of absolute symbol PnL."
            )

    status_counts = Counter(_u(r.get("status")) for r in rows)
    candidate_count = status_counts.get("CANDIDATE", 0)
    watch_count = status_counts.get("WATCH", 0)
    if watch_count > candidate_count:
        warnings.append(
            f"Status concentration warning: WATCH rows ({watch_count}) exceed CANDIDATE rows ({candidate_count})."
        )

    quality_values = {_norm(r.get("quality_score")) for r in rows if _norm(r.get("quality_score")) != ""}
    if len(quality_values) <= 1:
        warnings.append("quality_score appears constant or missing, so it is likely non-informative for decomposition.")

    summary_path = out_root / "decomposition_summary.md"
    tables_path = out_root / "decomposition_tables.csv"
    pos_path = out_root / "positive_contributors.csv"
    neg_path = out_root / "negative_contributors.csv"

    with tables_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = ["dimension", "bucket", "count", "sum_realized_r_multiple", "mean_realized_r_multiple"]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(grouped)

    for target, rows_out in ((pos_path, positive[:25]), (neg_path, negative[:25])):
        with target.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = ["dimension", "bucket", "count", "sum_realized_r_multiple", "mean_realized_r_multiple"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_out)

    count_all, sum_all, mean_all = _sum_mean(r_values_clean)
    count_tp2, sum_tp2, mean_tp2 = _sum_mean([_safe_float(r.get("realized_r_multiple")) or 0.0 for r in tp2_rows])
    count_stopped, sum_stopped, mean_stopped = _sum_mean([_safe_float(r.get("realized_r_multiple")) or 0.0 for r in stopped_rows])

    summary_lines = [
        "# Matched Rule Decomposition",
        "",
        "Research-only analytics. No strategy/live/validation behavior modified.",
        "",
        f"- rule_id: {rule_id or '<unspecified>'}",
        f"- input_csv: {input_path}",
        f"- matched_rows: {len(rows)}",
        f"- realized_r_count: {count_all}",
        f"- realized_r_sum: {sum_all:.4f}",
        f"- realized_r_mean: {mean_all:.4f}",
        "",
        "## Resolution Status Breakdown",
        "",
    ]
    for k, v in sorted(statuses.items(), key=lambda kv: (-kv[1], kv[0])):
        summary_lines.append(f"- {k}: {v}")

    summary_lines.extend(
        [
            "",
            "## TP2 vs STOPPED",
            "",
            f"- TP2 count/sum/mean: {count_tp2} / {sum_tp2:.4f} / {mean_tp2:.4f}",
            f"- STOPPED count/sum/mean: {count_stopped} / {sum_stopped:.4f} / {mean_stopped:.4f}",
            "",
            "## Warnings",
            "",
        ]
    )
    if warnings:
        summary_lines.extend(f"- {w}" for w in warnings)
    else:
        summary_lines.append("- None")

    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {
        "summary_path": str(summary_path),
        "tables_path": str(tables_path),
        "positive_path": str(pos_path),
        "negative_path": str(neg_path),
        "warning_count": len(warnings),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only decomposition of optimizer matched rows.")
    parser.add_argument("--matched-rows-csv", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--rule-id", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = decompose_matched_rows(
        matched_rows_csv=args.matched_rows_csv,
        output_root=args.output_root,
        rule_id=args.rule_id,
    )
    for k, v in result.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
