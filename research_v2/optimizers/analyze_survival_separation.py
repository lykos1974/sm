from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

SURVIVAL_STATUSES = {"TP2", "STOPPED"}
EXCLUDED_STATUSES = {"EXPIRED", "AMBIGUOUS", "TP1_ONLY", "TP1_THEN_BE"}
CATEGORICAL_FIELDS: tuple[str, ...] = (
    "symbol",
    "side",
    "status",
    "breakout_context",
    "pullback_quality",
    "trend_regime",
    "continuation_execution_class",
    "active_leg_boxes",
    "entry_distance_bucket",
)


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _value_bucket_quality(value: Any) -> str:
    q = _safe_float(value, default=float("nan"))
    if q != q:
        return ""
    if q < 0.4:
        return "Q0_<0.40"
    if q < 0.6:
        return "Q1_0.40_0.60"
    if q < 0.8:
        return "Q2_0.60_0.80"
    return "Q3_>=0.80"


def _value_bucket_recurrence(value: Any) -> str:
    cnt = int(_safe_float(value, default=0.0))
    if cnt <= 2:
        return "R2"
    if cnt == 3:
        return "R3"
    if cnt == 4:
        return "R4"
    return "R5_PLUS"


def _enrichment_rows(rows: list[dict[str, Any]], baseline_tp2_ratio: float, baseline_stopped_ratio: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for field in CATEGORICAL_FIELDS:
            grouped[(field, _norm(row.get(field, "")))].append(row)
        grouped[("quality_score", _value_bucket_quality(row.get("quality_score", "")))].append(row)
        grouped[("recurring_match_count", _value_bucket_recurrence(row.get("recurring_match_count", "")))].append(row)

    tp2_out: list[dict[str, Any]] = []
    stopped_out: list[dict[str, Any]] = []
    compare_out: list[dict[str, Any]] = []

    for (feature, value), members in sorted(grouped.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        count = len(members)
        tp2 = [r for r in members if _norm(r.get("resolution_status", "")).upper() == "TP2"]
        stopped = [r for r in members if _norm(r.get("resolution_status", "")).upper() == "STOPPED"]
        tp2_count = len(tp2)
        stopped_count = len(stopped)
        tp2_ratio = tp2_count / count if count else 0.0
        stopped_ratio = stopped_count / count if count else 0.0
        mean_r = sum(_safe_float(r.get("realized_r_multiple", "")) for r in members) / count if count else 0.0
        tp2_lift = tp2_ratio / baseline_tp2_ratio if baseline_tp2_ratio > 0 else 0.0
        stopped_lift = stopped_ratio / baseline_stopped_ratio if baseline_stopped_ratio > 0 else 0.0
        enrichment_tp2 = tp2_count / max(stopped_count, 1)
        enrichment_stopped = stopped_count / max(tp2_count, 1)

        base = {
            "feature": feature,
            "value": value,
            "count": count,
            "mean_realized_r_multiple": f"{mean_r:.6f}",
            "tp2_count": tp2_count,
            "stopped_count": stopped_count,
            "tp2_ratio": f"{tp2_ratio:.6f}",
            "stopped_ratio": f"{stopped_ratio:.6f}",
            "tp2_enrichment_ratio": f"{enrichment_tp2:.6f}",
            "stopped_enrichment_ratio": f"{enrichment_stopped:.6f}",
            "tp2_lift_vs_baseline": f"{tp2_lift:.6f}",
            "stopped_lift_vs_baseline": f"{stopped_lift:.6f}",
        }
        compare_out.append(base)
        if tp2_count > 0:
            tp2_out.append(base)
        if stopped_count > 0:
            stopped_out.append(base)

    tp2_out.sort(key=lambda r: (-_safe_float(r["tp2_lift_vs_baseline"]), -int(r["count"]), r["feature"], r["value"]))
    stopped_out.sort(key=lambda r: (-_safe_float(r["stopped_lift_vs_baseline"]), -int(r["count"]), r["feature"], r["value"]))
    compare_out.sort(key=lambda r: (r["feature"], r["value"]))
    return tp2_out, stopped_out, compare_out


def analyze_survival_separation(*, recurring_rows_csv: str, output_root: str, min_recurring_count: int = 2) -> dict[str, Any]:
    recurring_path = Path(recurring_rows_csv).resolve()
    output_path = Path(output_root).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    rows = _load_csv_rows(recurring_path)
    filtered = [
        r for r in rows
        if int(_safe_float(r.get("recurring_match_count", "0"), default=0.0)) >= min_recurring_count
    ]
    included = [r for r in filtered if _norm(r.get("resolution_status", "")).upper() in SURVIVAL_STATUSES]
    excluded = [r for r in filtered if _norm(r.get("resolution_status", "")).upper() in EXCLUDED_STATUSES]

    total = len(included)
    tp2 = [r for r in included if _norm(r.get("resolution_status", "")).upper() == "TP2"]
    stopped = [r for r in included if _norm(r.get("resolution_status", "")).upper() == "STOPPED"]
    tp2_ratio = len(tp2) / total if total else 0.0
    stopped_ratio = len(stopped) / total if total else 0.0

    tp2_enrichment, stopped_enrichment, comparison_rows = _enrichment_rows(included, tp2_ratio, stopped_ratio)

    symbol_outcomes: dict[str, tuple[int, int]] = {}
    for sym in ("ETH", "BTC", "SOL"):
        sym_rows = [r for r in included if _norm(r.get("symbol", "")).upper().startswith(sym)]
        sym_tp2 = sum(1 for r in sym_rows if _norm(r.get("resolution_status", "")).upper() == "TP2")
        symbol_outcomes[sym] = (len(sym_rows), sym_tp2)

    leg_box_stats = Counter((r.get("active_leg_boxes", ""), _norm(r.get("resolution_status", "")).upper()) for r in included)
    cont_stats = Counter((r.get("continuation_execution_class", ""), _norm(r.get("resolution_status", "")).upper()) for r in included)
    watch_rows = [r for r in included if _norm(r.get("status", "")).upper() == "WATCH"]
    watch_tp2 = sum(1 for r in watch_rows if _norm(r.get("resolution_status", "")).upper() == "TP2")

    rec_bucket_counts = Counter((_value_bucket_recurrence(r.get("recurring_match_count", "")), _norm(r.get("resolution_status", "")).upper()) for r in included)

    strongest_tp2 = [r for r in tp2_enrichment if int(r["count"]) >= 3][:10]
    strongest_stopped = [r for r in stopped_enrichment if int(r["count"]) >= 3][:10]
    ambiguous = [
        r for r in comparison_rows
        if int(r["count"]) >= 3 and abs(_safe_float(r["tp2_lift_vs_baseline"]) - 1.0) <= 0.10 and abs(_safe_float(r["stopped_lift_vs_baseline"]) - 1.0) <= 0.10
    ][:10]

    summary = [
        "# Survival Separation Analysis",
        "",
        f"Min recurring match count: {min_recurring_count}",
        f"Input rows: {len(rows)}",
        f"Rows after recurring filter: {len(filtered)}",
        f"Rows included in survival split (TP2/STOPPED): {len(included)}",
        f"Rows excluded (non-survival statuses): {len(excluded)}",
        f"Baseline TP2 ratio: {tp2_ratio:.3f}",
        f"Baseline STOPPED ratio: {stopped_ratio:.3f}",
        "",
        "## Diagnostics",
        "### ETH vs BTC vs SOL survival comparison",
    ]
    for sym, (count, sym_tp2) in symbol_outcomes.items():
        ratio = sym_tp2 / count if count else 0.0
        summary.append(f"- {sym}: count={count} TP2={sym_tp2} TP2_ratio={ratio:.3f}")

    summary.extend(["", "### active_leg_boxes survival progression"])
    for leg in sorted({k[0] for k in leg_box_stats}):
        t = leg_box_stats[(leg, "TP2")]
        s = leg_box_stats[(leg, "STOPPED")]
        total_leg = t + s
        summary.append(f"- active_leg_boxes={leg}: TP2={t} STOPPED={s} TP2_ratio={(t/total_leg):.3f}" if total_leg else f"- active_leg_boxes={leg}: none")

    summary.extend(["", "### WATCH persistence survival"])
    summary.append(f"- WATCH rows: {len(watch_rows)} TP2={watch_tp2} TP2_ratio={(watch_tp2/len(watch_rows)):.3f}" if watch_rows else "- WATCH rows: none")

    summary.extend(["", "### continuation_execution_class survival behavior"])
    for cls in sorted({k[0] for k in cont_stats}):
        t = cont_stats[(cls, "TP2")]
        s = cont_stats[(cls, "STOPPED")]
        tot = t + s
        summary.append(f"- class={cls}: TP2={t} STOPPED={s} TP2_ratio={(t/tot):.3f}" if tot else f"- class={cls}: none")

    summary.extend(["", "### recurring_match_count survival behavior"])
    for bucket in ("R2", "R3", "R4", "R5_PLUS"):
        t = rec_bucket_counts[(bucket, "TP2")]
        s = rec_bucket_counts[(bucket, "STOPPED")]
        tot = t + s
        if tot:
            summary.append(f"- {bucket}: TP2={t} STOPPED={s} TP2_ratio={(t/tot):.3f}")

    summary.extend(["", "## Strongest TP2-associated features"])
    summary.extend([f"- {r['feature']}={r['value']} lift={r['tp2_lift_vs_baseline']} count={r['count']}" for r in strongest_tp2] or ["- none"])
    summary.extend(["", "## Strongest STOPPED-associated features"])
    summary.extend([f"- {r['feature']}={r['value']} lift={r['stopped_lift_vs_baseline']} count={r['count']}" for r in strongest_stopped] or ["- none"])
    summary.extend(["", "## Ambiguous / non-separating features"])
    summary.extend([f"- {r['feature']}={r['value']} tp2_lift={r['tp2_lift_vs_baseline']} stopped_lift={r['stopped_lift_vs_baseline']} count={r['count']}" for r in ambiguous] or ["- none"])

    survivable = tp2_ratio >= 0.40
    similar = len(ambiguous) >= 3
    quality_informative = any(r["feature"] == "quality_score" and _safe_float(r["tp2_lift_vs_baseline"]) >= 1.15 for r in strongest_tp2)
    eth_concentrated = symbol_outcomes["ETH"][0] > max(symbol_outcomes["BTC"][0], symbol_outcomes["SOL"][0])
    r2 = rec_bucket_counts[("R2", "TP2")] / max(rec_bucket_counts[("R2", "TP2")] + rec_bucket_counts[("R2", "STOPPED")], 1)
    r5 = rec_bucket_counts[("R5_PLUS", "TP2")] / max(rec_bucket_counts[("R5_PLUS", "TP2")] + rec_bucket_counts[("R5_PLUS", "STOPPED")], 1)
    recurrence_corr = r5 > r2

    summary.extend([
        "",
        "## Research Questions",
        f"- Is there evidence of survivable continuation persistence? {'YES' if survivable else 'NO'}.",
        "- Which features most strongly separate TP2 from STOPPED? See strongest TP2/STOPPED sections above (ranked by lift).",
        f"- Are losers structurally similar to winners? {'YES' if similar else 'NO'}.",
        f"- Is current quality_score informative or useless? {'INFORMATIVE' if quality_informative else 'WEAK/USELESS'}.",
        f"- Is survival concentrated in ETH? {'YES' if eth_concentrated else 'NO'}.",
        f"- Does recurrence strength correlate with survival? {'YES' if recurrence_corr else 'NO'}.",
    ])

    summary_md = output_path / "survival_separation_summary.md"
    tp2_csv = output_path / "tp2_feature_enrichment.csv"
    stopped_csv = output_path / "stopped_feature_enrichment.csv"
    comparison_csv = output_path / "feature_comparison_table.csv"

    summary_md.write_text("\n".join(summary) + "\n", encoding="utf-8")
    for p, rows_out in ((tp2_csv, tp2_enrichment), (stopped_csv, stopped_enrichment), (comparison_csv, comparison_rows)):
        with p.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows_out[0].keys()) if rows_out else [])
            if rows_out:
                writer.writeheader()
                writer.writerows(rows_out)

    return {
        "input_rows": len(rows),
        "filtered_rows": len(filtered),
        "included_survival_rows": len(included),
        "excluded_non_survival_rows": len(excluded),
        "baseline_tp2_ratio": tp2_ratio,
        "baseline_stopped_ratio": stopped_ratio,
        "survival_separation_summary_md": str(summary_md),
        "tp2_feature_enrichment_csv": str(tp2_csv),
        "stopped_feature_enrichment_csv": str(stopped_csv),
        "feature_comparison_table_csv": str(comparison_csv),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only survival separation analyzer for recurring rows.")
    parser.add_argument("--recurring-rows-csv", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--min-recurring-count", type=int, default=2)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = analyze_survival_separation(
        recurring_rows_csv=args.recurring_rows_csv,
        output_root=args.output_root,
        min_recurring_count=args.min_recurring_count,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
