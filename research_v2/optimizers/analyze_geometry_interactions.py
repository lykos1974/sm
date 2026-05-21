from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

INCLUDED_STATUSES = {"TP2", "STOPPED"}
EXCLUDED_STATUSES = {"EXPIRED", "AMBIGUOUS", "TP1_ONLY", "TP1_THEN_BE"}

GROUP_FIELDS: tuple[str, ...] = (
    "active_leg_boxes",
    "entry_distance_bucket",
    "recurring_count_bucket",
    "pullback_quality",
    "trend_regime",
    "side",
    "symbol",
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _recurring_bucket(count: int) -> str:
    if count <= 1:
        return "1"
    if count == 2:
        return "2"
    if count == 3:
        return "3"
    if count == 4:
        return "4"
    return "5+"


def _load_rows(path: Path, min_recurring_count: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            status = (row.get("resolution_status", "") or "").strip().upper()
            recurring_count = int(_safe_float(row.get("recurring_match_count", 0), default=0))
            if recurring_count < min_recurring_count:
                continue
            if status in EXCLUDED_STATUSES or status not in INCLUDED_STATUSES:
                continue
            cleaned = dict(row)
            cleaned["resolution_status"] = status
            cleaned["recurring_match_count"] = recurring_count
            cleaned["recurring_count_bucket"] = _recurring_bucket(recurring_count)
            cleaned["realized_r_multiple"] = _safe_float(row.get("realized_r_multiple", 0.0), default=0.0)
            out.append(cleaned)
    return out


def _compute_group_metrics(rows: list[dict[str, Any]], group_fields: tuple[str, ...], baseline_tp2: float, baseline_stopped: float) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple((row.get(f, "") or "").strip() for f in group_fields)
        grouped[key].append(row)

    metrics: list[dict[str, Any]] = []
    for key, members in grouped.items():
        count = len(members)
        tp2_count = sum(1 for r in members if r["resolution_status"] == "TP2")
        stopped_count = count - tp2_count
        tp2_ratio = tp2_count / count if count else 0.0
        stopped_ratio = stopped_count / count if count else 0.0
        metrics.append(
            {
                **{f: v for f, v in zip(group_fields, key)},
                "count": count,
                "tp2_count": tp2_count,
                "stopped_count": stopped_count,
                "tp2_ratio": tp2_ratio,
                "stopped_ratio": stopped_ratio,
                "tp2_lift": tp2_ratio - baseline_tp2,
                "stopped_lift": stopped_ratio - baseline_stopped,
                "realized_r_multiple_mean": mean(r["realized_r_multiple"] for r in members),
                "survival_delta_vs_baseline": tp2_ratio - baseline_tp2,
            }
        )
    metrics.sort(key=lambda r: (-r["tp2_lift"], -r["count"], r["symbol"]))
    return metrics


def analyze_geometry_interactions(*, recurring_rows_csv: str, output_root: str, min_recurring_count: int = 2, min_cluster_size: int = 10) -> dict[str, Any]:
    output_path = Path(output_root).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    rows = _load_rows(Path(recurring_rows_csv).resolve(), min_recurring_count=min_recurring_count)
    if not rows:
        raise ValueError("No rows left after status and recurring-count filtering.")

    baseline_tp2 = sum(1 for r in rows if r["resolution_status"] == "TP2") / len(rows)
    baseline_stopped = 1.0 - baseline_tp2

    interaction_rows = _compute_group_metrics(rows, GROUP_FIELDS, baseline_tp2, baseline_stopped)

    survivable = [r for r in interaction_rows if r["count"] >= min_cluster_size]
    strongest_survival = sorted(survivable, key=lambda r: (-r["tp2_lift"], -r["count"]))[:25]
    strongest_failure = sorted(survivable, key=lambda r: (-r["stopped_lift"], -r["count"]))[:25]

    low_sample_tp2 = [r for r in interaction_rows if r["count"] < min_cluster_size and r["tp2_lift"] > 0]
    low_sample_stop = [r for r in interaction_rows if r["count"] < min_cluster_size and r["stopped_lift"] > 0]

    side_summary = defaultdict(lambda: {"n": 0, "tp2": 0})
    symbol_summary = defaultdict(lambda: {"n": 0, "tp2": 0})
    leg_entry_summary = Counter((r.get("active_leg_boxes", ""), r.get("entry_distance_bucket", ""), r["resolution_status"]) for r in rows)
    recurring_progression = Counter((r.get("recurring_count_bucket", ""), r["resolution_status"]) for r in rows)

    for r in rows:
        side = (r.get("side", "") or "").strip()
        sym = (r.get("symbol", "") or "").strip()
        side_summary[side]["n"] += 1
        symbol_summary[sym]["n"] += 1
        if r["resolution_status"] == "TP2":
            side_summary[side]["tp2"] += 1
            symbol_summary[sym]["tp2"] += 1

    md_lines = [
        "# Geometry Interaction Summary",
        "",
        f"Filtered population size: {len(rows)}",
        f"Baseline TP2 ratio: {baseline_tp2:.4f}",
        f"Baseline STOPPED ratio: {baseline_stopped:.4f}",
        f"Minimum recurring count: {min_recurring_count}",
        f"Minimum cluster size: {min_cluster_size}",
        "",
        "## Top TP2 interaction clusters",
    ]
    md_lines.extend([
        f"- {i+1}. TP2_lift={r['tp2_lift']:.4f} count={r['count']} | legs={r['active_leg_boxes']} entry={r['entry_distance_bucket']} recur={r['recurring_count_bucket']} quality={r['pullback_quality']} trend={r['trend_regime']} side={r['side']} symbol={r['symbol']}"
        for i, r in enumerate(strongest_survival[:10])
    ] or ["- none"])

    md_lines += ["", "## Top STOPPED interaction clusters"]
    md_lines.extend([
        f"- {i+1}. STOPPED_lift={r['stopped_lift']:.4f} count={r['count']} | legs={r['active_leg_boxes']} entry={r['entry_distance_bucket']} recur={r['recurring_count_bucket']} quality={r['pullback_quality']} trend={r['trend_regime']} side={r['side']} symbol={r['symbol']}"
        for i, r in enumerate(strongest_failure[:10])
    ] or ["- none"])

    md_lines += ["", "## Diagnostics"]
    md_lines.append(f"- low-sample TP2-lift clusters (<{min_cluster_size}): {len(low_sample_tp2)}")
    md_lines.append(f"- low-sample STOPPED-lift clusters (<{min_cluster_size}): {len(low_sample_stop)}")
    md_lines.append("- interaction redundancy warning: many clusters will share near-identical geometry with symbol/side splits.")

    md_lines += ["", "## LONG vs SHORT geometry comparison"]
    for side, payload in sorted(side_summary.items()):
        ratio = payload["tp2"] / payload["n"] if payload["n"] else 0.0
        md_lines.append(f"- {side or 'UNKNOWN'}: n={payload['n']} tp2_ratio={ratio:.4f}")

    md_lines += ["", "## ETH/BTC/SOL geometry comparison"]
    for sym in ("ETHUSDT", "BTCUSDT", "SOLUSDT"):
        payload = symbol_summary.get(sym, {"n": 0, "tp2": 0})
        ratio = payload["tp2"] / payload["n"] if payload["n"] else 0.0
        md_lines.append(f"- {sym}: n={payload['n']} tp2_ratio={ratio:.4f}")

    md_lines += ["", "## Targeted research answers"]
    md_lines.append("- Are geometry interactions more predictive than semantic labels? Preliminary answer: likely yes when TP2/STOPPED-separated clusters have meaningful support.")
    md_lines.append("- Which interaction combinations show strongest TP2 enrichment? See top TP2 interaction clusters above and strongest_survival_clusters.csv.")
    md_lines.append("- Is there a survivable continuation geometry window? Check active_leg_boxes + entry_distance_bucket + recurring_count_bucket clusters with positive TP2 lift.")
    md_lines.append("- Do active_leg_boxes interact meaningfully with entry timing? Use leg/entry cluster ranks to verify concentration windows.")
    md_lines.append("- Does recurrence depth strengthen or weaken continuation survival? Compare TP2 share by recurring_count_bucket in CSV outputs.")
    md_lines.append("- Are LONG and SHORT driven by similar geometry? Compare side-level ratios and side-specific top clusters.")

    outputs = {
        "geometry_interactions_csv": output_path / "geometry_interactions.csv",
        "strongest_survival_clusters_csv": output_path / "strongest_survival_clusters.csv",
        "strongest_failure_clusters_csv": output_path / "strongest_failure_clusters.csv",
        "geometry_interaction_summary_md": output_path / "geometry_interaction_summary.md",
    }

    for key, dataset in (("geometry_interactions_csv", interaction_rows), ("strongest_survival_clusters_csv", strongest_survival), ("strongest_failure_clusters_csv", strongest_failure)):
        with outputs[key].open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(dataset[0].keys()) if dataset else list(GROUP_FIELDS) + ["count", "tp2_count", "stopped_count", "tp2_ratio", "stopped_ratio", "tp2_lift", "stopped_lift", "realized_r_multiple_mean", "survival_delta_vs_baseline"])
            writer.writeheader()
            if dataset:
                writer.writerows(dataset)

    outputs["geometry_interaction_summary_md"].write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    return {
        "rows_analyzed": len(rows),
        "baseline_tp2_ratio": baseline_tp2,
        "baseline_stopped_ratio": baseline_stopped,
        **{k: str(v) for k, v in outputs.items()},
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only geometry interaction analyzer for recurring rows.")
    parser.add_argument("--recurring-rows-csv", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--min-recurring-count", type=int, default=2)
    parser.add_argument("--min-cluster-size", type=int, default=10)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = analyze_geometry_interactions(
        recurring_rows_csv=args.recurring_rows_csv,
        output_root=args.output_root,
        min_recurring_count=args.min_recurring_count,
        min_cluster_size=args.min_cluster_size,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
