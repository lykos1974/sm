from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

INCLUDED_RESOLUTIONS = {"TP2", "STOPPED"}
CLUSTER_FIELDS: tuple[str, ...] = (
    "active_leg_boxes",
    "entry_distance_bucket",
    "recurring_count_bucket",
    "pullback_quality",
    "trend_regime",
    "side",
    "symbol",
)
TRACKED_SYMBOLS: tuple[str, ...] = ("ETHUSDT", "BTCUSDT", "SOLUSDT")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _norm_intish(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return str(int(float(raw)))
    except (TypeError, ValueError):
        return raw


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _recurring_bucket_from_count(value: Any) -> str:
    c = int(_safe_float(value, default=0.0))
    if c <= 1:
        return "1"
    if c == 2:
        return "2"
    if c == 3:
        return "3"
    if c == 4:
        return "4"
    return "5+"


def _normalized_geometry(row: dict[str, Any]) -> dict[str, str]:
    recurring_bucket = (row.get("recurring_count_bucket") or "").strip()
    if not recurring_bucket:
        recurring_bucket = _recurring_bucket_from_count(row.get("recurring_match_count", "0"))

    return {
        "active_leg_boxes": _norm_intish(row.get("active_leg_boxes")),
        "entry_distance_bucket": _norm(row.get("entry_distance_bucket")),
        "recurring_count_bucket": _norm(recurring_bucket),
        "pullback_quality": _norm(row.get("pullback_quality")),
        "trend_regime": _norm(row.get("trend_regime")),
        "side": _norm(row.get("side")),
        "symbol": _norm(row.get("symbol")),
    }


def _build_cluster_key(row: dict[str, Any]) -> tuple[str, ...]:
    normalized = _normalized_geometry(row)
    return tuple(normalized[f] for f in CLUSTER_FIELDS)


def _select_failure_clusters(rows: list[dict[str, Any]], *, top_n: int, min_cluster_size: int) -> list[dict[str, Any]]:
    eligible = [r for r in rows if int(_safe_float(r.get("count", 0), default=0)) >= min_cluster_size]
    eligible.sort(
        key=lambda r: (
            -_safe_float(r.get("stopped_lift", 0.0), default=0.0),
            -int(_safe_float(r.get("count", 0), default=0)),
        )
    )
    return eligible[:top_n]


def _ratio(rows: list[dict[str, Any]], resolution: str) -> float:
    if not rows:
        return 0.0
    return sum(1 for r in rows if _norm(r.get("resolution_status")) == resolution) / len(rows)


def _metric_block(rows: list[dict[str, Any]]) -> dict[str, float | int]:
    return {
        "rows": len(rows),
        "tp2_ratio": _ratio(rows, "TP2"),
        "stopped_ratio": _ratio(rows, "STOPPED"),
        "mean_realized_r_multiple": mean([_safe_float(r.get("realized_r_multiple", 0.0), default=0.0) for r in rows]) if rows else 0.0,
        "tp2_count": sum(1 for r in rows if _norm(r.get("resolution_status")) == "TP2"),
        "stopped_count": sum(1 for r in rows if _norm(r.get("resolution_status")) == "STOPPED"),
    }


def _split_effect(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row.get(field, "") or "").strip()].append(row)
    out: list[dict[str, Any]] = []
    for value, members in sorted(grouped.items()):
        metrics = _metric_block(members)
        out.append({"dimension": field, "value": value, **metrics})
    return out


def validate_failure_filters(*, recurring_rows_csv: str, failure_clusters_csv: str, output_root: str, top_n_failure_clusters: int = 10, min_cluster_size: int = 10) -> dict[str, Any]:
    recurring_rows = _load_rows(Path(recurring_rows_csv).resolve())
    failure_clusters = _load_rows(Path(failure_clusters_csv).resolve())
    out_dir = Path(output_root).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    recurring_columns = sorted(recurring_rows[0].keys()) if recurring_rows else []
    failure_columns = sorted(failure_clusters[0].keys()) if failure_clusters else []

    population = [r for r in recurring_rows if _norm(r.get("resolution_status")) in INCLUDED_RESOLUTIONS]
    if not population:
        raise ValueError("No TP2/STOPPED rows found in recurring_rows input.")

    selected_clusters = _select_failure_clusters(
        failure_clusters,
        top_n=top_n_failure_clusters,
        min_cluster_size=min_cluster_size,
    )
    selected_keys = {_build_cluster_key(row) for row in selected_clusters}
    population_keys = [_build_cluster_key(r) for r in population]
    matched_keys = {key for key in population_keys if key in selected_keys}

    excluded_rows = [r for r, key in zip(population, population_keys) if key in selected_keys]
    retained_rows = [r for r, key in zip(population, population_keys) if key not in selected_keys]

    before = _metric_block(population)
    after = _metric_block(retained_rows)

    filter_effects = [
        {
            "metric": "tp2_ratio",
            "before": before["tp2_ratio"],
            "after": after["tp2_ratio"],
            "delta": after["tp2_ratio"] - before["tp2_ratio"],
        },
        {
            "metric": "stopped_ratio",
            "before": before["stopped_ratio"],
            "after": after["stopped_ratio"],
            "delta": after["stopped_ratio"] - before["stopped_ratio"],
        },
        {
            "metric": "mean_realized_r_multiple",
            "before": before["mean_realized_r_multiple"],
            "after": after["mean_realized_r_multiple"],
            "delta": after["mean_realized_r_multiple"] - before["mean_realized_r_multiple"],
        },
        {"metric": "rows", "before": before["rows"], "after": after["rows"], "delta": after["rows"] - before["rows"]},
        {"metric": "tp2_count_retained", "before": before["tp2_count"], "after": after["tp2_count"], "delta": after["tp2_count"] - before["tp2_count"]},
        {"metric": "tp2_count_removed", "before": 0, "after": before["tp2_count"] - after["tp2_count"], "delta": before["tp2_count"] - after["tp2_count"]},
        {"metric": "stopped_count_removed", "before": 0, "after": before["stopped_count"] - after["stopped_count"], "delta": before["stopped_count"] - after["stopped_count"]},
    ]

    side_before = {r["value"]: r for r in _split_effect(population, "side")}
    side_after = {r["value"]: r for r in _split_effect(retained_rows, "side")}
    symbol_before = {r["value"]: r for r in _split_effect(population, "symbol")}
    symbol_after = {r["value"]: r for r in _split_effect(retained_rows, "symbol")}

    warnings: list[str] = []
    removed_share = len(excluded_rows) / len(population)
    if removed_share > 0.5:
        warnings.append(f"over-filtering warning: removed {removed_share:.1%} of rows.")
    if len(retained_rows) < max(20, int(0.25 * len(population))):
        warnings.append("tiny remaining population warning: retained sample likely too small for robust inference.")
    tp2_removed = before["tp2_count"] - after["tp2_count"]
    if before["tp2_count"] > 0 and (tp2_removed / before["tp2_count"]) > 0.35:
        warnings.append("TP2 destruction warning: filter removes a large share of TP2 outcomes.")
    excluded_symbol_counts = Counter(_norm(r.get("symbol")) for r in excluded_rows)
    if excluded_rows:
        top_symbol, top_count = excluded_symbol_counts.most_common(1)[0]
        if (top_count / len(excluded_rows)) > 0.7:
            warnings.append(f"symbol concentration warning: excluded set is {top_count/len(excluded_rows):.1%} {top_symbol}.")
    if selected_clusters and not excluded_rows:
        warnings.append(
            "CRITICAL matching warning: selected failure clusters excluded zero rows; verify schema/value compatibility."
        )

    summary_lines = [
        "# Failure Filter Validation Summary",
        "",
        f"Input TP2/STOPPED population: {len(population)} rows",
        f"Selected failure clusters: {len(selected_clusters)} (top_n={top_n_failure_clusters}, min_cluster_size={min_cluster_size})",
        f"Excluded rows: {len(excluded_rows)} ({removed_share:.1%})",
        f"Recurring rows columns: {', '.join(recurring_columns) if recurring_columns else 'none'}",
        f"Failure clusters columns: {', '.join(failure_columns) if failure_columns else 'none'}",
        f"Selected keys count: {len(selected_keys)}",
        f"Matched keys count: {len(matched_keys)}",
        "First 5 selected cluster normalized keys:",
        *[f"- {key}" for key in list(sorted(selected_keys))[:5]],
        "First 5 matched normalized keys:",
        *[f"- {key}" for key in list(sorted(matched_keys))[:5]],
        "",
        "## BEFORE vs AFTER",
        f"- TP2 ratio: {before['tp2_ratio']:.4f} -> {after['tp2_ratio']:.4f}",
        f"- STOPPED ratio: {before['stopped_ratio']:.4f} -> {after['stopped_ratio']:.4f}",
        f"- mean realized_r_multiple: {before['mean_realized_r_multiple']:.4f} -> {after['mean_realized_r_multiple']:.4f}",
        f"- TP2 count retained: {after['tp2_count']} / {before['tp2_count']}",
        f"- TP2 count removed: {before['tp2_count'] - after['tp2_count']}",
        f"- STOPPED count removed: {before['stopped_count'] - after['stopped_count']}",
        "",
        "## LONG vs SHORT effect",
    ]
    for side in sorted(set(side_before) | set(side_after)):
        b = side_before.get(side, {"rows": 0, "tp2_ratio": 0.0, "stopped_ratio": 0.0})
        a = side_after.get(side, {"rows": 0, "tp2_ratio": 0.0, "stopped_ratio": 0.0})
        summary_lines.append(f"- {side or 'UNKNOWN'}: n {b['rows']} -> {a['rows']}, tp2_ratio {b['tp2_ratio']:.4f} -> {a['tp2_ratio']:.4f}")

    summary_lines += ["", "## ETH vs BTC vs SOL effect"]
    for sym in TRACKED_SYMBOLS:
        b = symbol_before.get(sym, {"rows": 0, "tp2_ratio": 0.0})
        a = symbol_after.get(sym, {"rows": 0, "tp2_ratio": 0.0})
        summary_lines.append(f"- {sym}: n {b['rows']} -> {a['rows']}, tp2_ratio {b['tp2_ratio']:.4f} -> {a['tp2_ratio']:.4f}")

    summary_lines += ["", "## Most useful excluded failure geometries"]
    for idx, row in enumerate(selected_clusters[:10], start=1):
        summary_lines.append(
            f"- {idx}. stopped_lift={_safe_float(row.get('stopped_lift', 0.0)):.4f} count={int(_safe_float(row.get('count', 0), default=0))} | "
            f"legs={row.get('active_leg_boxes','')} entry={row.get('entry_distance_bucket','')} recur={row.get('recurring_count_bucket','')} quality={row.get('pullback_quality','')} trend={row.get('trend_regime','')} side={row.get('side','')} symbol={row.get('symbol','')}"
        )

    statistically_meaningful = len(retained_rows) >= 50 and abs(after["tp2_ratio"] - before["tp2_ratio"]) >= 0.03
    summary_lines += [
        "",
        "## Targeted research answers",
        f"- Do failure filters improve continuation quality? {'YES' if after['tp2_ratio'] > before['tp2_ratio'] and after['mean_realized_r_multiple'] >= before['mean_realized_r_multiple'] else 'NO / MIXED'}.",
        f"- Are gains statistically meaningful or tiny-sample artifacts? {'Potentially meaningful' if statistically_meaningful else 'Likely fragile / sample-limited'} (retained_n={len(retained_rows)}).",
        "- Which failure geometries are most useful to exclude? The highest STOPPED-lift, adequately sized clusters listed above.",
        "- Does exclusion help LONG and SHORT equally? See side-level deltas; asymmetry indicates regime-specific benefit.",
        f"- Is the retained population structurally cleaner? {'Likely yes' if after['stopped_ratio'] < before['stopped_ratio'] else 'Not clearly'} based on STOPPED-ratio shift.",
        f"- Are we removing mostly noise or removing valuable TP2s too? Removed TP2 share={(tp2_removed / max(before['tp2_count'], 1)):.1%}.",
        "",
        "## Diagnostics",
    ]
    summary_lines.extend([f"- {w}" for w in warnings] or ["- no major warnings triggered"])

    outputs = {
        "failure_filter_summary_md": out_dir / "failure_filter_summary.md",
        "excluded_rows_csv": out_dir / "excluded_rows.csv",
        "retained_rows_csv": out_dir / "retained_rows.csv",
        "filter_effects_csv": out_dir / "filter_effects.csv",
    }

    outputs["failure_filter_summary_md"].write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    for path, rows in ((outputs["excluded_rows_csv"], excluded_rows), (outputs["retained_rows_csv"], retained_rows), (outputs["filter_effects_csv"], filter_effects)):
        with path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = list(rows[0].keys()) if rows else []
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            if fieldnames:
                writer.writeheader()
                writer.writerows(rows)

    return {
        "input_rows": len(recurring_rows),
        "included_rows": len(population),
        "selected_failure_clusters": len(selected_clusters),
        "excluded_rows": len(excluded_rows),
        "retained_rows": len(retained_rows),
        **{k: str(v) for k, v in outputs.items()},
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only failure-filter validator on recurring rows.")
    parser.add_argument("--recurring-rows-csv", required=True)
    parser.add_argument("--failure-clusters-csv", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--top-n-failure-clusters", type=int, default=10)
    parser.add_argument("--min-cluster-size", type=int, default=10)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = validate_failure_filters(
        recurring_rows_csv=args.recurring_rows_csv,
        failure_clusters_csv=args.failure_clusters_csv,
        output_root=args.output_root,
        top_n_failure_clusters=args.top_n_failure_clusters,
        min_cluster_size=args.min_cluster_size,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
