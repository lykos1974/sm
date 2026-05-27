from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

JOIN_KEYS = ("symbol", "timestamp", "pattern_name")
TIMESTAMP_KEYS = ("timestamp", "reversal_timestamp", "reference_ts", "reference_timestamp")
SYMBOL_KEYS = ("symbol", "asset", "ticker")

ALLOWED_FEATURES = (
    "pole_boxes",
    "pole_boxes_bucket",
    "retrace_boxes",
    "retrace_ratio",
    "retrace_ratio_bucket",
    "enhanced_by_opposing_pole",
    "opposing_pole_distance_columns",
)

FORBIDDEN_FEATURES = (
    "regime_class",
    "continuation_after_sideways",
    "volatility_compression_after_signal",
    "continuation_persistence_ge_1_box",
    "max_favorable_boxes_path",
    "max_adverse_boxes_path",
    "future_run",
)

CONTINUATION_OUTCOMES = {"BULLISH_CONTINUATION", "BEARISH_CONTINUATION"}


def _to_float(v: str | float | int | None, default: float = 0.0) -> float:
    if v is None:
        return default
    text = str(v).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return default
    return float(text)


def _safe_div(num: float, den: float) -> float:
    return (num / den) if den else 0.0


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _first_present(row: dict[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        text = str(row.get(key, "")).strip()
        if text and text.lower() not in {"none", "null", "nan"}:
            return text
    return ""


def _join_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        _first_present(row, SYMBOL_KEYS),
        _first_present(row, TIMESTAMP_KEYS),
        str(row.get("pattern_name", "")).strip(),
    )


def _compute_scores(sample_size: int, continuation_pct: float, failure_pct: float, asymmetry: float) -> tuple[float, float]:
    sample_weight = min(1.0, sample_size / 100.0)
    asymmetry_score = asymmetry
    stability_score = (continuation_pct * 0.60) - (failure_pct * 0.50)
    expectancy_score = (((continuation_pct * 0.45) + (asymmetry_score * 0.25) + (stability_score * 0.30)) * sample_weight)
    return round(expectancy_score, 6), round(asymmetry_score, 6)


def _clean_value(row: dict[str, str], key: str) -> str:
    text = str(row.get(key, "")).strip()
    if not text:
        return "NA"
    return text


def main() -> None:
    ap = argparse.ArgumentParser(description="Live-safe causal expectancy mining for pole patterns (research-only)")
    ap.add_argument("--input-labeled-csv", required=True)
    ap.add_argument("--input-btc-columns-csv", default="")
    ap.add_argument("--output-root", required=True)
    ap.add_argument("--min-sample-size", type=int, default=15)
    ap.add_argument("--top-k", type=int, default=20)
    args = ap.parse_args()

    labeled_rows = _load_csv(Path(args.input_labeled_csv))
    btc_rows: list[dict[str, str]] = []
    if args.input_btc_columns_csv:
        btc_rows = _load_csv(Path(args.input_btc_columns_csv))

    excluded_dims = [d for d in FORBIDDEN_FEATURES if any(d in r for r in labeled_rows)]

    btc_by_key: dict[tuple[str, str, str], dict[str, str]] = {}
    for r in btc_rows:
        btc_by_key[_join_key(r)] = r

    prepared: list[dict[str, str]] = []
    for row in labeled_rows:
        merged = dict(row)
        if btc_by_key:
            match = btc_by_key.get(_join_key(row), {})
            for f in ALLOWED_FEATURES:
                if (not str(merged.get(f, "")).strip()) and str(match.get(f, "")).strip():
                    merged[f] = str(match.get(f, "")).strip()
        prepared.append(merged)

    grouping_modes: dict[str, list[tuple[str, ...]]] = {
        "single": [(f,) for f in ALLOWED_FEATURES],
        "pair": [],
        "triple": [],
        "hierarchical": [
            ("pole_boxes_bucket", "retrace_ratio_bucket", "enhanced_by_opposing_pole", "opposing_pole_distance_columns"),
            ("pole_boxes", "retrace_ratio", "enhanced_by_opposing_pole"),
        ],
    }

    for i in range(len(ALLOWED_FEATURES)):
        for j in range(i + 1, len(ALLOWED_FEATURES)):
            grouping_modes["pair"].append((ALLOWED_FEATURES[i], ALLOWED_FEATURES[j]))
    for i in range(len(ALLOWED_FEATURES)):
        for j in range(i + 1, len(ALLOWED_FEATURES)):
            for k in range(j + 1, len(ALLOWED_FEATURES)):
                grouping_modes["triple"].append((ALLOWED_FEATURES[i], ALLOWED_FEATURES[j], ALLOWED_FEATURES[k]))

    ranking_rows: list[dict[str, str | int | float]] = []
    cluster_rows: list[dict[str, str | int | float]] = []
    dup_counts: defaultdict[tuple[str, str, str], int] = defaultdict(int)
    for r in prepared:
        dup_counts[_join_key(r)] += 1

    for mode in ("single", "pair", "triple", "hierarchical"):
        for dims in grouping_modes[mode]:
            grouped: dict[tuple[str, ...], list[dict[str, str]]] = {}
            for row in prepared:
                gk = tuple(_clean_value(row, d) for d in dims)
                grouped.setdefault(gk, []).append(row)

            for key in sorted(grouped.keys()):
                bucket = grouped[key]
                n = len(bucket)
                continuation = sum(1 for r in bucket if str(r.get("outcome_class", "")) in CONTINUATION_OUTCOMES)
                failure = sum(1 for r in bucket if str(r.get("outcome_class", "")) == "FAILED_REVERSAL")
                avg_fav = _safe_div(sum(_to_float(r.get("max_favorable_boxes")) for r in bucket), n)
                avg_adv = _safe_div(sum(_to_float(r.get("max_adverse_boxes")) for r in bucket), n)
                continuation_pct = _safe_div(continuation, n)
                failure_pct = _safe_div(failure, n)
                asymmetry = _safe_div(avg_fav - avg_adv, avg_fav + avg_adv) if (avg_fav + avg_adv) > 0 else 0.0
                expectancy, asymmetry_score = _compute_scores(n, continuation_pct, failure_pct, asymmetry)
                row_out = {
                    "grouping_mode": mode,
                    "dimensions_used": "|".join(dims),
                    "group_key": "|".join(key),
                    "sample_size": n,
                    "continuation_pct": round(continuation_pct, 6),
                    "failure_pct": round(failure_pct, 6),
                    "avg_max_favorable": round(avg_fav, 6),
                    "avg_max_adverse": round(avg_adv, 6),
                    "asymmetry_score": asymmetry_score,
                    "expectancy_score": expectancy,
                }
                cluster_rows.append(row_out)
                if n >= args.min_sample_size:
                    ranking_rows.append(row_out)

    ranking_rows.sort(key=lambda r: (-float(r["expectancy_score"]), -int(r["sample_size"]), str(r["grouping_mode"]), str(r["dimensions_used"]), str(r["group_key"])))

    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    fields = ["grouping_mode", "dimensions_used", "group_key", "sample_size", "continuation_pct", "failure_pct", "avg_max_favorable", "avg_max_adverse", "asymmetry_score", "expectancy_score"]
    with (output_root / "pole_live_safe_expectancy_rankings.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(ranking_rows)

    with (output_root / "pole_live_safe_clusters.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(cluster_rows)

    top20 = ranking_rows[:20]
    retrace_ratio_dom = any(("retrace_ratio" in str(r["dimensions_used"]) or "retrace_ratio_bucket" in str(r["dimensions_used"])) and float(r["expectancy_score"]) > 0 for r in top20)
    pole_le_8_survives = any("<=8" in str(r["group_key"]) for r in top20)
    opp_enh_matter = any("enhanced_by_opposing_pole" in str(r["dimensions_used"]) for r in top20)

    with (output_root / "pole_live_safe_expectancy_summary.md").open("w") as f:
        f.write("# Pole Live-Safe Expectancy Mining (Research-Only)\n\n")
        f.write("## Diagnostics\n")
        f.write(f"- rows loaded: {len(labeled_rows)}\n")
        f.write(f"- rows analyzed: {len(prepared)}\n")
        f.write(f"- grouping count: {len(cluster_rows)}\n")
        f.write(f"- surviving groups after min sample filter: {len(ranking_rows)}\n")
        f.write(f"- dimensions used: {', '.join(ALLOWED_FEATURES)}\n")
        f.write(f"- excluded forbidden dimensions: {', '.join(excluded_dims) if excluded_dims else 'none present'}\n")
        f.write(f"- duplicate key diagnostics: {sum(1 for v in dup_counts.values() if v > 1)} duplicate keys\n\n")
        sections = [
            "Strongest live-safe continuation geometries",
            "Strongest low-adverse geometries",
            "Most stable live-safe geometries",
            "Geometry interactions that survive without regime labels",
            "Whether retrace_ratio still dominates without all future features",
            "Whether opposing-pole enhancement still matters",
            "Whether pole size still matters",
        ]
        for s in sections:
            f.write(f"## {s}\n")
            if "retrace_ratio" in s:
                f.write(f"- result: {'YES' if retrace_ratio_dom else 'NO'} (top-{args.top_k} based diagnostic).\n\n")
            elif "opposing-pole" in s:
                f.write(f"- result: {'YES' if opp_enh_matter else 'NO'} (appears in top-{args.top_k} dimensions).\n\n")
            elif "pole size" in s:
                f.write(f"- result: {'YES' if pole_le_8_survives else 'NO'} (<=8 bucket presence in top-{args.top_k}).\n\n")
            else:
                rows = top20[:5]
                if not rows:
                    f.write("- none\n\n")
                else:
                    for r in rows:
                        f.write(f"- {r['grouping_mode']} | {r['dimensions_used']} | {r['group_key']} | n={r['sample_size']} | exp={r['expectancy_score']:.4f}\n")
                    f.write("\n")

    print(f"DIAG rows_loaded={len(labeled_rows)}")
    print(f"DIAG rows_analyzed={len(prepared)}")
    print(f"DIAG grouping_count={len(cluster_rows)}")
    print(f"DIAG surviving_groups_after_min_sample_filter={len(ranking_rows)}")


if __name__ == "__main__":
    main()
