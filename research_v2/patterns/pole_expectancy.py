from __future__ import annotations

import argparse
import csv
from pathlib import Path
from statistics import median

JOIN_KEYS = ("symbol", "timestamp", "pattern_name")
TIMESTAMP_KEYS = ("timestamp", "reversal_timestamp", "reference_ts", "reference_timestamp")
SYMBOL_KEYS = ("symbol", "asset", "ticker")

GROUP_DIMENSIONS = (
    "regime_class",
    "pole_boxes_bucket",
    "retrace_ratio_bucket",
    "enhanced_by_opposing_pole",
    "breakout_excess_boxes_bucket",
    "continuation_after_sideways",
    "volatility_compression_after_signal",
)

CONTINUATION_OUTCOMES = {"BULLISH_CONTINUATION", "BEARISH_CONTINUATION"}


def _to_float(v: str | float | int | None, default: float = 0.0) -> float:
    if v is None:
        return default

    text = str(v).strip()

    if not text:
        return default

    return float(text)


def _to_int(v: str | float | int | None, default: int = 0) -> int:
    if v is None:
        return default

    text = str(v).strip()

    if not text:
        return default

    return int(float(text))


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
    symbol = _first_present(row, SYMBOL_KEYS)
    timestamp = _first_present(row, TIMESTAMP_KEYS)
    pattern_name = str(row.get("pattern_name", "")).strip()

    return (symbol, timestamp, pattern_name)


def _compute_scores(
    sample_size: int,
    continuation_pct: float,
    failed_rev_pct: float,
    asymmetry: float,
    persistence: float,
) -> tuple[float, float, float]:

    sample_weight = min(1.0, sample_size / 100.0)

    asymmetry_score = asymmetry

    stability_score = (
        (continuation_pct * 0.60)
        + (persistence * 0.40)
        - (failed_rev_pct * 0.50)
    )

    expectancy_score = (
        (
            (continuation_pct * 0.45)
            + (asymmetry_score * 0.25)
            + (stability_score * 0.30)
        )
        * sample_weight
    )

    return (
        round(expectancy_score, 6),
        round(asymmetry_score, 6),
        round(stability_score, 6),
    )


def main() -> None:

    ap = argparse.ArgumentParser(
        description="Research-only structural expectancy mining for pole patterns"
    )

    ap.add_argument("--input-labeled-csv", required=True)
    ap.add_argument("--input-regimes-csv", required=True)
    ap.add_argument("--output-root", required=True)
    ap.add_argument("--min-sample-size", type=int, default=25)
    ap.add_argument("--top-k", type=int, default=10)

    args = ap.parse_args()

    labeled_rows = _load_csv(Path(args.input_labeled_csv))
    regime_rows = _load_csv(Path(args.input_regimes_csv))

    labeled_by_key: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    regime_by_key: dict[tuple[str, str, str], list[dict[str, str]]] = {}

    for row in labeled_rows:
        labeled_by_key.setdefault(_join_key(row), []).append(row)

    for row in regime_rows:
        regime_by_key.setdefault(_join_key(row), []).append(row)

    labeled_duplicate_keys = sum(
        1 for k, rows in labeled_by_key.items() if len(rows) > 1
    )

    regime_duplicate_keys = sum(
        1 for k, rows in regime_by_key.items() if len(rows) > 1
    )

    matched_rows: list[dict[str, str]] = []

    ambiguous_match_keys = 0

    matched_keys = sorted(
        set(labeled_by_key.keys()) & set(regime_by_key.keys())
    )

    for key in matched_keys:

        lrows = labeled_by_key[key]
        rrows = regime_by_key[key]

        if len(lrows) > 1 or len(rrows) > 1:
            ambiguous_match_keys += 1

        limit = min(len(lrows), len(rrows))

        for idx in range(limit):

            lrow = lrows[idx]
            rrow = rrows[idx]

            merged = dict(lrow)

            for dim in GROUP_DIMENSIONS:
                merged[dim] = rrow.get(dim, "")

            merged["regime_class"] = rrow.get("regime_class", "")

            matched_rows.append(merged)

    unmatched_labeled = sum(
        len(v)
        for k, v in labeled_by_key.items()
        if k not in regime_by_key
    )

    unmatched_regimes = sum(
        len(v)
        for k, v in regime_by_key.items()
        if k not in labeled_by_key
    )

    null_regime_rows = sum(
        1
        for row in matched_rows
        if not str(row.get("regime_class", "")).strip()
    )

    grouped: dict[tuple[str, ...], list[dict[str, str]]] = {}

    for row in matched_rows:
        key = tuple(str(row.get(dim, "")) for dim in GROUP_DIMENSIONS)
        grouped.setdefault(key, []).append(row)

    ranking_rows: list[dict[str, str | int | float]] = []

    for key in sorted(grouped.keys()):

        bucket = grouped[key]

        n = len(bucket)

        continuation = sum(
            1
            for r in bucket
            if str(r.get("outcome_class", "")) in CONTINUATION_OUTCOMES
        )

        failure = sum(
            1
            for r in bucket
            if str(r.get("outcome_class", "")) == "FAILED_REVERSAL"
        )

        sideways = sum(
            1
            for r in bucket
            if str(r.get("outcome_class", "")) == "SIDEWAYS"
        )

        trend_cont = sum(
            1
            for r in bucket
            if str(r.get("regime_class", "")) == "TREND_CONTINUATION"
        )

        fast_mr = sum(
            1
            for r in bucket
            if str(r.get("regime_class", "")) == "FAST_MEAN_REVERSION"
        )

        volatile_chop = sum(
            1
            for r in bucket
            if str(r.get("regime_class", "")) == "VOLATILE_CHOP"
        )

        failed_reversal_regime = sum(
            1
            for r in bucket
            if str(r.get("regime_class", "")) == "FAILED_REVERSAL"
        )

        max_fav = [
            _to_float(r.get("max_favorable_boxes"))
            for r in bucket
        ]

        max_adv = [
            _to_float(r.get("max_adverse_boxes"))
            for r in bucket
        ]

        persistence_vals = [
            _to_float(r.get("continuation_persistence_ge_1_box"))
            for r in bucket
        ]

        ratios = [
            (_safe_div(f, a) if a > 0 else f)
            for f, a in zip(max_fav, max_adv)
        ]

        continuation_pct = _safe_div(continuation, n)

        failure_pct = _safe_div(failure, n)

        avg_fav = _safe_div(sum(max_fav), n)

        avg_adv = _safe_div(sum(max_adv), n)

        asymmetry = (
            _safe_div(avg_fav - avg_adv, avg_fav + avg_adv)
            if (avg_fav + avg_adv) > 0
            else 0.0
        )

        avg_persistence = _safe_div(sum(persistence_vals), n)

        expectancy_score, asymmetry_score, stability_score = (
            _compute_scores(
                n,
                continuation_pct,
                _safe_div(failed_reversal_regime, n),
                asymmetry,
                avg_persistence,
            )
        )

        ranking_rows.append(
            {
                **{
                    dim: key[idx]
                    for idx, dim in enumerate(GROUP_DIMENSIONS)
                },

                "sample_size": n,

                "continuation_pct": round(
                    continuation_pct,
                    6,
                ),

                "failure_pct": round(
                    failure_pct,
                    6,
                ),

                "sideways_pct": round(
                    _safe_div(sideways, n),
                    6,
                ),

                "trend_continuation_pct": round(
                    _safe_div(trend_cont, n),
                    6,
                ),

                "fast_mean_reversion_pct": round(
                    _safe_div(fast_mr, n),
                    6,
                ),

                "volatile_chop_pct": round(
                    _safe_div(volatile_chop, n),
                    6,
                ),

                "failed_reversal_regime_pct": round(
                    _safe_div(failed_reversal_regime, n),
                    6,
                ),

                "avg_max_favorable": round(
                    avg_fav,
                    6,
                ),

                "median_max_favorable": round(
                    median(max_fav) if max_fav else 0.0,
                    6,
                ),

                "avg_max_adverse": round(
                    avg_adv,
                    6,
                ),

                "median_max_adverse": round(
                    median(max_adv) if max_adv else 0.0,
                    6,
                ),

                "avg_mfe_mae_ratio": round(
                    _safe_div(sum(ratios), len(ratios)),
                    6,
                ),

                "median_mfe_mae_ratio": round(
                    median(ratios) if ratios else 0.0,
                    6,
                ),

                "avg_continuation_persistence": round(
                    avg_persistence,
                    6,
                ),

                "median_continuation_persistence": round(
                    median(persistence_vals)
                    if persistence_vals
                    else 0.0,
                    6,
                ),

                "expectancy_score": expectancy_score,
                "asymmetry_score": asymmetry_score,
                "stability_score": stability_score,
            }
        )

    filtered = [
        r
        for r in ranking_rows
        if int(r["sample_size"]) >= args.min_sample_size
    ]

    filtered.sort(
        key=lambda r: (
            -float(r["expectancy_score"]),
            -int(r["sample_size"]),
            tuple(str(r[d]) for d in GROUP_DIMENSIONS),
        )
    )

    output_root = Path(args.output_root)

    output_root.mkdir(parents=True, exist_ok=True)

    ranking_fields = [
        *GROUP_DIMENSIONS,
        "sample_size",
        "continuation_pct",
        "failure_pct",
        "sideways_pct",
        "trend_continuation_pct",
        "fast_mean_reversion_pct",
        "volatile_chop_pct",
        "failed_reversal_regime_pct",
        "avg_max_favorable",
        "median_max_favorable",
        "avg_max_adverse",
        "median_max_adverse",
        "avg_mfe_mae_ratio",
        "median_mfe_mae_ratio",
        "avg_continuation_persistence",
        "median_continuation_persistence",
        "expectancy_score",
        "asymmetry_score",
        "stability_score",
    ]

    rankings_csv = output_root / "pole_expectancy_rankings.csv"

    with rankings_csv.open("w", newline="") as f:

        writer = csv.DictWriter(
            f,
            fieldnames=ranking_fields,
        )

        writer.writeheader()
        writer.writerows(filtered)

    clusters_csv = output_root / "pole_expectancy_clusters.csv"

    with clusters_csv.open("w", newline="") as f:

        writer = csv.DictWriter(
            f,
            fieldnames=ranking_fields,
        )

        writer.writeheader()
        writer.writerows(ranking_rows)

    def _top_line(metric: str, reverse: bool = True) -> str:

        if not filtered:
            return "- none"

        rows = sorted(
            filtered,
            key=lambda r: (
                float(r[metric]),
                float(r["expectancy_score"]),
                int(r["sample_size"]),
            ),
            reverse=reverse,
        )[: args.top_k]

        return "\n".join(
            f"- {r['regime_class']} | "
            f"n={r['sample_size']} | "
            f"expectancy={r['expectancy_score']:.4f} | "
            f"{metric}={float(r[metric]):.4f}"
            for r in rows
        )

    summary_md = output_root / "pole_expectancy_summary.md"

    with summary_md.open("w") as f:

        f.write("# Pole Expectancy Structural Mining (Research-Only)\n\n")

        f.write(
            "Deterministic diagnostics-first analyzer. "
            "No production strategy/runtime integration "
            "and no TP/SL execution assumptions.\n\n"
        )

        f.write("## Scoring Formulas\n")

        f.write(
            "- sample_weight = "
            "min(1.0, sample_size / 100.0)\n"
        )

        f.write(
            "- asymmetry_score = "
            "(avg_max_favorable - avg_max_adverse) / "
            "(avg_max_favorable + avg_max_adverse)\n"
        )

        f.write(
            "- stability_score = "
            "(0.60 * continuation_pct) + "
            "(0.40 * avg_continuation_persistence) - "
            "(0.50 * failed_reversal_regime_pct)\n"
        )

        f.write(
            "- expectancy_score = "
            "((0.45 * continuation_pct) + "
            "(0.25 * asymmetry_score) + "
            "(0.30 * stability_score)) * sample_weight\n\n"
        )

        f.write("## Join Diagnostics\n")

        f.write(f"- loaded_labeled_rows: {len(labeled_rows)}\n")
        f.write(f"- loaded_regime_rows: {len(regime_rows)}\n")
        f.write(f"- matched_rows: {len(matched_rows)}\n")
        f.write(f"- unmatched_labeled_rows: {unmatched_labeled}\n")
        f.write(f"- unmatched_regime_rows: {unmatched_regimes}\n")
        f.write(f"- duplicate_labeled_keys: {labeled_duplicate_keys}\n")
        f.write(f"- duplicate_regime_keys: {regime_duplicate_keys}\n")
        f.write(f"- null_regime_rows: {null_regime_rows}\n")
        f.write(f"- ambiguous_match_keys: {ambiguous_match_keys}\n")
        f.write(f"- grouping_count: {len(grouped)}\n")
        f.write(
            f"- subsets_after_min_sample_filter: {len(filtered)}\n\n"
        )

        f.write(
            "## strongest continuation clusters\n"
            + _top_line("continuation_pct")
            + "\n\n"
        )

        f.write(
            "## strongest failed-reversal clusters\n"
            + _top_line("failed_reversal_regime_pct")
            + "\n\n"
        )

        f.write(
            "## strongest trend-continuation clusters\n"
            + _top_line("trend_continuation_pct")
            + "\n\n"
        )

        f.write(
            "## strongest mean-reversion clusters\n"
            + _top_line("fast_mean_reversion_pct")
            + "\n\n"
        )

        f.write(
            "## smallest adverse excursion clusters\n"
            + _top_line("avg_max_adverse", reverse=False)
            + "\n\n"
        )

        f.write(
            "## highest persistence clusters\n"
            + _top_line("avg_continuation_persistence")
            + "\n\n"
        )

        f.write(
            "## highest asymmetry clusters\n"
            + _top_line("asymmetry_score")
            + "\n"
        )

    print(f"DIAG loaded_labeled_rows={len(labeled_rows)}")
    print(f"DIAG loaded_regime_rows={len(regime_rows)}")
    print(f"DIAG matched_rows={len(matched_rows)}")
    print(f"DIAG unmatched_labeled_rows={unmatched_labeled}")
    print(f"DIAG unmatched_regimes_rows={unmatched_regimes}")
    print(f"DIAG duplicate_labeled_keys={labeled_duplicate_keys}")
    print(f"DIAG duplicate_regime_keys={regime_duplicate_keys}")
    print(f"DIAG null_regime_rows={null_regime_rows}")
    print(f"DIAG ambiguous_match_keys={ambiguous_match_keys}")
    print(f"DIAG grouping_count={len(grouped)}")
    print(f"DIAG subsets_after_min_sample_filter={len(filtered)}")


if __name__ == "__main__":
    main()