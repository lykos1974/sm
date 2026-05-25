from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import median


REGIME_TREND_CONTINUATION = "TREND_CONTINUATION"
REGIME_VOLATILE_CHOP = "VOLATILE_CHOP"
REGIME_SIDEWAYS_COMPRESSION = "SIDEWAYS_COMPRESSION"
REGIME_FAILED_REVERSAL = "FAILED_REVERSAL"
REGIME_FAST_MEAN_REVERSION = "FAST_MEAN_REVERSION"


@dataclass(frozen=True)
class FollowthroughMetrics:
    favorable_run: int
    adverse_run: int
    delayed_continuation: int
    continuation_after_sideways: int
    volatility_compression_after_signal: float
    cumulative_favorable_progression: float
    cumulative_adverse_progression: float
    normalized_trajectory: tuple[float, ...]


def _to_int(v: str | float | int | None, default: int = 0) -> int:
    if v is None or v == "":
        return default
    return int(float(v))


def _to_float(v: str | float | int | None, default: float = 0.0) -> float:
    if v is None or v == "":
        return default
    return float(v)




def _parse_path_series(raw: str | None) -> list[float]:
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []
    values: list[float] = []
    for part in text.split(','):
        token = part.strip()
        if not token:
            continue
        values.append(float(token))
    return values

def _bucket_pole_size(v: float) -> str:
    if v <= 8:
        return "<=8"
    if v <= 13:
        return "9-13"
    if v <= 20:
        return "14-20"
    return ">20"


def _bucket_retrace(v: float) -> str:
    if v < 0.75:
        return "<0.75"
    if v < 1.00:
        return "0.75-1.00"
    if v <= 1.50:
        return "1.00-1.50"
    return ">1.50"


def _bucket_breakout_excess(v: float) -> str:
    if v <= 0:
        return "<=0"
    if v <= 1:
        return "0-1"
    if v <= 3:
        return "2-3"
    return ">3"


def _safe_ratio(num: float, den: float) -> float:
    return (num / den) if den != 0 else 0.0


def _consecutive_run(values: list[float], threshold: float = 0.0) -> int:
    run = 0
    for v in values:
        if v > threshold:
            run += 1
        else:
            break
    return run


def _classify_regime(pattern_name: str, fav: list[float], adv: list[float], future_obs: int, sideways_cols: int = 3) -> str:
    if future_obs <= 0:
        return REGIME_SIDEWAYS_COMPRESSION

    fav = [max(0.0, float(v)) for v in fav]
    adv = [max(0.0, float(v)) for v in adv]
    if not fav and not adv:
        return REGIME_SIDEWAYS_COMPRESSION

    max_fav = max(fav) if fav else 0.0
    max_adv = max(adv) if adv else 0.0
    early_fav = fav[:sideways_cols]
    early_adv = adv[:sideways_cols]
    early_fav_max = max(early_fav) if early_fav else 0.0
    early_adv_max = max(early_adv) if early_adv else 0.0

    # normalize directional bias using cumulative path progression (not only peaks)
    cum_fav = sum(fav)
    cum_adv = sum(adv)
    total_progress = cum_fav + cum_adv
    net_ratio = ((cum_fav - cum_adv) / total_progress) if total_progress > 0 else 0.0

    if max_fav <= 1 and max_adv <= 1:
        return REGIME_SIDEWAYS_COMPRESSION

    # strong early directional outcomes
    if max_adv >= 3 and early_adv_max >= 2 and early_fav_max <= 1 and net_ratio <= -0.30:
        return REGIME_FAST_MEAN_REVERSION
    if max_fav >= 3 and early_fav_max >= 2 and early_adv_max <= 1 and net_ratio >= 0.30:
        return REGIME_TREND_CONTINUATION

    # delayed continuation after early compression
    later_fav = fav[sideways_cols:]
    early_sideways = all((f < 2 and a < 2) for f, a in zip(early_fav, early_adv))
    late_breakout = (max(later_fav) if later_fav else 0.0) >= 3
    if early_sideways and late_breakout and net_ratio >= 0.20:
        return REGIME_TREND_CONTINUATION

    # sustained directional failure without meaningful continuation
    if max_adv >= 3 and max_fav <= 2 and net_ratio <= -0.20:
        return REGIME_FAILED_REVERSAL

    sign_flips = 0
    last_sign = 0
    for f, a in zip(fav, adv):
        cur_sign = 1 if f > a else (-1 if a > f else 0)
        if cur_sign != 0 and last_sign != 0 and cur_sign != last_sign:
            sign_flips += 1
        if cur_sign != 0:
            last_sign = cur_sign
    if sign_flips >= 2:
        return REGIME_VOLATILE_CHOP

    if abs(net_ratio) <= 0.15:
        return REGIME_SIDEWAYS_COMPRESSION

    return REGIME_TREND_CONTINUATION if net_ratio > 0 else REGIME_FAILED_REVERSAL


def _compute_followthrough(row: dict[str, str], future_columns: int) -> FollowthroughMetrics:
    favorable = _parse_path_series(row.get("fav_path"))
    adverse = _parse_path_series(row.get("adv_path"))

    if favorable and adverse:
        target_len = min(future_columns, len(favorable), len(adverse))
        favorable = favorable[:target_len]
        adverse = adverse[:target_len]

    if not favorable or not adverse:
        max_favorable = _to_float(row.get("max_favorable_boxes"), 0.0)
        max_adverse = _to_float(row.get("max_adverse_boxes"), 0.0)
        # fallback with minimal deterministic synthetic trajectory
        favorable = [max_favorable]
        adverse = [max_adverse]

    favorable_run = _consecutive_run(favorable)
    adverse_run = _consecutive_run(adverse)

    early_window = min(3, len(favorable))
    no_early_resolution = all(favorable[i] < 2 and adverse[i] < 2 for i in range(early_window))
    delayed_continuation = 1 if (no_early_resolution and any(v >= 3 for v in favorable[early_window:])) else 0
    continuation_after_sideways = delayed_continuation

    early_range = [abs(f - a) for f, a in zip(favorable[:early_window], adverse[:early_window])]
    later_window = min(3, max(0, len(favorable) - early_window))
    later_range = [
        abs(f - a)
        for f, a in zip(
            favorable[early_window : early_window + later_window],
            adverse[early_window : early_window + later_window],
        )
    ]
    early_mean = (sum(early_range) / len(early_range)) if early_range else 0.0
    later_mean = (sum(later_range) / len(later_range)) if later_range else early_mean
    if early_mean <= 0 and later_mean <= 0:
        compression = 0.0
    elif early_mean <= 0:
        compression = 2.0
    else:
        compression = min(3.0, _safe_ratio(later_mean, early_mean))

    cum_fav = sum(favorable)
    cum_adv = sum(adverse)
    denom = max(max(favorable) if favorable else 0.0, max(adverse) if adverse else 0.0, 1.0)
    normalized = tuple(round((f - a) / denom, 6) for f, a in zip(favorable, adverse))

    return FollowthroughMetrics(
        favorable_run=favorable_run,
        adverse_run=adverse_run,
        delayed_continuation=delayed_continuation,
        continuation_after_sideways=continuation_after_sideways,
        volatility_compression_after_signal=round(compression, 6),
        cumulative_favorable_progression=round(cum_fav, 6),
        cumulative_adverse_progression=round(cum_adv, 6),
        normalized_trajectory=normalized,
    )



def _first_present(row: dict[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"nan", "none", "null"}:
            return text
    return ""

def main() -> None:
    ap = argparse.ArgumentParser(description="Research-only pole follow-through regime analytics")
    ap.add_argument("--input-labeled-csv", help="Input from pole outcome labeling")
    ap.add_argument("--input-poles-csv", help="Alias input from pole outcome labeling")
    ap.add_argument("--input-columns-csv", help="Accepted for workflow compatibility; not used by this phase")
    ap.add_argument("--output-root", required=True)
    ap.add_argument("--future-columns", type=int, default=20)
    ap.add_argument("--diagnostics", action="store_true")
    args = ap.parse_args()
    input_csv = args.input_labeled_csv or args.input_poles_csv
    if not input_csv:
        raise SystemExit("One of --input-labeled-csv or --input-poles-csv is required")

    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    with Path(input_csv).open("r", newline="") as f:
        rows = list(csv.DictReader(f))


    if args.diagnostics:
        rows_with_valid_fav_path = 0
        rows_with_valid_adv_path = 0
        rows_with_both_paths = 0
        malformed_fav_paths = 0
        malformed_adv_paths = 0
        path_len_dist: dict[int, int] = {}
        for row in rows:
            fav_path = []
            adv_path = []
            try:
                fav_path = _parse_path_series(row.get("fav_path"))
            except ValueError:
                malformed_fav_paths += 1
            try:
                adv_path = _parse_path_series(row.get("adv_path"))
            except ValueError:
                malformed_adv_paths += 1
            if fav_path:
                rows_with_valid_fav_path += 1
                path_len_dist[len(fav_path)] = path_len_dist.get(len(fav_path), 0) + 1
            if adv_path:
                rows_with_valid_adv_path += 1
            if fav_path and adv_path:
                rows_with_both_paths += 1
        print(f"DIAG csv_load_rows={len(rows)}")
        print(f"DIAG rows_with_valid_fav_path={rows_with_valid_fav_path}")
        print(f"DIAG rows_with_valid_adv_path={rows_with_valid_adv_path}")
        print(f"DIAG rows_with_both_paths={rows_with_both_paths}")
        print(f"DIAG malformed_fav_path_rows={malformed_fav_paths}")
        print(f"DIAG malformed_adv_path_rows={malformed_adv_paths}")
        print(f"DIAG fav_path_length_distribution={path_len_dist}")

    regime_rows: list[dict[str, str | int | float]] = []
    for row in rows:
        ft = _compute_followthrough(row, args.future_columns)
        future_obs = _to_int(row.get("future_columns_observed"), 0)
        fav_path = _parse_path_series(row.get("fav_path"))[: args.future_columns]
        adv_path = _parse_path_series(row.get("adv_path"))[: args.future_columns]
        regime = _classify_regime(row.get("pattern_name", ""), fav_path, adv_path, future_obs)

        regime_rows.append(
            {
                "symbol": _first_present(row, ("symbol", "asset", "ticker")),
                "timestamp": _first_present(row, ("reversal_timestamp", "timestamp", "reference_ts", "reference_timestamp")),
                "pattern_name": row.get("pattern_name", ""),
                "regime_class": regime,
                "pole_boxes_bucket": _bucket_pole_size(_to_float(row.get("pole_boxes"), 0.0)),
                "retrace_ratio_bucket": _bucket_retrace(_to_float(row.get("retrace_ratio"), 0.0)),
                "enhanced_by_opposing_pole": str(row.get("enhanced_by_opposing_pole", "")),
                "breakout_excess_boxes_bucket": _bucket_breakout_excess(_to_float(row.get("breakout_excess_boxes"), 0.0)),
                "continuation_after_sideways": ft.continuation_after_sideways,
                "volatility_compression_after_signal": ft.volatility_compression_after_signal,
                "favorable_run": ft.favorable_run,
                "adverse_run": ft.adverse_run,
                "delayed_continuation": ft.delayed_continuation,
                "cumulative_favorable_progression": ft.cumulative_favorable_progression,
                "cumulative_adverse_progression": ft.cumulative_adverse_progression,
                "normalized_trajectory": "|".join(str(x) for x in ft.normalized_trajectory),
            }
        )


    if args.diagnostics:
        print(f"DIAG regime_classification_rows={len(regime_rows)}")

    regime_rows.sort(
        key=lambda r: (
            str(r["symbol"]),
            str(r["timestamp"]),
            str(r["pattern_name"]),
        )
    )

    regime_csv = output_root / "pole_followthrough_regimes.csv"
    fieldnames = list(regime_rows[0].keys()) if regime_rows else []
    with regime_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(regime_rows)


    if args.diagnostics:
        print(f"DIAG final_export_rows={len(regime_rows)}")

    # bucket combination stats
    grouped: dict[tuple[str, str, str, str, int], list[dict[str, str | int | float]]] = {}
    for r in regime_rows:
        key = (
            str(r["pole_boxes_bucket"]),
            str(r["retrace_ratio_bucket"]),
            str(r["enhanced_by_opposing_pole"]),
            str(r["breakout_excess_boxes_bucket"]),
            int(r["continuation_after_sideways"]),
        )
        grouped.setdefault(key, []).append(r)

    stats_rows: list[dict[str, str | int | float]] = []
    for key in sorted(grouped.keys()):
        bucket = grouped[key]
        n = len(bucket)
        delayed = sum(int(b["delayed_continuation"]) for b in bucket)
        comp = sum(float(b["volatility_compression_after_signal"]) for b in bucket) / n if n else 0.0
        fav_prog = sum(float(b["cumulative_favorable_progression"]) for b in bucket) / n if n else 0.0
        adv_prog = sum(float(b["cumulative_adverse_progression"]) for b in bucket) / n if n else 0.0
        trajs = [
            [float(x) for x in str(b["normalized_trajectory"]).split("|") if x != ""]
            for b in bucket
        ]
        t_len = min((len(t) for t in trajs), default=0)
        med_traj = [median([t[i] for t in trajs]) for i in range(t_len)] if t_len else []

        stats_rows.append(
            {
                "pole_boxes_bucket": key[0],
                "retrace_ratio_bucket": key[1],
                "enhanced_by_opposing_pole": key[2],
                "breakout_excess_boxes_bucket": key[3],
                "continuation_after_sideways": key[4],
                "n": n,
                "delayed_continuation_probability": round(_safe_ratio(delayed, n), 6),
                "mean_volatility_compression_after_signal": round(comp, 6),
                "mean_cumulative_favorable_progression": round(fav_prog, 6),
                "mean_cumulative_adverse_progression": round(adv_prog, 6),
                "median_normalized_trajectory": "|".join(f"{x:.6f}" for x in med_traj),
            }
        )

    stats_csv = output_root / "pole_followthrough_stats.csv"
    with stats_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(stats_rows[0].keys()) if stats_rows else [])
        if stats_rows:
            writer.writeheader()
            writer.writerows(stats_rows)

    summary_md = output_root / "pole_followthrough_summary.md"
    regime_counts: dict[str, int] = {}
    for r in regime_rows:
        regime_counts[str(r["regime_class"])] = regime_counts.get(str(r["regime_class"]), 0) + 1

    with summary_md.open("w") as f:
        f.write("# Pole Follow-through Regime Summary (Research-Only)\n\n")
        f.write("No strategy integration or production logic changes are performed. Diagnostics only.\n\n")
        f.write(f"- Total rows analyzed: {len(regime_rows)}\n")
        f.write("- Regime distribution:\n")
        for regime in sorted(regime_counts):
            pct = (100.0 * regime_counts[regime] / len(regime_rows)) if regime_rows else 0.0
            f.write(f"  - {regime}: {regime_counts[regime]} ({pct:.2f}%)\n")
        f.write("\n## Notes\n")
        f.write("- Classification uses only future-column structure, no trade simulation.\n")
        f.write("- Output is deterministic via explicit sorting and stable bucket transforms.\n")


if __name__ == "__main__":
    main()
