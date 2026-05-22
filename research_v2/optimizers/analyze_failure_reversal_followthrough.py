from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

from research_v2.optimizers.analyze_rule_overlap import _select_identity_method

SPLITS: tuple[str, ...] = ("train", "validation", "oos")


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _quality_bucket(value: Any) -> str:
    score = _safe_float(value, default=-1.0)
    if score < 50:
        return "<50"
    if score < 60:
        return "50-59"
    if score < 70:
        return "60-69"
    if score < 80:
        return "70-79"
    if score < 90:
        return "80-89"
    return "90+"


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _split_rows(rows: list[dict[str, Any]], train_fraction: float, validation_fraction: float, oos_fraction: float) -> dict[str, list[dict[str, Any]]]:
    if abs((train_fraction + validation_fraction + oos_fraction) - 1.0) > 1e-9:
        raise ValueError("train_fraction + validation_fraction + oos_fraction must equal 1.0")
    ordered = sorted(rows, key=lambda r: str(r.get("seed_reference_ts") or r.get("reference_ts", "")))
    n = len(ordered)
    train_end = int(n * train_fraction)
    validation_end = train_end + int(n * validation_fraction)
    return {
        "train": ordered[:train_end],
        "validation": ordered[train_end:validation_end],
        "oos": ordered[validation_end:],
    }


def _find_reversal(symbol_rows: list[dict[str, Any]], seed_idx: int, seed_side: str, forward_window_bars: int) -> tuple[dict[str, Any] | None, int | None]:
    opposite = "SHORT" if seed_side == "LONG" else "LONG"
    max_idx = min(len(symbol_rows), seed_idx + forward_window_bars + 1)
    for idx in range(seed_idx + 1, max_idx):
        candidate = symbol_rows[idx]
        if _norm(candidate.get("side")) != opposite:
            continue
        if _norm(candidate.get("resolution_status")) != "TP2":
            continue
        return candidate, idx - seed_idx
    return None, None


def analyze_failure_reversal_followthrough(*, labeled_dataset_path: str, output_root: str, forward_window_bars: int = 20, min_sample_size: int = 10, split_mode: str = "time", train_fraction: float = 0.60, validation_fraction: float = 0.20, oos_fraction: float = 0.20) -> dict[str, Any]:
    if split_mode != "time":
        raise ValueError("Only split_mode=time is supported.")

    rows = _load_rows(Path(labeled_dataset_path).resolve())
    _, _, identities = _select_identity_method(rows)
    row_identity_map = {id(row): row_id for row, row_id in zip(rows, identities)}

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol[_norm(row.get("symbol"))].append(row)
    for symbol in by_symbol:
        by_symbol[symbol] = sorted(by_symbol[symbol], key=lambda r: str(r.get("reference_ts", "")))

    seeds: list[dict[str, Any]] = []
    for symbol, symbol_rows in by_symbol.items():
        for idx, row in enumerate(symbol_rows):
            side = _norm(row.get("side"))
            if _norm(row.get("resolution_status")) != "STOPPED" or side not in {"LONG", "SHORT"}:
                continue
            reversal_row, distance = _find_reversal(symbol_rows, idx, side, forward_window_bars)
            reversal_found = reversal_row is not None
            seeds.append(
                {
                    "seed_row_identity": row_identity_map[id(row)],
                    "seed_side": side,
                    "seed_symbol": symbol,
                    "seed_reference_ts": row.get("reference_ts", ""),
                    "seed_breakout_context": _norm(row.get("breakout_context")),
                    "seed_pullback_quality": _norm(row.get("pullback_quality")),
                    "seed_active_leg_boxes": str(row.get("active_leg_boxes", "")).strip(),
                    "seed_quality_score": row.get("quality_score", ""),
                    "seed_quality_score_bucket": _quality_bucket(row.get("quality_score")),
                    "seed_trend_regime": _norm(row.get("trend_regime")),
                    "seed_continuation_execution_class": _norm(row.get("continuation_execution_class")),
                    "seed_late_extension": _norm(row.get("late_extension")),
                    "reversal_found": "1" if reversal_found else "0",
                    "reversal_side": _norm(reversal_row.get("side")) if reversal_row else "",
                    "reversal_reference_ts": reversal_row.get("reference_ts", "") if reversal_row else "",
                    "reversal_resolution_status": _norm(reversal_row.get("resolution_status")) if reversal_row else "",
                    "reversal_realized_r_multiple": _safe_float(reversal_row.get("realized_r_multiple"), 0.0) if reversal_row else 0.0,
                    "forward_distance_bars": distance if distance is not None else "",
                }
            )

    splits = _split_rows(seeds, train_fraction, validation_fraction, oos_fraction)

    def _ratio(numer: int, denom: int) -> float:
        return (numer / denom) if denom else 0.0

    def _split_metrics(rows_in_split: list[dict[str, Any]]) -> dict[str, Any]:
        rows_count = len(rows_in_split)
        success = [r for r in rows_in_split if r["reversal_found"] == "1"]
        l2s_total = sum(1 for r in rows_in_split if r["seed_side"] == "LONG")
        s2l_total = sum(1 for r in rows_in_split if r["seed_side"] == "SHORT")
        l2s_success = sum(1 for r in success if r["seed_side"] == "LONG")
        s2l_success = sum(1 for r in success if r["seed_side"] == "SHORT")
        success_r = [float(r["reversal_realized_r_multiple"]) for r in success]
        return {
            "rows": rows_count,
            "reversal_success_ratio": _ratio(len(success), rows_count),
            "reversal_mean_r": mean(success_r) if success_r else 0.0,
            "long_to_short_ratio": _ratio(l2s_success, l2s_total),
            "short_to_long_ratio": _ratio(s2l_success, s2l_total),
        }

    feature_fields = [
        "seed_breakout_context",
        "seed_pullback_quality",
        "seed_late_extension",
        "seed_active_leg_boxes",
        "seed_trend_regime",
        "seed_continuation_execution_class",
        "seed_side",
        "seed_symbol",
        "seed_quality_score_bucket",
    ]
    feature_summary_rows: list[dict[str, Any]] = []
    for field in feature_fields:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in seeds:
            groups[str(row.get(field, ""))].append(row)
        for value in sorted(groups):
            g = groups[value]
            successes = [r for r in g if r["reversal_found"] == "1"]
            tp2_ratio = _ratio(sum(1 for r in successes if r["reversal_resolution_status"] == "TP2"), len(g))
            feature_summary_rows.append(
                {
                    "group_by": field,
                    "group_value": value,
                    "seed_count": len(g),
                    "reversal_success_count": len(successes),
                    "reversal_success_ratio": _ratio(len(successes), len(g)),
                    "reversal_mean_r": mean(float(r["reversal_realized_r_multiple"]) for r in successes) if successes else 0.0,
                    "tp2_ratio": tp2_ratio,
                }
            )

    split_metrics_rows = [{"split": split, **_split_metrics(split_rows)} for split, split_rows in splits.items()]
    all_metrics = _split_metrics(seeds)

    warnings: list[dict[str, str]] = []
    train, validation, oos = (dict(r) for r in split_metrics_rows)
    if train["reversal_success_ratio"] > 0 and oos["reversal_success_ratio"] < train["reversal_success_ratio"] * 0.7:
        warnings.append({"warning_code": "OOS_COLLAPSE", "details": "OOS reversal success ratio collapsed versus train."})
    if train["reversal_success_ratio"] > 0 and validation["reversal_success_ratio"] < train["reversal_success_ratio"] * 0.7:
        warnings.append({"warning_code": "VALIDATION_COLLAPSE", "details": "Validation reversal success ratio collapsed versus train."})
    if train["reversal_success_ratio"] > 0 and (validation["reversal_success_ratio"] == 0.0 or oos["reversal_success_ratio"] == 0.0):
        warnings.append({"warning_code": "TRAIN_ONLY_EDGE", "details": "Reversal edge appears in train but vanishes in later splits."})
    if len(seeds) < min_sample_size:
        warnings.append({"warning_code": "SMALL_SAMPLE", "details": f"Total seeds={len(seeds)} below min_sample_size={min_sample_size}."})

    symbol_counts: dict[str, int] = defaultdict(int)
    for s in seeds:
        symbol_counts[s["seed_symbol"]] += 1
    if seeds:
        top_symbol = max(symbol_counts, key=symbol_counts.get)
        share = symbol_counts[top_symbol] / len(seeds)
        if share > 0.7:
            warnings.append({"warning_code": "SYMBOL_CONCENTRATION", "details": f"{top_symbol} contributes {share:.1%} of failure seeds."})

    if abs(all_metrics["long_to_short_ratio"] - all_metrics["short_to_long_ratio"]) > 0.25:
        warnings.append({"warning_code": "LONG_SHORT_ASYMMETRY", "details": "Large LONG→SHORT vs SHORT→LONG reversal success asymmetry."})

    out_dir = Path(output_root).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_lines = [
        "# Failure Reversal Follow-through Summary",
        "",
        f"- total failure seeds: {len(seeds)}",
        f"- reversal success count: {sum(1 for s in seeds if s['reversal_found'] == '1')}",
        f"- reversal success ratio: {all_metrics['reversal_success_ratio']:.4f}",
        f"- LONG→SHORT reversal ratio: {all_metrics['long_to_short_ratio']:.4f}",
        f"- SHORT→LONG reversal ratio: {all_metrics['short_to_long_ratio']:.4f}",
        f"- mean realized R of reversal rows: {all_metrics['reversal_mean_r']:.4f}",
        f"- ETH reversal success ratio: {_ratio(sum(1 for s in seeds if s['seed_symbol']=='ETH' and s['reversal_found']=='1'), sum(1 for s in seeds if s['seed_symbol']=='ETH')):.4f}",
        f"- BTC reversal success ratio: {_ratio(sum(1 for s in seeds if s['seed_symbol']=='BTC' and s['reversal_found']=='1'), sum(1 for s in seeds if s['seed_symbol']=='BTC')):.4f}",
        f"- SOL reversal success ratio: {_ratio(sum(1 for s in seeds if s['seed_symbol']=='SOL' and s['reversal_found']=='1'), sum(1 for s in seeds if s['seed_symbol']=='SOL')):.4f}",
        "",
        "## Direct answers",
        f"- Is reversal behavior stable? {'NO' if any(w['warning_code'] in {'OOS_COLLAPSE', 'VALIDATION_COLLAPSE', 'TRAIN_ONLY_EDGE'} for w in warnings) else 'YES'}.",
        f"- Is it symbol-concentrated? {'YES' if any(w['warning_code'] == 'SYMBOL_CONCENTRATION' for w in warnings) else 'NO'}.",
        "- Does reversal work better after late extensions? See reversal_feature_summary.csv group_by=seed_late_extension.",
        "- Are reversals stronger after DEEP pullbacks? See reversal_feature_summary.csv group_by=seed_pullback_quality.",
        f"- Is edge symmetric LONG/SHORT? {'NO' if any(w['warning_code'] == 'LONG_SHORT_ASYMMETRY' for w in warnings) else 'YES'}.",
        f"- Does OOS survive? {'NO' if any(w['warning_code'] == 'OOS_COLLAPSE' for w in warnings) else 'YES'}.",
        "",
        "## Split metrics",
    ]
    for row in split_metrics_rows:
        summary_lines.append(
            f"- {row['split']}: rows={row['rows']} success_ratio={row['reversal_success_ratio']:.4f} mean_r={row['reversal_mean_r']:.4f} L→S={row['long_to_short_ratio']:.4f} S→L={row['short_to_long_ratio']:.4f}"
        )
    summary_lines.extend(["", "## Warnings"])
    summary_lines.extend([f"- {w['warning_code']}: {w['details']}" for w in warnings] or ["- none"])

    (out_dir / "reversal_followthrough_summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    seed_fields = [
        "seed_row_identity", "seed_side", "seed_symbol", "seed_reference_ts", "seed_breakout_context", "seed_pullback_quality",
        "seed_active_leg_boxes", "seed_quality_score", "reversal_found", "reversal_side", "reversal_reference_ts",
        "reversal_resolution_status", "reversal_realized_r_multiple", "forward_distance_bars",
    ]
    with (out_dir / "reversal_seed_rows.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=seed_fields)
        writer.writeheader()
        writer.writerows([{k: row.get(k, "") for k in seed_fields} for row in seeds])

    with (out_dir / "reversal_feature_summary.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = ["group_by", "group_value", "seed_count", "reversal_success_count", "reversal_success_ratio", "reversal_mean_r", "tp2_ratio"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(feature_summary_rows)

    with (out_dir / "reversal_split_metrics.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = ["split", "rows", "reversal_success_ratio", "reversal_mean_r", "long_to_short_ratio", "short_to_long_ratio"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(split_metrics_rows)

    with (out_dir / "reversal_warnings.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = ["warning_code", "details"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(warnings)

    return {
        "summary_md": str(out_dir / "reversal_followthrough_summary.md"),
        "seed_rows_csv": str(out_dir / "reversal_seed_rows.csv"),
        "feature_summary_csv": str(out_dir / "reversal_feature_summary.csv"),
        "split_metrics_csv": str(out_dir / "reversal_split_metrics.csv"),
        "warnings_csv": str(out_dir / "reversal_warnings.csv"),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze failed continuation setups for opposite-side reversal follow-through.")
    parser.add_argument("--labeled-dataset-path", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--forward-window-bars", type=int, default=20)
    parser.add_argument("--min-sample-size", type=int, default=10)
    parser.add_argument("--split-mode", default="time", choices=["time"])
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--oos-fraction", type=float, default=0.20)
    return parser


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    analyze_failure_reversal_followthrough(
        labeled_dataset_path=args.labeled_dataset_path,
        output_root=args.output_root,
        forward_window_bars=args.forward_window_bars,
        min_sample_size=args.min_sample_size,
        split_mode=args.split_mode,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
        oos_fraction=args.oos_fraction,
    )
