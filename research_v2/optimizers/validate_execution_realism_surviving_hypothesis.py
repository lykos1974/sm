from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

from research_v2.optimizers.analyze_rule_overlap import _select_identity_method

SPLITS: tuple[str, ...] = ("train", "validation", "oos")
FINAL_STATUSES = {"TP2", "STOPPED", "EXPIRED", "AMBIGUOUS"}


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _split_rows(rows: list[dict[str, Any]], train_fraction: float, validation_fraction: float, oos_fraction: float) -> dict[str, list[dict[str, Any]]]:
    if abs((train_fraction + validation_fraction + oos_fraction) - 1.0) > 1e-9:
        raise ValueError("train_fraction + validation_fraction + oos_fraction must equal 1.0")
    ordered = sorted(rows, key=lambda r: str(r.get("selected_reference_ts") or ""))
    n = len(ordered)
    train_end = int(n * train_fraction)
    validation_end = train_end + int(n * validation_fraction)
    return {"train": ordered[:train_end], "validation": ordered[train_end:validation_end], "oos": ordered[validation_end:]}


def _first_short_watch(symbol_rows: list[dict[str, Any]], seed_idx: int, forward_window_structural: int, *, exclude_same_timestamp_opposite: bool = False) -> tuple[int | None, dict[str, Any] | None]:
    seed_ts = str(symbol_rows[seed_idx].get("reference_ts") or "")
    max_idx = min(len(symbol_rows), seed_idx + forward_window_structural + 1)
    for idx in range(seed_idx + 1, max_idx):
        row = symbol_rows[idx]
        if exclude_same_timestamp_opposite and str(row.get("reference_ts") or "") <= seed_ts:
            continue
        if _norm(row.get("side")) == "SHORT" and _norm(row.get("status")) == "WATCH":
            return idx, row
    return None, None


def _is_resolved(row: dict[str, Any]) -> bool:
    return _norm(row.get("resolution_status")) in FINAL_STATUSES


def _metrics(rows: list[dict[str, Any]]) -> dict[str, float | int]:
    selected = len(rows)
    resolved = [r for r in rows if r["is_resolved"] == "1"]
    tp2 = sum(1 for r in resolved if r["resolution_status"] == "TP2")
    stopped = sum(1 for r in resolved if r["resolution_status"] == "STOPPED")
    expired_or_amb = sum(1 for r in resolved if r["resolution_status"] in {"EXPIRED", "AMBIGUOUS"})
    r_before = [_safe_float(r.get("realized_r_before_cost"), 0.0) for r in resolved]
    r_after = [_safe_float(r.get("realized_r_after_cost"), 0.0) for r in resolved]
    distances = [_safe_float(r.get("structural_distance_seed_to_selected"), 0.0) for r in rows]
    return {
        "selected_signals": selected,
        "resolved_trades": len(resolved),
        "tp2_count": tp2,
        "tp2_rate": (tp2 / len(resolved)) if resolved else 0.0,
        "stopped_count": stopped,
        "stopped_rate": (stopped / len(resolved)) if resolved else 0.0,
        "expired_ambiguous_count": expired_or_amb,
        "expired_ambiguous_rate": (expired_or_amb / len(resolved)) if resolved else 0.0,
        "mean_realized_r_before_cost": mean(r_before) if r_before else 0.0,
        "mean_realized_r_after_cost": mean(r_after) if r_after else 0.0,
        "total_realized_r_after_cost": sum(r_after),
        "avg_structural_distance": mean(distances) if distances else 0.0,
    }


def validate_execution_realism_surviving_hypothesis(*, labeled_dataset_path: str, output_root: str, forward_structural_window: int = 20, cost_r_deduction: float = 0.0, min_oos_sample: int = 20, train_fraction: float = 0.60, validation_fraction: float = 0.20, oos_fraction: float = 0.20, seed_pullback_quality: str = "DEEP", seed_breakout_context: str = "LATE_EXTENSION", allow_any_breakout_context: bool = False, exclude_same_timestamp_opposite: bool = False) -> dict[str, str]:
    rows = _load_rows(Path(labeled_dataset_path).resolve())
    _, _, identities = _select_identity_method(rows)
    row_identity_map = {id(row): row_id for row, row_id in zip(rows, identities)}

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol[_norm(row.get("symbol"))].append(row)
    for symbol in by_symbol:
        by_symbol[symbol] = sorted(by_symbol[symbol], key=lambda r: str(r.get("reference_ts") or ""))

    selected_rows: list[dict[str, Any]] = []
    overlap_skips = 0

    for symbol, symbol_rows in by_symbol.items():
        active_until_idx = -1
        unresolved_active_trade = False
        for idx, row in enumerate(symbol_rows):
            if _norm(row.get("side")) != "LONG" or _norm(row.get("resolution_status")) != "STOPPED":
                continue
            if _norm(row.get("pullback_quality")) != _norm(seed_pullback_quality):
                continue
            if not allow_any_breakout_context and _norm(row.get("breakout_context")) != _norm(seed_breakout_context):
                continue
            if unresolved_active_trade:
                overlap_skips += 1
                continue
            if idx <= active_until_idx:
                overlap_skips += 1
                continue

            selected_idx, selected = _first_short_watch(symbol_rows, idx, forward_structural_window, exclude_same_timestamp_opposite=exclude_same_timestamp_opposite)
            if selected is None or selected_idx is None:
                continue

            realized_before = _safe_float(selected.get("realized_r_multiple"), 0.0)
            realized_after = realized_before - cost_r_deduction
            is_resolved = _is_resolved(selected)
            selected_rows.append({
                "seed_row_identity": row_identity_map[id(row)],
                "seed_symbol": symbol,
                "seed_reference_ts": row.get("reference_ts", ""),
                "selected_row_identity": row_identity_map[id(selected)],
                "selected_reference_ts": selected.get("reference_ts", ""),
                "selected_status": _norm(selected.get("status")),
                "selected_side": _norm(selected.get("side")),
                "resolution_status": _norm(selected.get("resolution_status")),
                "is_resolved": "1" if is_resolved else "0",
                "realized_r_before_cost": f"{realized_before:.6f}",
                "realized_r_after_cost": f"{realized_after:.6f}",
                "cost_r_deduction": f"{cost_r_deduction:.6f}",
                "structural_distance_seed_to_selected": str(selected_idx - idx),
            })
            active_until_idx = selected_idx
            if not is_resolved:
                unresolved_active_trade = True

    split_rows = _split_rows(selected_rows, train_fraction, validation_fraction, oos_fraction)
    all_metrics = _metrics(selected_rows)
    split_metrics = [{"split": split, **_metrics(rows_split)} for split, rows_split in split_rows.items()]

    short_watch_baseline = [r for r in rows if _norm(r.get("side")) == "SHORT" and _norm(r.get("status")) == "WATCH"]
    baseline_tp2_rate = sum(1 for r in short_watch_baseline if _norm(r.get("resolution_status")) == "TP2") / len(short_watch_baseline) if short_watch_baseline else 0.0

    warnings: list[dict[str, str]] = []
    oos_row = next((r for r in split_metrics if r["split"] == "oos"), None)
    train_row = next((r for r in split_metrics if r["split"] == "train"), None)
    if oos_row is not None and oos_row["selected_signals"] < min_oos_sample:
        warnings.append({"warning_code": "SMALL_OOS_SAMPLE", "details": f"oos selected_signals={oos_row['selected_signals']} < min_oos_sample={min_oos_sample}."})
    if train_row and oos_row and train_row["tp2_rate"] > 0 and oos_row["tp2_rate"] < train_row["tp2_rate"] * 0.7:
        warnings.append({"warning_code": "OOS_COLLAPSE", "details": "OOS TP2 rate collapsed versus train."})
    symbol_counts = Counter(r["seed_symbol"] for r in selected_rows)
    if selected_rows and symbol_counts:
        eth_share = symbol_counts.get("ETH", 0) / len(selected_rows)
        if eth_share > 0.7:
            warnings.append({"warning_code": "ETH_CONCENTRATION", "details": f"ETH contributes {eth_share:.1%} of selected signals."})
    if selected_rows and overlap_skips / len(selected_rows) > 0.3:
        warnings.append({"warning_code": "OVERLAP_HEAVY", "details": f"overlap_skips={overlap_skips} vs selected_signals={len(selected_rows)}."})
    if all_metrics["mean_realized_r_after_cost"] <= 0:
        warnings.append({"warning_code": "COST_SENSITIVITY_FAILURE", "details": "Mean realized R after cost is non-positive."})
    if all_metrics["tp2_rate"] < baseline_tp2_rate:
        warnings.append({"warning_code": "TP2_BELOW_UNCONDITIONAL_BASELINE", "details": f"conditioned_tp2_rate={all_metrics['tp2_rate']:.4f} < baseline_short_watch_tp2_rate={baseline_tp2_rate:.4f}."})

    symbol_rows = []
    for symbol, count in sorted(symbol_counts.items()):
        srows = [r for r in selected_rows if r["seed_symbol"] == symbol]
        m = _metrics(srows)
        symbol_rows.append({"split": f"symbol:{symbol}", **m})

    out_dir = Path(output_root).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    breakout_label = "ANY_BREAKOUT_CONTEXT" if allow_any_breakout_context else _norm(seed_breakout_context)
    summary_lines = [
        "# Execution Realism Summary",
        "",
        f"Hypothesis evaluated: FAILED LONG + {_norm(seed_pullback_quality)} + {breakout_label} -> first future same-symbol SHORT WATCH.",
        "",
        f"- selected signals: {all_metrics['selected_signals']}",
        f"- resolved trades: {all_metrics['resolved_trades']}",
        f"- TP2 count/rate: {all_metrics['tp2_count']} / {all_metrics['tp2_rate']:.4f}",
        f"- STOPPED count/rate: {all_metrics['stopped_count']} / {all_metrics['stopped_rate']:.4f}",
        f"- EXPIRED/AMBIGUOUS count/rate: {all_metrics['expired_ambiguous_count']} / {all_metrics['expired_ambiguous_rate']:.4f}",
        f"- mean realized R before cost: {all_metrics['mean_realized_r_before_cost']:.4f}",
        f"- mean realized R after cost: {all_metrics['mean_realized_r_after_cost']:.4f}",
        f"- total realized R after cost: {all_metrics['total_realized_r_after_cost']:.4f}",
        f"- overlap skips: {overlap_skips}",
        f"- average structural distance (seed->selected): {all_metrics['avg_structural_distance']:.4f}",
        "",
        "## Warnings",
    ]
    summary_lines.extend([f"- {w['warning_code']}: {w['details']}" for w in warnings] or ["- none"])
    (out_dir / "execution_realism_summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    with (out_dir / "execution_realism_trades.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = ["seed_row_identity", "seed_symbol", "seed_reference_ts", "selected_row_identity", "selected_reference_ts", "selected_status", "selected_side", "resolution_status", "is_resolved", "realized_r_before_cost", "realized_r_after_cost", "cost_r_deduction", "structural_distance_seed_to_selected"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(selected_rows)

    with (out_dir / "execution_realism_split_metrics.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = ["split", "selected_signals", "resolved_trades", "tp2_count", "tp2_rate", "stopped_count", "stopped_rate", "expired_ambiguous_count", "expired_ambiguous_rate", "mean_realized_r_before_cost", "mean_realized_r_after_cost", "total_realized_r_after_cost", "avg_structural_distance"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(split_metrics + symbol_rows)

    with (out_dir / "execution_realism_warnings.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = ["warning_code", "details"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(warnings)

    return {"summary_md": str(out_dir / "execution_realism_summary.md")}


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Research-only execution realism validation for surviving hypothesis.")
    p.add_argument("--labeled-dataset-path", required=True)
    p.add_argument("--output-root", required=True)
    p.add_argument("--forward-structural-window", type=int, default=20)
    p.add_argument("--cost-r-deduction", type=float, default=0.0)
    p.add_argument("--min-oos-sample", type=int, default=20)
    p.add_argument("--train-fraction", type=float, default=0.60)
    p.add_argument("--validation-fraction", type=float, default=0.20)
    p.add_argument("--oos-fraction", type=float, default=0.20)
    p.add_argument("--seed-pullback-quality", default="DEEP")
    p.add_argument("--seed-breakout-context", default="LATE_EXTENSION")
    p.add_argument("--allow-any-breakout-context", action="store_true")
    p.add_argument("--exclude-same-timestamp-opposite", action="store_true")
    return p


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    validate_execution_realism_surviving_hypothesis(
        labeled_dataset_path=args.labeled_dataset_path,
        output_root=args.output_root,
        forward_structural_window=args.forward_structural_window,
        cost_r_deduction=args.cost_r_deduction,
        min_oos_sample=args.min_oos_sample,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
        oos_fraction=args.oos_fraction,
        seed_pullback_quality=args.seed_pullback_quality,
        seed_breakout_context=args.seed_breakout_context,
        allow_any_breakout_context=args.allow_any_breakout_context,
        exclude_same_timestamp_opposite=args.exclude_same_timestamp_opposite,
    )
