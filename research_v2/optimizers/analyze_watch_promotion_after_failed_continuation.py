from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median
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


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _ratio(n: int, d: int) -> float:
    return n / d if d else 0.0


def _split_rows(rows: list[dict[str, Any]], train_fraction: float, validation_fraction: float, oos_fraction: float) -> dict[str, list[dict[str, Any]]]:
    if abs((train_fraction + validation_fraction + oos_fraction) - 1.0) > 1e-9:
        raise ValueError("train_fraction + validation_fraction + oos_fraction must equal 1.0")
    ordered = sorted(rows, key=lambda r: str(r.get("watch_reference_ts") or r.get("reference_ts", "")))
    n = len(ordered)
    train_end = int(n * train_fraction)
    validation_end = train_end + int(n * validation_fraction)
    return {"train": ordered[:train_end], "validation": ordered[train_end:validation_end], "oos": ordered[validation_end:]}


def _find_first_opposite_watch(symbol_rows: list[dict[str, Any]], seed_idx: int, forward_window_structural: int) -> tuple[int | None, dict[str, Any] | None]:
    max_idx = min(len(symbol_rows), seed_idx + forward_window_structural + 1)
    for idx in range(seed_idx + 1, max_idx):
        row = symbol_rows[idx]
        if _norm(row.get("side")) == "SHORT" and _norm(row.get("status")) == "WATCH":
            return idx, row
    return None, None


def _promotion_distances(symbol_rows: list[dict[str, Any]], watch_idx: int, forward_window_structural: int) -> tuple[int | None, int | None]:
    max_idx = min(len(symbol_rows), watch_idx + forward_window_structural + 1)
    candidate_distance: int | None = None
    tp2_distance: int | None = None
    for idx in range(watch_idx + 1, max_idx):
        row = symbol_rows[idx]
        if _norm(row.get("side")) != "SHORT":
            continue
        if candidate_distance is None and _norm(row.get("status")) == "CANDIDATE":
            candidate_distance = idx - watch_idx
        if tp2_distance is None and _norm(row.get("resolution_status")) == "TP2":
            tp2_distance = idx - watch_idx
        if candidate_distance is not None and tp2_distance is not None:
            break
    return candidate_distance, tp2_distance


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    promoted = [r for r in rows if r["distance_to_candidate"] != ""]
    tp2 = [r for r in rows if r["distance_to_tp2"] != ""]
    d_c = [int(r["distance_to_candidate"]) for r in promoted]
    d_t = [int(r["distance_to_tp2"]) for r in tp2]
    promoted_r = [_safe_float(r.get("watch_realized_r_multiple"), 0.0) for r in promoted]
    return {
        "watch_rows": len(rows),
        "watch_to_candidate_ratio": _ratio(len(promoted), len(rows)),
        "watch_to_tp2_ratio": _ratio(len(tp2), len(rows)),
        "median_distance_to_candidate": median(d_c) if d_c else 0.0,
        "median_distance_to_tp2": median(d_t) if d_t else 0.0,
        "mean_realized_r_promoted": mean(promoted_r) if promoted_r else 0.0,
    }


def analyze_watch_promotion_after_failed_continuation(*, labeled_dataset_path: str, output_root: str, forward_window_structural: int = 20, min_sample_size: int = 20, train_fraction: float = 0.60, validation_fraction: float = 0.20, oos_fraction: float = 0.20) -> dict[str, str]:
    rows = _load_rows(Path(labeled_dataset_path).resolve())
    _, _, identities = _select_identity_method(rows)
    row_identity_map = {id(row): row_id for row, row_id in zip(rows, identities)}

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol[_norm(row.get("symbol"))].append(row)
    for symbol in by_symbol:
        by_symbol[symbol] = sorted(by_symbol[symbol], key=lambda r: str(r.get("reference_ts", "")))

    baseline_watch_rows: list[dict[str, Any]] = []
    failure_watch_rows: list[dict[str, Any]] = []
    seed_rows: list[dict[str, Any]] = []

    for symbol, symbol_rows in by_symbol.items():
        for idx, row in enumerate(symbol_rows):
            side = _norm(row.get("side"))
            status = _norm(row.get("status"))
            if side == "SHORT" and status == "WATCH":
                d_c, d_t = _promotion_distances(symbol_rows, idx, forward_window_structural)
                baseline_watch_rows.append({
                    "population": "BASELINE_SHORT_WATCH",
                    "watch_row_identity": row_identity_map[id(row)],
                    "watch_symbol": symbol,
                    "watch_reference_ts": row.get("reference_ts", ""),
                    "watch_side": side,
                    "watch_status": status,
                    "watch_resolution_status": _norm(row.get("resolution_status")),
                    "watch_realized_r_multiple": row.get("realized_r_multiple", ""),
                    "distance_to_candidate": d_c if d_c is not None else "",
                    "distance_to_tp2": d_t if d_t is not None else "",
                })

            if side == "LONG" and _norm(row.get("resolution_status")) == "STOPPED":
                watch_idx, watch_row = _find_first_opposite_watch(symbol_rows, idx, forward_window_structural)
                seed_payload = {
                    "seed_row_identity": row_identity_map[id(row)],
                    "seed_symbol": symbol,
                    "seed_reference_ts": row.get("reference_ts", ""),
                    "seed_resolution_status": _norm(row.get("resolution_status")),
                    "seed_side": side,
                    "opposite_watch_found": "1" if watch_row else "0",
                    "opposite_watch_row_identity": row_identity_map[id(watch_row)] if watch_row else "",
                    "structural_distance_to_watch": (watch_idx - idx) if watch_idx is not None else "",
                }
                if watch_row is not None and watch_idx is not None:
                    d_c, d_t = _promotion_distances(symbol_rows, watch_idx, forward_window_structural)
                    watch_payload = {
                        "population": "FAILURE_CONDITIONED_SHORT_WATCH",
                        "watch_row_identity": row_identity_map[id(watch_row)],
                        "watch_symbol": symbol,
                        "watch_reference_ts": watch_row.get("reference_ts", ""),
                        "watch_side": _norm(watch_row.get("side")),
                        "watch_status": _norm(watch_row.get("status")),
                        "watch_resolution_status": _norm(watch_row.get("resolution_status")),
                        "watch_realized_r_multiple": watch_row.get("realized_r_multiple", ""),
                        "distance_to_candidate": d_c if d_c is not None else "",
                        "distance_to_tp2": d_t if d_t is not None else "",
                        "seed_row_identity": seed_payload["seed_row_identity"],
                        "seed_reference_ts": seed_payload["seed_reference_ts"],
                        "structural_distance_seed_to_watch": seed_payload["structural_distance_to_watch"],
                    }
                    failure_watch_rows.append(watch_payload)
                    seed_payload["distance_watch_to_candidate"] = watch_payload["distance_to_candidate"]
                    seed_payload["distance_watch_to_tp2"] = watch_payload["distance_to_tp2"]
                seed_rows.append(seed_payload)

    split_rows = _split_rows(failure_watch_rows, train_fraction, validation_fraction, oos_fraction)
    baseline_metrics = _metrics(baseline_watch_rows)
    failure_metrics = _metrics(failure_watch_rows)

    split_metrics = []
    for split in SPLITS:
        m = _metrics(split_rows[split])
        split_metrics.append({"split": split, "population": "FAILURE_CONDITIONED_SHORT_WATCH", **m})
        split_metrics.append({"split": split, "population": "BASELINE_SHORT_WATCH", **baseline_metrics})

    warnings: list[dict[str, str]] = []
    train_m = _metrics(split_rows["train"])
    val_m = _metrics(split_rows["validation"])
    oos_m = _metrics(split_rows["oos"])

    if train_m["watch_to_tp2_ratio"] > 0 and oos_m["watch_to_tp2_ratio"] < train_m["watch_to_tp2_ratio"] * 0.7:
        warnings.append({"warning_code": "OOS_COLLAPSE", "details": "OOS watch->TP2 conversion collapsed versus train."})
    if train_m["watch_to_tp2_ratio"] > 0 and (val_m["watch_to_tp2_ratio"] == 0.0 or oos_m["watch_to_tp2_ratio"] == 0.0):
        warnings.append({"warning_code": "TRAIN_ONLY_EDGE", "details": "TP2 promotion appears in train but vanishes later."})
    if len(failure_watch_rows) < min_sample_size:
        warnings.append({"warning_code": "SMALL_SAMPLE", "details": f"Failure-conditioned watch rows={len(failure_watch_rows)} < min_sample_size={min_sample_size}."})

    symbol_counts = Counter(r["watch_symbol"] for r in failure_watch_rows)
    if failure_watch_rows and symbol_counts:
        top_symbol, top_count = symbol_counts.most_common(1)[0]
        share = top_count / len(failure_watch_rows)
        if share > 0.7:
            warnings.append({"warning_code": "SYMBOL_CONCENTRATION", "details": f"{top_symbol} contributes {share:.1%} of failure-conditioned watch rows."})

    long_short_asym = failure_metrics["watch_to_tp2_ratio"] - baseline_metrics["watch_to_tp2_ratio"]
    if abs(long_short_asym) > 0.20:
        warnings.append({"warning_code": "LONG_SHORT_ASYMMETRY", "details": "Failure-conditioned LONG->SHORT promotion ratio is materially different from baseline SHORT watch TP2 ratio."})

    feature_summary = []
    for sym in ("ETH", "BTC", "SOL"):
        b = [r for r in baseline_watch_rows if r["watch_symbol"] == sym]
        f = [r for r in failure_watch_rows if r["watch_symbol"] == sym]
        feature_summary.append({
            "symbol": sym,
            "baseline_watch_count": len(b),
            "failure_watch_count": len(f),
            "baseline_watch_to_candidate": _metrics(b)["watch_to_candidate_ratio"] if b else 0.0,
            "failure_watch_to_candidate": _metrics(f)["watch_to_candidate_ratio"] if f else 0.0,
            "baseline_watch_to_tp2": _metrics(b)["watch_to_tp2_ratio"] if b else 0.0,
            "failure_watch_to_tp2": _metrics(f)["watch_to_tp2_ratio"] if f else 0.0,
        })

    out_dir = Path(output_root).resolve(); out_dir.mkdir(parents=True, exist_ok=True)
    summary = [
        "# WATCH Promotion Summary",
        "",
        "## Population definitions",
        "- Baseline population: all SHORT WATCH rows in labeled dataset.",
        "- Failure-conditioned population: first opposite SHORT WATCH after STOPPED LONG seed (same symbol, future-only, structural-distance window).",
        "",
        f"- baseline_short_watch_count: {len(baseline_watch_rows)}",
        f"- failure_conditioned_short_watch_count: {len(failure_watch_rows)}",
        f"- baseline WATCH->CANDIDATE: {baseline_metrics['watch_to_candidate_ratio']:.4f}",
        f"- failure-conditioned WATCH->CANDIDATE: {failure_metrics['watch_to_candidate_ratio']:.4f}",
        f"- baseline WATCH->TP2: {baseline_metrics['watch_to_tp2_ratio']:.4f}",
        f"- failure-conditioned WATCH->TP2: {failure_metrics['watch_to_tp2_ratio']:.4f}",
        f"- median distance to first CANDIDATE (baseline/failure): {baseline_metrics['median_distance_to_candidate']:.2f} / {failure_metrics['median_distance_to_candidate']:.2f}",
        f"- median distance to TP2 (baseline/failure): {baseline_metrics['median_distance_to_tp2']:.2f} / {failure_metrics['median_distance_to_tp2']:.2f}",
        f"- mean realized R of promoted rows (baseline/failure): {baseline_metrics['mean_realized_r_promoted']:.4f} / {failure_metrics['mean_realized_r_promoted']:.4f}",
        "",
        "## Split persistence (failure-conditioned)",
    ]
    for row in split_metrics:
        if row["population"] != "FAILURE_CONDITIONED_SHORT_WATCH":
            continue
        summary.append(f"- {row['split']}: n={row['watch_rows']} W->C={row['watch_to_candidate_ratio']:.4f} W->TP2={row['watch_to_tp2_ratio']:.4f}")
    summary.extend(["", "## Warnings"])
    summary.extend([f"- {w['warning_code']}: {w['details']}" for w in warnings] or ["- none"])
    (out_dir / "watch_promotion_summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")

    with (out_dir / "watch_promotion_split_metrics.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["split", "population", "watch_rows", "watch_to_candidate_ratio", "watch_to_tp2_ratio", "median_distance_to_candidate", "median_distance_to_tp2", "mean_realized_r_promoted"]
        w = csv.DictWriter(h, fieldnames=fields); w.writeheader(); w.writerows(split_metrics)

    with (out_dir / "watch_promotion_feature_summary.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["symbol", "baseline_watch_count", "failure_watch_count", "baseline_watch_to_candidate", "failure_watch_to_candidate", "baseline_watch_to_tp2", "failure_watch_to_tp2"]
        w = csv.DictWriter(h, fieldnames=fields); w.writeheader(); w.writerows(feature_summary)

    with (out_dir / "watch_promotion_seed_rows.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["seed_row_identity", "seed_symbol", "seed_reference_ts", "seed_resolution_status", "seed_side", "opposite_watch_found", "opposite_watch_row_identity", "structural_distance_to_watch", "distance_watch_to_candidate", "distance_watch_to_tp2"]
        w = csv.DictWriter(h, fieldnames=fields); w.writeheader(); w.writerows([{k: r.get(k, "") for k in fields} for r in seed_rows])

    with (out_dir / "watch_promotion_warnings.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["warning_code", "details"]
        w = csv.DictWriter(h, fieldnames=fields); w.writeheader(); w.writerows(warnings)

    return {"summary_md": str(out_dir / "watch_promotion_summary.md")}


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Research-only opposite WATCH promotion analysis after failed continuation.")
    p.add_argument("--labeled-dataset-path", required=True)
    p.add_argument("--output-root", required=True)
    p.add_argument("--forward-window-structural", type=int, default=20)
    p.add_argument("--min-sample-size", type=int, default=20)
    p.add_argument("--train-fraction", type=float, default=0.60)
    p.add_argument("--validation-fraction", type=float, default=0.20)
    p.add_argument("--oos-fraction", type=float, default=0.20)
    return p


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    analyze_watch_promotion_after_failed_continuation(
        labeled_dataset_path=args.labeled_dataset_path,
        output_root=args.output_root,
        forward_window_structural=args.forward_window_structural,
        min_sample_size=args.min_sample_size,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
        oos_fraction=args.oos_fraction,
    )
