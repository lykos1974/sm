from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from research_v2.optimizers.analyze_rule_overlap import _select_identity_method

SPLITS: tuple[str, ...] = ("train", "validation", "oos")


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _split_rows(rows: list[dict[str, Any]], train_fraction: float, validation_fraction: float, oos_fraction: float) -> dict[str, list[dict[str, Any]]]:
    if abs((train_fraction + validation_fraction + oos_fraction) - 1.0) > 1e-9:
        raise ValueError("train_fraction + validation_fraction + oos_fraction must equal 1.0")
    ordered = sorted(rows, key=lambda r: str(r.get("seed_reference_ts") or ""))
    n = len(ordered)
    train_end = int(n * train_fraction)
    validation_end = train_end + int(n * validation_fraction)
    return {"train": ordered[:train_end], "validation": ordered[train_end:validation_end], "oos": ordered[validation_end:]}


def _percentile(values: list[int], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    idx = q * (len(ordered) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _find_progression(symbol_rows: list[dict[str, Any]], seed_idx: int, opposite_side: str, forward_structural_window: int) -> dict[str, Any]:
    max_idx = min(len(symbol_rows), seed_idx + forward_structural_window + 1)
    watch_distance = None
    candidate_distance = None
    tp2_distance = None
    for idx in range(seed_idx + 1, max_idx):
        row = symbol_rows[idx]
        if _norm(row.get("side")) != opposite_side:
            continue
        status = _norm(row.get("status"))
        resolution = _norm(row.get("resolution_status"))
        distance = idx - seed_idx
        if watch_distance is None and status == "WATCH":
            watch_distance = distance
        if candidate_distance is None and status == "CANDIDATE":
            candidate_distance = distance
        if tp2_distance is None and resolution == "TP2":
            tp2_distance = distance
    return {
        "opposite_watch_found": "1" if watch_distance is not None else "0",
        "opposite_candidate_found": "1" if candidate_distance is not None else "0",
        "opposite_tp2_found": "1" if tp2_distance is not None else "0",
        "structural_distance_to_watch": watch_distance if watch_distance is not None else "",
        "structural_distance_to_candidate": candidate_distance if candidate_distance is not None else "",
        "structural_distance_to_tp2": tp2_distance if tp2_distance is not None else "",
        "opposite_watch_status": "WATCH" if watch_distance is not None else "",
        "opposite_candidate_status": "CANDIDATE" if candidate_distance is not None else "",
        "opposite_tp2_status": "TP2" if tp2_distance is not None else "",
    }


def analyze_structural_reversal_progression(*, labeled_dataset_path: str, output_root: str, forward_structural_window: int = 20, split_mode: str = "time", train_fraction: float = 0.60, validation_fraction: float = 0.20, oos_fraction: float = 0.20, min_sample_size: int = 10) -> dict[str, Any]:
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

    seed_rows: list[dict[str, Any]] = []
    for symbol, symbol_rows in by_symbol.items():
        for idx, row in enumerate(symbol_rows):
            side = _norm(row.get("side"))
            if _norm(row.get("resolution_status")) != "STOPPED" or side not in {"LONG", "SHORT"}:
                continue
            opposite_side = "SHORT" if side == "LONG" else "LONG"
            progression = _find_progression(symbol_rows, idx, opposite_side, forward_structural_window)
            seed_rows.append({
                "seed_row_identity": row_identity_map[id(row)],
                "seed_side": side,
                "seed_symbol": symbol,
                "seed_reference_ts": row.get("reference_ts", ""),
                "seed_breakout_context": _norm(row.get("breakout_context")),
                "seed_pullback_quality": _norm(row.get("pullback_quality")),
                "seed_active_leg_boxes": str(row.get("active_leg_boxes", "")).strip(),
                "seed_continuation_execution_class": _norm(row.get("continuation_execution_class")),
                "seed_trend_regime": _norm(row.get("trend_regime")),
                "seed_late_extension": _norm(row.get("late_extension")),
                **progression,
            })

    splits = _split_rows(seed_rows, train_fraction, validation_fraction, oos_fraction)

    def _ratio(n: int, d: int) -> float:
        return (n / d) if d else 0.0

    def _dist(rows_in: list[dict[str, Any]], key: str) -> list[int]:
        return [int(r[key]) for r in rows_in if str(r.get(key, "")).strip() != ""]

    def _split_metrics(rows_in: list[dict[str, Any]], side: str | None = None) -> dict[str, float]:
        scoped = [r for r in rows_in if r["seed_side"] == side] if side else rows_in
        tp2_dist = _dist(scoped, "structural_distance_to_tp2")
        return {
            "watch_emergence": _ratio(sum(1 for r in scoped if r["opposite_watch_found"] == "1"), len(scoped)),
            "candidate_emergence": _ratio(sum(1 for r in scoped if r["opposite_candidate_found"] == "1"), len(scoped)),
            "tp2_emergence": _ratio(sum(1 for r in scoped if r["opposite_tp2_found"] == "1"), len(scoped)),
            "median_tp2_distance": median(tp2_dist) if tp2_dist else 0.0,
            "immediate_tp2_ratio": _ratio(sum(1 for d in tp2_dist if d <= 3), len(scoped)),
            "seed_count": len(scoped),
        }

    feature_summary: list[dict[str, Any]] = []
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for r in seed_rows:
        key = (
            r["seed_breakout_context"], r["seed_pullback_quality"], r["seed_active_leg_boxes"], r["seed_continuation_execution_class"],
            r["seed_trend_regime"], r["seed_side"], r["seed_symbol"],
        )
        groups[key].append(r)
    for key, grp in sorted(groups.items()):
        watch_dist = _dist(grp, "structural_distance_to_watch")
        cand_dist = _dist(grp, "structural_distance_to_candidate")
        tp2_dist = _dist(grp, "structural_distance_to_tp2")
        feature_summary.append({
            "breakout_context": key[0], "pullback_quality": key[1], "active_leg_boxes": key[2], "continuation_execution_class": key[3], "trend_regime": key[4], "side": key[5], "symbol": key[6],
            "seed_count": len(grp),
            "watch_emergence_ratio": _ratio(sum(1 for r in grp if r["opposite_watch_found"] == "1"), len(grp)),
            "candidate_emergence_ratio": _ratio(sum(1 for r in grp if r["opposite_candidate_found"] == "1"), len(grp)),
            "tp2_emergence_ratio": _ratio(sum(1 for r in grp if r["opposite_tp2_found"] == "1"), len(grp)),
            "median_watch_distance": median(watch_dist) if watch_dist else 0.0,
            "median_candidate_distance": median(cand_dist) if cand_dist else 0.0,
            "median_tp2_distance": median(tp2_dist) if tp2_dist else 0.0,
            "immediate_tp2_ratio": _ratio(sum(1 for d in tp2_dist if d <= 3), len(grp)),
        })

    split_rows = []
    for split, split_data in splits.items():
        m_all = _split_metrics(split_data)
        m_l2s = _split_metrics(split_data, "LONG")
        m_s2l = _split_metrics(split_data, "SHORT")
        split_rows.append({"split": split, **m_all, "long_to_short_tp2_emergence": m_l2s["tp2_emergence"], "short_to_long_tp2_emergence": m_s2l["tp2_emergence"]})

    all_metrics = _split_metrics(seed_rows)
    l2s = _split_metrics(seed_rows, "LONG")
    s2l = _split_metrics(seed_rows, "SHORT")

    warnings: list[dict[str, str]] = []
    split_map = {r["split"]: r for r in split_rows}
    if split_map["train"]["tp2_emergence"] > 0 and split_map["oos"]["tp2_emergence"] < split_map["train"]["tp2_emergence"] * 0.7:
        warnings.append({"warning_code": "OOS_COLLAPSE", "details": "OOS TP2 emergence collapses versus train."})
    if split_map["train"]["tp2_emergence"] > 0 and (split_map["validation"]["tp2_emergence"] == 0.0 or split_map["oos"]["tp2_emergence"] == 0.0):
        warnings.append({"warning_code": "TRAIN_ONLY_EDGE", "details": "TP2 propagation appears in train but vanishes later."})
    symbol_counts: dict[str, int] = defaultdict(int)
    for r in seed_rows:
        symbol_counts[r["seed_symbol"]] += 1
    if seed_rows:
        top = max(symbol_counts, key=symbol_counts.get)
        share = symbol_counts[top] / len(seed_rows)
        if share > 0.7:
            warnings.append({"warning_code": "SYMBOL_CONCENTRATION", "details": f"{top} share={share:.1%} of seeds."})
    if len(seed_rows) < min_sample_size:
        warnings.append({"warning_code": "SMALL_SAMPLE", "details": f"Total seeds={len(seed_rows)} < min_sample_size={min_sample_size}."})
    if abs(l2s["tp2_emergence"] - s2l["tp2_emergence"]) > 0.25:
        warnings.append({"warning_code": "LONG_SHORT_ASYMMETRY", "details": "Large LONG→SHORT vs SHORT→LONG TP2 propagation asymmetry."})
    if all_metrics["tp2_emergence"] < 0.35:
        warnings.append({"warning_code": "WEAK_TP2_PROPAGATION", "details": "Overall TP2 propagation is weak."})

    out_dir = Path(output_root).resolve(); out_dir.mkdir(parents=True, exist_ok=True)
    summary = ["# Structural Reversal Progression Summary", "", f"- total failure seeds: {len(seed_rows)}", f"- LONG→SHORT WATCH/CANDIDATE/TP2: {l2s['watch_emergence']:.4f} / {l2s['candidate_emergence']:.4f} / {l2s['tp2_emergence']:.4f}", f"- SHORT→LONG WATCH/CANDIDATE/TP2: {s2l['watch_emergence']:.4f} / {s2l['candidate_emergence']:.4f} / {s2l['tp2_emergence']:.4f}"]
    for name, key in [("WATCH", "structural_distance_to_watch"), ("CANDIDATE", "structural_distance_to_candidate"), ("TP2", "structural_distance_to_tp2")]:
        vals = _dist(seed_rows, key)
        summary.append(f"- {name} distance p25/p50/p75: {_percentile(vals,0.25):.2f}/{_percentile(vals,0.50):.2f}/{_percentile(vals,0.75):.2f}; immediate(1-3)={_ratio(sum(1 for d in vals if d<=3), len(seed_rows)):.4f}")
    summary.extend(["", "## Direct answers", f"- Does failed continuation structurally propagate into opposite-side states? {'YES' if all_metrics['watch_emergence']>0.5 else 'PARTIAL'}.", f"- Is propagation immediate in structural terms? {'YES' if all_metrics['immediate_tp2_ratio']>=0.3 else 'NO'}.", f"- Is propagation asymmetric? {'YES' if any(w['warning_code']=='LONG_SHORT_ASYMMETRY' for w in warnings) else 'NO'}.", f"- Does TP2 propagation survive OOS? {'NO' if any(w['warning_code']=='OOS_COLLAPSE' for w in warnings) else 'YES'}.", f"- Are WATCH/CANDIDATE transitions stronger than TP2 transitions? {'YES' if all_metrics['watch_emergence']>=all_metrics['tp2_emergence'] and all_metrics['candidate_emergence']>=all_metrics['tp2_emergence'] else 'NO'}.", f"- Is propagation concentrated in ETH or broader? {'ETH_CONCENTRATED' if symbol_counts.get('ETH',0) / max(len(seed_rows),1) > 0.7 else 'BROADER'}."])
    summary.extend(["", "## Warnings"] + [f"- {w['warning_code']}: {w['details']}" for w in warnings] if warnings else ["", "## Warnings", "- none"])
    (out_dir / "structural_reversal_progression_summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")

    with (out_dir / "structural_progression_rows.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["seed_row_identity", "seed_side", "seed_symbol", "seed_reference_ts", "seed_breakout_context", "seed_pullback_quality", "seed_active_leg_boxes", "seed_continuation_execution_class", "opposite_watch_found", "opposite_candidate_found", "opposite_tp2_found", "structural_distance_to_watch", "structural_distance_to_candidate", "structural_distance_to_tp2", "opposite_watch_status", "opposite_candidate_status", "opposite_tp2_status"]
        w = csv.DictWriter(h, fieldnames=fields); w.writeheader(); w.writerows([{k: r.get(k, "") for k in fields} for r in seed_rows])
    with (out_dir / "structural_progression_feature_summary.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["breakout_context", "pullback_quality", "active_leg_boxes", "continuation_execution_class", "trend_regime", "side", "symbol", "seed_count", "watch_emergence_ratio", "candidate_emergence_ratio", "tp2_emergence_ratio", "median_watch_distance", "median_candidate_distance", "median_tp2_distance", "immediate_tp2_ratio"]
        w = csv.DictWriter(h, fieldnames=fields); w.writeheader(); w.writerows(feature_summary)
    with (out_dir / "structural_progression_split_metrics.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["split", "seed_count", "watch_emergence", "candidate_emergence", "tp2_emergence", "median_tp2_distance", "immediate_tp2_ratio", "long_to_short_tp2_emergence", "short_to_long_tp2_emergence"]
        w = csv.DictWriter(h, fieldnames=fields); w.writeheader(); w.writerows(split_rows)
    with (out_dir / "structural_progression_warnings.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["warning_code", "details"]
        w = csv.DictWriter(h, fieldnames=fields); w.writeheader(); w.writerows(warnings)

    return {"summary_md": str(out_dir / "structural_reversal_progression_summary.md")}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only structural reversal progression analyzer.")
    parser.add_argument("--labeled-dataset-path", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--forward-structural-window", type=int, default=20)
    parser.add_argument("--split-mode", default="time", choices=["time"])
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--oos-fraction", type=float, default=0.20)
    parser.add_argument("--min-sample-size", type=int, default=10)
    return parser


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    analyze_structural_reversal_progression(
        labeled_dataset_path=args.labeled_dataset_path,
        output_root=args.output_root,
        forward_structural_window=args.forward_structural_window,
        split_mode=args.split_mode,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
        oos_fraction=args.oos_fraction,
        min_sample_size=args.min_sample_size,
    )
