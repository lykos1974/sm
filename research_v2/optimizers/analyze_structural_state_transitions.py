from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from research_v2.optimizers.analyze_rule_overlap import _select_identity_method

SPLITS: tuple[str, ...] = ("train", "validation", "oos")
TRANSITIONS: tuple[str, ...] = (
    "opposite_watch",
    "opposite_candidate",
    "opposite_tp2",
    "opposite_stopped",
    "same_side_watch",
    "same_side_tp2",
)


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _split_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    ordered = sorted(rows, key=lambda r: str(r.get("seed_reference_ts") or ""))
    n = len(ordered)
    train_end = int(n * 0.60)
    validation_end = train_end + int(n * 0.20)
    return {"train": ordered[:train_end], "validation": ordered[train_end:validation_end], "oos": ordered[validation_end:]}


def _find_first_transition(symbol_rows: list[dict[str, Any]], seed_idx: int, forward_structural_window: int) -> tuple[dict[str, str], list[dict[str, str]]]:
    seed_row = symbol_rows[seed_idx]
    seed_side = _norm(seed_row.get("side"))
    opposite_side = "SHORT" if seed_side == "LONG" else "LONG"

    first_distance: dict[str, int | None] = {name: None for name in TRANSITIONS}
    same_ts_opposite = 0

    # same-timestamp opposite-state warning probe (same symbol, not future rows)
    seed_ts = str(seed_row.get("reference_ts") or "")
    for idx, row in enumerate(symbol_rows):
        if idx == seed_idx:
            continue
        if str(row.get("reference_ts") or "") != seed_ts:
            continue
        if _norm(row.get("side")) == opposite_side:
            same_ts_opposite = 1
            break

    max_idx = min(len(symbol_rows), seed_idx + forward_structural_window + 1)
    for idx in range(seed_idx + 1, max_idx):
        row = symbol_rows[idx]
        side = _norm(row.get("side"))
        status = _norm(row.get("status"))
        resolution = _norm(row.get("resolution_status"))
        distance = idx - seed_idx

        if side == opposite_side:
            if first_distance["opposite_watch"] is None and status == "WATCH":
                first_distance["opposite_watch"] = distance
            if first_distance["opposite_candidate"] is None and status == "CANDIDATE":
                first_distance["opposite_candidate"] = distance
            if first_distance["opposite_tp2"] is None and resolution == "TP2":
                first_distance["opposite_tp2"] = distance
            if first_distance["opposite_stopped"] is None and resolution == "STOPPED":
                first_distance["opposite_stopped"] = distance

        if side == seed_side:
            if first_distance["same_side_watch"] is None and status == "WATCH":
                first_distance["same_side_watch"] = distance
            if first_distance["same_side_tp2"] is None and resolution == "TP2":
                first_distance["same_side_tp2"] = distance

    out: dict[str, str] = {}
    for name in TRANSITIONS:
        found = first_distance[name] is not None
        out[f"{name}_found"] = "1" if found else "0"
        out[f"{name}_distance"] = str(first_distance[name]) if found else ""

    warnings = []
    if same_ts_opposite:
        warnings.append({"warning_code": "SAME_TIMESTAMP_OPPOSITE_STATE", "details": "Seed has opposite-side row at same timestamp."})

    return out, warnings


def _ratio(n: int, d: int) -> float:
    return (n / d) if d else 0.0


def _dist(rows: list[dict[str, Any]], key: str) -> list[int]:
    return [int(r[key]) for r in rows if str(r.get(key, "")).strip() != ""]


def _metrics(rows: list[dict[str, Any]]) -> dict[str, float]:
    seed_count = len(rows)
    metrics: dict[str, float] = {"seed_count": float(seed_count)}
    for name in TRANSITIONS:
        found_key = f"{name}_found"
        dist_key = f"{name}_distance"
        found_count = sum(1 for r in rows if r.get(found_key) == "1")
        dvals = _dist(rows, dist_key)
        metrics[f"prob_{name}"] = _ratio(found_count, seed_count)
        metrics[f"median_distance_{name}"] = float(median(dvals)) if dvals else 0.0
        metrics[f"immediate_ratio_{name}"] = _ratio(sum(1 for d in dvals if d <= 3), seed_count)

    next_counts = Counter()
    for r in rows:
        found = []
        for name in TRANSITIONS:
            if r.get(f"{name}_found") == "1":
                found.append((int(r[f"{name}_distance"]), name))
        if found:
            next_counts[sorted(found)[0][1]] += 1
        else:
            next_counts["none"] += 1

    for state in list(TRANSITIONS) + ["none"]:
        metrics[f"next_state_prob_{state}"] = _ratio(next_counts[state], seed_count)

    return metrics


def analyze_structural_state_transitions(*, labeled_dataset_path: str, output_root: str, forward_structural_window: int = 20, seed_side: str = "LONG", seed_pullback_quality: str = "DEEP", seed_resolution_status: str = "STOPPED") -> dict[str, Any]:
    rows = _load_rows(Path(labeled_dataset_path).resolve())
    _, _, identities = _select_identity_method(rows)
    row_identity_map = {id(row): row_id for row, row_id in zip(rows, identities)}

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol[_norm(row.get("symbol"))].append(row)
    for symbol in by_symbol:
        by_symbol[symbol] = sorted(by_symbol[symbol], key=lambda r: str(r.get("reference_ts") or ""))

    seed_rows: list[dict[str, Any]] = []
    row_warnings: list[dict[str, str]] = []
    for symbol, symbol_rows in by_symbol.items():
        for idx, row in enumerate(symbol_rows):
            if _norm(row.get("resolution_status")) != _norm(seed_resolution_status):
                continue
            if _norm(row.get("side")) != _norm(seed_side):
                continue
            if _norm(row.get("pullback_quality")) != _norm(seed_pullback_quality):
                continue
            transition_data, warnings = _find_first_transition(symbol_rows, idx, forward_structural_window)
            seed_record = {
                "seed_row_identity": row_identity_map[id(row)],
                "seed_symbol": symbol,
                "seed_reference_ts": str(row.get("reference_ts") or ""),
                "seed_side": _norm(row.get("side")),
                "seed_pullback_quality": _norm(row.get("pullback_quality")),
                "seed_resolution_status": _norm(row.get("resolution_status")),
                **transition_data,
            }
            seed_rows.append(seed_record)
            for w in warnings:
                row_warnings.append({**w, "seed_row_identity": seed_record["seed_row_identity"], "seed_symbol": symbol, "seed_reference_ts": seed_record["seed_reference_ts"]})

    split_rows = _split_rows(seed_rows)
    overall = _metrics(seed_rows)
    split_metrics = [{"split": s, **_metrics(split_rows[s])} for s in SPLITS]

    matrix_rows = []
    for state in TRANSITIONS:
        matrix_rows.append({"from_state": "FAILED_LONG_DEEP_STOPPED", "to_state": state, "probability": overall.get(f"prob_{state}", 0.0), "probability_type": "independent_first_occurrence_within_window"})
    matrix_rows.append({"from_state": "FAILED_LONG_DEEP_STOPPED", "to_state": "none", "probability": overall.get("next_state_prob_none", 0.0), "probability_type": "no_transition_found_in_window"})

    symbol_counts = Counter(r["seed_symbol"] for r in seed_rows)
    symbol_metrics = []
    for symbol, count in sorted(symbol_counts.items()):
        scoped = [r for r in seed_rows if r["seed_symbol"] == symbol]
        m = _metrics(scoped)
        symbol_metrics.append({
            "symbol": symbol,
            "seed_count": count,
            "prob_opposite_watch": m["prob_opposite_watch"],
            "prob_opposite_candidate": m["prob_opposite_candidate"],
            "prob_opposite_tp2": m["prob_opposite_tp2"],
            "prob_same_side_tp2": m["prob_same_side_tp2"],
        })

    warnings = list(row_warnings)
    oos = next((m for m in split_metrics if m["split"] == "oos"), None)
    train = next((m for m in split_metrics if m["split"] == "train"), None)
    if oos and oos["seed_count"] < 10:
        warnings.append({"warning_code": "SMALL_OOS_SAMPLE", "details": f"oos seed_count={int(oos['seed_count'])} < 10", "seed_row_identity": "", "seed_symbol": "", "seed_reference_ts": ""})
    if seed_rows and symbol_counts and (max(symbol_counts.values()) / len(seed_rows) > 0.7):
        top_symbol, top_count = symbol_counts.most_common(1)[0]
        warnings.append({"warning_code": "SYMBOL_CONCENTRATION", "details": f"{top_symbol} share={top_count / len(seed_rows):.2%}", "seed_row_identity": "", "seed_symbol": top_symbol, "seed_reference_ts": ""})
    if train and oos and train["prob_opposite_tp2"] > 0 and oos["prob_opposite_tp2"] < (train["prob_opposite_tp2"] * 0.5):
        warnings.append({"warning_code": "OOS_COLLAPSE", "details": "opposite_tp2 probability collapsed vs train", "seed_row_identity": "", "seed_symbol": "", "seed_reference_ts": ""})
    if seed_rows and sum(1 for r in seed_rows if sum(1 for t in TRANSITIONS if r[f"{t}_found"] == "1") >= 2) / len(seed_rows) > 0.5:
        warnings.append({"warning_code": "TRANSITION_AMBIGUITY", "details": "More than 50% of seeds have multiple transition types in window.", "seed_row_identity": "", "seed_symbol": "", "seed_reference_ts": ""})

    out_dir = Path(output_root).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_lines = [
        "# Structural State Transition Summary",
        "",
        f"- total seeds: {len(seed_rows)}",
        f"- opposite WATCH probability: {overall['prob_opposite_watch']:.4f}",
        f"- opposite CANDIDATE probability: {overall['prob_opposite_candidate']:.4f}",
        f"- opposite TP2 probability: {overall['prob_opposite_tp2']:.4f}",
        f"- same-side continuation probability (WATCH): {overall['prob_same_side_watch']:.4f}",
        f"- same-side continuation probability (TP2): {overall['prob_same_side_tp2']:.4f}",
        "",
        "## Next-state probabilities",
    ]
    for state in list(TRANSITIONS) + ["none"]:
        summary_lines.append(f"- {state}: {overall[f'next_state_prob_{state}']:.4f}")
    summary_lines.extend(["", "## Median structural distances"]) 
    for state in TRANSITIONS:
        summary_lines.append(f"- {state}: median={overall[f'median_distance_{state}']:.2f}, immediate<=3={overall[f'immediate_ratio_{state}']:.4f}")
    summary_lines.extend(["", "## Split stability (train/validation/oos)"])
    for m in split_metrics:
        summary_lines.append(
            f"- {m['split']}: seeds={int(m['seed_count'])}, opp_watch={m['prob_opposite_watch']:.4f}, opp_candidate={m['prob_opposite_candidate']:.4f}, opp_tp2={m['prob_opposite_tp2']:.4f}, same_tp2={m['prob_same_side_tp2']:.4f}"
        )
    summary_lines.extend(["", "## SOL/BTC/ETH comparison"])
    for sym in ("SOL", "BTC", "ETH"):
        sm = next((x for x in symbol_metrics if x["symbol"] == sym), None)
        if sm:
            summary_lines.append(f"- {sym}: seeds={sm['seed_count']}, opp_watch={sm['prob_opposite_watch']:.4f}, opp_candidate={sm['prob_opposite_candidate']:.4f}, opp_tp2={sm['prob_opposite_tp2']:.4f}, same_tp2={sm['prob_same_side_tp2']:.4f}")
        else:
            summary_lines.append(f"- {sym}: no seeds")
    summary_lines.extend(["", "## Structural semantics", "- Future progression is structural-row order progression within same symbol.", "- Same-timestamp later-row events are treated as future structural rows (not wall-clock time progression).", "- Matrix probabilities are independent first-occurrence event probabilities and are not required to sum to 1.", "", "## Warnings"])
    if warnings:
        for w in warnings:
            summary_lines.append(f"- {w['warning_code']}: {w['details']}")
    else:
        summary_lines.append("- none")

    (out_dir / "structural_state_transition_summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    with (out_dir / "structural_state_transition_rows.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["seed_row_identity", "seed_symbol", "seed_reference_ts", "seed_side", "seed_pullback_quality", "seed_resolution_status"] + [f"{t}_found" for t in TRANSITIONS] + [f"{t}_distance" for t in TRANSITIONS]
        w = csv.DictWriter(h, fieldnames=fields)
        w.writeheader()
        w.writerows([{k: r.get(k, "") for k in fields} for r in seed_rows])

    with (out_dir / "structural_state_transition_matrix.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["from_state", "to_state", "probability", "probability_type"]
        w = csv.DictWriter(h, fieldnames=fields)
        w.writeheader()
        w.writerows(matrix_rows)

    with (out_dir / "structural_state_transition_split_metrics.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["split", "seed_count"] + [f"prob_{t}" for t in TRANSITIONS] + [f"median_distance_{t}" for t in TRANSITIONS] + [f"immediate_ratio_{t}" for t in TRANSITIONS] + [f"next_state_prob_{t}" for t in TRANSITIONS] + ["next_state_prob_none"]
        w = csv.DictWriter(h, fieldnames=fields)
        w.writeheader()
        w.writerows([{k: r.get(k, "") for k in fields} for r in split_metrics])

    with (out_dir / "structural_state_transition_warnings.csv").open("w", encoding="utf-8", newline="") as h:
        fields = ["warning_code", "details", "seed_row_identity", "seed_symbol", "seed_reference_ts"]
        w = csv.DictWriter(h, fieldnames=fields)
        w.writeheader()
        w.writerows(warnings)

    return {"total_seeds": len(seed_rows), "output_root": str(out_dir)}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only structural state-transition analyzer.")
    parser.add_argument("--labeled-dataset-path", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--forward-structural-window", type=int, default=20)
    parser.add_argument("--seed-side", default="LONG")
    parser.add_argument("--seed-pullback-quality", default="DEEP")
    parser.add_argument("--seed-resolution-status", default="STOPPED")
    return parser


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    analyze_structural_state_transitions(
        labeled_dataset_path=args.labeled_dataset_path,
        output_root=args.output_root,
        forward_structural_window=args.forward_structural_window,
        seed_side=args.seed_side,
        seed_pullback_quality=args.seed_pullback_quality,
        seed_resolution_status=args.seed_resolution_status,
    )
