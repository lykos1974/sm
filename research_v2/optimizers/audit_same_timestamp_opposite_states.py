from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

TARGET_SYMBOLS = ("BINANCE_FUT:ETHUSDT", "BINANCE_FUT:BTCUSDT", "BINANCE_FUT:SOLUSDT")


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _set_string(values: set[str]) -> str:
    normalized = sorted(v for v in values if v)
    return "|".join(normalized)


def _classify_group(*, row_count: int, side_only_diff: bool, any_tp2: bool, both_tp2_present: bool, has_same_status: bool) -> str:
    if both_tp2_present:
        return "STRUCTURAL_POLARITY_TRANSITION"
    if side_only_diff and row_count == 2:
        return "PURE_SIDE_FLIP"
    if side_only_diff and row_count > 2:
        return "POSSIBLE_DUPLICATE_LABELING"
    if row_count > 2:
        return "MULTI_STATE_EXPANSION"
    if both_tp2_present:
        return "STRUCTURAL_POLARITY_TRANSITION"
    if not side_only_diff:
        return "SIDE_PLUS_STATUS_TRANSITION"
    if any_tp2 and has_same_status:
        return "STRUCTURAL_POLARITY_TRANSITION"
    return "POSSIBLE_DUPLICATE_LABELING"


def audit_same_timestamp_opposite_states(*, labeled_dataset_path: str, output_root: str) -> dict[str, Any]:
    rows = _load_rows(Path(labeled_dataset_path).resolve())

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (_norm(row.get("symbol")), str(row.get("reference_ts") or ""))
        grouped[key].append(row)

    opposite_groups: list[dict[str, str]] = []
    class_rows: list[dict[str, str]] = []

    long_deep_stopped_count = 0
    long_deep_stopped_affected = 0

    for row in rows:
        if _norm(row.get("side")) == "LONG" and _norm(row.get("pullback_quality")) == "DEEP" and _norm(row.get("resolution_status")) == "STOPPED":
            long_deep_stopped_count += 1

    for (symbol, reference_ts), gro in sorted(grouped.items()):
        sides = {_norm(r.get("side")) for r in gro}
        if not ({"LONG", "SHORT"} <= sides):
            continue

        statuses = {_norm(r.get("status")) for r in gro}
        resolutions = {_norm(r.get("resolution_status")) for r in gro}
        contexts = {_norm(r.get("breakout_context")) for r in gro}
        pullback_qualities = {_norm(r.get("pullback_quality")) for r in gro}
        exec_classes = {_norm(r.get("continuation_execution_class")) for r in gro}
        strategies = {_norm(r.get("strategy")) for r in gro}

        other_dims = [statuses, resolutions, contexts, pullback_qualities, exec_classes, strategies]
        side_only_diff = all(len({v for v in dim if v}) <= 1 for dim in other_dims)

        has_long_tp2 = any(_norm(r.get("side")) == "LONG" and _norm(r.get("resolution_status")) == "TP2" for r in gro)
        has_short_tp2 = any(_norm(r.get("side")) == "SHORT" and _norm(r.get("resolution_status")) == "TP2" for r in gro)
        any_tp2 = has_long_tp2 or has_short_tp2
        both_tp2_present = has_long_tp2 and has_short_tp2
        has_same_status = len(statuses) == 1 and "" not in statuses

        classification = _classify_group(
            row_count=len(gro),
            side_only_diff=side_only_diff,
            any_tp2=any_tp2,
            both_tp2_present=both_tp2_present,
            has_same_status=has_same_status,
        )

        group_record = {
            "symbol": symbol,
            "reference_ts": reference_ts,
            "row_count": str(len(gro)),
            "sides": _set_string(sides),
            "involved_statuses": _set_string(statuses),
            "involved_resolution_statuses": _set_string(resolutions),
            "involved_breakout_contexts": _set_string(contexts),
            "involved_pullback_quality": _set_string(pullback_qualities),
            "involved_continuation_execution_class": _set_string(exec_classes),
            "involved_strategies": _set_string(strategies),
            "side_only_difference": "1" if side_only_diff else "0",
            "has_tp2": "1" if any_tp2 else "0",
            "opposite_tp2_coexistence": "1" if both_tp2_present else "0",
            "classification": classification,
        }
        opposite_groups.append(group_record)
        class_rows.append({"symbol": symbol, "reference_ts": reference_ts, "classification": classification, "row_count": str(len(gro)), "side_only_difference": group_record["side_only_difference"], "opposite_tp2_coexistence": group_record["opposite_tp2_coexistence"]})

        if any(
            _norm(r.get("side")) == "LONG" and _norm(r.get("pullback_quality")) == "DEEP" and _norm(r.get("resolution_status")) == "STOPPED"
            for r in gro
        ):
            long_deep_stopped_affected += 1

    out_dir = Path(output_root).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    groups_path = out_dir / "same_timestamp_opposite_state_groups.csv"
    class_path = out_dir / "same_timestamp_opposite_state_classification.csv"
    summary_path = out_dir / "same_timestamp_opposite_state_summary.md"

    if opposite_groups:
        with groups_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(opposite_groups[0].keys()))
            writer.writeheader()
            writer.writerows(opposite_groups)
    else:
        groups_path.write_text("", encoding="utf-8")

    if class_rows:
        with class_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(class_rows[0].keys()))
            writer.writeheader()
            writer.writerows(class_rows)
    else:
        class_path.write_text("", encoding="utf-8")

    total_rows = len(rows)
    affected_rows = sum(int(g["row_count"]) for g in opposite_groups)
    pct_dataset = (affected_rows / total_rows) if total_rows else 0.0
    pct_deep_long_stopped = (long_deep_stopped_affected / long_deep_stopped_count) if long_deep_stopped_count else 0.0

    by_symbol_groups = Counter(g["symbol"] for g in opposite_groups)
    symbol_lines = []
    for symbol in TARGET_SYMBOLS:
        denom = sum(1 for r in rows if _norm(r.get("symbol")) == symbol)
        gcount = by_symbol_groups[symbol]
        symbol_lines.append(f"- {symbol}: groups={gcount}, affected_rows_share={(gcount / denom if denom else 0.0):.4f}")

    tp2_groups = sum(1 for g in opposite_groups if g["has_tp2"] == "1")
    opposite_tp2_groups = sum(1 for g in opposite_groups if g["opposite_tp2_coexistence"] == "1")

    warnings: list[str] = []
    duplicate_like_ratio = (sum(1 for g in opposite_groups if g["classification"] == "POSSIBLE_DUPLICATE_LABELING") / len(opposite_groups)) if opposite_groups else 0.0
    if duplicate_like_ratio > 0.25:
        warnings.append("HIGH_DUPLICATE_LIKE_BEHAVIOR")
    tp2_coexist_ratio = (opposite_tp2_groups / len(opposite_groups)) if opposite_groups else 0.0
    if tp2_coexist_ratio > 0.15:
        warnings.append("HEAVY_SAME_TIMESTAMP_TP2_COEXISTENCE")
    if opposite_groups and by_symbol_groups.most_common(1)[0][1] / len(opposite_groups) > 0.65:
        warnings.append("SYMBOL_CONCENTRATION")
    ambiguous_ratio = (sum(1 for g in opposite_groups if g["classification"] == "MULTI_STATE_EXPANSION") / len(opposite_groups)) if opposite_groups else 0.0
    if ambiguous_ratio > 0.30:
        warnings.append("AMBIGUOUS_STATE_EXPLOSION")

    summary_lines = [
        "# Same Timestamp Opposite State Causality Audit",
        "",
        f"- total_dataset_rows: {total_rows}",
        f"- opposite_state_groups: {len(opposite_groups)}",
        f"- affected_rows: {affected_rows}",
        f"- affected_dataset_percentage: {pct_dataset:.4%}",
        f"- deep_stopped_long_seed_rows: {long_deep_stopped_count}",
        f"- deep_stopped_long_seed_groups_affected: {long_deep_stopped_affected}",
        f"- deep_stopped_long_seed_groups_affected_percentage: {pct_deep_long_stopped:.4%}",
        f"- tp2_associated_groups: {tp2_groups}",
        f"- opposite_tp2_coexisting_groups: {opposite_tp2_groups}",
        "",
        "## ETH/BTC/SOL group distribution",
        *symbol_lines,
        "",
        "## Warnings",
    ]
    if warnings:
        summary_lines.extend(f"- {w}" for w in warnings)
    else:
        summary_lines.append("- none")

    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {
        "summary_path": str(summary_path),
        "groups_path": str(groups_path),
        "classification_path": str(class_path),
        "opposite_groups": len(opposite_groups),
        "warnings": warnings,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit same-timestamp opposite-side structural states.")
    parser.add_argument("--labeled-dataset-path", required=True)
    parser.add_argument("--output-root", required=True)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    audit_same_timestamp_opposite_states(
        labeled_dataset_path=args.labeled_dataset_path,
        output_root=args.output_root,
    )


if __name__ == "__main__":
    main()
