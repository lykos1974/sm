from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from research_v2.optimizers.analyze_rule_overlap import _load_top_rules, _select_identity_method
from research_v2.optimizers.manual_filter_evaluator import apply_rule

STRUCTURAL_FIELDS: tuple[str, ...] = (
    "symbol",
    "side",
    "status",
    "breakout_context",
    "pullback_quality",
    "trend_regime",
    "continuation_execution_class",
    "active_leg_boxes",
    "entry_distance_bucket",
    "quality_score",
    "resolution_status",
)


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _build_feature_breakdown(rows: list[dict[str, Any]], feature_names: tuple[str, ...]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    total = len(rows)
    for feature in feature_names:
        counts = Counter((r.get(feature, "") or "").strip() for r in rows)
        for value, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
            out.append(
                {
                    "feature": feature,
                    "value": value,
                    "count": count,
                    "share": f"{(count / total):.6f}" if total else "0.000000",
                }
            )
    return out


def analyze_recurring_rows(*, ranked_rules_csv: str, top_rules_dir: str, labeled_dataset_path: str, top_n: int = 20, output_root: str) -> dict[str, Any]:
    ranked_path = Path(ranked_rules_csv).resolve()
    top_rules_path = Path(top_rules_dir).resolve()
    dataset_path = Path(labeled_dataset_path).resolve()
    output_path = Path(output_root).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    rows = _load_csv_rows(dataset_path)
    identity_method, identity_descriptor, row_ids = _select_identity_method(rows)
    row_identity_map: dict[int, str] = {id(row): row_id for row, row_id in zip(rows, row_ids)}
    top_rules = _load_top_rules(ranked_path, top_rules_path, top_n)

    recurring: dict[str, dict[str, Any]] = {}
    rule_match_sizes: dict[str, int] = {}
    for entry in top_rules:
        rule_id = entry["rule_id"]
        matched_rows = apply_rule(rows, entry["rule"])
        matched_ids = [row_identity_map[id(row)] for row in matched_rows]
        if len(set(matched_ids)) != len(matched_ids):
            raise ValueError(
                f"Identity collision detected while mapping matched rows for rule {rule_id}. "
                f"matched_rows={len(matched_rows)} unique_matched_ids={len(set(matched_ids))}"
            )
        rule_match_sizes[rule_id] = len(matched_ids)
        for row in matched_rows:
            rid = row_identity_map[id(row)]
            slot = recurring.setdefault(
                rid,
                {
                    "row": row,
                    "rule_ids": [],
                },
            )
            slot["rule_ids"].append(rule_id)

    recurring_rows: list[dict[str, Any]] = []
    for rid, payload in recurring.items():
        row = payload["row"]
        matched_rule_ids = sorted(payload["rule_ids"])
        recurring_rows.append(
            {
                "row_identity": rid,
                "recurring_match_count": len(matched_rule_ids),
                "matched_rule_ids": ",".join(matched_rule_ids),
                "realized_r_multiple": row.get("realized_r_multiple", ""),
                **{field: row.get(field, "") for field in STRUCTURAL_FIELDS},
            }
        )

    recurring_rows.sort(
        key=lambda r: (
            -int(r["recurring_match_count"]),
            -_safe_float(r.get("realized_r_multiple", ""), default=-999999.0),
        )
    )

    breakdown_rows = _build_feature_breakdown(recurring_rows, STRUCTURAL_FIELDS)

    recurrence_dist = Counter(int(r["recurring_match_count"]) for r in recurring_rows)
    total_rows = len(recurring_rows)
    tp2_rows = [r for r in recurring_rows if (r.get("resolution_status", "") or "").upper() == "TP2"]
    stopped_rows = [r for r in recurring_rows if (r.get("resolution_status", "") or "").upper() == "STOPPED"]
    high_recurrence_rows = [r for r in recurring_rows if int(r["recurring_match_count"]) >= 2]
    high_rec_tp2 = [r for r in high_recurrence_rows if (r.get("resolution_status", "") or "").upper() == "TP2"]
    high_rec_stopped = [r for r in high_recurrence_rows if (r.get("resolution_status", "") or "").upper() == "STOPPED"]

    eth_high = [r for r in high_recurrence_rows if (r.get("symbol", "") or "").upper().startswith("ETH")]
    watch_high = [r for r in high_recurrence_rows if (r.get("status", "") or "").upper() == "WATCH"]
    candidate_high = [r for r in high_recurrence_rows if (r.get("status", "") or "").upper() == "CANDIDATE"]
    continuation_counts = Counter((r.get("continuation_execution_class", "") or "") for r in high_recurrence_rows)

    recurrence_table = [
        f"- count={count}: rows={freq} share={(freq / total_rows):.1%}" for count, freq in sorted(recurrence_dist.items(), reverse=True)
    ]

    top_tp2 = [f"  - {r['row_identity']} | recurring_match_count={r['recurring_match_count']} | realized_r_multiple={r.get('realized_r_multiple', '')}" for r in tp2_rows[:10]]
    top_stopped = [f"  - {r['row_identity']} | recurring_match_count={r['recurring_match_count']} | realized_r_multiple={r.get('realized_r_multiple', '')}" for r in stopped_rows[:10]]

    regime_counts = Counter((r.get("breakout_context", ""), r.get("pullback_quality", ""), r.get("active_leg_boxes", "")) for r in high_recurrence_rows)
    top_regime = regime_counts.most_common(1)[0] if regime_counts else None

    summary_lines = [
        "# Recurring Matched Row Analysis",
        "",
        f"Top rules analyzed: {len(top_rules)}",
        f"Identity method: {identity_method}",
        f"Identity field/composite: {identity_descriptor}",
        f"Rows matched by at least one top rule: {total_rows}",
        f"Rows matched by 2+ top rules: {len(high_recurrence_rows)} ({(len(high_recurrence_rows)/total_rows):.1%} if total_rows else 0)",
        "",
        "## Recurrence Distribution",
        *recurrence_table,
        "",
        "## Diagnostics",
        f"- recurring ETH concentration (2+ matches): {len(eth_high)}/{len(high_recurrence_rows)}",
        f"- WATCH vs CANDIDATE recurrence (2+ matches): WATCH={len(watch_high)} CANDIDATE={len(candidate_high)}",
        "- recurring continuation states (2+ matches): " + ", ".join(f"{k}:{v}" for k, v in continuation_counts.most_common()) if continuation_counts else "- recurring continuation states (2+ matches): none",
        f"- high-recurrence TP2 vs STOPPED: TP2={len(high_rec_tp2)} STOPPED={len(high_rec_stopped)}",
        "",
        "## Top recurring TP2 rows",
        *(top_tp2 if top_tp2 else ["  - none"]),
        "",
        "## Top recurring STOPPED rows",
        *(top_stopped if top_stopped else ["  - none"]),
        "",
        "## Research Questions",
    ]

    repeatedly_rediscovered = len(high_recurrence_rows) > 0 and (len(high_recurrence_rows) / max(total_rows, 1)) <= 0.40
    narrow_regime = bool(top_regime and len(high_recurrence_rows) > 0 and (top_regime[1] / len(high_recurrence_rows)) >= 0.60)
    tp2_dominant = len(high_rec_tp2) >= len(high_rec_stopped)

    summary_lines.extend(
        [
            f"- Is there a small repeatedly rediscovered setup population? {'YES' if repeatedly_rediscovered else 'NO'}.",
            f"- Are profitable rows concentrated in a narrow structural regime? {'YES' if narrow_regime else 'NO'}.",
            f"- Are top rules rediscovering the same setups repeatedly? {'YES' if len(high_recurrence_rows) > 0 else 'NO'}.",
            f"- Are high-recurrence rows mostly TP2 or mostly STOPPED? {'TP2' if tp2_dominant else 'STOPPED'}.",
        ]
    )

    recurring_csv = output_path / "recurring_rows.csv"
    recurring_summary_md = output_path / "recurring_rows_summary.md"
    breakdown_csv = output_path / "recurring_row_feature_breakdown.csv"

    for path, rows_out in ((recurring_csv, recurring_rows), (breakdown_csv, breakdown_rows)):
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows_out[0].keys()) if rows_out else [])
            if rows_out:
                writer.writeheader()
                writer.writerows(rows_out)

    recurring_summary_md.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {
        "top_rules_analyzed": len(top_rules),
        "identity_method": identity_method,
        "identity_field_or_composite": identity_descriptor,
        "recurring_rows_csv": str(recurring_csv),
        "recurring_rows_summary_md": str(recurring_summary_md),
        "recurring_row_feature_breakdown_csv": str(breakdown_csv),
        "rule_match_sizes": rule_match_sizes,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only recurring matched-row analyzer across top rules.")
    parser.add_argument("--ranked-rules-csv", required=True)
    parser.add_argument("--top-rules-dir", required=True)
    parser.add_argument("--labeled-dataset-path", required=True)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--output-root", required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = analyze_recurring_rows(
        ranked_rules_csv=args.ranked_rules_csv,
        top_rules_dir=args.top_rules_dir,
        labeled_dataset_path=args.labeled_dataset_path,
        top_n=args.top_n,
        output_root=args.output_root,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
