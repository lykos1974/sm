from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from research_v2.optimizers.manual_filter_evaluator import apply_rule

STRUCTURAL_FIELDS: tuple[str, ...] = (
    "side",
    "status",
    "breakout_context",
    "pullback_quality",
    "trend_regime",
    "continuation_execution_class",
    "active_leg_boxes",
    "entry_distance_bucket",
)


IDENTITY_COMPOSITE_FIELDS: tuple[str, ...] = (
    "symbol",
    "reference_ts",
    "side",
    "status",
    "strategy",
)


def _normalize(value: Any) -> str:
    return str(value or "").strip().upper()


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_top_rules(ranked_rules_csv: Path, top_rules_dir: Path, top_n: int) -> list[dict[str, Any]]:
    ranked_rows = _load_csv_rows(ranked_rules_csv)
    selected = ranked_rows[:top_n]
    rules: list[dict[str, Any]] = []
    for row in selected:
        rule_id = row.get("rule_id", "")
        candidates = sorted(top_rules_dir.glob(f"*_{rule_id}.json"))
        if not candidates and row.get("rule_json"):
            payload = json.loads(row["rule_json"])
        elif candidates:
            payload = json.loads(candidates[0].read_text(encoding="utf-8"))
        else:
            raise FileNotFoundError(f"No rule JSON found for top rule_id={rule_id}")
        rules.append({"rule_id": rule_id, "ranked_row": row, "rule": payload})
    return rules


def _validate_identity_uniqueness(identity_values: list[str], identity_descriptor: str) -> None:
    counts = Counter(identity_values)
    duplicates = [(value, cnt) for value, cnt in counts.items() if cnt > 1]
    if duplicates:
        sample = ", ".join(f"{value} (x{cnt})" for value, cnt in duplicates[:5])
        raise ValueError(
            f"Stable row identity '{identity_descriptor}' is not unique across dataset. "
            f"duplicate_count={len(duplicates)} sample_duplicates=[{sample}]"
        )


def _select_identity_method(rows: list[dict[str, Any]]) -> tuple[str, str, list[str]]:
    if not rows:
        raise ValueError("Input labeled dataset is empty; cannot perform overlap analysis.")

    def _values_for(field: str) -> list[str] | None:
        vals: list[str] = []
        for row in rows:
            value = row.get(field)
            if value in (None, ""):
                return None
            vals.append(f"{field}:{value}")
        return vals

    row_id_values = _values_for("row_id")
    if row_id_values is not None:
        _validate_identity_uniqueness(row_id_values, "row_id")
        return "field", "row_id", row_id_values

    setup_id_values = _values_for("setup_id")
    if setup_id_values is not None:
        _validate_identity_uniqueness(setup_id_values, "setup_id")
        return "field", "setup_id", setup_id_values

    available = set().union(*(row.keys() for row in rows))
    missing = [f for f in IDENTITY_COMPOSITE_FIELDS if f not in available]
    if missing:
        raise ValueError(
            "No stable identity available for overlap analysis. "
            "Expected unique 'row_id' or 'setup_id', or all composite fields present: "
            f"{', '.join(IDENTITY_COMPOSITE_FIELDS)}. Missing composite fields: {', '.join(missing)}"
        )

    composite_values: list[str] = []
    for row in rows:
        if any(row.get(field) in (None, "") for field in IDENTITY_COMPOSITE_FIELDS):
            raise ValueError(
                "No stable identity available for overlap analysis. Composite identity fields contain blank values: "
                + ", ".join(IDENTITY_COMPOSITE_FIELDS)
            )
        composite_values.append("|".join(f"{field}:{row[field]}" for field in IDENTITY_COMPOSITE_FIELDS))

    descriptor = "+".join(IDENTITY_COMPOSITE_FIELDS)
    _validate_identity_uniqueness(composite_values, descriptor)
    return "composite", descriptor, composite_values


def _jaccard(a: set[str], b: set[str]) -> float:
    union = a | b
    if not union:
        return 1.0
    return len(a & b) / len(union)


def _rule_structural_signature(rule: dict[str, Any]) -> str:
    cat = rule.get("categorical_filters", {}) or {}
    boolean = rule.get("boolean_filters", {}) or {}
    integer = rule.get("integer_filters", {}) or {}
    parts: list[str] = []
    for field in STRUCTURAL_FIELDS:
        if field in cat:
            vals = cat[field].get("values", [])
            parts.append(f"{field}={','.join(sorted(_normalize(v) for v in vals))}")
        elif field in integer:
            vals = integer[field].get("allowed", [])
            parts.append(f"{field}={','.join(sorted(str(v) for v in vals))}")
        elif field in boolean:
            vals = boolean[field].get("allowed", [])
            parts.append(f"{field}={','.join(sorted(str(v) for v in vals))}")
    return " | ".join(parts) if parts else "NO_STRUCTURAL_FILTERS"


def analyze_rule_overlap(*, ranked_rules_csv: str, top_rules_dir: str, labeled_dataset_path: str, top_n: int = 20, output_root: str) -> dict[str, Any]:
    ranked_path = Path(ranked_rules_csv).resolve()
    top_rules_path = Path(top_rules_dir).resolve()
    dataset_path = Path(labeled_dataset_path).resolve()
    output_path = Path(output_root).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    rows = _load_csv_rows(dataset_path)
    identity_method, identity_descriptor, row_ids = _select_identity_method(rows)
    row_identity_map: dict[int, str] = {id(row): row_id for row, row_id in zip(rows, row_ids)}
    top_rules = _load_top_rules(ranked_path, top_rules_path, top_n)

    rule_matches: dict[str, set[str]] = {}
    per_rule_summary: list[dict[str, Any]] = []
    symbol_counts_per_rule: dict[str, Counter[str]] = {}

    for entry in top_rules:
        rule_id = entry["rule_id"]
        matched_rows = apply_rule(rows, entry["rule"])
        matched_ids = {row_identity_map[id(row)] for row in matched_rows}
        if len(matched_ids) != len(matched_rows):
            raise ValueError(
                f"Identity collision detected while mapping matched rows for rule {rule_id}. "
                f"matched_rows={len(matched_rows)} unique_matched_ids={len(matched_ids)}"
            )
        rule_matches[rule_id] = matched_ids
        symbols = Counter(_normalize(r.get("symbol", "")) for r in matched_rows if _normalize(r.get("symbol", "")))
        symbol_counts_per_rule[rule_id] = symbols

        cat = entry["rule"].get("categorical_filters", {}) or {}
        num = entry["rule"].get("numeric_thresholds", {}) or {}
        integer = entry["rule"].get("integer_filters", {}) or {}
        quality_spec = num.get("quality_score", {}) if isinstance(num.get("quality_score"), dict) else {}
        per_rule_summary.append(
            {
                "rule_id": rule_id,
                "matched_rows": len(matched_ids),
                "signature": _rule_structural_signature(entry["rule"]),
                "symbol_top": symbols.most_common(1)[0][0] if symbols else "",
                "symbol_top_share": (symbols.most_common(1)[0][1] / len(matched_rows)) if (symbols and matched_rows) else 0.0,
                "status": ",".join(cat.get("status", {}).get("values", [])),
                "side": ",".join(cat.get("side", {}).get("values", [])),
                "breakout_context": ",".join(cat.get("breakout_context", {}).get("values", [])),
                "pullback_quality": ",".join(cat.get("pullback_quality", {}).get("values", [])),
                "trend_regime": ",".join(cat.get("trend_regime", {}).get("values", [])),
                "continuation_execution_class": ",".join(cat.get("continuation_execution_class", {}).get("values", [])),
                "entry_distance_bucket": ",".join(cat.get("entry_distance_bucket", {}).get("values", [])),
                "active_leg_boxes": ",".join(str(v) for v in integer.get("active_leg_boxes", {}).get("allowed", [])),
                "quality_score_min": quality_spec.get("min", ""),
                "quality_score_max": quality_spec.get("max", ""),
            }
        )

    overlap_rows: list[dict[str, Any]] = []
    rule_ids = [r["rule_id"] for r in top_rules]
    high_overlap_pairs = 0
    total_pairs = 0
    for left in rule_ids:
        for right in rule_ids:
            inter = len(rule_matches[left] & rule_matches[right])
            union = len(rule_matches[left] | rule_matches[right])
            jac = _jaccard(rule_matches[left], rule_matches[right])
            if left < right:
                total_pairs += 1
                if jac >= 0.7:
                    high_overlap_pairs += 1
            overlap_rows.append({"rule_id_left": left, "rule_id_right": right, "intersection": inter, "union": union, "jaccard": f"{jac:.6f}"})

    signature_groups: dict[str, list[str]] = defaultdict(list)
    for row in per_rule_summary:
        signature_groups[row["signature"]].append(row["rule_id"])

    clustered_rows: list[dict[str, Any]] = []
    for idx, (sig, ids) in enumerate(sorted(signature_groups.items(), key=lambda kv: (-len(kv[1]), kv[0])), start=1):
        for rule_id in ids:
            clustered_rows.append({"cluster_id": f"C{idx:02d}", "signature": sig, "rule_id": rule_id, "cluster_size": len(ids)})

    warnings: list[str] = []
    global_symbols = Counter()
    for counts in symbol_counts_per_rule.values():
        global_symbols.update(counts)
    if global_symbols:
        dominant_symbol, dominant_count = global_symbols.most_common(1)[0]
        total = sum(global_symbols.values())
        if total > 0 and dominant_count / total >= 0.7:
            warnings.append(f"one-symbol dominance: {dominant_symbol} share={dominant_count/total:.1%}")

    watch_rules = sum(1 for r in per_rule_summary if "WATCH" in _normalize(r.get("status", "")))
    if per_rule_summary and watch_rules / len(per_rule_summary) >= 0.7:
        warnings.append(f"WATCH-only dominance risk: {watch_rules}/{len(per_rule_summary)} top rules filter WATCH")

    if total_pairs > 0 and high_overlap_pairs / total_pairs >= 0.6:
        warnings.append("low diversity: majority of top-rule pairs have Jaccard >= 0.70")

    tiny_rules = [r["rule_id"] for r in per_rule_summary if int(r["matched_rows"]) < 10]
    if tiny_rules:
        warnings.append("tiny sample-size rules: " + ", ".join(tiny_rules))

    sides = {_normalize(r.get("side", "")) for r in per_rule_summary if r.get("side")}
    if len(sides) > 1:
        warnings.append("contradictory archetypes: mixed side filters across top rules")

    narrative = [
        "# Archetype Overlap Summary",
        "",
        f"Top rules analyzed: {len(rule_ids)}.",
        f"Identity method: {identity_method}.",
        f"Identity field/composite: {identity_descriptor}.",
        f"Matched row identity count (dataset): {len(row_ids)}.",
        "Clustering note: clustered_rule_groups.csv is based on textual structural signatures, not matched-row overlap.",
    ]
    if total_pairs > 0:
        overlap_ratio = high_overlap_pairs / total_pairs
        if overlap_ratio >= 0.6:
            narrative.append(f"Top rules heavily overlap ({overlap_ratio:.1%} of pairs above 0.70 Jaccard), suggesting a shared archetype.")
        else:
            narrative.append(f"Top rules have limited overlap ({overlap_ratio:.1%} of pairs above 0.70 Jaccard), suggesting multiple unrelated slices.")

    largest_cluster = max((len(v) for v in signature_groups.values()), default=0)
    narrative.append(f"Largest structural-signature cluster size: {largest_cluster}.")

    if global_symbols:
        sym, cnt = global_symbols.most_common(1)[0]
        narrative.append(f"Symbol concentration leader: {sym} ({cnt/sum(global_symbols.values()):.1%} of matched rows across top rules).")

    if warnings:
        narrative.extend(["", "## Warnings"] + [f"- {w}" for w in warnings])

    overlap_csv = output_path / "overlap_matrix.csv"
    summary_csv = output_path / "rule_feature_summary.csv"
    clusters_csv = output_path / "clustered_rule_groups.csv"
    archetype_md = output_path / "archetype_summary.md"

    for path, rows_out in (
        (overlap_csv, overlap_rows),
        (summary_csv, per_rule_summary),
        (clusters_csv, clustered_rows),
    ):
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows_out[0].keys()) if rows_out else [])
            if rows_out:
                writer.writeheader()
                writer.writerows(rows_out)

    archetype_md.write_text("\n".join(narrative) + "\n", encoding="utf-8")

    return {
        "top_rules_analyzed": len(rule_ids),
        "identity_method": identity_method,
        "identity_field_or_composite": identity_descriptor,
        "matched_row_identity_count": len(row_ids),
        "overlap_matrix_csv": str(overlap_csv),
        "rule_feature_summary_csv": str(summary_csv),
        "clustered_rule_groups_csv": str(clusters_csv),
        "archetype_summary_md": str(archetype_md),
        "warnings": warnings,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only overlap analyzer for random scanner top rules.")
    parser.add_argument("--ranked-rules-csv", required=True)
    parser.add_argument("--top-rules-dir", required=True)
    parser.add_argument("--labeled-dataset-path", required=True)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--output-root", required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = analyze_rule_overlap(
        ranked_rules_csv=args.ranked_rules_csv,
        top_rules_dir=args.top_rules_dir,
        labeled_dataset_path=args.labeled_dataset_path,
        top_n=args.top_n,
        output_root=args.output_root,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
