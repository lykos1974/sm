from __future__ import annotations

import argparse
import csv
import json
import random
from pathlib import Path
from typing import Any, Sequence

from research_v2.common.manifests import DatasetArtifact, new_manifest, write_manifest
from research_v2.common.naming import manifest_name, utc_timestamp_label
from research_v2.optimizers.manual_filter_evaluator import (
    SUPPORTED_BOOLEAN_FIELDS,
    SUPPORTED_CATEGORICAL_FIELDS,
    SUPPORTED_INTEGER_FIELDS,
    SUPPORTED_NUMERIC_FIELDS,
    _load_input_datasets,
    _safe_float,
    _validate_dataset_columns,
    apply_rule,
    build_metrics,
    split_rows_time,
)

OPTIMIZER_MODE = "random_filter_scanner_v1"
SCHEMA_VERSION = "random_filter_scanner.v1"


def _extract_available_values(rows: Sequence[dict[str, Any]], field: str) -> list[Any]:
    vals = sorted({str(r.get(field)).upper() for r in rows if r.get(field) not in (None, "")})
    return vals


def _extract_numeric_values(rows: Sequence[dict[str, Any]], field: str) -> list[float]:
    vals = sorted({_safe_float(r.get(field)) for r in rows if _safe_float(r.get(field)) is not None})
    return [v for v in vals if v is not None]


def _rule_complexity(rule: dict[str, Any]) -> int:
    return sum(len(rule.get(k, {}) or {}) for k in ("categorical_filters", "boolean_filters", "integer_filters", "numeric_thresholds"))


def _build_seed_rules(force_side: str | None) -> list[dict[str, Any]]:
    seeds: list[dict[str, Any]] = [
        {"rule_id": "seed_candidate_only", "categorical_filters": {"status": {"mode": "include", "values": ["CANDIDATE"]}}},
        {
            "rule_id": "seed_baseline_like",
            "categorical_filters": {
                "status": {"mode": "include", "values": ["CANDIDATE"]},
                "breakout_context": {"mode": "include", "values": ["POST_BREAKOUT_PULLBACK"]},
                "pullback_quality": {"mode": "include", "values": ["HEALTHY"]},
            },
            "boolean_filters": {"is_extended_move": {"allowed": [0]}},
            "integer_filters": {"active_leg_boxes": {"allowed": [2]}},
        },
    ]
    if force_side:
        seeds.append({"rule_id": "seed_forced_side", "categorical_filters": {"side": {"mode": "include", "values": [force_side]}}})
    else:
        seeds.append({"rule_id": "seed_long_only", "categorical_filters": {"side": {"mode": "include", "values": ["LONG"]}}})
    seeds.append({"rule_id": "seed_any_status", "categorical_filters": {"status": {"mode": "exclude", "values": ["__NOOP__"]}}})
    return seeds


def _random_rule(randomizer: random.Random, rows: Sequence[dict[str, Any]], max_filters: int, force_side: str | None, idx: int) -> dict[str, Any]:
    rule: dict[str, Any] = {"rule_id": f"random_{idx:05d}"}
    categorical: dict[str, Any] = {}
    boolean: dict[str, Any] = {}
    integer: dict[str, Any] = {}
    numeric: dict[str, Any] = {}

    if force_side:
        categorical["side"] = {"mode": "include", "values": [force_side]}

    candidate_fields = list(SUPPORTED_CATEGORICAL_FIELDS | SUPPORTED_BOOLEAN_FIELDS | SUPPORTED_INTEGER_FIELDS | SUPPORTED_NUMERIC_FIELDS)
    randomizer.shuffle(candidate_fields)
    target = randomizer.randint(1, max_filters)
    for field in candidate_fields:
        if _rule_complexity({"categorical_filters": categorical, "boolean_filters": boolean, "integer_filters": integer, "numeric_thresholds": numeric}) >= target:
            break
        if field in SUPPORTED_CATEGORICAL_FIELDS:
            values = _extract_available_values(rows, field)
            if not values:
                continue
            pick = randomizer.sample(values, k=min(len(values), randomizer.randint(1, min(2, len(values)))))
            categorical[field] = {"mode": "include", "values": pick}
        elif field in SUPPORTED_BOOLEAN_FIELDS:
            available = sorted({int(float(r.get(field))) for r in rows if r.get(field) not in (None, "")})
            if not available:
                continue
            boolean[field] = {"allowed": [randomizer.choice(available)]}
        elif field in SUPPORTED_INTEGER_FIELDS:
            available = sorted({int(float(r.get(field))) for r in rows if r.get(field) not in (None, "")})
            if not available:
                continue
            integer[field] = {"allowed": [randomizer.choice(available)]}
        elif field in SUPPORTED_NUMERIC_FIELDS:
            values = _extract_numeric_values(rows, field)
            if len(values) < 2:
                continue
            lo = values[randomizer.randint(0, len(values) - 2)]
            hi = values[randomizer.randint(values.index(lo) + 1, len(values) - 1)]
            numeric[field] = {"min": lo, "max": hi}

    if categorical:
        rule["categorical_filters"] = categorical
    if boolean:
        rule["boolean_filters"] = boolean
    if integer:
        rule["integer_filters"] = integer
    if numeric:
        rule["numeric_thresholds"] = numeric
    return rule


def _flatten(rule: dict[str, Any], metrics_by_split: dict[str, dict[str, Any]], complexity: int) -> dict[str, Any]:
    row: dict[str, Any] = {"rule_id": rule["rule_id"], "rule_json": json.dumps(rule, sort_keys=True), "complexity": complexity, "promotion_eligible": False}
    for split in ("all", "train", "validation", "oos"):
        m = metrics_by_split[split]
        for col in ("matched_rows", "candidate_rows_registered", "resolved_rows", "avg_realized_r_multiple", "total_realized_r_multiple", "stop_rate", "tp2_rate"):
            row[f"{col}_{split}"] = m.get(col)
    row["tp1_to_tp2_conversion_validation"] = metrics_by_split["validation"].get("tp1_to_tp2_conversion_rate")
    row["tp1_to_tp2_conversion_oos"] = metrics_by_split["oos"].get("tp1_to_tp2_conversion_rate")
    val = metrics_by_split["validation"].get("avg_realized_r_multiple") or 0.0
    oos = metrics_by_split["oos"].get("avg_realized_r_multiple") or 0.0
    row["overfit_warning"] = bool(val > 0 and oos <= 0)
    return row


def scan_rules(**kwargs: Any) -> dict[str, Any]:
    paths = kwargs["input_labeled_dataset_paths"]
    datasets = _load_input_datasets(paths)
    rows = [r for d in datasets for r in d.rows]
    _validate_dataset_columns(rows, {"categorical_filters": {}})
    splits = split_rows_time(rows, kwargs["train_fraction"], kwargs["validation_fraction"], kwargs["oos_fraction"])
    split_map = {s.name: s.rows for s in splits}

    rnd = random.Random(kwargs["random_seed"])
    seeds = _build_seed_rules(kwargs.get("force_side"))
    rules = seeds + [_random_rule(rnd, rows, kwargs["max_complexity"], kwargs.get("force_side"), i) for i in range(max(0, kwargs["max_rules"] - len(seeds)))]

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for rule in rules:
        complexity = _rule_complexity(rule)
        by_split: dict[str, dict[str, Any]] = {}
        for name in ("all", "train", "validation", "oos"):
            matched = apply_rule(split_map[name], rule)
            by_split[name] = build_metrics(rule["rule_id"], name, len(split_map[name]), matched)

        flat = _flatten(rule, by_split, complexity)
        reasons: list[str] = []
        if complexity > kwargs["max_complexity"]:
            reasons.append("complexity_too_high")
        if (by_split["validation"].get("avg_realized_r_multiple") or 0) <= 0:
            reasons.append("validation_avg_r_non_positive")
        if (by_split["validation"].get("total_realized_r_multiple") or 0) <= 0:
            reasons.append("validation_total_r_non_positive")
        if by_split["validation"].get("resolved_rows", 0) < kwargs["min_validation_resolved"]:
            reasons.append("validation_resolved_too_low")
        if by_split["oos"].get("resolved_rows", 0) < kwargs["min_oos_resolved"]:
            reasons.append("oos_resolved_too_low")
        if (by_split["oos"].get("avg_realized_r_multiple") or 0) <= 0:
            reasons.append("oos_avg_r_non_positive")
        if (by_split["oos"].get("total_realized_r_multiple") or 0) <= 0:
            reasons.append("oos_total_r_non_positive")
        flat["reject_reasons"] = ";".join(reasons)
        (rejected if reasons else accepted).append(flat)

    accepted.sort(key=lambda r: (
        -(r.get("avg_realized_r_multiple_validation") or -999),
        -(r.get("avg_realized_r_multiple_oos") or -999),
        -(r.get("total_realized_r_multiple_all") or -999),
        -(r.get("tp2_rate_validation") or -999),
        (r.get("stop_rate_validation") or 999),
        r["complexity"],
        -(r.get("resolved_rows_validation") or 0),
    ))

    run_id = f"run_{utc_timestamp_label()}"
    run_root = Path(kwargs["output_root"]).resolve() / f"random_filter_scanner__{run_id}__v001"
    run_root.mkdir(parents=True, exist_ok=True)
    ranked_csv = run_root / "ranked_rules.csv"
    rejected_csv = run_root / "rejected_rules.csv"
    ranked_md = run_root / "ranked_rules.md"
    manifest_path = run_root / manifest_name(run_id)
    top_dir = run_root / "top_rules"
    top_dir.mkdir(exist_ok=True)

    fieldnames = sorted(set().union(*(r.keys() for r in (accepted + rejected)))) if (accepted or rejected) else ["rule_id"]
    for target_rows, p in ((accepted, ranked_csv), (rejected, rejected_csv)):
        with p.open("w", encoding="utf-8", newline="") as h:
            w = csv.DictWriter(h, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(target_rows)

    top_n = min(10, len(accepted))
    for i in range(top_n):
        payload = json.loads(accepted[i]["rule_json"])
        (top_dir / f"{i+1:02d}_{accepted[i]['rule_id']}.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    ranked_md.write_text(
        "# Random/Grid Filter Scanner\n\n"
        "Research-only. Advisory only. No live promotion.\n\n"
        f"Accepted rules: {len(accepted)} / {len(rules)}\n",
        encoding="utf-8",
    )
    manifest = new_manifest(run_id=run_id, source_context={"mode": OPTIMIZER_MODE, "schema_version": SCHEMA_VERSION, "read_only": True, "advisory_only": True})
    manifest.artifacts.extend([
        DatasetArtifact(stage="optimizer", artifact_type="scanner_ranked_csv", relative_path="ranked_rules.csv", row_count=len(accepted)),
        DatasetArtifact(stage="optimizer", artifact_type="scanner_rejected_csv", relative_path="rejected_rules.csv", row_count=len(rejected)),
        DatasetArtifact(stage="optimizer", artifact_type="scanner_ranked_md", relative_path="ranked_rules.md", row_count=1),
    ])
    write_manifest(manifest_path, manifest)

    return {"run_id": run_id, "output_root": str(run_root), "ranked_rules_csv": str(ranked_csv), "rejected_rules_csv": str(rejected_csv), "ranked_rules_md": str(ranked_md), "manifest": str(manifest_path), "accepted": len(accepted), "rejected": len(rejected)}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Research-only random/grid filter scanner over labeled setup datasets.")
    p.add_argument("--input-labeled-dataset-path", action="append", required=True)
    p.add_argument("--output-root", required=True)
    p.add_argument("--split-mode", default="time", choices=["time"])
    p.add_argument("--train-fraction", type=float, default=0.6)
    p.add_argument("--validation-fraction", type=float, default=0.2)
    p.add_argument("--oos-fraction", type=float, default=0.2)
    p.add_argument("--max-rules", type=int, default=5000)
    p.add_argument("--max-complexity", type=int, default=5)
    p.add_argument("--random-seed", type=int, default=1337)
    p.add_argument("--force-side", choices=["LONG", "SHORT"], default=None)
    p.add_argument("--min-validation-resolved", type=int, default=5)
    p.add_argument("--min-oos-resolved", type=int, default=5)
    return p


def main() -> None:
    args = build_parser().parse_args()
    result = scan_rules(
        input_labeled_dataset_paths=args.input_labeled_dataset_path,
        output_root=args.output_root,
        split_mode=args.split_mode,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
        oos_fraction=args.oos_fraction,
        max_rules=args.max_rules,
        random_seed=args.random_seed,
        force_side=args.force_side,
        min_validation_resolved=args.min_validation_resolved,
        min_oos_resolved=args.min_oos_resolved,
        max_complexity=args.max_complexity,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
