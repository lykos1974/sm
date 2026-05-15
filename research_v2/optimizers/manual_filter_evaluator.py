from __future__ import annotations

import argparse
import csv
import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.analytics.io import read_labeled_dataset
from research_v2.common.manifests import DatasetArtifact, new_manifest, write_manifest
from research_v2.common.naming import manifest_name, utc_timestamp_label

OPTIMIZER_MODE = "manual_filter_rule_evaluator_v1"
SCHEMA_VERSION = "manual_filter_rule_evaluator.v1"

SUPPORTED_CATEGORICAL_FIELDS: frozenset[str] = frozenset(
    {
        "side",
        "status",
        "breakout_context",
        "pullback_quality",
        "trend_regime",
        "entry_distance_bucket",
        "continuation_execution_class",
        "pattern_family",
    }
)
SUPPORTED_BOOLEAN_FIELDS: frozenset[str] = frozenset({"is_extended_move", "is_baseline_profile_match"})
SUPPORTED_INTEGER_FIELDS: frozenset[str] = frozenset({"active_leg_boxes"})
SUPPORTED_NUMERIC_FIELDS: frozenset[str] = frozenset(
    {
        "quality_score",
        "entry_distance_boxes",
        "continuation_quality_score",
        "extension_penalty",
    }
)

REQUIRED_DATASET_COLUMNS: frozenset[str] = frozenset(
    {"reference_ts", "status", "resolution_status", "realized_r_multiple"}
)

RES_STOPPED = "STOPPED"
RES_TP1_ONLY = "TP1_ONLY"
RES_TP1_BE = "TP1_THEN_BE"
RES_TP2 = "TP2"
RES_AMBIGUOUS = "AMBIGUOUS"
RES_PENDING_VALUES = {"", "PENDING", "UNRESOLVED", "NEVER_ACTIVATED", "NONE"}
RES_NON_RESOLVED_VALUES = {"PENDING", "UNRESOLVED", "NEVER_ACTIVATED"}
TP1_TOUCH_RESOLUTIONS = {RES_TP1_ONLY, RES_TP1_BE, RES_TP2}

SUMMARY_COLUMNS: tuple[str, ...] = (
    "rule_id",
    "split",
    "total_rows",
    "matched_rows",
    "candidate_rows_registered",
    "resolved_rows",
    "win_rate_non_ambiguous",
    "avg_realized_r_multiple",
    "total_realized_r_multiple",
    "tp1_to_tp2_conversion_rate",
    "stop_rate",
    "tp2_rate",
    "pending_count",
    "ambiguous_count",
)


@dataclass(frozen=True)
class LoadedDataset:
    path: Path
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class SplitRows:
    name: str
    rows: list[dict[str, Any]]


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, "", "None"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _normalize_text(value: Any) -> str:
    return str(value or "").upper()


def _resolution(row: dict[str, Any]) -> str:
    return _normalize_text(row.get("resolution_status"))


def _is_resolved(row: dict[str, Any]) -> bool:
    status = _resolution(row)
    return bool(status) and status not in RES_NON_RESOLVED_VALUES


def _is_non_ambiguous_resolved(row: dict[str, Any]) -> bool:
    return _is_resolved(row) and _resolution(row) != RES_AMBIGUOUS


def _is_pending(row: dict[str, Any]) -> bool:
    return _resolution(row) in RES_PENDING_VALUES


def _numeric_series(rows: Iterable[dict[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _safe_float(row.get(field))
        if value is not None:
            values.append(value)
    return values


def _load_rule(rule_json_path: Path) -> dict[str, Any]:
    if not rule_json_path.exists():
        raise FileNotFoundError(f"Rule JSON not found: {rule_json_path}")
    with rule_json_path.open("r", encoding="utf-8") as handle:
        rule = json.load(handle)
    if not isinstance(rule, dict):
        raise ValueError("Rule JSON must contain a JSON object.")
    return rule


def _rule_section(rule: dict[str, Any], canonical_name: str) -> dict[str, Any]:
    aliases = {
        "categorical": ("categorical_filters", "categorical"),
        "boolean": ("boolean_filters", "boolean"),
        "numeric": ("numeric_thresholds", "numeric"),
        "integer": ("integer_filters", "integer"),
    }[canonical_name]
    merged: dict[str, Any] = {}
    for alias in aliases:
        section = rule.get(alias, {})
        if section is None:
            continue
        if not isinstance(section, dict):
            raise ValueError(f"Rule section '{alias}' must be a JSON object.")
        merged.update(section)
    return merged


def _referenced_rule_fields(rule: dict[str, Any]) -> set[str]:
    fields: set[str] = set()
    for section_name in ("categorical", "boolean", "numeric", "integer"):
        fields.update(_rule_section(rule, section_name).keys())
    return fields


def _validate_supported_fields(rule: dict[str, Any]) -> None:
    unsupported: list[str] = []
    for field in _rule_section(rule, "categorical"):
        if field not in SUPPORTED_CATEGORICAL_FIELDS:
            unsupported.append(f"categorical:{field}")
    for field in _rule_section(rule, "boolean"):
        if field not in SUPPORTED_BOOLEAN_FIELDS:
            unsupported.append(f"boolean:{field}")
    for field in _rule_section(rule, "numeric"):
        if field not in SUPPORTED_NUMERIC_FIELDS:
            unsupported.append(f"numeric:{field}")
    for field in _rule_section(rule, "integer"):
        if field not in SUPPORTED_INTEGER_FIELDS:
            unsupported.append(f"integer:{field}")
    if unsupported:
        raise ValueError(
            "Unsupported rule field(s): "
            + ", ".join(sorted(unsupported))
            + ". Phase 1 only supports existing frozen-label/setup descriptor fields."
        )


def _validate_dataset_columns(rows: Sequence[dict[str, Any]], rule: dict[str, Any]) -> None:
    if not rows:
        raise ValueError("Input labeled dataset is empty; cannot evaluate a rule.")

    available = set().union(*(row.keys() for row in rows))
    missing_required = sorted(REQUIRED_DATASET_COLUMNS - available)
    if "realized_r_multiple" in missing_required:
        raise ValueError(
            "Input dataset is missing required column 'realized_r_multiple'; "
            "a labeled/analytics export is required before running the optimizer."
        )
    if missing_required:
        raise ValueError("Input dataset is missing required column(s): " + ", ".join(missing_required))

    referenced = _referenced_rule_fields(rule)
    missing_referenced = sorted(referenced - available)
    if missing_referenced:
        raise ValueError(
            "Rule references column(s) missing from the labeled dataset: "
            + ", ".join(missing_referenced)
        )


def _categorical_passes(row: dict[str, Any], field: str, spec: Any) -> bool:
    if not isinstance(spec, dict):
        raise ValueError(f"Categorical filter for '{field}' must be an object.")
    mode = str(spec.get("mode", "include")).lower()
    if mode in {"disabled", "off"}:
        return True
    if mode not in {"include", "exclude"}:
        raise ValueError(f"Categorical filter for '{field}' has unsupported mode: {mode}")
    values = spec.get("values")
    if not isinstance(values, list) or not values:
        raise ValueError(f"Categorical filter for '{field}' requires a non-empty 'values' list.")
    normalized_values = {_normalize_text(value) for value in values}
    present = _normalize_text(row.get(field)) in normalized_values
    return present if mode == "include" else not present


def _boolean_passes(row: dict[str, Any], field: str, spec: Any) -> bool:
    if not isinstance(spec, dict):
        raise ValueError(f"Boolean filter for '{field}' must be an object.")
    allowed = spec.get("allowed")
    if not isinstance(allowed, list) or not allowed:
        raise ValueError(f"Boolean filter for '{field}' requires a non-empty 'allowed' list.")
    row_value = _safe_int(row.get(field))
    allowed_values = {_safe_int(value) for value in allowed}
    return row_value is not None and row_value in allowed_values


def _numeric_passes(row: dict[str, Any], field: str, spec: Any) -> bool:
    if not isinstance(spec, dict):
        raise ValueError(f"Numeric threshold for '{field}' must be an object.")
    value = _safe_float(row.get(field))
    if value is None:
        return False
    min_value = spec.get("min")
    max_value = spec.get("max")
    if min_value is not None and value < float(min_value):
        return False
    if max_value is not None and value > float(max_value):
        return False
    return True


def _integer_passes(row: dict[str, Any], field: str, spec: Any) -> bool:
    if not isinstance(spec, dict):
        raise ValueError(f"Integer filter for '{field}' must be an object.")
    value = _safe_int(row.get(field))
    if value is None:
        return False
    allowed = spec.get("allowed") or spec.get("allowed_values")
    if allowed is not None:
        if not isinstance(allowed, list) or not allowed:
            raise ValueError(f"Integer filter for '{field}' allowed values must be a non-empty list.")
        return value in {_safe_int(v) for v in allowed}
    min_value = spec.get("min")
    max_value = spec.get("max")
    if min_value is not None and value < int(min_value):
        return False
    if max_value is not None and value > int(max_value):
        return False
    return True


def row_matches_rule(row: dict[str, Any], rule: dict[str, Any]) -> bool:
    for field, spec in _rule_section(rule, "categorical").items():
        if not _categorical_passes(row, field, spec):
            return False
    for field, spec in _rule_section(rule, "boolean").items():
        if not _boolean_passes(row, field, spec):
            return False
    for field, spec in _rule_section(rule, "numeric").items():
        if not _numeric_passes(row, field, spec):
            return False
    for field, spec in _rule_section(rule, "integer").items():
        if not _integer_passes(row, field, spec):
            return False
    return True


def apply_rule(rows: Sequence[dict[str, Any]], rule: dict[str, Any]) -> list[dict[str, Any]]:
    return [row for row in rows if row_matches_rule(row, rule)]


def split_rows_time(
    rows: Sequence[dict[str, Any]],
    train_fraction: float,
    validation_fraction: float,
    oos_fraction: float,
) -> list[SplitRows]:
    total_fraction = train_fraction + validation_fraction + oos_fraction
    if abs(total_fraction - 1.0) > 1e-9:
        raise ValueError(
            "Split fractions must sum to 1.0; got "
            f"train={train_fraction}, validation={validation_fraction}, oos={oos_fraction}."
        )
    for name, fraction in (
        ("train", train_fraction),
        ("validation", validation_fraction),
        ("oos", oos_fraction),
    ):
        if fraction < 0:
            raise ValueError(f"{name} fraction must be non-negative.")

    sorted_rows = sorted(
        rows,
        key=lambda row: (
            _safe_float(row.get("reference_ts")) is None,
            _safe_float(row.get("reference_ts")) or 0.0,
        ),
    )
    row_count = len(sorted_rows)
    train_end = int(row_count * train_fraction)
    validation_end = train_end + int(row_count * validation_fraction)
    return [
        SplitRows(name="all", rows=list(sorted_rows)),
        SplitRows(name="train", rows=list(sorted_rows[:train_end])),
        SplitRows(name="validation", rows=list(sorted_rows[train_end:validation_end])),
        SplitRows(name="oos", rows=list(sorted_rows[validation_end:])),
    ]


def build_metrics(
    rule_id: str,
    split_name: str,
    total_rows: int,
    matched_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    matched = list(matched_rows)
    matched_count = len(matched)
    candidate_rows_registered = sum(
        1 for row in matched if _normalize_text(row.get("status")) == "CANDIDATE"
    )
    resolved_rows = sum(1 for row in matched if _is_resolved(row))
    non_ambiguous_rows = [row for row in matched if _is_non_ambiguous_resolved(row)]
    non_ambiguous_wins = sum(
        1 for row in non_ambiguous_rows if (_safe_float(row.get("realized_r_multiple")) or 0.0) > 0.0
    )
    realized_values = _numeric_series(matched, "realized_r_multiple")
    stopped_count = sum(1 for row in matched if _resolution(row) == RES_STOPPED)
    tp2_count = sum(1 for row in matched if _resolution(row) == RES_TP2)
    tp1_touch_count = sum(1 for row in matched if _resolution(row) in TP1_TOUCH_RESOLUTIONS)
    pending_count = sum(1 for row in matched if _is_pending(row))
    ambiguous_count = sum(1 for row in matched if _resolution(row) == RES_AMBIGUOUS)

    return {
        "rule_id": rule_id,
        "split": split_name,
        "total_rows": total_rows,
        "matched_rows": matched_count,
        "candidate_rows_registered": candidate_rows_registered,
        "resolved_rows": resolved_rows,
        "win_rate_non_ambiguous": _rate(non_ambiguous_wins, len(non_ambiguous_rows)),
        "avg_realized_r_multiple": statistics.mean(realized_values) if realized_values else None,
        "total_realized_r_multiple": sum(realized_values) if realized_values else None,
        "tp1_to_tp2_conversion_rate": _rate(tp2_count, tp1_touch_count),
        "stop_rate": _rate(stopped_count, matched_count),
        "tp2_rate": _rate(tp2_count, matched_count),
        "pending_count": pending_count,
        "ambiguous_count": ambiguous_count,
    }


def write_csv(rows: Iterable[dict[str, Any]], output_path: Path, fieldnames: Sequence[str]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(payload: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _format_metric(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def write_markdown_summary(
    output_path: Path,
    *,
    run_id: str,
    rule_id: str,
    input_paths: Sequence[Path],
    rule: dict[str, Any],
    metrics: Sequence[dict[str, Any]],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Manual Filter Rule Evaluation",
        "",
        "Research-only advisory output. This tool evaluates frozen labeled setup datasets only; it does not modify strategy logic, live traders, PnF engine, pattern detection, database schema, or promote rules automatically.",
        "",
        f"- Run ID: `{run_id}`",
        f"- Rule ID: `{rule_id}`",
        "- Optimizer mode: `manual_filter_rule_evaluator_v1`",
        "",
        "## Inputs",
        "",
    ]
    for path in input_paths:
        lines.append(f"- `{path}`")
    lines.extend(
        [
            "",
            "## Rule JSON",
            "",
            "```json",
            json.dumps(rule, indent=2, sort_keys=True),
            "```",
            "",
            "## Metrics",
            "",
            "| Split | Total Rows | Matched Rows | Candidates | Resolved | Win Rate | Avg R "
            "| Total R | TP1 -> TP2 | Stop Rate | TP2 Rate | Pending | Ambiguous |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in metrics:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["split"]),
                    _format_metric(row["total_rows"]),
                    _format_metric(row["matched_rows"]),
                    _format_metric(row["candidate_rows_registered"]),
                    _format_metric(row["resolved_rows"]),
                    _format_metric(row["win_rate_non_ambiguous"]),
                    _format_metric(row["avg_realized_r_multiple"]),
                    _format_metric(row["total_realized_r_multiple"]),
                    _format_metric(row["tp1_to_tp2_conversion_rate"]),
                    _format_metric(row["stop_rate"]),
                    _format_metric(row["tp2_rate"]),
                    _format_metric(row["pending_count"]),
                    _format_metric(row["ambiguous_count"]),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Safety Notes",
            "",
            "- Advisory research output only.",
            "- No live execution.",
            "- No strategy patching.",
            "- No automatic promotion.",
        ]
    )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _relative_to_root(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _load_input_datasets(paths: Sequence[str]) -> list[LoadedDataset]:
    if not paths:
        raise ValueError("At least one --input-labeled-dataset-path is required.")
    datasets: list[LoadedDataset] = []
    for raw_path in paths:
        path = Path(raw_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Input labeled dataset not found: {path}")
        rows = read_labeled_dataset(path)
        for index, row in enumerate(rows):
            row.setdefault("source_dataset_path", str(path))
            row.setdefault("source_dataset_row", index)
        datasets.append(LoadedDataset(path=path, rows=rows))
    return datasets


def evaluate_rule(
    *,
    input_labeled_dataset_paths: Sequence[str],
    rule_json_path: str,
    output_root: str | None,
    split_mode: str,
    train_fraction: float,
    validation_fraction: float,
    oos_fraction: float,
    write_matched_rows: bool,
    dry_run: bool = False,
) -> dict[str, Any]:
    if split_mode != "time":
        raise ValueError("Phase 1 supports only --split-mode time.")

    rule_path = Path(rule_json_path).resolve()
    rule = _load_rule(rule_path)
    _validate_supported_fields(rule)

    datasets = _load_input_datasets(input_labeled_dataset_paths)
    combined_rows = [row for dataset in datasets for row in dataset.rows]
    _validate_dataset_columns(combined_rows, rule)

    splits = split_rows_time(combined_rows, train_fraction, validation_fraction, oos_fraction)
    rule_id = str(rule.get("rule_id") or rule.get("name") or rule_path.stem)
    metrics: list[dict[str, Any]] = []
    matched_by_split: dict[str, list[dict[str, Any]]] = {}
    for split in splits:
        matched = apply_rule(split.rows, rule)
        matched_by_split[split.name] = matched
        metrics.append(build_metrics(rule_id, split.name, len(split.rows), matched))

    run_id = f"run_{utc_timestamp_label()}"
    output_base = Path(output_root or "data/research/optimizers").resolve()
    run_root = output_base / f"manual_filter_evaluator__{run_id}__v001"
    summary_csv_path = run_root / "evaluated_rule.csv"
    summary_json_path = run_root / "evaluated_rule.json"
    summary_md_path = run_root / "summary.md"
    matched_rows_path = run_root / "matched_rows.csv"
    manifest_path = run_root / manifest_name(run_id)

    result: dict[str, Any] = {
        "run_id": run_id,
        "mode": OPTIMIZER_MODE,
        "schema_version": SCHEMA_VERSION,
        "rule_id": rule_id,
        "input_labeled_dataset_paths": [str(dataset.path) for dataset in datasets],
        "rule_json_path": str(rule_path),
        "output_root": str(run_root),
        "evaluated_rule_csv_path": str(summary_csv_path),
        "evaluated_rule_json_path": str(summary_json_path),
        "summary_markdown_path": str(summary_md_path),
        "manifest_path": str(manifest_path),
        "matched_rows_csv_path": str(matched_rows_path) if write_matched_rows else None,
        "dry_run": dry_run,
        "metrics": metrics,
    }

    if not dry_run:
        write_csv(metrics, summary_csv_path, SUMMARY_COLUMNS)
        write_json({"rule": rule, "metrics": metrics, "run": result}, summary_json_path)
        write_markdown_summary(
            summary_md_path,
            run_id=run_id,
            rule_id=rule_id,
            input_paths=[dataset.path for dataset in datasets],
            rule=rule,
            metrics=metrics,
        )
        if write_matched_rows:
            matched_all = matched_by_split["all"]
            if matched_all:
                fieldnames = sorted(set().union(*(row.keys() for row in matched_all)))
            else:
                fieldnames = sorted(set().union(*(row.keys() for row in combined_rows)))
            write_csv(matched_all, matched_rows_path, fieldnames)

        manifest = new_manifest(
            run_id=run_id,
            source_context={
                "mode": OPTIMIZER_MODE,
                "schema_version": SCHEMA_VERSION,
                "input_labeled_dataset_paths": [str(dataset.path) for dataset in datasets],
                "rule_json_path": str(rule_path),
                "split_mode": split_mode,
                "train_fraction": train_fraction,
                "validation_fraction": validation_fraction,
                "oos_fraction": oos_fraction,
                "read_only": True,
                "advisory_only": True,
                "auto_promote": False,
            },
        )
        manifest.artifacts.extend(
            [
                DatasetArtifact(
                    stage="optimizer",
                    artifact_type="manual_filter_evaluation_csv",
                    relative_path=_relative_to_root(summary_csv_path, run_root),
                    row_count=len(metrics),
                    notes="single_rule_split_metrics",
                ),
                DatasetArtifact(
                    stage="optimizer",
                    artifact_type="manual_filter_evaluation_json",
                    relative_path=_relative_to_root(summary_json_path, run_root),
                    row_count=1,
                    notes="single_rule_payload",
                ),
                DatasetArtifact(
                    stage="optimizer",
                    artifact_type="manual_filter_evaluation_markdown",
                    relative_path=_relative_to_root(summary_md_path, run_root),
                    row_count=1,
                    notes="human_readable_advisory_summary",
                ),
            ]
        )
        if write_matched_rows:
            manifest.artifacts.append(
                DatasetArtifact(
                    stage="optimizer",
                    artifact_type="manual_filter_matched_rows_csv",
                    relative_path=_relative_to_root(matched_rows_path, run_root),
                    row_count=len(matched_by_split["all"]),
                    notes="optional_all_split_matched_rows",
                )
            )
        write_manifest(manifest_path, manifest)

    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only manual filter evaluator for frozen labeled PnF setup datasets."
    )
    parser.add_argument(
        "--input-labeled-dataset-path",
        action="append",
        required=True,
        help="Path to frozen labeled dataset (.csv or .parquet). Repeatable.",
    )
    parser.add_argument("--rule-json", required=True, help="Path to manual filter rule JSON.")
    parser.add_argument(
        "--output-root",
        default="data/research/optimizers",
        help="Output root for advisory artifacts.",
    )
    parser.add_argument(
        "--split-mode",
        default="time",
        choices=["time"],
        help="Split mode. Phase 1 supports time only.",
    )
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--oos-fraction", type=float, default=0.20)
    parser.add_argument(
        "--format",
        dest="formats",
        action="append",
        choices=["csv", "json", "md"],
        default=None,
        help="Accepted for CLI compatibility; Phase 1 writes CSV, JSON, Markdown, and manifest artifacts.",
    )
    parser.add_argument("--write-matched-rows", action="store_true", help="Also write all-split matched rows CSV.")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate without writing artifacts.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = evaluate_rule(
        input_labeled_dataset_paths=args.input_labeled_dataset_path,
        rule_json_path=args.rule_json,
        output_root=args.output_root,
        split_mode=args.split_mode,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
        oos_fraction=args.oos_fraction,
        write_matched_rows=args.write_matched_rows,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
