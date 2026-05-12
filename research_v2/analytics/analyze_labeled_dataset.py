from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

from research_v2.analytics.io import read_labeled_dataset, write_report_json, write_summary_csv
from research_v2.analytics.schema import ANALYSIS_MODE, DEFAULT_GROUP_BY, SCHEMA_VERSION
from research_v2.common.manifests import DatasetArtifact, new_manifest, write_manifest
from research_v2.common.naming import manifest_name, utc_timestamp_label, versioned_dataset_name
from research_v2.common.paths import ResearchPaths, ensure_research_directories, resolve_research_paths

LABEL_LABELED = "LABELED"
LABEL_INVALID = "INVALID"

ACTIVATED = "ACTIVATED"
NEVER_ACTIVATED = "NEVER_ACTIVATED"

RES_STOPPED = "STOPPED"
RES_TP1_ONLY = "TP1_ONLY"
RES_TP1_BE = "TP1_THEN_BE"
RES_TP2 = "TP2"
RES_AMBIGUOUS = "AMBIGUOUS"
RES_EXPIRED = "EXPIRED"

TP1_TOUCH_RESOLUTIONS = {RES_TP1_ONLY, RES_TP1_BE, RES_TP2}


def _resolve_paths(output_root: str | None) -> ResearchPaths:
    if output_root:
        root = Path(output_root).resolve()
        return ResearchPaths(
            repo_root=root,
            data_root=root,
            setups_root=root / "setups",
            labels_root=root / "labels",
            analysis_root=root / "analysis",
            manifests_root=root / "manifests",
        )
    return resolve_research_paths()


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _group_key(row: dict[str, Any], group_by: list[str]) -> tuple[Any, ...]:
    return tuple(row.get(k) for k in group_by)


def _count_resolution(rows: list[dict[str, Any]], resolution: str) -> int:
    return sum(1 for row in rows if str(row.get("resolution_status") or "").upper() == resolution)


def _count_activation(rows: list[dict[str, Any]], activation: str) -> int:
    return sum(1 for row in rows if str(row.get("activation_status") or "").upper() == activation)


def _count_label(rows: list[dict[str, Any]], label_status: str) -> int:
    return sum(1 for row in rows if str(row.get("label_status") or "").upper() == label_status)


def _count_candidate_rows(rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in rows if str(row.get("status") or "").upper() == "CANDIDATE")


def _is_resolved(row: dict[str, Any]) -> bool:
    status = str(row.get("resolution_status") or "").upper()
    return bool(status) and status not in {"PENDING", "UNRESOLVED", "NEVER_ACTIVATED"}


def _is_non_ambiguous_resolved(row: dict[str, Any]) -> bool:
    status = str(row.get("resolution_status") or "").upper()
    return _is_resolved(row) and status != RES_AMBIGUOUS


def _count_resolved(rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in rows if _is_resolved(row))


def _count_non_ambiguous_wins(rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if _is_non_ambiguous_resolved(row) and (_safe_float(row.get("realized_r_multiple")) or 0.0) > 0.0
    )


def _numeric_series(rows: list[dict[str, Any]], field: str) -> list[float]:
    return [
        value
        for value in (_safe_float(row.get(field)) for row in rows)
        if value is not None
    ]


def _build_outcome_scorecard(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_rows = len(rows)
    valid_rows = _count_label(rows, LABEL_LABELED)
    invalid_rows = _count_label(rows, LABEL_INVALID)

    activated_count = _count_activation(rows, ACTIVATED)
    never_activated_count = _count_activation(rows, NEVER_ACTIVATED)

    stopped_count = _count_resolution(rows, RES_STOPPED)
    tp1_only_count = _count_resolution(rows, RES_TP1_ONLY)
    tp1_then_be_count = _count_resolution(rows, RES_TP1_BE)
    tp2_count = _count_resolution(rows, RES_TP2)
    ambiguous_count = _count_resolution(rows, RES_AMBIGUOUS)
    expired_count = _count_resolution(rows, RES_EXPIRED)
    candidate_rows_registered = _count_candidate_rows(rows)
    resolved_rows = _count_resolved(rows)
    non_ambiguous_rows = [row for row in rows if _is_non_ambiguous_resolved(row)]
    non_ambiguous_win_count = _count_non_ambiguous_wins(rows)

    tp1_touch_count = sum(
        1
        for row in rows
        if str(row.get("resolution_status") or "").upper() in TP1_TOUCH_RESOLUTIONS
    )

    realized_values = _numeric_series(rows, "realized_r_multiple")
    outcome_proxy_values = _numeric_series(rows, "outcome_r_proxy")

    return {
        "analysis_mode": ANALYSIS_MODE,
        "schema_version": SCHEMA_VERSION,
        "total_rows": total_rows,
        "valid_labeled_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "activation_rate": _rate(activated_count, total_rows),
        "never_activated_count": never_activated_count,
        "never_activated_rate": _rate(never_activated_count, total_rows),
        "activated_count": activated_count,
        "activated_rate": _rate(activated_count, total_rows),
        "stopped_count": stopped_count,
        "stopped_rate": _rate(stopped_count, total_rows),
        "tp1_only_count": tp1_only_count,
        "tp1_only_rate": _rate(tp1_only_count, total_rows),
        "tp1_then_be_count": tp1_then_be_count,
        "tp1_then_be_rate": _rate(tp1_then_be_count, total_rows),
        "tp2_count": tp2_count,
        "tp2_rate": _rate(tp2_count, total_rows),
        "ambiguous_count": ambiguous_count,
        "ambiguous_rate": _rate(ambiguous_count, total_rows),
        "expired_count": expired_count,
        "expired_rate": _rate(expired_count, total_rows),
        "candidate_rows_registered": candidate_rows_registered,
        "resolved_rows": resolved_rows,
        "win_rate_non_ambiguous": _rate(non_ambiguous_win_count, len(non_ambiguous_rows)),
        "avg_realized_r_multiple": statistics.mean(realized_values) if realized_values else None,
        "median_realized_r_multiple": statistics.median(realized_values) if realized_values else None,
        "total_realized_r_multiple": sum(realized_values) if realized_values else None,
        "avg_outcome_r_proxy": statistics.mean(outcome_proxy_values) if outcome_proxy_values else None,
        "total_outcome_r_proxy": sum(outcome_proxy_values) if outcome_proxy_values else None,
        "tp1_touch_count": tp1_touch_count,
        "tp1_touch_rate": _rate(tp1_touch_count, total_rows),
        "tp2_count_for_progression": tp2_count,
        "tp2_rate_for_progression": _rate(tp2_count, total_rows),
        "tp1_to_tp2_conversion_rate": _rate(tp2_count, tp1_touch_count),
    }


def _build_group_row(bucket: list[dict[str, Any]], group_key: tuple[Any, ...], group_by: list[str]) -> dict[str, Any]:
    out = {field: group_key[idx] for idx, field in enumerate(group_by)}

    realized_values = _numeric_series(bucket, "realized_r_multiple")
    outcome_proxy_values = _numeric_series(bucket, "outcome_r_proxy")

    tp2_count = _count_resolution(bucket, RES_TP2)
    candidate_rows_registered = _count_candidate_rows(bucket)
    resolved_rows = _count_resolved(bucket)
    non_ambiguous_rows = [row for row in bucket if _is_non_ambiguous_resolved(row)]
    non_ambiguous_win_count = _count_non_ambiguous_wins(bucket)
    tp1_touch_count = sum(
        1
        for row in bucket
        if str(row.get("resolution_status") or "").upper() in TP1_TOUCH_RESOLUTIONS
    )

    out.update(
        {
            "row_count": len(bucket),
            "candidate_rows_registered": candidate_rows_registered,
            "valid_labeled_rows": _count_label(bucket, LABEL_LABELED),
            "invalid_rows": _count_label(bucket, LABEL_INVALID),
            "activated_count": _count_activation(bucket, ACTIVATED),
            "never_activated_count": _count_activation(bucket, NEVER_ACTIVATED),
            "tp1_touch_count": tp1_touch_count,
            "tp2_count": tp2_count,
            "resolved_rows": resolved_rows,
            "win_rate_non_ambiguous": _rate(non_ambiguous_win_count, len(non_ambiguous_rows)),
            "tp1_to_tp2_conversion_rate": _rate(tp2_count, tp1_touch_count),
            "stopped_count": _count_resolution(bucket, RES_STOPPED),
            "tp1_only_count": _count_resolution(bucket, RES_TP1_ONLY),
            "tp1_then_be_count": _count_resolution(bucket, RES_TP1_BE),
            "ambiguous_count": _count_resolution(bucket, RES_AMBIGUOUS),
            "expired_count": _count_resolution(bucket, RES_EXPIRED),
            "avg_realized_r_multiple": statistics.mean(realized_values) if realized_values else None,
            "total_realized_r_multiple": sum(realized_values) if realized_values else None,
            "avg_outcome_r_proxy": statistics.mean(outcome_proxy_values) if outcome_proxy_values else None,
            "total_outcome_r_proxy": sum(outcome_proxy_values) if outcome_proxy_values else None,
        }
    )
    return out


def _build_grouped_summary(rows: list[dict[str, Any]], group_by: list[str]) -> list[dict[str, Any]]:
    grouped_rows: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped_rows[_group_key(row, group_by)].append(row)

    out: list[dict[str, Any]] = []
    for key in sorted(grouped_rows.keys(), key=lambda x: tuple(str(v) for v in x)):
        out.append(_build_group_row(grouped_rows[key], key, group_by))
    return out


def _relative_to_data_root(path: Path, data_root: Path) -> str:
    try:
        return str(path.relative_to(data_root))
    except Exception:
        return str(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze deterministic labeled dataset into research scorecards.")
    parser.add_argument("--input-labeled-dataset-path", required=True, help="Path to labeled dataset (.parquet or .csv)")
    parser.add_argument("--source-manifest-path", default=None, help="Optional source labels manifest path")
    parser.add_argument("--output-root", default=None, help="Optional data root override")
    parser.add_argument("--group-by", nargs="*", default=list(DEFAULT_GROUP_BY), help="Grouping fields for breakdown summary")
    parser.add_argument("--symbols", nargs="*", default=None, help="Optional symbols filter")
    parser.add_argument("--notes", default="", help="Optional analysis notes")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for quick runs")
    parser.add_argument("--dry-run", action="store_true", help="Read/aggregate only; do not write artifacts")
    return parser


def run_analysis(args: argparse.Namespace) -> dict[str, Any]:
    input_dataset_path = Path(args.input_labeled_dataset_path).resolve()
    if not input_dataset_path.exists():
        raise FileNotFoundError(f"Input labeled dataset not found: {input_dataset_path}")

    group_by = [g for g in args.group_by if g]
    if not group_by:
        raise ValueError("At least one --group-by field is required")

    output_paths = _resolve_paths(args.output_root)
    ensure_research_directories(output_paths)

    labeled_rows = read_labeled_dataset(input_dataset_path, limit=args.limit)
    symbols_filter = {s for s in (args.symbols or []) if s}
    if symbols_filter:
        labeled_rows = [row for row in labeled_rows if str(row.get("symbol") or "") in symbols_filter]

    grouped_summary = _build_grouped_summary(labeled_rows, group_by)
    scorecard = _build_outcome_scorecard(labeled_rows)

    run_id = f"run_{utc_timestamp_label()}"
    grouped_filename = versioned_dataset_name("analysis_grouped", run_id, "csv")
    scorecard_filename = versioned_dataset_name("analysis_scorecard", run_id, "json")
    grouped_path = output_paths.analysis_root / grouped_filename
    scorecard_path = output_paths.analysis_root / scorecard_filename

    manifest = new_manifest(
        run_id=run_id,
        source_context={
            "mode": ANALYSIS_MODE,
            "schema_version": SCHEMA_VERSION,
            "source_labeled_dataset_path": str(input_dataset_path),
            "source_manifest_path": args.source_manifest_path,
            "group_by": group_by,
            "symbols_filter": sorted(symbols_filter),
            "notes": args.notes,
            "dry_run": bool(args.dry_run),
            "limit": args.limit,
        },
    )

    result: dict[str, Any] = {
        "run_id": run_id,
        "row_count": len(labeled_rows),
        "group_count": len(grouped_summary),
        "grouped_summary_path": str(grouped_path),
        "scorecard_path": str(scorecard_path),
        "manifest_path": None,
        "dry_run": bool(args.dry_run),
        "scorecard_preview": {
            "total_rows": scorecard.get("total_rows"),
            "valid_labeled_rows": scorecard.get("valid_labeled_rows"),
            "tp2_count": scorecard.get("tp2_count"),
            "tp1_to_tp2_conversion_rate": scorecard.get("tp1_to_tp2_conversion_rate"),
            "candidate_rows_registered": scorecard.get("candidate_rows_registered"),
            "resolved_rows": scorecard.get("resolved_rows"),
            "win_rate_non_ambiguous": scorecard.get("win_rate_non_ambiguous"),
        },
    }

    if not args.dry_run:
        write_summary_csv(grouped_summary, grouped_path)
        write_report_json(scorecard, scorecard_path)

        manifest.artifacts.append(
            DatasetArtifact(
                stage="analytics",
                artifact_type="analysis_grouped_csv",
                relative_path=_relative_to_data_root(grouped_path, output_paths.data_root),
                row_count=len(grouped_summary),
                notes="grouped_outcome_breakdown",
            )
        )
        manifest.artifacts.append(
            DatasetArtifact(
                stage="analytics",
                artifact_type="analysis_scorecard_json",
                relative_path=_relative_to_data_root(scorecard_path, output_paths.data_root),
                row_count=1,
                notes="overall_outcome_scorecard",
            )
        )

        manifest_path = output_paths.manifests_root / manifest_name(run_id)
        write_manifest(manifest_path, manifest)
        result["manifest_path"] = str(manifest_path)

    return result


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = run_analysis(args)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
