from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from research_v2.common.manifests import DatasetArtifact, new_manifest, write_manifest
from research_v2.common.naming import manifest_name, utc_timestamp_label, versioned_dataset_name
from research_v2.common.paths import ResearchPaths, ensure_research_directories, resolve_research_paths
from research_v2.setup_dataset.io import SOURCE_TABLE, fetch_setup_rows, write_dataset
from research_v2.setup_dataset.schema import EXPORT_COLUMNS, SCHEMA_VERSION


def _load_settings(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Settings file must contain a JSON object: {path}")
    return payload


def _resolve_db_path(settings_path: Path, settings: dict[str, Any], db_path_override: str | None) -> Path:
    if db_path_override:
        raw = db_path_override
    else:
        raw = str(settings.get("strategy_validation_db_path") or "strategy_validation.db")

    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    return (settings_path.parent / candidate).resolve()


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


def _relative_to_data_root(path: Path, data_root: Path) -> str:
    try:
        return str(path.relative_to(data_root))
    except Exception:
        return str(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export frozen setup dataset from strategy_setups.")
    parser.add_argument(
        "--settings-path",
        default="pnf_mvp/settings.research_clean.json",
        help="JSON settings path used to resolve strategy_validation_db_path",
    )
    parser.add_argument(
        "--validation-db-path",
        default=None,
        help="Optional override for strategy validation sqlite db path",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional symbol filter (space-separated)",
    )
    parser.add_argument("--reference-ts-from", type=int, default=None, help="Optional lower bound (unix seconds)")
    parser.add_argument("--reference-ts-to", type=int, default=None, help="Optional upper bound (unix seconds)")
    parser.add_argument(
        "--output-root",
        default=None,
        help="Optional data root override. Defaults to repo data/research",
    )
    parser.add_argument(
        "--format",
        choices=["auto", "parquet", "csv"],
        default="auto",
        help="Parquet-first output mode. Use csv for explicit fallback.",
    )
    parser.add_argument("--notes", default="", help="Optional run notes saved to manifest source_context")
    parser.add_argument("--dry-run", action="store_true", help="Read and validate only; do not write outputs")
    return parser


def run_export(args: argparse.Namespace) -> dict[str, Any]:
    settings_path = Path(args.settings_path).resolve()
    settings = _load_settings(settings_path)
    db_path = _resolve_db_path(settings_path, settings, args.validation_db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Validation DB not found: {db_path}")

    symbols = [s for s in (args.symbols or []) if s]
    output_paths = _resolve_paths(args.output_root)
    ensure_research_directories(output_paths)

    rows = fetch_setup_rows(
        db_path=db_path,
        symbols=symbols or None,
        reference_ts_from=args.reference_ts_from,
        reference_ts_to=args.reference_ts_to,
    )
    status_counts: dict[str, int] = {}
    for row in rows:
        status_key = str(row.get("status") or "").upper() or "UNKNOWN"
        status_counts[status_key] = status_counts.get(status_key, 0) + 1

    run_id = f"run_{utc_timestamp_label()}"
    preferred_ext = "parquet" if args.format in {"auto", "parquet"} else "csv"
    dataset_filename = versioned_dataset_name("setups", run_id, preferred_ext)
    dataset_path = output_paths.setups_root / dataset_filename

    manifest = new_manifest(
        run_id=run_id,
        source_context={
            "mode": "setup_dataset_export",
            "schema_version": SCHEMA_VERSION,
            "source_table": SOURCE_TABLE,
            "source_validation_db_path": str(db_path),
            "settings_path": str(settings_path),
            "symbol_scope": symbols,
            "reference_ts_from": args.reference_ts_from,
            "reference_ts_to": args.reference_ts_to,
            "notes": args.notes,
            "dry_run": bool(args.dry_run),
            "column_order": list(EXPORT_COLUMNS),
            "status_preservation": "status column is exported exactly as stored in strategy_setups",
            "status_counts": status_counts,
        },
    )

    result: dict[str, Any] = {
        "run_id": run_id,
        "row_count": len(rows),
        "dataset_path": str(dataset_path),
        "dataset_format": None,
        "manifest_path": None,
        "dry_run": bool(args.dry_run),
    }

    if not args.dry_run:
        actual_fmt = write_dataset(rows=rows, output_path=dataset_path, fmt=args.format)
        artifact_path = dataset_path if actual_fmt == "parquet" else dataset_path.with_suffix(".csv")
        manifest.artifacts.append(
            DatasetArtifact(
                stage="setup_dataset",
                artifact_type=f"setup_dataset_{actual_fmt}",
                relative_path=_relative_to_data_root(artifact_path, output_paths.data_root),
                row_count=len(rows),
                notes="frozen_setup_dataset",
            )
        )
        manifest_path = output_paths.manifests_root / manifest_name(run_id)
        write_manifest(manifest_path, manifest)

        result["dataset_format"] = actual_fmt
        result["dataset_path"] = str(artifact_path)
        result["manifest_path"] = str(manifest_path)

    return result


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = run_export(args)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
