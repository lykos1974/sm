from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from research_v2.common.manifests import DatasetArtifact, new_manifest, write_manifest
from research_v2.common.naming import manifest_name, utc_timestamp_label, versioned_dataset_name
from research_v2.common.paths import ResearchPaths, ensure_research_directories, resolve_research_paths
from research_v2.filters.io import read_setup_dataset, write_setup_dataset

FILTER_MODE = "profitable_family_v1"
FILTER_RULES = {
    "side": "LONG",
    "breakout_context": "POST_BREAKOUT_PULLBACK",
    "active_leg_boxes": 2,
    "quality_grade": "A",
}


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


def _as_int(value: Any) -> int | None:
    if value in (None, "", "None"):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _matches_profitable_family(row: dict[str, Any]) -> bool:
    side = str(row.get("side") or "").upper()
    breakout_context = str(row.get("breakout_context") or "").upper()
    quality_grade = str(row.get("quality_grade") or "").upper()
    active_leg_boxes = _as_int(row.get("active_leg_boxes"))

    return (
        side == FILTER_RULES["side"]
        and breakout_context == FILTER_RULES["breakout_context"]
        and active_leg_boxes == FILTER_RULES["active_leg_boxes"]
        and quality_grade == FILTER_RULES["quality_grade"]
    )


def _relative_to_data_root(path: Path, data_root: Path) -> str:
    try:
        return str(path.relative_to(data_root))
    except Exception:
        return str(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Filter setup dataset to profitable family v1.")
    parser.add_argument("--input-setup-dataset-path", required=True, help="Path to setup dataset artifact (.parquet/.csv)")
    parser.add_argument("--source-manifest-path", default=None, help="Optional source setup manifest path")
    parser.add_argument("--output-root", default=None, help="Optional data root override")
    parser.add_argument("--format", choices=["auto", "parquet", "csv"], default="auto")
    parser.add_argument("--notes", default="", help="Optional run notes")
    parser.add_argument("--dry-run", action="store_true", help="Read/filter only; do not write artifacts")
    return parser


def run_filter(args: argparse.Namespace) -> dict[str, Any]:
    input_path = Path(args.input_setup_dataset_path).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input setup dataset not found: {input_path}")

    output_paths = _resolve_paths(args.output_root)
    ensure_research_directories(output_paths)

    rows, columns = read_setup_dataset(input_path)
    filtered_rows = [row for row in rows if _matches_profitable_family(row)]

    run_id = f"run_{utc_timestamp_label()}"
    preferred_ext = "parquet" if args.format in {"auto", "parquet"} else "csv"
    artifact_name = versioned_dataset_name("setups_filtered_profitable_v1", run_id, preferred_ext)
    output_path = output_paths.setups_root / artifact_name

    manifest = new_manifest(
        run_id=run_id,
        source_context={
            "mode": "filter_setup_dataset",
            "filter_mode": FILTER_MODE,
            "filter_rules": FILTER_RULES,
            "source_setup_dataset_path": str(input_path),
            "source_manifest_path": args.source_manifest_path,
            "input_row_count": len(rows),
            "output_row_count": len(filtered_rows),
            "notes": args.notes,
            "dry_run": bool(args.dry_run),
        },
    )

    result: dict[str, Any] = {
        "run_id": run_id,
        "input_row_count": len(rows),
        "output_row_count": len(filtered_rows),
        "filter_mode": FILTER_MODE,
        "output_path": str(output_path),
        "output_format": None,
        "manifest_path": None,
        "dry_run": bool(args.dry_run),
    }

    if not args.dry_run:
        actual_fmt = write_setup_dataset(filtered_rows, columns, output_path, args.format)
        actual_output_path = output_path if actual_fmt == "parquet" else output_path.with_suffix(".csv")
        manifest.artifacts.append(
            DatasetArtifact(
                stage="setup_filter",
                artifact_type=f"setups_filtered_{actual_fmt}",
                relative_path=_relative_to_data_root(actual_output_path, output_paths.data_root),
                row_count=len(filtered_rows),
                notes=FILTER_MODE,
            )
        )
        manifest_path = output_paths.manifests_root / manifest_name(run_id)
        write_manifest(manifest_path, manifest)

        result["output_path"] = str(actual_output_path)
        result["output_format"] = actual_fmt
        result["manifest_path"] = str(manifest_path)

    return result


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = run_filter(args)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
