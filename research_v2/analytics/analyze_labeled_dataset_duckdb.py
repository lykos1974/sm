from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from research_v2.analytics.duckdb_io import connect_duckdb, query_rows, register_labeled_dataset_view, write_rows_csv
from research_v2.analytics.schema import ANALYSIS_MODE, DEFAULT_GROUP_BY, SCHEMA_VERSION
from research_v2.common.manifests import DatasetArtifact, new_manifest, write_manifest
from research_v2.common.naming import manifest_name, utc_timestamp_label, versioned_dataset_name
from research_v2.common.paths import ResearchPaths, ensure_research_directories, resolve_research_paths

SUPPORTED_SUFFIXES = {".parquet", ".csv"}


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


def _build_symbol_filter_clause(symbols: list[str]) -> tuple[str, list[str]]:
    if not symbols:
        return "", []
    placeholders = ", ".join(["?"] * len(symbols))
    return f"WHERE symbol IN ({placeholders})", symbols


def _build_grouped_query(group_by: list[str], symbol_filter_clause: str) -> str:
    group_cols = ", ".join(group_by)
    return f"""
WITH filtered AS (
    SELECT *
    FROM labeled
    {symbol_filter_clause}
),
base AS (
    SELECT
        {group_cols},
        COUNT(*) AS row_count,
        SUM(CASE WHEN UPPER(label_status) = 'LABELED' THEN 1 ELSE 0 END) AS valid_labeled_rows,
        SUM(CASE WHEN UPPER(label_status) = 'INVALID' THEN 1 ELSE 0 END) AS invalid_rows,
        SUM(CASE WHEN UPPER(activation_status) = 'ACTIVATED' THEN 1 ELSE 0 END) AS activated_count,
        SUM(CASE WHEN UPPER(activation_status) = 'NEVER_ACTIVATED' THEN 1 ELSE 0 END) AS never_activated_count,
        SUM(CASE WHEN UPPER(resolution_status) = 'STOPPED' THEN 1 ELSE 0 END) AS stopped_count,
        SUM(CASE WHEN UPPER(resolution_status) = 'TP1_ONLY' THEN 1 ELSE 0 END) AS tp1_only_count,
        SUM(CASE WHEN UPPER(resolution_status) = 'TP1_THEN_BE' THEN 1 ELSE 0 END) AS tp1_then_be_count,
        SUM(CASE WHEN UPPER(resolution_status) = 'TP2' THEN 1 ELSE 0 END) AS tp2_count,
        SUM(CASE WHEN UPPER(resolution_status) = 'AMBIGUOUS' THEN 1 ELSE 0 END) AS ambiguous_count,
        SUM(CASE WHEN UPPER(resolution_status) = 'EXPIRED' THEN 1 ELSE 0 END) AS expired_count,
        SUM(CASE WHEN UPPER(resolution_status) IN ('TP1_ONLY','TP1_THEN_BE','TP2') THEN 1 ELSE 0 END) AS tp1_touch_count,
        AVG(TRY_CAST(realized_r_multiple AS DOUBLE)) AS avg_realized_r_multiple,
        MEDIAN(TRY_CAST(realized_r_multiple AS DOUBLE)) AS median_realized_r_multiple,
        SUM(TRY_CAST(realized_r_multiple AS DOUBLE)) AS total_realized_r_multiple,
        AVG(TRY_CAST(outcome_r_proxy AS DOUBLE)) AS avg_outcome_r_proxy,
        SUM(TRY_CAST(outcome_r_proxy AS DOUBLE)) AS total_outcome_r_proxy
    FROM filtered
    GROUP BY {group_cols}
)
SELECT
    *,
    activated_count * 1.0 / NULLIF(row_count, 0) AS activated_rate,
    never_activated_count * 1.0 / NULLIF(row_count, 0) AS never_activated_rate,
    stopped_count * 1.0 / NULLIF(row_count, 0) AS stopped_rate,
    tp1_only_count * 1.0 / NULLIF(row_count, 0) AS tp1_only_rate,
    tp1_then_be_count * 1.0 / NULLIF(row_count, 0) AS tp1_then_be_rate,
    tp2_count * 1.0 / NULLIF(row_count, 0) AS tp2_rate,
    ambiguous_count * 1.0 / NULLIF(row_count, 0) AS ambiguous_rate,
    expired_count * 1.0 / NULLIF(row_count, 0) AS expired_rate,
    tp1_touch_count * 1.0 / NULLIF(row_count, 0) AS tp1_touch_rate,
    tp2_count * 1.0 / NULLIF(tp1_touch_count, 0) AS tp1_to_tp2_conversion_rate
FROM base
ORDER BY {group_cols}
"""


def _build_scorecard_query(symbol_filter_clause: str) -> str:
    return f"""
WITH filtered AS (
    SELECT *
    FROM labeled
    {symbol_filter_clause}
)
SELECT
    '{ANALYSIS_MODE}_duckdb' AS analysis_mode,
    '{SCHEMA_VERSION}' AS schema_version,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN UPPER(label_status) = 'LABELED' THEN 1 ELSE 0 END) AS valid_labeled_rows,
    SUM(CASE WHEN UPPER(label_status) = 'INVALID' THEN 1 ELSE 0 END) AS invalid_rows,
    SUM(CASE WHEN UPPER(activation_status) = 'ACTIVATED' THEN 1 ELSE 0 END) AS activated_count,
    SUM(CASE WHEN UPPER(activation_status) = 'NEVER_ACTIVATED' THEN 1 ELSE 0 END) AS never_activated_count,
    SUM(CASE WHEN UPPER(resolution_status) = 'STOPPED' THEN 1 ELSE 0 END) AS stopped_count,
    SUM(CASE WHEN UPPER(resolution_status) = 'TP1_ONLY' THEN 1 ELSE 0 END) AS tp1_only_count,
    SUM(CASE WHEN UPPER(resolution_status) = 'TP1_THEN_BE' THEN 1 ELSE 0 END) AS tp1_then_be_count,
    SUM(CASE WHEN UPPER(resolution_status) = 'TP2' THEN 1 ELSE 0 END) AS tp2_count,
    SUM(CASE WHEN UPPER(resolution_status) = 'AMBIGUOUS' THEN 1 ELSE 0 END) AS ambiguous_count,
    SUM(CASE WHEN UPPER(resolution_status) = 'EXPIRED' THEN 1 ELSE 0 END) AS expired_count,
    SUM(CASE WHEN UPPER(resolution_status) IN ('TP1_ONLY','TP1_THEN_BE','TP2') THEN 1 ELSE 0 END) AS tp1_touch_count,
    AVG(TRY_CAST(realized_r_multiple AS DOUBLE)) AS avg_realized_r_multiple,
    MEDIAN(TRY_CAST(realized_r_multiple AS DOUBLE)) AS median_realized_r_multiple,
    SUM(TRY_CAST(realized_r_multiple AS DOUBLE)) AS total_realized_r_multiple,
    AVG(TRY_CAST(outcome_r_proxy AS DOUBLE)) AS avg_outcome_r_proxy,
    SUM(TRY_CAST(outcome_r_proxy AS DOUBLE)) AS total_outcome_r_proxy,
    SUM(CASE WHEN UPPER(activation_status) = 'ACTIVATED' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS activation_rate,
    SUM(CASE WHEN UPPER(activation_status) = 'NEVER_ACTIVATED' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS never_activated_rate,
    SUM(CASE WHEN UPPER(resolution_status) = 'STOPPED' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS stopped_rate,
    SUM(CASE WHEN UPPER(resolution_status) = 'TP1_ONLY' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS tp1_only_rate,
    SUM(CASE WHEN UPPER(resolution_status) = 'TP1_THEN_BE' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS tp1_then_be_rate,
    SUM(CASE WHEN UPPER(resolution_status) = 'TP2' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS tp2_rate,
    SUM(CASE WHEN UPPER(resolution_status) = 'AMBIGUOUS' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS ambiguous_rate,
    SUM(CASE WHEN UPPER(resolution_status) = 'EXPIRED' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS expired_rate,
    SUM(CASE WHEN UPPER(resolution_status) IN ('TP1_ONLY','TP1_THEN_BE','TP2') THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS tp1_touch_rate,
    SUM(CASE WHEN UPPER(resolution_status) = 'TP2' THEN 1 ELSE 0 END) * 1.0 /
        NULLIF(SUM(CASE WHEN UPPER(resolution_status) IN ('TP1_ONLY','TP1_THEN_BE','TP2') THEN 1 ELSE 0 END), 0) AS tp1_to_tp2_conversion_rate
FROM filtered
"""


def _read_sql_input(sql_query: str | None, sql_file: str | None) -> tuple[str | None, str | None]:
    if sql_query and sql_file:
        raise ValueError("Use either --sql-query or --sql-file, not both")

    if sql_query is not None:
        normalized = str(sql_query).strip()
        if not normalized:
            raise ValueError("--sql-query was provided but is empty")
        return normalized, "cli"

    if sql_file is not None:
        file_path = Path(sql_file)
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")
        if not file_path.is_file():
            raise ValueError(f"SQL path is not a file: {file_path}")
        text = file_path.read_text(encoding="utf-8").strip()
        if not text:
            raise ValueError(f"SQL file is empty: {file_path}")
        return text, "file"

    return None, None


def _relative_to_data_root(path: Path, data_root: Path) -> str:
    try:
        return str(path.relative_to(data_root))
    except Exception:
        return str(path)


def _preflight_validate(args: argparse.Namespace) -> dict[str, Any]:
    input_path = Path(args.input_labeled_dataset_path).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input labeled dataset not found: {input_path}")

    suffix = input_path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise ValueError(
            f"Unsupported input dataset suffix '{suffix}'. Supported: {sorted(SUPPORTED_SUFFIXES)}"
        )

    group_by = [g for g in args.group_by if g]
    if not group_by:
        raise ValueError("At least one --group-by field is required")

    symbols = [s for s in (args.symbols or []) if s]
    sql_text, sql_source = _read_sql_input(args.sql_query, args.sql_file)

    return {
        "input_path": input_path,
        "suffix": suffix,
        "group_by": group_by,
        "symbols": symbols,
        "sql_text": sql_text,
        "sql_source": sql_source,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DuckDB analytics for frozen labeled datasets.")
    parser.add_argument("--input-labeled-dataset-path", required=True, help="Path to labeled dataset (.parquet or .csv)")
    parser.add_argument("--source-manifest-path", default=None, help="Optional source labels manifest path")
    parser.add_argument("--output-root", default=None, help="Optional data root override")
    parser.add_argument("--group-by", nargs="*", default=list(DEFAULT_GROUP_BY), help="Grouping fields for grouped summary")
    parser.add_argument("--symbols", nargs="*", default=None, help="Optional symbols filter")
    parser.add_argument("--sql-query", default=None, help="Optional ad-hoc SQL query to run on DuckDB view 'labeled'")
    parser.add_argument("--sql-file", default=None, help="Optional path to SQL file for ad-hoc query")
    parser.add_argument("--notes", default="", help="Optional notes")
    parser.add_argument("--dry-run", action="store_true", help="Compile queries only; do not write artifacts")
    return parser


def run_analysis_duckdb(args: argparse.Namespace) -> dict[str, Any]:
    preflight = _preflight_validate(args)
    input_dataset_path: Path = preflight["input_path"]
    group_by: list[str] = preflight["group_by"]
    symbols: list[str] = preflight["symbols"]
    user_sql: str | None = preflight["sql_text"]
    sql_source: str | None = preflight["sql_source"]

    symbol_filter_clause, symbol_filter_params = _build_symbol_filter_clause(symbols)
    output_paths = _resolve_paths(args.output_root)
    ensure_research_directories(output_paths)

    grouped_query = _build_grouped_query(group_by, symbol_filter_clause)
    scorecard_query = _build_scorecard_query(symbol_filter_clause)

    messages: list[str] = [
        f"DuckDB input: {input_dataset_path}",
        f"Grouping fields: {group_by}",
    ]
    if symbols:
        messages.append(f"Symbols filter: {symbols}")
    if user_sql is not None:
        messages.append(f"SQL mode: enabled (source={sql_source})")
    else:
        messages.append("SQL mode: disabled")

    conn = connect_duckdb()
    try:
        register_labeled_dataset_view(conn, input_dataset_path)

        grouped_rows = query_rows(conn, grouped_query, symbol_filter_params)
        scorecard_rows = query_rows(conn, scorecard_query, symbol_filter_params)
        scorecard = scorecard_rows[0] if scorecard_rows else {}

        sql_rows: list[dict[str, Any]] | None = None
        if user_sql:
            sql_rows = query_rows(conn, user_sql)

        run_id = f"run_{utc_timestamp_label()}"
        grouped_path = output_paths.analysis_root / versioned_dataset_name("analysis_grouped_duckdb", run_id, "csv")
        scorecard_path = output_paths.analysis_root / versioned_dataset_name("analysis_scorecard_duckdb", run_id, "json")
        sql_output_path = None
        if user_sql is not None:
            sql_output_path = output_paths.analysis_root / versioned_dataset_name("analysis_sql_duckdb", run_id, "csv")

        manifest = new_manifest(
            run_id=run_id,
            source_context={
                "mode": f"{ANALYSIS_MODE}_duckdb",
                "schema_version": SCHEMA_VERSION,
                "engine": "duckdb",
                "source_labeled_dataset_path": str(input_dataset_path),
                "source_manifest_path": args.source_manifest_path,
                "group_by": group_by,
                "symbols_filter": symbols,
                "sql_mode": user_sql is not None,
                "sql_source": sql_source,
                "sql_file": args.sql_file,
                "sql_query": args.sql_query,
                "notes": args.notes,
                "dry_run": bool(args.dry_run),
            },
        )

        result: dict[str, Any] = {
            "run_id": run_id,
            "row_count": int(scorecard.get("total_rows") or 0),
            "group_count": len(grouped_rows),
            "grouped_summary_path": str(grouped_path),
            "scorecard_path": str(scorecard_path),
            "sql_output_path": str(sql_output_path) if sql_output_path else None,
            "manifest_path": None,
            "dry_run": bool(args.dry_run),
            "engine": "duckdb",
            "messages": messages,
        }

        if args.dry_run:
            messages.append(f"Dry-run: would write grouped summary to {grouped_path}")
            messages.append(f"Dry-run: would write scorecard to {scorecard_path}")
            if sql_output_path is not None:
                messages.append(f"Dry-run: would write SQL output to {sql_output_path}")
            messages.append(f"Dry-run: would write manifest to {output_paths.manifests_root / manifest_name(run_id)}")
            return result

        write_rows_csv(grouped_rows, grouped_path)
        scorecard_path.parent.mkdir(parents=True, exist_ok=True)
        scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        manifest.artifacts.append(
            DatasetArtifact(
                stage="analytics",
                artifact_type="analysis_grouped_duckdb_csv",
                relative_path=_relative_to_data_root(grouped_path, output_paths.data_root),
                row_count=len(grouped_rows),
                notes="duckdb_grouped_summary",
            )
        )
        manifest.artifacts.append(
            DatasetArtifact(
                stage="analytics",
                artifact_type="analysis_scorecard_duckdb_json",
                relative_path=_relative_to_data_root(scorecard_path, output_paths.data_root),
                row_count=1,
                notes="duckdb_overall_scorecard",
            )
        )

        if sql_rows is not None and sql_output_path is not None:
            write_rows_csv(sql_rows, sql_output_path)
            manifest.artifacts.append(
                DatasetArtifact(
                    stage="analytics",
                    artifact_type="analysis_sql_duckdb_csv",
                    relative_path=_relative_to_data_root(sql_output_path, output_paths.data_root),
                    row_count=len(sql_rows),
                    notes="duckdb_ad_hoc_sql_output",
                )
            )

        manifest_path = output_paths.manifests_root / manifest_name(run_id)
        write_manifest(manifest_path, manifest)
        result["manifest_path"] = str(manifest_path)
        return result
    finally:
        conn.close()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        result = run_analysis_duckdb(args)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        raise SystemExit(f"ERROR: {exc}")

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
