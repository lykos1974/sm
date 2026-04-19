from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from research_v2.common.manifests import DatasetArtifact, new_manifest, write_manifest
from research_v2.common.naming import manifest_name, utc_timestamp_label, versioned_dataset_name
from research_v2.common.paths import ResearchPaths, ensure_research_directories, resolve_research_paths
from research_v2.labeling.io import (
    fetch_future_candles,
    load_settings,
    read_setup_dataset,
    resolve_candles_db_path,
    write_labels_dataset,
)
from research_v2.labeling.schema import LABEL_COLUMNS, LABEL_MODE_V1_INDEPENDENT, SCHEMA_VERSION

LABEL_STATUS_LABELED = "LABELED"
LABEL_STATUS_SKIPPED = "SKIPPED"
LABEL_STATUS_INVALID = "INVALID"

ACTIVATION_NEVER = "NEVER_ACTIVATED"
ACTIVATION_ACTIVE = "ACTIVATED"
ACTIVATION_AMBIGUOUS = "AMBIGUOUS_ACTIVATION"

RESOLUTION_UNRESOLVED = "UNRESOLVED"
RESOLUTION_STOPPED = "STOPPED"
RESOLUTION_TP1_ONLY = "TP1_ONLY"
RESOLUTION_TP1_THEN_BE = "TP1_THEN_BE"
RESOLUTION_TP2 = "TP2"
RESOLUTION_AMBIGUOUS = "AMBIGUOUS"
RESOLUTION_EXPIRED = "EXPIRED"


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


def _safe_int(value: Any) -> int | None:
    if value in (None, "", "None"):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _side_normalized(value: Any) -> str:
    return str(value or "").upper().strip()


def _touch_activate(side: str, ideal_entry: float, candle: dict[str, Any]) -> bool:
    low = _safe_float(candle.get("low"))
    high = _safe_float(candle.get("high"))
    if low is None or high is None:
        return False
    if side == "LONG":
        return low <= ideal_entry
    if side == "SHORT":
        return high >= ideal_entry
    return False


def _hit_stop(side: str, invalidation: float, candle: dict[str, Any]) -> bool:
    low = _safe_float(candle.get("low"))
    high = _safe_float(candle.get("high"))
    if low is None or high is None:
        return False
    if side == "LONG":
        return low <= invalidation
    if side == "SHORT":
        return high >= invalidation
    return False


def _hit_tp(side: str, target: float, candle: dict[str, Any]) -> bool:
    low = _safe_float(candle.get("low"))
    high = _safe_float(candle.get("high"))
    if low is None or high is None:
        return False
    if side == "LONG":
        return high >= target
    if side == "SHORT":
        return low <= target
    return False


def _compute_rr(entry: float, invalidation: float, target: float, side: str) -> float | None:
    risk = abs(entry - invalidation)
    if risk <= 0:
        return None
    if side == "LONG":
        reward = target - entry
    else:
        reward = entry - target
    return reward / risk


def _base_label_row(setup_row: dict[str, Any], label_mode: str, source_dataset_artifact: str, source_manifest_path: str | None) -> dict[str, Any]:
    return {
        "symbol": setup_row.get("symbol"),
        "reference_ts": setup_row.get("reference_ts"),
        "side": setup_row.get("side"),
        "status": setup_row.get("status"),
        "strategy": setup_row.get("strategy"),
        "breakout_context": setup_row.get("breakout_context"),
        "pullback_quality": setup_row.get("pullback_quality"),
        "active_leg_boxes": setup_row.get("active_leg_boxes"),
        "quality_score": setup_row.get("quality_score"),
        "quality_grade": setup_row.get("quality_grade"),
        "source_dataset_artifact": source_dataset_artifact,
        "source_manifest_path": source_manifest_path,
        "label_status": LABEL_STATUS_SKIPPED,
        "activation_status": ACTIVATION_NEVER,
        "resolution_status": RESOLUTION_UNRESOLVED,
        "realized_r_multiple": None,
        "outcome_r_proxy": None,
        "label_mode": label_mode,
        "label_notes": "",
        "activation_ts": None,
        "resolution_ts": None,
        "horizon_minutes": None,
        "source_candles_db_path": None,
    }


def evaluate_setup_outcome_v1(
    setup_row: dict[str, Any],
    future_candles: list[dict[str, Any]],
    horizon_minutes: int,
    label_mode: str,
    source_dataset_artifact: str,
    source_manifest_path: str | None,
    source_candles_db_path: str,
) -> dict[str, Any]:
    """Independent per-setup deterministic labeling engine (research mode v1)."""
    out = _base_label_row(setup_row, label_mode, source_dataset_artifact, source_manifest_path)
    out["horizon_minutes"] = int(horizon_minutes)
    out["source_candles_db_path"] = source_candles_db_path

    symbol = str(setup_row.get("symbol") or "")
    side = _side_normalized(setup_row.get("side"))
    reference_ts = _safe_int(setup_row.get("reference_ts"))
    # reference_ts and candle close_time are expected in milliseconds.
    ideal_entry = _safe_float(setup_row.get("ideal_entry"))
    invalidation = _safe_float(setup_row.get("invalidation"))
    tp1 = _safe_float(setup_row.get("tp1"))
    tp2 = _safe_float(setup_row.get("tp2"))
    rr1 = _safe_float(setup_row.get("rr1"))
    rr2 = _safe_float(setup_row.get("rr2"))

    if not symbol or side not in {"LONG", "SHORT"} or reference_ts is None:
        out["label_status"] = LABEL_STATUS_INVALID
        out["label_notes"] = "missing core fields: symbol/side/reference_ts"
        return out

    if ideal_entry is None or invalidation is None or tp1 is None or tp2 is None:
        out["label_status"] = LABEL_STATUS_INVALID
        out["label_notes"] = "missing required price fields: ideal_entry/invalidation/tp1/tp2"
        return out

    if abs(ideal_entry - invalidation) <= 0:
        out["label_status"] = LABEL_STATUS_INVALID
        out["label_notes"] = "invalid risk: ideal_entry equals invalidation"
        return out

    if rr1 is None:
        rr1 = _compute_rr(ideal_entry, invalidation, tp1, side)
    if rr2 is None:
        rr2 = _compute_rr(ideal_entry, invalidation, tp2, side)

    activated = False
    tp1_hit = False

    for candle in future_candles:
        candle_ts = _safe_int(candle.get("close_time"))
        if candle_ts is None or candle_ts <= reference_ts:
            continue

        if not activated:
            entry_touched = _touch_activate(side, ideal_entry, candle)
            if not entry_touched:
                continue

            stop_touched_same_candle = _hit_stop(side, invalidation, candle)
            if stop_touched_same_candle:
                out["label_status"] = LABEL_STATUS_LABELED
                out["activation_status"] = ACTIVATION_AMBIGUOUS
                out["resolution_status"] = RESOLUTION_AMBIGUOUS
                out["resolution_ts"] = candle_ts
                out["label_notes"] = "entry and stop touched in same activation candle"
                return out

            activated = True
            out["activation_status"] = ACTIVATION_ACTIVE
            out["activation_ts"] = candle_ts

        if not tp1_hit:
            hit_stop = _hit_stop(side, invalidation, candle)
            hit_tp1 = _hit_tp(side, tp1, candle)
            hit_tp2 = _hit_tp(side, tp2, candle)

            if hit_stop and (hit_tp1 or hit_tp2):
                out["label_status"] = LABEL_STATUS_LABELED
                out["resolution_status"] = RESOLUTION_AMBIGUOUS
                out["resolution_ts"] = candle_ts
                out["label_notes"] = "stop and target touched in same candle before TP1 lock-in"
                return out

            if hit_stop:
                out["label_status"] = LABEL_STATUS_LABELED
                out["resolution_status"] = RESOLUTION_STOPPED
                out["resolution_ts"] = candle_ts
                out["realized_r_multiple"] = -1.0
                out["outcome_r_proxy"] = -1.0
                out["label_notes"] = "stop touched before TP1"
                return out

            if hit_tp2:
                out["label_status"] = LABEL_STATUS_LABELED
                out["resolution_status"] = RESOLUTION_TP2
                out["resolution_ts"] = candle_ts
                out["realized_r_multiple"] = rr2
                out["outcome_r_proxy"] = rr2
                out["label_notes"] = "tp2 touched"
                return out

            if hit_tp1:
                tp1_hit = True
                continue

        else:
            be_price = ideal_entry
            hit_be = _hit_stop(side, be_price, candle)
            hit_tp2 = _hit_tp(side, tp2, candle)

            if hit_be and hit_tp2:
                out["label_status"] = LABEL_STATUS_LABELED
                out["resolution_status"] = RESOLUTION_AMBIGUOUS
                out["resolution_ts"] = candle_ts
                out["label_notes"] = "tp2 and breakeven touched in same candle after TP1"
                return out

            if hit_tp2:
                out["label_status"] = LABEL_STATUS_LABELED
                out["resolution_status"] = RESOLUTION_TP2
                out["resolution_ts"] = candle_ts
                out["realized_r_multiple"] = rr2
                out["outcome_r_proxy"] = rr2
                out["label_notes"] = "tp2 touched after TP1"
                return out

            if hit_be:
                out["label_status"] = LABEL_STATUS_LABELED
                out["resolution_status"] = RESOLUTION_TP1_THEN_BE
                out["resolution_ts"] = candle_ts
                partial = (rr1 * 0.5) if rr1 is not None else 0.5
                out["realized_r_multiple"] = partial
                out["outcome_r_proxy"] = partial
                out["label_notes"] = "tp1 then breakeven stop"
                return out

    out["label_status"] = LABEL_STATUS_LABELED
    if not activated:
        out["activation_status"] = ACTIVATION_NEVER
        out["resolution_status"] = RESOLUTION_EXPIRED
        out["realized_r_multiple"] = 0.0
        out["outcome_r_proxy"] = 0.0
        out["label_notes"] = "never activated within horizon"
        return out

    out["activation_status"] = ACTIVATION_ACTIVE
    if tp1_hit:
        out["resolution_status"] = RESOLUTION_TP1_ONLY
        out["realized_r_multiple"] = None
        out["outcome_r_proxy"] = rr1
        out["label_notes"] = "tp1 touched but tp2/be unresolved within horizon"
    else:
        out["resolution_status"] = RESOLUTION_EXPIRED
        out["realized_r_multiple"] = None
        out["outcome_r_proxy"] = 0.0
        out["label_notes"] = "activated but unresolved within horizon"
    return out


def _relative_to_data_root(path: Path, data_root: Path) -> str:
    try:
        return str(path.relative_to(data_root))
    except Exception:
        return str(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Label frozen setup dataset with independent deterministic v1 engine.")
    parser.add_argument("--input-dataset-path", required=True, help="Path to frozen setup dataset (.parquet or .csv)")
    parser.add_argument("--source-manifest-path", default=None, help="Optional source setup manifest path")
    parser.add_argument("--settings-path", default="pnf_mvp/settings.research_clean.json", help="Settings path used to resolve candles database_path")
    parser.add_argument("--candles-db-path", default=None, help="Optional direct candles DB override")
    parser.add_argument("--output-root", default=None, help="Optional data root override")
    parser.add_argument("--format", choices=["auto", "parquet", "csv"], default="auto")
    parser.add_argument("--label-mode", default=LABEL_MODE_V1_INDEPENDENT, help="Labeling mode marker stored in output")
    parser.add_argument("--horizon-minutes", type=int, default=240, help="Per-setup forward horizon in minutes")
    parser.add_argument("--symbols", nargs="*", default=None, help="Optional symbols filter")
    parser.add_argument("--notes", default="", help="Optional run notes")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for small runs")
    parser.add_argument("--dry-run", action="store_true", help="Read/evaluate only; do not write artifacts")
    return parser


def run_labeling(args: argparse.Namespace) -> dict[str, Any]:
    input_dataset_path = Path(args.input_dataset_path).resolve()
    if not input_dataset_path.exists():
        raise FileNotFoundError(f"Input dataset not found: {input_dataset_path}")

    settings_path = Path(args.settings_path).resolve() if args.settings_path else None
    settings_payload = load_settings(settings_path) if settings_path and settings_path.exists() else None
    candles_db_path = resolve_candles_db_path(settings_path, settings_payload, args.candles_db_path)
    if not candles_db_path.exists():
        raise FileNotFoundError(f"Candles DB not found: {candles_db_path}")

    output_paths = _resolve_paths(args.output_root)
    ensure_research_directories(output_paths)

    setup_rows = read_setup_dataset(input_dataset_path, limit=args.limit)
    symbols_filter = {s for s in (args.symbols or []) if s}

    labels_rows: list[dict[str, Any]] = []
    for setup_row in setup_rows:
        symbol = str(setup_row.get("symbol") or "")
        if symbols_filter and symbol not in symbols_filter:
            continue

        reference_ts = _safe_int(setup_row.get("reference_ts"))
    # reference_ts and candle close_time are expected in milliseconds.
        if reference_ts is None:
            future_candles: list[dict[str, Any]] = []
        else:
            future_candles = fetch_future_candles(
                db_path=candles_db_path,
                symbol=symbol,
                reference_ts=reference_ts,
                horizon_minutes=args.horizon_minutes,
            )

        labels_rows.append(
            evaluate_setup_outcome_v1(
                setup_row=setup_row,
                future_candles=future_candles,
                horizon_minutes=args.horizon_minutes,
                label_mode=args.label_mode,
                source_dataset_artifact=input_dataset_path.name,
                source_manifest_path=args.source_manifest_path,
                source_candles_db_path=str(candles_db_path),
            )
        )

    run_id = f"run_{utc_timestamp_label()}"
    preferred_ext = "parquet" if args.format in {"auto", "parquet"} else "csv"
    label_filename = versioned_dataset_name("labels", run_id, preferred_ext)
    label_path = output_paths.labels_root / label_filename

    manifest = new_manifest(
        run_id=run_id,
        source_context={
            "mode": "label_setup_dataset",
            "label_mode": args.label_mode,
            "schema_version": SCHEMA_VERSION,
            "source_dataset_path": str(input_dataset_path),
            "source_manifest_path": args.source_manifest_path,
            "settings_path": str(settings_path) if settings_path else None,
            "source_candles_db_path": str(candles_db_path),
            "horizon_minutes": int(args.horizon_minutes),
            "symbol_scope": sorted(symbols_filter),
            "notes": args.notes,
            "dry_run": bool(args.dry_run),
            "limit": args.limit,
            "column_order": list(LABEL_COLUMNS),
        },
    )

    result: dict[str, Any] = {
        "run_id": run_id,
        "row_count": len(labels_rows),
        "label_path": str(label_path),
        "label_format": None,
        "manifest_path": None,
        "dry_run": bool(args.dry_run),
    }

    if not args.dry_run:
        actual_fmt = write_labels_dataset(labels_rows, output_path=label_path, fmt=args.format)
        actual_label_path = label_path if actual_fmt == "parquet" else label_path.with_suffix(".csv")

        manifest.artifacts.append(
            DatasetArtifact(
                stage="labeling",
                artifact_type=f"labels_{actual_fmt}",
                relative_path=_relative_to_data_root(actual_label_path, output_paths.data_root),
                row_count=len(labels_rows),
                notes=f"label_mode={args.label_mode}",
            )
        )
        manifest_path = output_paths.manifests_root / manifest_name(run_id)
        write_manifest(manifest_path, manifest)

        result["label_format"] = actual_fmt
        result["label_path"] = str(actual_label_path)
        result["manifest_path"] = str(manifest_path)

    return result


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = run_labeling(args)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
