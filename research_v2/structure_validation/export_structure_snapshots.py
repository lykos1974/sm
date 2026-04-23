from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from research_v2.common.manifests import DatasetArtifact, new_manifest, write_manifest
from research_v2.common.paths import ensure_research_directories, resolve_research_paths


SNAPSHOT_COLUMNS = [
    "symbol",
    "close_ts",
    "current_column_kind",
    "current_column_top",
    "current_column_bottom",
    "trend_state",
    "trend_regime",
    "immediate_slope",
    "swing_direction",
    "support_level",
    "resistance_level",
    "breakout_context",
    "is_extended_move",
    "active_leg_boxes",
    "last_meaningful_x_high",
    "last_meaningful_o_low",
    "latest_signal_name",
    "market_state",
    "last_price",
    "impulse_boxes",
    "pullback_boxes",
    "impulse_to_pullback_ratio",
    "notes_json",
    "notes_hash",
]


def _bootstrap_pnf_mvp_imports(repo_root: Path) -> None:
    pnf_mvp_dir = repo_root / "pnf_mvp"
    if str(pnf_mvp_dir) not in sys.path:
        sys.path.insert(0, str(pnf_mvp_dir))


def _load_settings(settings_path: Path) -> dict[str, Any]:
    with settings_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Settings file must contain a JSON object: {settings_path}")
    return payload


def _split_symbols(settings: dict[str, Any], symbols_arg: str | None) -> list[str]:
    configured = [str(s) for s in settings.get("symbols", [])]
    if not symbols_arg:
        return configured
    requested = [s.strip() for s in symbols_arg.split(",") if s.strip()]
    if not requested:
        return configured
    return [s for s in configured if s in requested]


def _load_all_closed_candles(storage: Any, symbol: str) -> list[dict[str, Any]]:
    candles = storage.load_recent_candles(symbol, None)
    return candles[:-1] if len(candles) > 1 else []


def _stable_run_key(
    settings_path: Path,
    symbols: list[str],
    output_format: str,
    max_candles: int | None,
) -> str:
    payload = {
        "settings_path": str(settings_path.resolve()),
        "symbols": symbols,
        "output_format": output_format,
        "max_candles": max_candles,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _notes_payload(notes: Any) -> tuple[str, str]:
    if isinstance(notes, list):
        notes_json = json.dumps(notes, ensure_ascii=False, separators=(",", ":"))
    else:
        notes_json = json.dumps([], ensure_ascii=False, separators=(",", ":"))
    notes_hash = hashlib.sha1(notes_json.encode("utf-8")).hexdigest()
    return notes_json, notes_hash


def _snapshot_row(symbol: str, close_ts: int, state: dict[str, Any]) -> dict[str, Any]:
    notes_json, notes_hash = _notes_payload(state.get("notes"))
    row = {
        "symbol": symbol,
        "close_ts": int(close_ts),
        "current_column_kind": state.get("current_column_kind"),
        "current_column_top": state.get("current_column_top"),
        "current_column_bottom": state.get("current_column_bottom"),
        "trend_state": state.get("trend_state"),
        "trend_regime": state.get("trend_regime"),
        "immediate_slope": state.get("immediate_slope"),
        "swing_direction": state.get("swing_direction"),
        "support_level": state.get("support_level"),
        "resistance_level": state.get("resistance_level"),
        "breakout_context": state.get("breakout_context"),
        "is_extended_move": state.get("is_extended_move"),
        "active_leg_boxes": state.get("active_leg_boxes"),
        "last_meaningful_x_high": state.get("last_meaningful_x_high"),
        "last_meaningful_o_low": state.get("last_meaningful_o_low"),
        "latest_signal_name": state.get("latest_signal_name"),
        "market_state": state.get("market_state"),
        "last_price": state.get("last_price"),
        "impulse_boxes": state.get("impulse_boxes"),
        "pullback_boxes": state.get("pullback_boxes"),
        "impulse_to_pullback_ratio": state.get("impulse_to_pullback_ratio"),
        "notes_json": notes_json,
        "notes_hash": notes_hash,
    }
    return row


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SNAPSHOT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export baseline structure snapshots from build_structure_state().")
    parser.add_argument(
        "--settings-path",
        default="pnf_mvp/settings.research_clean.json",
        help="JSON settings path used to resolve candle DB path",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Optional comma-separated symbols. Defaults to all settings symbols.",
    )
    parser.add_argument(
        "--output-format",
        choices=["jsonl", "csv"],
        default="jsonl",
        help="Snapshot output format.",
    )
    parser.add_argument(
        "--max-candles",
        type=int,
        default=None,
        help="Optional per-symbol limit for number of closed candles replayed.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Replay and summarize only; do not write snapshot/manifest files.",
    )
    return parser


def run_export(args: argparse.Namespace) -> dict[str, Any]:
    settings_path = Path(args.settings_path).resolve()
    settings = _load_settings(settings_path)

    repo_root = Path(__file__).resolve().parents[2]
    _bootstrap_pnf_mvp_imports(repo_root)

    from pnf_engine import PnFEngine, PnFProfile  # pylint: disable=import-outside-toplevel
    from storage import Storage  # pylint: disable=import-outside-toplevel
    from structure_engine import build_structure_state  # pylint: disable=import-outside-toplevel

    symbols = _split_symbols(settings, args.symbols)
    if not symbols:
        raise ValueError("No symbols selected. Check --symbols or settings symbols.")

    db_path_raw = str(settings.get("database_path") or "pnf_mvp.db")
    db_path = Path(db_path_raw)
    if not db_path.is_absolute():
        db_path = (settings_path.parent / db_path).resolve()

    storage = Storage(str(db_path))

    paths = resolve_research_paths(repo_root=repo_root)
    structure_root = paths.data_root / "structure_snapshots"
    ensure_research_directories(paths)
    structure_root.mkdir(parents=True, exist_ok=True)

    run_key = _stable_run_key(
        settings_path=settings_path,
        symbols=symbols,
        output_format=args.output_format,
        max_candles=args.max_candles,
    )
    extension = "jsonl" if args.output_format == "jsonl" else "csv"
    output_path = structure_root / f"structure_snapshots_{run_key}.{extension}"
    manifest_path = paths.manifests_root / f"structure_snapshots_manifest_{run_key}.json"

    rows: list[dict[str, Any]] = []
    symbol_counts: dict[str, int] = {}

    for symbol in symbols:
        profile_data = settings["profiles"][symbol]
        profile = PnFProfile(
            name=symbol,
            box_size=float(profile_data["box_size"]),
            reversal_boxes=int(profile_data["reversal_boxes"]),
        )
        engine = PnFEngine(profile)
        candles = _load_all_closed_candles(storage, symbol)
        if args.max_candles is not None:
            candles = candles[: max(0, int(args.max_candles))]

        for candle in candles:
            close_ts = int(candle["close_time"])
            close_price = float(candle["close"])

            engine.update_from_price(close_ts, close_price)
            structure_state = build_structure_state(
                symbol=symbol,
                profile=profile,
                columns=engine.columns,
                latest_signal_name=engine.latest_signal_name(),
                market_state=engine.market_state(),
                last_price=getattr(engine, "last_price", None),
            )
            rows.append(_snapshot_row(symbol=symbol, close_ts=close_ts, state=structure_state))

        symbol_counts[symbol] = len(candles)

    manifest = new_manifest(
        run_id=f"structure_snapshots_{run_key}",
        source_context={
            "mode": "structure_snapshot_baseline",
            "settings_path": str(settings_path),
            "source_database_path": str(db_path),
            "symbols": symbols,
            "output_format": args.output_format,
            "max_candles": args.max_candles,
            "dry_run": bool(args.dry_run),
            "columns": SNAPSHOT_COLUMNS,
            "read_only": True,
            "writes_strategy_validation_db": False,
        },
    )

    result = {
        "run_key": run_key,
        "symbols": symbols,
        "symbol_candle_counts": symbol_counts,
        "row_count": len(rows),
        "output_format": args.output_format,
        "output_path": str(output_path),
        "manifest_path": str(manifest_path),
        "dry_run": bool(args.dry_run),
    }

    if not args.dry_run:
        if args.output_format == "jsonl":
            _write_jsonl(output_path, rows)
        else:
            _write_csv(output_path, rows)

        manifest.artifacts.append(
            DatasetArtifact(
                stage="structure_validation",
                artifact_type=f"structure_snapshots_{args.output_format}",
                relative_path=str(output_path.relative_to(paths.data_root)),
                row_count=len(rows),
                notes="baseline_structure_snapshots",
            )
        )
        write_manifest(manifest_path, manifest)

    return result


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = run_export(args)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
