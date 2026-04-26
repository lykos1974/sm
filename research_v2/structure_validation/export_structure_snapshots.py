"""CLI replay exporter for per-candle structure snapshots.

Usage:
    python -m research_v2.structure_validation.export_structure_snapshots \
        --settings-path pnf_mvp/settings.research_clean.json \
        --symbols BINANCE_FUT:BTCUSDT,BINANCE_FUT:ETHUSDT \
        --output-format jsonl \
        --max-candles 5000
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data" / "research"
DEFAULT_EXPORT_DIR = DATA_ROOT / "structure_snapshots"
DEFAULT_MANIFEST_DIR = DATA_ROOT / "manifests"
PNF_MVP_DIR = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_DIR) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_DIR))

from pnf_engine import PnFEngine, PnFProfile  # noqa: E402
from structure_engine import build_structure_state  # noqa: E402


CSV_FIELDS = [
    "symbol",
    "candle_index",
    "close_time",
    "close",
    "high",
    "low",
    "column_count",
    "latest_signal_name",
    "market_state",
    "trend_state",
    "trend_regime",
    "immediate_slope",
    "swing_direction",
    "breakout_context",
    "is_extended_move",
    "active_leg_boxes",
    "support_level",
    "resistance_level",
    "last_meaningful_x_high",
    "last_meaningful_o_low",
    "current_column_kind",
    "current_column_top",
    "current_column_bottom",
    "impulse_boxes",
    "pullback_boxes",
    "impulse_to_pullback_ratio",
    "notes_json",
    "notes_hash",
]


def _load_settings(settings_path: Path) -> dict[str, Any]:
    with settings_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _resolve_db_path(settings_path: Path, settings_payload: dict[str, Any]) -> Path:
    raw_db_path = settings_payload.get("database_path")
    if not raw_db_path:
        raise ValueError("settings file must include 'database_path'")

    db_path = Path(str(raw_db_path))
    if not db_path.is_absolute():
        db_path = (settings_path.parent / db_path).resolve()

    return db_path


def _parse_symbols(settings_payload: dict[str, Any], symbols_raw: str) -> list[str]:
    settings_symbols = list(settings_payload.get("symbols") or [])
    requested = [token.strip() for token in symbols_raw.split(",") if token.strip()]
    if not requested:
        raise ValueError("--symbols must contain at least one symbol")

    unknown = [symbol for symbol in requested if symbol not in settings_symbols]
    if unknown:
        raise ValueError(f"symbols not found in settings: {unknown}")

    return requested


def _profile_for_symbol(settings_payload: dict[str, Any], symbol: str) -> PnFProfile:
    profile_payload = (settings_payload.get("profiles") or {}).get(symbol)
    if not profile_payload:
        raise ValueError(f"missing profile settings for symbol: {symbol}")

    return PnFProfile(
        name=symbol,
        box_size=float(profile_payload["box_size"]),
        reversal_boxes=int(profile_payload["reversal_boxes"]),
    )


def _load_closed_candles_read_only(db_path: Path, symbol: str, max_candles: int | None) -> list[dict[str, Any]]:
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT close_time, close, high, low
            FROM candles
            WHERE symbol=? AND interval='1m'
            ORDER BY open_time ASC
            """,
            (symbol,),
        ).fetchall()
    finally:
        conn.close()

    candles = [dict(row) for row in rows]
    closed_candles = candles[:-1] if len(candles) > 1 else []
    if max_candles is not None:
        return closed_candles[:max_candles]
    return closed_candles


def _snapshot_row(
    *,
    symbol: str,
    candle_index: int,
    candle: dict[str, Any],
    engine: PnFEngine,
    structure_state: dict[str, Any],
) -> dict[str, Any]:
    notes_json = json.dumps(structure_state.get("notes") or [], ensure_ascii=False)
    notes_hash = hashlib.sha256(notes_json.encode("utf-8")).hexdigest()

    return {
        "symbol": symbol,
        "candle_index": candle_index,
        "close_time": int(candle["close_time"]),
        "close": float(candle["close"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "column_count": len(engine.columns),
        "latest_signal_name": engine.latest_signal_name(),
        "market_state": engine.market_state(),
        "trend_state": structure_state.get("trend_state"),
        "trend_regime": structure_state.get("trend_regime"),
        "immediate_slope": structure_state.get("immediate_slope"),
        "swing_direction": structure_state.get("swing_direction"),
        "breakout_context": structure_state.get("breakout_context"),
        "is_extended_move": structure_state.get("is_extended_move"),
        "active_leg_boxes": structure_state.get("active_leg_boxes"),
        "support_level": structure_state.get("support_level"),
        "resistance_level": structure_state.get("resistance_level"),
        "last_meaningful_x_high": structure_state.get("last_meaningful_x_high"),
        "last_meaningful_o_low": structure_state.get("last_meaningful_o_low"),
        "current_column_kind": structure_state.get("current_column_kind"),
        "current_column_top": structure_state.get("current_column_top"),
        "current_column_bottom": structure_state.get("current_column_bottom"),
        "impulse_boxes": structure_state.get("impulse_boxes"),
        "pullback_boxes": structure_state.get("pullback_boxes"),
        "impulse_to_pullback_ratio": structure_state.get("impulse_to_pullback_ratio"),
        "notes_json": notes_json,
        "notes_hash": notes_hash,
    }


def _build_manifest(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_type": "structure_snapshots_export",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "settings_path": summary["settings_path"],
        "database_path": summary["database_path"],
        "symbols": summary["symbols"],
        "output_format": summary["output_format"],
        "output_path": summary["output_path"],
        "dry_run": summary["dry_run"],
        "max_candles": summary["max_candles"],
        "rows_per_symbol": summary["rows_per_symbol"],
        "total_rows": summary["total_rows"],
    }


def export_structure_snapshots(args: argparse.Namespace) -> dict[str, Any]:
    settings_path = Path(args.settings_path).resolve()
    settings_payload = _load_settings(settings_path)
    db_path = _resolve_db_path(settings_path, settings_payload)

    if not db_path.exists():
        raise FileNotFoundError(f"candles DB not found: {db_path}")

    symbols = _parse_symbols(settings_payload, args.symbols)
    max_candles = int(args.max_candles) if args.max_candles is not None else None
    if max_candles is not None and max_candles <= 0:
        raise ValueError("--max-candles must be > 0 when provided")

    output_format = str(args.output_format).lower().strip()
    if output_format not in {"jsonl", "csv"}:
        raise ValueError("--output-format must be one of: jsonl,csv")

    default_output_path = DEFAULT_EXPORT_DIR / f"structure_snapshots.{output_format}"
    output_path = Path(args.output_path).resolve() if args.output_path else default_output_path
    manifest_path = DEFAULT_MANIFEST_DIR / "structure_snapshots_manifest.json"

    total_rows = 0
    rows_per_symbol: dict[str, int] = {}

    writer_csv = None
    handle = None
    try:
        if not args.dry_run:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            handle = output_path.open("w", encoding="utf-8", newline="" if output_format == "csv" else None)
            if output_format == "csv":
                writer_csv = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
                writer_csv.writeheader()

        for symbol in symbols:
            profile = _profile_for_symbol(settings_payload, symbol)
            engine = PnFEngine(profile)

            closed_candles = _load_closed_candles_read_only(db_path, symbol, max_candles)

            symbol_rows = 0
            for idx, candle in enumerate(closed_candles, start=1):
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

                row = _snapshot_row(
                    symbol=symbol,
                    candle_index=idx,
                    candle=candle,
                    engine=engine,
                    structure_state=structure_state,
                )

                if not args.dry_run:
                    if output_format == "jsonl":
                        assert handle is not None
                        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
                    else:
                        assert writer_csv is not None
                        writer_csv.writerow(row)

                symbol_rows += 1
                total_rows += 1

            rows_per_symbol[symbol] = symbol_rows

    finally:
        if handle is not None:
            handle.close()

    summary = {
        "settings_path": str(settings_path),
        "database_path": str(db_path),
        "symbols": symbols,
        "output_format": output_format,
        "output_path": str(output_path.resolve()),
        "manifest_path": str(manifest_path.resolve()),
        "dry_run": bool(args.dry_run),
        "max_candles": max_candles,
        "rows_per_symbol": rows_per_symbol,
        "total_rows": total_rows,
    }

    if not args.dry_run:
        DEFAULT_MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
        manifest_payload = _build_manifest(summary)
        with manifest_path.open("w", encoding="utf-8") as handle_manifest:
            json.dump(manifest_payload, handle_manifest, indent=2, sort_keys=True)
            handle_manifest.write("\n")

    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay candles and export per-candle structure snapshots")
    parser.add_argument("--settings-path", required=True, help="Path to settings JSON containing database_path/symbols/profiles")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbol list to replay (must exist in settings)")
    parser.add_argument("--output-format", required=True, choices=["jsonl", "csv"], help="Snapshot export format")
    parser.add_argument("--max-candles", type=int, default=None, help="Optional max closed candles to replay per symbol")
    parser.add_argument("--dry-run", action="store_true", help="Replay and summarize without writing output files")
    parser.add_argument("--output-path", default=None, help="Optional explicit output file path")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    summary = export_structure_snapshots(args)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
