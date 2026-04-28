"""Dual-run structure state equivalence harness.

Usage:
    python -m research_v2.structure_validation.compare_structure_states \
        --settings-path pnf_mvp/settings.research_clean.json \
        --symbols BINANCE_FUT:BTCUSDT,BINANCE_FUT:ETHUSDT \
        --max-candles 5000
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data" / "research"
DEFAULT_DIFF_DIR = DATA_ROOT / "structure_diffs"
PNF_MVP_DIR = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_DIR) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_DIR))

from pnf_engine import PnFEngine, PnFProfile  # noqa: E402
from structure_engine import build_structure_state  # noqa: E402

from research_v2.structure_validation.incremental_structure_state import (  # noqa: E402
    IncrementalStructureState,
)


_MISSING = object()
_FLOAT_PRECISION = 10


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


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return round(value, _FLOAT_PRECISION)
    return value


def _normalize_single(value: Any) -> Any:
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key in sorted(value.keys()):
            normalized[str(key)] = _normalize_single(value[key])
        return normalized

    if isinstance(value, (list, tuple)):
        return [_normalize_single(item) for item in value]

    return _normalize_scalar(value)


def _normalize_pair(old_value: Any, new_value: Any) -> tuple[Any, Any]:
    if old_value is _MISSING:
        old_value = None
    if new_value is _MISSING:
        new_value = None

    if isinstance(old_value, dict) or isinstance(new_value, dict):
        old_dict = old_value if isinstance(old_value, dict) else {}
        new_dict = new_value if isinstance(new_value, dict) else {}
        keys = sorted(set(old_dict.keys()) | set(new_dict.keys()))

        normalized_old: dict[str, Any] = {}
        normalized_new: dict[str, Any] = {}
        for key in keys:
            old_child = old_dict.get(key, _MISSING)
            new_child = new_dict.get(key, _MISSING)
            norm_old_child, norm_new_child = _normalize_pair(old_child, new_child)
            normalized_old[str(key)] = norm_old_child
            normalized_new[str(key)] = norm_new_child
        return normalized_old, normalized_new

    if isinstance(old_value, (list, tuple)) or isinstance(new_value, (list, tuple)):
        old_list = old_value if isinstance(old_value, (list, tuple)) else []
        new_list = new_value if isinstance(new_value, (list, tuple)) else []
        return _normalize_single(list(old_list)), _normalize_single(list(new_list))

    return _normalize_scalar(old_value), _normalize_scalar(new_value)


def _top_level_diff_keys(old_state: dict[str, Any], new_state: dict[str, Any]) -> list[str]:
    diff_keys: list[str] = []
    for key in sorted(set(old_state.keys()) | set(new_state.keys())):
        if old_state.get(key) != new_state.get(key):
            diff_keys.append(str(key))
    return diff_keys


def compare_structure_states(args: argparse.Namespace) -> dict[str, Any]:
    settings_path = Path(args.settings_path).resolve()
    settings_payload = _load_settings(settings_path)
    db_path = _resolve_db_path(settings_path, settings_payload)

    if not db_path.exists():
        raise FileNotFoundError(f"candles DB not found: {db_path}")

    symbols = _parse_symbols(settings_payload, args.symbols)
    max_candles = int(args.max_candles) if args.max_candles is not None else None
    if max_candles is not None and max_candles <= 0:
        raise ValueError("--max-candles must be > 0 when provided")

    mismatches: list[dict[str, Any]] = []
    rows_per_symbol: dict[str, int] = {}
    total_rows = 0

    mode = str(args.mode)
    implementation_status: dict[str, Any] = {}
    timing_totals_ns: dict[str, int] = {
        "legacy_state_build": 0,
        "incremental_update": 0,
        "incremental_snapshot": 0,
        "incremental_update_and_snapshot": 0,
        "normalize_and_diff": 0,
        "candle_loop_total": 0,
    }
    timing_by_symbol_ns: dict[str, dict[str, int]] = {}

    for symbol in symbols:
        profile = _profile_for_symbol(settings_payload, symbol)
        engine = PnFEngine(profile)
        incremental_state = (
            IncrementalStructureState(symbol=symbol, profile=profile) if mode == "incremental" else None
        )
        closed_candles = _load_closed_candles_read_only(db_path, symbol, max_candles)

        symbol_rows = 0
        symbol_timing_ns = {
            "legacy_state_build": 0,
            "incremental_update": 0,
            "incremental_snapshot": 0,
            "incremental_update_and_snapshot": 0,
            "normalize_and_diff": 0,
            "candle_loop_total": 0,
        }
        for idx, candle in enumerate(closed_candles, start=1):
            candle_started_ns = time.perf_counter_ns()
            close_ts = int(candle["close_time"])
            close_price = float(candle["close"])
            engine.update_from_price(close_ts, close_price)

            legacy_started_ns = time.perf_counter_ns()
            old_state = build_structure_state(
                symbol=symbol,
                profile=profile,
                columns=engine.columns,
                latest_signal_name=engine.latest_signal_name(),
                market_state=engine.market_state(),
                last_price=getattr(engine, "last_price", None),
            )
            legacy_elapsed_ns = time.perf_counter_ns() - legacy_started_ns
            symbol_timing_ns["legacy_state_build"] += legacy_elapsed_ns
            timing_totals_ns["legacy_state_build"] += legacy_elapsed_ns

            if mode == "placeholder":
                new_state = old_state
            else:
                assert incremental_state is not None
                incremental_update_started_ns = time.perf_counter_ns()
                incremental_state.update_from_engine(
                    engine=engine,
                    latest_signal_name=engine.latest_signal_name(),
                    market_state=engine.market_state(),
                    last_price=getattr(engine, "last_price", None),
                )
                incremental_update_elapsed_ns = time.perf_counter_ns() - incremental_update_started_ns
                symbol_timing_ns["incremental_update"] += incremental_update_elapsed_ns
                timing_totals_ns["incremental_update"] += incremental_update_elapsed_ns

                incremental_snapshot_started_ns = time.perf_counter_ns()
                new_state = incremental_state.snapshot(engine=engine)
                incremental_snapshot_elapsed_ns = time.perf_counter_ns() - incremental_snapshot_started_ns
                symbol_timing_ns["incremental_snapshot"] += incremental_snapshot_elapsed_ns
                timing_totals_ns["incremental_snapshot"] += incremental_snapshot_elapsed_ns

                incremental_elapsed_ns = incremental_update_elapsed_ns + incremental_snapshot_elapsed_ns
                symbol_timing_ns["incremental_update_and_snapshot"] += incremental_elapsed_ns
                timing_totals_ns["incremental_update_and_snapshot"] += incremental_elapsed_ns
                implementation_status[symbol] = incremental_state.implementation_status()

            normalize_started_ns = time.perf_counter_ns()
            normalized_old, normalized_new = _normalize_pair(old_state, new_state)
            if normalized_old != normalized_new:
                assert isinstance(normalized_old, dict)
                assert isinstance(normalized_new, dict)
                mismatches.append(
                    {
                        "symbol": symbol,
                        "candle_index": idx,
                        "differing_keys": _top_level_diff_keys(normalized_old, normalized_new),
                        "old": normalized_old,
                        "new": normalized_new,
                    }
                )
            normalize_elapsed_ns = time.perf_counter_ns() - normalize_started_ns
            symbol_timing_ns["normalize_and_diff"] += normalize_elapsed_ns
            timing_totals_ns["normalize_and_diff"] += normalize_elapsed_ns

            candle_elapsed_ns = time.perf_counter_ns() - candle_started_ns
            symbol_timing_ns["candle_loop_total"] += candle_elapsed_ns
            timing_totals_ns["candle_loop_total"] += candle_elapsed_ns

            symbol_rows += 1
            total_rows += 1

        rows_per_symbol[symbol] = symbol_rows
        timing_by_symbol_ns[symbol] = symbol_timing_ns

    mismatch_count = len(mismatches)
    mismatch_rate = (float(mismatch_count) / float(total_rows)) if total_rows else 0.0
    total_loop_seconds = float(timing_totals_ns["candle_loop_total"]) / 1_000_000_000.0
    legacy_build_us_per_row = (
        float(timing_totals_ns["legacy_state_build"]) / float(total_rows) / 1_000.0 if total_rows else 0.0
    )
    incremental_update_us_per_row = (
        float(timing_totals_ns["incremental_update"]) / float(total_rows) / 1_000.0 if total_rows else 0.0
    )
    incremental_snapshot_us_per_row = (
        float(timing_totals_ns["incremental_snapshot"]) / float(total_rows) / 1_000.0 if total_rows else 0.0
    )
    rows_per_second_total = (float(total_rows) / total_loop_seconds) if total_loop_seconds > 0.0 else 0.0

    mismatch_log_path: Path | None = None
    if args.mismatch_log:
        mismatch_log_path = Path(args.mismatch_log).resolve()
        mismatch_log_path.parent.mkdir(parents=True, exist_ok=True)
        with mismatch_log_path.open("w", encoding="utf-8") as handle:
            for item in mismatches:
                handle.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")

    return {
        "settings_path": str(settings_path),
        "database_path": str(db_path),
        "symbols": symbols,
        "max_candles": max_candles,
        "rows_per_symbol": rows_per_symbol,
        "total_rows": total_rows,
        "mismatches": mismatch_count,
        "mismatch_rate": round(mismatch_rate, _FLOAT_PRECISION),
        "mismatch_log_path": str(mismatch_log_path) if mismatch_log_path else None,
        "deterministic": True,
        "db_mode": "read_only",
        "mode": mode,
        "timing_totals_ns": timing_totals_ns,
        "timing_totals_ms": {key: round(value / 1_000_000.0, 6) for key, value in timing_totals_ns.items()},
        "incremental_update_s": round(float(timing_totals_ns["incremental_update"]) / 1_000_000_000.0, 6),
        "incremental_snapshot_s": round(float(timing_totals_ns["incremental_snapshot"]) / 1_000_000_000.0, 6),
        "legacy_build_us_per_row": round(legacy_build_us_per_row, 6),
        "incremental_update_us_per_row": round(incremental_update_us_per_row, 6),
        "incremental_snapshot_us_per_row": round(incremental_snapshot_us_per_row, 6),
        "rows_per_second_total": round(rows_per_second_total, 6),
        "timing_by_symbol_ns": timing_by_symbol_ns,
        "timing_by_symbol_ms": {
            symbol: {key: round(value / 1_000_000.0, 6) for key, value in symbol_timings.items()}
            for symbol, symbol_timings in timing_by_symbol_ns.items()
        },
        "implementation_status": implementation_status if mode == "incremental" else {
            "snapshot_strategy": "placeholder_equals_legacy",
            "delegated_fields": "all",
        },
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare legacy and incremental structure state builders on candle replay")
    parser.add_argument("--settings-path", required=True, help="Path to settings JSON containing database_path/symbols/profiles")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbol list to replay (must exist in settings)")
    parser.add_argument("--max-candles", type=int, default=None, help="Optional max closed candles to replay per symbol")
    parser.add_argument(
        "--mismatch-log",
        default=None,
        help="Optional JSONL output path for mismatch rows; if omitted, no mismatch file is written",
    )
    parser.add_argument(
        "--mode",
        choices=["placeholder", "incremental"],
        default="placeholder",
        help="Comparison mode: placeholder mirrors legacy; incremental uses IncrementalStructureState prototype",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    summary = compare_structure_states(args)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
