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

    for symbol in symbols:
        profile = _profile_for_symbol(settings_payload, symbol)
        engine = PnFEngine(profile)
        closed_candles = _load_closed_candles_read_only(db_path, symbol, max_candles)

        symbol_rows = 0
        for idx, candle in enumerate(closed_candles, start=1):
            close_ts = int(candle["close_time"])
            close_price = float(candle["close"])
            engine.update_from_price(close_ts, close_price)

            old_state = build_structure_state(
                symbol=symbol,
                profile=profile,
                columns=engine.columns,
                latest_signal_name=engine.latest_signal_name(),
                market_state=engine.market_state(),
                last_price=getattr(engine, "last_price", None),
            )

            # Placeholder until incremental implementation is available.
            new_state = old_state

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

            symbol_rows += 1
            total_rows += 1

        rows_per_symbol[symbol] = symbol_rows

    mismatch_count = len(mismatches)
    mismatch_rate = (float(mismatch_count) / float(total_rows)) if total_rows else 0.0

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
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    summary = compare_structure_states(args)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
