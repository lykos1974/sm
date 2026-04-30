# strategy_historical_backfill.py (BE-ready integration scaffold + funnel diagnostics)

# NOTE:
# This version prepares BE integration but does NOT alter resolution logic inside StrategyValidationStore.
# To fully enable BE, StrategyValidationStore must be extended.
# This file keeps baseline setup logic unchanged and adds research diagnostics only.

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

from pnf_engine import PnFProfile, PnFEngine
from storage import Storage
from structure_engine import build_structure_state
from strategy_engine import evaluate_pullback_retest_long, evaluate_pullback_retest_short
from strategy_validation import StrategyValidationStore

# BE module (ready for future integration)
from trade_management_be import BE_MODE, BE_TRIGGER_R

if TYPE_CHECKING:
    from research_v2.structure_validation.incremental_structure_state import IncrementalStructureState

VALIDATION_ELIGIBLE_STATUSES = {"CANDIDATE", "WATCH"}
OPTIONAL_DIAGNOSTIC_SETUP_FIELDS = {
    "continuation_strength_v1",
    "cs_geometry_component",
    "cs_profile_tag",
}
DEFAULT_FUNNEL_CSV_PATH = "exports/strategy_funnel_diagnostics.csv"
DEFAULT_PERF_JSON_PATH = "exports/strategy_perf_summary.json"


FUNNEL_FIELD_ORDER = [
    "symbol",
    "reference_ts",
    "side",
    "status",
    "strategy",
    "reason",
    "reject_reason",
    "quality_score",
    "quality_grade",
    "trend_state",
    "trend_regime",
    "immediate_slope",
    "breakout_context",
    "market_state",
    "latest_signal_name",
    "is_extended_move",
    "active_leg_boxes",
    "zone_low",
    "zone_high",
    "ideal_entry",
    "invalidation",
    "risk",
    "tp1",
    "tp2",
    "rr1",
    "rr2",
    "blocked_by_existing_open_trade",
    "registered_to_validation",
]


def load_settings(settings_path: str) -> dict:
    with open(settings_path, "r", encoding="utf-8") as f:
        return json.load(f)



def build_profiles(settings: dict) -> Dict[str, PnFProfile]:
    profiles: Dict[str, PnFProfile] = {}
    for symbol in settings["symbols"]:
        p = settings["profiles"][symbol]
        profiles[symbol] = PnFProfile(
            name=symbol,
            box_size=float(p["box_size"]),
            reversal_boxes=int(p["reversal_boxes"]),
        )
    return profiles



def split_symbols(settings: dict, symbols_arg: str | None) -> List[str]:
    if not symbols_arg:
        return list(settings["symbols"])
    wanted = [s.strip() for s in symbols_arg.split(",") if s.strip()]
    return [s for s in settings["symbols"] if s in wanted]



def load_all_closed_candles(storage: Storage, symbol: str) -> List[dict]:
    candles = storage.load_recent_candles(symbol, None)
    return candles[:-1] if len(candles) > 1 else []


def _shadow_normalize_structure(value: Any) -> Any:
    if isinstance(value, dict):
        normalized: Dict[str, Any] = {}
        for key, nested_value in value.items():
            if key == "notes":
                continue
            normalized[key] = _shadow_normalize_structure(nested_value)
        return normalized
    if isinstance(value, list):
        return [_shadow_normalize_structure(item) for item in value]
    return value


def _build_shadow_mismatch_details(legacy_structure: dict, incremental_structure: dict) -> Dict[str, Any]:
    legacy_keys = set(legacy_structure.keys())
    incremental_keys = set(incremental_structure.keys())
    return {
        "missing_in_incremental": sorted(legacy_keys - incremental_keys),
        "extra_in_incremental": sorted(incremental_keys - legacy_keys),
        "value_differences": sorted(
            key
            for key in (legacy_keys & incremental_keys)
            if legacy_structure.get(key) != incremental_structure.get(key)
        ),
    }


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _values_equal_with_tolerance(legacy_value: Any, shadow_value: Any, tolerance: float = 1e-9) -> bool:
    if _is_numeric(legacy_value) and _is_numeric(shadow_value):
        legacy_float = float(legacy_value)
        shadow_float = float(shadow_value)
        if math.isnan(legacy_float) and math.isnan(shadow_float):
            return True
        return abs(legacy_float - shadow_float) <= tolerance
    return legacy_value == shadow_value


def _compare_strategy_results(legacy_result: Any, shadow_result: Any) -> Dict[str, Any]:
    if legacy_result is None and shadow_result is None:
        return {
            "is_mismatch": False,
            "differing_fields": [],
            "status_mismatch": False,
            "registration_impact_mismatch": False,
        }

    if (legacy_result is None) != (shadow_result is None):
        legacy_status = str((legacy_result or {}).get("status") or "").upper()
        shadow_status = str((shadow_result or {}).get("status") or "").upper()
        return {
            "is_mismatch": True,
            "differing_fields": ["__presence__"],
            "status_mismatch": legacy_status != shadow_status,
            "registration_impact_mismatch": (
                (legacy_status in VALIDATION_ELIGIBLE_STATUSES)
                != (shadow_status in VALIDATION_ELIGIBLE_STATUSES)
            ),
        }

    legacy_map = dict(legacy_result)
    shadow_map = dict(shadow_result)
    keys = sorted(set(legacy_map.keys()) | set(shadow_map.keys()))
    differing_fields: List[str] = []
    for key in keys:
        legacy_has = key in legacy_map
        shadow_has = key in shadow_map
        legacy_value = legacy_map.get(key)
        shadow_value = shadow_map.get(key)

        if legacy_has and shadow_has and _values_equal_with_tolerance(legacy_value, shadow_value):
            continue

        if key in OPTIONAL_DIAGNOSTIC_SETUP_FIELDS and (
            (not legacy_has and shadow_has and shadow_value is None)
            or (legacy_has and not shadow_has and legacy_value is None)
        ):
            continue

        differing_fields.append(key)

    legacy_status = str(legacy_map.get("status") or "").upper()
    shadow_status = str(shadow_map.get("status") or "").upper()
    return {
        "is_mismatch": bool(differing_fields),
        "differing_fields": differing_fields,
        "status_mismatch": legacy_status != shadow_status,
        "registration_impact_mismatch": (
            (legacy_status in VALIDATION_ELIGIBLE_STATUSES) != (shadow_status in VALIDATION_ELIGIBLE_STATUSES)
        ),
    }


def evaluate_setups(
    symbol: str, profile: PnFProfile, engine: PnFEngine
) -> Tuple[dict, List[dict], Dict[str, float], Dict[str, Any]]:
    t_start = time.perf_counter()
    t0 = time.perf_counter()
    structure = build_structure_state(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        latest_signal_name=engine.latest_signal_name(),
        market_state=engine.market_state(),
        last_price=getattr(engine, "last_price", None),
    )
    elapsed_build_structure_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    setup_long = evaluate_pullback_retest_long(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )
    elapsed_eval_long_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    setup_short = evaluate_pullback_retest_short(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )
    elapsed_eval_short_s = time.perf_counter() - t0

    setups = [s for s in (setup_long, setup_short) if s]
    elapsed_total_s = time.perf_counter() - t_start
    timings = {
        "elapsed_build_structure_s": elapsed_build_structure_s,
        "elapsed_eval_long_s": elapsed_eval_long_s,
        "elapsed_eval_short_s": elapsed_eval_short_s,
        "elapsed_total_s": elapsed_total_s,
    }
    return structure, setups, timings, {"LONG": setup_long, "SHORT": setup_short}



def reset_validation_db(validation_db_path: str) -> None:
    base = Path(validation_db_path)
    for suffix in ("", "-shm", "-wal"):
        p = Path(str(base) + suffix)
        if p.exists():
            p.unlink()



def table_row_count(db_path: str, table_name: str = "strategy_setups") -> int:
    if not Path(db_path).exists():
        return 0
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if row is None:
            return 0
        return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    finally:
        conn.close()



def _coerce_bool_int(value: Any) -> int:
    return 1 if bool(value) else 0



def build_funnel_row(
    *,
    symbol: str,
    reference_ts: int,
    setup: Dict[str, Any],
    structure: Dict[str, Any],
    blocked_by_existing_open_trade: bool,
    registered_to_validation: bool,
) -> Dict[str, Any]:
    row = {
        "symbol": symbol,
        "reference_ts": int(reference_ts),
        "side": setup.get("side"),
        "status": setup.get("status"),
        "strategy": setup.get("strategy"),
        "reason": setup.get("reason"),
        "reject_reason": setup.get("reject_reason"),
        "quality_score": setup.get("quality_score"),
        "quality_grade": setup.get("quality_grade"),
        "trend_state": structure.get("trend_state"),
        "trend_regime": structure.get("trend_regime"),
        "immediate_slope": structure.get("immediate_slope"),
        "breakout_context": structure.get("breakout_context"),
        "market_state": structure.get("market_state"),
        "latest_signal_name": structure.get("latest_signal_name"),
        "is_extended_move": _coerce_bool_int(structure.get("is_extended_move", False)),
        "active_leg_boxes": structure.get("active_leg_boxes"),
        "zone_low": setup.get("zone_low"),
        "zone_high": setup.get("zone_high"),
        "ideal_entry": setup.get("ideal_entry"),
        "invalidation": setup.get("invalidation"),
        "risk": setup.get("risk"),
        "tp1": setup.get("tp1"),
        "tp2": setup.get("tp2"),
        "rr1": setup.get("rr1"),
        "rr2": setup.get("rr2"),
        "blocked_by_existing_open_trade": 1 if blocked_by_existing_open_trade else 0,
        "registered_to_validation": 1 if registered_to_validation else 0,
    }
    return row



def write_funnel_csv(rows: List[Dict[str, Any]], csv_path: str) -> str:
    out_path = Path(csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FUNNEL_FIELD_ORDER, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return str(out_path.resolve())



def status_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "evaluated_total": len(rows),
        "rejected_total": 0,
        "watch_total": 0,
        "candidate_total": 0,
        "registered_total": 0,
        "blocked_by_open_trade_total": 0,
    }
    for row in rows:
        status = str(row.get("status") or "").upper()
        if status == "REJECT":
            counts["rejected_total"] += 1
        elif status == "WATCH":
            counts["watch_total"] += 1
        elif status == "CANDIDATE":
            counts["candidate_total"] += 1
        if int(row.get("registered_to_validation") or 0) == 1:
            counts["registered_total"] += 1
        if int(row.get("blocked_by_existing_open_trade") or 0) == 1:
            counts["blocked_by_open_trade_total"] += 1
    return counts



def main() -> None:
    parser = argparse.ArgumentParser(description="Historical validation backfill for strategy_validation.db")
    parser.add_argument("--settings", default="settings.json")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--reset-validation-db", action="store_true")
    parser.add_argument(
        "--funnel-csv",
        default=DEFAULT_FUNNEL_CSV_PATH,
        help="CSV path for evaluated-setup funnel diagnostics export",
    )
    parser.add_argument(
        "--perf-json",
        default=DEFAULT_PERF_JSON_PATH,
        help="JSON path for performance instrumentation summary export",
    )
    parser.add_argument(
        "--perf-progress-every",
        type=int,
        default=1000,
        help="Emit perf progress log every N candles per symbol",
    )
    parser.add_argument(
        "--incremental-shadow-structure",
        action="store_true",
        help="Run incremental structure shadow comparison without changing strategy behavior",
    )
    parser.add_argument(
        "--use-incremental-structure",
        action="store_true",
        help="Use incremental structure snapshot as authoritative strategy structure with mandatory legacy guard comparisons",
    )
    parser.add_argument(
        "--use-incremental-structure-fast",
        action="store_true",
        help="Use incremental structure snapshot as authoritative strategy structure without per-candle legacy guard comparisons",
    )
    args = parser.parse_args()
    if args.use_incremental_structure and args.use_incremental_structure_fast:
        parser.error(
            "--use-incremental-structure and --use-incremental-structure-fast are mutually exclusive; choose one mode"
        )
    use_incremental_fast = bool(args.use_incremental_structure_fast)
    use_incremental_authoritative = bool(args.use_incremental_structure)
    use_incremental_shadow = bool(args.incremental_shadow_structure or use_incremental_authoritative)
    if use_incremental_fast:
        use_incremental_shadow = False

    settings = load_settings(args.settings)
    validation_db_path = settings.get("strategy_validation_db_path", "strategy_validation.db")

    if args.reset_validation_db:
        reset_validation_db(validation_db_path)

    storage = Storage(settings["database_path"])
    validation_store = StrategyValidationStore(validation_db_path)
    profiles = build_profiles(settings)
    symbols = split_symbols(settings, args.symbols)

    print(f"BE MODE: {BE_MODE} | BE_TRIGGER_R: {BE_TRIGGER_R}")

    funnel_rows: List[Dict[str, Any]] = []
    run_perf: Dict[str, Any] = {
        "symbols": {},
        "totals": {
            "candles": 0,
            "elapsed_engine_update_s": 0.0,
            "elapsed_update_pending_s": 0.0,
            "elapsed_eval_s": 0.0,
            "elapsed_register_s": 0.0,
            "setups_evaluated": 0,
            "register_calls": 0,
            "elapsed_build_structure_s": 0.0,
            "elapsed_eval_long_s": 0.0,
            "elapsed_eval_short_s": 0.0,
            "structure_compare_s": 0.0,
            "shadow_strategy_eval_s": 0.0,
            "shadow_compare_s": 0.0,
            "funnel_row_build_s": 0.0,
            "funnel_csv_write_s": 0.0,
            "perf_json_write_s": 0.0,
            "structure_source": (
                "incremental_fast" if use_incremental_fast else ("incremental" if use_incremental_authoritative else "legacy")
            ),
        },
    }
    if use_incremental_shadow:
        run_perf["totals"].update(
            {
                "incremental_shadow_rows": 0,
                "incremental_shadow_mismatches": 0,
                "incremental_shadow_first_mismatch": None,
                "incremental_shadow_update_s": 0.0,
                "incremental_shadow_snapshot_s": 0.0,
                "strategy_shadow_rows": 0,
                "strategy_shadow_mismatches": 0,
                "strategy_shadow_first_mismatch": None,
                "strategy_shadow_status_mismatches": 0,
                "strategy_shadow_registration_impact_mismatches": 0,
            }
        )

    for symbol in symbols:
        profile = profiles[symbol]
        engine = PnFEngine(profile)
        candles = load_all_closed_candles(storage, symbol)
        symbol_perf = {
            "candles": 0,
            "elapsed_engine_update_s": 0.0,
            "elapsed_update_pending_s": 0.0,
            "elapsed_eval_s": 0.0,
            "elapsed_register_s": 0.0,
            "setups_evaluated": 0,
            "register_calls": 0,
            "elapsed_build_structure_s": 0.0,
            "elapsed_eval_long_s": 0.0,
            "elapsed_eval_short_s": 0.0,
            "structure_compare_s": 0.0,
            "shadow_strategy_eval_s": 0.0,
            "shadow_compare_s": 0.0,
            "funnel_row_build_s": 0.0,
        }
        if use_incremental_shadow:
            symbol_perf.update(
                {
                    "incremental_shadow_rows": 0,
                    "incremental_shadow_mismatches": 0,
                    "incremental_shadow_first_mismatch": None,
                    "incremental_shadow_update_s": 0.0,
                    "incremental_shadow_snapshot_s": 0.0,
                    "strategy_shadow_rows": 0,
                    "strategy_shadow_mismatches": 0,
                    "strategy_shadow_first_mismatch": None,
                    "strategy_shadow_status_mismatches": 0,
                    "strategy_shadow_registration_impact_mismatches": 0,
                }
            )
        symbol_perf["structure_source"] = (
            "incremental_fast" if use_incremental_fast else ("incremental" if use_incremental_authoritative else "legacy")
        )
        incremental_shadow_state: Any = None
        if use_incremental_shadow or use_incremental_fast:
            repo_root = Path(__file__).resolve().parents[1]
            if str(repo_root) not in sys.path:
                sys.path.insert(0, str(repo_root))
            from research_v2.structure_validation.incremental_structure_state import IncrementalStructureState

            incremental_shadow_state = IncrementalStructureState(symbol=symbol, profile=profile)
        progress_every = max(1, int(args.perf_progress_every))

        for i, candle in enumerate(candles, start=1):
            close_ts = int(candle["close_time"])
            close_price = float(candle["close"])
            high_price = float(candle["high"])
            low_price = float(candle["low"])

            t0 = time.perf_counter()
            engine.update_from_price(close_ts, close_price)
            symbol_perf["elapsed_engine_update_s"] += time.perf_counter() - t0

            # CURRENT resolution (unchanged)
            t0 = time.perf_counter()
            validation_store.update_pending_with_candle(
                symbol=symbol,
                close_ts=close_ts,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
            )
            symbol_perf["elapsed_update_pending_s"] += time.perf_counter() - t0

            legacy_structure = None
            legacy_setup_map: Dict[str, Any] = {}
            structure: Dict[str, Any] = {}
            setup_map: Dict[str, Any] = {}
            if use_incremental_fast:
                if incremental_shadow_state is None:
                    raise RuntimeError("incremental fast mode requires initialized incremental state")
                t_fast = time.perf_counter()
                incremental_shadow_state.update_from_engine(
                    engine=engine,
                    latest_signal_name=engine.latest_signal_name(),
                    market_state=engine.market_state(),
                    last_price=getattr(engine, "last_price", None),
                )
                symbol_perf["incremental_shadow_update_s"] = symbol_perf.get("incremental_shadow_update_s", 0.0) + (
                    time.perf_counter() - t_fast
                )
                t_fast = time.perf_counter()
                structure = incremental_shadow_state.snapshot_no_delegate()
                symbol_perf["incremental_shadow_snapshot_s"] = symbol_perf.get("incremental_shadow_snapshot_s", 0.0) + (
                    time.perf_counter() - t_fast
                )
                t_fast_eval = time.perf_counter()
                setup_map = {
                    "LONG": evaluate_pullback_retest_long(
                        symbol=symbol,
                        profile=profile,
                        columns=engine.columns,
                        structure_state=structure,
                    ),
                    "SHORT": evaluate_pullback_retest_short(
                        symbol=symbol,
                        profile=profile,
                        columns=engine.columns,
                        structure_state=structure,
                    ),
                }
                elapsed_fast_eval_s = time.perf_counter() - t_fast_eval
                symbol_perf["elapsed_eval_s"] += elapsed_fast_eval_s
                symbol_perf["elapsed_eval_long_s"] += elapsed_fast_eval_s / 2.0
                symbol_perf["elapsed_eval_short_s"] += elapsed_fast_eval_s / 2.0
            else:
                t0 = time.perf_counter()
                legacy_structure, _legacy_setups, eval_timings, legacy_setup_map = evaluate_setups(symbol, profile, engine)
                symbol_perf["elapsed_eval_s"] += time.perf_counter() - t0
                symbol_perf["elapsed_build_structure_s"] += float(eval_timings["elapsed_build_structure_s"])
                symbol_perf["elapsed_eval_long_s"] += float(eval_timings["elapsed_eval_long_s"])
                symbol_perf["elapsed_eval_short_s"] += float(eval_timings["elapsed_eval_short_s"])
                structure = legacy_structure
                setup_map = legacy_setup_map
            symbol_perf["candles"] += 1
            if use_incremental_authoritative and incremental_shadow_state is None:
                raise RuntimeError("incremental authoritative mode requires initialized incremental shadow state")
            if incremental_shadow_state is not None and not use_incremental_fast:
                t_shadow = time.perf_counter()
                incremental_shadow_state.update_from_engine(
                    engine=engine,
                    latest_signal_name=engine.latest_signal_name(),
                    market_state=engine.market_state(),
                    last_price=getattr(engine, "last_price", None),
                )
                symbol_perf["incremental_shadow_update_s"] += time.perf_counter() - t_shadow

                t_shadow = time.perf_counter()
                incremental_structure = incremental_shadow_state.snapshot_no_delegate()
                symbol_perf["incremental_shadow_snapshot_s"] += time.perf_counter() - t_shadow
                symbol_perf["incremental_shadow_rows"] += 1

                t_structure_compare = time.perf_counter()
                normalized_legacy = _shadow_normalize_structure(legacy_structure)
                normalized_incremental = _shadow_normalize_structure(incremental_structure)
                if normalized_legacy != normalized_incremental:
                    symbol_perf["incremental_shadow_mismatches"] += 1
                    if symbol_perf["incremental_shadow_first_mismatch"] is None:
                        symbol_perf["incremental_shadow_first_mismatch"] = {
                            "symbol": symbol,
                            "close_ts": close_ts,
                            "details": _build_shadow_mismatch_details(
                                normalized_legacy,
                                normalized_incremental,
                            ),
                        }
                symbol_perf["structure_compare_s"] += time.perf_counter() - t_structure_compare

                t_shadow_eval = time.perf_counter()
                shadow_setup_map = {
                    "LONG": evaluate_pullback_retest_long(
                        symbol=symbol,
                        profile=profile,
                        columns=engine.columns,
                        structure_state=incremental_structure,
                    ),
                    "SHORT": evaluate_pullback_retest_short(
                        symbol=symbol,
                        profile=profile,
                        columns=engine.columns,
                        structure_state=incremental_structure,
                    ),
                }
                symbol_perf["shadow_strategy_eval_s"] += time.perf_counter() - t_shadow_eval

                t_shadow_compare = time.perf_counter()
                for side in ("LONG", "SHORT"):
                    symbol_perf["strategy_shadow_rows"] += 1
                    comparison = _compare_strategy_results(
                        legacy_setup_map.get(side),
                        shadow_setup_map.get(side),
                    )
                    if comparison["status_mismatch"]:
                        symbol_perf["strategy_shadow_status_mismatches"] += 1
                    if comparison["registration_impact_mismatch"]:
                        symbol_perf["strategy_shadow_registration_impact_mismatches"] += 1
                    if comparison["is_mismatch"]:
                        symbol_perf["strategy_shadow_mismatches"] += 1
                        if symbol_perf["strategy_shadow_first_mismatch"] is None:
                            symbol_perf["strategy_shadow_first_mismatch"] = {
                                "symbol": symbol,
                                "close_ts": close_ts,
                                "candle_index": i,
                                "side": side,
                                "differing_fields": comparison["differing_fields"],
                                "legacy_result": legacy_setup_map.get(side),
                                "shadow_result": shadow_setup_map.get(side),
                            }
                    if use_incremental_authoritative and (
                        comparison["status_mismatch"] or comparison["registration_impact_mismatch"]
                    ):
                        print(
                            "[GUARD_FAIL] "
                            f"symbol={symbol} close_ts={close_ts} candle_index={i} side={side} "
                            f"status_mismatch={comparison['status_mismatch']} "
                            "registration_impact_mismatch="
                            f"{comparison['registration_impact_mismatch']} "
                            f"first_structure_mismatch={json.dumps(symbol_perf['incremental_shadow_first_mismatch'], sort_keys=True)} "
                            f"first_strategy_mismatch={json.dumps(symbol_perf['strategy_shadow_first_mismatch'], sort_keys=True)}"
                        )
                        raise RuntimeError(
                            "Incremental authoritative guard failure: strategy status/registration-impact mismatch detected"
                        )
                symbol_perf["shadow_compare_s"] += time.perf_counter() - t_shadow_compare
                if use_incremental_authoritative:
                    structure = incremental_structure
                    setup_map = shadow_setup_map

            setups = [s for s in (setup_map.get("LONG"), setup_map.get("SHORT")) if s]
            symbol_perf["setups_evaluated"] += len(setups)

            for setup in setups:
                status = str(setup.get("status") or "").upper()
                eligible_for_validation = status in VALIDATION_ELIGIBLE_STATUSES

                blocked_by_open_trade = False
                registered_to_validation = False

                if eligible_for_validation:
                    if (
                        not validation_store.allow_multiple_trades_per_symbol
                        and validation_store.has_open_trade_for_symbol(symbol)
                    ):
                        blocked_by_open_trade = True
                    else:
                        t0 = time.perf_counter()
                        setup_id = validation_store.register_setup(
                            symbol=symbol,
                            setup=setup,
                            structure_state=structure,
                            reference_ts=close_ts,
                        )
                        symbol_perf["elapsed_register_s"] += time.perf_counter() - t0
                        symbol_perf["register_calls"] += 1
                        registered_to_validation = setup_id is not None
                        if setup_id is None and not validation_store.allow_multiple_trades_per_symbol:
                            blocked_by_open_trade = True

                t_funnel_row = time.perf_counter()
                funnel_rows.append(
                    build_funnel_row(
                        symbol=symbol,
                        reference_ts=close_ts,
                        setup=setup,
                        structure=structure,
                        blocked_by_existing_open_trade=blocked_by_open_trade,
                        registered_to_validation=registered_to_validation,
                    )
                )
                symbol_perf["funnel_row_build_s"] += time.perf_counter() - t_funnel_row

            if i % progress_every == 0:
                perf_snapshot = validation_store.get_perf_snapshot()
                up = perf_snapshot["update_pending"].get(symbol, {})
                elapsed_update_pending_ms = symbol_perf["elapsed_update_pending_s"] * 1000.0
                elapsed_eval_ms = symbol_perf["elapsed_eval_s"] * 1000.0
                elapsed_register_ms = symbol_perf["elapsed_register_s"] * 1000.0
                print(
                    "[PERF] "
                    f"symbol={symbol} i={i} pending={up.get('current_pending_count', 0)} "
                    f"scanned={up.get('trades_scanned', 0)} updated={up.get('trades_updated', 0)} "
                    f"activated={up.get('trades_activated', 0)} resolved={up.get('trades_resolved', 0)} "
                    f"sql_updates={up.get('sql_update_count', 0)} sql_inserts={up.get('sql_insert_count', 0)} "
                    f"diag_sql_updates={up.get('update_pending_sql_updates_total', 0)} "
                    f"diag_event_updates={up.get('update_pending_event_updates', 0)} "
                    f"diag_progress_updates={up.get('update_pending_progress_updates', 0)} "
                    f"sql_selects={up.get('sql_select_count', 0)} "
                    f"elapsed_update_pending_ms={elapsed_update_pending_ms:.3f} "
                    f"elapsed_eval_ms={elapsed_eval_ms:.3f} "
                    f"elapsed_register_ms={elapsed_register_ms:.3f} "
                    f"elapsed_build_structure_ms={symbol_perf['elapsed_build_structure_s'] * 1000.0:.3f} "
                    f"elapsed_eval_long_ms={symbol_perf['elapsed_eval_long_s'] * 1000.0:.3f} "
                    f"elapsed_eval_short_ms={symbol_perf['elapsed_eval_short_s'] * 1000.0:.3f} "
                    f"funnel_row_build_ms={symbol_perf['funnel_row_build_s'] * 1000.0:.3f}"
                    + (
                        " "
                        f"incremental_shadow_rows={symbol_perf['incremental_shadow_rows']} "
                        f"incremental_shadow_mismatches={symbol_perf['incremental_shadow_mismatches']} "
                        f"structure_compare_ms={symbol_perf['structure_compare_s'] * 1000.0:.3f} "
                        f"shadow_strategy_eval_ms={symbol_perf['shadow_strategy_eval_s'] * 1000.0:.3f} "
                        f"shadow_compare_ms={symbol_perf['shadow_compare_s'] * 1000.0:.3f} "
                        f"strategy_shadow_rows={symbol_perf['strategy_shadow_rows']} "
                        f"strategy_shadow_mismatches={symbol_perf['strategy_shadow_mismatches']}"
                        if use_incremental_shadow
                        else ""
                    )
                )

        run_perf["symbols"][symbol] = symbol_perf
        run_perf["totals"]["candles"] += int(symbol_perf["candles"])
        run_perf["totals"]["elapsed_engine_update_s"] += float(symbol_perf["elapsed_engine_update_s"])
        run_perf["totals"]["elapsed_update_pending_s"] += float(symbol_perf["elapsed_update_pending_s"])
        run_perf["totals"]["elapsed_eval_s"] += float(symbol_perf["elapsed_eval_s"])
        run_perf["totals"]["elapsed_register_s"] += float(symbol_perf["elapsed_register_s"])
        run_perf["totals"]["setups_evaluated"] += int(symbol_perf["setups_evaluated"])
        run_perf["totals"]["register_calls"] += int(symbol_perf["register_calls"])
        run_perf["totals"]["elapsed_build_structure_s"] += float(symbol_perf["elapsed_build_structure_s"])
        run_perf["totals"]["elapsed_eval_long_s"] += float(symbol_perf["elapsed_eval_long_s"])
        run_perf["totals"]["elapsed_eval_short_s"] += float(symbol_perf["elapsed_eval_short_s"])
        run_perf["totals"]["structure_compare_s"] += float(symbol_perf["structure_compare_s"])
        run_perf["totals"]["shadow_strategy_eval_s"] += float(symbol_perf["shadow_strategy_eval_s"])
        run_perf["totals"]["shadow_compare_s"] += float(symbol_perf["shadow_compare_s"])
        run_perf["totals"]["funnel_row_build_s"] += float(symbol_perf["funnel_row_build_s"])
        if use_incremental_shadow:
            run_perf["totals"]["incremental_shadow_rows"] += int(symbol_perf["incremental_shadow_rows"])
            run_perf["totals"]["incremental_shadow_mismatches"] += int(symbol_perf["incremental_shadow_mismatches"])
            run_perf["totals"]["incremental_shadow_update_s"] += float(symbol_perf["incremental_shadow_update_s"])
            run_perf["totals"]["incremental_shadow_snapshot_s"] += float(symbol_perf["incremental_shadow_snapshot_s"])
            run_perf["totals"]["strategy_shadow_rows"] += int(symbol_perf["strategy_shadow_rows"])
            run_perf["totals"]["strategy_shadow_mismatches"] += int(symbol_perf["strategy_shadow_mismatches"])
            run_perf["totals"]["strategy_shadow_status_mismatches"] += int(
                symbol_perf["strategy_shadow_status_mismatches"]
            )
            run_perf["totals"]["strategy_shadow_registration_impact_mismatches"] += int(
                symbol_perf["strategy_shadow_registration_impact_mismatches"]
            )
            if (
                run_perf["totals"]["incremental_shadow_first_mismatch"] is None
                and symbol_perf["incremental_shadow_first_mismatch"] is not None
            ):
                run_perf["totals"]["incremental_shadow_first_mismatch"] = symbol_perf[
                    "incremental_shadow_first_mismatch"
                ]
            if (
                run_perf["totals"]["strategy_shadow_first_mismatch"] is None
                and symbol_perf["strategy_shadow_first_mismatch"] is not None
            ):
                run_perf["totals"]["strategy_shadow_first_mismatch"] = symbol_perf["strategy_shadow_first_mismatch"]

        perf_snapshot = validation_store.get_perf_snapshot()
        up = perf_snapshot["update_pending"].get(symbol, {})
        avg_pending = (
            float(up.get("pending_count_total", 0)) / float(up.get("call_count", 1))
            if up.get("call_count", 0) > 0
            else 0.0
        )
        print(
            "[PERF_SUMMARY] "
            f"symbol={symbol} candles={symbol_perf['candles']} max_pending={up.get('max_pending_count', 0)} "
            f"avg_pending={avg_pending:.4f} total_scanned={up.get('trades_scanned', 0)} "
            f"total_updated={up.get('trades_updated', 0)} total_resolved={up.get('trades_resolved', 0)} "
            f"total_sql_updates={up.get('sql_update_count', 0)} total_sql_inserts={up.get('sql_insert_count', 0)} "
            f"diag_sql_updates={up.get('update_pending_sql_updates_total', 0)} "
            f"diag_event_updates={up.get('update_pending_event_updates', 0)} "
            f"diag_event_activation={up.get('update_pending_event_activation', 0)} "
            f"diag_event_tp1_hit={up.get('update_pending_event_tp1_hit', 0)} "
            f"diag_event_final_resolution={up.get('update_pending_event_final_resolution', 0)} "
            f"diag_event_stop_loss={up.get('update_pending_event_stop_loss', 0)} "
            f"diag_event_break_even={up.get('update_pending_event_break_even', 0)} "
            f"diag_event_timeout_expiry={up.get('update_pending_event_timeout_expiry', 0)} "
            f"diag_progress_updates={up.get('update_pending_progress_updates', 0)} "
            f"diag_progress_unresolved_active={up.get('update_pending_progress_unresolved_active', 0)} "
            f"diag_progress_pending_not_activated={up.get('update_pending_progress_pending_not_activated', 0)} "
            f"diag_only_timestamp_updates={up.get('update_pending_only_timestamp_updates', 0)} "
            f"diag_excursion_updates={up.get('update_pending_excursion_updates', 0)} "
            f"diag_noop_candidate_updates={up.get('update_pending_noop_candidate_updates', 0)} "
            f"total_sql_selects={up.get('sql_select_count', 0)} "
            f"elapsed_update_pending_s={symbol_perf['elapsed_update_pending_s']:.6f} "
            f"elapsed_eval_s={symbol_perf['elapsed_eval_s']:.6f} "
            f"elapsed_register_s={symbol_perf['elapsed_register_s']:.6f} "
            f"elapsed_build_structure_s={symbol_perf['elapsed_build_structure_s']:.6f} "
            f"elapsed_eval_long_s={symbol_perf['elapsed_eval_long_s']:.6f} "
            f"elapsed_eval_short_s={symbol_perf['elapsed_eval_short_s']:.6f} "
            f"funnel_row_build_s={symbol_perf['funnel_row_build_s']:.6f} "
            f"structure_source={symbol_perf['structure_source']}"
            + (
                " "
                f"incremental_shadow_rows={symbol_perf['incremental_shadow_rows']} "
                f"incremental_shadow_mismatches={symbol_perf['incremental_shadow_mismatches']} "
                        f"structure_compare_ms={symbol_perf['structure_compare_s'] * 1000.0:.3f} "
                        f"shadow_strategy_eval_ms={symbol_perf['shadow_strategy_eval_s'] * 1000.0:.3f} "
                        f"shadow_compare_ms={symbol_perf['shadow_compare_s'] * 1000.0:.3f} "
                f"incremental_shadow_mismatch_rate="
                f"{(float(symbol_perf['incremental_shadow_mismatches']) / float(symbol_perf['incremental_shadow_rows'])) if symbol_perf['incremental_shadow_rows'] > 0 else 0.0:.6f} "
                f"incremental_shadow_update_s={symbol_perf['incremental_shadow_update_s']:.6f} "
                f"incremental_shadow_snapshot_s={symbol_perf['incremental_shadow_snapshot_s']:.6f} "
                f"structure_compare_s={symbol_perf['structure_compare_s']:.6f} "
                f"shadow_strategy_eval_s={symbol_perf['shadow_strategy_eval_s']:.6f} "
                f"shadow_compare_s={symbol_perf['shadow_compare_s']:.6f} "
                f"incremental_shadow_first_mismatch={json.dumps(symbol_perf['incremental_shadow_first_mismatch'], sort_keys=True)} "
                f"strategy_shadow_rows={symbol_perf['strategy_shadow_rows']} "
                "strategy_shadow_mismatch_rate="
                f"{(float(symbol_perf['strategy_shadow_mismatches']) / float(symbol_perf['strategy_shadow_rows'])) if symbol_perf['strategy_shadow_rows'] > 0 else 0.0:.6f} "
                f"strategy_shadow_mismatches={symbol_perf['strategy_shadow_mismatches']} "
                f"strategy_shadow_status_mismatches={symbol_perf['strategy_shadow_status_mismatches']} "
                "strategy_shadow_registration_impact_mismatch_rate="
                f"{(float(symbol_perf['strategy_shadow_registration_impact_mismatches']) / float(symbol_perf['strategy_shadow_rows'])) if symbol_perf['strategy_shadow_rows'] > 0 else 0.0:.6f} "
                f"strategy_shadow_registration_impact_mismatches={symbol_perf['strategy_shadow_registration_impact_mismatches']} "
                f"strategy_shadow_first_mismatch={json.dumps(symbol_perf['strategy_shadow_first_mismatch'], sort_keys=True)}"
                if use_incremental_shadow
                else ""
            )
        )

    t_funnel_csv_write = time.perf_counter()
    csv_file = write_funnel_csv(funnel_rows, args.funnel_csv)
    run_perf["totals"]["funnel_csv_write_s"] = time.perf_counter() - t_funnel_csv_write
    counts = status_counts(funnel_rows)

    print(f"validation_rows={table_row_count(validation_db_path)}")
    print(f"funnel_csv={csv_file}")
    print(
        " | ".join(
            [
                f"evaluated_total={counts['evaluated_total']}",
                f"rejected_total={counts['rejected_total']}",
                f"watch_total={counts['watch_total']}",
                f"candidate_total={counts['candidate_total']}",
                f"registered_total={counts['registered_total']}",
                f"blocked_by_open_trade_total={counts['blocked_by_open_trade_total']}",
            ]
        )
    )

    perf_snapshot = validation_store.get_perf_snapshot()
    hottest_symbol = None
    hottest_symbol_elapsed = -1.0
    for symbol, sp in run_perf["symbols"].items():
        total_elapsed = (
            float(sp["elapsed_engine_update_s"])
            + float(sp["elapsed_update_pending_s"])
            + float(sp["elapsed_eval_s"])
            + float(sp["elapsed_register_s"])
        )
        if total_elapsed > hottest_symbol_elapsed:
            hottest_symbol_elapsed = total_elapsed
            hottest_symbol = symbol

    stage_totals = {
        "engine_update": run_perf["totals"]["elapsed_engine_update_s"],
        "update_pending": run_perf["totals"]["elapsed_update_pending_s"],
        "evaluate_setups": run_perf["totals"]["elapsed_eval_s"],
        "register_setup": run_perf["totals"]["elapsed_register_s"],
        "build_structure_state": run_perf["totals"]["elapsed_build_structure_s"],
        "evaluate_pullback_retest_long": run_perf["totals"]["elapsed_eval_long_s"],
        "evaluate_pullback_retest_short": run_perf["totals"]["elapsed_eval_short_s"],
        "structure_compare": run_perf["totals"]["structure_compare_s"],
        "shadow_strategy_eval": run_perf["totals"]["shadow_strategy_eval_s"],
        "shadow_compare": run_perf["totals"]["shadow_compare_s"],
        "funnel_row_build": run_perf["totals"]["funnel_row_build_s"],
        "funnel_csv_write": run_perf["totals"]["funnel_csv_write_s"],
        "perf_json_write": run_perf["totals"]["perf_json_write_s"],
    }
    hottest_stage = max(stage_totals.items(), key=lambda kv: kv[1])[0] if stage_totals else "n/a"
    update_pending_total_scanned = sum(
        int(v.get("trades_scanned", 0)) for v in perf_snapshot["update_pending"].values()
    )
    update_pending_total_sql_updates = sum(
        int(v.get("sql_update_count", 0)) for v in perf_snapshot["update_pending"].values()
    )
    update_pending_diag_sql_updates_total = sum(
        int(v.get("update_pending_sql_updates_total", 0)) for v in perf_snapshot["update_pending"].values()
    )
    update_pending_diag_event_updates_total = sum(
        int(v.get("update_pending_event_updates", 0)) for v in perf_snapshot["update_pending"].values()
    )
    update_pending_diag_progress_updates_total = sum(
        int(v.get("update_pending_progress_updates", 0)) for v in perf_snapshot["update_pending"].values()
    )
    update_pending_diag_noop_candidate_updates_total = sum(
        int(v.get("update_pending_noop_candidate_updates", 0)) for v in perf_snapshot["update_pending"].values()
    )

    print(
        "[PERF_RUN] "
        f"symbols={len(symbols)} total_candles={run_perf['totals']['candles']} "
        f"total_scanned={update_pending_total_scanned} total_sql_updates={update_pending_total_sql_updates} "
        f"diag_sql_updates={update_pending_diag_sql_updates_total} "
        f"diag_event_updates={update_pending_diag_event_updates_total} "
        f"diag_progress_updates={update_pending_diag_progress_updates_total} "
        f"diag_noop_candidate_updates={update_pending_diag_noop_candidate_updates_total} "
        f"hottest_symbol={hottest_symbol} hottest_stage={hottest_stage} "
        f"structure_source={run_perf['totals']['structure_source']}"
        + (
            " "
            f"incremental_shadow_rows={run_perf['totals']['incremental_shadow_rows']} "
            f"incremental_shadow_mismatches={run_perf['totals']['incremental_shadow_mismatches']} "
            "incremental_shadow_mismatch_rate="
            f"{(float(run_perf['totals']['incremental_shadow_mismatches']) / float(run_perf['totals']['incremental_shadow_rows'])) if run_perf['totals']['incremental_shadow_rows'] > 0 else 0.0:.6f} "
            f"incremental_shadow_update_s={run_perf['totals']['incremental_shadow_update_s']:.6f} "
            f"incremental_shadow_snapshot_s={run_perf['totals']['incremental_shadow_snapshot_s']:.6f} "
            f"structure_compare_s={run_perf['totals']['structure_compare_s']:.6f} "
            f"shadow_strategy_eval_s={run_perf['totals']['shadow_strategy_eval_s']:.6f} "
            f"shadow_compare_s={run_perf['totals']['shadow_compare_s']:.6f} "
            f"incremental_shadow_first_mismatch={json.dumps(run_perf['totals']['incremental_shadow_first_mismatch'], sort_keys=True)} "
            f"strategy_shadow_rows={run_perf['totals']['strategy_shadow_rows']} "
            "strategy_shadow_mismatch_rate="
            f"{(float(run_perf['totals']['strategy_shadow_mismatches']) / float(run_perf['totals']['strategy_shadow_rows'])) if run_perf['totals']['strategy_shadow_rows'] > 0 else 0.0:.6f} "
            f"strategy_shadow_mismatches={run_perf['totals']['strategy_shadow_mismatches']} "
            f"strategy_shadow_status_mismatches={run_perf['totals']['strategy_shadow_status_mismatches']} "
            "strategy_shadow_registration_impact_mismatch_rate="
            f"{(float(run_perf['totals']['strategy_shadow_registration_impact_mismatches']) / float(run_perf['totals']['strategy_shadow_rows'])) if run_perf['totals']['strategy_shadow_rows'] > 0 else 0.0:.6f} "
            f"strategy_shadow_registration_impact_mismatches={run_perf['totals']['strategy_shadow_registration_impact_mismatches']} "
            f"strategy_shadow_first_mismatch={json.dumps(run_perf['totals']['strategy_shadow_first_mismatch'], sort_keys=True)}"
            if use_incremental_shadow
            else ""
        )
    )

    perf_output = {
        "run": run_perf,
        "validation_store_perf": perf_snapshot,
        "stage_totals": stage_totals,
        "hottest_symbol": hottest_symbol,
        "hottest_stage": hottest_stage,
        "update_pending_total_scanned": update_pending_total_scanned,
        "update_pending_total_sql_updates": update_pending_total_sql_updates,
        "update_pending_diag_sql_updates_total": update_pending_diag_sql_updates_total,
        "update_pending_diag_event_updates_total": update_pending_diag_event_updates_total,
        "update_pending_diag_progress_updates_total": update_pending_diag_progress_updates_total,
        "update_pending_diag_noop_candidate_updates_total": update_pending_diag_noop_candidate_updates_total,
    }
    perf_json_path = Path(args.perf_json)
    perf_json_path.parent.mkdir(parents=True, exist_ok=True)
    t_perf_json_write = time.perf_counter()
    perf_json_path.write_text(json.dumps(perf_output, indent=2, sort_keys=True), encoding="utf-8")
    run_perf["totals"]["perf_json_write_s"] = time.perf_counter() - t_perf_json_write
    print(f"perf_json={str(perf_json_path.resolve())}")
    print("DONE")


if __name__ == "__main__":
    main()
