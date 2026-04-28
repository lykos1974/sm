# strategy_historical_backfill.py (BE-ready integration scaffold + funnel diagnostics)

# NOTE:
# This version prepares BE integration but does NOT alter resolution logic inside StrategyValidationStore.
# To fully enable BE, StrategyValidationStore must be extended.
# This file keeps baseline setup logic unchanged and adds research diagnostics only.

from __future__ import annotations

import argparse
import csv
import json
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


def evaluate_setups(symbol: str, profile: PnFProfile, engine: PnFEngine) -> Tuple[dict, List[dict], Dict[str, float]]:
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
    return structure, setups, timings



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
    args = parser.parse_args()

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
        },
    }
    if args.incremental_shadow_structure:
        run_perf["totals"].update(
            {
                "incremental_shadow_rows": 0,
                "incremental_shadow_mismatches": 0,
                "incremental_shadow_first_mismatch": None,
                "incremental_shadow_update_s": 0.0,
                "incremental_shadow_snapshot_s": 0.0,
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
        }
        if args.incremental_shadow_structure:
            symbol_perf.update(
                {
                    "incremental_shadow_rows": 0,
                    "incremental_shadow_mismatches": 0,
                    "incremental_shadow_first_mismatch": None,
                    "incremental_shadow_update_s": 0.0,
                    "incremental_shadow_snapshot_s": 0.0,
                }
            )
        incremental_shadow_state: Any = None
        if args.incremental_shadow_structure:
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

            t0 = time.perf_counter()
            structure, setups, eval_timings = evaluate_setups(symbol, profile, engine)
            symbol_perf["elapsed_eval_s"] += time.perf_counter() - t0
            symbol_perf["setups_evaluated"] += len(setups)
            symbol_perf["candles"] += 1
            symbol_perf["elapsed_build_structure_s"] += float(eval_timings["elapsed_build_structure_s"])
            symbol_perf["elapsed_eval_long_s"] += float(eval_timings["elapsed_eval_long_s"])
            symbol_perf["elapsed_eval_short_s"] += float(eval_timings["elapsed_eval_short_s"])
            if incremental_shadow_state is not None:
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

                normalized_legacy = _shadow_normalize_structure(structure)
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
                    f"sql_selects={up.get('sql_select_count', 0)} "
                    f"elapsed_update_pending_ms={elapsed_update_pending_ms:.3f} "
                    f"elapsed_eval_ms={elapsed_eval_ms:.3f} "
                    f"elapsed_register_ms={elapsed_register_ms:.3f} "
                    f"elapsed_build_structure_ms={symbol_perf['elapsed_build_structure_s'] * 1000.0:.3f} "
                    f"elapsed_eval_long_ms={symbol_perf['elapsed_eval_long_s'] * 1000.0:.3f} "
                    f"elapsed_eval_short_ms={symbol_perf['elapsed_eval_short_s'] * 1000.0:.3f}"
                    + (
                        " "
                        f"incremental_shadow_rows={symbol_perf['incremental_shadow_rows']} "
                        f"incremental_shadow_mismatches={symbol_perf['incremental_shadow_mismatches']}"
                        if args.incremental_shadow_structure
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
        if args.incremental_shadow_structure:
            run_perf["totals"]["incremental_shadow_rows"] += int(symbol_perf["incremental_shadow_rows"])
            run_perf["totals"]["incremental_shadow_mismatches"] += int(symbol_perf["incremental_shadow_mismatches"])
            run_perf["totals"]["incremental_shadow_update_s"] += float(symbol_perf["incremental_shadow_update_s"])
            run_perf["totals"]["incremental_shadow_snapshot_s"] += float(symbol_perf["incremental_shadow_snapshot_s"])
            if (
                run_perf["totals"]["incremental_shadow_first_mismatch"] is None
                and symbol_perf["incremental_shadow_first_mismatch"] is not None
            ):
                run_perf["totals"]["incremental_shadow_first_mismatch"] = symbol_perf[
                    "incremental_shadow_first_mismatch"
                ]

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
            f"total_sql_selects={up.get('sql_select_count', 0)} "
            f"elapsed_update_pending_s={symbol_perf['elapsed_update_pending_s']:.6f} "
            f"elapsed_eval_s={symbol_perf['elapsed_eval_s']:.6f} "
            f"elapsed_register_s={symbol_perf['elapsed_register_s']:.6f} "
            f"elapsed_build_structure_s={symbol_perf['elapsed_build_structure_s']:.6f} "
            f"elapsed_eval_long_s={symbol_perf['elapsed_eval_long_s']:.6f} "
            f"elapsed_eval_short_s={symbol_perf['elapsed_eval_short_s']:.6f}"
            + (
                " "
                f"incremental_shadow_rows={symbol_perf['incremental_shadow_rows']} "
                f"incremental_shadow_mismatches={symbol_perf['incremental_shadow_mismatches']} "
                f"incremental_shadow_mismatch_rate="
                f"{(float(symbol_perf['incremental_shadow_mismatches']) / float(symbol_perf['incremental_shadow_rows'])) if symbol_perf['incremental_shadow_rows'] > 0 else 0.0:.6f} "
                f"incremental_shadow_update_s={symbol_perf['incremental_shadow_update_s']:.6f} "
                f"incremental_shadow_snapshot_s={symbol_perf['incremental_shadow_snapshot_s']:.6f} "
                f"incremental_shadow_first_mismatch={json.dumps(symbol_perf['incremental_shadow_first_mismatch'], sort_keys=True)}"
                if args.incremental_shadow_structure
                else ""
            )
        )

    csv_file = write_funnel_csv(funnel_rows, args.funnel_csv)
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
    }
    hottest_stage = max(stage_totals.items(), key=lambda kv: kv[1])[0] if stage_totals else "n/a"
    update_pending_total_scanned = sum(
        int(v.get("trades_scanned", 0)) for v in perf_snapshot["update_pending"].values()
    )
    update_pending_total_sql_updates = sum(
        int(v.get("sql_update_count", 0)) for v in perf_snapshot["update_pending"].values()
    )

    print(
        "[PERF_RUN] "
        f"symbols={len(symbols)} total_candles={run_perf['totals']['candles']} "
        f"total_scanned={update_pending_total_scanned} total_sql_updates={update_pending_total_sql_updates} "
        f"hottest_symbol={hottest_symbol} hottest_stage={hottest_stage}"
        + (
            " "
            f"incremental_shadow_rows={run_perf['totals']['incremental_shadow_rows']} "
            f"incremental_shadow_mismatches={run_perf['totals']['incremental_shadow_mismatches']} "
            "incremental_shadow_mismatch_rate="
            f"{(float(run_perf['totals']['incremental_shadow_mismatches']) / float(run_perf['totals']['incremental_shadow_rows'])) if run_perf['totals']['incremental_shadow_rows'] > 0 else 0.0:.6f} "
            f"incremental_shadow_update_s={run_perf['totals']['incremental_shadow_update_s']:.6f} "
            f"incremental_shadow_snapshot_s={run_perf['totals']['incremental_shadow_snapshot_s']:.6f} "
            f"incremental_shadow_first_mismatch={json.dumps(run_perf['totals']['incremental_shadow_first_mismatch'], sort_keys=True)}"
            if args.incremental_shadow_structure
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
    }
    perf_json_path = Path(args.perf_json)
    perf_json_path.parent.mkdir(parents=True, exist_ok=True)
    perf_json_path.write_text(json.dumps(perf_output, indent=2, sort_keys=True), encoding="utf-8")
    print(f"perf_json={str(perf_json_path.resolve())}")
    print("DONE")


if __name__ == "__main__":
    main()
