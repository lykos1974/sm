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
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pnf_engine import PnFProfile, PnFEngine
from storage import Storage
from structure_engine import build_structure_state
from strategy_engine import evaluate_pullback_retest_long, evaluate_pullback_retest_short
from strategy_validation import StrategyValidationStore

# BE module (ready for future integration)
from trade_management_be import BE_MODE, BE_TRIGGER_R

VALIDATION_ELIGIBLE_STATUSES = {"CANDIDATE", "WATCH"}
DEFAULT_FUNNEL_CSV_PATH = "exports/strategy_funnel_diagnostics.csv"


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



def evaluate_setups(symbol: str, profile: PnFProfile, engine: PnFEngine) -> Tuple[dict, List[dict]]:
    structure = build_structure_state(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        latest_signal_name=engine.latest_signal_name(),
        market_state=engine.market_state(),
        last_price=getattr(engine, "last_price", None),
    )

    setup_long = evaluate_pullback_retest_long(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )

    setup_short = evaluate_pullback_retest_short(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )

    setups = [s for s in (setup_long, setup_short) if s]
    return structure, setups



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



def _safe_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return ""



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

    for symbol in symbols:
        profile = profiles[symbol]
        engine = PnFEngine(profile)
        candles = load_all_closed_candles(storage, symbol)

        for candle in candles:
            close_ts = int(candle["close_time"])
            close_price = float(candle["close"])
            high_price = float(candle["high"])
            low_price = float(candle["low"])

            engine.update_from_price(close_ts, close_price)

            # CURRENT resolution (unchanged)
            validation_store.update_pending_with_candle(
                symbol=symbol,
                close_ts=close_ts,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
            )

            structure, setups = evaluate_setups(symbol, profile, engine)
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
                        setup_id = validation_store.register_setup(
                            symbol=symbol,
                            setup=setup,
                            structure_state=structure,
                            reference_ts=close_ts,
                        )
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
    print("DONE")


if __name__ == "__main__":
    main()
