# strategy_historical_backfill.py (BE-ready integration scaffold)

# NOTE:
# This version prepares BE integration but does NOT alter resolution logic inside StrategyValidationStore.
# To fully enable BE, StrategyValidationStore must be extended.
# This file is safe and keeps baseline behavior.

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

from pnf_engine import PnFProfile, PnFEngine
from storage import Storage
from structure_engine import build_structure_state
from strategy_engine import evaluate_pullback_retest_long, evaluate_pullback_retest_short
from strategy_validation import StrategyValidationStore

# BE module (ready for future integration)
from trade_management_be import BE_MODE, BE_TRIGGER_R

VALIDATION_ELIGIBLE_STATUSES = {"CANDIDATE", "WATCH"}


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical validation backfill for strategy_validation.db")
    parser.add_argument("--settings", default="settings.json")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--reset-validation-db", action="store_true")
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
                if str(setup.get("status")).upper() not in VALIDATION_ELIGIBLE_STATUSES:
                    continue

                validation_store.register_setup(
                    symbol=symbol,
                    setup=setup,
                    structure_state=structure,
                    reference_ts=close_ts,
                )

    print("DONE")


if __name__ == "__main__":
    main()
