from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, List

from storage import Storage
from strategy_validation import StrategyValidationStore
from trade_management_be import BE_MODE, BE_TRIGGER_R

VALIDATION_ELIGIBLE_STATUSES = {"CANDIDATE", "WATCH"}


def load_settings(settings_path: str) -> dict:
    with open(settings_path, "r", encoding="utf-8") as f:
        return json.load(f)


def split_symbols(settings: dict, symbols_arg: str | None) -> List[str]:
    if not symbols_arg:
        return list(settings["symbols"])
    wanted = [s.strip() for s in symbols_arg.split(",") if s.strip()]
    return [s for s in settings["symbols"] if s in wanted]


def reset_validation_db(validation_db_path: str) -> None:
    base = Path(validation_db_path)
    for suffix in ("", "-shm", "-wal"):
        p = Path(str(base) + suffix)
        if p.exists():
            p.unlink()


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def load_lean_setups(csv_path: str) -> List[dict]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    out: List[dict] = []
    for r in rows:
        status = str(r.get("status") or "").upper()
        if status not in VALIDATION_ELIGIBLE_STATUSES:
            continue

        symbol = str(r.get("symbol") or "").strip()
        ref_ts = _safe_int(r.get("reference_ts"))
        if not symbol or ref_ts is None:
            continue

        setup = {
            "status": status,
            "side": r.get("side"),
            "strategy": r.get("strategy"),
            "zone_low": _safe_float(r.get("zone_low")),
            "zone_high": _safe_float(r.get("zone_high")),
            "ideal_entry": _safe_float(r.get("ideal_entry")),
            "invalidation": _safe_float(r.get("invalidation")),
            "risk": _safe_float(r.get("risk")),
            "tp1": _safe_float(r.get("tp1")),
            "tp2": _safe_float(r.get("tp2")),
            "rr1": _safe_float(r.get("rr1")),
            "rr2": _safe_float(r.get("rr2")),
            "reason": r.get("reason"),
            "reject_reason": r.get("reject_reason"),
            "quality_score": _safe_float(r.get("quality_score")),
            "quality_grade": r.get("quality_grade"),
            "pullback_quality": r.get("pullback_quality"),
            "risk_quality": r.get("risk_quality"),
            "reward_quality": r.get("reward_quality"),
        }

        structure = {
            "trend_state": r.get("trend_state"),
            "trend_regime": r.get("trend_regime"),
            "immediate_slope": r.get("immediate_slope"),
            "breakout_context": r.get("breakout_context"),
            "market_state": r.get("market_state"),
            "latest_signal_name": r.get("latest_signal_name"),
            "active_leg_boxes": _safe_int(r.get("active_leg_boxes")),
            "support_level": _safe_float(r.get("support_level")),
            "resistance_level": _safe_float(r.get("resistance_level")),
            "is_extended_move": bool(int(float(r.get("is_extended_move") or 0))),
            "current_column_index": _safe_int(r.get("current_column_index")),
        }

        out.append(
            {
                "symbol": symbol,
                "reference_ts": ref_ts,
                "setup": setup,
                "structure": structure,
            }
        )

    out.sort(key=lambda x: (x["symbol"], x["reference_ts"]))
    return out


def bucket_setups(rows: Iterable[dict]) -> dict[int, list[dict]]:
    d: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        d[int(r["reference_ts"])].append(r)
    return d


def load_bounded_candles(storage: Storage, symbol: str, start_ts: int, end_ts: int) -> list[dict]:
    candles = storage.load_candles_after(symbol, start_ts - 1)
    bounded = [c for c in candles if int(c["close_time"]) <= end_ts]
    return bounded[:-1] if len(bounded) > 1 else bounded


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


def simulate(
    settings_path: str,
    setups_csv: str,
    symbols_arg: str | None,
    validation_db: str | None,
    reset_db: bool,
    simulation_horizon_bars: int,
) -> dict[str, Any]:
    t0 = time.time()

    settings = load_settings(settings_path)
    if validation_db is None:
        validation_db = settings.get("strategy_validation_db_path", "strategy_validation.db")

    if reset_db:
        reset_validation_db(validation_db)

    storage = Storage(settings["database_path"])
    store = StrategyValidationStore(validation_db)
    symbols = split_symbols(settings, symbols_arg)

    t_load0 = time.time()
    rows = load_lean_setups(setups_csv)
    load_setups_sec = time.time() - t_load0

    print(f"eligible_rows_loaded={len(rows)}")
    print(f"simulation_horizon_bars={simulation_horizon_bars}")
    print(f"BE_MODE={BE_MODE} | BE_TRIGGER_R={BE_TRIGGER_R}")
    print(f"load_setups_sec={load_setups_sec:.2f}")

    total_registered = 0
    total_blocked = 0
    total_candles_replayed = 0

    for symbol in symbols:
        symbol_rows = [r for r in rows if r["symbol"] == symbol]
        if not symbol_rows:
            print(f"{symbol} | no eligible rows")
            continue

        symbol_t0 = time.time()
        start_ts = min(r["reference_ts"] for r in symbol_rows)
        last_setup_ts = max(r["reference_ts"] for r in symbol_rows)
        end_ts = last_setup_ts + simulation_horizon_bars * 60000

        buckets = bucket_setups(symbol_rows)
        candles = load_bounded_candles(storage, symbol, start_ts, end_ts)

        registered = 0
        blocked = 0
        candles_replayed = 0
        update_pending_sec = 0.0
        register_sec = 0.0

        for c in candles:
            ts = int(c["close_time"])
            t_upd = time.time()
            store.update_pending_with_candle(
                symbol=symbol,
                close_ts=ts,
                high_price=float(c["high"]),
                low_price=float(c["low"]),
                close_price=float(c["close"]),
            )
            update_pending_sec += time.time() - t_upd
            candles_replayed += 1

            for r in buckets.get(ts, []):
                if not store.allow_multiple_trades_per_symbol and store.has_open_trade_for_symbol(symbol):
                    blocked += 1
                    continue

                t_reg = time.time()
                sid = store.register_setup(
                    symbol=symbol,
                    setup=r["setup"],
                    structure_state=r["structure"],
                    reference_ts=ts,
                    snapshot_path=None,
                    active_column_index=r["structure"].get("current_column_index"),
                )
                register_sec += time.time() - t_reg

                if sid:
                    registered += 1
                else:
                    blocked += 1

        symbol_dt = time.time() - symbol_t0
        total_registered += registered
        total_blocked += blocked
        total_candles_replayed += candles_replayed

        print(
            f"{symbol} | registered={registered} | blocked={blocked} | "
            f"candles_replayed={candles_replayed} | "
            f"update_pending_sec={update_pending_sec:.2f} | register_sec={register_sec:.2f} | "
            f"duration_sec={symbol_dt:.2f}"
        )

    store.flush()
    store.close()

    dt = time.time() - t0
    validation_rows = table_row_count(validation_db)
    candles_per_sec = (total_candles_replayed / dt) if dt > 0 else 0.0

    print(f"validation_rows={validation_rows}")
    print(
        f"registered_total={total_registered} | blocked_total={total_blocked} | "
        f"candles_replayed_total={total_candles_replayed} | candles_per_sec={candles_per_sec:.2f}"
    )
    print(f"duration_sec={dt:.2f}")
    print("DONE")

    return {
        "validation_rows": validation_rows,
        "registered_total": total_registered,
        "blocked_total": total_blocked,
        "candles_replayed_total": total_candles_replayed,
        "duration_sec": dt,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Phase 2 trade simulation from lean simulation input CSV")
    p.add_argument("--settings", default="settings.research_clean.json")
    p.add_argument("--setups-csv", required=True)
    p.add_argument("--symbols", default=None)
    p.add_argument("--validation-db", default=None)
    p.add_argument("--reset-validation-db", action="store_true")
    p.add_argument("--simulation-horizon-bars", type=int, default=5000)
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    simulate(
        settings_path=args.settings,
        setups_csv=args.setups_csv,
        symbols_arg=args.symbols,
        validation_db=args.validation_db,
        reset_db=bool(args.reset_validation_db),
        simulation_horizon_bars=int(args.simulation_horizon_bars),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
