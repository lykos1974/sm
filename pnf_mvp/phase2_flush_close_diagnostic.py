from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from storage import Storage
from strategy_validation import StrategyValidationStore

VALIDATION_ELIGIBLE_STATUSES = {"CANDIDATE", "WATCH"}


def load_settings(settings_path: str) -> dict:
    with open(settings_path, "r", encoding="utf-8") as f:
        return json.load(f)


def reset_validation_db(validation_db_path: str) -> None:
    base = Path(validation_db_path)
    for suffix in ("", "-shm", "-wal"):
        p = Path(str(base) + suffix)
        if p.exists():
            p.unlink()


def _safe_json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    text = str(value).strip()
    if not text:
        return {}
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def load_generated_setups(csv_path: str, symbol: str) -> list[dict]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    out: list[dict] = []
    for r in rows:
        if str(r.get("symbol") or "").strip() != symbol:
            continue
        status = str(r.get("status") or "").upper()
        if status not in VALIDATION_ELIGIBLE_STATUSES:
            continue

        try:
            reference_ts = int(float(r["reference_ts"]))
        except Exception:
            continue

        setup = _safe_json_loads(r.get("raw_setup_json"))
        structure = _safe_json_loads(r.get("raw_structure_json"))

        if not setup:
            setup = {
                "status": r.get("status"),
                "side": r.get("side"),
                "strategy": r.get("strategy"),
                "zone_low": r.get("zone_low"),
                "zone_high": r.get("zone_high"),
                "ideal_entry": r.get("ideal_entry"),
                "invalidation": r.get("invalidation"),
                "risk": r.get("risk"),
                "tp1": r.get("tp1"),
                "tp2": r.get("tp2"),
                "rr1": r.get("rr1"),
                "rr2": r.get("rr2"),
                "reason": r.get("reason"),
                "reject_reason": r.get("reject_reason"),
                "quality_score": r.get("quality_score"),
                "quality_grade": r.get("quality_grade"),
            }

        if not structure:
            structure = {
                "trend_state": r.get("trend_state"),
                "trend_regime": r.get("trend_regime"),
                "immediate_slope": r.get("immediate_slope"),
                "breakout_context": r.get("breakout_context"),
                "market_state": r.get("market_state"),
                "latest_signal_name": r.get("latest_signal_name"),
                "active_leg_boxes": r.get("active_leg_boxes"),
                "support_level": r.get("support_level"),
                "resistance_level": r.get("resistance_level"),
                "current_column_index": None,
            }

        out.append(
            {
                "symbol": symbol,
                "reference_ts": reference_ts,
                "setup": setup,
                "structure": structure,
            }
        )

    out.sort(key=lambda x: x["reference_ts"])
    return out


def load_bounded_candles(storage: Storage, symbol: str, start_ts: int, bars: int) -> list[dict]:
    candles = storage.load_candles_after(symbol, start_ts - 1)
    if len(candles) > 1:
        candles = candles[:-1]
    return candles[:bars]


def table_row_count(db_path: str, table_name: str = "strategy_setups") -> int:
    if not Path(db_path).exists():
        return 0
    conn = sqlite3.connect(db_path, timeout=5)
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


def main() -> int:
    p = argparse.ArgumentParser(description="Phase 2 flush/close diagnostic")
    p.add_argument("--settings", default="settings.research_clean.json")
    p.add_argument("--setups-csv", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--validation-db", default="data/strategy_validation_phase2_flush_diag.sqlite3")
    p.add_argument("--reset-validation-db", action="store_true")
    p.add_argument("--max-setups", type=int, default=20)
    p.add_argument("--max-bars", type=int, default=1000)
    args = p.parse_args()

    settings = load_settings(args.settings)
    if args.reset_validation_db:
        reset_validation_db(args.validation_db)

    t0 = time.time()
    storage = Storage(settings["database_path"])
    all_setups = load_generated_setups(args.setups_csv, args.symbol)
    if not all_setups:
        print("no eligible setups found")
        return 1

    setups = all_setups[: max(1, args.max_setups)]
    start_ts = setups[0]["reference_ts"]
    candles = load_bounded_candles(storage, args.symbol, start_ts, args.max_bars)
    if not candles:
        print("no candles loaded")
        return 1

    buckets: dict[int, list[dict]] = {}
    for row in setups:
        buckets.setdefault(int(row["reference_ts"]), []).append(row)

    store = StrategyValidationStore(args.validation_db)

    registered = 0
    blocked = 0

    print(f"symbol={args.symbol}")
    print(f"diagnostic_setups_used={len(setups)}")
    print(f"diagnostic_bars_used={len(candles)}")

    for c in candles:
        ts = int(c["close_time"])
        store.update_pending_with_candle(
            symbol=args.symbol,
            close_ts=ts,
            high_price=float(c["high"]),
            low_price=float(c["low"]),
            close_price=float(c["close"]),
        )

        for row in buckets.get(ts, []):
            has_open = store.has_open_trade_for_symbol(args.symbol)
            if not store.allow_multiple_trades_per_symbol and has_open:
                blocked += 1
                continue

            sid = store.register_setup(
                symbol=args.symbol,
                setup=row["setup"],
                structure_state=row["structure"],
                reference_ts=ts,
                snapshot_path=None,
                active_column_index=row["structure"].get("current_column_index"),
            )
            if sid:
                registered += 1
            else:
                blocked += 1

    print(f"loop_done | registered={registered} | blocked={blocked} | elapsed_sec={time.time() - t0:.4f}")

    t1 = time.time()
    print("before_flush")
    store.flush()
    print(f"after_flush | flush_sec={time.time() - t1:.4f}")

    t2 = time.time()
    print("before_close")
    store.close()
    print(f"after_close | close_sec={time.time() - t2:.4f}")

    t3 = time.time()
    print("before_row_count")
    rows = table_row_count(args.validation_db)
    print(f"after_row_count | row_count_sec={time.time() - t3:.4f} | validation_rows={rows}")

    print(f"duration_sec={time.time() - t0:.4f}")
    print("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
