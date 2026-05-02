#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any

OUTCOME_STOP_FIRST = "STOP_FIRST"
OUTCOME_TP1_FIRST = "TP1_FIRST"
OUTCOME_TP2_FIRST = "TP2_FIRST"
OUTCOME_AMBIGUOUS = "AMBIGUOUS_STOP_TP"
OUTCOME_NO_HIT = "NO_HIT_UNTIL_END"
OUTCOME_INVALID_RISK = "INVALID_RISK"


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def load_trigger_rows(csv_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            trigger = _to_int(row.get("shadow_continuation_trigger"))
            if trigger != 1:
                continue
            rows.append(row)
    return rows


def load_future_candles(conn: sqlite3.Connection, symbol: str, reference_ts: int) -> list[tuple[int, float, float, float]]:
    cur = conn.execute(
        """
        SELECT close_time, high, low, close
        FROM candles
        WHERE symbol = ?
          AND interval = '1m'
          AND close_time > ?
        ORDER BY close_time ASC
        """,
        (symbol, reference_ts),
    )
    out: list[tuple[int, float, float, float]] = []
    for close_time, high, low, close in cur.fetchall():
        out.append((int(close_time), float(high), float(low), float(close)))
    return out


def classify_outcome(candles: list[tuple[int, float, float, float]], entry: float, stop: float) -> tuple[str, int | None, int | None]:
    risk = entry - stop
    if risk <= 0.0:
        return OUTCOME_INVALID_RISK, None, None

    tp1 = entry + (2.0 * risk)
    tp2 = entry + (3.0 * risk)

    for idx, (close_time, high, low, _close) in enumerate(candles, start=1):
        hit_stop = low <= stop
        hit_tp1 = high >= tp1
        hit_tp2 = high >= tp2

        if hit_stop and (hit_tp1 or hit_tp2):
            return OUTCOME_AMBIGUOUS, idx, close_time
        if hit_stop:
            return OUTCOME_STOP_FIRST, idx, close_time
        if hit_tp2:
            return OUTCOME_TP2_FIRST, idx, close_time
        if hit_tp1:
            return OUTCOME_TP1_FIRST, idx, close_time

    return OUTCOME_NO_HIT, None, None


def pct(n: int, d: int) -> float:
    return (float(n) / float(d)) if d > 0 else 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Shadow continuation trigger outcome analysis (diagnostics-only)")
    parser.add_argument("--funnel-csv", default="exports/v4_shadow_continuation_fixed.csv")
    parser.add_argument("--db-path", default="data/pnf_mvp_research_clean.sqlite3")
    parser.add_argument("--output-csv", default=None, help="Optional per-trigger outcomes CSV path")
    args = parser.parse_args()

    funnel_csv = Path(args.funnel_csv)
    db_path = Path(args.db_path)

    trigger_rows = load_trigger_rows(funnel_csv)
    total_triggers = len(trigger_rows)

    counts = Counter()
    valid_triggers = 0
    candles_to_event_values: list[int] = []
    risk_values: list[float] = []
    result_rows: list[dict[str, Any]] = []

    conn = sqlite3.connect(str(db_path))
    try:
        for row in trigger_rows:
            symbol = str(row.get("symbol") or "").strip()
            reference_ts = _to_int(row.get("reference_ts"))
            entry = _to_float(row.get("shadow_entry_price"))
            stop = _to_float(row.get("shadow_stop_price"))

            if not symbol or reference_ts is None or entry is None or stop is None:
                outcome = OUTCOME_INVALID_RISK
                candles_to_event = None
                event_ts = None
                risk = None
            else:
                risk = entry - stop
                if risk <= 0:
                    outcome = OUTCOME_INVALID_RISK
                    candles_to_event = None
                    event_ts = None
                else:
                    valid_triggers += 1
                    risk_values.append(risk)
                    future_candles = load_future_candles(conn, symbol, reference_ts)
                    outcome, candles_to_event, event_ts = classify_outcome(future_candles, entry, stop)
                    if candles_to_event is not None:
                        candles_to_event_values.append(candles_to_event)

            counts[outcome] += 1
            result_rows.append(
                {
                    "symbol": symbol,
                    "reference_ts": reference_ts,
                    "shadow_entry_price": entry,
                    "shadow_stop_price": stop,
                    "risk": risk,
                    "outcome": outcome,
                    "candles_to_event": candles_to_event,
                    "event_ts": event_ts,
                }
            )
    finally:
        conn.close()

    print("=== Shadow Continuation Outcome Analysis (Conservative Ambiguity) ===")
    print(f"funnel_csv={funnel_csv}")
    print(f"db_path={db_path}")
    print(f"total_triggers={total_triggers}")
    print(f"valid_triggers={valid_triggers}")
    print(f"{OUTCOME_STOP_FIRST}={counts[OUTCOME_STOP_FIRST]}")
    print(f"{OUTCOME_TP1_FIRST}={counts[OUTCOME_TP1_FIRST]}")
    print(f"{OUTCOME_TP2_FIRST}={counts[OUTCOME_TP2_FIRST]}")
    print(f"{OUTCOME_AMBIGUOUS}={counts[OUTCOME_AMBIGUOUS]}")
    print(f"{OUTCOME_NO_HIT}={counts[OUTCOME_NO_HIT]}")
    print(f"{OUTCOME_INVALID_RISK}={counts[OUTCOME_INVALID_RISK]}")

    print("--- Rates (denominator=valid_triggers) ---")
    print(f"stop_rate={pct(counts[OUTCOME_STOP_FIRST], valid_triggers):.6f}")
    print(f"tp1_rate={pct(counts[OUTCOME_TP1_FIRST], valid_triggers):.6f}")
    print(f"tp2_rate={pct(counts[OUTCOME_TP2_FIRST], valid_triggers):.6f}")
    print(f"ambiguous_rate={pct(counts[OUTCOME_AMBIGUOUS], valid_triggers):.6f}")
    print(f"no_hit_rate={pct(counts[OUTCOME_NO_HIT], valid_triggers):.6f}")

    if candles_to_event_values:
        print(f"median_candles_to_event={median(candles_to_event_values):.2f}")
    else:
        print("median_candles_to_event=")

    if risk_values:
        sorted_risks = sorted(risk_values)
        print(f"risk_count={len(sorted_risks)}")
        print(f"risk_min={sorted_risks[0]:.8f}")
        print(f"risk_median={median(sorted_risks):.8f}")
        print(f"risk_max={sorted_risks[-1]:.8f}")
    else:
        print("risk_count=0")

    if args.output_csv:
        out_path = Path(args.output_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "symbol",
                    "reference_ts",
                    "shadow_entry_price",
                    "shadow_stop_price",
                    "risk",
                    "outcome",
                    "candles_to_event",
                    "event_ts",
                ],
            )
            writer.writeheader()
            for r in result_rows:
                writer.writerow(r)
        print(f"output_csv={out_path.resolve()}")


if __name__ == "__main__":
    main()
