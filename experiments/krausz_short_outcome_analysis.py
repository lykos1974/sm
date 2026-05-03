#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any

OUTCOME_STOP = "STOP"
OUTCOME_TP1 = "TP1"
OUTCOME_TP2 = "TP2"


R_MAP = {
    OUTCOME_STOP: -1.0,
    OUTCOME_TP1: 2.0,
    OUTCOME_TP2: 3.0,
}


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def load_candidates(csv_path: Path, sample_limit: int | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if _to_int(row.get("shadow_krausz_short_candidate")) != 1:
                continue
            rows.append(row)
            if sample_limit is not None and len(rows) >= sample_limit:
                break
    return rows


def load_future_candles(conn: sqlite3.Connection, symbol: str, reference_ts: int) -> list[tuple[int, float, float]]:
    cur = conn.execute(
        """
        SELECT close_time, high, low
        FROM candles
        WHERE symbol = ?
          AND interval = '1m'
          AND close_time > ?
        ORDER BY close_time ASC
        """,
        (symbol, reference_ts),
    )
    return [(int(ts), float(high), float(low)) for ts, high, low in cur.fetchall()]


def classify_short(candles: list[tuple[int, float, float]], stop: float, tp1: float, tp2: float) -> tuple[str, int, int]:
    for idx, (close_time, high, low) in enumerate(candles, start=1):
        hit_stop = high >= stop
        hit_tp2 = low <= tp2
        hit_tp1 = low <= tp1

        if hit_stop:
            return OUTCOME_STOP, idx, close_time
        if hit_tp2:
            return OUTCOME_TP2, idx, close_time
        if hit_tp1:
            return OUTCOME_TP1, idx, close_time

    return OUTCOME_STOP, len(candles), candles[-1][0] if candles else -1


def fmt_pct(v: float) -> str:
    return f"{v:.4f}"


def safe_median(values: list[int]) -> float | None:
    return float(median(values)) if values else None


def print_table(title: str, headers: list[str], rows: list[list[str]]) -> None:
    print(f"\n{title}")
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sep = "-+-".join("-" * widths[i] for i in range(len(headers)))
    print(line)
    print(sep)
    for row in rows:
        print(" | ".join(row[i].ljust(widths[i]) for i in range(len(headers))))


def main() -> None:
    parser = argparse.ArgumentParser(description="Krausz short outcome + timing analysis (analysis-only)")
    parser.add_argument("--input-csv", default="pnf_mvp/exports/v6_krausz_eth_sol.csv")
    parser.add_argument("--db-path", default="pnf_mvp/data/pnf_mvp_research_clean.sqlite3")
    parser.add_argument("--sample-limit", type=int, default=None)
    parser.add_argument("--output-csv", default=None)
    args = parser.parse_args()

    rows = load_candidates(Path(args.input_csv), args.sample_limit)
    conn = sqlite3.connect(args.db_path)
    trade_results: list[dict[str, Any]] = []
    try:
        for row in rows:
            symbol = str(row.get("symbol") or "").strip()
            reference_ts = _to_int(row.get("reference_ts"))
            entry = _to_float(row.get("shadow_krausz_short_entry"))
            stop = _to_float(row.get("shadow_krausz_short_stop"))
            tp1 = _to_float(row.get("shadow_krausz_short_tp1"))
            tp2 = _to_float(row.get("shadow_krausz_short_tp2"))

            if not symbol or None in (reference_ts, entry, stop, tp1, tp2):
                continue

            candles = load_future_candles(conn, symbol, reference_ts)
            outcome, bars_to_event, event_ts = classify_short(candles, stop, tp1, tp2)
            trade_results.append(
                {
                    "symbol": symbol,
                    "reference_ts": reference_ts,
                    "entry": entry,
                    "stop": stop,
                    "tp1": tp1,
                    "tp2": tp2,
                    "outcome": outcome,
                    "bars_to_event": bars_to_event,
                    "event_ts": event_ts,
                    "r_multiple": R_MAP[outcome],
                }
            )
    finally:
        conn.close()

    total = len(trade_results)
    stop_count = sum(1 for r in trade_results if r["outcome"] == OUTCOME_STOP)
    tp1_count = sum(1 for r in trade_results if r["outcome"] == OUTCOME_TP1)
    tp2_count = sum(1 for r in trade_results if r["outcome"] == OUTCOME_TP2)

    bars_all = [r["bars_to_event"] for r in trade_results]
    bars_tp1 = [r["bars_to_event"] for r in trade_results if r["outcome"] == OUTCOME_TP1]
    bars_tp2 = [r["bars_to_event"] for r in trade_results if r["outcome"] == OUTCOME_TP2]
    bars_stop = [r["bars_to_event"] for r in trade_results if r["outcome"] == OUTCOME_STOP]

    avg_r = sum(r["r_multiple"] for r in trade_results) / total if total else 0.0

    print("=== KRAUSZ SHORT BREAKDOWN — OUTCOME + TIMING ANALYSIS ===")
    print(f"input_csv={args.input_csv}")
    print(f"db_path={args.db_path}")
    print(f"total_trades={total}")

    global_rows = [
        ["stop_count", str(stop_count)],
        ["tp1_count", str(tp1_count)],
        ["tp2_count", str(tp2_count)],
        ["stop_rate", fmt_pct(stop_count / total if total else 0.0)],
        ["tp1_rate", fmt_pct(tp1_count / total if total else 0.0)],
        ["tp2_rate", fmt_pct(tp2_count / total if total else 0.0)],
        ["avg_R_multiple", f"{avg_r:.4f}"],
        ["median_bars_to_event", str(safe_median(bars_all))],
        ["median_bars_to_tp1", str(safe_median(bars_tp1))],
        ["median_bars_to_tp2", str(safe_median(bars_tp2))],
        ["median_bars_to_stop", str(safe_median(bars_stop))],
    ]
    print_table("GLOBAL METRICS", ["metric", "value"], global_rows)

    buckets = [
        ("<=50", lambda x: x <= 50),
        ("51-200", lambda x: 50 < x <= 200),
        ("201-500", lambda x: 200 < x <= 500),
        ("500+", lambda x: x > 500),
    ]
    bucket_rows: list[list[str]] = []
    for label, pred in buckets:
        bucket_trades = [r for r in trade_results if pred(r["bars_to_event"])]
        n = len(bucket_trades)
        if n == 0:
            bucket_rows.append([label, "0", "0.0000", "0.0000", "0.0000", "0.0000"])
            continue
        s = sum(1 for r in bucket_trades if r["outcome"] == OUTCOME_STOP)
        t1 = sum(1 for r in bucket_trades if r["outcome"] == OUTCOME_TP1)
        t2 = sum(1 for r in bucket_trades if r["outcome"] == OUTCOME_TP2)
        bavg = sum(r["r_multiple"] for r in bucket_trades) / n
        bucket_rows.append([label, str(n), f"{bavg:.4f}", fmt_pct(t1 / n), fmt_pct(t2 / n), fmt_pct(s / n)])
    print_table("TIMING BUCKETS", ["bucket", "trades", "avg_R", "tp1_rate", "tp2_rate", "stop_rate"], bucket_rows)

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in trade_results:
        by_symbol[r["symbol"]].append(r)
    symbol_rows: list[list[str]] = []
    for symbol in sorted(by_symbol.keys()):
        vals = by_symbol[symbol]
        n = len(vals)
        s = sum(1 for r in vals if r["outcome"] == OUTCOME_STOP)
        t1 = sum(1 for r in vals if r["outcome"] == OUTCOME_TP1)
        t2 = sum(1 for r in vals if r["outcome"] == OUTCOME_TP2)
        ravg = sum(r["r_multiple"] for r in vals) / n if n else 0.0
        med = safe_median([r["bars_to_event"] for r in vals])
        symbol_rows.append([symbol, str(n), f"{ravg:.4f}", fmt_pct(t1 / n), fmt_pct(t2 / n), fmt_pct(s / n), str(med)])
    print_table("SYMBOL BREAKDOWN", ["symbol", "trades", "avg_R", "tp1_rate", "tp2_rate", "stop_rate", "median_bars_to_event"], symbol_rows)

    if args.output_csv:
        out_path = Path(args.output_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(trade_results[0].keys()) if trade_results else [])
            if trade_results:
                writer.writeheader()
                writer.writerows(trade_results)
        print(f"\noutput_csv={out_path.resolve()}")


if __name__ == "__main__":
    main()
