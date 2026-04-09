"""
strategy_trade_export.py

Exports resolved trades from strategy_validation.db and prints summary stats.

Key reporting rule:
- TP1 is an event
- TP1_PARTIAL_THEN_BE and TP2 are terminal outcomes
- TP2 rows are allowed to also have tp1_hit = 1
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd


DB_PATH = "strategy_validation.db"
TABLE_NAME = "strategy_setups"
CSV_PATH = "strategy_trades_export.csv"


def safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def safe_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def realized_r_from_row(row) -> Optional[float]:
    resolution = str(row["resolution_status"] or "").upper()
    rr1 = safe_float(row["rr1"])
    rr2 = safe_float(row["rr2"])

    if resolution == "STOPPED":
        return -1.0
    if resolution == "TP1_PARTIAL_THEN_BE":
        return 0.5 * rr1 if rr1 is not None else None
    if resolution == "TP2":
        if rr1 is None or rr2 is None:
            return None
        return 0.5 * rr1 + 0.5 * rr2
    return None


def realized_return_pct_from_row(row) -> Optional[float]:
    entry = safe_float(row["activated_price"])
    if entry is None:
        entry = safe_float(row["ideal_entry"])

    resolution = str(row["resolution_status"] or "").upper()
    resolved_price = safe_float(row["resolved_price"])
    tp1_price = safe_float(row["tp1_price"])
    side = str(row["side"] or "").upper()

    if entry is None or side not in {"LONG", "SHORT"}:
        return None

    def leg_pct(exit_price: Optional[float]) -> Optional[float]:
        if exit_price is None:
            return None
        if side == "LONG":
            return ((exit_price - entry) / entry) * 100.0
        return ((entry - exit_price) / entry) * 100.0

    if resolution == "STOPPED":
        return leg_pct(resolved_price)

    if resolution in {"TP1_PARTIAL_THEN_BE", "TP2"}:
        p1 = leg_pct(tp1_price)
        p2 = leg_pct(resolved_price)
        if p1 is None or p2 is None:
            return None
        return 0.5 * p1 + 0.5 * p2

    return None


def load_trades(db_path: str, table_name: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(
            f"SELECT * FROM {table_name} ORDER BY COALESCE(resolved_ts, 0), created_ts",
            conn,
        )
    finally:
        conn.close()


def build_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["tp1_hit"] = out["tp1_hit"].fillna(0).astype(int)
    out["realized_r_multiple"] = out.apply(realized_r_from_row, axis=1)
    out["realized_return_pct"] = out.apply(realized_return_pct_from_row, axis=1)

    def lifecycle(row):
        resolution = str(row["resolution_status"] or "").upper()
        tp1_hit = int(row.get("tp1_hit", 0) or 0)
        if resolution == "STOPPED":
            return "ACTIVATED_STOPPED"
        if resolution == "TP1_PARTIAL_THEN_BE":
            return "ACTIVATED_TP1_PARTIAL_THEN_BE"
        if resolution == "TP2":
            return "ACTIVATED_TP1_THEN_TP2" if tp1_hit else "ACTIVATED_TP2"
        if resolution == "AMBIGUOUS":
            return "AMBIGUOUS"
        if resolution == "PENDING":
            return "PENDING"
        return resolution

    out["trade_lifecycle"] = out.apply(lifecycle, axis=1)
    out["tp1_touched"] = out["tp1_hit"].apply(lambda x: 1 if int(x) == 1 else 0)

    export_cols = [
        "resolved_ts", "symbol", "side", "status", "activation_status", "trade_lifecycle",
        "resolution_status", "tp1_touched", "tp1_hit", "tp1_hit_ts", "tp1_price",
        "breakout_context", "pullback_quality", "risk_quality", "reward_quality",
        "quality_score", "quality_grade", "ideal_entry", "activated_price",
        "invalidation", "tp1", "tp2", "resolved_price",
        "realized_return_pct", "realized_r_multiple", "resolution_note", "setup_id",
    ]
    export_cols = [c for c in export_cols if c in out.columns]
    return out[export_cols]


def print_summary(df: pd.DataFrame) -> None:
    resolved = df[df["resolution_status"] != "PENDING"].copy()
    activated = resolved[resolved["activation_status"] == "ACTIVE"].copy()
    non_amb = activated[activated["resolution_status"] != "AMBIGUOUS"].copy()

    print("\n=== ALL RESOLVED ROWS ===")
    print(f"total_resolved_rows: {len(resolved)}")
    print(f"tp1_touched_rows: {int(resolved['tp1_touched'].sum())}")
    print(f"wins_tp2: {int((resolved['resolution_status'] == 'TP2').sum())}")
    print(f"partial_be: {int((resolved['resolution_status'] == 'TP1_PARTIAL_THEN_BE').sum())}")
    print(f"losses: {int((resolved['resolution_status'] == 'STOPPED').sum())}")
    print(f"ambiguous: {int((resolved['resolution_status'] == 'AMBIGUOUS').sum())}")
    print(f"activated_rows: {len(activated)}")

    print("\n=== ACTIVATED TRADES ONLY ===")
    print(f"activated_rows: {len(activated)}")
    print(f"tp1_touched_rows: {int(activated['tp1_touched'].sum())}")
    print(f"wins_tp2: {int((activated['resolution_status'] == 'TP2').sum())}")
    print(f"partial_be: {int((activated['resolution_status'] == 'TP1_PARTIAL_THEN_BE').sum())}")
    print(f"losses: {int((activated['resolution_status'] == 'STOPPED').sum())}")
    print(f"ambiguous: {int((activated['resolution_status'] == 'AMBIGUOUS').sum())}")

    if len(non_amb) > 0:
        wins_or_partial = non_amb["resolution_status"].isin(["TP2", "TP1_PARTIAL_THEN_BE"]).sum()
        losses_only = (non_amb["resolution_status"] == "STOPPED").sum()
        win_rate = wins_or_partial / (wins_or_partial + losses_only) if (wins_or_partial + losses_only) else 0.0
        print(f"win_rate_non_ambiguous: {win_rate:.4f}")
        print(f"avg_realized_r_multiple: {non_amb['realized_r_multiple'].dropna().mean():.4f}")
        print(f"total_realized_r_multiple: {non_amb['realized_r_multiple'].dropna().sum():.4f}")

    print("\n=== LIFECYCLE BREAKDOWN ===")
    lifecycle = (
        resolved.groupby("trade_lifecycle")
        .agg(
            rows=("setup_id", "count"),
            tp1_touched=("tp1_touched", "sum"),
            avg_realized_r_multiple=("realized_r_multiple", "mean"),
            total_realized_r_multiple=("realized_r_multiple", "sum"),
        )
        .reset_index()
    )
    print(lifecycle.to_string(index=False))

    print("\n=== ACTIVATED BY SYMBOL ===")
    by_symbol = (
        activated.groupby("symbol")
        .agg(
            trades=("setup_id", "count"),
            tp1_touched=("tp1_touched", "sum"),
            tp1be=("resolution_status", lambda s: int((s == "TP1_PARTIAL_THEN_BE").sum())),
            tp2=("resolution_status", lambda s: int((s == "TP2").sum())),
            stopped=("resolution_status", lambda s: int((s == "STOPPED").sum())),
            avg_realized_r_multiple=("realized_r_multiple", "mean"),
        )
        .reset_index()
    )
    print(by_symbol.to_string(index=False))


def export_csv(df: pd.DataFrame, out_path: str) -> str:
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def main() -> None:
    df = load_trades(DB_PATH, TABLE_NAME)
    resolved = df[df["resolution_status"] != "PENDING"].copy()
    print(f"Loaded {len(resolved)} resolved rows from {DB_PATH} / table={TABLE_NAME}")
    export_df = build_export_dataframe(df)
    print_summary(export_df)
    csv_file = export_csv(export_df, CSV_PATH)
    print(f"\nCSV exported to: {Path(csv_file).resolve()}")


if __name__ == "__main__":
    main()
