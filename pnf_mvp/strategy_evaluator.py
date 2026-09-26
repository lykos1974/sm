# strategy_evaluator.py
"""
Evaluator for strategy_validation.py output.

Reads strategy_validation.db and evaluates the strategy_setups table.
Works even if the DB is missing, empty, or still has only pending rows.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from strategy_trade_export import (
    ECONOMIC_CHRONOLOGY_SQL,
    build_accounting_breakdown,
    build_accounting_summary,
    compute_trade_metrics,
)


DB_PATH = "strategy_validation.db"
TABLE_NAME = "strategy_setups"


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def load_data(db_path: str = DB_PATH) -> pd.DataFrame:
    db_file = Path(db_path)
    if not db_file.exists():
        raise FileNotFoundError(
            f"Database file not found: {db_file.resolve()}"
        )

    conn = connect(str(db_file))
    try:
        if not table_exists(conn, TABLE_NAME):
            tables = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
                conn,
            )
            known = ", ".join(tables["name"].tolist()) if not tables.empty else "(none)"
            raise RuntimeError(
                f"Table '{TABLE_NAME}' was not found in {db_file.name}. Existing tables: {known}"
            )

        df = pd.read_sql_query(
            f"SELECT * FROM {TABLE_NAME} ORDER BY {ECONOMIC_CHRONOLOGY_SQL}", conn
        )
        return df
    finally:
        conn.close()


def basic_stats(df: pd.DataFrame) -> dict:
    metrics = df if "r_pessimistic_bound" in df.columns else compute_trade_metrics(df)
    return build_accounting_summary(metrics)


def count_breakdown(df: pd.DataFrame, field: str) -> pd.DataFrame:
    if field not in df.columns:
        return pd.DataFrame()

    out = (
        df.groupby(field, dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", field], ascending=[False, True], na_position="last")
    )
    return out


def outcome_breakdown(df: pd.DataFrame, field: str) -> pd.DataFrame:
    if field not in df.columns or "resolution_status" not in df.columns:
        return pd.DataFrame()

    metrics = df if "r_pessimistic_bound" in df.columns else compute_trade_metrics(df)
    return build_accounting_breakdown(metrics, field).set_index(field)


def expectancy_breakdown(df: pd.DataFrame, field: str) -> pd.DataFrame:
    if field not in df.columns or "resolution_status" not in df.columns:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()
    metrics = compute_trade_metrics(df)
    return accounting_breakdown(metrics, field).set_index(field)


def accounting_breakdown(df: pd.DataFrame, field: str) -> pd.DataFrame:
    metrics = df if "r_pessimistic_bound" in df.columns else compute_trade_metrics(df)
    return build_accounting_breakdown(metrics, field)


def print_section(title: str, df: pd.DataFrame | None = None) -> None:
    print(f"\n=== {title} ===")
    if df is None:
        return
    if df.empty:
        print("(no data)")
    else:
        print(df.to_string())


def main() -> None:
    try:
        df = load_data(DB_PATH)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return

    print(f"Loaded {len(df)} rows from {DB_PATH} / table={TABLE_NAME}")

    print("\n=== BASIC STATS ===")
    stats = basic_stats(df)
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"{key}: {value:.4f}")
        else:
            print(f"{key}: {value}")

    print_section("COUNT BY STATUS", count_breakdown(df, "status"))
    print_section("COUNT BY SIDE", count_breakdown(df, "side"))
    print_section("COUNT BY BREAKOUT CONTEXT", count_breakdown(df, "breakout_context"))

    print_section("OUTCOME BY STATUS", outcome_breakdown(df, "status"))
    print_section("OUTCOME BY SIDE", outcome_breakdown(df, "side"))
    print_section("OUTCOME BY BREAKOUT CONTEXT", outcome_breakdown(df, "breakout_context"))
    print_section("OUTCOME BY PULLBACK QUALITY", outcome_breakdown(df, "pullback_quality"))
    print_section("OUTCOME BY RISK QUALITY", outcome_breakdown(df, "risk_quality"))

    print_section("EXPECTANCY BY STATUS", expectancy_breakdown(df, "status"))
    print_section("EXPECTANCY BY BREAKOUT CONTEXT", expectancy_breakdown(df, "breakout_context"))


if __name__ == "__main__":
    main()
