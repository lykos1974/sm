"""
strategy_trade_export.py

Activation-aware trade export from strategy_validation.db.

Exports:
- strategy_trades_export.csv
- strategy_tp2_review.csv
- strategy_stopped_review.csv
- strategy_diagnostics_breakdowns.csv
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import List

import pandas as pd


DB_PATH = "strategy_validation.db"
TABLE_NAME = "strategy_setups"
CSV_PATH = "strategy_trades_export.csv"
TP2_REVIEW_PATH = "strategy_tp2_review.csv"
STOPPED_REVIEW_PATH = "strategy_stopped_review.csv"
DIAG_BREAKDOWNS_PATH = "strategy_diagnostics_breakdowns.csv"

RESOLVED_STATUSES = (
    "TP1",
    "TP2",
    "STOPPED",
    "EXPIRED",
    "AMBIGUOUS",
    "TP1_PARTIAL_THEN_BE",
)


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


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(r["name"]) for r in rows]


def load_resolved_trades(db_path: str = DB_PATH) -> pd.DataFrame:
    db_file = Path(db_path)
    if not db_file.exists():
        raise FileNotFoundError(f"Database file not found: {db_file.resolve()}")

    conn = connect(str(db_file))
    try:
        if not table_exists(conn, TABLE_NAME):
            raise RuntimeError(f"Table '{TABLE_NAME}' not found in {db_file.name}")

        available = set(get_table_columns(conn, TABLE_NAME))

        wanted = [
            "setup_id",
            "created_ts",
            "updated_ts",
            "symbol",
            "strategy",
            "side",
            "status",
            "reference_ts",
            "horizon_bars",
            "bars_observed",
            "trend_state",
            "trend_regime",
            "immediate_slope",
            "breakout_context",
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
            "pullback_quality",
            "risk_quality",
            "reward_quality",
            "quality_score",
            "quality_grade",
            "reason",
            "reject_reason",
            "activation_status",
            "activated_ts",
            "activated_price",
            "tp1_hit",
            "tp1_hit_ts",
            "tp1_price",
            "resolution_status",
            "resolved_ts",
            "resolved_price",
            "resolution_note",
            "max_favorable_excursion",
            "max_adverse_excursion",
            "raw_setup_json",
        ]

        select_expr = []
        for col in wanted:
            if col in available:
                select_expr.append(col)
            else:
                select_expr.append(f"NULL AS {col}")

        placeholders = ",".join("?" for _ in RESOLVED_STATUSES)
        query = f"""
            SELECT
                {", ".join(select_expr)}
            FROM {TABLE_NAME}
            WHERE resolution_status IN ({placeholders})
            ORDER BY resolved_ts ASC, created_ts ASC
        """
        return pd.read_sql_query(query, conn, params=list(RESOLVED_STATUSES))
    finally:
        conn.close()


def compute_trade_metrics(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "setup_id",
        "created_ts",
        "updated_ts",
        "reference_ts",
        "resolved_ts",
        "symbol",
        "strategy",
        "side",
        "status",
        "activation_status",
        "trade_lifecycle",
        "resolution_status",
        "breakout_context",
        "pullback_quality",
        "risk_quality",
        "reward_quality",
        "quality_score",
        "quality_grade",
        "continuation_strength_v1",
        "ideal_entry",
        "activated_ts",
        "activated_price",
        "entry_price",
        "invalidation",
        "tp1",
        "tp2",
        "exit_price",
        "tp1_hit",
        "tp1_hit_ts",
        "tp1_price",
        "realized_return_pct",
        "realized_r_multiple",
        "outcome_r_multiple_proxy",
        "consistency_flag",
        "bars_observed",
        "trend_state",
        "trend_regime",
        "immediate_slope",
        "is_extended_move",
        "active_leg_boxes",
        "reason",
        "reject_reason",
        "resolution_note",
    ]

    if df.empty:
        return pd.DataFrame(columns=cols)

    out = df.copy()

    out["activation_status"] = out["activation_status"].fillna("UNKNOWN")
    out["tp1_hit"] = pd.to_numeric(out["tp1_hit"], errors="coerce").fillna(0).astype(int)
    out["quality_score"] = pd.to_numeric(out["quality_score"], errors="coerce")
    out["active_leg_boxes"] = pd.to_numeric(out["active_leg_boxes"], errors="coerce")
    out["is_extended_move"] = out["is_extended_move"].fillna(0).astype(int)

    def lifecycle(row: pd.Series) -> str:
        if str(row["activation_status"]).upper() not in ("ACTIVE",):
            return "NEVER_ACTIVATED"
        status = str(row["resolution_status"]).upper()
        if status == "TP1_PARTIAL_THEN_BE":
            return "ACTIVATED_TP1_PARTIAL_THEN_BE"
        return f"ACTIVATED_{status}"

    out["trade_lifecycle"] = out.apply(lifecycle, axis=1)
    out["entry_price"] = out["activated_price"].where(out["activated_price"].notna(), out["ideal_entry"])
    out["exit_price"] = out["resolved_price"]

    def calc_realized_return_pct(row: pd.Series):
        entry = row["entry_price"]
        exit_price = row["exit_price"]
        side = str(row["side"]).upper()
        if pd.isna(entry) or pd.isna(exit_price) or entry == 0:
            return None
        if side == "LONG":
            return ((exit_price - entry) / entry) * 100.0
        if side == "SHORT":
            return ((entry - exit_price) / entry) * 100.0
        return None

    def calc_realized_r_multiple(row: pd.Series):
        entry = row["entry_price"]
        stop = row["invalidation"]
        exit_price = row["exit_price"]
        side = str(row["side"]).upper()
        if pd.isna(entry) or pd.isna(stop) or pd.isna(exit_price):
            return None
        denom = abs(entry - stop)
        if denom <= 0:
            return None
        if side == "LONG":
            return (exit_price - entry) / denom
        if side == "SHORT":
            return (entry - exit_price) / denom
        return None

    rr_map = {
        "TP1": 2.0,
        "TP2": 3.0,
        "STOPPED": -1.0,
        "EXPIRED": 0.0,
        "AMBIGUOUS": 0.0,
        "TP1_PARTIAL_THEN_BE": 1.0,
    }

    out["realized_return_pct"] = out.apply(calc_realized_return_pct, axis=1)
    out["realized_r_multiple"] = out.apply(calc_realized_r_multiple, axis=1)
    out["outcome_r_multiple_proxy"] = out["resolution_status"].map(rr_map)

    def consistency_flag(row: pd.Series) -> str:
        status = str(row["resolution_status"]).upper()
        realized_r = row["realized_r_multiple"]
        if pd.isna(realized_r):
            return "NO_PRICE_METRIC"
        if status in ("TP1", "TP2", "TP1_PARTIAL_THEN_BE") and realized_r < 0:
            return "INCONSISTENT_WIN_NEGATIVE_R"
        if status == "STOPPED" and realized_r > 0:
            return "INCONSISTENT_STOP_POSITIVE_R"
        return "OK"

    out["consistency_flag"] = out.apply(consistency_flag, axis=1)

    def extract_continuation_strength_v1(row: pd.Series):
        raw = row.get("raw_setup_json")
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                value = parsed.get("continuation_strength_v1")
                if value is not None:
                    return value
            except Exception:
                pass
        return row.get("continuation_strength_v1")

    out["continuation_strength_v1"] = pd.to_numeric(
        out.apply(extract_continuation_strength_v1, axis=1),
        errors="coerce",
    )

    return out[[c for c in cols if c in out.columns]]


def build_summary_all_resolved(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "total_resolved_rows": 0,
            "wins": 0,
            "partial_be": 0,
            "losses": 0,
            "expired": 0,
            "ambiguous": 0,
            "never_activated": 0,
            "activated_rows": 0,
            "inconsistent_rows": 0,
        }

    never_activated = int((df["trade_lifecycle"] == "NEVER_ACTIVATED").sum())
    activated_rows = int((df["trade_lifecycle"] != "NEVER_ACTIVATED").sum())
    wins = int((df["resolution_status"] == "TP2").sum())
    partial_be = int((df["resolution_status"] == "TP1_PARTIAL_THEN_BE").sum())
    losses = int((df["resolution_status"] == "STOPPED").sum())
    expired = int((df["resolution_status"] == "EXPIRED").sum())
    ambiguous = int((df["resolution_status"] == "AMBIGUOUS").sum())
    inconsistent = int((df["consistency_flag"] != "OK").sum())

    return {
        "total_resolved_rows": int(len(df)),
        "wins": wins,
        "partial_be": partial_be,
        "losses": losses,
        "expired": expired,
        "ambiguous": ambiguous,
        "never_activated": never_activated,
        "activated_rows": activated_rows,
        "inconsistent_rows": inconsistent,
    }


def build_summary_activated_only(df: pd.DataFrame) -> dict:
    if df.empty or "trade_lifecycle" not in df.columns:
        return {
            "activated_rows": 0,
            "wins": 0,
            "partial_be": 0,
            "losses": 0,
            "expired_after_activation": 0,
            "ambiguous": 0,
            "win_rate_non_ambiguous": 0.0,
            "avg_realized_return_pct": 0.0,
            "median_realized_return_pct": 0.0,
            "avg_realized_r_multiple": 0.0,
            "median_realized_r_multiple": 0.0,
            "total_realized_r_multiple": 0.0,
            "avg_outcome_r_proxy": 0.0,
            "total_outcome_r_proxy": 0.0,
            "inconsistent_rows": 0,
        }

    active = df[df["trade_lifecycle"] != "NEVER_ACTIVATED"].copy()
    if active.empty:
        return {
            "activated_rows": 0,
            "wins": 0,
            "partial_be": 0,
            "losses": 0,
            "expired_after_activation": 0,
            "ambiguous": 0,
            "win_rate_non_ambiguous": 0.0,
            "avg_realized_return_pct": 0.0,
            "median_realized_return_pct": 0.0,
            "avg_realized_r_multiple": 0.0,
            "median_realized_r_multiple": 0.0,
            "total_realized_r_multiple": 0.0,
            "avg_outcome_r_proxy": 0.0,
            "total_outcome_r_proxy": 0.0,
            "inconsistent_rows": 0,
        }

    wins = int((active["resolution_status"] == "TP2").sum())
    partial_be = int((active["resolution_status"] == "TP1_PARTIAL_THEN_BE").sum())
    losses = int((active["resolution_status"] == "STOPPED").sum())
    ambiguous = int((active["resolution_status"] == "AMBIGUOUS").sum())
    expired = int((active["resolution_status"] == "EXPIRED").sum())
    wl_den = wins + partial_be + losses
    inconsistent = int((active["consistency_flag"] != "OK").sum())

    return {
        "activated_rows": int(len(active)),
        "wins": wins,
        "partial_be": partial_be,
        "losses": losses,
        "expired_after_activation": expired,
        "ambiguous": ambiguous,
        "win_rate_non_ambiguous": float((wins + partial_be) / wl_den) if wl_den else 0.0,
        "avg_realized_return_pct": float(active["realized_return_pct"].dropna().mean()) if active["realized_return_pct"].notna().any() else 0.0,
        "median_realized_return_pct": float(active["realized_return_pct"].dropna().median()) if active["realized_return_pct"].notna().any() else 0.0,
        "avg_realized_r_multiple": float(active["realized_r_multiple"].dropna().mean()) if active["realized_r_multiple"].notna().any() else 0.0,
        "median_realized_r_multiple": float(active["realized_r_multiple"].dropna().median()) if active["realized_r_multiple"].notna().any() else 0.0,
        "total_realized_r_multiple": float(active["realized_r_multiple"].dropna().sum()) if active["realized_r_multiple"].notna().any() else 0.0,
        "avg_outcome_r_proxy": float(active["outcome_r_multiple_proxy"].dropna().mean()) if active["outcome_r_multiple_proxy"].notna().any() else 0.0,
        "total_outcome_r_proxy": float(active["outcome_r_multiple_proxy"].dropna().sum()) if active["outcome_r_multiple_proxy"].notna().any() else 0.0,
        "inconsistent_rows": inconsistent,
    }


def print_summary(title: str, summary: dict) -> None:
    print(f"\n=== {title} ===")
    for key, value in summary.items():
        if isinstance(value, float):
            print(f"{key}: {value:.4f}")
        else:
            print(f"{key}: {value}")


def print_df(title: str, table: pd.DataFrame) -> None:
    print(f"\n=== {title} ===")
    if table.empty:
        print("(empty)")
    else:
        print(table.to_string(index=False))


def build_score_bucket_breakdown(active: pd.DataFrame) -> pd.DataFrame:
    if active.empty:
        return pd.DataFrame(columns=["section", "group", "trades", "tp1_touched", "tp2", "stopped", "avg_r", "tp1_rate", "tp2_rate"])

    bucketed = active.copy()

    def score_bucket(s: float) -> str:
        if pd.isna(s):
            return "UNKNOWN"
        if s < 70:
            return "0-69"
        if s < 80:
            return "70-79"
        if s < 90:
            return "80-89"
        return "90-100"

    bucketed["group"] = bucketed["quality_score"].apply(score_bucket)

    out = (
        bucketed.groupby("group")
        .agg(
            trades=("setup_id", "count"),
            tp1_touched=("tp1_hit", "sum"),
            tp2=("resolution_status", lambda x: (x == "TP2").sum()),
            stopped=("resolution_status", lambda x: (x == "STOPPED").sum()),
            avg_r=("realized_r_multiple", "mean"),
        )
        .reset_index()
    )
    out["tp1_rate"] = out["tp1_touched"] / out["trades"]
    out["tp2_rate"] = out["tp2"] / out["trades"]
    out.insert(0, "section", "score_bucket")
    return out.sort_values("group")


def build_pullback_side_breakdown(active: pd.DataFrame) -> pd.DataFrame:
    if active.empty:
        return pd.DataFrame(columns=["section", "group", "trades", "tp1_touched", "tp2", "stopped", "avg_r", "tp1_rate", "tp2_rate"])

    bucketed = active.copy()
    bucketed["group"] = (
        bucketed["side"].fillna("UNKNOWN").astype(str)
        + "_"
        + bucketed["pullback_quality"].fillna("UNKNOWN").astype(str)
    )

    out = (
        bucketed.groupby("group")
        .agg(
            trades=("setup_id", "count"),
            tp1_touched=("tp1_hit", "sum"),
            tp2=("resolution_status", lambda x: (x == "TP2").sum()),
            stopped=("resolution_status", lambda x: (x == "STOPPED").sum()),
            avg_r=("realized_r_multiple", "mean"),
        )
        .reset_index()
    )
    out["tp1_rate"] = out["tp1_touched"] / out["trades"]
    out["tp2_rate"] = out["tp2"] / out["trades"]
    out.insert(0, "section", "pullback_side")
    return out.sort_values("group")


def build_active_leg_boxes_breakdown(active: pd.DataFrame) -> pd.DataFrame:
    if active.empty:
        return pd.DataFrame(columns=["section", "group", "trades", "tp1_touched", "tp2", "stopped", "avg_r", "tp1_rate", "tp2_rate"])

    bucketed = active.copy()

    def leg_bucket(x: float) -> str:
        if pd.isna(x):
            return "UNKNOWN"
        i = int(x)
        if i == 1:
            return "1"
        if i == 2:
            return "2"
        if i == 3:
            return "3"
        return "4+"

    bucketed["group"] = bucketed["active_leg_boxes"].apply(leg_bucket)

    out = (
        bucketed.groupby("group")
        .agg(
            trades=("setup_id", "count"),
            tp1_touched=("tp1_hit", "sum"),
            tp2=("resolution_status", lambda x: (x == "TP2").sum()),
            stopped=("resolution_status", lambda x: (x == "STOPPED").sum()),
            avg_r=("realized_r_multiple", "mean"),
        )
        .reset_index()
    )
    out["tp1_rate"] = out["tp1_touched"] / out["trades"]
    out["tp2_rate"] = out["tp2"] / out["trades"]
    out.insert(0, "section", "active_leg_boxes")
    order = {"1": 1, "2": 2, "3": 3, "4+": 4, "UNKNOWN": 99}
    out["_ord"] = out["group"].map(order).fillna(999)
    out = out.sort_values("_ord").drop(columns="_ord")
    return out


def build_continuation_strength_v1_breakdown(active: pd.DataFrame) -> pd.DataFrame:
    if active.empty:
        return pd.DataFrame(columns=["section", "group", "trades", "tp1_touched", "tp2", "stopped", "avg_r", "tp1_rate", "tp2_rate"])

    bucketed = active.copy()

    def cs_bucket(x: float) -> str:
        if pd.isna(x):
            return "UNKNOWN"
        v = float(x)
        if v < 35:
            return "0-34"
        if v < 50:
            return "35-49"
        if v < 65:
            return "50-64"
        if v < 80:
            return "65-79"
        return "80-100"

    bucketed["group"] = bucketed["continuation_strength_v1"].apply(cs_bucket)

    out = (
        bucketed.groupby("group")
        .agg(
            trades=("setup_id", "count"),
            tp1_touched=("tp1_hit", "sum"),
            tp2=("resolution_status", lambda x: (x == "TP2").sum()),
            stopped=("resolution_status", lambda x: (x == "STOPPED").sum()),
            avg_r=("realized_r_multiple", "mean"),
        )
        .reset_index()
    )
    out["tp1_rate"] = out["tp1_touched"] / out["trades"]
    out["tp2_rate"] = out["tp2"] / out["trades"]
    out.insert(0, "section", "continuation_strength_v1")
    order = {"0-34": 1, "35-49": 2, "50-64": 3, "65-79": 4, "80-100": 5, "UNKNOWN": 99}
    out["_ord"] = out["group"].map(order).fillna(999)
    out = out.sort_values("_ord").drop(columns="_ord")
    return out


def build_tp1_to_tp2_conversion(active: pd.DataFrame) -> pd.DataFrame:
    tp1_df = active[active["tp1_hit"] == 1].copy()
    if tp1_df.empty:
        return pd.DataFrame(columns=["section", "group", "tp1_trades", "tp2", "tp2_after_tp1_rate"])

    out = (
        tp1_df.groupby("side")
        .agg(
            tp1_trades=("setup_id", "count"),
            tp2=("resolution_status", lambda x: (x == "TP2").sum()),
        )
        .reset_index()
        .rename(columns={"side": "group"})
    )
    out["tp2_after_tp1_rate"] = out["tp2"] / out["tp1_trades"]
    out.insert(0, "section", "tp1_to_tp2_conversion")
    return out.sort_values("group")


def build_review_export(df: pd.DataFrame, resolution_status: str) -> pd.DataFrame:
    subset = df[df["resolution_status"] == resolution_status].copy()
    if subset.empty:
        return subset

    review_cols = [
        "symbol",
        "side",
        "resolution_status",
        "quality_score",
        "quality_grade",
        "pullback_quality",
        "risk_quality",
        "reward_quality",
        "breakout_context",
        "trend_state",
        "trend_regime",
        "immediate_slope",
        "is_extended_move",
        "active_leg_boxes",
        "tp1_hit",
        "ideal_entry",
        "invalidation",
        "tp1",
        "tp2",
        "entry_price",
        "exit_price",
        "realized_r_multiple",
        "reason",
        "resolution_note",
        "reference_ts",
        "activated_ts",
        "resolved_ts",
        "setup_id",
    ]
    review_cols = [c for c in review_cols if c in subset.columns]

    sort_cols = [c for c in ["symbol", "reference_ts", "resolved_ts"] if c in subset.columns]
    if sort_cols:
        subset = subset.sort_values(sort_cols, ascending=[True] * len(sort_cols))
    return subset[review_cols]


def print_breakdowns(df: pd.DataFrame) -> None:
    if df.empty:
        print("\n(no resolved trades yet)")
        return

    print("\n=== CONSISTENCY BREAKDOWN ===")
    consistency = (
        df.groupby("consistency_flag")
        .agg(
            rows=("setup_id", "count"),
            avg_realized_r_multiple=("realized_r_multiple", "mean"),
            avg_outcome_r_proxy=("outcome_r_multiple_proxy", "mean"),
        )
        .sort_values("rows", ascending=False)
    )
    print(consistency.to_string())

    print("\n=== LIFECYCLE BREAKDOWN ===")
    lifecycle = (
        df.groupby("trade_lifecycle")
        .agg(
            rows=("setup_id", "count"),
            avg_realized_return_pct=("realized_return_pct", "mean"),
            avg_realized_r_multiple=("realized_r_multiple", "mean"),
            total_realized_r_multiple=("realized_r_multiple", "sum"),
            avg_outcome_r_proxy=("outcome_r_multiple_proxy", "mean"),
            total_outcome_r_proxy=("outcome_r_multiple_proxy", "sum"),
        )
        .sort_values("rows", ascending=False)
    )
    print(lifecycle.to_string())

    active = df[df["trade_lifecycle"] != "NEVER_ACTIVATED"].copy()
    if active.empty:
        return

    print("\n=== ACTIVATED BY SIDE ===")
    by_side = (
        active.groupby("side")
        .agg(
            trades=("setup_id", "count"),
            avg_realized_r_multiple=("realized_r_multiple", "mean"),
            total_realized_r_multiple=("realized_r_multiple", "sum"),
            avg_outcome_r_proxy=("outcome_r_multiple_proxy", "mean"),
            total_outcome_r_proxy=("outcome_r_multiple_proxy", "sum"),
        )
        .sort_values("trades", ascending=False)
    )
    print(by_side.to_string())

    print("\n=== ACTIVATED BY STATUS ===")
    by_status = (
        active.groupby("status")
        .agg(
            trades=("setup_id", "count"),
            avg_realized_r_multiple=("realized_r_multiple", "mean"),
            total_realized_r_multiple=("realized_r_multiple", "sum"),
            avg_outcome_r_proxy=("outcome_r_multiple_proxy", "mean"),
            total_outcome_r_proxy=("outcome_r_multiple_proxy", "sum"),
        )
        .sort_values("trades", ascending=False)
    )
    print(by_status.to_string())

    print("\n=== ACTIVATED BY BREAKOUT CONTEXT ===")
    by_context = (
        active.groupby("breakout_context")
        .agg(
            trades=("setup_id", "count"),
            avg_realized_r_multiple=("realized_r_multiple", "mean"),
            total_realized_r_multiple=("realized_r_multiple", "sum"),
            avg_outcome_r_proxy=("outcome_r_multiple_proxy", "mean"),
            total_outcome_r_proxy=("outcome_r_multiple_proxy", "sum"),
        )
        .sort_values("trades", ascending=False)
    )
    print(by_context.to_string())

    print_df("SCORE BUCKET BREAKDOWN", build_score_bucket_breakdown(active))
    print_df("CONTINUATION STRENGTH V1 BREAKDOWN", build_continuation_strength_v1_breakdown(active))
    print_df("PULLBACK + SIDE BREAKDOWN", build_pullback_side_breakdown(active))
    print_df("ACTIVE LEG BOXES BREAKDOWN", build_active_leg_boxes_breakdown(active))
    print_df("TP1 -> TP2 CONVERSION", build_tp1_to_tp2_conversion(active))


def export_csv(df: pd.DataFrame, csv_path: str) -> str:
    out_path = Path(csv_path).resolve()
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return str(out_path)


def build_diagnostics_export(active: pd.DataFrame) -> pd.DataFrame:
    tables = [
        build_score_bucket_breakdown(active),
        build_continuation_strength_v1_breakdown(active),
        build_pullback_side_breakdown(active),
        build_active_leg_boxes_breakdown(active),
    ]
    merged = pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()

    conv = build_tp1_to_tp2_conversion(active)
    if not conv.empty:
        conv2 = conv.copy()
        if "tp1_trades" in conv2.columns:
            conv2 = conv2.rename(columns={"tp1_trades": "trades"})
        if "tp2_after_tp1_rate" in conv2.columns:
            conv2["tp1_rate"] = None
            conv2["tp2_rate"] = conv2["tp2_after_tp1_rate"]
        merged = pd.concat([merged, conv2], ignore_index=True, sort=False)

    return merged


def main() -> None:
    try:
        raw = load_resolved_trades(DB_PATH)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return

    trades = compute_trade_metrics(raw)
    print(f"Loaded {len(trades)} resolved rows from {DB_PATH} / table={TABLE_NAME}")

    print_summary("ALL RESOLVED ROWS", build_summary_all_resolved(trades))
    print_summary("ACTIVATED TRADES ONLY", build_summary_activated_only(trades))
    print_breakdowns(trades)

    active = trades[trades["trade_lifecycle"] != "NEVER_ACTIVATED"].copy()

    csv_file = export_csv(trades, CSV_PATH)
    tp2_file = export_csv(build_review_export(active, "TP2"), TP2_REVIEW_PATH)
    stopped_file = export_csv(build_review_export(active, "STOPPED"), STOPPED_REVIEW_PATH)
    diag_file = export_csv(build_diagnostics_export(active), DIAG_BREAKDOWNS_PATH)

    print(f"\nCSV exported to: {csv_file}")
    print(f"TP2 review exported to: {tp2_file}")
    print(f"STOPPED review exported to: {stopped_file}")
    print(f"Diagnostics exported to: {diag_file}")


if __name__ == "__main__":
    main()
