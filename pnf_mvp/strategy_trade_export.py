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

import argparse
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


def load_validation_rows(db_path: str = DB_PATH) -> pd.DataFrame:
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
            "cs_geometry_component",
            "cs_profile_tag",
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
            "activation_tick_size",
            "activation_tick_source",
            "ambiguous_pessimistic_r",
            "ambiguous_optimistic_r",
            "raw_setup_json",
        ]

        select_expr = []
        for col in wanted:
            if col in available:
                select_expr.append(col)
            else:
                select_expr.append(f"NULL AS {col}")

        query = f"""
            SELECT
                {", ".join(select_expr)}
            FROM {TABLE_NAME}
            ORDER BY created_ts ASC, setup_id ASC
        """
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def load_resolved_trades(db_path: str = DB_PATH) -> pd.DataFrame:
    """Compatibility alias; accounting now requires every registered setup."""
    return load_validation_rows(db_path)


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
        "cs_geometry_component",
        "cs_profile_tag",
        "pullback_position_bucket",
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
        "r_pessimistic_bound",
        "r_optimistic_bound",
        "activation_tick_size",
        "activation_tick_source",
        "ambiguous_pessimistic_r",
        "ambiguous_optimistic_r",
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
    for col in (
        "activation_tick_size",
        "ambiguous_pessimistic_r",
        "ambiguous_optimistic_r",
    ):
        if col not in out.columns:
            out[col] = None
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "activation_tick_source" not in out.columns:
        out["activation_tick_source"] = None

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
        status = str(row["resolution_status"]).upper()
        if status == "AMBIGUOUS":
            return None
        if pd.isna(entry) or pd.isna(stop) or pd.isna(exit_price):
            return None
        denom = abs(entry - stop)
        if denom <= 0:
            return None
        exit_r = (
            (exit_price - entry) / denom
            if side == "LONG"
            else (entry - exit_price) / denom
            if side == "SHORT"
            else None
        )
        if exit_r is None:
            return None
        if status == "STOPPED":
            return -1.0
        if status in ("TP2", "TP1_PARTIAL_THEN_BE") and int(row.get("tp1_hit") or 0):
            first_price = row.get("tp1_price")
            if pd.isna(first_price):
                return None
            first_r = (
                (first_price - entry) / denom
                if side == "LONG"
                else (entry - first_price) / denom
            )
            return 0.5 * first_r + 0.5 * exit_r
        return exit_r

    out["realized_return_pct"] = out.apply(calc_realized_return_pct, axis=1)
    out["realized_r_multiple"] = out.apply(calc_realized_r_multiple, axis=1)
    out["r_pessimistic_bound"] = out["realized_r_multiple"].where(
        out["resolution_status"] != "AMBIGUOUS", out["ambiguous_pessimistic_r"]
    )
    out["r_optimistic_bound"] = out["realized_r_multiple"].where(
        out["resolution_status"] != "AMBIGUOUS", out["ambiguous_optimistic_r"]
    )

    def consistency_flag(row: pd.Series) -> str:
        status = str(row["resolution_status"]).upper()
        realized_r = row["realized_r_multiple"]
        if status == "AMBIGUOUS":
            if pd.notna(row["ambiguous_pessimistic_r"]) and pd.notna(
                row["ambiguous_optimistic_r"]
            ):
                return "OK"
            return "INCONSISTENT_AMBIGUOUS_MISSING_BOUNDS"
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

    def extract_cs_geometry_component(row: pd.Series):
        raw = row.get("raw_setup_json")
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                value = parsed.get("cs_geometry_component")
                if isinstance(value, str) and value.strip():
                    return value
            except Exception:
                pass

        existing = row.get("cs_geometry_component")
        if isinstance(existing, str) and existing.strip():
            return existing

        pq = str(row.get("pullback_quality") or "").strip().upper()
        if pq:
            return f"{pq}_GEOMETRY"
        return "UNCLASSIFIED_GEOMETRY"

    def extract_cs_profile_tag(row: pd.Series):
        raw = row.get("raw_setup_json")
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                value = parsed.get("cs_profile_tag")
                if isinstance(value, str) and value.strip():
                    return value
            except Exception:
                pass

        existing = row.get("cs_profile_tag")
        if isinstance(existing, str) and existing.strip():
            return existing

        side = str(row.get("side") or "UNKNOWN_SIDE").upper()
        breakout_context = str(row.get("breakout_context") or "NO_BREAKOUT_CONTEXT").upper()
        trend_regime = str(row.get("trend_regime") or "NO_TREND_REGIME").upper()
        is_extended = int(row.get("is_extended_move") or 0)
        ext_tag = "EXTENDED" if is_extended else "NON_EXTENDED"
        return f"{side}_{breakout_context}__{trend_regime}__{ext_tag}"

    out["cs_geometry_component"] = out.apply(extract_cs_geometry_component, axis=1)
    out["cs_profile_tag"] = out.apply(extract_cs_profile_tag, axis=1)

    def extract_pullback_position_bucket(row: pd.Series):
        raw = row.get("raw_setup_json")
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                value = parsed.get("pullback_position_bucket")
                if isinstance(value, str) and value.strip():
                    return value.strip().upper()
            except Exception:
                pass
        return "UNKNOWN"

    out["pullback_position_bucket"] = out.apply(extract_pullback_position_bucket, axis=1)

    return out[[c for c in cols if c in out.columns]]


def _max_drawdown(values: list[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    maximum = 0.0
    for value in values:
        cumulative += float(value)
        peak = max(peak, cumulative)
        maximum = max(maximum, peak - cumulative)
    return maximum


def build_accounting_summary(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "registered_rows": 0,
            "resolved_rows": 0,
            "ambiguous_branched_rows": 0,
            "pending_rows": 0,
            "expired_rows": 0,
            "missed_rows": 0,
            "unclassified_rows": 0,
            "headline_denominator_rows": 0,
            "total_r_pessimistic": 0.0,
            "total_r_optimistic": 0.0,
            "expectancy_r_pessimistic": 0.0,
            "expectancy_r_optimistic": 0.0,
            "win_rate_pessimistic": 0.0,
            "win_rate_optimistic": 0.0,
            "max_drawdown_r_pessimistic": 0.0,
            "max_drawdown_r_optimistic": 0.0,
        }

    work = df.copy()
    statuses = work["resolution_status"].fillna("PENDING").astype(str).str.upper()
    activation = work.get(
        "activation_status", pd.Series("PENDING", index=work.index)
    ).fillna("PENDING").astype(str).str.upper()
    resolved_mask = statuses.isin({"TP1", "TP2", "STOPPED", "TP1_PARTIAL_THEN_BE"})
    ambiguous_mask = statuses.eq("AMBIGUOUS")
    pending_mask = statuses.eq("PENDING")
    expired_mask = statuses.eq("EXPIRED") & activation.eq("ACTIVE")
    missed_mask = statuses.eq("EXPIRED") & ~activation.eq("ACTIVE")
    classified = resolved_mask | ambiguous_mask | pending_mask | expired_mask | missed_mask
    headline_mask = resolved_mask | ambiguous_mask
    headline = work.loc[headline_mask].copy()
    denominator = int(len(headline))
    lower = pd.to_numeric(headline.get("r_pessimistic_bound"), errors="coerce")
    upper = pd.to_numeric(headline.get("r_optimistic_bound"), errors="coerce")
    bounds_complete = bool(lower.notna().all() and upper.notna().all())

    if denominator and bounds_complete:
        total_lower = float(lower.sum())
        total_upper = float(upper.sum())
        expectancy_lower = total_lower / denominator
        expectancy_upper = total_upper / denominator
        win_lower = float((lower > 0).sum() / denominator)
        win_upper = float((upper > 0).sum() / denominator)
        drawdown_lower = _max_drawdown(lower.tolist())
        drawdown_upper = _max_drawdown(upper.tolist())
    elif denominator:
        total_lower = total_upper = None
        expectancy_lower = expectancy_upper = None
        win_lower = win_upper = None
        drawdown_lower = drawdown_upper = None
    else:
        total_lower = total_upper = 0.0
        expectancy_lower = expectancy_upper = 0.0
        win_lower = win_upper = 0.0
        drawdown_lower = drawdown_upper = 0.0

    return {
        "registered_rows": int(len(work)),
        "resolved_rows": int(resolved_mask.sum()),
        "ambiguous_branched_rows": int(ambiguous_mask.sum()),
        "pending_rows": int(pending_mask.sum()),
        "expired_rows": int(expired_mask.sum()),
        "missed_rows": int(missed_mask.sum()),
        "unclassified_rows": int((~classified).sum()),
        "headline_denominator_rows": denominator,
        "total_r_pessimistic": total_lower,
        "total_r_optimistic": total_upper,
        "expectancy_r_pessimistic": expectancy_lower,
        "expectancy_r_optimistic": expectancy_upper,
        "win_rate_pessimistic": win_lower,
        "win_rate_optimistic": win_upper,
        "max_drawdown_r_pessimistic": drawdown_lower,
        "max_drawdown_r_optimistic": drawdown_upper,
    }


def build_accounting_breakdown(df: pd.DataFrame, field: str) -> pd.DataFrame:
    columns = [field, *build_accounting_summary(pd.DataFrame()).keys()]
    if df.empty or field not in df.columns:
        return pd.DataFrame(columns=columns)
    rows = []
    for group, subset in df.groupby(field, dropna=False, sort=True):
        rows.append({field: group, **build_accounting_summary(subset)})
    return pd.DataFrame(rows, columns=columns)


def _bucketed_accounting(
    df: pd.DataFrame, *, section: str, groups: pd.Series
) -> pd.DataFrame:
    work = df.copy()
    work["group"] = groups.reindex(work.index).fillna("UNKNOWN").astype(str)
    out = build_accounting_breakdown(work, "group")
    out.insert(0, "section", section)
    return out


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
        print("\n(no validation setups yet)")
        return
    for field in (
        "trade_lifecycle",
        "side",
        "status",
        "breakout_context",
        "pullback_quality",
        "risk_quality",
        "cs_geometry_component",
        "cs_profile_tag",
    ):
        print_df(
            f"ACCOUNTING BY {field.upper()}",
            build_accounting_breakdown(df, field),
        )


def export_csv(df: pd.DataFrame, csv_path: str) -> str:
    out_path = Path(csv_path).resolve()
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return str(out_path)


def build_diagnostics_export(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["section", "group", *build_accounting_summary(df).keys()])

    tables = []
    for field in (
        "side",
        "status",
        "breakout_context",
        "pullback_quality",
        "risk_quality",
        "cs_geometry_component",
        "cs_profile_tag",
        "trade_lifecycle",
    ):
        groups = df[field] if field in df.columns else pd.Series("UNKNOWN", index=df.index)
        tables.append(_bucketed_accounting(df, section=field, groups=groups))

    score = pd.to_numeric(df["quality_score"], errors="coerce")
    score_groups = score.apply(
        lambda value: "UNKNOWN" if pd.isna(value) else (
            "0-69" if value < 70 else "70-79" if value < 80 else "80-89" if value < 90 else "90-100"
        )
    )
    tables.append(_bucketed_accounting(df, section="score_bucket", groups=score_groups))

    leg = pd.to_numeric(df["active_leg_boxes"], errors="coerce")
    leg_groups = leg.apply(
        lambda value: "UNKNOWN" if pd.isna(value) else str(int(value)) if int(value) <= 3 else "4+"
    )
    tables.append(_bucketed_accounting(df, section="active_leg_boxes", groups=leg_groups))

    side_values = df.get("side", pd.Series("UNKNOWN", index=df.index))
    pullback_values = df.get(
        "pullback_quality", pd.Series("UNKNOWN", index=df.index)
    )
    pullback_side = (
        side_values.fillna("UNKNOWN").astype(str)
        + "_"
        + pullback_values.fillna("UNKNOWN").astype(str)
    )
    tables.append(_bucketed_accounting(df, section="pullback_side", groups=pullback_side))
    return pd.concat(tables, ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Activation-aware trade export from strategy_validation.db")
    parser.add_argument("--db-path", default=DB_PATH, help="Path to strategy validation sqlite database")
    args = parser.parse_args()

    try:
        raw = load_validation_rows(args.db_path)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return

    trades = compute_trade_metrics(raw)
    print(f"Loaded {len(trades)} validation rows from {args.db_path} / table={TABLE_NAME}")

    print_summary("RECONCILED VALIDATION ACCOUNTING", build_accounting_summary(trades))
    print_breakdowns(trades)

    active = trades[trades["trade_lifecycle"] != "NEVER_ACTIVATED"].copy()

    csv_file = export_csv(trades, CSV_PATH)
    tp2_file = export_csv(build_review_export(active, "TP2"), TP2_REVIEW_PATH)
    stopped_file = export_csv(build_review_export(active, "STOPPED"), STOPPED_REVIEW_PATH)
    diag_file = export_csv(build_diagnostics_export(trades), DIAG_BREAKDOWNS_PATH)

    print(f"\nCSV exported to: {csv_file}")
    print(f"TP2 review exported to: {tp2_file}")
    print(f"STOPPED review exported to: {stopped_file}")
    print(f"Diagnostics exported to: {diag_file}")


if __name__ == "__main__":
    main()
