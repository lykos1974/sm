"""Research-only portfolio reality audit for the validated PnF pole execution baseline.

This module intentionally keeps the execution baseline fixed:
- PENDING_LIMIT_THROUGH_ENTRY_WITH_EXPIRY at the intended NEXT_COLUMN_OPEN_ENTRY price
- fixed three-box stop
- fixed 2.5R target
- break-even stop after +2R

It does not modify production strategy code, optimize parameters, activate live trading,
or output PROMOTE. The audit only converts resolved fixed-baseline trade outcomes into
portfolio-level R accounting under simple fixed-fractional assumptions.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from research_v2.patterns.pole_be_research_audit import _be_classify
from research_v2.patterns.pole_core_motif_entry_timing_audit import (
    EntryTimingObservation,
)
from research_v2.patterns.pole_core_motif_execution_reality_audit import Opportunity
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import (
    ENTRY_CANDIDATE,
    _load_observations,
)
from research_v2.patterns.pole_next_open_limit_fill_reality_audit import (
    LIMIT_FILL_MODEL,
    _limit_touched,
    _post_fill_observation,
    _stop_touched,
    _target_touched,
)
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import (
    _parse_candle_symbol,
)
from research_v2.patterns.pole_core_motif_sl_candidates import (
    _parse_symbol_input,
    _round,
)

TARGET_R = 2.5
BREAK_EVEN_TRIGGER_R = 2.0
LIMIT_EXPIRY_CANDLES = 3
RISK_PER_TRADE_R = 1.0
COMBINED = "COMBINED"
DEFAULT_INITIAL_CAPITAL_USDT = 1000.0
DEFAULT_FIXED_POSITION_SIZE_USDT = 50.0
ALLOWED_VERDICTS = (
    "PORTFOLIO_READY_RESEARCH",
    "PORTFOLIO_PROMISING_BUT_RISKY",
    "PORTFOLIO_FRAGILE",
    "INSUFFICIENT_DATA",
)
OUTPUT_NAMES = (
    "portfolio_reality_summary.md",
    "portfolio_reality_trade_sequence.csv",
    "portfolio_reality_equity_curve.csv",
    "portfolio_reality_symbol_contribution.csv",
    "portfolio_reality_monthly.csv",
    "portfolio_reality_quarterly.csv",
    "portfolio_reality_flags.csv",
    "portfolio_reality_manifest.json",
)
TRADE_FIELDS = [
    "trade_id",
    "opportunity_id",
    "symbol",
    "direction",
    "entry_timestamp",
    "entry_time_utc",
    "exit_timestamp",
    "exit_time_utc",
    "classification",
    "result_R",
    "cumulative_R",
    "active_positions_at_entry",
    "active_risk_R_at_entry",
]
EQUITY_FIELDS = [
    "sequence",
    "exit_timestamp",
    "exit_time_utc",
    "symbol",
    "result_R",
    "cumulative_R",
    "drawdown_R",
]
SYMBOL_FIELDS = [
    "symbol",
    "trades",
    "wins",
    "losses",
    "BE_exits",
    "total_R",
    "expectancy_R",
    "contribution_percentage",
]
PERIOD_FIELDS = ["period", "trades", "total_R", "win_rate", "max_losing_streak"]
FLAG_FIELDS = ["flag", "severity", "details"]
PORTFOLIO_OPPORTUNITY_ID_METHOD = "symbol+row_number+direction+observable_entry_ts"
MONEY_OUTPUT_NAMES = ("equity_curve_usdt.csv", "monthly_returns_usdt.csv")
COST_OUTPUT_NAMES = ("cost_adjusted_equity_curve_usdt.csv",)
TRADE_SEQUENCE_MONEY_OUTPUT_NAMES = (
    "money_equity_curve.csv",
    "monthly_returns_usdt.csv",
)
TRADE_SEQUENCE_COST_OUTPUT_NAMES = ("cost_adjusted_equity_curve.csv",)
TRADE_SEQUENCE_SIZING_OUTPUT_NAME = "portfolio_reality_trade_sequence_sizing.csv"
MONEY_EQUITY_FIELDS = [
    "sequence",
    "exit_timestamp",
    "exit_time_utc",
    "symbol",
    "result_R",
    "fixed_position_size_usdt",
    "pnl_usdt",
    "equity_usdt",
    "drawdown_usdt",
    "drawdown_percent",
]
MONEY_MONTHLY_FIELDS = [
    "month",
    "starting_equity_usdt",
    "ending_equity_usdt",
    "pnl_usdt",
    "return_percent",
    "trades",
]
COST_EQUITY_FIELDS = [
    "sequence",
    "exit_timestamp",
    "exit_time_utc",
    "symbol",
    "result_R",
    "fixed_position_size_usdt",
    "approximate_notional_usdt",
    "gross_pnl_usdt",
    "total_cost_usdt",
    "net_pnl_usdt",
    "net_equity_usdt",
    "net_drawdown_usdt",
    "net_drawdown_percent",
]


@dataclass(frozen=True)
class PortfolioTrade:
    trade_id: str
    opportunity_id: str
    symbol: str
    direction: str
    entry_ts: int
    exit_ts: int
    classification: str
    result_r: float
    active_positions_at_entry: int
    active_risk_r_at_entry: float
    entry_price: float | None = None
    stop_price: float | None = None


def _ts_to_utc(ts: int | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(
        ts / 1000 if ts > 10_000_000_000 else ts, tz=UTC
    ).isoformat()


def _dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts / 1000 if ts > 10_000_000_000 else ts, tz=UTC)


def _percentile(values: list[float], percentile: float) -> float | str:
    if not values:
        return ""
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return _round(ordered[index])


def _safe_mean(values: list[float]) -> float | str:
    return _round(mean(values)) if values else ""


def _safe_median(values: list[float]) -> float | str:
    return _round(median(values)) if values else ""


def _pending_limit_be_classify(
    rep: Any, candles: list[Any], expiry_candles: int
) -> tuple[str, float | None, int | None, int | None, str]:
    if expiry_candles <= 0:
        raise ValueError("expiry_candles must be positive")
    if (
        rep.geometry_status != "OBSERVABLE"
        or rep.observable_entry_ts is None
        or rep.entry is None
        or rep.stop is None
    ):
        return rep.geometry_status, None, None, None, rep.geometry_details

    eligible = [candle for candle in candles if candle.ts >= rep.observable_entry_ts]
    if not eligible:
        return (
            "UNKNOWN_MISSING_CANDLES",
            None,
            None,
            None,
            "no candles exist at or after the intended next-open timestamp",
        )

    active_window = eligible[:expiry_candles]
    first_fill = next(
        (candle for candle in active_window if _limit_touched(candle, rep)), None
    )
    if first_fill is None:
        return (
            "MISSED_LIMIT_FILL",
            None,
            None,
            None,
            f"pending limit was cancelled after {expiry_candles} candle(s) and cannot fill later",
        )

    if _stop_touched(first_fill, rep):
        return (
            "SAME_CANDLE_FILL_STOP_CONSERVATIVE",
            -1.0,
            first_fill.ts,
            first_fill.ts,
            "fill candle also contains the stop level; conservative same-candle handling records a stop",
        )
    if _target_touched(first_fill, rep, TARGET_R):
        return (
            "SAME_CANDLE_FILL_TARGET_AMBIGUOUS",
            None,
            first_fill.ts,
            first_fill.ts,
            "fill candle also contains the target level; OHLC cannot prove target occurred after the limit fill",
        )

    post_fill = _post_fill_observation(rep, first_fill.ts, candles)
    classification, result_r, exit_ts, details = _be_classify(
        post_fill, candles, BREAK_EVEN_TRIGGER_R
    )
    return classification, result_r, first_fill.ts, exit_ts, details


def _portfolio_opportunity_key(row: EntryTimingObservation) -> tuple[Any, ...]:
    """Return the portfolio execution identity for one raw motif observation.

    The portfolio audit must first reproduce the standalone pending-limit execution
    audit one observation at a time.  Portfolio overlap/sequencing may reduce the
    accepted trade sequence later, but pre-portfolio identity is intentionally
    symbol-scoped and row-scoped so equal row numbers/timestamps on different
    markets cannot overwrite each other.
    """
    return (row.symbol, row.row_number, row.direction, row.observable_entry_ts)


def _build_portfolio_opportunities(
    observations: list[EntryTimingObservation],
) -> list[Opportunity]:
    opportunities: list[Opportunity] = []
    for ordinal, row in enumerate(
        sorted(
            observations,
            key=lambda item: (
                item.symbol,
                item.row_number,
                item.direction,
                -1 if item.observable_entry_ts is None else item.observable_entry_ts,
            ),
        ),
        start=1,
    ):
        key = _portfolio_opportunity_key(row)
        opportunities.append(
            Opportunity(
                f"OPP-{ordinal:06d}",
                key,
                (row,),
                row,
            )
        )
    return opportunities


def _resolved_outcomes(
    opportunities: list[Opportunity],
    candles_by_symbol: dict[str, Any],
    expiry_candles: int = LIMIT_EXPIRY_CANDLES,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    outcomes: list[dict[str, Any]] = []
    flags: list[dict[str, str]] = []
    for opportunity in opportunities:
        rep = opportunity.representative
        classification, result_r, entry_ts, exit_ts, details = (
            _pending_limit_be_classify(
                rep, candles_by_symbol[rep.symbol], expiry_candles
            )
        )
        if result_r is None or exit_ts is None or entry_ts is None:
            flags.append(
                {
                    "flag": "UNRESOLVED_OR_UNTRADEABLE_OPPORTUNITY",
                    "severity": "INFO",
                    "details": f"{opportunity.opportunity_id} {rep.symbol} row {rep.row_number}: {classification} ({details})",
                }
            )
            continue
        outcomes.append(
            {
                "opportunity": opportunity,
                "symbol": rep.symbol,
                "direction": rep.direction,
                "entry_ts": int(entry_ts),
                "exit_ts": int(exit_ts),
                "classification": classification,
                "result_r": float(result_r),
                "entry_price": rep.entry,
                "stop_price": rep.stop,
            }
        )
    return outcomes, flags


def _pre_portfolio_stage_counts(
    opportunities: list[Opportunity],
    candles_by_symbol: dict[str, Any],
    outcomes: list[dict[str, Any]],
    overlap_flags: list[dict[str, str]] | None = None,
    expiry_candles: int = LIMIT_EXPIRY_CANDLES,
) -> dict[str, int]:
    pending_orders = 0
    filled = 0
    cancelled = 0
    for opportunity in opportunities:
        rep = opportunity.representative
        if (
            rep.geometry_status != "OBSERVABLE"
            or rep.observable_entry_ts is None
            or rep.entry is None
            or rep.stop is None
        ):
            continue
        pending_orders += 1
        candles = candles_by_symbol[rep.symbol]
        active_window = [
            candle for candle in candles if candle.ts >= rep.observable_entry_ts
        ][:expiry_candles]
        if any(_limit_touched(candle, rep) for candle in active_window):
            filled += 1
        else:
            cancelled += 1
    return {
        "raw_observations": sum(
            len(opportunity.observations) for opportunity in opportunities
        ),
        "motif_eligible": len(opportunities),
        "deduplicated": len(opportunities),
        "pending_orders": pending_orders,
        "filled": filled,
        "cancelled": cancelled,
        "portfolio_accepted": len(outcomes),
        "overlap_skipped": (
            0
            if overlap_flags is None
            else sum(
                1
                for flag in overlap_flags
                if flag["flag"] == "SAME_SYMBOL_OVERLAP_SKIPPED"
            )
        ),
    }


def _apply_one_position_per_symbol(
    outcomes: list[dict[str, Any]],
) -> tuple[list[PortfolioTrade], list[dict[str, str]]]:
    flags: list[dict[str, str]] = []
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for outcome in outcomes:
        grouped[outcome["entry_ts"]].append(outcome)

    active_until_by_symbol: dict[str, int] = {}
    selected: list[PortfolioTrade] = []
    ordinal = 1
    for entry_ts in sorted(grouped):
        active_until_by_symbol = {
            symbol: exit_ts
            for symbol, exit_ts in active_until_by_symbol.items()
            if exit_ts > entry_ts
        }
        active_positions = len(active_until_by_symbol)
        active_risk = active_positions * RISK_PER_TRADE_R
        pending_additions: dict[str, int] = {}
        for outcome in sorted(
            grouped[entry_ts],
            key=lambda row: (
                row["symbol"],
                row["exit_ts"],
                row["opportunity"].opportunity_id,
            ),
        ):
            symbol = outcome["symbol"]
            if symbol in active_until_by_symbol or symbol in pending_additions:
                flags.append(
                    {
                        "flag": "SAME_SYMBOL_OVERLAP_SKIPPED",
                        "severity": "INFO",
                        "details": f"{outcome['opportunity'].opportunity_id} {symbol} entry {entry_ts} skipped by one-position-per-symbol rule",
                    }
                )
                continue
            selected.append(
                PortfolioTrade(
                    trade_id=f"TRADE-{ordinal:06d}",
                    opportunity_id=outcome["opportunity"].opportunity_id,
                    symbol=symbol,
                    direction=outcome["direction"],
                    entry_ts=entry_ts,
                    exit_ts=outcome["exit_ts"],
                    classification=outcome["classification"],
                    result_r=outcome["result_r"],
                    active_positions_at_entry=active_positions,
                    active_risk_r_at_entry=active_risk,
                    entry_price=outcome.get("entry_price"),
                    stop_price=outcome.get("stop_price"),
                )
            )
            pending_additions[symbol] = outcome["exit_ts"]
            ordinal += 1
        active_until_by_symbol.update(pending_additions)
    return selected, flags


def _risk_sizing_values(
    trade: PortfolioTrade, fixed_risk_usdt: float
) -> dict[str, Any]:
    if trade.entry_price is None or trade.stop_price is None:
        return {
            "entry_price": "",
            "stop_price": "",
            "risk_per_unit": "",
            "fixed_risk_usdt": _round(fixed_risk_usdt),
            "position_qty": "",
            "approximate_notional_usdt": "",
        }
    risk_per_unit = abs(trade.entry_price - trade.stop_price)
    if risk_per_unit <= 0:
        position_qty = ""
        approximate_notional = ""
    else:
        position_qty = fixed_risk_usdt / risk_per_unit
        approximate_notional = position_qty * abs(trade.entry_price)
    return {
        "entry_price": _round(trade.entry_price),
        "stop_price": _round(trade.stop_price),
        "risk_per_unit": _round(risk_per_unit),
        "fixed_risk_usdt": _round(fixed_risk_usdt),
        "position_qty": "" if position_qty == "" else _round(position_qty),
        "approximate_notional_usdt": (
            "" if approximate_notional == "" else _round(approximate_notional)
        ),
    }


def _trade_fields(fixed_risk_usdt: float | None = None) -> list[str]:
    if fixed_risk_usdt is None:
        return TRADE_FIELDS
    return [
        *TRADE_FIELDS,
        "entry_price",
        "stop_price",
        "risk_per_unit",
        "fixed_risk_usdt",
        "position_qty",
        "approximate_notional_usdt",
    ]


def _trade_rows(
    trades: list[PortfolioTrade], fixed_risk_usdt: float | None = None
) -> list[dict[str, Any]]:
    cumulative = 0.0
    rows: list[dict[str, Any]] = []
    for trade in sorted(
        trades, key=lambda row: (row.exit_ts, row.entry_ts, row.symbol, row.trade_id)
    ):
        cumulative += trade.result_r
        row = {
            "trade_id": trade.trade_id,
            "opportunity_id": trade.opportunity_id,
            "symbol": trade.symbol,
            "direction": trade.direction,
            "entry_timestamp": trade.entry_ts,
            "entry_time_utc": _ts_to_utc(trade.entry_ts),
            "exit_timestamp": trade.exit_ts,
            "exit_time_utc": _ts_to_utc(trade.exit_ts),
            "classification": trade.classification,
            "result_R": _round(trade.result_r),
            "cumulative_R": _round(cumulative),
            "active_positions_at_entry": trade.active_positions_at_entry,
            "active_risk_R_at_entry": _round(trade.active_risk_r_at_entry),
        }
        if fixed_risk_usdt is not None:
            row.update(_risk_sizing_values(trade, fixed_risk_usdt))
        rows.append(row)
    return rows


def _equity_curve(
    trades: list[PortfolioTrade],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    rows: list[dict[str, Any]] = []
    peak_ts_for_drawdown: int | None = None
    drawdown_start_ts: int | None = None
    max_recovery_time: int | str = ""
    for sequence, trade in enumerate(
        sorted(
            trades,
            key=lambda row: (row.exit_ts, row.entry_ts, row.symbol, row.trade_id),
        ),
        start=1,
    ):
        cumulative += trade.result_r
        if cumulative >= peak:
            if drawdown_start_ts is not None:
                max_recovery_time = max(
                    max_recovery_time if isinstance(max_recovery_time, int) else 0,
                    trade.exit_ts - drawdown_start_ts,
                )
                drawdown_start_ts = None
            peak = cumulative
            peak_ts_for_drawdown = trade.exit_ts
        drawdown = peak - cumulative
        if drawdown > 0 and drawdown_start_ts is None:
            drawdown_start_ts = peak_ts_for_drawdown
        max_drawdown = max(max_drawdown, drawdown)
        rows.append(
            {
                "sequence": sequence,
                "exit_timestamp": trade.exit_ts,
                "exit_time_utc": _ts_to_utc(trade.exit_ts),
                "symbol": trade.symbol,
                "result_R": _round(trade.result_r),
                "cumulative_R": _round(cumulative),
                "drawdown_R": _round(drawdown),
            }
        )
    summary = {
        "total_R": _round(cumulative),
        "average_R_per_trade": _safe_mean([trade.result_r for trade in trades]),
        "median_R_per_trade": _safe_median([trade.result_r for trade in trades]),
        "max_drawdown_R": _round(max_drawdown),
        "max_drawdown_percent_of_peak_R": (
            _round(max_drawdown / peak * 100) if peak > 0 else ""
        ),
        "recovery_time_after_drawdown": max_recovery_time,
    }
    return rows, summary


def _money_config(
    initial_capital_usdt: float | None, fixed_position_size_usdt: float | None
) -> tuple[float, float] | None:
    if initial_capital_usdt is None and fixed_position_size_usdt is None:
        return None
    initial_capital = (
        DEFAULT_INITIAL_CAPITAL_USDT
        if initial_capital_usdt is None
        else float(initial_capital_usdt)
    )
    fixed_position_size = (
        DEFAULT_FIXED_POSITION_SIZE_USDT
        if fixed_position_size_usdt is None
        else float(fixed_position_size_usdt)
    )
    if initial_capital <= 0:
        raise ValueError(
            "--initial-capital must be positive when money simulation is enabled"
        )
    if fixed_position_size <= 0:
        raise ValueError(
            "--fixed-position-size must be positive when money simulation is enabled"
        )
    return initial_capital, fixed_position_size


def _money_equity_curve(
    trades: list[PortfolioTrade],
    initial_capital_usdt: float,
    fixed_position_size_usdt: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    equity = initial_capital_usdt
    peak = initial_capital_usdt
    max_drawdown_usdt = 0.0
    max_drawdown_percent = 0.0
    rows: list[dict[str, Any]] = []
    ordered = sorted(
        trades, key=lambda row: (row.exit_ts, row.entry_ts, row.symbol, row.trade_id)
    )
    for sequence, trade in enumerate(ordered, start=1):
        pnl_usdt = trade.result_r * fixed_position_size_usdt
        equity += pnl_usdt
        peak = max(peak, equity)
        drawdown_usdt = peak - equity
        drawdown_percent = drawdown_usdt / peak * 100 if peak > 0 else 0.0
        max_drawdown_usdt = max(max_drawdown_usdt, drawdown_usdt)
        max_drawdown_percent = max(max_drawdown_percent, drawdown_percent)
        rows.append(
            {
                "sequence": sequence,
                "exit_timestamp": trade.exit_ts,
                "exit_time_utc": _ts_to_utc(trade.exit_ts),
                "symbol": trade.symbol,
                "result_R": _round(trade.result_r),
                "fixed_position_size_usdt": _round(fixed_position_size_usdt),
                "pnl_usdt": _round(pnl_usdt),
                "equity_usdt": _round(equity),
                "drawdown_usdt": _round(drawdown_usdt),
                "drawdown_percent": _round(drawdown_percent),
            }
        )
    summary = {
        "initial_capital_usdt": _round(initial_capital_usdt),
        "fixed_position_size_usdt": _round(fixed_position_size_usdt),
        "final_equity_usdt": _round(equity),
        "total_pnl_usdt": _round(equity - initial_capital_usdt),
        "max_drawdown_usdt": _round(max_drawdown_usdt),
        "max_drawdown_percent": _round(max_drawdown_percent),
    }
    return rows, summary


def _cost_config(
    fee_bps: float | None, slippage_bps: float | None, money_enabled: bool
) -> tuple[float, float] | None:
    if fee_bps is None and slippage_bps is None:
        return None
    if not money_enabled:
        raise ValueError(
            "--fee-bps and --slippage-bps are only valid when money simulation is enabled"
        )
    fee = 0.0 if fee_bps is None else float(fee_bps)
    slippage = 0.0 if slippage_bps is None else float(slippage_bps)
    if fee < 0:
        raise ValueError("--fee-bps must be non-negative")
    if slippage < 0:
        raise ValueError("--slippage-bps must be non-negative")
    return fee, slippage


def _cost_adjusted_equity_curve(
    trades: list[PortfolioTrade],
    initial_capital_usdt: float,
    fixed_position_size_usdt: float,
    fee_bps: float,
    slippage_bps: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    net_equity = initial_capital_usdt
    peak = initial_capital_usdt
    max_drawdown_usdt = 0.0
    max_drawdown_percent = 0.0
    total_gross_pnl = 0.0
    total_cost = 0.0
    rows: list[dict[str, Any]] = []
    cost_rate = (fee_bps + slippage_bps) / 10_000
    ordered = sorted(
        trades, key=lambda row: (row.exit_ts, row.entry_ts, row.symbol, row.trade_id)
    )
    for sequence, trade in enumerate(ordered, start=1):
        gross_pnl = trade.result_r * fixed_position_size_usdt
        approximate_notional = fixed_position_size_usdt
        trade_cost = approximate_notional * cost_rate * 2
        net_pnl = gross_pnl - trade_cost
        net_equity += net_pnl
        peak = max(peak, net_equity)
        drawdown_usdt = peak - net_equity
        drawdown_percent = drawdown_usdt / peak * 100 if peak > 0 else 0.0
        max_drawdown_usdt = max(max_drawdown_usdt, drawdown_usdt)
        max_drawdown_percent = max(max_drawdown_percent, drawdown_percent)
        total_gross_pnl += gross_pnl
        total_cost += trade_cost
        rows.append(
            {
                "sequence": sequence,
                "exit_timestamp": trade.exit_ts,
                "exit_time_utc": _ts_to_utc(trade.exit_ts),
                "symbol": trade.symbol,
                "result_R": _round(trade.result_r),
                "fixed_position_size_usdt": _round(fixed_position_size_usdt),
                "approximate_notional_usdt": _round(approximate_notional),
                "gross_pnl_usdt": _round(gross_pnl),
                "total_cost_usdt": _round(trade_cost),
                "net_pnl_usdt": _round(net_pnl),
                "net_equity_usdt": _round(net_equity),
                "net_drawdown_usdt": _round(drawdown_usdt),
                "net_drawdown_percent": _round(drawdown_percent),
            }
        )
    summary = {
        "enabled": True,
        "fee_bps": _round(fee_bps),
        "slippage_bps": _round(slippage_bps),
        "notional_assumption": "approximate_notional_usdt uses fixed_position_size_usdt because trade-level notional is not available in the resolved portfolio baseline",
        "cost_timing_assumption": "fee and slippage bps are charged on approximate notional for entry and exit",
        "gross_pnl_usdt": _round(total_gross_pnl),
        "total_cost_usdt": _round(total_cost),
        "net_pnl_usdt": _round(net_equity - initial_capital_usdt),
        "final_net_equity_usdt": _round(net_equity),
        "max_net_drawdown_usdt": _round(max_drawdown_usdt),
        "max_net_drawdown_percent": _round(max_drawdown_percent),
    }
    return rows, summary


def _monthly_money_rows(
    money_rows: list[dict[str, Any]], initial_capital_usdt: float
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current_month: str | None = None
    starting_equity = initial_capital_usdt
    ending_equity = initial_capital_usdt
    pnl = 0.0
    trades = 0

    def flush() -> None:
        if current_month is None:
            return
        rows.append(
            {
                "month": current_month,
                "starting_equity_usdt": _round(starting_equity),
                "ending_equity_usdt": _round(ending_equity),
                "pnl_usdt": _round(pnl),
                "return_percent": (
                    _round(pnl / starting_equity * 100) if starting_equity > 0 else ""
                ),
                "trades": trades,
            }
        )

    for money_row in money_rows:
        month = _dt(int(money_row["exit_timestamp"])).strftime("%Y-%m")
        if current_month is None:
            current_month = month
        elif month != current_month:
            flush()
            current_month = month
            starting_equity = ending_equity
            pnl = 0.0
            trades = 0
        pnl += float(money_row["pnl_usdt"])
        ending_equity = float(money_row["equity_usdt"])
        trades += 1
    flush()
    return rows


def _load_trade_sequence_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), [dict(row) for row in reader]


def _trade_sequence_sizing_status(
    fieldnames: list[str], rows: list[dict[str, str]]
) -> tuple[bool, str]:
    missing_columns = [
        field for field in ("entry_price", "stop_price") if field not in fieldnames
    ]
    if missing_columns:
        return False, f"missing required sizing column(s): {', '.join(missing_columns)}"
    for index, row in enumerate(rows, start=2):
        if row.get("entry_price") in (None, "") or row.get("stop_price") in (None, ""):
            return False, f"missing entry_price/stop_price value at row {index}"
        try:
            entry_price = float(row["entry_price"])
            stop_price = float(row["stop_price"])
        except (TypeError, ValueError):
            return False, f"invalid entry_price/stop_price value at row {index}"
        if abs(entry_price - stop_price) <= 0:
            return False, f"non-positive risk_per_unit at row {index}"
    return True, ""


def _trade_sequence_sizing_fields(fieldnames: list[str]) -> list[str]:
    sizing_fields = [
        "entry_price",
        "stop_price",
        "risk_per_unit",
        "fixed_risk_usdt",
        "position_qty",
        "approximate_notional_usdt",
    ]
    return [*fieldnames, *[field for field in sizing_fields if field not in fieldnames]]


def _trade_sequence_sizing_rows(
    fieldnames: list[str], rows: list[dict[str, str]], fixed_risk_usdt: float
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in rows:
        output_row: dict[str, Any] = {field: row.get(field, "") for field in fieldnames}
        trade = PortfolioTrade(
            trade_id=row.get("trade_id", ""),
            opportunity_id=row.get("opportunity_id", ""),
            symbol=row.get("symbol", ""),
            direction=row.get("direction", ""),
            entry_ts=0,
            exit_ts=0,
            classification=row.get("classification", ""),
            result_r=0.0,
            active_positions_at_entry=0,
            active_risk_r_at_entry=0.0,
            entry_price=float(row["entry_price"]),
            stop_price=float(row["stop_price"]),
        )
        output_row.update(_risk_sizing_values(trade, fixed_risk_usdt))
        enriched.append(output_row)
    return enriched


def _load_trade_sequence(path: Path) -> list[PortfolioTrade]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [
            field
            for field in (
                "trade_id",
                "symbol",
                "entry_timestamp",
                "exit_timestamp",
                "result_R",
            )
            if field not in (reader.fieldnames or [])
        ]
        if missing:
            raise ValueError(
                f"--trade-sequence is missing required column(s): {', '.join(missing)}"
            )
        trades: list[PortfolioTrade] = []
        for line_number, row in enumerate(reader, start=2):
            try:
                trades.append(
                    PortfolioTrade(
                        trade_id=row.get("trade_id") or f"TRADE-{line_number - 1:06d}",
                        opportunity_id=row.get("opportunity_id", ""),
                        symbol=row["symbol"],
                        direction=row.get("direction", ""),
                        entry_ts=int(float(row["entry_timestamp"])),
                        exit_ts=int(float(row["exit_timestamp"])),
                        classification=row.get("classification", ""),
                        result_r=float(row["result_R"]),
                        active_positions_at_entry=int(
                            float(row.get("active_positions_at_entry") or 0)
                        ),
                        active_risk_r_at_entry=float(
                            row.get("active_risk_R_at_entry") or 0.0
                        ),
                        entry_price=(
                            float(row["entry_price"])
                            if row.get("entry_price") not in (None, "")
                            else None
                        ),
                        stop_price=(
                            float(row["stop_price"])
                            if row.get("stop_price") not in (None, "")
                            else None
                        ),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"invalid --trade-sequence row {line_number}: {exc}"
                ) from exc
    return trades


def _money_cost_outputs(
    trades: list[PortfolioTrade],
    initial_capital_usdt: float | None,
    fixed_position_size_usdt: float | None,
    fee_bps: float | None,
    slippage_bps: float | None,
) -> tuple[
    list[dict[str, Any]],
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
]:
    money_config = _money_config(initial_capital_usdt, fixed_position_size_usdt)
    if money_config is None:
        raise ValueError(
            "money simulation requires --initial-capital or --fixed-position-size"
        )
    cost_config = _cost_config(fee_bps, slippage_bps, True)
    initial_capital, fixed_position_size = money_config
    money_rows, money_summary = _money_equity_curve(
        trades, initial_capital, fixed_position_size
    )
    monthly_money_rows = _monthly_money_rows(money_rows, initial_capital)
    cost_rows: list[dict[str, Any]] = []
    cost_summary: dict[str, Any] = {}
    if cost_config:
        fee, slippage = cost_config
        cost_rows, cost_summary = _cost_adjusted_equity_curve(
            trades, initial_capital, fixed_position_size, fee, slippage
        )
    return money_rows, money_summary, monthly_money_rows, cost_rows, cost_summary


def _write_money_summary(
    path: Path, money_summary: dict[str, Any], cost_summary: dict[str, Any]
) -> None:
    with path.open("x") as handle:
        handle.write("# Portfolio trade sequence money simulation\n\n")
        handle.write("## Money simulation (USDT)\n\n")
        for key, value in money_summary.items():
            handle.write(f"- `{key}`: {value}\n")
        if cost_summary:
            handle.write("\n## Cost-adjusted money simulation (USDT)\n\n")
            for key, value in cost_summary.items():
                handle.write(f"- `{key}`: {value}\n")


def run_trade_sequence(
    trade_sequence: Path,
    output_root: Path,
    initial_capital_usdt: float | None = None,
    fixed_position_size_usdt: float | None = None,
    fee_bps: float | None = None,
    slippage_bps: float | None = None,
) -> None:
    fieldnames, source_rows = _load_trade_sequence_rows(trade_sequence)
    trades = _load_trade_sequence(trade_sequence)
    cost_enabled = fee_bps is not None or slippage_bps is not None
    money_config = _money_config(initial_capital_usdt, fixed_position_size_usdt)
    if money_config is None:
        raise ValueError(
            "money simulation requires --initial-capital or --fixed-position-size"
        )
    sizing_available, missing_sizing_reason = _trade_sequence_sizing_status(
        fieldnames, source_rows
    )
    output_names = (
        "portfolio_reality_summary.md",
        "portfolio_reality_manifest.json",
        *TRADE_SEQUENCE_MONEY_OUTPUT_NAMES,
        *(TRADE_SEQUENCE_COST_OUTPUT_NAMES if cost_enabled else ()),
        *((TRADE_SEQUENCE_SIZING_OUTPUT_NAME,) if sizing_available else ()),
    )
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in output_names if (output_root / name).exists()]
    if existing:
        raise FileExistsError(
            f"refusing to overwrite existing trade-sequence simulation output(s): {', '.join(existing)}"
        )
    money_rows, money_summary, monthly_money_rows, cost_rows, cost_summary = (
        _money_cost_outputs(
            trades,
            initial_capital_usdt,
            fixed_position_size_usdt,
            fee_bps,
            slippage_bps,
        )
    )
    _write_csv(
        output_root / TRADE_SEQUENCE_MONEY_OUTPUT_NAMES[0],
        MONEY_EQUITY_FIELDS,
        money_rows,
    )
    _write_csv(
        output_root / TRADE_SEQUENCE_MONEY_OUTPUT_NAMES[1],
        MONEY_MONTHLY_FIELDS,
        monthly_money_rows,
    )
    if sizing_available:
        _write_csv(
            output_root / TRADE_SEQUENCE_SIZING_OUTPUT_NAME,
            _trade_sequence_sizing_fields(fieldnames),
            _trade_sequence_sizing_rows(fieldnames, source_rows, money_config[1]),
        )
    if cost_enabled:
        _write_csv(
            output_root / TRADE_SEQUENCE_COST_OUTPUT_NAMES[0],
            COST_EQUITY_FIELDS,
            cost_rows,
        )
    _write_money_summary(
        output_root / "portfolio_reality_summary.md", money_summary, cost_summary
    )
    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_portfolio_reality_trade_sequence_money_cost_simulation",
        "research_only": True,
        "not_production": True,
        "strategy_promotion": False,
        "production_modifications": False,
        "input_trade_sequence": str(trade_sequence),
        "resolved_portfolio_trades": len(trades),
        "sizing_available": sizing_available,
        "missing_sizing_reason": "" if sizing_available else missing_sizing_reason,
        "money_simulation": {
            "enabled": True,
            "initial_capital_usdt": money_summary["initial_capital_usdt"],
            "fixed_position_size_usdt": money_summary["fixed_position_size_usdt"],
            "summary": {
                "final_equity_usdt": money_summary["final_equity_usdt"],
                "total_pnl_usdt": money_summary["total_pnl_usdt"],
                "max_drawdown_usdt": money_summary["max_drawdown_usdt"],
                "max_drawdown_percent": money_summary["max_drawdown_percent"],
            },
            "artifacts": list(TRADE_SEQUENCE_MONEY_OUTPUT_NAMES),
        },
        "artifacts": [
            "portfolio_reality_summary.md",
            *TRADE_SEQUENCE_MONEY_OUTPUT_NAMES,
            *(TRADE_SEQUENCE_COST_OUTPUT_NAMES if cost_enabled else ()),
            *((TRADE_SEQUENCE_SIZING_OUTPUT_NAME,) if sizing_available else ()),
        ],
    }
    if cost_enabled:
        manifest["cost_adjusted_summary"] = {
            **cost_summary,
            "artifacts": list(TRADE_SEQUENCE_COST_OUTPUT_NAMES),
        }
    with (output_root / "portfolio_reality_manifest.json").open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _max_streak(values: Iterable[float], predicate: Any) -> int:
    best = current = 0
    for value in values:
        if predicate(value):
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _symbol_rows(trades: list[PortfolioTrade], total_r: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in sorted({trade.symbol for trade in trades}):
        scoped = [trade for trade in trades if trade.symbol == symbol]
        symbol_total = sum(trade.result_r for trade in scoped)
        rows.append(
            {
                "symbol": symbol,
                "trades": len(scoped),
                "wins": sum(trade.result_r > 0 for trade in scoped),
                "losses": sum(trade.result_r < 0 for trade in scoped),
                "BE_exits": sum(trade.result_r == 0 for trade in scoped),
                "total_R": _round(symbol_total),
                "expectancy_R": _round(symbol_total / len(scoped)) if scoped else "",
                "contribution_percentage": (
                    _round(symbol_total / total_r * 100) if total_r > 0 else ""
                ),
            }
        )
    return rows


def _period_rows(trades: list[PortfolioTrade], period: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[PortfolioTrade]] = defaultdict(list)
    for trade in trades:
        dt = _dt(trade.exit_ts)
        key = (
            dt.strftime("%Y-%m")
            if period == "month"
            else f"{dt.year}-Q{((dt.month - 1) // 3) + 1}"
        )
        grouped[key].append(trade)
    rows: list[dict[str, Any]] = []
    for key in sorted(grouped):
        scoped = sorted(
            grouped[key],
            key=lambda row: (row.exit_ts, row.entry_ts, row.symbol, row.trade_id),
        )
        total = sum(trade.result_r for trade in scoped)
        wins = sum(trade.result_r > 0 for trade in scoped)
        rows.append(
            {
                "period": key,
                "trades": len(scoped),
                "total_R": _round(total),
                "win_rate": _round(wins / len(scoped)) if scoped else "",
                "max_losing_streak": _max_streak(
                    (trade.result_r for trade in scoped), lambda value: value < 0
                ),
            }
        )
    return rows


def _exposure_summary(trades: list[PortfolioTrade]) -> dict[str, Any]:
    concurrent_after_entry = [trade.active_positions_at_entry + 1 for trade in trades]
    active_risk_after_entry = [
        value * RISK_PER_TRADE_R for value in concurrent_after_entry
    ]
    return {
        "median_concurrent_positions": _safe_median(
            [float(value) for value in concurrent_after_entry]
        ),
        "p90_concurrent_positions": _percentile(
            [float(value) for value in concurrent_after_entry], 0.9
        ),
        "max_concurrent_positions": (
            max(concurrent_after_entry) if concurrent_after_entry else 0
        ),
        "average_active_risk_R": _safe_mean(active_risk_after_entry),
        "peak_active_risk_R": (
            _round(max(active_risk_after_entry)) if active_risk_after_entry else 0.0
        ),
        "entries_over_2R_active_risk": sum(
            value > 2.0 for value in active_risk_after_entry
        ),
        "entries_over_2R_active_risk_rate": (
            _round(
                sum(value > 2.0 for value in active_risk_after_entry)
                / len(active_risk_after_entry)
            )
            if active_risk_after_entry
            else 0.0
        ),
    }


def _risk_flags(
    trades: list[PortfolioTrade],
    equity: dict[str, Any],
    symbol_rows: list[dict[str, Any]],
    period_rows: list[dict[str, Any]],
    exposure: dict[str, Any],
) -> list[dict[str, str]]:
    flags: list[dict[str, str]] = [
        {
            "flag": "RESEARCH_ONLY",
            "severity": "INFO",
            "details": "portfolio audit only; no production strategy, live trading, or execution parameter changes",
        },
        {
            "flag": "NO_PROMOTION",
            "severity": "INFO",
            "details": "allowed verdicts never include PROMOTE",
        },
    ]
    total_r = float(equity["total_R"])
    if not trades:
        flags.append(
            {
                "flag": "INSUFFICIENT_DATA",
                "severity": "HIGH",
                "details": "no resolved portfolio trades were available",
            }
        )
        return flags
    if symbol_rows and total_r > 0:
        top = max(symbol_rows, key=lambda row: float(row["total_R"]))
        if float(top["contribution_percentage"] or 0) >= 40:
            severity = (
                "HIGH" if float(top["contribution_percentage"] or 0) >= 60 else "MEDIUM"
            )
            flags.append(
                {
                    "flag": "SYMBOL_CONCENTRATION",
                    "severity": severity,
                    "details": f"{top['symbol']} contributes {top['contribution_percentage']}% of total_R",
                }
            )
    if total_r > 0 and float(equity["max_drawdown_R"]) / total_r >= 0.25:
        ratio = float(equity["max_drawdown_R"]) / total_r
        flags.append(
            {
                "flag": "DRAWDOWN_LARGE_RELATIVE_TO_TOTAL_R",
                "severity": "HIGH" if ratio >= 0.5 else "MEDIUM",
                "details": f"max_drawdown_R is {_round(ratio * 100)}% of total_R",
            }
        )
    losing_streak = _max_streak(
        (
            trade.result_r
            for trade in sorted(trades, key=lambda row: (row.exit_ts, row.entry_ts))
        ),
        lambda value: value < 0,
    )
    if losing_streak >= 5:
        flags.append(
            {
                "flag": "LONG_LOSING_STREAK",
                "severity": "HIGH" if losing_streak >= 8 else "MEDIUM",
                "details": f"longest losing streak is {losing_streak}",
            }
        )
    if float(exposure["entries_over_2R_active_risk_rate"]) >= 0.10:
        flags.append(
            {
                "flag": "FREQUENT_EXPOSURE_OVER_2R",
                "severity": (
                    "HIGH"
                    if float(exposure["entries_over_2R_active_risk_rate"]) >= 0.25
                    else "MEDIUM"
                ),
                "details": f"{exposure['entries_over_2R_active_risk_rate']} of entries exceed 2R active risk",
            }
        )
    if period_rows and total_r > 0:
        top_period = max(period_rows, key=lambda row: float(row["total_R"]))
        if float(top_period["total_R"]) / total_r >= 0.40:
            ratio = float(top_period["total_R"]) / total_r
            flags.append(
                {
                    "flag": "PERIOD_CONCENTRATION",
                    "severity": "HIGH" if ratio >= 0.60 else "MEDIUM",
                    "details": f"{top_period['period']} contributes {_round(ratio * 100)}% of total_R",
                }
            )
    return flags


def _verdict(
    trades: list[PortfolioTrade], equity: dict[str, Any], flags: list[dict[str, str]]
) -> tuple[str, str]:
    if len(trades) < 30:
        return (
            "INSUFFICIENT_DATA",
            "fewer than 30 resolved portfolio trades are available",
        )
    total_r = float(equity["total_R"])
    if total_r <= 0:
        return "PORTFOLIO_FRAGILE", "portfolio total_R is non-positive"
    high_flags = [
        row["flag"]
        for row in flags
        if row["severity"] == "HIGH"
        and row["flag"] not in {"RESEARCH_ONLY", "NO_PROMOTION"}
    ]
    medium_flags = [row["flag"] for row in flags if row["severity"] == "MEDIUM"]
    if high_flags:
        return (
            "PORTFOLIO_FRAGILE",
            f"high-severity stress flags present: {', '.join(high_flags)}",
        )
    if medium_flags:
        return (
            "PORTFOLIO_PROMISING_BUT_RISKY",
            f"positive total_R with stress flags: {', '.join(medium_flags)}",
        )
    return (
        "PORTFOLIO_READY_RESEARCH",
        "positive total_R with no configured portfolio stress flags; research-only status remains",
    )


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def run(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    output_root: Path,
    candle_symbols: dict[str, str] | None = None,
    initial_capital_usdt: float | None = None,
    fixed_position_size_usdt: float | None = None,
    fee_bps: float | None = None,
    slippage_bps: float | None = None,
) -> None:
    symbols, observations, candles_by_symbol = _load_observations(
        symbol_inputs, columns_inputs, candles_inputs, candle_symbols or {}
    )
    money_config = _money_config(initial_capital_usdt, fixed_position_size_usdt)
    cost_config = _cost_config(fee_bps, slippage_bps, money_config is not None)
    output_names = (
        (
            *OUTPUT_NAMES,
            *MONEY_OUTPUT_NAMES,
            *(COST_OUTPUT_NAMES if cost_config else ()),
        )
        if money_config
        else OUTPUT_NAMES
    )
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in output_names if (output_root / name).exists()]
    if existing:
        raise FileExistsError(
            f"refusing to overwrite existing portfolio reality output(s): {', '.join(existing)}"
        )

    opportunities = _build_portfolio_opportunities(observations)
    outcomes, unresolved_flags = _resolved_outcomes(opportunities, candles_by_symbol)
    trades, overlap_flags = _apply_one_position_per_symbol(outcomes)
    stage_counts = _pre_portfolio_stage_counts(
        opportunities, candles_by_symbol, outcomes, overlap_flags
    )
    stage_counts["portfolio_accepted"] = len(trades)
    trade_fixed_risk_usdt = money_config[1] if money_config else None
    trade_rows = _trade_rows(trades, trade_fixed_risk_usdt)
    equity_rows, equity = _equity_curve(trades)
    total_r = float(equity["total_R"])
    symbols_table = _symbol_rows(trades, total_r)
    monthly_rows = _period_rows(trades, "month")
    quarterly_rows = _period_rows(trades, "quarter")
    exposure = _exposure_summary(trades)
    money_rows: list[dict[str, Any]] = []
    money_summary: dict[str, Any] = {}
    monthly_money_rows: list[dict[str, Any]] = []
    cost_rows: list[dict[str, Any]] = []
    cost_summary: dict[str, Any] = {}
    if money_config:
        money_rows, money_summary, monthly_money_rows, cost_rows, cost_summary = (
            _money_cost_outputs(
                trades,
                initial_capital_usdt,
                fixed_position_size_usdt,
                fee_bps,
                slippage_bps,
            )
        )
    ordered_results = [
        trade.result_r
        for trade in sorted(
            trades,
            key=lambda row: (row.exit_ts, row.entry_ts, row.symbol, row.trade_id),
        )
    ]
    streaks = {
        "longest_losing_streak": _max_streak(ordered_results, lambda value: value < 0),
        "longest_flat_BE_streak": _max_streak(
            ordered_results, lambda value: value == 0
        ),
        "longest_non_winning_streak": _max_streak(
            ordered_results, lambda value: value <= 0
        ),
    }
    flags = [*unresolved_flags, *overlap_flags]
    stress_flags = _risk_flags(trades, equity, symbols_table, monthly_rows, exposure)
    flags.extend(stress_flags)
    verdict, reason = _verdict(trades, equity, flags)

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# PnF pole portfolio reality audit\n\n")
        handle.write(
            "Research only. NOT PRODUCTION. NOT PROMOTED. This audit does not alter strategy code or live trading behavior.\n\n"
        )
        handle.write("## Fixed execution baseline\n\n")
        handle.write(
            f"- Entry: `{LIMIT_FILL_MODEL}` at the intended `{ENTRY_CANDIDATE}` price\n"
            f"- Limit expiry: {LIMIT_EXPIRY_CANDLES} candle(s)\n"
            "- Stop: fixed 3-box stop\n"
            "- Target: fixed 2.5R\n"
            "- Management: move stop to break-even after +2R after the limit fill\n"
            "- No TP1, TP2, trailing, scaling, or pyramiding\n\n"
        )
        handle.write(f"## Verdict: **{verdict}**\n\n{reason}.\n\n")
        handle.write("## Equity curve metrics\n\n")
        for key, value in {**equity, **streaks}.items():
            handle.write(f"- `{key}`: {value}\n")
        if money_config:
            handle.write("\n## Money simulation (USDT)\n\n")
            for key, value in money_summary.items():
                handle.write(f"- `{key}`: {value}\n")
        if cost_config:
            handle.write("\n## Cost-adjusted money simulation (USDT)\n\n")
            for key, value in cost_summary.items():
                handle.write(f"- `{key}`: {value}\n")
        handle.write("\n## Portfolio exposure\n\n")
        for key, value in exposure.items():
            handle.write(f"- `{key}`: {value}\n")
        handle.write(
            "\n## Symbol contribution\n\n| symbol | trades | wins | losses | BE exits | total R | expectancy R | contribution % |\n|---|---:|---:|---:|---:|---:|---:|---:|\n"
        )
        for row in symbols_table:
            handle.write(
                f"| {row['symbol']} | {row['trades']} | {row['wins']} | {row['losses']} | {row['BE_exits']} | {row['total_R']} | {row['expectancy_R']} | {row['contribution_percentage']} |\n"
            )
        handle.write(
            "\n## Stress flags\n\n| flag | severity | details |\n|---|---|---|\n"
        )
        for row in flags:
            handle.write(f"| {row['flag']} | {row['severity']} | {row['details']} |\n")

    _write_csv(
        output_root / OUTPUT_NAMES[1], _trade_fields(trade_fixed_risk_usdt), trade_rows
    )
    _write_csv(output_root / OUTPUT_NAMES[2], EQUITY_FIELDS, equity_rows)
    _write_csv(output_root / OUTPUT_NAMES[3], SYMBOL_FIELDS, symbols_table)
    _write_csv(output_root / OUTPUT_NAMES[4], PERIOD_FIELDS, monthly_rows)
    _write_csv(output_root / OUTPUT_NAMES[5], PERIOD_FIELDS, quarterly_rows)
    _write_csv(output_root / OUTPUT_NAMES[6], FLAG_FIELDS, flags)
    if money_config:
        _write_csv(output_root / MONEY_OUTPUT_NAMES[0], MONEY_EQUITY_FIELDS, money_rows)
        _write_csv(
            output_root / MONEY_OUTPUT_NAMES[1],
            MONEY_MONTHLY_FIELDS,
            monthly_money_rows,
        )
    if cost_config:
        _write_csv(output_root / COST_OUTPUT_NAMES[0], COST_EQUITY_FIELDS, cost_rows)
    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_portfolio_reality_audit",
        "research_only": True,
        "not_production": True,
        "strategy_promotion": False,
        "production_modifications": False,
        "entry": ENTRY_CANDIDATE,
        "execution_model": LIMIT_FILL_MODEL,
        "limit_expiry_candles": LIMIT_EXPIRY_CANDLES,
        "stop": "fixed_3_box_stop",
        "target_R": TARGET_R,
        "break_even_after_R": BREAK_EVEN_TRIGGER_R,
        "risk_assumption": "1R fixed fractional per trade; no compounding; R-based equity curve",
        "notional_sizing_validation": {
            "enabled": money_config is not None,
            "formula": "position_qty = fixed_risk_usdt / abs(entry_price - stop_price)",
            "fixed_risk_usdt_source": (
                "--fixed-position-size money simulation 1R amount"
                if money_config
                else "not requested"
            ),
            "sizable_trades": sum(
                trade.entry_price is not None
                and trade.stop_price is not None
                and abs(trade.entry_price - trade.stop_price) > 0
                for trade in trades
            ),
            "missing_or_invalid_geometry_trades": sum(
                trade.entry_price is None
                or trade.stop_price is None
                or abs(trade.entry_price - trade.stop_price) <= 0
                for trade in trades
            ),
        },
        "positioning_assumption": "one position per symbol; simultaneous positions across symbols allowed",
        "management_rules": {
            "tp1": False,
            "tp2": False,
            "trailing": False,
            "scaling": False,
            "pyramiding": False,
        },
        "symbols": symbols,
        "input_observations": len(observations),
        "unique_opportunities": len(opportunities),
        "resolved_portfolio_trades": len(trades),
        "portfolio_opportunity_id_method": PORTFOLIO_OPPORTUNITY_ID_METHOD,
        "pipeline_stage_counts": stage_counts,
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "verdict": verdict,
        "verdict_reason": reason,
        "summary_metrics": {**equity, **streaks, **exposure},
        "artifacts": [
            *OUTPUT_NAMES[:-1],
            *(MONEY_OUTPUT_NAMES if money_config else ()),
            *(COST_OUTPUT_NAMES if cost_config else ()),
        ],
    }
    if money_config:
        manifest["money_simulation"] = {
            "enabled": True,
            "initial_capital_usdt": money_summary["initial_capital_usdt"],
            "fixed_position_size_usdt": money_summary["fixed_position_size_usdt"],
            "summary": {
                "final_equity_usdt": money_summary["final_equity_usdt"],
                "total_pnl_usdt": money_summary["total_pnl_usdt"],
                "max_drawdown_usdt": money_summary["max_drawdown_usdt"],
                "max_drawdown_percent": money_summary["max_drawdown_percent"],
            },
            "artifacts": list(MONEY_OUTPUT_NAMES),
        }
    if cost_config:
        manifest["cost_adjusted_summary"] = {
            **cost_summary,
            "artifacts": list(COST_OUTPUT_NAMES),
        }
    with (output_root / OUTPUT_NAMES[7]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Research-only portfolio reality audit for the fixed PnF pole baseline"
    )
    parser.add_argument(
        "--trade-sequence",
        type=Path,
        default=None,
        help="Existing portfolio_reality_trade_sequence.csv for money/cost simulation without raw inputs",
    )
    parser.add_argument(
        "--symbol-input",
        action="append",
        type=_parse_symbol_input,
        metavar="SYMBOL=CSV",
    )
    parser.add_argument(
        "--columns-input",
        action="append",
        type=_parse_symbol_input,
        metavar="SYMBOL=CSV",
    )
    parser.add_argument(
        "--candles-input",
        action="append",
        type=_parse_symbol_input,
        metavar="SYMBOL=CSV_OR_DB",
    )
    parser.add_argument(
        "--candle-symbol",
        action="append",
        default=[],
        type=_parse_candle_symbol,
        metavar="SYMBOL=DB_SYMBOL",
    )
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=None,
        help="Optional starting capital in USDT for the money simulation layer",
    )
    parser.add_argument(
        "--fixed-position-size",
        type=float,
        default=None,
        help="Optional fixed USDT position size per 1R result for money simulation",
    )
    parser.add_argument(
        "--fee-bps",
        type=float,
        default=None,
        help="Optional fee cost in basis points for cost-adjusted money simulation",
    )
    parser.add_argument(
        "--slippage-bps",
        type=float,
        default=None,
        help="Optional slippage cost in basis points for cost-adjusted money simulation",
    )
    args = parser.parse_args()
    try:
        if args.trade_sequence is not None:
            run_trade_sequence(
                args.trade_sequence,
                args.output_root,
                args.initial_capital,
                args.fixed_position_size,
                args.fee_bps,
                args.slippage_bps,
            )
            return
        if (
            args.symbol_input is None
            or args.columns_input is None
            or args.candles_input is None
        ):
            parser.error(
                "--symbol-input, --columns-input, and --candles-input are required unless --trade-sequence is supplied"
            )
        run(
            dict(args.symbol_input),
            dict(args.columns_input),
            dict(args.candles_input),
            args.output_root,
            dict(args.candle_symbol),
            args.initial_capital,
            args.fixed_position_size,
            args.fee_bps,
            args.slippage_bps,
        )
    except (FileExistsError, OSError, sqlite3.Error, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
