#!/usr/bin/env python3
"""Deterministic historical replay parity check for the MEXC Pole live trader.

The replay feeds historical candles to the existing live trader one timestamp at a
time and compares the first-seen live trade-plan sequence with the sequence
produced by the existing research strategy pipeline on the same candle prefixes.
It does not change strategy logic, execution logic, schemas, or place orders.
"""
from __future__ import annotations

import argparse
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

import mexc_pole_live_trader as live
from research_v2.patterns import mexc_pole_strategy_v1 as strategy

DEFAULT_REPLAY_DB = Path("pnf_mvp/data/pnf_mvp_research_clean.sqlite3")
DEFAULT_SUMMARY = Path("replay_summary.md")


@dataclass(frozen=True)
class ReplayTrade:
    opportunity_id: str
    symbol: str
    direction: str
    observable_entry_ts: int
    entry_price: str
    stop_price: str
    target_price: str
    break_even_trigger_price: str

    @classmethod
    def from_live_plan(cls, plan: live.TradePlan) -> "ReplayTrade":
        return cls(
            opportunity_id=plan.opportunity_id,
            symbol=plan.symbol,
            direction=plan.direction,
            observable_entry_ts=plan.observable_entry_ts,
            entry_price=_decimal_text(plan.entry_price),
            stop_price=_decimal_text(plan.stop_price),
            target_price=_decimal_text(plan.target_price),
            break_even_trigger_price=_decimal_text(plan.break_even_trigger_price),
        )

    @classmethod
    def from_research_row(cls, row: dict[str, Any]) -> "ReplayTrade":
        return cls(
            opportunity_id=str(row["source_opportunity_id"]),
            symbol=str(row["symbol"]),
            direction=str(row["direction"]),
            observable_entry_ts=int(row["observable_entry_ts"]),
            entry_price=_decimal_text(row["entry_price"]),
            stop_price=_decimal_text(row["stop_price"]),
            target_price=_decimal_text(row["target_price"]),
            break_even_trigger_price=_decimal_text(row["break_even_trigger_price"]),
        )


def _decimal_text(value: Any) -> str:
    return format(Decimal(str(value)).quantize(Decimal("0.000001")), "f")


class ReplayClient:
    def get_contract_spec(self, venue_symbol: str) -> live.ContractSpec:
        return live.ContractSpec(symbol=venue_symbol.split(":", 1)[-1])

    def query_position(self, symbol: str) -> list[dict[str, Any]]: return []
    def query_open_orders(self, symbol: str) -> list[dict[str, Any]]: return []
    def query_order(self, order_id: str) -> dict[str, Any]: return {}
    def query_plan_orders(self, symbol: str) -> list[dict[str, Any]]: return []
    def get_mark_price(self, venue_symbol: str) -> Decimal: return Decimal("0")
    def sync_trade(self, row: Any) -> dict[str, Any]: return {"status": row["status"]}
    def place_entry(self, plan: live.TradePlan, order_type: str) -> dict[str, Any]: raise RuntimeError("replay never places orders")
    def place_stop(self, plan: live.TradePlan) -> dict[str, Any]: raise RuntimeError("replay never places orders")
    def place_target(self, plan: live.TradePlan) -> dict[str, Any]: raise RuntimeError("replay never places orders")
    def replace_stop_to_break_even(self, trade_id: int, plan: live.TradePlan) -> dict[str, Any]: raise RuntimeError("replay never places orders")


@contextmanager
def _prefix_loader(prefixes: dict[str, list[tuple[int, float, float, float, float]]]) -> Iterator[None]:
    original = strategy._load_market_candles
    def load(_db_path: Path, symbol: str) -> list[tuple[int, float, float, float, float]]:
        return list(prefixes.get(symbol, []))
    strategy._load_market_candles = load  # type: ignore[assignment]
    try:
        yield
    finally:
        strategy._load_market_candles = original  # type: ignore[assignment]


def _research_trades(prefixes: dict[str, list[tuple[int, float, float, float, float]]], config: live.LiveConfig) -> list[ReplayTrade]:
    box_sizes = {**strategy.DEFAULT_BOX_SIZES, **(config.box_sizes or {})}
    observations = []
    for symbol in config.allowed_symbols:
        candles = prefixes.get(symbol, [])
        columns = strategy._build_columns(symbol, candles, box_sizes[symbol]) if candles else []
        observations.extend(strategy._detect_core_observations(symbol, columns, box_sizes[symbol], candles) if candles else [])
    opportunities = strategy._observable_opportunities(strategy._build_opportunities(observations))
    rows = strategy._trade_plan_rows(strategy._select_one_position_per_symbol(opportunities), float(config.fixed_risk_usdt))
    return [ReplayTrade.from_research_row(row) for row in rows]


def _append_first_seen(target: list[ReplayTrade], seen: set[str], candidates: list[ReplayTrade]) -> None:
    for trade in sorted(candidates, key=lambda row: (row.observable_entry_ts, row.symbol, row.opportunity_id)):
        if trade.opportunity_id not in seen:
            seen.add(trade.opportunity_id)
            target.append(trade)


def run_replay(db_path: Path = DEFAULT_REPLAY_DB, summary_path: Path = DEFAULT_SUMMARY) -> dict[str, Any]:
    config = live.LiveConfig(candles_db_path=db_path, trade_plan_csv_path=summary_path.with_suffix(".trade_plan.csv"))
    try:
        all_candles = {symbol: strategy._load_market_candles(db_path, symbol) for symbol in config.allowed_symbols}
    except sqlite3.OperationalError as exc:
        if "no such table: candles" not in str(exc):
            raise
        all_candles = {symbol: [] for symbol in config.allowed_symbols}
    timeline = sorted({candle[0] for candles in all_candles.values() for candle in candles})
    prefixes = {symbol: [] for symbol in config.allowed_symbols}
    next_idx = {symbol: 0 for symbol in config.allowed_symbols}
    live_sequence: list[ReplayTrade] = []
    research_sequence: list[ReplayTrade] = []
    live_seen: set[str] = set()
    research_seen: set[str] = set()
    client = ReplayClient()

    for ts in timeline:
        for symbol, candles in all_candles.items():
            while next_idx[symbol] < len(candles) and candles[next_idx[symbol]][0] <= ts:
                prefixes[symbol].append(candles[next_idx[symbol]])
                next_idx[symbol] += 1
        research_now = _research_trades(prefixes, config)
        with _prefix_loader(prefixes):
            live_now = [ReplayTrade.from_live_plan(plan) for plan in live.generate_trade_plans(config, client)]
        _append_first_seen(research_sequence, research_seen, research_now)
        _append_first_seen(live_sequence, live_seen, live_now)

    missing = [trade for trade in research_sequence if trade not in live_sequence]
    extra = [trade for trade in live_sequence if trade not in research_sequence]
    first_mismatch = ""
    for index, expected in enumerate(research_sequence):
        observed = live_sequence[index] if index < len(live_sequence) else None
        if observed != expected:
            first_mismatch = f"index {index}: live={observed} research={expected}"
            break
    if not first_mismatch and len(live_sequence) != len(research_sequence):
        first_mismatch = f"length mismatch: live={len(live_sequence)} research={len(research_sequence)}"
    passed = sum(1 for live_trade, research_trade in zip(live_sequence, research_sequence) if live_trade == research_trade)
    failed = max(len(live_sequence), len(research_sequence)) - passed
    summary = {
        "status": "PASS" if live_sequence == research_sequence else "FAIL",
        "opportunities_detected": len(research_seen),
        "parity_passed_count": passed,
        "parity_failed_count": failed,
        "trades_generated": len(live_sequence),
        "trades_matching_research": passed if live_sequence == research_sequence else sum(1 for trade in live_sequence if trade in research_sequence),
        "trades_missing": len(missing),
        "extra_trades": len(extra),
        "first_mismatch": first_mismatch or "None",
    }
    write_summary(summary_path, summary)
    return summary


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    lines = ["# MEXC Pole live replay parity summary", ""]
    for key in ("status", "opportunities_detected", "parity_passed_count", "parity_failed_count", "trades_generated", "trades_matching_research", "trades_missing", "extra_trades", "first_mismatch"):
        lines.append(f"- {key.replace('_', ' ')}: {summary[key]}")
    lines.append("")
    lines.append("PASS only if live trade sequence == research trade sequence.")
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay historical candles through the live MEXC Pole trader and verify research parity")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_REPLAY_DB)
    parser.add_argument("--summary-path", type=Path, default=DEFAULT_SUMMARY)
    args = parser.parse_args()
    summary = run_replay(args.db_path, args.summary_path)
    print(summary["status"])
    if summary["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
