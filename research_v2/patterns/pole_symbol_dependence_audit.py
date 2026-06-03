"""Research-only symbol dependence audit for the PnF pole portfolio chain.

Fixed chain under audit:
- NEXT_COLUMN_OPEN_ENTRY
- fixed three-box stop
- fixed 2.5R target
- break-even stop after +2R

This module consumes resolved portfolio trades from ``portfolio_reality_trade_sequence.csv``
and performs symbol dependence stress tests only. It does not modify production strategy
code, optimize execution parameters, activate live trading, or output PROMOTE.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_sl_candidates import _round

EXPECTED_SYMBOLS = ("BTC", "ETH", "SOL", "ENA", "HYPE", "SUI", "TAO")
ALLOWED_VERDICTS = (
    "EDGE_DISTRIBUTED",
    "EDGE_PARTIALLY_CONCENTRATED",
    "EDGE_BTC_DEPENDENT",
    "INSUFFICIENT_DATA",
)
OUTPUT_NAMES = (
    "pole_symbol_dependence_summary.md",
    "pole_symbol_dependence_symbol_metrics.csv",
    "pole_symbol_dependence_removal_tests.csv",
    "pole_symbol_dependence_equal_weight.csv",
    "pole_symbol_dependence_manifest.json",
)
SYMBOL_FIELDS = ["symbol", "trades", "wins", "losses", "BE_exits", "win_rate", "expectancy_R", "total_R", "contribution_percentage"]
REMOVAL_FIELDS = ["removed_symbol", "trades", "win_rate", "expectancy_R", "total_R", "verdict"]
EQUAL_WEIGHT_FIELDS = ["symbol", "sampled_trades", "source_trades", "win_rate", "expectancy_R", "total_R"]


@dataclass(frozen=True)
class AuditTrade:
    trade_id: str
    symbol: str
    exit_ts: int
    result_r: float


def _canonical_symbol(raw: str) -> str:
    symbol = raw.strip().upper()
    if ":" in symbol:
        symbol = symbol.rsplit(":", 1)[-1]
    if symbol.endswith("USDT"):
        symbol = symbol[:-4]
    return symbol


def _to_float(value: Any) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid numeric result_R: {value!r}") from exc


def _to_int(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value!r}") from exc


def _load_trades(path: Path) -> list[AuditTrade]:
    with path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or [])
        missing = {"symbol", "result_R"} - fields
        if missing:
            raise ValueError(f"missing required trade-sequence column(s): {', '.join(sorted(missing))}")
        rows = list(reader)

    trades: list[AuditTrade] = []
    for ordinal, row in enumerate(rows, start=1):
        symbol = _canonical_symbol(row.get("symbol", ""))
        if not symbol:
            raise ValueError(f"row {ordinal}: missing symbol")
        trade_id = str(row.get("trade_id") or f"ROW-{ordinal:06d}")
        exit_ts = _to_int(row.get("exit_timestamp") or row.get("entry_timestamp") or ordinal)
        trades.append(AuditTrade(trade_id=trade_id, symbol=symbol, exit_ts=exit_ts, result_r=_to_float(row.get("result_R"))))
    return trades


def _safe_div(numerator: float, denominator: float) -> float | str:
    return _round(numerator / denominator) if denominator else ""


def _metrics(trades: Iterable[AuditTrade]) -> dict[str, Any]:
    scoped = list(trades)
    total = sum(trade.result_r for trade in scoped)
    wins = sum(trade.result_r > 0 for trade in scoped)
    losses = sum(trade.result_r < 0 for trade in scoped)
    be_exits = sum(trade.result_r == 0 for trade in scoped)
    return {
        "trades": len(scoped),
        "wins": wins,
        "losses": losses,
        "BE_exits": be_exits,
        "win_rate": _safe_div(wins, len(scoped)),
        "expectancy_R": _safe_div(total, len(scoped)),
        "total_R": _round(total),
    }


def _symbol_rows(trades: list[AuditTrade]) -> list[dict[str, Any]]:
    total_r = sum(trade.result_r for trade in trades)
    rows: list[dict[str, Any]] = []
    for symbol in sorted({trade.symbol for trade in trades}):
        scoped = [trade for trade in trades if trade.symbol == symbol]
        metrics = _metrics(scoped)
        contribution = _round(float(metrics["total_R"]) / total_r * 100) if total_r > 0 else ""
        rows.append({"symbol": symbol, **metrics, "contribution_percentage": contribution})
    return rows


def _removal_rows(trades: list[AuditTrade], symbols: Iterable[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        scoped = [trade for trade in trades if trade.symbol != symbol]
        metrics = _metrics(scoped)
        rows.append({"removed_symbol": symbol, **{k: metrics[k] for k in ("trades", "win_rate", "expectancy_R", "total_R")}, "verdict": _verdict(scoped, _symbol_rows(scoped))})
    return rows


def _equal_weight_rows(trades: list[AuditTrade], symbols: Iterable[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_symbol: dict[str, list[AuditTrade]] = defaultdict(list)
    for trade in sorted(trades, key=lambda row: (row.exit_ts, row.trade_id)):
        by_symbol[trade.symbol].append(trade)
    present = [symbol for symbol in symbols if by_symbol.get(symbol)]
    cap = min((len(by_symbol[symbol]) for symbol in present), default=0)
    rows: list[dict[str, Any]] = []
    sampled: list[AuditTrade] = []
    for symbol in present:
        scoped = by_symbol[symbol][:cap]
        sampled.extend(scoped)
        metrics = _metrics(scoped)
        rows.append({"symbol": symbol, "sampled_trades": metrics["trades"], "source_trades": len(by_symbol[symbol]), "win_rate": metrics["win_rate"], "expectancy_R": metrics["expectancy_R"], "total_R": metrics["total_R"]})
    return rows, _metrics(sampled)


def _contribution_normalized_expectancy(symbol_rows: list[dict[str, Any]]) -> float | str:
    positive_rows = [row for row in symbol_rows if int(row["trades"]) > 0]
    if not positive_rows:
        return ""
    return _round(sum(float(row["expectancy_R"]) for row in positive_rows) / len(positive_rows))


def _verdict(trades: list[AuditTrade], symbol_rows: list[dict[str, Any]]) -> str:
    if len(trades) < 30 or len(symbol_rows) < 3:
        return "INSUFFICIENT_DATA"
    portfolio = _metrics(trades)
    if float(portfolio["expectancy_R"] or 0) <= 0:
        return "INSUFFICIENT_DATA"
    btc = next((row for row in symbol_rows if row["symbol"] == "BTC"), None)
    without_btc = _metrics(trade for trade in trades if trade.symbol != "BTC")
    if btc and float(btc["contribution_percentage"] or 0) >= 50 and float(without_btc["expectancy_R"] or 0) <= 0:
        return "EDGE_BTC_DEPENDENT"
    top_contribution = max((float(row["contribution_percentage"] or 0) for row in symbol_rows), default=0.0)
    positive_symbols = sum(float(row["expectancy_R"] or 0) > 0 for row in symbol_rows)
    if top_contribution >= 50 or positive_symbols < max(3, len(symbol_rows) // 2):
        return "EDGE_PARTIALLY_CONCENTRATED"
    return "EDGE_DISTRIBUTED"


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value: Any) -> str:
    return str(value) if value != "" else "NA"


def run(trade_sequence: Path, output_root: Path, symbols: Iterable[str] = EXPECTED_SYMBOLS) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing symbol dependence output(s): {', '.join(existing)}")

    expected_symbols = tuple(_canonical_symbol(symbol) for symbol in symbols)
    trades = _load_trades(trade_sequence)
    portfolio = _metrics(trades)
    symbol_rows = _symbol_rows(trades)
    without_btc = _metrics(trade for trade in trades if trade.symbol != "BTC")
    removal_rows = _removal_rows(trades, expected_symbols)
    equal_rows, equal_portfolio = _equal_weight_rows(trades, expected_symbols)
    normalized_expectancy = _contribution_normalized_expectancy(symbol_rows)
    verdict = _verdict(trades, symbol_rows)

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# PnF pole symbol dependence audit\n\n")
        handle.write("Research only. NOT PRODUCTION. NOT PROMOTED. This audit does not alter strategy code or live trading behavior.\n\n")
        handle.write("## Fixed chain\n\n")
        handle.write("- Entry: `NEXT_COLUMN_OPEN_ENTRY`\n- Stop: fixed 3-box stop\n- Target: fixed 2.5R\n- Management: move stop to break-even after +2R\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n")
        handle.write("## Portfolio excluding BTC\n\n")
        for key in ("trades", "win_rate", "expectancy_R", "total_R"):
            handle.write(f"- `{key}`: {_fmt(without_btc[key])}\n")
        handle.write("\n## Equal-weight opportunity test\n\n")
        for key in ("trades", "win_rate", "expectancy_R", "total_R"):
            handle.write(f"- `{key}`: {_fmt(equal_portfolio[key])}\n")
        handle.write(f"\n## Contribution-normalized expectancy\n\n- `contribution_normalized_expectancy_R`: {_fmt(normalized_expectancy)}\n")
        handle.write("\n## Symbol contribution\n\n| symbol | trades | win rate | expectancy R | total R | contribution % |\n|---|---:|---:|---:|---:|---:|\n")
        for row in symbol_rows:
            handle.write(f"| {row['symbol']} | {row['trades']} | {_fmt(row['win_rate'])} | {_fmt(row['expectancy_R'])} | {_fmt(row['total_R'])} | {_fmt(row['contribution_percentage'])} |\n")
        handle.write("\n## Symbol removal test\n\n| removed symbol | trades | win rate | expectancy R | total R | verdict |\n|---|---:|---:|---:|---:|---|\n")
        for row in removal_rows:
            handle.write(f"| {row['removed_symbol']} | {row['trades']} | {_fmt(row['win_rate'])} | {_fmt(row['expectancy_R'])} | {_fmt(row['total_R'])} | {row['verdict']} |\n")
        handle.write("\n## Questions\n\n")
        handle.write(f"1. Expectancy without BTC is {'positive' if float(without_btc['expectancy_R'] or 0) > 0 else 'not positive'} (`{_fmt(without_btc['expectancy_R'])}` R/trade).\n")
        top = max(symbol_rows, key=lambda row: float(row["total_R"])) if symbol_rows else None
        handle.write(f"2. Top contributor: {top['symbol']} at {_fmt(top['contribution_percentage'])}% of total R.\n" if top else "2. Top contributor: NA.\n")
        handle.write(f"3. Equal-weight expectancy is {'positive' if float(equal_portfolio['expectancy_R'] or 0) > 0 else 'not positive'} (`{_fmt(equal_portfolio['expectancy_R'])}` R/trade).\n")
        handle.write("4. BTC dependence is treated as a structural risk when BTC contributes >=50% and BTC removal turns expectancy non-positive; otherwise concentration may still be a partial concentration or trade-count artifact.\n")

    _write_csv(output_root / OUTPUT_NAMES[1], SYMBOL_FIELDS, symbol_rows)
    _write_csv(output_root / OUTPUT_NAMES[2], REMOVAL_FIELDS, removal_rows)
    _write_csv(output_root / OUTPUT_NAMES[3], EQUAL_WEIGHT_FIELDS, equal_rows)
    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_symbol_dependence_audit",
        "research_only": True,
        "not_production": True,
        "strategy_promotion": False,
        "production_modifications": False,
        "input_trade_sequence": str(trade_sequence),
        "entry": "NEXT_COLUMN_OPEN_ENTRY",
        "stop": "fixed_3_box_stop",
        "target_R": 2.5,
        "break_even_after_R": 2.0,
        "symbols_tested": list(expected_symbols),
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "verdict": verdict,
        "portfolio": portfolio,
        "portfolio_without_BTC": without_btc,
        "equal_weight_portfolio": equal_portfolio,
        "contribution_normalized_expectancy_R": normalized_expectancy,
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[4]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only PnF pole symbol dependence audit")
    parser.add_argument("--trade-sequence", required=True, type=Path, help="portfolio_reality_trade_sequence.csv from the fixed PnF pole portfolio audit")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--symbol", action="append", default=[], help="optional explicit symbol removal/equal-weight universe; repeatable")
    args = parser.parse_args()
    try:
        run(args.trade_sequence, args.output_root, args.symbol or EXPECTED_SYMBOLS)
    except (FileExistsError, OSError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
