"""Research-only market-family correction audit for the PnF pole baseline stability output.

This narrow correction consumes an existing ``portfolio_reality_trade_sequence.csv`` and
re-evaluates only the market-stability section after recovering missing market-family
metadata from the known research universe:

- BINANCE: BTC, ETH, SOL
- MEXC: ENA, HYPE, SUI, TAO

It intentionally does not rebuild chronology, expectancy, execution, portfolio,
break-even, or symbol-dependence research. It does not modify production strategy
logic, historical trade records, parameters, or the research log.
"""
from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_baseline_stability_audit import (
    ALLOWED_VERDICTS,
    MARKET_FIELDS,
    StabilityTrade,
    _canonical_symbol,
    _concentration,
    _flags,
    _fmt,
    _group_rows,
    _load_trades,
    _market_family_from_row,
    _metrics,
    _rolling_rows,
    _to_float,
    _to_int,
    _verdict,
)

MARKET_FIX_OUTPUT_NAMES = (
    "baseline_stability_market_fix_summary.md",
    "baseline_stability_market_fix_breakdown.csv",
    "baseline_stability_market_fix_flags.csv",
    "baseline_stability_market_fix_manifest.json",
)
KNOWN_MARKET_BY_SYMBOL = {
    "BTC": "BINANCE",
    "ETH": "BINANCE",
    "SOL": "BINANCE",
    "ENA": "MEXC",
    "HYPE": "MEXC",
    "SUI": "MEXC",
    "TAO": "MEXC",
}
UNKNOWN_MARKET_VALUES = {"", "UNKNOWN", "UNKNOWN_MARKET", "UNKNOWN_FAMILY", "NA", "N/A", "NONE", "NULL"}
MARKET_FIX_FLAG_FIELDS = ["flag", "severity", "scope", "details"]
MANAGEMENT_RULES = {"pyramiding": False, "scaling": False, "tp1": False, "tp2": False, "trailing": False}


def inspect_trade_sequence(path: Path) -> dict[str, Any]:
    """Return lightweight column-presence findings without altering input records."""
    with path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        rows = list(reader)
    return {
        "path": str(path),
        "columns": columns,
        "row_count": len(rows),
        "symbol_exists": "symbol" in columns,
        "market_exists": "market" in columns or "market_family" in columns,
        "exchange_exists": "exchange" in columns,
        "source_exists": "source" in columns or "source_market" in columns or "data_source" in columns,
        "explicit_market_columns": [column for column in ("market_family", "market", "exchange", "venue", "source_market", "data_source", "source") if column in columns],
    }


def market_from_symbol(symbol: str) -> str:
    """Map canonical research-universe symbols to their known market family."""
    return KNOWN_MARKET_BY_SYMBOL.get(_canonical_symbol(symbol), "UNKNOWN")


def recover_market_family(row: dict[str, Any], raw_symbol: str) -> tuple[str, str]:
    """Recover market family for reporting; never writes back to historical rows."""
    explicit = _market_family_from_row(row, raw_symbol).strip().upper()
    if explicit not in UNKNOWN_MARKET_VALUES:
        return explicit, "explicit_metadata"
    mapped = market_from_symbol(raw_symbol)
    if mapped != "UNKNOWN":
        return mapped, "symbol_mapping_recovery"
    return "UNKNOWN", "unmapped_symbol"


def load_corrected_trades(path: Path) -> tuple[list[StabilityTrade], dict[str, int]]:
    """Load trades with research-only market-family recovery from known symbols."""
    with path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or [])
        missing = {"symbol", "result_R"} - fields
        if missing:
            raise ValueError(f"missing required trade-sequence column(s): {', '.join(sorted(missing))}")
        rows = list(reader)

    recovery_counts = {"explicit_metadata": 0, "symbol_mapping_recovery": 0, "unmapped_symbol": 0}
    trades: list[StabilityTrade] = []
    for ordinal, row in enumerate(rows, start=1):
        raw_symbol = str(row.get("symbol") or "")
        symbol = _canonical_symbol(raw_symbol)
        if not symbol:
            raise ValueError(f"row {ordinal}: missing symbol")
        market_family, recovery_source = recover_market_family(row, raw_symbol)
        recovery_counts[recovery_source] += 1
        exit_ts = _to_int(row.get("exit_timestamp") or row.get("entry_timestamp"), ordinal)
        trades.append(
            StabilityTrade(
                trade_id=str(row.get("trade_id") or f"ROW-{ordinal:06d}"),
                symbol=symbol,
                market_family=market_family,
                entry_ts=_to_int(row.get("entry_timestamp"), exit_ts),
                exit_ts=exit_ts,
                classification=str(row.get("classification") or "").strip().upper(),
                result_r=_to_float(row.get("result_R")),
            )
        )
    return sorted(trades, key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.symbol, trade.trade_id)), recovery_counts


def _market_positive_by_family(market_rows: list[dict[str, Any]]) -> dict[str, bool]:
    return {row["market_family"]: float(row.get("expectancy_R") or 0) > 0 and float(row.get("total_R") or 0) > 0 for row in market_rows}


def _concentration_rows(total_r: float, market_rows: list[dict[str, Any]], symbol_rows: list[dict[str, Any]], quarterly_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _concentration(total_r, market_rows, "market", "market_family"),
        _concentration(total_r, symbol_rows, "symbol", "symbol"),
        _concentration(total_r, quarterly_rows, "time", "period"),
    ]


def _corrected_verdict(trades: list[StabilityTrade], flags: list[dict[str, str]], concentration_rows: list[dict[str, Any]], market_rows: list[dict[str, Any]]) -> tuple[str, str]:
    positive_markets = _market_positive_by_family(market_rows)
    required_markets_positive = all(positive_markets.get(market, False) for market in ("BINANCE", "MEXC"))
    high_concentration_scopes = {row["scope"] for row in concentration_rows if row["classification"] == "highly_concentrated"}
    if len(trades) < 30 or sum(trade.result_r for trade in trades) <= 0:
        return "INSUFFICIENT_DATA", "not enough positive resolved evidence is available for a corrected market-stability judgment"
    high_flags = [row for row in flags if row["severity"] == "HIGH" and row["flag"] not in {"RESEARCH_ONLY", "NO_PROMOTION", "HIGH_CONCENTRATION"}]
    high_names = {row["flag"] for row in high_flags}
    if high_names & {"YEAR_EXPECTANCY_NON_POSITIVE", "ROLLING_EXPECTANCY_COLLAPSE"}:
        return "FRAGILE_OVER_TIME", "corrected market attribution does not remove a non-positive year or rolling-window collapse"
    if high_concentration_scopes:
        scopes = ", ".join(sorted(high_concentration_scopes))
        return "CONCENTRATED_EDGE", f"corrected attribution still shows high concentration in: {scopes}"
    if not required_markets_positive:
        return "CONCENTRATED_EDGE", "corrected attribution does not show independently positive edge in both BINANCE and MEXC"
    if high_flags or any(row["severity"] == "MEDIUM" for row in flags):
        return "MOSTLY_STABLE", "corrected attribution shows positive edge in both market families with remaining non-fatal validation flags"
    return "STABLE_BASELINE", "corrected attribution shows independently positive edge across both market families without concentration or time-collapse flags"


def _market_fix_flags(
    trades: list[StabilityTrade],
    recovery_counts: dict[str, int],
    market_rows: list[dict[str, Any]],
    symbol_rows: list[dict[str, Any]],
    quarterly_rows: list[dict[str, Any]],
    baseline_flags: list[dict[str, str]],
) -> list[dict[str, str]]:
    total_r = sum(trade.result_r for trade in trades)
    rows: list[dict[str, str]] = [
        {"flag": "RESEARCH_ONLY_MARKET_FIX", "severity": "INFO", "scope": "market_fix", "details": "market-family attribution was recovered only in audit memory; trade records and production logic were not modified"},
        {"flag": "NO_RECOMPUTATION", "severity": "INFO", "scope": "market_fix", "details": "consumed existing portfolio trade sequence; did not rerun chronology, expectancy, execution, portfolio, BE, or symbol-dependence research"},
    ]
    if recovery_counts.get("symbol_mapping_recovery", 0):
        rows.append({"flag": "MISSING_MARKET_RECOVERED", "severity": "INFO", "scope": "market", "details": f"recovered {recovery_counts['symbol_mapping_recovery']} rows via explicit BTC/ETH/SOL->BINANCE and ENA/HYPE/SUI/TAO->MEXC mapping"})
    if recovery_counts.get("unmapped_symbol", 0):
        rows.append({"flag": "UNMAPPED_SYMBOL_MARKET_UNKNOWN", "severity": "HIGH", "scope": "market", "details": f"{recovery_counts['unmapped_symbol']} rows could not be assigned to BINANCE or MEXC"})
    for row in _concentration_rows(total_r, market_rows, symbol_rows, quarterly_rows):
        if row["classification"] == "highly_concentrated":
            rows.append({"flag": "HIGH_CONCENTRATION", "severity": "HIGH", "scope": row["scope"], "details": f"{row['top']} contributes {row['top_contribution_percentage']}% of total_R"})
        elif row["classification"] == "moderately_concentrated":
            rows.append({"flag": "MODERATE_CONCENTRATION", "severity": "MEDIUM", "scope": row["scope"], "details": f"{row['top']} contributes {row['top_contribution_percentage']}% of total_R"})
    for row in baseline_flags:
        if row["flag"] in {"YEAR_EXPECTANCY_NON_POSITIVE", "ROLLING_EXPECTANCY_COLLAPSE", "MARKET_EXPECTANCY_NON_POSITIVE", "BROKEN_SYMBOLS", "QUARTER_EXPECTANCY_NON_POSITIVE"}:
            rows.append(row)
    return rows


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _appendix_block(verdict: str, portfolio: dict[str, Any], market_rows: list[dict[str, Any]]) -> str:
    market_notes = "\n".join(f"- {row['market_family']}: trades={row['trades']}, expectancy_R={row['expectancy_R']}, total_R={row['total_R']}." for row in market_rows)
    return "\n".join(
        [
            "## 2026-06-03 — Appendix: PnF Pole Baseline Stability Market-Family Correction",
            "",
            "### Scope",
            "- Research-only correction audit; not production and not promoted.",
            "- Corrected missing market-family attribution using the known research universe only: BTC/ETH/SOL -> BINANCE; ENA/HYPE/SUI/TAO -> MEXC.",
            "- No chronology, expectancy, execution, portfolio, BE, symbol-dependence, strategy, or parameter research was rerun.",
            "",
            "### Corrected Verdict",
            f"- Verdict: `{verdict}`.",
            f"- Trades: `{_fmt(portfolio['trades'])}`.",
            f"- Expectancy_R: `{_fmt(portfolio['expectancy_R'])}`.",
            f"- Total_R: `{_fmt(portfolio['total_R'])}`.",
            "",
            "### Corrected Market-Family Stability",
            market_notes,
            "",
            "### Promotion Status",
            "- Research-only; no PROMOTE decision is made by this correction.",
        ]
    )


def run(trade_sequence: Path, output_root: Path, baseline_output_root: Path | None = None, rolling_window: int = 20) -> None:
    if rolling_window <= 0:
        raise ValueError("rolling window must be positive")
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in MARKET_FIX_OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing market-fix output(s): {', '.join(existing)}")

    inspection = inspect_trade_sequence(trade_sequence)
    original_trades = _load_trades(trade_sequence)
    trades, recovery_counts = load_corrected_trades(trade_sequence)
    portfolio = _metrics(trades)
    market_rows = _group_rows(trades, "market")
    symbol_rows = _group_rows(trades, "symbol")
    quarterly_rows = _group_rows(trades, "quarter")
    yearly_rows = _group_rows(trades, "year")
    rolling = _rolling_rows(trades, rolling_window)
    baseline_flags = _flags(trades, market_rows, symbol_rows, yearly_rows, quarterly_rows, rolling)
    flags = _market_fix_flags(trades, recovery_counts, market_rows, symbol_rows, quarterly_rows, baseline_flags)
    concentrations = _concentration_rows(float(portfolio["total_R"] or 0), market_rows, symbol_rows, quarterly_rows)
    original_market_rows = _group_rows(original_trades, "market")
    original_market_concentration = _concentration(float(sum(trade.result_r for trade in original_trades)), original_market_rows, "market", "market_family")
    previous_verdict, previous_reason = _verdict(original_trades, _flags(original_trades, original_market_rows, _group_rows(original_trades, "symbol"), _group_rows(original_trades, "year"), _group_rows(original_trades, "quarter"), _rolling_rows(original_trades, rolling_window)))
    verdict, reason = _corrected_verdict(trades, flags, concentrations, market_rows)
    market_positive = _market_positive_by_family(market_rows)
    only_unknown_market_reason_removed = (
        previous_verdict == "CONCENTRATED_EDGE"
        and original_market_concentration.get("top") == "UNKNOWN"
        and original_market_concentration.get("classification") == "highly_concentrated"
        and all(market_positive.get(market, False) for market in ("BINANCE", "MEXC"))
        and "market" not in {row["scope"] for row in concentrations if row["classification"] == "highly_concentrated"}
    )
    appendix = _appendix_block(verdict, portfolio, market_rows) if verdict in {"STABLE_BASELINE", "MOSTLY_STABLE"} else ""

    with (output_root / MARKET_FIX_OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# PnF pole baseline stability market-family correction audit\n\n")
        handle.write("Research only. NOT PRODUCTION. NOT PROMOTED. This audit consumes the existing portfolio trade sequence and corrects market-family attribution only in reporting memory.\n\n")
        handle.write("## Trade-sequence field inspection\n\n")
        handle.write(f"- Input: `{trade_sequence}`\n")
        handle.write(f"- Rows: `{inspection['row_count']}`\n")
        handle.write(f"- Columns: `{', '.join(inspection['columns'])}`\n")
        for key in ("symbol_exists", "market_exists", "exchange_exists", "source_exists"):
            handle.write(f"- `{key}`: `{inspection[key]}`\n")
        handle.write(f"- Explicit market/source columns detected: `{', '.join(inspection['explicit_market_columns']) or 'none'}`\n")
        handle.write("\n## Mapping applied\n\n")
        handle.write("- BINANCE: BTC, ETH, SOL\n- MEXC: ENA, HYPE, SUI, TAO\n")
        handle.write(f"- Rows using explicit metadata: `{recovery_counts['explicit_metadata']}`\n")
        handle.write(f"- Rows recovered from symbol mapping: `{recovery_counts['symbol_mapping_recovery']}`\n")
        handle.write(f"- Rows left UNKNOWN: `{recovery_counts['unmapped_symbol']}`\n\n")
        handle.write("## Corrected market stability\n\n")
        handle.write("| market family | trades | wins | losses | BE exits | expectancy R | total R | win rate | drawdown R | losing streak |\n")
        handle.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in market_rows:
            handle.write(f"| {row['market_family']} | {row['trades']} | {row['wins']} | {row['losses']} | {row['BE_exits']} | {_fmt(row['expectancy_R'])} | {_fmt(row['total_R'])} | {_fmt(row['win_rate'])} | {_fmt(row['max_drawdown_R'])} | {row['longest_losing_streak']} |\n")
        handle.write("\n## Concentration review\n\n")
        for row in concentrations:
            handle.write(f"- `{row['scope']}` concentration: `{row['classification']}` (top=`{_fmt(row['top'])}`, contribution=`{_fmt(row['top_contribution_percentage'])}%`).\n")
        handle.write("\n## Corrected verdict review\n\n")
        handle.write(f"- Previous reconstructed verdict from uncorrected attribution: `{previous_verdict}` ({previous_reason}).\n")
        handle.write(f"- Corrected verdict: **{verdict}** — {reason}.\n")
        handle.write(f"- Is market concentration still present? `{'market' in {row['scope'] for row in concentrations if row['classification'] in {'highly_concentrated', 'moderately_concentrated'}}}`\n")
        handle.write(f"- Is edge positive in both market families? `{all(market_positive.get(market, False) for market in ('BINANCE', 'MEXC'))}`\n")
        handle.write(f"- Does corrected market attribution change the final verdict? `{previous_verdict != verdict}`\n")
        if only_unknown_market_reason_removed:
            handle.write("- The only removed CONCENTRATED_EDGE driver was UNKNOWN market-family attribution, and BINANCE and MEXC remain positive independently.\n")
        handle.write("\n## Proposed research log appendix\n\n")
        if appendix:
            handle.write("Prepared for manual append only; this audit did not modify `docs/research_results_log.md`.\n\n```markdown\n")
            handle.write(appendix)
            handle.write("\n```\n")
        else:
            handle.write("No appendix was prepared because the corrected verdict is not STABLE_BASELINE or MOSTLY_STABLE.\n")

    _write_csv(output_root / MARKET_FIX_OUTPUT_NAMES[1], MARKET_FIELDS, market_rows)
    _write_csv(output_root / MARKET_FIX_OUTPUT_NAMES[2], MARKET_FIX_FLAG_FIELDS, flags)
    manifest = {
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "input_trade_sequence": str(trade_sequence),
        "baseline_output_root": str(baseline_output_root) if baseline_output_root else "",
        "outputs": list(MARKET_FIX_OUTPUT_NAMES),
        "trade_sequence_inspection": inspection,
        "known_market_by_symbol": KNOWN_MARKET_BY_SYMBOL,
        "recovery_counts": recovery_counts,
        "market_positive_by_family": market_positive,
        "concentration": concentrations,
        "previous_reconstructed_verdict": previous_verdict,
        "previous_reconstructed_verdict_reason": previous_reason,
        "corrected_verdict": verdict,
        "corrected_verdict_reason": reason,
        "only_unknown_market_reason_removed": only_unknown_market_reason_removed,
        "appendix_prepared": bool(appendix),
        "research_only": True,
        "not_production": True,
        "production_modifications": False,
        "strategy_modifications": False,
        "parameter_modifications": False,
        "strategy_promotion": False,
        "research_log_modified": False,
        "reran_chronology": False,
        "reran_expectancy": False,
        "reran_execution": False,
        "reran_portfolio": False,
        "reran_break_even": False,
        "reran_symbol_dependence": False,
        "entry": "NEXT_COLUMN_OPEN_ENTRY",
        "stop": "fixed_3_box_stop",
        "target_R": 2.5,
        "break_even_after_R": 2.0,
        "management_rules": MANAGEMENT_RULES,
        "allowed_verdicts": list(ALLOWED_VERDICTS),
    }
    with (output_root / MARKET_FIX_OUTPUT_NAMES[3]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run research-only market-family correction for an existing PnF pole baseline stability audit.")
    parser.add_argument("--trade-sequence", required=True, type=Path, help="Existing portfolio_reality_trade_sequence.csv")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--baseline-output-root", type=Path, default=None, help="Optional existing baseline_stability output directory, recorded for manifest traceability only")
    parser.add_argument("--rolling-window", type=int, default=20)
    args = parser.parse_args(argv)
    run(args.trade_sequence, args.output_root, args.baseline_output_root, args.rolling_window)


if __name__ == "__main__":
    main()
