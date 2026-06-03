"""Final research-only stability audit for the validated PnF pole baseline.

Fixed chain under audit:
- NEXT_COLUMN_OPEN_ENTRY
- fixed three-box stop
- fixed 2.5R target
- break-even stop after +2R

This module consumes an already-generated portfolio trade sequence (preferably
``portfolio_reality_trade_sequence.csv``) and performs validation-only stability
analysis across market families, symbols, time, rolling windows, drawdown, and
concentration. It does not rebuild prior research chains, modify production
strategy logic, optimize parameters, activate live trading, or output PROMOTE.
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

ALLOWED_VERDICTS = (
    "STABLE_BASELINE",
    "MOSTLY_STABLE",
    "FRAGILE_OVER_TIME",
    "CONCENTRATED_EDGE",
    "INSUFFICIENT_DATA",
)
OUTPUT_NAMES = (
    "baseline_stability_summary.md",
    "baseline_stability_symbol_breakdown.csv",
    "baseline_stability_market_breakdown.csv",
    "baseline_stability_yearly.csv",
    "baseline_stability_quarterly.csv",
    "baseline_stability_rolling.csv",
    "baseline_stability_flags.csv",
    "baseline_stability_manifest.json",
)
SYMBOL_FIELDS = [
    "symbol",
    "trades",
    "wins",
    "losses",
    "BE_exits",
    "expectancy_R",
    "total_R",
    "win_rate",
    "BE_rate",
    "contribution_percentage",
    "flag",
]
MARKET_FIELDS = ["market_family", "trades", "wins", "losses", "BE_exits", "expectancy_R", "total_R", "win_rate", "BE_rate", "max_drawdown_R", "longest_losing_streak"]
PERIOD_FIELDS = ["period", "trades", "wins", "losses", "BE_exits", "expectancy_R", "total_R", "win_rate", "BE_rate", "max_drawdown_R", "longest_losing_streak"]
ROLLING_FIELDS = ["window_start_sequence", "window_end_sequence", "start_exit_time_utc", "end_exit_time_utc", "trades", "expectancy_R", "total_R", "win_rate"]
FLAG_FIELDS = ["flag", "severity", "scope", "details"]
MARKET_HINT_COLUMNS = ("market_family", "market", "exchange", "venue", "source_market", "data_source")
REQUIRED_LOG_PATH = Path("docs/research_results_log.md")


@dataclass(frozen=True)
class StabilityTrade:
    trade_id: str
    symbol: str
    market_family: str
    entry_ts: int
    exit_ts: int
    classification: str
    result_r: float


def _ts_to_utc(ts: int | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(ts / 1000 if ts > 10_000_000_000 else ts, tz=UTC).isoformat()


def _dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts / 1000 if ts > 10_000_000_000 else ts, tz=UTC)


def _to_float(value: Any) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid numeric result_R: {value!r}") from exc


def _to_int(value: Any, fallback: int = 0) -> int:
    text = str(value or "").strip()
    if not text:
        return fallback
    try:
        return int(float(text))
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value!r}") from exc


def _canonical_symbol(raw: str) -> str:
    symbol = raw.strip().upper()
    for separator in (":", "/"):
        if separator in symbol:
            symbol = symbol.rsplit(separator, 1)[-1]
    if symbol.endswith("USDT"):
        symbol = symbol[:-4]
    return symbol


def _market_family_from_row(row: dict[str, Any], raw_symbol: str) -> str:
    for column in MARKET_HINT_COLUMNS:
        value = str(row.get(column) or "").strip().upper()
        if value:
            if "BINANCE" in value:
                return "BINANCE"
            if "MEXC" in value:
                return "MEXC"
            return value
    symbol = raw_symbol.strip().upper()
    if ":" in symbol:
        prefix = symbol.split(":", 1)[0]
        if "BINANCE" in prefix:
            return "BINANCE"
        if "MEXC" in prefix:
            return "MEXC"
        return prefix or "UNKNOWN"
    return "UNKNOWN"


def _load_trades(path: Path) -> list[StabilityTrade]:
    with path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or [])
        missing = {"symbol", "result_R"} - fields
        if missing:
            raise ValueError(f"missing required trade-sequence column(s): {', '.join(sorted(missing))}")
        rows = list(reader)

    trades: list[StabilityTrade] = []
    for ordinal, row in enumerate(rows, start=1):
        raw_symbol = str(row.get("symbol") or "")
        symbol = _canonical_symbol(raw_symbol)
        if not symbol:
            raise ValueError(f"row {ordinal}: missing symbol")
        exit_ts = _to_int(row.get("exit_timestamp") or row.get("entry_timestamp"), ordinal)
        trades.append(
            StabilityTrade(
                trade_id=str(row.get("trade_id") or f"ROW-{ordinal:06d}"),
                symbol=symbol,
                market_family=_market_family_from_row(row, raw_symbol),
                entry_ts=_to_int(row.get("entry_timestamp"), exit_ts),
                exit_ts=exit_ts,
                classification=str(row.get("classification") or "").strip().upper(),
                result_r=_to_float(row.get("result_R")),
            )
        )
    return sorted(trades, key=lambda row: (row.exit_ts, row.entry_ts, row.symbol, row.trade_id))


def _safe_rate(numerator: int | float, denominator: int | float) -> float | str:
    return _round(numerator / denominator) if denominator else ""


def _max_losing_streak(trades: Iterable[StabilityTrade]) -> int:
    best = current = 0
    for trade in trades:
        if trade.result_r < 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _drawdown(trades: Iterable[StabilityTrade]) -> float:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for trade in trades:
        cumulative += trade.result_r
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return _round(max_drawdown)


def _metrics(trades: Iterable[StabilityTrade]) -> dict[str, Any]:
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
        "expectancy_R": _round(total / len(scoped)) if scoped else "",
        "total_R": _round(total),
        "win_rate": _safe_rate(wins, len(scoped)),
        "BE_rate": _safe_rate(be_exits, len(scoped)),
    }


def _group_rows(trades: list[StabilityTrade], group_name: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[StabilityTrade]] = defaultdict(list)
    for trade in trades:
        if group_name == "symbol":
            key = trade.symbol
        elif group_name == "market":
            key = trade.market_family
        elif group_name == "year":
            key = str(_dt(trade.exit_ts).year)
        elif group_name == "quarter":
            dt = _dt(trade.exit_ts)
            key = f"{dt.year}-Q{((dt.month - 1) // 3) + 1}"
        else:
            raise ValueError(f"unsupported group: {group_name}")
        grouped[key].append(trade)

    total_r = sum(trade.result_r for trade in trades)
    rows: list[dict[str, Any]] = []
    for key in sorted(grouped):
        scoped = sorted(grouped[key], key=lambda row: (row.exit_ts, row.entry_ts, row.symbol, row.trade_id))
        metrics = _metrics(scoped)
        row_key = "symbol" if group_name == "symbol" else "market_family" if group_name == "market" else "period"
        row = {row_key: key, **metrics}
        if group_name in {"market", "year", "quarter"}:
            row["max_drawdown_R"] = _drawdown(scoped)
            row["longest_losing_streak"] = _max_losing_streak(scoped)
        if group_name == "symbol":
            row["contribution_percentage"] = _round(float(metrics["total_R"]) / total_r * 100) if total_r > 0 else ""
            row["flag"] = "BROKEN" if len(scoped) >= 5 and float(metrics["expectancy_R"] or 0) < 0 else ""
        rows.append(row)
    return rows


def _rolling_rows(trades: list[StabilityTrade], window: int) -> list[dict[str, Any]]:
    if not trades:
        return []
    effective_window = min(window, len(trades))
    rows: list[dict[str, Any]] = []
    for start in range(0, len(trades) - effective_window + 1):
        scoped = trades[start : start + effective_window]
        metrics = _metrics(scoped)
        rows.append(
            {
                "window_start_sequence": start + 1,
                "window_end_sequence": start + effective_window,
                "start_exit_time_utc": _ts_to_utc(scoped[0].exit_ts),
                "end_exit_time_utc": _ts_to_utc(scoped[-1].exit_ts),
                "trades": effective_window,
                "expectancy_R": metrics["expectancy_R"],
                "total_R": metrics["total_R"],
                "win_rate": metrics["win_rate"],
            }
        )
    return rows


def _concentration(total_r: float, rows: list[dict[str, Any]], label: str, key: str) -> dict[str, Any]:
    positive_rows = [row for row in rows if float(row.get("total_R") or 0) > 0]
    if total_r <= 0 or not positive_rows:
        return {"scope": label, "top": "", "top_contribution_percentage": "", "classification": "INSUFFICIENT_DATA"}
    top = max(positive_rows, key=lambda row: float(row["total_R"]))
    pct = _round(float(top["total_R"]) / total_r * 100)
    if pct >= 60:
        classification = "highly_concentrated"
    elif pct >= 40:
        classification = "moderately_concentrated"
    else:
        classification = "diversified"
    return {"scope": label, "top": top[key], "top_contribution_percentage": pct, "classification": classification}


def _flags(
    trades: list[StabilityTrade],
    market_rows: list[dict[str, Any]],
    symbol_rows: list[dict[str, Any]],
    yearly_rows: list[dict[str, Any]],
    quarterly_rows: list[dict[str, Any]],
    rolling_rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    flags: list[dict[str, str]] = [
        {"flag": "RESEARCH_ONLY", "severity": "INFO", "scope": "baseline_stability", "details": "validation-only audit; no production strategy or execution parameter changes"},
        {"flag": "NO_PROMOTION", "severity": "INFO", "scope": "baseline_stability", "details": "allowed verdicts never include PROMOTE"},
    ]
    total_r = sum(trade.result_r for trade in trades)
    if len(trades) < 30:
        flags.append({"flag": "INSUFFICIENT_DATA", "severity": "HIGH", "scope": "portfolio", "details": "fewer than 30 resolved portfolio trades are available"})
    if not any(row["market_family"] == "BINANCE" for row in market_rows):
        flags.append({"flag": "MISSING_BINANCE_GROUP", "severity": "MEDIUM", "scope": "market", "details": "no Binance market-family rows were detected in the supplied trade sequence"})
    if not any(row["market_family"] == "MEXC" for row in market_rows):
        flags.append({"flag": "MISSING_MEXC_GROUP", "severity": "MEDIUM", "scope": "market", "details": "no MEXC market-family rows were detected in the supplied trade sequence"})
    for row in market_rows:
        if int(row["trades"]) >= 5 and float(row["expectancy_R"] or 0) <= 0:
            flags.append({"flag": "MARKET_EXPECTANCY_NON_POSITIVE", "severity": "HIGH", "scope": row["market_family"], "details": f"market family expectancy_R={row['expectancy_R']}"})
    for row in yearly_rows:
        if int(row["trades"]) >= 5 and float(row["expectancy_R"] or 0) <= 0:
            flags.append({"flag": "YEAR_EXPECTANCY_NON_POSITIVE", "severity": "HIGH", "scope": row["period"], "details": f"year expectancy_R={row['expectancy_R']}"})
    for row in quarterly_rows:
        if int(row["trades"]) >= 5 and float(row["expectancy_R"] or 0) <= 0:
            flags.append({"flag": "QUARTER_EXPECTANCY_NON_POSITIVE", "severity": "MEDIUM", "scope": row["period"], "details": f"quarter expectancy_R={row['expectancy_R']}"})
    if rolling_rows:
        worst = min(rolling_rows, key=lambda row: float(row["expectancy_R"] or 0))
        if float(worst["expectancy_R"] or 0) <= 0:
            flags.append({"flag": "ROLLING_EXPECTANCY_COLLAPSE", "severity": "HIGH", "scope": f"{worst['window_start_sequence']}-{worst['window_end_sequence']}", "details": f"worst rolling expectancy_R={worst['expectancy_R']}"})
    broken = [row["symbol"] for row in symbol_rows if row.get("flag") == "BROKEN"]
    if broken:
        flags.append({"flag": "BROKEN_SYMBOLS", "severity": "MEDIUM", "scope": "symbol", "details": ", ".join(broken)})
    for concentration in (
        _concentration(total_r, symbol_rows, "symbol", "symbol"),
        _concentration(total_r, quarterly_rows, "time", "period"),
        _concentration(total_r, market_rows, "market", "market_family"),
    ):
        if concentration["classification"] == "highly_concentrated":
            flags.append({"flag": "HIGH_CONCENTRATION", "severity": "HIGH", "scope": concentration["scope"], "details": f"{concentration['top']} contributes {concentration['top_contribution_percentage']}% of total_R"})
        elif concentration["classification"] == "moderately_concentrated":
            flags.append({"flag": "MODERATE_CONCENTRATION", "severity": "MEDIUM", "scope": concentration["scope"], "details": f"{concentration['top']} contributes {concentration['top_contribution_percentage']}% of total_R"})
    return flags


def _verdict(trades: list[StabilityTrade], flags: list[dict[str, str]]) -> tuple[str, str]:
    if len(trades) < 30 or sum(trade.result_r for trade in trades) <= 0:
        return "INSUFFICIENT_DATA", "not enough positive resolved evidence is available for a final stability judgment"
    high_flags = [row for row in flags if row["severity"] == "HIGH" and row["flag"] not in {"RESEARCH_ONLY", "NO_PROMOTION"}]
    medium_flags = [row for row in flags if row["severity"] == "MEDIUM"]
    high_names = {row["flag"] for row in high_flags}
    if "HIGH_CONCENTRATION" in high_names:
        return "CONCENTRATED_EDGE", "positive edge remains, but contribution concentration is high"
    if high_names & {"YEAR_EXPECTANCY_NON_POSITIVE", "ROLLING_EXPECTANCY_COLLAPSE"}:
        return "FRAGILE_OVER_TIME", "positive edge has a non-positive year or rolling window"
    if high_flags:
        return "MOSTLY_STABLE", "positive edge has high-severity validation flags outside the primary time-collapse checks"
    if medium_flags:
        return "MOSTLY_STABLE", "positive edge survives with moderate validation flags"
    return "STABLE_BASELINE", "positive edge is independently stable across configured market, symbol, time, drawdown, and concentration checks"


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value: Any) -> str:
    return str(value) if value != "" else "NA"


def _appendix_block(verdict: str, portfolio: dict[str, Any], strongest: dict[str, Any] | None, weakest: dict[str, Any] | None) -> str:
    return "\n".join(
        [
            "## 2026-06-03 — Phase: PnF Pole Motif Final Baseline Stability Audit",
            "",
            "### Scope",
            "- Research-only validation layer; not production and not promoted.",
            "- Fixed chain: `NEXT_COLUMN_OPEN_ENTRY`, fixed 3-box stop, fixed 2.5R target, break-even after +2R.",
            "- No TP1, TP2, trailing, scaling, pyramiding, filters, or optimization variants were introduced.",
            "",
            "### Key Summary",
            "",
            "| Metric | Value |",
            "|---|---:|",
            f"| verdict | {verdict} |",
            f"| trades | {_fmt(portfolio['trades'])} |",
            f"| win_rate | {_fmt(portfolio['win_rate'])} |",
            f"| expectancy_R | {_fmt(portfolio['expectancy_R'])} |",
            f"| total_R | {_fmt(portfolio['total_R'])} |",
            "",
            "### Stability Notes",
            f"- Strongest symbol: {(strongest or {}).get('symbol', 'NA')}.",
            f"- Weakest symbol: {(weakest or {}).get('symbol', 'NA')}.",
            "- Verdict remains research-only and must not be read as PROMOTE.",
        ]
    )


def run(trade_sequence: Path, output_root: Path, rolling_window: int = 20, research_log: Path = REQUIRED_LOG_PATH) -> None:
    if rolling_window <= 0:
        raise ValueError("rolling window must be positive")
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing baseline stability output(s): {', '.join(existing)}")

    research_log_inspected = research_log.exists()
    if not research_log_inspected:
        raise FileNotFoundError(f"required research log not found: {research_log}")
    research_log.read_text()

    trades = _load_trades(trade_sequence)
    portfolio = _metrics(trades)
    market_rows = _group_rows(trades, "market")
    symbol_rows = _group_rows(trades, "symbol")
    yearly_rows = _group_rows(trades, "year")
    quarterly_rows = _group_rows(trades, "quarter")
    rolling = _rolling_rows(trades, rolling_window)
    flags = _flags(trades, market_rows, symbol_rows, yearly_rows, quarterly_rows, rolling)
    verdict, reason = _verdict(trades, flags)
    strongest = max(symbol_rows, key=lambda row: float(row["total_R"])) if symbol_rows else None
    weakest = min(symbol_rows, key=lambda row: float(row["expectancy_R"] or 0)) if symbol_rows else None
    worst_rolling = min(rolling, key=lambda row: float(row["expectancy_R"] or 0)) if rolling else None
    best_rolling = max(rolling, key=lambda row: float(row["expectancy_R"] or 0)) if rolling else None
    total_r = float(portfolio["total_R"] or 0)
    concentrations = [
        _concentration(total_r, symbol_rows, "symbol", "symbol"),
        _concentration(total_r, quarterly_rows, "time", "period"),
        _concentration(total_r, market_rows, "market", "market_family"),
    ]
    appendix = _appendix_block(verdict, portfolio, strongest, weakest) if verdict in {"STABLE_BASELINE", "MOSTLY_STABLE"} else ""

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# PnF pole baseline stability audit\n\n")
        handle.write("Research only. NOT PRODUCTION. NOT PROMOTED. This audit consumes existing trade-sequence output and does not alter strategy logic.\n\n")
        handle.write("## Fixed validated research baseline\n\n")
        handle.write("- Entry: `NEXT_COLUMN_OPEN_ENTRY`\n- Stop: fixed 3-box stop\n- Target: fixed 2.5R\n- Management: move stop to break-even after +2R\n- No TP1, TP2, trailing, scaling, pyramiding, filters, or optimization variants\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n{reason}.\n\n")
        handle.write("## Portfolio scorecard\n\n")
        for key in ("trades", "wins", "losses", "BE_exits", "expectancy_R", "total_R", "win_rate", "BE_rate"):
            handle.write(f"- `{key}`: {_fmt(portfolio[key])}\n")
        handle.write("\n## Market stability\n\n| market family | trades | expectancy R | total R | win rate | max drawdown R | losing streak |\n|---|---:|---:|---:|---:|---:|---:|\n")
        for row in market_rows:
            handle.write(f"| {row['market_family']} | {row['trades']} | {_fmt(row['expectancy_R'])} | {_fmt(row['total_R'])} | {_fmt(row['win_rate'])} | {_fmt(row['max_drawdown_R'])} | {row['longest_losing_streak']} |\n")
        handle.write("\n## Symbol stability\n\n")
        handle.write(f"- Strongest symbol: `{(strongest or {}).get('symbol', 'NA')}`\n")
        handle.write(f"- Weakest symbol: `{(weakest or {}).get('symbol', 'NA')}`\n")
        handle.write("\n## Time stability\n\n")
        handle.write("- Yearly and quarterly details are written to the CSV outputs.\n")
        if worst_rolling and best_rolling:
            handle.write(f"- Worst rolling period: rows {worst_rolling['window_start_sequence']}-{worst_rolling['window_end_sequence']} expectancy_R={worst_rolling['expectancy_R']} total_R={worst_rolling['total_R']}\n")
            handle.write(f"- Best rolling period: rows {best_rolling['window_start_sequence']}-{best_rolling['window_end_sequence']} expectancy_R={best_rolling['expectancy_R']} total_R={best_rolling['total_R']}\n")
        handle.write("\n## Concentration stability\n\n")
        for row in concentrations:
            handle.write(f"- `{row['scope']}`: {row['classification']} (top={_fmt(row['top'])}, contribution={_fmt(row['top_contribution_percentage'])}%)\n")
        handle.write("\n## Proposed research log appendix\n\n")
        if appendix:
            handle.write("The following block was prepared for manual append only; this audit did not modify `docs/research_results_log.md`.\n\n")
            handle.write("```markdown\n")
            handle.write(appendix)
            handle.write("\n```\n")
        else:
            handle.write("No positive-verdict appendix was prepared because the verdict is not STABLE_BASELINE or MOSTLY_STABLE.\n")

    _write_csv(output_root / OUTPUT_NAMES[1], SYMBOL_FIELDS, symbol_rows)
    _write_csv(output_root / OUTPUT_NAMES[2], MARKET_FIELDS, market_rows)
    _write_csv(output_root / OUTPUT_NAMES[3], PERIOD_FIELDS, yearly_rows)
    _write_csv(output_root / OUTPUT_NAMES[4], PERIOD_FIELDS, quarterly_rows)
    _write_csv(output_root / OUTPUT_NAMES[5], ROLLING_FIELDS, rolling)
    _write_csv(output_root / OUTPUT_NAMES[6], FLAG_FIELDS, flags)
    manifest = {
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "input_trade_sequence": str(trade_sequence),
        "research_log_inspected": str(research_log),
        "research_log_modified": False,
        "research_only": True,
        "not_production": True,
        "production_modifications": False,
        "strategy_promotion": False,
        "entry": "NEXT_COLUMN_OPEN_ENTRY",
        "stop": "fixed_3_box_stop",
        "target_R": 2.5,
        "break_even_after_R": 2.0,
        "management_rules": {"pyramiding": False, "scaling": False, "tp1": False, "tp2": False, "trailing": False},
        "rolling_window_trades": rolling_window,
        "outputs": list(OUTPUT_NAMES),
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "verdict": verdict,
        "verdict_reason": reason,
        "appendix_prepared": bool(appendix),
    }
    with (output_root / OUTPUT_NAMES[7]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run final research-only stability audit from an existing portfolio trade sequence.")
    parser.add_argument("--trade-sequence", required=True, type=Path, help="Existing portfolio_reality_trade_sequence.csv or equivalent resolved trade sequence")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--rolling-window", type=int, default=20)
    parser.add_argument("--research-log", type=Path, default=REQUIRED_LOG_PATH)
    args = parser.parse_args(argv)
    run(args.trade_sequence, args.output_root, args.rolling_window, args.research_log)


if __name__ == "__main__":
    main()
