"""Focused research-only survivor audit for CAND-000053 and CAND-000065.

This module replays exactly two already-sampled causal P+2 candidate definitions
against the validated reconstruction and audits whether each survivor remains
credible by symbol, quarter, year, leave-one-symbol-out, leave-one-quarter-out,
leave-one-year-out, and simple realized-R equity diagnostics. It deliberately
performs no genetic algorithm, optimization, live trader change, detector change,
production strategy change, schema change, or candidate promotion.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import _parse_symbol_input, _round
from research_v2.patterns.pole_p2_candidate_sampler import SamplerOutcome, _build_outcomes, _metric_counts, _universe_consistency
from research_v2.patterns.pole_p2_candidate_stability_audit import (
    ALLOWED_VERDICTS,
    CandidateRule,
    _matches_rule,
    _numeric,
    _parse_rule_definition,
    _retention,
    _verdict,
)
from research_v2.patterns.pole_p2_causal_motif_audit import EXPECTED_SYMBOLS, MOTIF_NAME, _validate_full_universe
from research_v2.patterns.pole_p2_edge_segmentation_audit import BASELINE_P2_EXPECTANCY, BASELINE_P2_TOTAL_R, BASELINE_P2_TRADES, load_segmented_observations

TARGET_CANDIDATE_IDS = ("CAND-000053", "CAND-000065")
DEFAULT_MIN_CANDIDATE_TRADES = 30
DEFAULT_MIN_REMOVAL_TRADES = 10
OUTPUT_NAMES = (
    "p2_survivor_audit_summary.md",
    "p2_survivor_audit_symbol_breakdown.csv",
    "p2_survivor_audit_quarter_breakdown.csv",
    "p2_survivor_audit_year_breakdown.csv",
    "p2_survivor_audit_leave_symbol_out.csv",
    "p2_survivor_audit_leave_quarter_out.csv",
    "p2_survivor_audit_leave_year_out.csv",
    "p2_survivor_audit_equity_metrics.csv",
    "p2_survivor_audit_manifest.json",
)
BREAKDOWN_FIELDS = [
    "candidate_id",
    "breakdown_type",
    "breakdown_value",
    "source_rank",
    "source_candidate_verdict",
    "rule_width",
    "rule_definition",
    "base_trades",
    "base_expectancy",
    "base_total_R",
    "trades",
    "wins",
    "losses",
    "break_even_exits",
    "win_rate",
    "expectancy",
    "total_R",
    "trade_share",
    "expectancy_delta_vs_base",
    "total_R_share",
]
LEAVE_OUT_FIELDS = [
    "candidate_id",
    "leave_out_type",
    "left_out_value",
    "source_rank",
    "source_candidate_verdict",
    "rule_width",
    "rule_definition",
    "base_trades",
    "base_expectancy",
    "base_total_R",
    "remaining_trades",
    "remaining_wins",
    "remaining_losses",
    "remaining_break_even_exits",
    "remaining_win_rate",
    "remaining_expectancy",
    "remaining_total_R",
    "delta_expectancy",
    "delta_total_R",
    "expectancy_retention",
    "insufficient_after_leave_out",
]
EQUITY_FIELDS = [
    "candidate_id",
    "source_rank",
    "source_candidate_verdict",
    "rule_width",
    "rule_definition",
    "trades",
    "wins",
    "losses",
    "break_even_exits",
    "win_rate",
    "expectancy",
    "total_R",
    "gross_win_R",
    "gross_loss_R",
    "profit_factor",
    "ending_equity_R",
    "max_drawdown_R",
    "longest_losing_streak",
    "longest_non_win_streak",
    "best_trade_R",
    "worst_trade_R",
    "candidate_verdict",
]


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _load_target_rules(candidate_input: Path, target_ids: tuple[str, ...] = TARGET_CANDIDATE_IDS) -> list[CandidateRule]:
    with candidate_input.open("r", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rules: list[CandidateRule] = []
    for source_rank, row in enumerate(rows, start=1):
        candidate_id = row.get("candidate_id", "")
        if candidate_id not in target_ids:
            continue
        rule_definition = row["rule_definition"]
        rules.append(CandidateRule(
            candidate_id=candidate_id,
            source_rank=source_rank,
            source_candidate_verdict=row.get("candidate_verdict", ""),
            rule_width=int(row.get("rule_width") or 0),
            rule_definition=rule_definition,
            predicates=_parse_rule_definition(rule_definition),
            source_metrics=dict(row),
        ))
    found = {rule.candidate_id for rule in rules}
    missing = [candidate_id for candidate_id in target_ids if candidate_id not in found]
    if missing:
        raise ValueError(f"candidate_input is missing required survivor candidate id(s): {', '.join(missing)}")
    order = {candidate_id: index for index, candidate_id in enumerate(target_ids)}
    return sorted(rules, key=lambda rule: order[rule.candidate_id])


def _selected_outcomes(outcomes: list[SamplerOutcome], rule: CandidateRule) -> list[SamplerOutcome]:
    return [outcome for outcome in outcomes if _matches_rule(outcome, rule)]


def _trade_outcomes(outcomes: list[SamplerOutcome]) -> list[SamplerOutcome]:
    return [outcome for outcome in outcomes if outcome.realized_r is not None]


def _base_columns(rule: CandidateRule, base_metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": rule.candidate_id,
        "source_rank": rule.source_rank,
        "source_candidate_verdict": rule.source_candidate_verdict,
        "rule_width": rule.rule_width,
        "rule_definition": rule.rule_definition,
        "base_trades": base_metrics.get("trades", ""),
        "base_expectancy": base_metrics.get("expectancy", ""),
        "base_total_R": base_metrics.get("total_R", ""),
    }


def _breakdown_rows(rule: CandidateRule, selected: list[SamplerOutcome], dimension: str, label: str) -> list[dict[str, Any]]:
    base_metrics = _metric_counts(selected)
    base_trades = int(base_metrics.get("trades") or 0)
    base_expectancy = base_metrics.get("expectancy", "")
    base_total_r = _numeric(base_metrics.get("total_R"))
    values = sorted({row.segments.get(dimension, "UNKNOWN") for row in _trade_outcomes(selected) if row.segments.get(dimension, "UNKNOWN") != "UNKNOWN"})
    rows: list[dict[str, Any]] = []
    for value in values:
        members = [row for row in selected if row.segments.get(dimension) == value]
        metrics = _metric_counts(members)
        trades = int(metrics.get("trades") or 0)
        expectancy = metrics.get("expectancy", "")
        total_r = metrics.get("total_R", "")
        rows.append({
            **_base_columns(rule, base_metrics),
            "breakdown_type": label,
            "breakdown_value": value,
            "trades": trades,
            "wins": metrics.get("wins", ""),
            "losses": metrics.get("losses", ""),
            "break_even_exits": metrics.get("break_even_exits", ""),
            "win_rate": metrics.get("win_rate", ""),
            "expectancy": expectancy,
            "total_R": total_r,
            "trade_share": _round(trades / base_trades) if base_trades else "",
            "expectancy_delta_vs_base": "" if expectancy == "" or base_expectancy == "" else _round(_numeric(expectancy) - _numeric(base_expectancy)),
            "total_R_share": _round(_numeric(total_r) / base_total_r) if base_total_r else "",
        })
    return rows


def _leave_out_rows(
    rule: CandidateRule,
    selected: list[SamplerOutcome],
    dimension: str,
    label: str,
    min_removal_trades: int,
) -> list[dict[str, Any]]:
    base_metrics = _metric_counts(selected)
    base_expectancy = base_metrics.get("expectancy", "")
    base_total_r = base_metrics.get("total_R", "")
    values = sorted({row.segments.get(dimension, "UNKNOWN") for row in _trade_outcomes(selected) if row.segments.get(dimension, "UNKNOWN") != "UNKNOWN"})
    rows: list[dict[str, Any]] = []
    for value in values:
        remaining = [row for row in selected if row.segments.get(dimension) != value]
        metrics = _metric_counts(remaining)
        trades = int(metrics.get("trades") or 0)
        expectancy = metrics.get("expectancy", "")
        total_r = metrics.get("total_R", "")
        insufficient = trades < min_removal_trades
        rows.append({
            **_base_columns(rule, base_metrics),
            "leave_out_type": label,
            "left_out_value": value,
            "remaining_trades": trades,
            "remaining_wins": metrics.get("wins", ""),
            "remaining_losses": metrics.get("losses", ""),
            "remaining_break_even_exits": metrics.get("break_even_exits", ""),
            "remaining_win_rate": metrics.get("win_rate", ""),
            "remaining_expectancy": expectancy,
            "remaining_total_R": total_r,
            "delta_expectancy": "" if expectancy == "" or base_expectancy == "" else _round(_numeric(expectancy) - _numeric(base_expectancy)),
            "delta_total_R": "" if total_r == "" or base_total_r == "" else _round(_numeric(total_r) - _numeric(base_total_r)),
            "expectancy_retention": _retention(base_expectancy, expectancy, insufficient),
            "insufficient_after_leave_out": str(insufficient),
        })
    return rows


def _max_drawdown(realized: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in realized:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return _round(max_dd)


def _longest_streak(realized: list[float], predicate: Any) -> int:
    longest = 0
    current = 0
    for value in realized:
        if predicate(value):
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _equity_row(rule: CandidateRule, selected: list[SamplerOutcome], min_candidate_trades: int) -> dict[str, Any]:
    metrics = _metric_counts(selected)
    realized = [float(row.realized_r) for row in selected if row.realized_r is not None]
    gross_win = sum(value for value in realized if value > 0)
    gross_loss = sum(value for value in realized if value < 0)
    profit_factor = _round(gross_win / abs(gross_loss)) if gross_loss else ""
    score_metrics = {
        "worst_case_expectancy_retention": 1.0 if _numeric(metrics.get("expectancy")) > 0 else 0.0,
        "stability_score": 100.0 if _numeric(metrics.get("expectancy")) > 0 else 0.0,
        "symbol_sensitivity": 0.0,
        "quarter_sensitivity": 0.0,
        "year_sensitivity": 0.0,
    }
    return {
        "candidate_id": rule.candidate_id,
        "source_rank": rule.source_rank,
        "source_candidate_verdict": rule.source_candidate_verdict,
        "rule_width": rule.rule_width,
        "rule_definition": rule.rule_definition,
        "trades": metrics.get("trades", ""),
        "wins": metrics.get("wins", ""),
        "losses": metrics.get("losses", ""),
        "break_even_exits": metrics.get("break_even_exits", ""),
        "win_rate": metrics.get("win_rate", ""),
        "expectancy": metrics.get("expectancy", ""),
        "total_R": metrics.get("total_R", ""),
        "gross_win_R": _round(gross_win),
        "gross_loss_R": _round(gross_loss),
        "profit_factor": profit_factor,
        "ending_equity_R": _round(sum(realized)),
        "max_drawdown_R": _max_drawdown(realized),
        "longest_losing_streak": _longest_streak(realized, lambda value: value < 0),
        "longest_non_win_streak": _longest_streak(realized, lambda value: value <= 0),
        "best_trade_R": _round(max(realized)) if realized else "",
        "worst_trade_R": _round(min(realized)) if realized else "",
        "candidate_verdict": _verdict(metrics, score_metrics, min_candidate_trades),
    }


def _summary_verdict(leave_rows: list[dict[str, Any]], equity_row: dict[str, Any], min_candidate_trades: int) -> str:
    trades = int(equity_row.get("trades") or 0)
    expectancy = _numeric(equity_row.get("expectancy"))
    if trades < min_candidate_trades or expectancy <= 0.0:
        return "INSUFFICIENT_DATA"
    retentions = [_numeric(row.get("expectancy_retention")) for row in leave_rows]
    if retentions and min(retentions) <= 0.0:
        return "HIGHLY_UNSTABLE"
    if any(row.get("leave_out_type") == "SYMBOL" and abs(_numeric(row.get("delta_expectancy"))) > 0.65 * abs(expectancy) for row in leave_rows):
        return "SYMBOL_DEPENDENT"
    if any(row.get("leave_out_type") in {"QUARTER", "YEAR"} and abs(_numeric(row.get("delta_expectancy"))) > 0.65 * abs(expectancy) for row in leave_rows):
        return "TIME_DEPENDENT"
    return "ROBUST_BUT_CONCENTRATED"


def _write_summary(
    path: Path,
    rules: list[CandidateRule],
    equity_rows: list[dict[str, Any]],
    leave_rows: list[dict[str, Any]],
    universe: dict[str, Any],
    candidate_input: Path,
    min_candidate_trades: int,
) -> None:
    verdict_by_candidate = {
        row["candidate_id"]: _summary_verdict([leave for leave in leave_rows if leave["candidate_id"] == row["candidate_id"]], row, min_candidate_trades)
        for row in equity_rows
    }
    counts = Counter(verdict_by_candidate.values())
    with path.open("x") as handle:
        handle.write("# P+2 focused survivor audit: CAND-000053 and CAND-000065\n\n")
        handle.write("Research only. No GA, optimization, live trader, detector, production strategy, exchange code, schema change, or candidate promotion is included.\n\n")
        handle.write(f"- `candidate_input`: {candidate_input}\n")
        handle.write(f"- `audited_candidate_ids`: {', '.join(rule.candidate_id for rule in rules)}\n")
        handle.write(f"- `universe_status`: {universe['status']}\n")
        handle.write("\n## Survivor verdict counts\n\n")
        for verdict in ALLOWED_VERDICTS:
            handle.write(f"- `{verdict}`: {counts.get(verdict, 0)}\n")
        handle.write("\n## Survivor equity metrics\n\n")
        handle.write("| candidate_id | verdict | trades | expectancy | total_R | profit_factor | max_drawdown_R | rule |\n")
        handle.write("|---|---|---:|---:|---:|---:|---:|---|\n")
        for row in equity_rows:
            handle.write(
                f"| {row['candidate_id']} | {verdict_by_candidate[row['candidate_id']]} | {row['trades']} | {row['expectancy']} | "
                f"{row['total_R']} | {row['profit_factor']} | {row['max_drawdown_R']} | {row['rule_definition']} |\n"
            )
        handle.write("\n## Output files\n\n")
        for name in OUTPUT_NAMES:
            handle.write(f"- `{name}`\n")


def run(
    candidate_input: Path,
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    output_root: Path,
    candle_symbols: dict[str, str] | None = None,
    *,
    limit_rows_per_symbol: int | None = None,
    require_full_universe: bool = True,
    min_candidate_trades: int = DEFAULT_MIN_CANDIDATE_TRADES,
    min_removal_trades: int = DEFAULT_MIN_REMOVAL_TRADES,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing P+2 survivor audit output(s): {', '.join(existing)}")

    symbols, segmented, candles_by_symbol, load_flags = load_segmented_observations(
        symbol_inputs,
        columns_inputs,
        candles_inputs,
        candle_symbols or {},
        limit_rows_per_symbol=limit_rows_per_symbol,
    )
    _validate_full_universe(symbols, require_full_universe)
    outcomes = _build_outcomes(segmented, candles_by_symbol)
    universe = _universe_consistency(_metric_counts(outcomes))
    rules = _load_target_rules(candidate_input)

    symbol_rows: list[dict[str, Any]] = []
    quarter_rows: list[dict[str, Any]] = []
    year_rows: list[dict[str, Any]] = []
    leave_symbol_rows: list[dict[str, Any]] = []
    leave_quarter_rows: list[dict[str, Any]] = []
    leave_year_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []

    for rule in rules:
        selected = _selected_outcomes(outcomes, rule)
        symbol_rows.extend(_breakdown_rows(rule, selected, "symbol", "SYMBOL"))
        quarter_rows.extend(_breakdown_rows(rule, selected, "quarter", "QUARTER"))
        year_rows.extend(_breakdown_rows(rule, selected, "year", "YEAR"))
        leave_symbol_rows.extend(_leave_out_rows(rule, selected, "symbol", "SYMBOL", min_removal_trades))
        leave_quarter_rows.extend(_leave_out_rows(rule, selected, "quarter", "QUARTER", min_removal_trades))
        leave_year_rows.extend(_leave_out_rows(rule, selected, "year", "YEAR", min_removal_trades))
        equity_rows.append(_equity_row(rule, selected, min_candidate_trades))

    all_leave_rows = [*leave_symbol_rows, *leave_quarter_rows, *leave_year_rows]
    _write_summary(output_root / OUTPUT_NAMES[0], rules, equity_rows, all_leave_rows, universe, candidate_input, min_candidate_trades)
    _write_csv(output_root / OUTPUT_NAMES[1], BREAKDOWN_FIELDS, symbol_rows)
    _write_csv(output_root / OUTPUT_NAMES[2], BREAKDOWN_FIELDS, quarter_rows)
    _write_csv(output_root / OUTPUT_NAMES[3], BREAKDOWN_FIELDS, year_rows)
    _write_csv(output_root / OUTPUT_NAMES[4], LEAVE_OUT_FIELDS, leave_symbol_rows)
    _write_csv(output_root / OUTPUT_NAMES[5], LEAVE_OUT_FIELDS, leave_quarter_rows)
    _write_csv(output_root / OUTPUT_NAMES[6], LEAVE_OUT_FIELDS, leave_year_rows)
    _write_csv(output_root / OUTPUT_NAMES[7], EQUITY_FIELDS, equity_rows)

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_p2_survivor_audit",
        "research_only": True,
        "scope": "focused_survivor_audit_only",
        "candidate_ids": list(TARGET_CANDIDATE_IDS),
        "production_modifications": False,
        "live_trader_modifications": False,
        "detector_modifications": False,
        "exchange_code_modifications": False,
        "strategy_promotion": False,
        "genetic_algorithm": False,
        "optimization": False,
        "machine_learning": False,
        "motif": MOTIF_NAME,
        "required_symbols": list(EXPECTED_SYMBOLS),
        "full_seven_market_universe": set(symbols) == set(EXPECTED_SYMBOLS),
        "symbols": symbols,
        "candidate_input": str(candidate_input),
        "min_candidate_trades": min_candidate_trades,
        "min_removal_trades": min_removal_trades,
        "baseline_reference": {"trades": BASELINE_P2_TRADES, "expectancy": BASELINE_P2_EXPECTANCY, "total_R": BASELINE_P2_TOTAL_R},
        "universe_consistency": universe,
        "audit_dimensions": ["symbol", "quarter", "year"],
        "artifacts": list(OUTPUT_NAMES[:-1]),
        "load_flags": load_flags,
    }
    with (output_root / OUTPUT_NAMES[8]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only focused survivor audit for CAND-000053 and CAND-000065")
    parser.add_argument("--candidate-input", required=True, type=Path, help="p2_candidate_sampler_top100.csv or p2_candidate_sampler_results.csv containing CAND-000053 and CAND-000065")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--limit-rows-per-symbol", type=int, help="optional small-sample diagnostic cap after P+2 motif filtering")
    parser.add_argument("--allow-partial-universe", action="store_true", help="allow test/diagnostic runs outside the full BTC/ETH/SOL/ENA/HYPE/SUI/TAO universe")
    parser.add_argument("--min-candidate-trades", type=int, default=DEFAULT_MIN_CANDIDATE_TRADES)
    parser.add_argument("--min-removal-trades", type=int, default=DEFAULT_MIN_REMOVAL_TRADES)
    args = parser.parse_args()
    try:
        run(
            args.candidate_input,
            dict(args.symbol_input),
            dict(args.columns_input),
            dict(args.candles_input),
            args.output_root,
            dict(args.candle_symbol),
            limit_rows_per_symbol=args.limit_rows_per_symbol,
            require_full_universe=not args.allow_partial_universe,
            min_candidate_trades=args.min_candidate_trades,
            min_removal_trades=args.min_removal_trades,
        )
    except (FileExistsError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
