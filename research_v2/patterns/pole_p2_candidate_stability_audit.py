"""Research-only stability audit for deterministic P+2 candidate populations.

This module replays already-sampled causal P+2 candidate definitions against the
validated reconstruction and audits whether the candidate edge survives removing
single symbols, calendar periods, and market/exchange families. It deliberately
performs no genetic algorithm, optimization, live trader change, detector change,
strategy change, schema change, or candidate promotion.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import _parse_symbol_input, _round
from research_v2.patterns.pole_p2_candidate_sampler import SamplerOutcome, _build_outcomes, _metric_counts, _universe_consistency
from research_v2.patterns.pole_p2_causal_motif_audit import EXPECTED_SYMBOLS, MOTIF_NAME, _validate_full_universe
from research_v2.patterns.pole_p2_edge_segmentation_audit import BASELINE_P2_EXPECTANCY, BASELINE_P2_TOTAL_R, BASELINE_P2_TRADES, load_segmented_observations

DEFAULT_TOP_N = 20
DEFAULT_MIN_CANDIDATE_TRADES = 30
DEFAULT_MIN_REMOVAL_TRADES = 10
SYMBOL_REMOVALS = ("BTC", "ETH", "SOL", "ENA", "HYPE", "SUI", "TAO")
MARKET_REMOVALS = ("BINANCE", "MEXC")
ALLOWED_VERDICTS = (
    "ROBUST_ACROSS_SYMBOLS_AND_TIME",
    "ROBUST_BUT_CONCENTRATED",
    "SYMBOL_DEPENDENT",
    "TIME_DEPENDENT",
    "HIGHLY_UNSTABLE",
    "INSUFFICIENT_DATA",
)
OUTPUT_NAMES = (
    "p2_candidate_stability_summary.md",
    "p2_candidate_stability_results.csv",
    "p2_candidate_stability_symbol_removal.csv",
    "p2_candidate_stability_quarter_removal.csv",
    "p2_candidate_stability_year_removal.csv",
    "p2_candidate_stability_market_removal.csv",
    "p2_candidate_stability_manifest.json",
)
SUMMARY_FIELDS = [
    "stability_rank",
    "candidate_id",
    "source_rank",
    "source_candidate_verdict",
    "rule_width",
    "rule_definition",
    "base_trades",
    "base_expectancy",
    "base_total_R",
    "dominant_symbol",
    "dominant_symbol_delta_expectancy",
    "dominant_quarter",
    "dominant_quarter_delta_expectancy",
    "dominant_year",
    "dominant_year_delta_expectancy",
    "average_expectancy_retention",
    "worst_case_expectancy_retention",
    "symbol_sensitivity",
    "quarter_sensitivity",
    "year_sensitivity",
    "market_sensitivity",
    "stability_score",
    "candidate_verdict",
]
REMOVAL_FIELDS = [
    "candidate_id",
    "removal_type",
    "removed_value",
    "base_trades",
    "base_expectancy",
    "base_total_R",
    "trades",
    "expectancy",
    "total_R",
    "delta_expectancy",
    "delta_total_R",
    "expectancy_retention",
    "insufficient_after_removal",
]
_RULE_PART_RE = re.compile(r"^(?P<label>.+)\((?P<dimension>[^()]+) in \[(?P<values>.*)\]\)$")


@dataclass(frozen=True)
class CandidateRule:
    candidate_id: str
    source_rank: int
    source_candidate_verdict: str
    rule_width: int
    rule_definition: str
    predicates: tuple[tuple[str, frozenset[str]], ...]
    source_metrics: dict[str, Any]


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _parse_rule_definition(rule_definition: str) -> tuple[tuple[str, frozenset[str]], ...]:
    predicates: list[tuple[str, frozenset[str]]] = []
    for part in rule_definition.split(" + "):
        match = _RULE_PART_RE.match(part.strip())
        if not match:
            raise ValueError(f"cannot parse candidate rule part: {part!r}")
        dimension = match.group("dimension").strip()
        values = frozenset(value.strip() for value in match.group("values").split(",") if value.strip())
        if not dimension or not values:
            raise ValueError(f"candidate rule part has empty dimension or values: {part!r}")
        predicates.append((dimension, values))
    return tuple(predicates)


def _load_candidate_rules(candidate_input: Path, top_n: int, representative_only: bool) -> list[CandidateRule]:
    with candidate_input.open("r", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if representative_only:
        rows = [row for row in rows if row.get("is_cluster_representative", "True") == "True"]
    rules: list[CandidateRule] = []
    for source_rank, row in enumerate(rows[:top_n], start=1):
        rule_definition = row["rule_definition"]
        rules.append(CandidateRule(
            candidate_id=row.get("candidate_id") or f"SOURCE-{source_rank:06d}",
            source_rank=source_rank,
            source_candidate_verdict=row.get("candidate_verdict", ""),
            rule_width=int(row.get("rule_width") or 0),
            rule_definition=rule_definition,
            predicates=_parse_rule_definition(rule_definition),
            source_metrics=dict(row),
        ))
    return rules


def _matches_rule(outcome: SamplerOutcome, rule: CandidateRule) -> bool:
    return all(outcome.segments.get(dimension, "UNKNOWN") in values for dimension, values in rule.predicates)


def _selected_outcomes(outcomes: list[SamplerOutcome], rule: CandidateRule) -> list[SamplerOutcome]:
    return [outcome for outcome in outcomes if _matches_rule(outcome, rule)]


def _numeric(value: Any, default: float = 0.0) -> float:
    if value == "" or value is None:
        return default
    return float(value)


def _retention(base_expectancy: Any, after_expectancy: Any, insufficient: bool) -> float:
    base = _numeric(base_expectancy)
    after = _numeric(after_expectancy)
    if insufficient or base <= 0.0:
        return 0.0
    return _round(after / base)


def _removal_row(
    candidate_id: str,
    removal_type: str,
    removed_value: str,
    base_metrics: dict[str, Any],
    remaining: list[SamplerOutcome],
    min_removal_trades: int,
) -> dict[str, Any]:
    metrics = _metric_counts(remaining)
    insufficient = int(metrics.get("trades") or 0) < min_removal_trades
    base_expectancy = base_metrics.get("expectancy", "")
    base_total_r = base_metrics.get("total_R", "")
    expectancy = metrics.get("expectancy", "")
    total_r = metrics.get("total_R", "")
    return {
        "candidate_id": candidate_id,
        "removal_type": removal_type,
        "removed_value": removed_value,
        "base_trades": base_metrics.get("trades", ""),
        "base_expectancy": base_expectancy,
        "base_total_R": base_total_r,
        "trades": metrics.get("trades", ""),
        "expectancy": expectancy,
        "total_R": total_r,
        "delta_expectancy": "" if expectancy == "" or base_expectancy == "" else _round(_numeric(expectancy) - _numeric(base_expectancy)),
        "delta_total_R": "" if total_r == "" or base_total_r == "" else _round(_numeric(total_r) - _numeric(base_total_r)),
        "expectancy_retention": _retention(base_expectancy, expectancy, insufficient),
        "insufficient_after_removal": str(insufficient),
    }


def _dominant(rows: list[dict[str, Any]]) -> tuple[str, float]:
    if not rows:
        return "", 0.0
    ranked = sorted(rows, key=lambda row: (_numeric(row.get("delta_expectancy")), _numeric(row.get("delta_total_R"))))
    row = ranked[0]
    return str(row["removed_value"]), _numeric(row.get("delta_expectancy"))


def _sensitivity(rows: list[dict[str, Any]], base_expectancy: Any) -> float:
    base = abs(_numeric(base_expectancy))
    if base <= 0.0 or not rows:
        return 1.0
    worst_abs_delta = max(abs(_numeric(row.get("delta_expectancy"))) for row in rows)
    return _round(worst_abs_delta / base)


def _stability_score(
    symbol_rows: list[dict[str, Any]],
    quarter_rows: list[dict[str, Any]],
    year_rows: list[dict[str, Any]],
    market_rows: list[dict[str, Any]],
    base_expectancy: Any,
) -> dict[str, float]:
    all_rows = [*symbol_rows, *quarter_rows, *year_rows]
    retentions = [_numeric(row.get("expectancy_retention")) for row in all_rows]
    average_retention = _round(sum(retentions) / len(retentions)) if retentions else 0.0
    worst_retention = _round(min(retentions)) if retentions else 0.0
    symbol_sensitivity = _sensitivity(symbol_rows, base_expectancy)
    quarter_sensitivity = _sensitivity(quarter_rows, base_expectancy)
    year_sensitivity = _sensitivity(year_rows, base_expectancy)
    market_sensitivity = _sensitivity(market_rows, base_expectancy) if market_rows else 0.0
    score = 100.0 * (
        0.35 * max(min(average_retention, 1.25), -1.0) / 1.25
        + 0.25 * max(min(worst_retention, 1.0), -1.0)
        + 0.15 * (1.0 - min(symbol_sensitivity, 1.0))
        + 0.15 * (1.0 - min(quarter_sensitivity, 1.0))
        + 0.10 * (1.0 - min(year_sensitivity, 1.0))
    )
    return {
        "average_expectancy_retention": average_retention,
        "worst_case_expectancy_retention": worst_retention,
        "symbol_sensitivity": symbol_sensitivity,
        "quarter_sensitivity": quarter_sensitivity,
        "year_sensitivity": year_sensitivity,
        "market_sensitivity": market_sensitivity,
        "stability_score": _round(max(0.0, min(score, 100.0))),
    }


def _verdict(base_metrics: dict[str, Any], score_metrics: dict[str, float], min_candidate_trades: int) -> str:
    trades = int(base_metrics.get("trades") or 0)
    expectancy = _numeric(base_metrics.get("expectancy"))
    if trades < min_candidate_trades or expectancy <= 0.0:
        return "INSUFFICIENT_DATA"
    if score_metrics["worst_case_expectancy_retention"] <= 0.0 or score_metrics["stability_score"] < 35.0:
        return "HIGHLY_UNSTABLE"
    if score_metrics["symbol_sensitivity"] > 0.65:
        return "SYMBOL_DEPENDENT"
    if score_metrics["quarter_sensitivity"] > 0.65 or score_metrics["year_sensitivity"] > 0.65:
        return "TIME_DEPENDENT"
    if score_metrics["stability_score"] >= 70.0 and score_metrics["worst_case_expectancy_retention"] >= 0.50:
        return "ROBUST_ACROSS_SYMBOLS_AND_TIME"
    return "ROBUST_BUT_CONCENTRATED"


def _rank_key(row: dict[str, Any]) -> tuple[float, float, float]:
    return (_numeric(row["stability_score"]), _numeric(row["base_expectancy"]), _numeric(row["base_total_R"]))


def _write_summary(path: Path, summary_rows: list[dict[str, Any]], universe: dict[str, Any], candidate_input: Path, top_n: int) -> None:
    counts = Counter(row["candidate_verdict"] for row in summary_rows)
    with path.open("x") as handle:
        handle.write("# P+2 candidate stability audit\n\n")
        handle.write("Research only. No GA, optimization, live trader, detector, production strategy, exchange code, schema change, or candidate promotion is included.\n\n")
        handle.write(f"- `candidate_input`: {candidate_input}\n")
        handle.write(f"- `configured_top_n`: {top_n}\n")
        handle.write(f"- `audited_candidates`: {len(summary_rows)}\n")
        handle.write(f"- `universe_status`: {universe['status']}\n")
        handle.write("\n## Deterministic stability score\n\n")
        handle.write("The stability score is deterministic and combines average expectancy retention, worst-case expectancy retention, symbol sensitivity, quarter sensitivity, and year sensitivity. It is not optimized or trained.\n\n")
        handle.write("```text\nstability_score = 100 * (0.35*clamped_avg_retention/1.25 + 0.25*clamped_worst_retention + 0.15*(1-symbol_sensitivity) + 0.15*(1-quarter_sensitivity) + 0.10*(1-year_sensitivity))\n```\n\n")
        handle.write("Candidates are ranked by stability score, then expectancy, then total_R.\n\n")
        handle.write("## Verdict counts\n\n")
        for verdict in ALLOWED_VERDICTS:
            handle.write(f"- `{verdict}`: {counts.get(verdict, 0)}\n")
        handle.write("\n## Stability-ranked candidates\n\n")
        handle.write("| rank | candidate_id | verdict | trades | expectancy | total_R | stability_score | dominant_symbol | dominant_quarter | dominant_year | rule |\n")
        handle.write("|---:|---|---|---:|---:|---:|---:|---|---|---|---|\n")
        for row in summary_rows[:20]:
            handle.write(
                f"| {row['stability_rank']} | {row['candidate_id']} | {row['candidate_verdict']} | {row['base_trades']} | "
                f"{row['base_expectancy']} | {row['base_total_R']} | {row['stability_score']} | {row['dominant_symbol']} | "
                f"{row['dominant_quarter']} | {row['dominant_year']} | {row['rule_definition']} |\n"
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
    top_n: int = DEFAULT_TOP_N,
    representative_only: bool = True,
    limit_rows_per_symbol: int | None = None,
    require_full_universe: bool = True,
    min_candidate_trades: int = DEFAULT_MIN_CANDIDATE_TRADES,
    min_removal_trades: int = DEFAULT_MIN_REMOVAL_TRADES,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing P+2 candidate stability output(s): {', '.join(existing)}")

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
    candidate_rules = _load_candidate_rules(candidate_input, top_n, representative_only)

    summary_rows: list[dict[str, Any]] = []
    symbol_rows_all: list[dict[str, Any]] = []
    quarter_rows_all: list[dict[str, Any]] = []
    year_rows_all: list[dict[str, Any]] = []
    market_rows_all: list[dict[str, Any]] = []

    for rule in candidate_rules:
        selected = _selected_outcomes(outcomes, rule)
        base_metrics = _metric_counts(selected)
        present_symbols = sorted({outcome.segments.get("symbol", "UNKNOWN") for outcome in selected if outcome.realized_r is not None})
        present_quarters = sorted({outcome.segments.get("quarter", "UNKNOWN") for outcome in selected if outcome.realized_r is not None and outcome.segments.get("quarter") != "UNKNOWN"})
        present_years = sorted({outcome.segments.get("year", "UNKNOWN") for outcome in selected if outcome.realized_r is not None and outcome.segments.get("year") != "UNKNOWN"})
        present_markets = sorted({outcome.segments.get("exchange", "UNKNOWN") for outcome in selected if outcome.realized_r is not None})

        symbol_rows = [
            _removal_row(rule.candidate_id, "SYMBOL", symbol, base_metrics, [row for row in selected if row.segments.get("symbol") != symbol], min_removal_trades)
            for symbol in SYMBOL_REMOVALS
            if symbol in present_symbols
        ]
        quarter_rows = [
            _removal_row(rule.candidate_id, "QUARTER", quarter, base_metrics, [row for row in selected if row.segments.get("quarter") != quarter], min_removal_trades)
            for quarter in present_quarters
        ]
        year_rows = [
            _removal_row(rule.candidate_id, "YEAR", year, base_metrics, [row for row in selected if row.segments.get("year") != year], min_removal_trades)
            for year in present_years
        ]
        market_rows = [
            _removal_row(rule.candidate_id, "MARKET", market, base_metrics, [row for row in selected if row.segments.get("exchange") != market], min_removal_trades)
            for market in MARKET_REMOVALS
            if market in present_markets
        ]
        symbol_rows_all.extend(symbol_rows)
        quarter_rows_all.extend(quarter_rows)
        year_rows_all.extend(year_rows)
        market_rows_all.extend(market_rows)

        score_metrics = _stability_score(symbol_rows, quarter_rows, year_rows, market_rows, base_metrics.get("expectancy", ""))
        dominant_symbol, dominant_symbol_delta = _dominant(symbol_rows)
        dominant_quarter, dominant_quarter_delta = _dominant(quarter_rows)
        dominant_year, dominant_year_delta = _dominant(year_rows)
        summary_rows.append({
            "stability_rank": 0,
            "candidate_id": rule.candidate_id,
            "source_rank": rule.source_rank,
            "source_candidate_verdict": rule.source_candidate_verdict,
            "rule_width": rule.rule_width,
            "rule_definition": rule.rule_definition,
            "base_trades": base_metrics.get("trades", ""),
            "base_expectancy": base_metrics.get("expectancy", ""),
            "base_total_R": base_metrics.get("total_R", ""),
            "dominant_symbol": dominant_symbol,
            "dominant_symbol_delta_expectancy": dominant_symbol_delta,
            "dominant_quarter": dominant_quarter,
            "dominant_quarter_delta_expectancy": dominant_quarter_delta,
            "dominant_year": dominant_year,
            "dominant_year_delta_expectancy": dominant_year_delta,
            **score_metrics,
            "candidate_verdict": _verdict(base_metrics, score_metrics, min_candidate_trades),
        })

    summary_rows = sorted(summary_rows, key=_rank_key, reverse=True)
    for rank, row in enumerate(summary_rows, start=1):
        row["stability_rank"] = rank

    _write_csv(output_root / OUTPUT_NAMES[1], SUMMARY_FIELDS, summary_rows)
    _write_csv(output_root / OUTPUT_NAMES[2], REMOVAL_FIELDS, symbol_rows_all)
    _write_csv(output_root / OUTPUT_NAMES[3], REMOVAL_FIELDS, quarter_rows_all)
    _write_csv(output_root / OUTPUT_NAMES[4], REMOVAL_FIELDS, year_rows_all)
    _write_csv(output_root / OUTPUT_NAMES[5], REMOVAL_FIELDS, market_rows_all)
    _write_summary(output_root / OUTPUT_NAMES[0], summary_rows, universe, candidate_input, top_n)

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_p2_candidate_stability_audit",
        "research_only": True,
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
        "top_n": top_n,
        "representative_only": representative_only,
        "min_candidate_trades": min_candidate_trades,
        "min_removal_trades": min_removal_trades,
        "baseline_reference": {"trades": BASELINE_P2_TRADES, "expectancy": BASELINE_P2_EXPECTANCY, "total_R": BASELINE_P2_TOTAL_R},
        "universe_consistency": universe,
        "ranking_logic": ["stability_score", "expectancy", "total_R"],
        "stability_score_inputs": [
            "average_expectancy_retention",
            "worst_case_expectancy_retention",
            "symbol_sensitivity",
            "quarter_sensitivity",
            "year_sensitivity",
        ],
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "load_flags": load_flags,
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[6]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only stability audit for deterministic causal P+2 candidates")
    parser.add_argument("--candidate-input", required=True, type=Path, help="p2_candidate_sampler_top100.csv or p2_candidate_sampler_results.csv")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N, help="number of ranked candidate rows to audit; default: 20")
    parser.add_argument("--include-suppressed", action="store_true", help="include suppressed/non-representative rows when candidate-input is the full sampler results file")
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
            top_n=args.top_n,
            representative_only=not args.include_suppressed,
            limit_rows_per_symbol=args.limit_rows_per_symbol,
            require_full_universe=not args.allow_partial_universe,
            min_candidate_trades=args.min_candidate_trades,
            min_removal_trades=args.min_removal_trades,
        )
    except FileExistsError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
