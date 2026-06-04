"""Phase 1 deterministic candidate sampler for the causal P+2 pole universe.

Research-only tool. This module enumerates controlled 1-way, 2-way, and 3-way
causal feature intersections over the validated P+2 pole/reversal/confirmation
universe. It deliberately does not implement a genetic algorithm, evolutionary
search, mutation, crossover, live trading, detector changes, or strategy changes.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_be_research_audit import _be_classify
from research_v2.patterns.pole_core_motif_execution_reality_audit import _build_opportunities
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import _parse_symbol_input, _round
from research_v2.patterns.pole_p2_causal_motif_audit import (
    BREAK_EVEN_TRIGGER_R,
    EXPECTED_SYMBOLS,
    MOTIF_NAME,
    TARGET_R,
    _validate_full_universe,
)
from research_v2.patterns.pole_p2_edge_segmentation_audit import (
    BASELINE_P2_EXPECTANCY,
    BASELINE_P2_TOTAL_R,
    BASELINE_P2_TRADES,
    SegmentedObservation,
    _segment_map,
    load_segmented_observations,
)

BASELINE_P2_OBSERVATIONS = 4042
BASELINE_P2_WINS = 1143
BASELINE_P2_LOSSES = 2606
BASELINE_P2_BREAK_EVEN_EXITS = 274
UNIVERSE_EXPECTANCY_TOLERANCE = 0.000001
UNIVERSE_TOTAL_R_TOLERANCE = 0.000001
DEFAULT_MIN_TRADES = 100
DEFAULT_MIN_WINS = 25
MAX_RULE_WIDTH = 3
TOP_N = 100
DEFAULT_MAX_CANDIDATES_TO_CLUSTER = 5000
JACCARD_DUPLICATE_THRESHOLD = 0.85
ALLOWED_VERDICTS = (
    "ROBUST_SUBPOPULATION_FOUND",
    "PROMISING_SUBPOPULATIONS_FOUND",
    "EDGE_CONCENTRATED_BUT_UNSTABLE",
    "NO_ROBUST_SUBPOPULATION",
    "INSUFFICIENT_DATA",
)
OUTPUT_NAMES = (
    "p2_candidate_sampler_results.csv",
    "p2_candidate_sampler_top100.csv",
    "p2_candidate_sampler_summary.md",
    "p2_candidate_sampler_manifest.json",
)
RESULT_FIELDS = [
    "candidate_id",
    "candidate_verdict",
    "cluster_id",
    "is_cluster_representative",
    "suppressed_by_candidate_id",
    "novelty_jaccard_to_cluster_representative",
    "rule_width",
    "rule_definition",
    "rule_fingerprint",
    "observations",
    "unique_opportunities",
    "trades",
    "wins",
    "losses",
    "break_even_exits",
    "win_rate",
    "expectancy",
    "total_R",
    "quality_score",
    "symbol_hhi",
    "top_symbol",
    "top_symbol_trade_share",
    "year_hhi",
    "top_year",
    "top_year_trade_share",
    "quarter_hhi",
    "top_quarter",
    "top_quarter_trade_share",
]


@dataclass(frozen=True)
class SamplerOutcome:
    outcome_id: str
    observation_count: int
    symbol: str
    direction: str
    classification: str
    realized_r: float | None
    segments: dict[str, str]


@dataclass(frozen=True)
class Predicate:
    predicate_id: str
    dimension: str
    label: str
    values: frozenset[str]

    def matches(self, outcome: SamplerOutcome) -> bool:
        return outcome.segments.get(self.dimension, "UNKNOWN") in self.values

    def rule_part(self) -> str:
        values = ",".join(sorted(self.values))
        return f"{self.label}({self.dimension} in [{values}])"


@dataclass(frozen=True)
class Candidate:
    predicates: tuple[Predicate, ...]
    selected_ids: frozenset[str]
    metrics: dict[str, Any]


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _ts_to_parts(ts: int | None) -> tuple[str, str, str]:
    if ts is None:
        return "UNKNOWN", "UNKNOWN", "UNKNOWN"
    dt = datetime.fromtimestamp(ts / 1000 if ts > 10_000_000_000 else ts, tz=UTC)
    return str(dt.year), f"{dt.year}-Q{((dt.month - 1) // 3) + 1}", f"{dt.year}-{dt.month:02d}"


def _market_family_predicates(symbols: list[str]) -> list[Predicate]:
    symbol_set = set(symbols)
    groups = {
        "MAJORS_BTC_ETH_SOL": {"BTC", "ETH", "SOL"},
        "SEGMENTATION_HINT_ENA_HYPE_TAO": {"ENA", "HYPE", "TAO"},
        "ALT_EX_BTC_ETH_SOL": {"ENA", "HYPE", "SUI", "TAO"},
    }
    predicates: list[Predicate] = []
    for label, values in groups.items():
        usable = frozenset(value for value in values if value in symbol_set)
        if usable:
            predicates.append(Predicate(f"symbol_group={label}", "symbol", label, usable))
    return predicates


def _build_outcomes(segmented: list[SegmentedObservation], candles_by_symbol: dict[str, list[Any]]) -> list[SamplerOutcome]:
    by_key = {(row.observation.symbol, row.observation.row_number): row for row in segmented}
    opportunities = _build_opportunities([row.observation for row in segmented])
    outcomes: list[SamplerOutcome] = []
    for opportunity in opportunities:
        rep = opportunity.representative
        segment_row = by_key[(rep.symbol, rep.row_number)]
        classification, realized_r, _ts, _details = _be_classify(rep, candles_by_symbol[rep.symbol], BREAK_EVEN_TRIGGER_R)
        segments = dict(_segment_map(segment_row))
        year, quarter, month = _ts_to_parts(rep.observable_entry_ts)
        segments.update({"year": year, "quarter": quarter, "month": month})
        outcomes.append(SamplerOutcome(
            outcome_id=opportunity.opportunity_id,
            observation_count=len(opportunity.observations),
            symbol=rep.symbol,
            direction=rep.direction,
            classification=classification,
            realized_r=realized_r,
            segments=segments,
        ))
    return outcomes


def _metric_counts(outcomes: Iterable[SamplerOutcome]) -> dict[str, Any]:
    rows = list(outcomes)
    counts = Counter(row.classification for row in rows)
    wins = counts["TARGET_FIRST"]
    losses = counts["STOP_FIRST"]
    be_exits = counts["BREAK_EVEN_EXIT"]
    trades = wins + losses + be_exits
    total_r = sum(row.realized_r for row in rows if row.realized_r is not None)
    return {
        "observations": sum(row.observation_count for row in rows),
        "unique_opportunities": len(rows),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "break_even_exits": be_exits,
        "win_rate": _round(wins / (wins + losses)) if wins + losses else "",
        "expectancy": _round(total_r / trades) if trades else "",
        "total_R": _round(total_r),
    }


def _hhi(rows: list[SamplerOutcome], dimension: str) -> tuple[float | str, str, float | str]:
    trade_rows = [row for row in rows if row.realized_r is not None]
    if not trade_rows:
        return "", "", ""
    counts = Counter(row.segments.get(dimension, "UNKNOWN") for row in trade_rows)
    total = sum(counts.values())
    shares = {key: value / total for key, value in counts.items()}
    top_key, top_share = max(shares.items(), key=lambda item: (item[1], item[0]))
    return _round(sum(share * share for share in shares.values())), top_key, _round(top_share)


def _quality_score(metrics: dict[str, Any]) -> float | str:
    expectancy = metrics.get("expectancy")
    trades = int(metrics.get("trades") or 0)
    total_r = float(metrics.get("total_R") or 0.0)
    if expectancy == "" or trades <= 0:
        return ""
    symbol_hhi = float(metrics.get("symbol_hhi") or 0.0)
    year_hhi = float(metrics.get("year_hhi") or 0.0)
    quarter_hhi = float(metrics.get("quarter_hhi") or 0.0)
    trade_count_score = min(math.log10(max(trades, 1)) / math.log10(BASELINE_P2_TRADES), 1.0)
    total_r_score = max(min(total_r / BASELINE_P2_TOTAL_R, 2.0), -2.0)
    concentration_penalty = (
        0.20 * max(0.0, symbol_hhi - 0.45)
        + 0.10 * max(0.0, year_hhi - 0.40)
        + 0.10 * max(0.0, quarter_hhi - 0.30)
    )
    score = float(expectancy) + 0.05 * trade_count_score + 0.10 * total_r_score - concentration_penalty
    return _round(score)


def _candidate_verdict(metrics: dict[str, Any], min_trades: int, min_wins: int) -> str:
    trades = int(metrics.get("trades") or 0)
    wins = int(metrics.get("wins") or 0)
    expectancy = metrics.get("expectancy")
    total_r = float(metrics.get("total_R") or 0.0)
    top_symbol_share = float(metrics.get("top_symbol_trade_share") or 0.0)
    top_year_share = float(metrics.get("top_year_trade_share") or 0.0)
    top_quarter_share = float(metrics.get("top_quarter_trade_share") or 0.0)
    if trades < min_trades or wins < min_wins or expectancy == "":
        return "INSUFFICIENT_DATA"
    if float(expectancy) <= BASELINE_P2_EXPECTANCY or total_r <= 0:
        return "NO_ROBUST_SUBPOPULATION"
    if (
        trades >= max(200, min_trades)
        and wins >= max(50, min_wins)
        and float(expectancy) >= BASELINE_P2_EXPECTANCY + 0.05
        and top_symbol_share <= 0.65
        and top_year_share <= 0.60
        and top_quarter_share <= 0.50
    ):
        return "ROBUST_SUBPOPULATION_FOUND"
    if top_symbol_share > 0.65 or top_year_share > 0.60 or top_quarter_share > 0.50:
        return "EDGE_CONCENTRATED_BUT_UNSTABLE"
    return "PROMISING_SUBPOPULATIONS_FOUND"


def _rule_definition(predicates: tuple[Predicate, ...]) -> str:
    return " + ".join(predicate.rule_part() for predicate in predicates)


def _rule_fingerprint(predicates: tuple[Predicate, ...]) -> str:
    canonical = "|".join(predicate.predicate_id for predicate in predicates)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _evaluate_predicates(predicates: tuple[Predicate, ...], outcomes: list[SamplerOutcome]) -> Candidate:
    selected = [row for row in outcomes if all(predicate.matches(row) for predicate in predicates)]
    metrics = _metric_counts(selected)
    symbol_hhi, top_symbol, top_symbol_share = _hhi(selected, "symbol")
    year_hhi, top_year, top_year_share = _hhi(selected, "year")
    quarter_hhi, top_quarter, top_quarter_share = _hhi(selected, "quarter")
    metrics.update({
        "symbol_hhi": symbol_hhi,
        "top_symbol": top_symbol,
        "top_symbol_trade_share": top_symbol_share,
        "year_hhi": year_hhi,
        "top_year": top_year,
        "top_year_trade_share": top_year_share,
        "quarter_hhi": quarter_hhi,
        "top_quarter": top_quarter,
        "top_quarter_trade_share": top_quarter_share,
    })
    metrics["quality_score"] = _quality_score(metrics)
    return Candidate(predicates, frozenset(row.outcome_id for row in selected), metrics)


def _build_base_predicates(outcomes: list[SamplerOutcome], symbols: list[str]) -> list[Predicate]:
    predicates: list[Predicate] = []
    for symbol in symbols:
        predicates.append(Predicate(f"symbol={symbol}", "symbol", symbol, frozenset({symbol})))
    predicates.extend(_market_family_predicates(symbols))
    dimensions = (
        "direction",
        "pole_boxes",
        "pole_duration",
        "pole_velocity",
        "relative_pole_size",
        "reversal_boxes",
        "retrace_quality",
        "trend_regime",
        "current_pnf_direction",
        "choppiness",
        "breakout_context",
        "exchange",
        "year",
        "quarter",
        "month",
    )
    for dimension in dimensions:
        values = sorted({row.segments.get(dimension, "UNKNOWN") for row in outcomes})
        for value in values:
            if value != "UNKNOWN":
                predicates.append(Predicate(f"{dimension}={value}", dimension, value, frozenset({value})))
    return sorted({predicate.predicate_id: predicate for predicate in predicates}.values(), key=lambda predicate: predicate.predicate_id)


def _prefilter_predicates(
    predicates: list[Predicate], outcomes: list[SamplerOutcome], min_trades: int, min_wins: int
) -> tuple[list[Predicate], int]:
    usable: list[Predicate] = []
    rejected = 0
    for predicate in predicates:
        metrics = _evaluate_predicates((predicate,), outcomes).metrics
        if int(metrics.get("trades") or 0) >= min_trades and int(metrics.get("wins") or 0) >= min_wins:
            usable.append(predicate)
        else:
            rejected += 1
    return usable, rejected


def _valid_combo(predicates: tuple[Predicate, ...]) -> bool:
    dimensions = [predicate.dimension for predicate in predicates]
    return len(set(dimensions)) == len(dimensions)


def enumerate_candidates(
    outcomes: list[SamplerOutcome],
    symbols: list[str],
    *,
    min_trades: int = DEFAULT_MIN_TRADES,
    min_wins: int = DEFAULT_MIN_WINS,
) -> tuple[list[Candidate], dict[str, Any]]:
    base_predicates = _build_base_predicates(outcomes, symbols)
    usable_predicates, rejected_base_predicates = _prefilter_predicates(base_predicates, outcomes, min_trades, min_wins)
    candidates: list[Candidate] = []
    evaluated = 0
    rejected_tiny = 0
    for width in range(1, MAX_RULE_WIDTH + 1):
        for combo in combinations(usable_predicates, width):
            if not _valid_combo(combo):
                continue
            evaluated += 1
            ordered = tuple(sorted(combo, key=lambda predicate: predicate.predicate_id))
            candidate = _evaluate_predicates(ordered, outcomes)
            if int(candidate.metrics.get("trades") or 0) < min_trades or int(candidate.metrics.get("wins") or 0) < min_wins:
                rejected_tiny += 1
                continue
            candidates.append(candidate)
    stats = {
        "base_predicates": len(base_predicates),
        "usable_base_predicates": len(usable_predicates),
        "rejected_base_predicates": rejected_base_predicates,
        "evaluated_combinations": evaluated,
        "rejected_tiny_candidates": rejected_tiny,
        "accepted_candidates": len(candidates),
        "max_rule_width": MAX_RULE_WIDTH,
    }
    return candidates, stats


def _candidate_sort_key(candidate: Candidate) -> tuple[float, float, int, str]:
    quality = candidate.metrics.get("quality_score")
    expectancy = candidate.metrics.get("expectancy")
    return (
        float(quality) if quality != "" else -999.0,
        float(expectancy) if expectancy != "" else -999.0,
        int(candidate.metrics.get("trades") or 0),
        _rule_fingerprint(candidate.predicates),
    )


def _jaccard(left: frozenset[str], right: frozenset[str]) -> float:
    if not left and not right:
        return 1.0
    return len(left & right) / len(left | right) if left or right else 0.0


def _cluster_candidates(candidates: list[Candidate], max_candidates_to_cluster: int, min_trades: int, min_wins: int) -> list[dict[str, Any]]:
    ranked = sorted(candidates, key=_candidate_sort_key, reverse=True)[:max_candidates_to_cluster]
    representatives: list[tuple[str, Candidate]] = []
    rows: list[dict[str, Any]] = []
    for ordinal, candidate in enumerate(ranked, start=1):
        best_cluster_id = ""
        best_rep_id = ""
        best_similarity = 0.0
        for cluster_id, representative in representatives:
            similarity = _jaccard(candidate.selected_ids, representative.selected_ids)
            if similarity > best_similarity:
                best_similarity = similarity
                best_cluster_id = cluster_id
                best_rep_id = representative.metrics["candidate_id"]
        is_representative = best_similarity < JACCARD_DUPLICATE_THRESHOLD
        if is_representative:
            cluster_id = f"CLUSTER-{len(representatives) + 1:04d}"
            suppressed_by = ""
            novelty = 1.0
        else:
            cluster_id = best_cluster_id
            suppressed_by = best_rep_id
            novelty = _round(1.0 - best_similarity)
        candidate_id = f"CAND-{ordinal:06d}"
        candidate.metrics["candidate_id"] = candidate_id
        candidate.metrics["candidate_verdict"] = _candidate_verdict(candidate.metrics, min_trades, min_wins)
        if is_representative:
            representatives.append((cluster_id, candidate))
        rows.append({
            "candidate_id": candidate_id,
            "candidate_verdict": candidate.metrics["candidate_verdict"],
            "cluster_id": cluster_id,
            "is_cluster_representative": str(is_representative),
            "suppressed_by_candidate_id": suppressed_by,
            "novelty_jaccard_to_cluster_representative": novelty,
            "rule_width": len(candidate.predicates),
            "rule_definition": _rule_definition(candidate.predicates),
            "rule_fingerprint": _rule_fingerprint(candidate.predicates),
            **candidate.metrics,
        })
    return rows


def _top_rows(clustered_rows: list[dict[str, Any]], limit: int = TOP_N) -> list[dict[str, Any]]:
    return [row for row in clustered_rows if row["is_cluster_representative"] == "True"][:limit]


def _universe_consistency(all_metrics: dict[str, Any]) -> dict[str, Any]:
    actual_expectancy = all_metrics.get("expectancy")
    actual_total_r = all_metrics.get("total_R")
    expectancy_delta = "" if actual_expectancy == "" else _round(float(actual_expectancy) - BASELINE_P2_EXPECTANCY)
    total_r_delta = "" if actual_total_r == "" else _round(float(actual_total_r) - BASELINE_P2_TOTAL_R)
    expected = {
        "observations": BASELINE_P2_OBSERVATIONS,
        "trades": BASELINE_P2_TRADES,
        "wins": BASELINE_P2_WINS,
        "losses": BASELINE_P2_LOSSES,
        "break_even_exits": BASELINE_P2_BREAK_EVEN_EXITS,
        "expectancy": BASELINE_P2_EXPECTANCY,
        "total_R": BASELINE_P2_TOTAL_R,
    }
    actual = {key: all_metrics.get(key, "") for key in expected}
    mismatches: list[str] = []
    for key in ("observations", "trades", "wins", "losses", "break_even_exits"):
        if int(actual.get(key) or 0) != int(expected[key]):
            mismatches.append(key)
    if expectancy_delta == "" or abs(float(expectancy_delta)) > UNIVERSE_EXPECTANCY_TOLERANCE:
        mismatches.append("expectancy")
    if total_r_delta == "" or abs(float(total_r_delta)) > UNIVERSE_TOTAL_R_TOLERANCE:
        mismatches.append("total_R")
    return {
        "status": "UNIVERSE_MATCH" if not mismatches else "UNIVERSE_MISMATCH",
        "expected": expected,
        "actual": actual,
        "deltas": {
            "observations": int(actual.get("observations") or 0) - BASELINE_P2_OBSERVATIONS,
            "trades": int(actual.get("trades") or 0) - BASELINE_P2_TRADES,
            "wins": int(actual.get("wins") or 0) - BASELINE_P2_WINS,
            "losses": int(actual.get("losses") or 0) - BASELINE_P2_LOSSES,
            "break_even_exits": int(actual.get("break_even_exits") or 0) - BASELINE_P2_BREAK_EVEN_EXITS,
            "expectancy": expectancy_delta,
            "total_R": total_r_delta,
        },
        "mismatched_fields": mismatches,
    }


def _run_verdict(top_rows: list[dict[str, Any]], all_metrics: dict[str, Any], min_trades: int) -> str:
    if int(all_metrics.get("trades") or 0) < min_trades:
        return "INSUFFICIENT_DATA"
    verdicts = [row["candidate_verdict"] for row in top_rows]
    if "ROBUST_SUBPOPULATION_FOUND" in verdicts:
        return "ROBUST_SUBPOPULATION_FOUND"
    if "PROMISING_SUBPOPULATIONS_FOUND" in verdicts:
        return "PROMISING_SUBPOPULATIONS_FOUND"
    if "EDGE_CONCENTRATED_BUT_UNSTABLE" in verdicts:
        return "EDGE_CONCENTRATED_BUT_UNSTABLE"
    return "NO_ROBUST_SUBPOPULATION"


def _write_summary(
    path: Path,
    verdict: str,
    all_metrics: dict[str, Any],
    universe: dict[str, Any],
    enumeration_stats: dict[str, Any],
    top_rows: list[dict[str, Any]],
    min_trades: int,
    min_wins: int,
) -> None:
    with path.open("x") as handle:
        handle.write("# P+2 deterministic candidate sampler summary\n\n")
        handle.write("Research only. No live trader, detector, production strategy, exchange code, genetic algorithm, mutation, crossover, or candidate promotion is included.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n")
        handle.write("## Universe reconstruction\n\n")
        handle.write(f"- `status`: {universe['status']}\n")
        for key in ("observations", "trades", "wins", "losses", "break_even_exits", "expectancy", "total_R"):
            handle.write(f"- `actual_{key}`: {all_metrics.get(key, '')}\n")
            handle.write(f"- `expected_{key}`: {universe['expected'][key]}\n")
        handle.write("\n## Minimum sample requirements\n\n")
        handle.write(f"- `min_trades`: {min_trades}\n")
        handle.write(f"- `min_wins`: {min_wins}\n")
        handle.write("- Candidates below either threshold are rejected before ranking.\n\n")
        handle.write("## Deterministic ranking logic\n\n")
        handle.write("The quality score is deterministic and combines expectancy, total_R, trade-count support, and concentration penalties. It is not genetic scoring and uses no evolution, mutation, or crossover.\n\n")
        handle.write("```text\nquality_score = expectancy + 0.05 * trade_count_score + 0.10 * total_R_score - concentration_penalties\n```\n\n")
        handle.write("Concentration penalties apply to symbol HHI above 0.45, year HHI above 0.40, and quarter HHI above 0.30.\n\n")
        handle.write("## Novelty control\n\n")
        handle.write(f"Candidates are clustered greedily by selected-opportunity Jaccard similarity. A candidate with similarity >= {JACCARD_DUPLICATE_THRESHOLD} to an earlier higher-ranked representative is marked suppressed; `top100` includes cluster representatives only.\n\n")
        handle.write("## Enumeration stats\n\n")
        for key, value in enumeration_stats.items():
            handle.write(f"- `{key}`: {value}\n")
        handle.write("\n## Top cluster representatives\n\n")
        handle.write("| candidate_id | verdict | rule | trades | wins | expectancy | total_R | score | top_symbol_share | top_quarter_share |\n")
        handle.write("|---|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in top_rows[:10]:
            handle.write(
                f"| {row['candidate_id']} | {row['candidate_verdict']} | {row['rule_definition']} | "
                f"{row['trades']} | {row['wins']} | {row['expectancy']} | {row['total_R']} | "
                f"{row['quality_score']} | {row['top_symbol_trade_share']} | {row['top_quarter_trade_share']} |\n"
            )
        handle.write("\n## Output files\n\n")
        for name in OUTPUT_NAMES:
            handle.write(f"- `{name}`\n")
        handle.write("\n## Allowed verdicts\n\n")
        for allowed in ALLOWED_VERDICTS:
            handle.write(f"- `{allowed}`\n")


def run(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    output_root: Path,
    candle_symbols: dict[str, str] | None = None,
    *,
    limit_rows_per_symbol: int | None = None,
    require_full_universe: bool = True,
    min_trades: int = DEFAULT_MIN_TRADES,
    min_wins: int = DEFAULT_MIN_WINS,
    max_candidates_to_cluster: int = DEFAULT_MAX_CANDIDATES_TO_CLUSTER,
) -> dict[str, Any]:
    symbols, segmented, candles_by_symbol, load_flags = load_segmented_observations(
        symbol_inputs,
        columns_inputs,
        candles_inputs,
        candle_symbols or {},
        limit_rows_per_symbol=limit_rows_per_symbol,
    )
    _validate_full_universe(symbols, require_full_universe)
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing P+2 candidate sampler output(s): {', '.join(existing)}")

    outcomes = _build_outcomes(segmented, candles_by_symbol)
    all_metrics = _metric_counts(outcomes)
    universe = _universe_consistency(all_metrics)
    candidates, enumeration_stats = enumerate_candidates(outcomes, symbols, min_trades=min_trades, min_wins=min_wins)
    clustered_rows = _cluster_candidates(candidates, max_candidates_to_cluster, min_trades, min_wins)
    top_rows = _top_rows(clustered_rows)
    verdict = _run_verdict(top_rows, all_metrics, min_trades)

    _write_csv(output_root / OUTPUT_NAMES[0], RESULT_FIELDS, clustered_rows)
    _write_csv(output_root / OUTPUT_NAMES[1], RESULT_FIELDS, top_rows)
    _write_summary(output_root / OUTPUT_NAMES[2], verdict, all_metrics, universe, enumeration_stats, top_rows, min_trades, min_wins)

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_p2_candidate_sampler_phase1",
        "research_only": True,
        "production_modifications": False,
        "live_trader_modifications": False,
        "detector_modifications": False,
        "exchange_code_modifications": False,
        "strategy_promotion": False,
        "genetic_algorithm": False,
        "evolution": False,
        "mutation": False,
        "crossover": False,
        "motif": MOTIF_NAME,
        "knowable_at": "P+2:pole->reversal->confirmation",
        "entry": "NEXT_COLUMN_OPEN_ENTRY",
        "target_R": TARGET_R,
        "break_even_after_R": BREAK_EVEN_TRIGGER_R,
        "required_symbols": list(EXPECTED_SYMBOLS),
        "full_seven_market_universe": set(symbols) == set(EXPECTED_SYMBOLS),
        "symbols": symbols,
        "limit_rows_per_symbol": limit_rows_per_symbol,
        "min_trades": min_trades,
        "min_wins": min_wins,
        "max_rule_width": MAX_RULE_WIDTH,
        "forbidden_features": ["opposing_pole_distance_columns", "enhanced_by_opposing_pole", "future_outcome_labels", "post_entry_information"],
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "verdict": verdict,
        "aggregate": all_metrics,
        "universe_consistency": universe,
        "enumeration_stats": enumeration_stats,
        "novelty_control": {
            "method": "greedy_selected_opportunity_jaccard_clustering",
            "duplicate_threshold": JACCARD_DUPLICATE_THRESHOLD,
            "top100_cluster_representatives_only": True,
            "max_candidates_to_cluster": max_candidates_to_cluster,
        },
        "ranking_logic": {
            "formula": "expectancy + 0.05*trade_count_score + 0.10*total_R_score - concentration_penalties",
            "concentration_penalties": {"symbol_hhi_above": 0.45, "year_hhi_above": 0.40, "quarter_hhi_above": 0.30},
        },
        "load_flags": load_flags,
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[3]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only Phase 1 deterministic sampler for causal P+2 pole candidates")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--limit-rows-per-symbol", type=int, help="optional diagnostic cap after P+2 motif filtering")
    parser.add_argument("--allow-partial-universe", action="store_true", help="allow diagnostic runs outside the full BTC/ETH/SOL/ENA/HYPE/SUI/TAO universe")
    parser.add_argument("--min-trades", type=int, default=DEFAULT_MIN_TRADES, help="minimum break-even-managed trades per candidate")
    parser.add_argument("--min-wins", type=int, default=DEFAULT_MIN_WINS, help="minimum TARGET_FIRST wins per candidate")
    parser.add_argument("--max-candidates-to-cluster", type=int, default=DEFAULT_MAX_CANDIDATES_TO_CLUSTER, help="deterministic cap on ranked candidates retained for clustering/output")
    args = parser.parse_args()
    try:
        run(
            dict(args.symbol_input),
            dict(args.columns_input),
            dict(args.candles_input),
            args.output_root,
            dict(args.candle_symbol),
            limit_rows_per_symbol=args.limit_rows_per_symbol,
            require_full_universe=not args.allow_partial_universe,
            min_trades=args.min_trades,
            min_wins=args.min_wins,
            max_candidates_to_cluster=args.max_candidates_to_cluster,
        )
    except (FileExistsError, OSError, sqlite3.Error, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
