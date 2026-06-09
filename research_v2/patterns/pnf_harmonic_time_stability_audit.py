"""Research-only time stability audit for harmonic reaction-ratio clusters.

This module replays raw PnF columns independently inside fixed calendar periods
using the SLOW harmonic swing threshold only (5 boxes / 0.382), then measures
whether the same nearest-harmonic reaction-ratio clusters persist through time.
It intentionally does not implement pattern detection, setup generation,
expectancy, entries, exits, signals, scanners, or live/demo trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Iterable, Sequence
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_v2.patterns.pnf_harmonic_ratio_stability_audit import (
    HARMONIC_LEVELS,
    SYMBOL_ORDER,
    Reaction,
    cluster_strength_rows,
    nearest_reactions,
)
from research_v2.patterns.pnf_harmonic_swing_threshold_audit import (
    ThresholdSet,
    load_columns,
    run_threshold_audit,
)

DEFAULT_COLUMNS_INPUT = (
    Path("research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/columns/BTCUSDT_columns.csv"),
    Path("research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/columns/ETHUSDT_columns.csv"),
    Path("research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/columns/SOLUSDT_columns.csv"),
)
DEFAULT_OUTPUT_ROOT = Path(
    "research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit"
)
SLOW_THRESHOLD = ThresholdSet("SLOW", 5.0, 0.382)
MISSING_RANK = len(HARMONIC_LEVELS) + 1

OUTPUT_TIME_SUMMARY = "harmonic_time_stability_summary.csv"
OUTPUT_LEVEL_RANKINGS = "harmonic_level_rankings.csv"
OUTPUT_LEVEL_SURVIVAL = "harmonic_level_survival.csv"
OUTPUT_CROSS_PERIOD_CONSISTENCY = "harmonic_cross_period_consistency.csv"
OUTPUT_PERIOD_COMPARISON = "harmonic_period_comparison.csv"
OUTPUT_REPORT = "harmonic_time_stability_report.md"
OUTPUT_NAMES = (
    OUTPUT_TIME_SUMMARY,
    OUTPUT_LEVEL_RANKINGS,
    OUTPUT_LEVEL_SURVIVAL,
    OUTPUT_CROSS_PERIOD_CONSISTENCY,
    OUTPUT_PERIOD_COMPARISON,
    OUTPUT_REPORT,
)

TIME_SUMMARY_FIELDS = [
    "period",
    "symbol",
    "total_reactions",
    "total_confirming_reactions",
    "total_internal_reactions",
]
LEVEL_RANKING_FIELDS = [
    "period",
    "harmonic_level",
    "observed_frequency",
    "expected_frequency",
    "enrichment_factor",
    "cluster_strength_score",
    "rank",
]
LEVEL_SURVIVAL_FIELDS = [
    "harmonic_level",
    "present_2024",
    "present_2025",
    "present_2026",
    "present_all_years",
    "best_rank",
    "worst_rank",
    "average_rank",
]
CROSS_PERIOD_CONSISTENCY_FIELDS = [
    "harmonic_level",
    "rank_stddev",
    "frequency_stddev",
    "enrichment_stddev",
    "consistency_score",
]
PERIOD_COMPARISON_FIELDS = [
    "period_a",
    "period_b",
    "harmonic_level",
    "rank_difference",
    "frequency_difference",
    "enrichment_difference",
]


@dataclass(frozen=True)
class PeriodSpec:
    name: str
    start: datetime
    end: datetime
    required: bool = False


def _format_number(value: float | int | str | None) -> str | int:
    if value is None:
        return ""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return value
    if not math.isfinite(value):
        return ""
    return f"{value:.12g}"


def _to_float(value: Any, *, default: float = math.nan) -> float:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _timestamp_seconds(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        parsed = math.nan
    if math.isfinite(parsed):
        # Local PnF exports commonly store millisecond epochs. Convert large
        # numeric timestamps to seconds so calendar period boundaries are causal.
        return parsed / 1000.0 if parsed > 10_000_000_000 else parsed
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _period_specs() -> list[PeriodSpec]:
    utc = timezone.utc
    return [
        PeriodSpec("2024", datetime(2024, 1, 1, tzinfo=utc), datetime(2025, 1, 1, tzinfo=utc), True),
        PeriodSpec("2025", datetime(2025, 1, 1, tzinfo=utc), datetime(2026, 1, 1, tzinfo=utc), True),
        PeriodSpec("2026", datetime(2026, 1, 1, tzinfo=utc), datetime(2027, 1, 1, tzinfo=utc), True),
        PeriodSpec("2024H1", datetime(2024, 1, 1, tzinfo=utc), datetime(2024, 7, 1, tzinfo=utc)),
        PeriodSpec("2024H2", datetime(2024, 7, 1, tzinfo=utc), datetime(2025, 1, 1, tzinfo=utc)),
        PeriodSpec("2025H1", datetime(2025, 1, 1, tzinfo=utc), datetime(2025, 7, 1, tzinfo=utc)),
        PeriodSpec("2025H2", datetime(2025, 7, 1, tzinfo=utc), datetime(2026, 1, 1, tzinfo=utc)),
        PeriodSpec("2026H1", datetime(2026, 1, 1, tzinfo=utc), datetime(2026, 7, 1, tzinfo=utc)),
        PeriodSpec("2026H2", datetime(2026, 7, 1, tzinfo=utc), datetime(2027, 1, 1, tzinfo=utc)),
    ]


def _column_time(column: Any) -> float | None:
    return _timestamp_seconds(getattr(column, "completion_time", "")) or _timestamp_seconds(
        getattr(column, "end_ts", "")
    )


def _columns_for_period(columns: Sequence[Any], period: PeriodSpec) -> list[Any]:
    start = period.start.timestamp()
    end = period.end.timestamp()
    return [
        column
        for column in columns
        if (_column_time(column) is not None and start <= float(_column_time(column)) < end)
    ]


def _period_has_data(columns: Sequence[Any], period: PeriodSpec) -> bool:
    return bool(_columns_for_period(columns, period))


def _symbol_name(symbol: str) -> str:
    text = symbol.upper().strip()
    token = text.rsplit(":", 1)[-1]
    for prefix in SYMBOL_ORDER:
        if text == prefix or token == prefix or token.startswith(f"{prefix}USDT"):
            return prefix
    return text


def _reaction_from_threshold_row(period: str, index: int, row: dict[str, Any]) -> Reaction:
    symbol = _symbol_name(str(row.get("symbol") or ""))
    return Reaction(
        reaction_id=f"{period}:{symbol}:SLOW:{index:06d}",
        threshold_name="SLOW",
        symbol=symbol,
        active_direction=str(row.get("active_direction") or "").strip(),
        candidate_direction=str(row.get("candidate_direction") or "").strip(),
        reaction_kind=str(row.get("reaction_kind") or "").strip(),
        candidate_boxes=_to_float(row.get("candidate_boxes")),
        active_swing_boxes=_to_float(row.get("active_swing_boxes")),
        raw_ratio=_to_float(row.get("reaction_ratio")),
        column_id=str(row.get("column_id") or "").strip(),
        knowledge_time=str(row.get("completion_time") or "").strip(),
        active_start_ts=str(row.get("active_start_ts") or "").strip(),
        active_end_ts=str(row.get("active_end_ts") or "").strip(),
    )


def _period_reactions(period: str, columns: Sequence[Any]) -> list[Reaction]:
    if not columns:
        return []
    results = run_threshold_audit(columns, [SLOW_THRESHOLD])
    reactions: list[Reaction] = []
    for index, row in enumerate(results["reactions"], start=1):
        if row.get("threshold_name") != "SLOW":
            continue
        reaction = _reaction_from_threshold_row(period, index, row)
        if reaction.symbol in SYMBOL_ORDER and math.isfinite(reaction.raw_ratio):
            reactions.append(reaction)
    return reactions


def _rankings_for_period(period: str, reactions: Sequence[Reaction]) -> list[dict[str, Any]]:
    nearest = nearest_reactions(reactions, HARMONIC_LEVELS)
    strength_rows = cluster_strength_rows(nearest, HARMONIC_LEVELS)
    if not reactions:
        return [
            {
                "period": period,
                "harmonic_level": _format_number(level),
                "observed_frequency": _format_number(0.0),
                "expected_frequency": "",
                "enrichment_factor": _format_number(0.0),
                "cluster_strength_score": _format_number(0.0),
                "rank": MISSING_RANK,
            }
            for level in HARMONIC_LEVELS
        ]
    return [
        {
            "period": period,
            "harmonic_level": row["harmonic_level"],
            "observed_frequency": row["observed_frequency"],
            "expected_frequency": row["expected_frequency"],
            "enrichment_factor": row["enrichment_factor"],
            "cluster_strength_score": row["cluster_strength_score"],
            "rank": row["rank"],
        }
        for row in strength_rows
    ]


def _summary_rows(period_reactions: dict[str, list[Reaction]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for period in period_reactions:
        reactions = period_reactions[period]
        for symbol in SYMBOL_ORDER:
            symbol_reactions = [reaction for reaction in reactions if reaction.symbol == symbol]
            rows.append(
                {
                    "period": period,
                    "symbol": symbol,
                    "total_reactions": len(symbol_reactions),
                    "total_confirming_reactions": sum(
                        1 for reaction in symbol_reactions if reaction.reaction_kind == "CONFIRMING"
                    ),
                    "total_internal_reactions": sum(
                        1 for reaction in symbol_reactions if reaction.reaction_kind == "INTERNAL"
                    ),
                }
            )
    return rows


def _ranking_lookup(rows: Sequence[dict[str, Any]]) -> dict[tuple[str, float], dict[str, Any]]:
    lookup: dict[tuple[str, float], dict[str, Any]] = {}
    for row in rows:
        lookup[(str(row["period"]), _to_float(row["harmonic_level"]))] = row
    return lookup


def _metric(row: dict[str, Any] | None, field: str, missing: float = 0.0) -> float:
    if row is None:
        return missing
    value = _to_float(row.get(field), default=missing)
    return value if math.isfinite(value) else missing


def _survival_rows(ranking_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    lookup = _ranking_lookup(ranking_rows)
    rows: list[dict[str, Any]] = []
    for level in HARMONIC_LEVELS:
        yearly_rows = {year: lookup.get((year, level)) for year in ("2024", "2025", "2026")}
        present = {
            year: _metric(row, "observed_frequency") > 0 for year, row in yearly_rows.items()
        }
        present_ranks = [
            int(_metric(row, "rank", missing=MISSING_RANK))
            for year, row in yearly_rows.items()
            if present[year]
        ]
        rows.append(
            {
                "harmonic_level": _format_number(level),
                "present_2024": present["2024"],
                "present_2025": present["2025"],
                "present_2026": present["2026"],
                "present_all_years": all(present.values()),
                "best_rank": min(present_ranks) if present_ranks else "",
                "worst_rank": max(present_ranks) if present_ranks else "",
                "average_rank": _format_number(mean(present_ranks) if present_ranks else math.nan),
            }
        )
    return rows


def _consistency_rows(ranking_rows: Sequence[dict[str, Any]], periods: Sequence[str]) -> list[dict[str, Any]]:
    lookup = _ranking_lookup(ranking_rows)
    rows: list[dict[str, Any]] = []
    for level in HARMONIC_LEVELS:
        ranks = [_metric(lookup.get((period, level)), "rank", missing=MISSING_RANK) for period in periods]
        frequencies = [_metric(lookup.get((period, level)), "observed_frequency") for period in periods]
        enrichments = [_metric(lookup.get((period, level)), "enrichment_factor") for period in periods]
        rank_stddev = pstdev(ranks) if len(ranks) > 1 else 0.0
        frequency_stddev = pstdev(frequencies) if len(frequencies) > 1 else 0.0
        enrichment_stddev = pstdev(enrichments) if len(enrichments) > 1 else 0.0
        mean_frequency = mean(frequencies) if frequencies else 0.0
        mean_enrichment = mean(enrichments) if enrichments else 0.0
        consistency_score = (mean_frequency * (1.0 + mean_enrichment)) / (
            1.0 + rank_stddev + frequency_stddev + enrichment_stddev
        )
        rows.append(
            {
                "harmonic_level": _format_number(level),
                "rank_stddev": _format_number(rank_stddev),
                "frequency_stddev": _format_number(frequency_stddev),
                "enrichment_stddev": _format_number(enrichment_stddev),
                "consistency_score": _format_number(consistency_score),
            }
        )
    return sorted(rows, key=lambda row: -_to_float(row["consistency_score"]))


def _comparison_rows(ranking_rows: Sequence[dict[str, Any]], periods: Sequence[str]) -> list[dict[str, Any]]:
    lookup = _ranking_lookup(ranking_rows)
    rows: list[dict[str, Any]] = []
    for period_a, period_b in combinations(periods, 2):
        for level in HARMONIC_LEVELS:
            row_a = lookup.get((period_a, level))
            row_b = lookup.get((period_b, level))
            rows.append(
                {
                    "period_a": period_a,
                    "period_b": period_b,
                    "harmonic_level": _format_number(level),
                    "rank_difference": _format_number(
                        _metric(row_b, "rank", missing=MISSING_RANK)
                        - _metric(row_a, "rank", missing=MISSING_RANK)
                    ),
                    "frequency_difference": _format_number(
                        _metric(row_b, "observed_frequency")
                        - _metric(row_a, "observed_frequency")
                    ),
                    "enrichment_difference": _format_number(
                        _metric(row_b, "enrichment_factor")
                        - _metric(row_a, "enrichment_factor")
                    ),
                }
            )
    return rows


def _top_levels(ranking_rows: Sequence[dict[str, Any]], periods: Sequence[str], limit: int = 5) -> list[str]:
    scores: dict[float, list[float]] = {level: [] for level in HARMONIC_LEVELS}
    lookup = _ranking_lookup(ranking_rows)
    for level in HARMONIC_LEVELS:
        for period in periods:
            scores[level].append(_metric(lookup.get((period, level)), "cluster_strength_score"))
    ordered = sorted(scores.items(), key=lambda item: (-mean(item[1]), item[0]))
    return [_format_number(level) for level, _values in ordered[:limit]]


def _levels_where(rows: Sequence[dict[str, Any]], field: str) -> list[str]:
    return [str(row["harmonic_level"]) for row in rows if str(row.get(field)) == "True"]


def _zero_frequency_levels(ranking_rows: Sequence[dict[str, Any]], periods: Sequence[str]) -> list[str]:
    lookup = _ranking_lookup(ranking_rows)
    out: list[str] = []
    for level in HARMONIC_LEVELS:
        if all(_metric(lookup.get((period, level)), "observed_frequency") == 0 for period in periods):
            out.append(str(_format_number(level)))
    return out



def _collapsed_levels(ranking_rows: Sequence[dict[str, Any]]) -> list[str]:
    lookup = _ranking_lookup(ranking_rows)
    out: list[str] = []
    for level in HARMONIC_LEVELS:
        yearly_presence = [
            _metric(lookup.get((year, level)), "observed_frequency") > 0
            for year in ("2024", "2025", "2026")
        ]
        if any(yearly_presence) and not all(yearly_presence):
            out.append(str(_format_number(level)))
    return out


def _cross_symbol_recurs_answer(period_reactions: dict[str, list[Reaction]], level: float) -> str:
    yearly_symbols: dict[str, set[str]] = {year: set() for year in ("2024", "2025", "2026")}
    populated_years = [year for year in yearly_symbols if period_reactions.get(year)]
    for year in yearly_symbols:
        for row in nearest_reactions(period_reactions.get(year, []), HARMONIC_LEVELS):
            if row.nearest_level == level:
                yearly_symbols[year].add(row.reaction.symbol)
    all_years_cross_symbol = all(
        all(symbol in yearly_symbols[year] for symbol in SYMBOL_ORDER)
        for year in yearly_symbols
    )
    if all_years_cross_symbol:
        return "Yes."
    populated_cross_symbol = bool(populated_years) and all(
        all(symbol in yearly_symbols[year] for symbol in SYMBOL_ORDER)
        for year in populated_years
    )
    if populated_cross_symbol:
        return "No across all required years; yes inside populated local years only."
    return "No."

def _symbol_specific_levels(period_reactions: dict[str, list[Reaction]]) -> list[str]:
    counts: dict[float, dict[str, int]] = {
        level: {symbol: 0 for symbol in SYMBOL_ORDER} for level in HARMONIC_LEVELS
    }
    for reactions in period_reactions.values():
        for row in nearest_reactions(reactions, HARMONIC_LEVELS):
            counts[row.nearest_level][row.reaction.symbol] += 1
    out: list[str] = []
    for level, symbol_counts in counts.items():
        total = sum(symbol_counts.values())
        if not total:
            continue
        if max(symbol_counts.values()) / total >= 0.70 and sum(1 for value in symbol_counts.values() if value) < 3:
            out.append(str(_format_number(level)))
    return out


def _write_report(
    path: Path,
    *,
    periods: Sequence[str],
    ranking_rows: Sequence[dict[str, Any]],
    survival_rows: Sequence[dict[str, Any]],
    consistency_rows: Sequence[dict[str, Any]],
    period_reactions: dict[str, list[Reaction]],
) -> None:
    survivors = _levels_where(survival_rows, "present_all_years")
    collapsed = _collapsed_levels(ranking_rows)
    stable = [
        str(row["harmonic_level"])
        for row in consistency_rows
        if _to_float(row.get("consistency_score")) > 0
    ][:5]
    noise = _zero_frequency_levels(ranking_rows, periods)
    symbol_specific = _symbol_specific_levels(period_reactions)
    top_levels = _top_levels(ranking_rows, periods)
    yearly_totals = {year: len(period_reactions.get(year, [])) for year in ("2024", "2025", "2026")}

    lines = [
        "# Harmonic Time Stability Report",
        "",
        "## 1. Which harmonic levels are strongest overall?",
        ", ".join(top_levels) if top_levels else "None.",
        "",
        "## 2. Which harmonic levels survive all years?",
        (
            ", ".join(survivors)
            if survivors
            else f"None; required yearly reaction totals were {yearly_totals}."
        ),
        "",
        "## 3. Which harmonic levels collapse when time changes?",
        ", ".join(collapsed) if collapsed else "None detected.",
        "",
        "## 4. Which harmonic levels are most stable?",
        ", ".join(stable) if stable else "None.",
        "",
        "## 5. Which harmonic levels are symbol-specific?",
        ", ".join(symbol_specific) if symbol_specific else "None detected.",
        "",
        "## 6. Does 0.382 remain a cross-symbol recurring level?",
        _cross_symbol_recurs_answer(period_reactions, 0.382),
        "",
        "## 7. Does 0.618 remain a cross-symbol recurring level?",
        _cross_symbol_recurs_answer(period_reactions, 0.618),
        "",
        "## 8. Does 1.618 remain a cross-symbol recurring level?",
        _cross_symbol_recurs_answer(period_reactions, 1.618),
        "",
        "## 9. Which levels are likely structural?",
        ", ".join(survivors) if survivors else "None validated as structural in this time audit.",
        "",
        "## 10. Which levels appear to be noise?",
        ", ".join(noise) if noise else "None are completely absent across audited periods.",
        "",
    ]
    path.write_text("\n".join(line for line in lines if line is not None), encoding="utf-8")


def _write_csv(path: Path, fields: Sequence[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run_audit(
    *,
    columns_input: Sequence[str | Path] = DEFAULT_COLUMNS_INPUT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    columns = load_columns([Path(path) for path in columns_input])
    period_specs = _period_specs()
    audited_periods = [
        period for period in period_specs if period.required or _period_has_data(columns, period)
    ]

    period_reactions: dict[str, list[Reaction]] = {}
    for period in audited_periods:
        period_columns = _columns_for_period(columns, period)
        period_reactions[period.name] = _period_reactions(period.name, period_columns)

    ranking_rows: list[dict[str, Any]] = []
    for period in audited_periods:
        ranking_rows.extend(_rankings_for_period(period.name, period_reactions[period.name]))

    period_names = [period.name for period in audited_periods]
    summary = _summary_rows(period_reactions)
    survival = _survival_rows(ranking_rows)
    consistency = _consistency_rows(ranking_rows, period_names)
    comparisons = _comparison_rows(ranking_rows, period_names)

    output_path = Path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)
    _write_csv(output_path / OUTPUT_TIME_SUMMARY, TIME_SUMMARY_FIELDS, summary)
    _write_csv(output_path / OUTPUT_LEVEL_RANKINGS, LEVEL_RANKING_FIELDS, ranking_rows)
    _write_csv(output_path / OUTPUT_LEVEL_SURVIVAL, LEVEL_SURVIVAL_FIELDS, survival)
    _write_csv(
        output_path / OUTPUT_CROSS_PERIOD_CONSISTENCY,
        CROSS_PERIOD_CONSISTENCY_FIELDS,
        consistency,
    )
    _write_csv(output_path / OUTPUT_PERIOD_COMPARISON, PERIOD_COMPARISON_FIELDS, comparisons)
    _write_report(
        output_path / OUTPUT_REPORT,
        periods=period_names,
        ranking_rows=ranking_rows,
        survival_rows=survival,
        consistency_rows=consistency,
        period_reactions=period_reactions,
    )

    return {
        "input_columns": len(columns),
        "audited_periods": ",".join(period_names),
        "output_root": str(output_path),
        "output_files": [str(output_path / name) for name in OUTPUT_NAMES],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only time stability audit for SLOW harmonic ratio clusters."
    )
    parser.add_argument(
        "--columns-input",
        nargs="+",
        default=[str(path) for path in DEFAULT_COLUMNS_INPUT],
        help="Raw completed PnF column CSVs for BTCUSDT, ETHUSDT, and SOLUSDT.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Directory where time-stability outputs will be written.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = run_audit(columns_input=args.columns_input, output_root=args.output_root)
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
