"""Research-only audit of harmonic reaction-ratio stability.

This module consumes the SLOW swing-universe reaction export produced by
``pnf_harmonic_swing_threshold_audit.py`` and measures whether raw reaction
ratios naturally concentrate near pre-declared harmonic levels. It intentionally
performs no pattern recognition, no Gartley/Bat/Crab/Butterfly/AB=CD detection,
no setup generation, no expectancy analysis, and no production/scanner/live
trader changes.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Sequence

DEFAULT_REACTIONS_INPUT = Path(
    "research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/"
    "harmonic_reactions_by_threshold.csv"
)
DEFAULT_OUTPUT_ROOT = Path(
    "research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit"
)
DEFAULT_THRESHOLD_NAME = "SLOW"

HARMONIC_LEVELS = (
    0.236,
    0.382,
    0.500,
    0.618,
    0.707,
    0.786,
    0.886,
    1.000,
    1.130,
    1.270,
    1.414,
    1.618,
    2.000,
    2.240,
    2.618,
)
SYMBOL_ORDER = ("BTC", "ETH", "SOL")

OUTPUT_SUMMARY = "harmonic_ratio_summary.csv"
OUTPUT_DISTRIBUTION = "harmonic_ratio_distribution.csv"
OUTPUT_SYMBOL_BREAKDOWN = "harmonic_ratio_symbol_breakdown.csv"
OUTPUT_CLUSTER_STRENGTH = "harmonic_ratio_cluster_strength.csv"
OUTPUT_NEAREST_LEVEL = "harmonic_ratio_nearest_level.csv"
OUTPUT_NAMES = (
    OUTPUT_SUMMARY,
    OUTPUT_DISTRIBUTION,
    OUTPUT_SYMBOL_BREAKDOWN,
    OUTPUT_CLUSTER_STRENGTH,
    OUTPUT_NEAREST_LEVEL,
)

NEAREST_LEVEL_FIELDS = [
    "reaction_id",
    "threshold_name",
    "symbol",
    "direction",
    "active_direction",
    "candidate_direction",
    "reaction_kind",
    "raw_ratio",
    "nearest_harmonic_level",
    "distance_from_nearest_level",
    "knowledge_time",
    "candidate_boxes",
    "active_swing_boxes",
    "column_id",
    "active_start_ts",
    "active_end_ts",
]
SUMMARY_FIELDS = [
    "harmonic_level",
    "count",
    "percentage",
    "average_distance",
    "median_distance",
    "cluster_strength_score",
]
DISTRIBUTION_FIELDS = [
    "bucket",
    "count",
    "percentage",
    "min_ratio",
    "max_ratio",
]
SYMBOL_BREAKDOWN_FIELDS = [
    "symbol",
    "harmonic_level",
    "count",
    "percentage_within_symbol",
    "average_distance",
    "median_distance",
    "symbol_rank",
]
CLUSTER_STRENGTH_FIELDS = [
    "harmonic_level",
    "rank",
    "observed_frequency",
    "expected_frequency",
    "observed_count",
    "expected_count",
    "enrichment_factor",
    "average_distance",
    "median_distance",
    "proximity_score",
    "cluster_strength_score",
    "btc_count",
    "eth_count",
    "sol_count",
    "btc_rank",
    "eth_rank",
    "sol_rank",
    "dominates_all_three_symbols",
    "symbol_specific_behavior",
]

DISTRIBUTION_BUCKETS = (
    ("0.00-0.10", 0.00, 0.10),
    ("0.10-0.20", 0.10, 0.20),
    ("0.20-0.30", 0.20, 0.30),
    ("0.30-0.40", 0.30, 0.40),
    ("0.40-0.50", 0.40, 0.50),
    ("0.50-0.60", 0.50, 0.60),
    ("0.60-0.70", 0.60, 0.70),
    ("0.70-0.80", 0.70, 0.80),
    ("0.80-0.90", 0.80, 0.90),
    ("0.90-1.00", 0.90, 1.00),
    ("1.00-1.25", 1.00, 1.25),
    ("1.25-1.50", 1.25, 1.50),
    ("1.50-1.75", 1.50, 1.75),
    ("1.75-2.00", 1.75, 2.00),
    ("2.00-2.50", 2.00, 2.50),
    ("2.50-3.00", 2.50, 3.00),
    ("3.00+", 3.00, math.inf),
)


@dataclass(frozen=True)
class Reaction:
    """Raw reaction ratio observation from the SLOW harmonic swing universe."""

    reaction_id: str
    threshold_name: str
    symbol: str
    active_direction: str
    candidate_direction: str
    reaction_kind: str
    candidate_boxes: float
    active_swing_boxes: float
    raw_ratio: float
    column_id: str
    knowledge_time: str
    active_start_ts: str
    active_end_ts: str

    @property
    def direction(self) -> str:
        """Use the reaction/candidate direction as the audited direction."""

        return self.candidate_direction


@dataclass(frozen=True)
class NearestReaction:
    reaction: Reaction
    nearest_level: float
    distance: float


def _first_value(row: dict[str, Any], aliases: Sequence[str]) -> Any:
    for alias in aliases:
        value = row.get(alias)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _to_float(value: Any, *, label: str, row_number: int) -> float:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"row {row_number}: invalid {label}: {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"row {row_number}: non-finite {label}: {value!r}")
    return parsed


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


def _normalize_symbol(value: str) -> str:
    text = value.strip().upper()
    for symbol in SYMBOL_ORDER:
        if text == symbol or text.startswith(symbol):
            return symbol
    return text


def _mean(values: Sequence[float]) -> float:
    return mean(values) if values else math.nan


def _median(values: Sequence[float]) -> float:
    return median(values) if values else math.nan


def _nearest_level(raw_ratio: float, harmonic_levels: Sequence[float]) -> tuple[float, float]:
    level = min(harmonic_levels, key=lambda candidate: abs(raw_ratio - candidate))
    return level, abs(raw_ratio - level)


def _level_boundaries(harmonic_levels: Sequence[float]) -> dict[float, tuple[float, float]]:
    """Return nearest-level Voronoi intervals bounded by adjacent midpoints."""

    ordered = sorted(harmonic_levels)
    boundaries: dict[float, tuple[float, float]] = {}
    for index, level in enumerate(ordered):
        lower = 0.0 if index == 0 else (ordered[index - 1] + level) / 2.0
        upper = math.inf if index == len(ordered) - 1 else (level + ordered[index + 1]) / 2.0
        boundaries[level] = (lower, upper)
    return boundaries


def _uniform_expected_frequencies(
    *,
    harmonic_levels: Sequence[float],
    observed_min: float,
    observed_max: float,
) -> dict[float, float]:
    """Expected nearest-level frequencies under a continuous uniform ratio model.

    The null model is uniform across the observed reaction-ratio range. Each
    harmonic level owns the interval where it is the nearest declared level,
    clipped to the observed min/max. This directly answers whether the observed
    nearest-level counts are stronger than a uniform distribution would imply.
    """

    if observed_max <= observed_min:
        return {level: math.nan for level in harmonic_levels}

    total_width = observed_max - observed_min
    frequencies: dict[float, float] = {}
    for level, (lower, upper) in _level_boundaries(harmonic_levels).items():
        clipped_lower = max(lower, observed_min)
        clipped_upper = min(upper, observed_max)
        width = max(0.0, clipped_upper - clipped_lower)
        frequencies[level] = width / total_width
    return frequencies


def load_reactions(
    paths: Sequence[str | Path],
    *,
    threshold_name: str = DEFAULT_THRESHOLD_NAME,
) -> list[Reaction]:
    rows: list[Reaction] = []
    target_threshold = threshold_name.strip().upper()
    for path in paths:
        csv_path = Path(path)
        with csv_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ValueError(f"{csv_path}: expected a CSV header")
            for row_number, row in enumerate(reader, start=2):
                raw_threshold = str(row.get("threshold_name") or "").strip()
                if raw_threshold.upper() != target_threshold:
                    continue
                symbol = _normalize_symbol(str(_first_value(row, ("symbol",)) or ""))
                if symbol not in SYMBOL_ORDER:
                    continue
                raw_ratio = _to_float(
                    _first_value(row, ("reaction_ratio", "raw_ratio", "ratio")),
                    label="reaction_ratio",
                    row_number=row_number,
                )
                candidate_boxes = _to_float(
                    _first_value(row, ("candidate_boxes", "reaction_boxes")),
                    label="candidate_boxes",
                    row_number=row_number,
                )
                active_swing_boxes = _to_float(
                    _first_value(row, ("active_swing_boxes", "prior_swing_boxes")),
                    label="active_swing_boxes",
                    row_number=row_number,
                )
                rows.append(
                    Reaction(
                        reaction_id=f"{symbol}:{target_threshold}:{len(rows) + 1:06d}",
                        threshold_name=raw_threshold,
                        symbol=symbol,
                        active_direction=str(row.get("active_direction") or "").strip(),
                        candidate_direction=str(row.get("candidate_direction") or "").strip(),
                        reaction_kind=str(row.get("reaction_kind") or "").strip(),
                        candidate_boxes=candidate_boxes,
                        active_swing_boxes=active_swing_boxes,
                        raw_ratio=raw_ratio,
                        column_id=str(row.get("column_id") or "").strip(),
                        knowledge_time=str(
                            _first_value(
                                row,
                                (
                                    "knowledge_time",
                                    "completion_time",
                                    "completed_at",
                                    "end_ts",
                                ),
                            )
                            or ""
                        ).strip(),
                        active_start_ts=str(row.get("active_start_ts") or "").strip(),
                        active_end_ts=str(row.get("active_end_ts") or "").strip(),
                    )
                )
    return rows


def nearest_reactions(
    reactions: Sequence[Reaction], harmonic_levels: Sequence[float]
) -> list[NearestReaction]:
    rows: list[NearestReaction] = []
    for reaction in reactions:
        level, distance = _nearest_level(reaction.raw_ratio, harmonic_levels)
        rows.append(NearestReaction(reaction=reaction, nearest_level=level, distance=distance))
    return rows


def _counts_by_level(rows: Sequence[NearestReaction]) -> dict[float, int]:
    counts: dict[float, int] = defaultdict(int)
    for row in rows:
        counts[row.nearest_level] += 1
    return counts


def _symbol_level_ranks(rows: Sequence[NearestReaction]) -> dict[str, dict[float, int]]:
    ranks: dict[str, dict[float, int]] = {}
    for symbol in SYMBOL_ORDER:
        counts: dict[float, int] = defaultdict(int)
        for row in rows:
            if row.reaction.symbol == symbol:
                counts[row.nearest_level] += 1
        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        ranks[symbol] = {level: index + 1 for index, (level, _count) in enumerate(ordered)}
    return ranks


def _proximity_score(
    level: float,
    avg_distance: float,
    harmonic_levels: Sequence[float],
) -> float:
    if not math.isfinite(avg_distance):
        return math.nan
    lower, upper = _level_boundaries(harmonic_levels)[level]
    finite_upper = upper if math.isfinite(upper) else level + (level - lower)
    half_width = max(level - lower, finite_upper - level)
    if half_width <= 0:
        return math.nan
    return max(0.0, 1.0 - (avg_distance / half_width))


def summary_rows(
    rows: Sequence[NearestReaction], harmonic_levels: Sequence[float]
) -> list[dict[str, Any]]:
    total = len(rows)
    cluster_rows = cluster_strength_rows(rows, harmonic_levels)
    score_by_level = {
        float(row["harmonic_level"]): row["cluster_strength_score"] for row in cluster_rows
    }
    out: list[dict[str, Any]] = []
    for level in harmonic_levels:
        level_rows = [row for row in rows if row.nearest_level == level]
        distances = [row.distance for row in level_rows]
        out.append(
            {
                "harmonic_level": _format_number(level),
                "count": len(level_rows),
                "percentage": _format_number((len(level_rows) / total) * 100.0 if total else math.nan),
                "average_distance": _format_number(_mean(distances)),
                "median_distance": _format_number(_median(distances)),
                "cluster_strength_score": _format_number(score_by_level.get(level, math.nan)),
            }
        )
    return out


def distribution_rows(rows: Sequence[NearestReaction]) -> list[dict[str, Any]]:
    ratios = [row.reaction.raw_ratio for row in rows]
    total = len(ratios)
    out: list[dict[str, Any]] = []
    for bucket, lower, upper in DISTRIBUTION_BUCKETS:
        in_bucket = [ratio for ratio in ratios if lower <= ratio < upper]
        out.append(
            {
                "bucket": bucket,
                "count": len(in_bucket),
                "percentage": _format_number((len(in_bucket) / total) * 100.0 if total else math.nan),
                "min_ratio": _format_number(min(in_bucket) if in_bucket else math.nan),
                "max_ratio": _format_number(max(in_bucket) if in_bucket else math.nan),
            }
        )
    return out


def symbol_breakdown_rows(
    rows: Sequence[NearestReaction], harmonic_levels: Sequence[float]
) -> list[dict[str, Any]]:
    ranks = _symbol_level_ranks(rows)
    out: list[dict[str, Any]] = []
    for symbol in SYMBOL_ORDER:
        symbol_rows = [row for row in rows if row.reaction.symbol == symbol]
        total = len(symbol_rows)
        for level in harmonic_levels:
            level_rows = [row for row in symbol_rows if row.nearest_level == level]
            distances = [row.distance for row in level_rows]
            out.append(
                {
                    "symbol": symbol,
                    "harmonic_level": _format_number(level),
                    "count": len(level_rows),
                    "percentage_within_symbol": _format_number((len(level_rows) / total) * 100.0 if total else math.nan),
                    "average_distance": _format_number(_mean(distances)),
                    "median_distance": _format_number(_median(distances)),
                    "symbol_rank": ranks.get(symbol, {}).get(level, ""),
                }
            )
    return out


def cluster_strength_rows(
    rows: Sequence[NearestReaction], harmonic_levels: Sequence[float]
) -> list[dict[str, Any]]:
    total = len(rows)
    ratios = [row.reaction.raw_ratio for row in rows]
    expected_frequencies = _uniform_expected_frequencies(
        harmonic_levels=harmonic_levels,
        observed_min=min(ratios) if ratios else math.nan,
        observed_max=max(ratios) if ratios else math.nan,
    )
    counts = _counts_by_level(rows)
    symbol_ranks = _symbol_level_ranks(rows)

    unranked: list[dict[str, Any]] = []
    for level in harmonic_levels:
        level_rows = [row for row in rows if row.nearest_level == level]
        distances = [row.distance for row in level_rows]
        observed_count = counts.get(level, 0)
        observed_frequency = observed_count / total if total else math.nan
        expected_frequency = expected_frequencies.get(level, math.nan)
        expected_count = expected_frequency * total if math.isfinite(expected_frequency) else math.nan
        enrichment = (
            observed_frequency / expected_frequency
            if expected_frequency and math.isfinite(expected_frequency)
            else math.nan
        )
        avg_distance = _mean(distances)
        proximity = _proximity_score(level, avg_distance, harmonic_levels)
        if observed_count == 0:
            cluster_strength = 0.0
        else:
            cluster_strength = (
                enrichment * proximity
                if math.isfinite(enrichment) and math.isfinite(proximity)
                else math.nan
            )
        symbol_counts = {
            symbol: sum(
                1
                for row in rows
                if row.reaction.symbol == symbol and row.nearest_level == level
            )
            for symbol in SYMBOL_ORDER
        }
        top_symbol_count = max(symbol_counts.values()) if symbol_counts else 0
        all_symbol_count = sum(symbol_counts.values())
        symbol_specific = (
            all_symbol_count > 0
            and top_symbol_count / all_symbol_count >= 0.70
            and sum(1 for count in symbol_counts.values() if count > 0) < len(SYMBOL_ORDER)
        )
        unranked.append(
            {
                "harmonic_level": _format_number(level),
                "rank": 0,
                "observed_frequency": _format_number(observed_frequency),
                "expected_frequency": _format_number(expected_frequency),
                "observed_count": observed_count,
                "expected_count": _format_number(expected_count),
                "enrichment_factor": _format_number(enrichment),
                "average_distance": _format_number(avg_distance),
                "median_distance": _format_number(_median(distances)),
                "proximity_score": _format_number(proximity),
                "cluster_strength_score": _format_number(cluster_strength),
                "btc_count": symbol_counts["BTC"],
                "eth_count": symbol_counts["ETH"],
                "sol_count": symbol_counts["SOL"],
                "btc_rank": symbol_ranks.get("BTC", {}).get(level, ""),
                "eth_rank": symbol_ranks.get("ETH", {}).get(level, ""),
                "sol_rank": symbol_ranks.get("SOL", {}).get(level, ""),
                "dominates_all_three_symbols": all(
                    symbol_ranks.get(symbol, {}).get(level) == 1 for symbol in SYMBOL_ORDER
                ),
                "symbol_specific_behavior": symbol_specific,
                        "_sort_score": (
                    cluster_strength if math.isfinite(cluster_strength) else -math.inf
                ),
                "_sort_observed_count": observed_count,
            }
        )

    ranked = sorted(
        unranked,
        key=lambda row: (
            -float(row["_sort_score"]),
            -int(row["_sort_observed_count"]),
            float(row["harmonic_level"]),
        ),
    )
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
        del row["_sort_score"]
        del row["_sort_observed_count"]
    return ranked


def nearest_level_rows(rows: Sequence[NearestReaction]) -> list[dict[str, Any]]:
    return [
        {
            "reaction_id": row.reaction.reaction_id,
            "threshold_name": row.reaction.threshold_name,
            "symbol": row.reaction.symbol,
            "direction": row.reaction.direction,
            "active_direction": row.reaction.active_direction,
            "candidate_direction": row.reaction.candidate_direction,
            "reaction_kind": row.reaction.reaction_kind,
            "raw_ratio": _format_number(row.reaction.raw_ratio),
            "nearest_harmonic_level": _format_number(row.nearest_level),
            "distance_from_nearest_level": _format_number(row.distance),
            "knowledge_time": row.reaction.knowledge_time,
            "candidate_boxes": _format_number(row.reaction.candidate_boxes),
            "active_swing_boxes": _format_number(row.reaction.active_swing_boxes),
            "column_id": row.reaction.column_id,
            "active_start_ts": row.reaction.active_start_ts,
            "active_end_ts": row.reaction.active_end_ts,
        }
        for row in rows
    ]


def _write_csv(path: Path, fields: Sequence[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run_audit(
    *,
    reactions_input: Sequence[str | Path],
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    threshold_name: str = DEFAULT_THRESHOLD_NAME,
    harmonic_levels: Sequence[float] = HARMONIC_LEVELS,
) -> dict[str, Any]:
    """Run the research-only SLOW reaction-ratio stability audit."""

    reactions = load_reactions(reactions_input, threshold_name=threshold_name)
    nearest = nearest_reactions(reactions, harmonic_levels)

    output_path = Path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)
    _write_csv(
        output_path / OUTPUT_SUMMARY,
        SUMMARY_FIELDS,
        summary_rows(nearest, harmonic_levels),
    )
    _write_csv(
        output_path / OUTPUT_DISTRIBUTION,
        DISTRIBUTION_FIELDS,
        distribution_rows(nearest),
    )
    _write_csv(
        output_path / OUTPUT_SYMBOL_BREAKDOWN,
        SYMBOL_BREAKDOWN_FIELDS,
        symbol_breakdown_rows(nearest, harmonic_levels),
    )
    _write_csv(
        output_path / OUTPUT_CLUSTER_STRENGTH,
        CLUSTER_STRENGTH_FIELDS,
        cluster_strength_rows(nearest, harmonic_levels),
    )
    _write_csv(
        output_path / OUTPUT_NEAREST_LEVEL,
        NEAREST_LEVEL_FIELDS,
        nearest_level_rows(nearest),
    )

    return {
        "input_reactions": len(reactions),
        "threshold_name": threshold_name,
        "output_root": str(output_path),
        "output_files": [str(output_path / name) for name in OUTPUT_NAMES],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only SLOW harmonic reaction-ratio stability audit."
    )
    parser.add_argument(
        "--reactions-input",
        nargs="+",
        default=[str(DEFAULT_REACTIONS_INPUT)],
        help="CSV export(s) from pnf_harmonic_swing_threshold_audit.py.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Directory where audit CSV outputs will be written.",
    )
    parser.add_argument(
        "--threshold-name",
        default=DEFAULT_THRESHOLD_NAME,
        help="Threshold universe to audit; defaults to SLOW.",
    )
    parser.add_argument(
        "--harmonic-levels",
        nargs="*",
        type=float,
        default=list(HARMONIC_LEVELS),
        help="Declared harmonic levels used for nearest-level clustering.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = run_audit(
        reactions_input=args.reactions_input,
        output_root=args.output_root,
        threshold_name=args.threshold_name,
        harmonic_levels=tuple(args.harmonic_levels),
    )
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
