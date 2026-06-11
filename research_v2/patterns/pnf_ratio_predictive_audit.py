"""Research-only audit for reaction-ratio bucket predictive behavior.

This module consumes the local harmonic reaction export and asks whether SLOW
reaction-ratio buckets are followed by materially different confirmed swing
behavior. It intentionally performs no detector work, no strategy/expectancy
analysis, and no production or scanner changes.
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Sequence

DEFAULT_REACTIONS_INPUT = Path(
    "research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/"
    "harmonic_reactions_by_threshold.csv"
)
DEFAULT_OUTPUT_ROOT = Path(
    "research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit"
)
DEFAULT_THRESHOLD_NAME = "SLOW"

OUTPUT_SUMMARY = "ratio_predictive_summary.csv"
OUTPUT_REPORT = "ratio_predictive_report.md"
OUTPUT_BY_SYMBOL = "ratio_predictive_by_symbol.csv"
OUTPUT_BY_YEAR = "ratio_predictive_by_year.csv"
OUTPUT_STABILITY_REPORT = "ratio_predictive_stability_report.md"
OUTPUT_NAMES = (
    OUTPUT_SUMMARY,
    OUTPUT_REPORT,
    OUTPUT_BY_SYMBOL,
    OUTPUT_BY_YEAR,
    OUTPUT_STABILITY_REPORT,
)

DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
DEFAULT_YEARS = (2024, 2025, 2026)
MIN_ROBUST_MEASURED_OBSERVATIONS = 30

RATIO_BUCKETS = (
    ("0.20-0.30", 0.20, 0.30),
    ("0.30-0.40", 0.30, 0.40),
    ("0.40-0.50", 0.40, 0.50),
    ("0.50-0.60", 0.50, 0.60),
    ("0.60-0.70", 0.60, 0.70),
    ("0.70-0.80", 0.70, 0.80),
    ("0.90-1.10", 0.90, 1.10),
    ("1.20-1.35", 1.20, 1.35),
    ("1.55-1.70", 1.55, 1.70),
    ("2.00-2.10", 2.00, 2.10),
)

SUMMARY_FIELDS = [
    "bucket",
    "bucket_lower_inclusive",
    "bucket_upper_exclusive",
    "raw_reaction_count",
    "count",
    "avg_next_swing_size",
    "median_next_swing_size",
    "continuation_frequency",
    "reversal_frequency",
    "continuation_count",
    "reversal_count",
    "unresolved_count",
]

STABILITY_BY_SYMBOL_FIELDS = [
    "symbol",
    "bucket",
    "raw_reactions",
    "measured_count",
    "continuation_frequency",
    "reversal_frequency",
    "median_next_swing",
    "avg_next_swing",
]

STABILITY_BY_YEAR_FIELDS = [
    "year",
    "bucket",
    "raw_reactions",
    "measured_count",
    "continuation_frequency",
    "reversal_frequency",
    "median_next_swing",
]


@dataclass(frozen=True)
class Reaction:
    threshold_name: str
    symbol: str
    active_direction: str
    candidate_direction: str
    reaction_kind: str
    candidate_boxes: float
    active_swing_boxes: float
    reaction_ratio: float
    column_id: str
    completion_time: float
    active_start_ts: str
    active_end_ts: str


@dataclass(frozen=True)
class MeasuredReaction:
    reaction: Reaction
    next_confirmed: Reaction | None

    @property
    def has_next_confirmed(self) -> bool:
        return self.next_confirmed is not None

    @property
    def next_swing_size(self) -> float:
        if self.next_confirmed is None:
            return math.nan
        return self.next_confirmed.candidate_boxes

    @property
    def outcome(self) -> str:
        """Classify the next confirmed swing relative to the audited reaction.

        A continuation means the next confirmed swing is back in the original
        active-swing direction. A reversal means the next confirmed swing is in
        the reaction/candidate direction. This keeps the audit causal by using
        only confirmed swings strictly after the reaction completion time.
        """

        if self.next_confirmed is None:
            return "unresolved"
        if self.next_confirmed.candidate_direction == self.reaction.active_direction:
            return "continuation"
        if self.next_confirmed.candidate_direction == self.reaction.candidate_direction:
            return "reversal"
        return "other"


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


def _completion_sort_key(reaction: Reaction) -> tuple[float, int]:
    try:
        column_id = int(float(reaction.column_id))
    except ValueError:
        column_id = 0
    return reaction.completion_time, column_id


def normalize_symbol(symbol: str) -> str:
    """Return the canonical market symbol used by stability outputs.

    Reaction exports may carry exchange-qualified symbols such as
    ``BINANCE_FUT:BTCUSDT`` while the by-symbol stability output is keyed by
    bare symbols such as ``BTCUSDT``.  Keep the normalization intentionally
    narrow: strip only an exchange/venue prefix separated by ``:`` and preserve
    the market symbol itself.
    """

    return str(symbol or "").strip().split(":")[-1]


def load_reactions(
    path: str | Path,
    *,
    threshold_name: str = DEFAULT_THRESHOLD_NAME,
) -> list[Reaction]:
    target_threshold = threshold_name.strip().upper()
    reactions: list[Reaction] = []
    csv_path = Path(path)
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{csv_path}: expected a CSV header")
        for row_number, row in enumerate(reader, start=2):
            raw_threshold = str(row.get("threshold_name") or "").strip()
            if raw_threshold.upper() != target_threshold:
                continue
            reactions.append(
                Reaction(
                    threshold_name=raw_threshold,
                    symbol=str(row.get("symbol") or "").strip(),
                    active_direction=str(row.get("active_direction") or "").strip(),
                    candidate_direction=str(
                        row.get("candidate_direction") or ""
                    ).strip(),
                    reaction_kind=str(row.get("reaction_kind") or "").strip(),
                    candidate_boxes=_to_float(
                        row.get("candidate_boxes"),
                        label="candidate_boxes",
                        row_number=row_number,
                    ),
                    active_swing_boxes=_to_float(
                        row.get("active_swing_boxes"),
                        label="active_swing_boxes",
                        row_number=row_number,
                    ),
                    reaction_ratio=_to_float(
                        row.get("reaction_ratio"),
                        label="reaction_ratio",
                        row_number=row_number,
                    ),
                    column_id=str(row.get("column_id") or "").strip(),
                    completion_time=_to_float(
                        row.get("completion_time"),
                        label="completion_time",
                        row_number=row_number,
                    ),
                    active_start_ts=str(row.get("active_start_ts") or "").strip(),
                    active_end_ts=str(row.get("active_end_ts") or "").strip(),
                )
            )
    return reactions


def measure_next_confirmed_swings(
    reactions: Sequence[Reaction],
) -> list[MeasuredReaction]:
    confirmed_by_symbol: dict[str, list[Reaction]] = {}
    for reaction in reactions:
        if reaction.reaction_kind.upper() == "CONFIRMING":
            confirmed_by_symbol.setdefault(reaction.symbol, []).append(reaction)
    for symbol in confirmed_by_symbol:
        confirmed_by_symbol[symbol].sort(key=_completion_sort_key)

    measured: list[MeasuredReaction] = []
    for reaction in sorted(
        reactions, key=lambda item: (item.symbol, *_completion_sort_key(item))
    ):
        next_confirmed = next(
            (
                confirmed
                for confirmed in confirmed_by_symbol.get(reaction.symbol, [])
                if confirmed.completion_time > reaction.completion_time
            ),
            None,
        )
        measured.append(
            MeasuredReaction(reaction=reaction, next_confirmed=next_confirmed)
        )
    return measured


def _bucket_rows(
    rows: Sequence[MeasuredReaction], lower: float, upper: float
) -> list[MeasuredReaction]:
    return [row for row in rows if lower <= row.reaction.reaction_ratio < upper]


def _bucket_metrics(bucket_rows: Sequence[MeasuredReaction]) -> dict[str, Any]:
    resolved_rows = [row for row in bucket_rows if row.has_next_confirmed]
    next_sizes = [row.next_swing_size for row in resolved_rows]
    continuation_count = sum(
        1 for row in resolved_rows if row.outcome == "continuation"
    )
    reversal_count = sum(1 for row in resolved_rows if row.outcome == "reversal")
    count = len(resolved_rows)
    return {
        "raw_reactions": len(bucket_rows),
        "measured_count": count,
        "avg_next_swing": _format_number(mean(next_sizes) if next_sizes else math.nan),
        "median_next_swing": _format_number(
            median(next_sizes) if next_sizes else math.nan
        ),
        "continuation_frequency": _format_number(
            continuation_count / count if count else math.nan
        ),
        "reversal_frequency": _format_number(
            reversal_count / count if count else math.nan
        ),
        "continuation_count": continuation_count,
        "reversal_count": reversal_count,
        "unresolved_count": len(bucket_rows) - count,
    }


def build_summary_rows(rows: Sequence[MeasuredReaction]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for bucket, lower, upper in RATIO_BUCKETS:
        bucket_rows = _bucket_rows(rows, lower, upper)
        metrics = _bucket_metrics(bucket_rows)
        summary.append(
            {
                "bucket": bucket,
                "bucket_lower_inclusive": _format_number(lower),
                "bucket_upper_exclusive": _format_number(upper),
                "raw_reaction_count": metrics["raw_reactions"],
                "count": metrics["measured_count"],
                "avg_next_swing_size": metrics["avg_next_swing"],
                "median_next_swing_size": metrics["median_next_swing"],
                "continuation_frequency": metrics["continuation_frequency"],
                "reversal_frequency": metrics["reversal_frequency"],
                "continuation_count": metrics["continuation_count"],
                "reversal_count": metrics["reversal_count"],
                "unresolved_count": metrics["unresolved_count"],
            }
        )
    return summary


def _completion_year(completion_time: float) -> int | None:
    timestamp = (
        completion_time / 1000 if completion_time > 10_000_000_000 else completion_time
    )
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).year
    except (OverflowError, OSError, ValueError):
        return None


def build_symbol_rows(
    rows: Sequence[MeasuredReaction],
    *,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for symbol in symbols:
        normalized_symbol = normalize_symbol(symbol)
        symbol_rows = [
            row
            for row in rows
            if normalize_symbol(row.reaction.symbol) == normalized_symbol
        ]
        for bucket, lower, upper in RATIO_BUCKETS:
            metrics = _bucket_metrics(_bucket_rows(symbol_rows, lower, upper))
            output.append(
                {
                    "symbol": symbol,
                    "bucket": bucket,
                    "raw_reactions": metrics["raw_reactions"],
                    "measured_count": metrics["measured_count"],
                    "continuation_frequency": metrics["continuation_frequency"],
                    "reversal_frequency": metrics["reversal_frequency"],
                    "median_next_swing": metrics["median_next_swing"],
                    "avg_next_swing": metrics["avg_next_swing"],
                }
            )
    return output


def build_year_rows(
    rows: Sequence[MeasuredReaction],
    *,
    years: Sequence[int] = DEFAULT_YEARS,
) -> list[dict[str, Any]]:
    rows_by_year: dict[int, list[MeasuredReaction]] = {year: [] for year in years}
    for row in rows:
        year = _completion_year(row.reaction.completion_time)
        if year in rows_by_year:
            rows_by_year[year].append(row)

    output: list[dict[str, Any]] = []
    for year in years:
        year_rows = rows_by_year[year]
        for bucket, lower, upper in RATIO_BUCKETS:
            metrics = _bucket_metrics(_bucket_rows(year_rows, lower, upper))
            output.append(
                {
                    "year": year,
                    "bucket": bucket,
                    "raw_reactions": metrics["raw_reactions"],
                    "measured_count": metrics["measured_count"],
                    "continuation_frequency": metrics["continuation_frequency"],
                    "reversal_frequency": metrics["reversal_frequency"],
                    "median_next_swing": metrics["median_next_swing"],
                }
            )
    return output


def write_csv(
    path: Path, rows: Sequence[dict[str, Any]], fieldnames: Sequence[str]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _markdown_table(rows: Sequence[dict[str, Any]]) -> str:
    headers = [
        "Bucket",
        "Raw reactions",
        "Measured count",
        "Avg next swing",
        "Median next swing",
        "Continuation freq",
        "Reversal freq",
        "Unresolved",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join(["---"] * len(headers)) + "|",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["bucket"]),
                    str(row["raw_reaction_count"]),
                    str(row["count"]),
                    str(row["avg_next_swing_size"]),
                    str(row["median_next_swing_size"]),
                    str(row["continuation_frequency"]),
                    str(row["reversal_frequency"]),
                    str(row["unresolved_count"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _material_behavior_notes(rows: Sequence[dict[str, Any]]) -> list[str]:
    notes: list[str] = []
    measured = [row for row in rows if int(row["count"]) > 0]
    if not measured:
        return ["No requested bucket has a measurable future confirmed swing sample."]

    max_measured = max(int(row["count"]) for row in measured)
    max_raw = max(int(row["raw_reaction_count"]) for row in rows)
    if max_measured < MIN_ROBUST_MEASURED_OBSERVATIONS:
        notes.append(
            "Measured observations are sparse for robust predictive claims; "
            f"the largest requested bucket has {max_measured} measured reactions "
            f"({max_raw} raw reactions)."
        )
    else:
        notes.append(
            "Requested buckets include substantial measured samples; "
            f"the largest requested bucket has {max_measured} measured reactions "
            f"({max_raw} raw reactions), so the prior sparse-sample warning does not apply."
        )
    high_size_rows = [
        row for row in measured if str(row["avg_next_swing_size"]) not in {"", "nan"}
    ]
    if high_size_rows:
        largest = max(high_size_rows, key=lambda row: float(row["avg_next_swing_size"]))
        smallest = min(
            high_size_rows, key=lambda row: float(row["avg_next_swing_size"])
        )
        notes.append(
            f"The largest observed average next swing is bucket {largest['bucket']} "
            f"at {largest['avg_next_swing_size']} boxes, while the smallest is "
            f"bucket {smallest['bucket']} at {smallest['avg_next_swing_size']} boxes."
        )
    one_sided_sparse = [
        row
        for row in measured
        if int(row["count"]) < MIN_ROBUST_MEASURED_OBSERVATIONS
        and (
            str(row["continuation_frequency"]) == "1"
            or str(row["reversal_frequency"]) == "1"
        )
    ]
    if one_sided_sparse:
        notes.append(
            "Some buckets appear one-sided, but only in cells below the measured-count "
            f"robustness floor of {MIN_ROBUST_MEASURED_OBSERVATIONS}."
        )
    notes.append(
        "Research conclusion: treat the bucket table as descriptive evidence only; require "
        "symbol and year stability checks before using the ratio split for further research."
    )
    return notes


def _frequency(row: dict[str, Any], key: str) -> float | None:
    value = str(row.get(key, "")).strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _bucket_lookup(
    rows: Sequence[dict[str, Any]], *keys: str
) -> dict[Any, dict[str, Any]]:
    if len(keys) == 1:
        key = keys[0]
        return {row[key]: row for row in rows}
    return {tuple(row[key] for key in keys): row for row in rows}


def _boundary_summary(rows: Sequence[dict[str, Any]]) -> str:
    lookup = _bucket_lookup(rows, "bucket")
    below_buckets = [
        lookup[bucket] for bucket in ("0.20-0.30", "0.30-0.40") if bucket in lookup
    ]
    above_buckets = [
        lookup[bucket]
        for bucket in ("0.40-0.50", "0.50-0.60", "0.60-0.70", "0.70-0.80", "0.90-1.10")
        if bucket in lookup
    ]
    below_reversal = [
        _frequency(row, "reversal_frequency")
        for row in below_buckets
        if int(row.get("count", row.get("measured_count", 0)))
        >= MIN_ROBUST_MEASURED_OBSERVATIONS
    ]
    above_continuation = [
        _frequency(row, "continuation_frequency")
        for row in above_buckets
        if int(row.get("count", row.get("measured_count", 0)))
        >= MIN_ROBUST_MEASURED_OBSERVATIONS
    ]
    below_reversal = [value for value in below_reversal if value is not None]
    above_continuation = [value for value in above_continuation if value is not None]
    if not below_reversal or not above_continuation:
        return "not confirmed because one side of the 0.40 boundary lacks robust measured buckets"
    if min(below_reversal) > 0.5 and min(above_continuation) > 0.5:
        return "confirmed descriptively: sub-0.40 buckets skew reversal while 0.40+ buckets skew continuation"
    return "not confirmed: the requested reversal/continuation split does not hold across robust buckets"


def _group_boundary_status(
    rows: Sequence[dict[str, Any]],
    *,
    group_key: str,
    groups: Sequence[Any],
) -> dict[Any, str]:
    lookup = _bucket_lookup(rows, group_key, "bucket")
    statuses: dict[Any, str] = {}
    for group in groups:
        group_rows = [
            lookup[(group, bucket)]
            for bucket, _, _ in RATIO_BUCKETS
            if (group, bucket) in lookup
        ]
        normalized = [
            {
                "bucket": row["bucket"],
                "count": row["measured_count"],
                "continuation_frequency": row["continuation_frequency"],
                "reversal_frequency": row["reversal_frequency"],
            }
            for row in group_rows
        ]
        statuses[group] = _boundary_summary(normalized)
    return statuses


def write_stability_report(
    path: Path,
    *,
    summary_rows: Sequence[dict[str, Any]],
    symbol_rows: Sequence[dict[str, Any]],
    year_rows: Sequence[dict[str, Any]],
) -> None:
    max_measured = max((int(row["count"]) for row in summary_rows), default=0)
    bug_confirmed = max_measured > 2
    boundary = _boundary_summary(summary_rows)
    symbol_status = _group_boundary_status(
        symbol_rows, group_key="symbol", groups=DEFAULT_SYMBOLS
    )
    year_status = _group_boundary_status(
        year_rows, group_key="year", groups=DEFAULT_YEARS
    )
    cross_symbol = all(
        "confirmed descriptively" in status for status in symbol_status.values()
    )
    cross_year = all(
        "confirmed descriptively" in status for status in year_status.values()
    )
    robust = (
        "yes"
        if "confirmed descriptively" in boundary and cross_symbol and cross_year
        else "no"
    )

    report = [
        "# Ratio Predictive Stability Report",
        "",
        "1. **Is the report bug confirmed?** "
        + (
            "Yes. The prior narrative was stale hard-coded sparse-sample text; measured bucket counts are read from the table and the largest measured bucket is "
            f"{max_measured}, not two."
            if bug_confirmed
            else "No for this dataset: the largest measured bucket is two or fewer, but the reporting code no longer hard-codes that value."
        ),
        "",
        f"2. **Is the 0.40 regime split real?** {boundary}.",
        "",
        "3. **Is it symbol-specific or cross-symbol?** "
        + (
            "Cross-symbol in the requested symbols."
            if cross_symbol
            else "Not proven cross-symbol. Per-symbol statuses: "
            + "; ".join(
                f"{symbol}: {status}" for symbol, status in symbol_status.items()
            )
            + "."
        ),
        "",
        "4. **Is it stable across years?** "
        + (
            "Stable across 2024, 2025, and 2026."
            if cross_year
            else "Not proven stable across all requested years. Per-year statuses: "
            + "; ".join(f"{year}: {status}" for year, status in year_status.items())
            + "."
        ),
        "",
        "5. **Is this sufficiently robust to justify further harmonic research?** "
        + (
            "Yes, as research-only descriptive evidence that merits additional harmonic work."
            if robust == "yes"
            else "No. Keep it research-only until both symbol and year stability are confirmed with robust measured samples."
        ),
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(report), encoding="utf-8")


def write_report(
    path: Path,
    *,
    input_path: Path,
    threshold_name: str,
    rows: Sequence[MeasuredReaction],
    summary_rows: Sequence[dict[str, Any]],
) -> None:
    total_reactions = len(rows)
    measured_reactions = sum(1 for row in rows if row.has_next_confirmed)
    confirming_reactions = sum(
        1 for row in rows if row.reaction.reaction_kind.upper() == "CONFIRMING"
    )
    internal_reactions = total_reactions - confirming_reactions
    report = [
        "# Ratio Predictive Research Report",
        "",
        "## Scope",
        "- Research only: no detector, no strategy, no expectancy, and no production changes.",
        f"- Input: `{input_path.as_posix()}`.",
        f"- Threshold: `{threshold_name}` only.",
        "- Buckets are lower-inclusive and upper-exclusive.",
        "- Next confirmed swing is the first later same-symbol SLOW `CONFIRMING` reaction; rows without a later confirmed swing are counted as unresolved and excluded from average/median/frequency denominators.",
        "- Continuation means the next confirmed swing returns to the current active-swing direction; reversal means it confirms in the reaction/candidate direction.",
        "",
        "## Dataset Coverage",
        f"- SLOW reactions: {total_reactions}",
        f"- SLOW confirming reactions: {confirming_reactions}",
        f"- SLOW internal reactions: {internal_reactions}",
        f"- Reactions with a later confirmed swing: {measured_reactions}",
        f"- Reactions without a later confirmed swing: {total_reactions - measured_reactions}",
        "",
        "## Bucket Results",
        _markdown_table(summary_rows),
        "",
        "## Material Behavior Assessment",
    ]
    report.extend(f"- {note}" for note in _material_behavior_notes(summary_rows))
    report.extend(
        [
            "",
            "## Answer",
            "See the material behavior assessment and stability report for whether observed bucket separation is supported by the measured-count, symbol, and year checks.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(report), encoding="utf-8")


def run_audit(
    *,
    reactions_input: str | Path = DEFAULT_REACTIONS_INPUT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    threshold_name: str = DEFAULT_THRESHOLD_NAME,
) -> list[dict[str, Any]]:
    input_path = Path(reactions_input)
    output_path = Path(output_root)
    reactions = load_reactions(input_path, threshold_name=threshold_name)
    measured = measure_next_confirmed_swings(reactions)
    summary = build_summary_rows(measured)
    symbol_rows = build_symbol_rows(measured)
    year_rows = build_year_rows(measured)
    write_csv(output_path / OUTPUT_SUMMARY, summary, SUMMARY_FIELDS)
    write_csv(output_path / OUTPUT_BY_SYMBOL, symbol_rows, STABILITY_BY_SYMBOL_FIELDS)
    write_csv(output_path / OUTPUT_BY_YEAR, year_rows, STABILITY_BY_YEAR_FIELDS)
    write_report(
        output_path / OUTPUT_REPORT,
        input_path=input_path,
        threshold_name=threshold_name,
        rows=measured,
        summary_rows=summary,
    )
    write_stability_report(
        output_path / OUTPUT_STABILITY_REPORT,
        summary_rows=summary,
        symbol_rows=symbol_rows,
        year_rows=year_rows,
    )
    return summary


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reactions-input", type=Path, default=DEFAULT_REACTIONS_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--threshold-name", default=DEFAULT_THRESHOLD_NAME)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    run_audit(
        reactions_input=args.reactions_input,
        output_root=args.output_root,
        threshold_name=args.threshold_name,
    )


if __name__ == "__main__":
    main()
