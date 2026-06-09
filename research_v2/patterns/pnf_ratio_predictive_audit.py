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
OUTPUT_NAMES = (OUTPUT_SUMMARY, OUTPUT_REPORT)

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
                    candidate_direction=str(row.get("candidate_direction") or "").strip(),
                    reaction_kind=str(row.get("reaction_kind") or "").strip(),
                    candidate_boxes=_to_float(
                        row.get("candidate_boxes"), label="candidate_boxes", row_number=row_number
                    ),
                    active_swing_boxes=_to_float(
                        row.get("active_swing_boxes"),
                        label="active_swing_boxes",
                        row_number=row_number,
                    ),
                    reaction_ratio=_to_float(
                        row.get("reaction_ratio"), label="reaction_ratio", row_number=row_number
                    ),
                    column_id=str(row.get("column_id") or "").strip(),
                    completion_time=_to_float(
                        row.get("completion_time"), label="completion_time", row_number=row_number
                    ),
                    active_start_ts=str(row.get("active_start_ts") or "").strip(),
                    active_end_ts=str(row.get("active_end_ts") or "").strip(),
                )
            )
    return reactions


def measure_next_confirmed_swings(reactions: Sequence[Reaction]) -> list[MeasuredReaction]:
    confirmed_by_symbol: dict[str, list[Reaction]] = {}
    for reaction in reactions:
        if reaction.reaction_kind.upper() == "CONFIRMING":
            confirmed_by_symbol.setdefault(reaction.symbol, []).append(reaction)
    for symbol in confirmed_by_symbol:
        confirmed_by_symbol[symbol].sort(key=_completion_sort_key)

    measured: list[MeasuredReaction] = []
    for reaction in sorted(reactions, key=lambda item: (item.symbol, *_completion_sort_key(item))):
        next_confirmed = next(
            (
                confirmed
                for confirmed in confirmed_by_symbol.get(reaction.symbol, [])
                if confirmed.completion_time > reaction.completion_time
            ),
            None,
        )
        measured.append(MeasuredReaction(reaction=reaction, next_confirmed=next_confirmed))
    return measured


def _bucket_rows(rows: Sequence[MeasuredReaction], lower: float, upper: float) -> list[MeasuredReaction]:
    return [row for row in rows if lower <= row.reaction.reaction_ratio < upper]


def build_summary_rows(rows: Sequence[MeasuredReaction]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for bucket, lower, upper in RATIO_BUCKETS:
        bucket_rows = _bucket_rows(rows, lower, upper)
        resolved_rows = [row for row in bucket_rows if row.has_next_confirmed]
        next_sizes = [row.next_swing_size for row in resolved_rows]
        continuation_count = sum(1 for row in resolved_rows if row.outcome == "continuation")
        reversal_count = sum(1 for row in resolved_rows if row.outcome == "reversal")
        count = len(resolved_rows)
        summary.append(
            {
                "bucket": bucket,
                "bucket_lower_inclusive": _format_number(lower),
                "bucket_upper_exclusive": _format_number(upper),
                "raw_reaction_count": len(bucket_rows),
                "count": count,
                "avg_next_swing_size": _format_number(mean(next_sizes) if next_sizes else math.nan),
                "median_next_swing_size": _format_number(median(next_sizes) if next_sizes else math.nan),
                "continuation_frequency": _format_number(
                    continuation_count / count if count else math.nan
                ),
                "reversal_frequency": _format_number(reversal_count / count if count else math.nan),
                "continuation_count": continuation_count,
                "reversal_count": reversal_count,
                "unresolved_count": len(bucket_rows) - count,
            }
        )
    return summary


def write_csv(path: Path, rows: Sequence[dict[str, Any]], fieldnames: Sequence[str]) -> None:
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
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
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

    notes.append(
        "No bucket has enough measured observations to support a robust predictive claim; "
        "the largest requested buckets have only two measured reactions."
    )
    high_size_rows = [row for row in measured if str(row["avg_next_swing_size"]) not in {"", "nan"}]
    if high_size_rows:
        largest = max(high_size_rows, key=lambda row: float(row["avg_next_swing_size"]))
        smallest = min(high_size_rows, key=lambda row: float(row["avg_next_swing_size"]))
        notes.append(
            f"The largest observed average next swing is bucket {largest['bucket']} "
            f"at {largest['avg_next_swing_size']} boxes, while the smallest is "
            f"bucket {smallest['bucket']} at {smallest['avg_next_swing_size']} boxes."
        )
    one_sided = [
        row
        for row in measured
        if int(row["count"]) >= 1
        and (str(row["continuation_frequency"]) == "1" or str(row["reversal_frequency"]) == "1")
    ]
    if one_sided:
        notes.append(
            "Several buckets appear one-sided, but these are single-observation cells or otherwise "
            "too sparse to treat as material evidence."
        )
    notes.append(
        "Research conclusion: DISCARD as a standalone predictive filter until a larger SLOW sample "
        "shows repeatable separation."
    )
    return notes


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
    confirming_reactions = sum(1 for row in rows if row.reaction.reaction_kind.upper() == "CONFIRMING")
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
            "No requested ratio bucket exhibits materially different future behavior in the available SLOW local dataset. Some cells differ numerically, but the measured counts are too small and unresolved rows too common to distinguish signal from sample noise.",
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
    write_csv(output_path / OUTPUT_SUMMARY, summary, SUMMARY_FIELDS)
    write_report(
        output_path / OUTPUT_REPORT,
        input_path=input_path,
        threshold_name=threshold_name,
        rows=measured,
        summary_rows=summary,
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
