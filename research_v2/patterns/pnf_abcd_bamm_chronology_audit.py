"""Research-only AB=CD BAMM chronology audit.

This module measures chronology only. It loads an existing AB=CD geometry
candidate file and an existing validated harmonic reaction stream at runtime,
then asks whether a BAMM B-break was observable before, after, or at the same
knowable time as projected D.

Guardrails:
- Do not create or reconstruct ABCD geometry.
- Do not use FAST reactions.
- Do not create strategy, expectancy, PnL, entries, exits, stops, or targets.
- Do not classify projected-D reachability from aggregate ``CD_boxes >= AB_boxes``.
  D reach time must come from the validated reaction chronology.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_GEOMETRY_CANDIDATES = Path(
    "research_v2/patterns/abcd_geometry_local_v1/abcd_geometry_candidates.csv"
)
DEFAULT_REACTIONS = Path(
    "research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/"
    "harmonic_reactions_by_threshold.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_bamm_chronology_local_v1")
EXPECTED_GEOMETRY_ROWS = 7823
OUTCOMES = (
    "B_BEFORE_D",
    "B_AFTER_D",
    "B_SAME_AS_D",
    "INVALID_BEFORE_D",
    "UNRESOLVED",
)
SUMMARY_FIELDS = [
    "total_bamm_triggers_measured",
    "b_before_d_count",
    "b_before_d_pct",
    "b_after_d_count",
    "b_after_d_pct",
    "b_same_as_d_count",
    "b_same_as_d_pct",
    "invalid_before_d_count",
    "invalid_before_d_pct",
    "unresolved_count",
    "unresolved_pct",
]
SAMPLE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "outcome",
    "b_break_time",
    "d_reach_time",
    "invalidation_time",
    "c_time",
    "d_time",
    "cd_direction",
    "AB_boxes",
    "BC_boxes",
    "CD_boxes",
]


@dataclass(frozen=True)
class GeometryCandidate:
    candidate_id: str
    symbol: str
    year: str
    c_time: str
    d_time: str
    cd_direction: str
    ab_boxes: float
    bc_boxes: float
    cd_boxes: float
    c_ts: float


@dataclass(frozen=True)
class Reaction:
    symbol: str
    direction: str
    event_time: str
    event_ts: float
    candidate_boxes: float
    column_id: str


@dataclass(frozen=True)
class ChronologyOutcome:
    candidate: GeometryCandidate
    outcome: str
    b_break_time: str
    d_reach_time: str
    invalidation_time: str


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_symbol(value: Any) -> str:
    return _normalize_text(value).split(":")[-1]


def _normalize_direction(value: Any) -> str:
    return _normalize_text(value).upper()


def _parse_float(value: Any) -> float | None:
    try:
        parsed = float(_normalize_text(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_time(value: Any) -> float | None:
    text = _normalize_text(value)
    if not text:
        return None
    numeric = _parse_float(text)
    if numeric is not None:
        return numeric / 1000.0 if numeric > 10_000_000_000 else numeric
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _year_from_time(value: str) -> str:
    parsed = _parse_time(value)
    if parsed is None:
        return "UNKNOWN"
    return str(datetime.fromtimestamp(parsed, tz=timezone.utc).year)


def _required(row: dict[str, Any], field: str, row_number: int) -> str:
    value = _normalize_text(row.get(field))
    if not value:
        raise ValueError(f"missing {field} on row {row_number}")
    return value


def _required_float(row: dict[str, Any], field: str, row_number: int) -> float:
    parsed = _parse_float(row.get(field))
    if parsed is None:
        raise ValueError(f"missing or non-finite {field} on row {row_number}")
    return parsed


def _first_present(row: dict[str, Any], fields: Iterable[str]) -> str:
    for field in fields:
        value = _normalize_text(row.get(field))
        if value:
            return value
    return ""


def _fmt_pct(count: int, total: int) -> str:
    if total == 0:
        return "0"
    return f"{(count / total) * 100:.10f}".rstrip("0").rstrip(".")


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def load_geometry_candidates(path: Path, expected_rows: int) -> list[GeometryCandidate]:
    rows, _fields = _read_csv(path)
    if len(rows) != expected_rows:
        raise SystemExit(
            f"Geometry candidate row validation failed: loaded {len(rows)}, "
            f"expected {expected_rows}"
        )

    candidates: list[GeometryCandidate] = []
    for row_number, row in enumerate(rows, start=2):
        c_time = _required(row, "c_time", row_number)
        c_ts = _parse_time(c_time)
        if c_ts is None:
            raise ValueError(f"missing or invalid c_time on row {row_number}")
        year = _normalize_text(row.get("year")) or _year_from_time(
            _required(row, "d_time", row_number)
        )
        candidates.append(
            GeometryCandidate(
                candidate_id=_required(row, "candidate_id", row_number),
                symbol=_normalize_symbol(_required(row, "symbol", row_number)),
                year=year,
                c_time=c_time,
                d_time=_required(row, "d_time", row_number),
                cd_direction=_normalize_direction(
                    _required(row, "cd_direction", row_number)
                ),
                ab_boxes=_required_float(row, "AB_boxes", row_number),
                bc_boxes=_required_float(row, "BC_boxes", row_number),
                cd_boxes=_required_float(row, "CD_boxes", row_number),
                c_ts=c_ts,
            )
        )
    return candidates


def _reaction_is_eligible(row: dict[str, Any]) -> bool:
    threshold = _normalize_direction(row.get("threshold_name"))
    if threshold and threshold != "SLOW":
        return False
    kind = _normalize_direction(row.get("reaction_kind"))
    if kind and kind != "CONFIRMING":
        return False
    return True


def load_validated_reactions(path: Path) -> dict[str, list[Reaction]]:
    rows, _fields = _read_csv(path)
    by_symbol: dict[str, list[Reaction]] = defaultdict(list)
    for row_number, row in enumerate(rows, start=2):
        if not _reaction_is_eligible(row):
            continue
        symbol = _normalize_symbol(row.get("symbol"))
        direction = _normalize_direction(
            _first_present(row, ("candidate_direction", "direction", "leg_direction"))
        )
        event_time = _first_present(
            row,
            ("knowledge_time", "completion_time", "confirmed_time", "pivot_time", "time"),
        )
        event_ts = _parse_time(event_time)
        boxes = _parse_float(_first_present(row, ("candidate_boxes", "boxes")))
        if not symbol or not direction or event_ts is None or boxes is None:
            raise ValueError(f"invalid validated reaction row {row_number}")
        by_symbol[symbol].append(
            Reaction(
                symbol=symbol,
                direction=direction,
                event_time=event_time,
                event_ts=event_ts,
                candidate_boxes=boxes,
                column_id=_first_present(row, ("column_id", "pivot_column_id")),
            )
        )
    for symbol in by_symbol:
        by_symbol[symbol].sort(key=lambda reaction: (reaction.event_ts, reaction.column_id))
    return dict(by_symbol)


def is_bamm_trigger_candidate(candidate: GeometryCandidate) -> bool:
    """Geometry-only trigger filter; chronology is measured later from reactions."""
    return candidate.cd_boxes > candidate.bc_boxes and candidate.bc_boxes < candidate.ab_boxes


def _first_reaction_at_or_after(
    reactions: Sequence[Reaction],
    *,
    start_ts: float,
    direction: str,
    minimum_boxes: float,
) -> Reaction | None:
    for reaction in reactions:
        if reaction.event_ts < start_ts:
            continue
        if reaction.direction != direction:
            continue
        if reaction.candidate_boxes >= minimum_boxes:
            return reaction
    return None


def _first_invalidation(
    reactions: Sequence[Reaction],
    *,
    start_ts: float,
    cd_direction: str,
    before_ts: float | None,
) -> Reaction | None:
    for reaction in reactions:
        if reaction.event_ts < start_ts:
            continue
        if before_ts is not None and reaction.event_ts >= before_ts:
            continue
        if reaction.direction != cd_direction:
            return reaction
    return None


def classify_chronology(candidate: GeometryCandidate, reactions: Sequence[Reaction]) -> ChronologyOutcome:
    b_break = _first_reaction_at_or_after(
        reactions,
        start_ts=candidate.c_ts,
        direction=candidate.cd_direction,
        minimum_boxes=candidate.bc_boxes,
    )
    d_reach = _first_reaction_at_or_after(
        reactions,
        start_ts=candidate.c_ts,
        direction=candidate.cd_direction,
        minimum_boxes=candidate.ab_boxes,
    )
    invalidation = _first_invalidation(
        reactions,
        start_ts=candidate.c_ts,
        cd_direction=candidate.cd_direction,
        before_ts=d_reach.event_ts if d_reach is not None else None,
    )

    if invalidation is not None:
        outcome = "INVALID_BEFORE_D"
    elif b_break is None or d_reach is None:
        outcome = "UNRESOLVED"
    elif b_break.event_ts < d_reach.event_ts:
        outcome = "B_BEFORE_D"
    elif b_break.event_ts > d_reach.event_ts:
        outcome = "B_AFTER_D"
    else:
        outcome = "B_SAME_AS_D"

    return ChronologyOutcome(
        candidate=candidate,
        outcome=outcome,
        b_break_time=b_break.event_time if b_break is not None else "",
        d_reach_time=d_reach.event_time if d_reach is not None else "",
        invalidation_time=invalidation.event_time if invalidation is not None else "",
    )


def summarize(outcomes: Iterable[ChronologyOutcome]) -> dict[str, Any]:
    counts = Counter(outcome.outcome for outcome in outcomes)
    total = sum(counts.values())
    return {
        "total_bamm_triggers_measured": total,
        "b_before_d_count": counts["B_BEFORE_D"],
        "b_before_d_pct": _fmt_pct(counts["B_BEFORE_D"], total),
        "b_after_d_count": counts["B_AFTER_D"],
        "b_after_d_pct": _fmt_pct(counts["B_AFTER_D"], total),
        "b_same_as_d_count": counts["B_SAME_AS_D"],
        "b_same_as_d_pct": _fmt_pct(counts["B_SAME_AS_D"], total),
        "invalid_before_d_count": counts["INVALID_BEFORE_D"],
        "invalid_before_d_pct": _fmt_pct(counts["INVALID_BEFORE_D"], total),
        "unresolved_count": counts["UNRESOLVED"],
        "unresolved_pct": _fmt_pct(counts["UNRESOLVED"], total),
    }


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _outcome_sample_row(outcome: ChronologyOutcome) -> dict[str, Any]:
    candidate = outcome.candidate
    return {
        "candidate_id": candidate.candidate_id,
        "symbol": candidate.symbol,
        "year": candidate.year,
        "outcome": outcome.outcome,
        "b_break_time": outcome.b_break_time,
        "d_reach_time": outcome.d_reach_time,
        "invalidation_time": outcome.invalidation_time,
        "c_time": candidate.c_time,
        "d_time": candidate.d_time,
        "cd_direction": candidate.cd_direction,
        "AB_boxes": candidate.ab_boxes,
        "BC_boxes": candidate.bc_boxes,
        "CD_boxes": candidate.cd_boxes,
    }


def _write_report(
    path: Path,
    *,
    geometry_count: int,
    summary: dict[str, Any],
    by_symbol: Sequence[dict[str, Any]],
    by_year: Sequence[dict[str, Any]],
) -> None:
    lines = [
        f"Geometry candidate rows loaded: {geometry_count}",
        "",
        "# AB=CD BAMM Chronology Audit",
        "",
        "## Scope",
        "",
        "- Research-only chronology audit.",
        "- Loaded existing geometry candidates and validated harmonic reactions at runtime.",
        "- No new geometry, ABCD reconstruction, FAST artifacts, strategy logic, expectancy, PnL, entries, exits, stops, or targets.",
        "- Projected-D reach is classified from reaction chronology, not from aggregate `CD_boxes >= AB_boxes`.",
        "",
        "## Required Answers",
        "",
        f"1. Total BAMM triggers measured: {summary['total_bamm_triggers_measured']}",
        f"2. B_BEFORE_D count and %: {summary['b_before_d_count']} ({summary['b_before_d_pct']}%)",
        f"3. B_AFTER_D count and %: {summary['b_after_d_count']} ({summary['b_after_d_pct']}%)",
        f"4. B_SAME_AS_D count and %: {summary['b_same_as_d_count']} ({summary['b_same_as_d_pct']}%)",
        f"5. INVALID_BEFORE_D count and %: {summary['invalid_before_d_count']} ({summary['invalid_before_d_pct']}%)",
        f"6. UNRESOLVED count and %: {summary['unresolved_count']} ({summary['unresolved_pct']}%)",
        "7. Is BAMM observable before D often enough to justify outcome research? Review `B_BEFORE_D` frequency only; this report makes no profitability or trading recommendation.",
        "",
        "## By Symbol",
        "",
        "| Symbol | Total | B_BEFORE_D | B_BEFORE_D % | B_AFTER_D | B_AFTER_D % | B_SAME_AS_D | B_SAME_AS_D % | INVALID_BEFORE_D | INVALID_BEFORE_D % | UNRESOLVED | UNRESOLVED % |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines.extend(_markdown_group_row(row, "symbol") for row in by_symbol)
    lines.extend(
        [
            "",
            "## By Year",
            "",
            "| Year | Total | B_BEFORE_D | B_BEFORE_D % | B_AFTER_D | B_AFTER_D % | B_SAME_AS_D | B_SAME_AS_D % | INVALID_BEFORE_D | INVALID_BEFORE_D % | UNRESOLVED | UNRESOLVED % |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    lines.extend(_markdown_group_row(row, "year") for row in by_year)
    lines.extend(
        [
            "",
            "## Research Guardrail",
            "",
            "No profitability conclusion, expectancy conclusion, strategy recommendation, or trade-simulation claim is made.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _markdown_group_row(row: dict[str, Any], group_field: str) -> str:
    return (
        f"| {row[group_field]} | {row['total_bamm_triggers_measured']} | "
        f"{row['b_before_d_count']} | {row['b_before_d_pct']} | "
        f"{row['b_after_d_count']} | {row['b_after_d_pct']} | "
        f"{row['b_same_as_d_count']} | {row['b_same_as_d_pct']} | "
        f"{row['invalid_before_d_count']} | {row['invalid_before_d_pct']} | "
        f"{row['unresolved_count']} | {row['unresolved_pct']} |"
    )


def _group_rows(outcomes: Sequence[ChronologyOutcome], attr: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[ChronologyOutcome]] = defaultdict(list)
    for outcome in outcomes:
        grouped[getattr(outcome.candidate, attr)].append(outcome)
    return [
        {attr: group, **summarize(group_outcomes)}
        for group, group_outcomes in sorted(grouped.items())
    ]


def run_audit(
    geometry_candidates_path: Path,
    reactions_path: Path,
    output_root: Path,
    expected_geometry_rows: int,
) -> None:
    candidates = load_geometry_candidates(geometry_candidates_path, expected_geometry_rows)
    reactions_by_symbol = load_validated_reactions(reactions_path)
    bamm_candidates = [candidate for candidate in candidates if is_bamm_trigger_candidate(candidate)]
    outcomes = [
        classify_chronology(candidate, reactions_by_symbol.get(candidate.symbol, ()))
        for candidate in bamm_candidates
    ]

    summary = summarize(outcomes)
    by_symbol = _group_rows(outcomes, "symbol")
    by_year = _group_rows(outcomes, "year")
    sample = [_outcome_sample_row(outcome) for outcome in outcomes[:100]]

    _write_csv(output_root / "abcd_bamm_chronology_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_bamm_chronology_by_symbol.csv", by_symbol, ["symbol", *SUMMARY_FIELDS])
    _write_csv(output_root / "abcd_bamm_chronology_by_year.csv", by_year, ["year", *SUMMARY_FIELDS])
    _write_csv(output_root / "abcd_bamm_chronology_sample.csv", sample, SAMPLE_FIELDS)
    _write_report(
        output_root / "abcd_bamm_chronology_report.md",
        geometry_count=len(candidates),
        summary=summary,
        by_symbol=by_symbol,
        by_year=by_year,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Research-only AB=CD BAMM chronology audit.")
    parser.add_argument("--geometry-candidates", type=Path, default=DEFAULT_GEOMETRY_CANDIDATES)
    parser.add_argument("--reactions", type=Path, default=DEFAULT_REACTIONS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--expected-geometry-rows", type=int, default=EXPECTED_GEOMETRY_ROWS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_audit(
        geometry_candidates_path=args.geometry_candidates,
        reactions_path=args.reactions,
        output_root=args.output_root,
        expected_geometry_rows=args.expected_geometry_rows,
    )


if __name__ == "__main__":
    main()
