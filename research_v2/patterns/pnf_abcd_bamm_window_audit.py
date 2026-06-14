"""Research-only AB=CD BAMM window audit.

This module measures the chronology window between an observed B-break and the
projected-D / PRZ confirmation using existing runtime inputs only.

Guardrails:
- Do not create or reconstruct ABCDs.
- Do not use FAST artifacts.
- Do not create strategy, expectancy, PnL, entries, exits, stops, or targets.
- Do not make profitability or trading conclusions.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
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
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_bamm_window_local_v1")
EXPECTED_GEOMETRY_ROWS = 7823

CLASSIFICATIONS = (
    "VALID_BAMM_WINDOW",
    "SAME_TIME_AS_D",
    "AFTER_D",
    "NO_B_BREAK_BEFORE_D",
    "UNRESOLVED",
)

SUMMARY_FIELDS = [
    "geometry_rows_loaded",
    "valid_bamm_window_count",
    "valid_bamm_window_pct",
    "same_time_as_d_count",
    "same_time_as_d_pct",
    "after_d_count",
    "after_d_pct",
    "no_b_break_before_d_count",
    "no_b_break_before_d_pct",
    "unresolved_count",
    "unresolved_pct",
    "median_columns_from_b_break_to_d",
    "median_time_from_b_break_to_d_seconds",
    "median_time_from_b_break_to_d",
    "enough_valid_pre_d_population_for_next_phase",
]
GROUP_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]
SAMPLE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "classification",
    "b_break_time",
    "d_confirmation_time",
    "b_break_column_id",
    "d_confirmation_column_id",
    "columns_from_b_break_to_d",
    "time_from_b_break_to_d_seconds",
    "time_from_b_break_to_d",
    "b_break_before_d_confirmation",
    "stalled_after_b_break_before_d",
    "stall_time",
    "stall_column_id",
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
    column_sort: int | None


@dataclass(frozen=True)
class WindowOutcome:
    candidate: GeometryCandidate
    classification: str
    b_break: Reaction | None
    d_confirmation: Reaction | None
    stall: Reaction | None


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


def _column_sort(value: Any) -> int | None:
    text = _normalize_text(value)
    if not text:
        return None
    numeric = _parse_float(text)
    if numeric is not None:
        return int(numeric)
    digits = "".join(char for char in text if char.isdigit())
    return int(digits) if digits else None


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


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def _fmt(value: float | int | str | None) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    return f"{value:.10f}".rstrip("0").rstrip(".")


def _fmt_pct(count: int, total: int) -> str:
    return _fmt((count / total) * 100) if total else "0"


def _fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return ""
    total = int(round(seconds))
    days, rem = divmod(total, 86_400)
    hours, rem = divmod(rem, 3_600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def load_geometry_candidates(path: Path, expected_rows: int) -> list[GeometryCandidate]:
    rows, _fields = _read_csv(path)
    if len(rows) != expected_rows:
        raise SystemExit(
            f"Geometry candidate row validation failed: loaded {len(rows)}, expected {expected_rows}"
        )
    candidates: list[GeometryCandidate] = []
    for row_number, row in enumerate(rows, start=2):
        c_time = _required(row, "c_time", row_number)
        c_ts = _parse_time(c_time)
        if c_ts is None:
            raise ValueError(f"missing or invalid c_time on row {row_number}")
        candidates.append(
            GeometryCandidate(
                candidate_id=_required(row, "candidate_id", row_number),
                symbol=_normalize_symbol(_required(row, "symbol", row_number)),
                year=_normalize_text(row.get("year")) or _year_from_time(_required(row, "d_time", row_number)),
                c_time=c_time,
                d_time=_required(row, "d_time", row_number),
                cd_direction=_normalize_direction(_required(row, "cd_direction", row_number)),
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
    return not kind or kind == "CONFIRMING"


def load_validated_reactions(path: Path) -> dict[str, list[Reaction]]:
    rows, _fields = _read_csv(path)
    by_symbol: dict[str, list[Reaction]] = defaultdict(list)
    for row_number, row in enumerate(rows, start=2):
        if not _reaction_is_eligible(row):
            continue
        symbol = _normalize_symbol(row.get("symbol"))
        direction = _normalize_direction(_first_present(row, ("candidate_direction", "direction", "leg_direction")))
        event_time = _first_present(row, ("knowledge_time", "completion_time", "confirmed_time", "pivot_time", "time"))
        event_ts = _parse_time(event_time)
        boxes = _parse_float(_first_present(row, ("candidate_boxes", "boxes")))
        if not symbol or not direction or event_ts is None or boxes is None:
            raise ValueError(f"invalid validated reaction row {row_number}")
        column_id = _first_present(row, ("column_id", "pivot_column_id"))
        by_symbol[symbol].append(Reaction(symbol, direction, event_time, event_ts, boxes, column_id, _column_sort(column_id)))
    for symbol in by_symbol:
        by_symbol[symbol].sort(key=lambda reaction: (reaction.event_ts, reaction.column_sort or -1, reaction.column_id))
    return dict(by_symbol)


def _first_reaction_at_or_after(reactions: Sequence[Reaction], *, start_ts: float, direction: str, minimum_boxes: float) -> Reaction | None:
    for reaction in reactions:
        if reaction.event_ts < start_ts:
            continue
        if reaction.direction == direction and reaction.candidate_boxes >= minimum_boxes:
            return reaction
    return None


def _first_stall_between(reactions: Sequence[Reaction], *, start: Reaction | None, end: Reaction | None, cd_direction: str) -> Reaction | None:
    if start is None or end is None or start.event_ts >= end.event_ts:
        return None
    for reaction in reactions:
        if reaction.event_ts <= start.event_ts or reaction.event_ts >= end.event_ts:
            continue
        if reaction.direction != cd_direction:
            return reaction
    return None


def classify_window(candidate: GeometryCandidate, reactions: Sequence[Reaction]) -> WindowOutcome:
    b_break = _first_reaction_at_or_after(reactions, start_ts=candidate.c_ts, direction=candidate.cd_direction, minimum_boxes=candidate.bc_boxes)
    d_confirmation = _first_reaction_at_or_after(reactions, start_ts=candidate.c_ts, direction=candidate.cd_direction, minimum_boxes=candidate.ab_boxes)
    if b_break is None and d_confirmation is None:
        classification = "UNRESOLVED"
    elif b_break is None:
        classification = "NO_B_BREAK_BEFORE_D"
    elif d_confirmation is None:
        classification = "UNRESOLVED"
    elif b_break.event_ts < d_confirmation.event_ts:
        classification = "VALID_BAMM_WINDOW"
    elif b_break.event_ts == d_confirmation.event_ts:
        classification = "SAME_TIME_AS_D"
    else:
        classification = "AFTER_D"
    stall = _first_stall_between(reactions, start=b_break, end=d_confirmation, cd_direction=candidate.cd_direction)
    return WindowOutcome(candidate, classification, b_break, d_confirmation, stall)


def _column_distance(outcome: WindowOutcome) -> int | None:
    if outcome.b_break is None or outcome.d_confirmation is None:
        return None
    if outcome.b_break.column_sort is None or outcome.d_confirmation.column_sort is None:
        return None
    return outcome.d_confirmation.column_sort - outcome.b_break.column_sort


def _time_distance(outcome: WindowOutcome) -> float | None:
    if outcome.b_break is None or outcome.d_confirmation is None:
        return None
    return outcome.d_confirmation.event_ts - outcome.b_break.event_ts


def _population_flag(valid_count: int) -> str:
    return "YES" if valid_count >= 100 else "NO"


def summarize(outcomes: Sequence[WindowOutcome], geometry_rows_loaded: int) -> dict[str, Any]:
    counts = Counter(outcome.classification for outcome in outcomes)
    total = len(outcomes)
    valid = [outcome for outcome in outcomes if outcome.classification == "VALID_BAMM_WINDOW"]
    column_distances = [distance for outcome in valid if (distance := _column_distance(outcome)) is not None]
    time_distances = [distance for outcome in valid if (distance := _time_distance(outcome)) is not None]
    median_columns = statistics.median(column_distances) if column_distances else None
    median_seconds = statistics.median(time_distances) if time_distances else None
    return {
        "geometry_rows_loaded": geometry_rows_loaded,
        "valid_bamm_window_count": counts["VALID_BAMM_WINDOW"],
        "valid_bamm_window_pct": _fmt_pct(counts["VALID_BAMM_WINDOW"], total),
        "same_time_as_d_count": counts["SAME_TIME_AS_D"],
        "same_time_as_d_pct": _fmt_pct(counts["SAME_TIME_AS_D"], total),
        "after_d_count": counts["AFTER_D"],
        "after_d_pct": _fmt_pct(counts["AFTER_D"], total),
        "no_b_break_before_d_count": counts["NO_B_BREAK_BEFORE_D"],
        "no_b_break_before_d_pct": _fmt_pct(counts["NO_B_BREAK_BEFORE_D"], total),
        "unresolved_count": counts["UNRESOLVED"],
        "unresolved_pct": _fmt_pct(counts["UNRESOLVED"], total),
        "median_columns_from_b_break_to_d": _fmt(median_columns),
        "median_time_from_b_break_to_d_seconds": _fmt(median_seconds),
        "median_time_from_b_break_to_d": _fmt_duration(median_seconds),
        "enough_valid_pre_d_population_for_next_phase": _population_flag(counts["VALID_BAMM_WINDOW"]),
    }


def _sample_row(outcome: WindowOutcome) -> dict[str, Any]:
    candidate = outcome.candidate
    columns = _column_distance(outcome)
    seconds = _time_distance(outcome)
    return {
        "candidate_id": candidate.candidate_id,
        "symbol": candidate.symbol,
        "year": candidate.year,
        "classification": outcome.classification,
        "b_break_time": outcome.b_break.event_time if outcome.b_break else "",
        "d_confirmation_time": outcome.d_confirmation.event_time if outcome.d_confirmation else "",
        "b_break_column_id": outcome.b_break.column_id if outcome.b_break else "",
        "d_confirmation_column_id": outcome.d_confirmation.column_id if outcome.d_confirmation else "",
        "columns_from_b_break_to_d": _fmt(columns),
        "time_from_b_break_to_d_seconds": _fmt(seconds),
        "time_from_b_break_to_d": _fmt_duration(seconds),
        "b_break_before_d_confirmation": outcome.classification == "VALID_BAMM_WINDOW",
        "stalled_after_b_break_before_d": outcome.stall is not None,
        "stall_time": outcome.stall.event_time if outcome.stall else "",
        "stall_column_id": outcome.stall.column_id if outcome.stall else "",
        "c_time": candidate.c_time,
        "d_time": candidate.d_time,
        "cd_direction": candidate.cd_direction,
        "AB_boxes": _fmt(candidate.ab_boxes),
        "BC_boxes": _fmt(candidate.bc_boxes),
        "CD_boxes": _fmt(candidate.cd_boxes),
    }


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _group_rows(outcomes: Sequence[WindowOutcome], attr: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[WindowOutcome]] = defaultdict(list)
    for outcome in outcomes:
        grouped[getattr(outcome.candidate, attr)].append(outcome)
    return [{attr: group, **summarize(group_outcomes, len(group_outcomes))} for group, group_outcomes in sorted(grouped.items())]


def _markdown_group_row(row: dict[str, Any], group_field: str) -> str:
    return (
        f"| {row[group_field]} | {row['geometry_rows_loaded']} | "
        f"{row['valid_bamm_window_count']} | {row['valid_bamm_window_pct']}% | "
        f"{row['same_time_as_d_count']} | {row['same_time_as_d_pct']}% | "
        f"{row['after_d_count']} | {row['after_d_pct']}% | "
        f"{row['no_b_break_before_d_count']} | {row['no_b_break_before_d_pct']}% | "
        f"{row['unresolved_count']} | {row['unresolved_pct']}% | "
        f"{row['median_columns_from_b_break_to_d']} | {row['median_time_from_b_break_to_d']} |"
    )


def _write_report(path: Path, *, summary: dict[str, Any], by_symbol: Sequence[dict[str, Any]], by_year: Sequence[dict[str, Any]]) -> None:
    lines = [
        f"Geometry candidate rows loaded: {summary['geometry_rows_loaded']}",
        "",
        "# AB=CD BAMM Window Audit",
        "",
        "## Scope",
        "",
        "- Research-only BAMM window audit.",
        "- Loaded existing geometry candidates and validated harmonic reactions at runtime.",
        "- No local data is embedded in this script, no ABCDs are reconstructed, and no FAST artifacts are used.",
        "- No strategy, expectancy, PnL, entries, exits, stops, targets, profitability conclusion, or trading conclusion is produced.",
        "",
        "## Required Answers",
        "",
        f"1. Total geometry rows loaded: {summary['geometry_rows_loaded']}",
        f"2. Valid BAMM windows count and %: {summary['valid_bamm_window_count']} ({summary['valid_bamm_window_pct']}%)",
        f"3. Same-time-as-D count and %: {summary['same_time_as_d_count']} ({summary['same_time_as_d_pct']}%)",
        f"4. After-D count and %: {summary['after_d_count']} ({summary['after_d_pct']}%)",
        f"5. No pre-D B-break count and %: {summary['no_b_break_before_d_count']} ({summary['no_b_break_before_d_pct']}%)",
        f"6. Median columns from B-break to D: {summary['median_columns_from_b_break_to_d']}",
        f"7. Median time from B-break to D: {summary['median_time_from_b_break_to_d']} ({summary['median_time_from_b_break_to_d_seconds']} seconds)",
        f"8. Is there enough valid pre-D BAMM window population for next research phase? {summary['enough_valid_pre_d_population_for_next_phase']}.",
        "",
        "## By Symbol",
        "",
        "| Symbol | Rows | Valid | Valid % | Same-time | Same-time % | After-D | After-D % | No pre-D B-break | No pre-D B-break % | Unresolved | Unresolved % | Median columns | Median time |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines.extend(_markdown_group_row(row, "symbol") for row in by_symbol)
    lines.extend([
        "",
        "## By Year",
        "",
        "| Year | Rows | Valid | Valid % | Same-time | Same-time % | After-D | After-D % | No pre-D B-break | No pre-D B-break % | Unresolved | Unresolved % | Median columns | Median time |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    lines.extend(_markdown_group_row(row, "year") for row in by_year)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_audit(geometry_candidates_path: Path, reactions_path: Path, output_root: Path, expected_geometry_rows: int) -> None:
    candidates = load_geometry_candidates(geometry_candidates_path, expected_geometry_rows)
    reactions_by_symbol = load_validated_reactions(reactions_path)
    outcomes = [classify_window(candidate, reactions_by_symbol.get(candidate.symbol, ())) for candidate in candidates]
    summary = summarize(outcomes, len(candidates))
    by_symbol = _group_rows(outcomes, "symbol")
    by_year = _group_rows(outcomes, "year")
    sample = [_sample_row(outcome) for outcome in outcomes[:100]]

    _write_csv(output_root / "abcd_bamm_window_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_bamm_window_by_symbol.csv", by_symbol, GROUP_FIELDS)
    _write_csv(output_root / "abcd_bamm_window_by_year.csv", by_year, BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_bamm_window_sample.csv", sample, SAMPLE_FIELDS)
    _write_report(output_root / "abcd_bamm_window_report.md", summary=summary, by_symbol=by_symbol, by_year=by_year)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Research-only AB=CD BAMM window audit.")
    parser.add_argument("--geometry-candidates", type=Path, default=DEFAULT_GEOMETRY_CANDIDATES)
    parser.add_argument("--reactions", type=Path, default=DEFAULT_REACTIONS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--expected-geometry-rows", type=int, default=EXPECTED_GEOMETRY_ROWS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_audit(args.geometry_candidates, args.reactions, args.output_root, args.expected_geometry_rows)


if __name__ == "__main__":
    main()
