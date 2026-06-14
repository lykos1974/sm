"""Research-only AB=CD BAMM value audit.

This module compares projected-D completion frequency for geometry candidates
that have an exported BAMM window versus geometry candidates that do not.

Guardrails:
- Do not create or reconstruct ABCDs.
- Do not use FAST artifacts.
- Do not create strategy, expectancy, PnL, entries, exits, stops, targets, or trading rules.
- Do not make profitability conclusions, trade recommendations, or strategy conclusions.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_GEOMETRY_CANDIDATES = Path(
    "research_v2/patterns/abcd_geometry_local_v1/abcd_geometry_candidates.csv"
)
DEFAULT_BAMM_WINDOWS = Path(
    "research_v2/patterns/abcd_bamm_window_local_v1/abcd_bamm_window_candidates.csv"
)
DEFAULT_REACTIONS = Path(
    "research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/"
    "harmonic_reactions_by_threshold.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_bamm_value_local_v1")
EXPECTED_GEOMETRY_ROWS = 7823
EXPECTED_BAMM_WINDOWS = 1999

BAMM_GROUP = "BAMM"
NON_BAMM_GROUP = "NON_BAMM"

SUMMARY_FIELDS = [
    "bamm_count",
    "non_bamm_count",
    "bamm_d_completion_count",
    "non_bamm_d_completion_count",
    "bamm_d_completion_pct",
    "non_bamm_d_completion_pct",
    "difference_percentage_points",
    "stable_across_symbols",
    "stable_across_years",
    "final_research_decision",
]
GROUP_FIELDS = [
    "symbol",
    *SUMMARY_FIELDS,
]
BY_YEAR_FIELDS = [
    "year",
    *SUMMARY_FIELDS,
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
class ValueOutcome:
    candidate: GeometryCandidate
    group: str
    d_completion: Reaction | None


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


def _pct(count: int, total: int) -> float:
    return (count / total) * 100 if total else 0.0


def _fmt_pct(count: int, total: int) -> str:
    return _fmt(_pct(count, total))


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


def load_bamm_candidate_ids(path: Path, expected_rows: int) -> set[str]:
    rows, _fields = _read_csv(path)
    if len(rows) != expected_rows:
        raise SystemExit(f"BAMM window row validation failed: loaded {len(rows)}, expected {expected_rows}")
    candidate_ids: set[str] = set()
    for row_number, row in enumerate(rows, start=2):
        candidate_id = _required(row, "candidate_id", row_number)
        if candidate_id in candidate_ids:
            raise ValueError(f"duplicate BAMM candidate_id on row {row_number}: {candidate_id}")
        candidate_ids.add(candidate_id)
    return candidate_ids


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
        by_symbol[symbol].append(
            Reaction(symbol, direction, event_time, event_ts, boxes, column_id, _column_sort(column_id))
        )
    for symbol in by_symbol:
        by_symbol[symbol].sort(key=lambda reaction: (reaction.event_ts, reaction.column_sort or -1, reaction.column_id))
    return dict(by_symbol)


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
        if reaction.direction == direction and reaction.candidate_boxes >= minimum_boxes:
            return reaction
    return None


def classify_value(
    candidate: GeometryCandidate,
    reactions: Sequence[Reaction],
    bamm_candidate_ids: set[str],
) -> ValueOutcome:
    group = BAMM_GROUP if candidate.candidate_id in bamm_candidate_ids else NON_BAMM_GROUP
    d_completion = _first_reaction_at_or_after(
        reactions,
        start_ts=candidate.c_ts,
        direction=candidate.cd_direction,
        minimum_boxes=candidate.ab_boxes,
    )
    return ValueOutcome(candidate, group, d_completion)


def _completion_count(outcomes: Sequence[ValueOutcome], group: str) -> int:
    return sum(1 for outcome in outcomes if outcome.group == group and outcome.d_completion is not None)


def _group_count(outcomes: Sequence[ValueOutcome], group: str) -> int:
    return sum(1 for outcome in outcomes if outcome.group == group)


def _is_stable(grouped_rows: Sequence[dict[str, Any]]) -> str:
    if not grouped_rows:
        return "NO"
    for row in grouped_rows:
        if row["bamm_count"] == 0 or row["non_bamm_count"] == 0:
            return "NO"
        if float(row["difference_percentage_points"] or 0) <= 0:
            return "NO"
    return "YES"


def _research_decision(summary: dict[str, Any]) -> str:
    if (
        float(summary["difference_percentage_points"] or 0) > 0
        and summary["stable_across_symbols"] == "YES"
        and summary["stable_across_years"] == "YES"
    ):
        return "BAMM_ADDS_INFORMATION"
    return "BAMM_DOES_NOT_ADD_INFORMATION"


def summarize(outcomes: Sequence[ValueOutcome]) -> dict[str, Any]:
    bamm_count = _group_count(outcomes, BAMM_GROUP)
    non_bamm_count = _group_count(outcomes, NON_BAMM_GROUP)
    bamm_completed = _completion_count(outcomes, BAMM_GROUP)
    non_bamm_completed = _completion_count(outcomes, NON_BAMM_GROUP)
    difference = _pct(bamm_completed, bamm_count) - _pct(non_bamm_completed, non_bamm_count)
    return {
        "bamm_count": bamm_count,
        "non_bamm_count": non_bamm_count,
        "bamm_d_completion_count": bamm_completed,
        "non_bamm_d_completion_count": non_bamm_completed,
        "bamm_d_completion_pct": _fmt_pct(bamm_completed, bamm_count),
        "non_bamm_d_completion_pct": _fmt_pct(non_bamm_completed, non_bamm_count),
        "difference_percentage_points": _fmt(difference),
        "stable_across_symbols": "",
        "stable_across_years": "",
        "final_research_decision": "",
    }


def _group_rows(outcomes: Sequence[ValueOutcome], attr: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[ValueOutcome]] = defaultdict(list)
    for outcome in outcomes:
        grouped[getattr(outcome.candidate, attr)].append(outcome)
    return [{attr: group, **summarize(group_outcomes)} for group, group_outcomes in sorted(grouped.items())]


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _markdown_group_row(row: dict[str, Any], group_field: str) -> str:
    return (
        f"| {row[group_field]} | {row['bamm_count']} | {row['non_bamm_count']} | "
        f"{row['bamm_d_completion_pct']}% | {row['non_bamm_d_completion_pct']}% | "
        f"{row['difference_percentage_points']} |"
    )


def _write_report(
    path: Path,
    *,
    summary: dict[str, Any],
    by_symbol: Sequence[dict[str, Any]],
    by_year: Sequence[dict[str, Any]],
) -> None:
    lines = [
        "# AB=CD BAMM Value Audit",
        "",
        "## Scope and Guardrails",
        "",
        "- Research-only comparison of projected-D completion frequency for BAMM versus NON_BAMM geometry candidates.",
        "- Uses existing geometry candidates, exported BAMM-window candidate IDs, and validated harmonic reactions at runtime.",
        "- Does not run local data inside this report, inspect datasets, reconstruct ABCDs, or use FAST artifacts.",
        "- Does not create strategy, entries, exits, stops, targets, PnL, expectancy, profitability conclusions, or trading conclusions.",
        "",
        "## Definitions",
        "",
        "- BAMM group: `candidate_id` is present in the BAMM-window input file.",
        "- NON_BAMM group: geometry candidate `candidate_id` is absent from the BAMM-window input file.",
        "- D completion: first eligible validated reaction at or after candidate C time with matching CD direction and boxes greater than or equal to AB boxes.",
        "- Difference in percentage points: BAMM D-completion percentage minus NON_BAMM D-completion percentage.",
        "",
        "## Required Answers",
        "",
        f"1. BAMM count: {summary['bamm_count']}",
        f"2. NON_BAMM count: {summary['non_bamm_count']}",
        f"3. BAMM D-completion %: {summary['bamm_d_completion_pct']}%",
        f"4. NON_BAMM D-completion %: {summary['non_bamm_d_completion_pct']}%",
        f"5. Difference in percentage points: {summary['difference_percentage_points']}",
        f"6. Stable across BTCUSDT / ETHUSDT / SOLUSDT? {summary['stable_across_symbols']}.",
        f"7. Stable across 2024 / 2025 / 2026? {summary['stable_across_years']}.",
        f"8. Final research decision: {summary['final_research_decision']}.",
        "",
        "## By Symbol",
        "",
        "| Symbol | BAMM count | NON_BAMM count | BAMM D-completion % | NON_BAMM D-completion % | Difference pp |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    lines.extend(_markdown_group_row(row, "symbol") for row in by_symbol)
    lines.extend(
        [
            "",
            "## By Year",
            "",
            "| Year | BAMM count | NON_BAMM count | BAMM D-completion % | NON_BAMM D-completion % | Difference pp |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    lines.extend(_markdown_group_row(row, "year") for row in by_year)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_audit(
    geometry_candidates_path: Path,
    bamm_windows_path: Path,
    reactions_path: Path,
    output_root: Path,
    expected_geometry_rows: int,
    expected_bamm_windows: int,
) -> None:
    candidates = load_geometry_candidates(geometry_candidates_path, expected_geometry_rows)
    bamm_candidate_ids = load_bamm_candidate_ids(bamm_windows_path, expected_bamm_windows)
    geometry_candidate_ids = {candidate.candidate_id for candidate in candidates}
    unknown_bamm_ids = bamm_candidate_ids - geometry_candidate_ids
    if unknown_bamm_ids:
        raise SystemExit(f"BAMM window candidate IDs missing from geometry candidates: {len(unknown_bamm_ids)}")

    reactions_by_symbol = load_validated_reactions(reactions_path)
    outcomes = [
        classify_value(candidate, reactions_by_symbol.get(candidate.symbol, ()), bamm_candidate_ids)
        for candidate in candidates
    ]
    by_symbol = _group_rows(outcomes, "symbol")
    by_year = _group_rows(outcomes, "year")
    summary = summarize(outcomes)
    summary["stable_across_symbols"] = _is_stable(by_symbol)
    summary["stable_across_years"] = _is_stable(by_year)
    summary["final_research_decision"] = _research_decision(summary)

    _write_csv(output_root / "abcd_bamm_value_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_bamm_value_by_symbol.csv", by_symbol, GROUP_FIELDS)
    _write_csv(output_root / "abcd_bamm_value_by_year.csv", by_year, BY_YEAR_FIELDS)
    _write_report(output_root / "abcd_bamm_value_report.md", summary=summary, by_symbol=by_symbol, by_year=by_year)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Research-only AB=CD BAMM value audit.")
    parser.add_argument("--geometry-candidates", type=Path, default=DEFAULT_GEOMETRY_CANDIDATES)
    parser.add_argument("--bamm-windows", type=Path, default=DEFAULT_BAMM_WINDOWS)
    parser.add_argument("--reactions", type=Path, default=DEFAULT_REACTIONS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--expected-geometry-rows", type=int, default=EXPECTED_GEOMETRY_ROWS)
    parser.add_argument("--expected-bamm-windows", type=int, default=EXPECTED_BAMM_WINDOWS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_audit(
        args.geometry_candidates,
        args.bamm_windows,
        args.reactions,
        args.output_root,
        args.expected_geometry_rows,
        args.expected_bamm_windows,
    )


if __name__ == "__main__":
    main()
