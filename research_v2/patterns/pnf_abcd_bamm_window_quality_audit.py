"""Research-only AB=CD BAMM window structural quality audit.

This module consumes prior BAMM-window output, geometry candidates, and validated
harmonic reactions to classify quality between the observed B-break and the
projected-D confirmation for VALID_BAMM_WINDOW rows only.

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
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_BAMM_WINDOWS = Path(
    "research_v2/patterns/abcd_bamm_window_local_v1/abcd_bamm_window_candidates.csv"
)
DEFAULT_GEOMETRY_CANDIDATES = Path(
    "research_v2/patterns/abcd_geometry_local_v1/abcd_geometry_candidates.csv"
)
DEFAULT_REACTIONS = Path(
    "research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/"
    "harmonic_reactions_by_threshold.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_bamm_window_quality_local_v1")
EXPECTED_GEOMETRY_ROWS = 7823
EXPECTED_VALID_BAMM_WINDOWS = 1999
DEFAULT_DEEP_PULLBACK_THRESHOLD = 0.50

QUALITY_CLASSES = (
    "REACHED_D_CLEAN",
    "REACHED_D_AFTER_DEEP_PULLBACK",
    "FAILED_BEFORE_D",
    "UNRESOLVED",
)

SUMMARY_FIELDS = [
    "valid_bamm_windows_measured",
    "deep_pullback_threshold",
    "reached_d_clean_count",
    "reached_d_clean_pct",
    "reached_d_after_deep_pullback_count",
    "reached_d_after_deep_pullback_pct",
    "failed_before_d_count",
    "failed_before_d_pct",
    "unresolved_count",
    "unresolved_pct",
    "structural_quality_sufficient_for_phase_4_trade_model_research",
]
GROUP_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]
SAMPLE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "quality_classification",
    "b_break_time",
    "d_confirmation_time",
    "invalidating_reaction_time",
    "deep_pullback_time",
    "b_break_to_d_boxes",
    "deep_pullback_boxes_threshold",
    "invalidating_boxes_threshold",
    "max_adverse_boxes_before_d",
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
    cd_direction: str
    ab_boxes: float
    bc_boxes: float
    cd_boxes: float


@dataclass(frozen=True)
class BammWindow:
    candidate_id: str
    symbol: str
    year: str
    b_break_time: str
    d_confirmation_time: str
    b_break_ts: float | None
    d_confirmation_ts: float | None


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
class QualityOutcome:
    window: BammWindow
    candidate: GeometryCandidate | None
    classification: str
    invalidating_reaction: Reaction | None
    deep_pullback_reaction: Reaction | None
    max_adverse_boxes_before_d: float | None
    b_break_to_d_boxes: float | None
    deep_pullback_boxes_threshold: float | None
    invalidating_boxes_threshold: float | None


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


def _first_present(row: dict[str, Any], fields: Iterable[str]) -> str:
    for field in fields:
        value = _normalize_text(row.get(field))
        if value:
            return value
    return ""


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


def _pct_fraction(count: int, total: int) -> float:
    return count / total if total else 0.0


def _reaction_is_eligible(row: dict[str, Any]) -> bool:
    threshold = _normalize_direction(row.get("threshold_name"))
    if threshold and threshold != "SLOW":
        return False
    kind = _normalize_direction(row.get("reaction_kind"))
    return not kind or kind == "CONFIRMING"


def load_geometry_candidates(path: Path, expected_rows: int) -> dict[str, GeometryCandidate]:
    rows, _fields = _read_csv(path)
    if len(rows) != expected_rows:
        raise SystemExit(
            f"Geometry candidate row validation failed: loaded {len(rows)}, expected {expected_rows}"
        )
    candidates: dict[str, GeometryCandidate] = {}
    for row_number, row in enumerate(rows, start=2):
        candidate_id = _required(row, "candidate_id", row_number)
        candidates[candidate_id] = GeometryCandidate(
            candidate_id=candidate_id,
            symbol=_normalize_symbol(_required(row, "symbol", row_number)),
            year=_normalize_text(row.get("year")) or _year_from_time(_required(row, "d_time", row_number)),
            cd_direction=_normalize_direction(_required(row, "cd_direction", row_number)),
            ab_boxes=_required_float(row, "AB_boxes", row_number),
            bc_boxes=_required_float(row, "BC_boxes", row_number),
            cd_boxes=_required_float(row, "CD_boxes", row_number),
        )
    return candidates


def load_valid_bamm_windows(path: Path, expected_valid_windows: int) -> list[BammWindow]:
    rows, _fields = _read_csv(path)
    valid_rows = [row for row in rows if _normalize_direction(row.get("classification")) == "VALID_BAMM_WINDOW"]
    if len(valid_rows) != expected_valid_windows:
        raise SystemExit(
            "Valid BAMM window row validation failed: "
            f"loaded {len(valid_rows)}, expected {expected_valid_windows}"
        )
    windows: list[BammWindow] = []
    for row_number, row in enumerate(valid_rows, start=2):
        b_break_time = _required(row, "b_break_time", row_number)
        d_confirmation_time = _required(row, "d_confirmation_time", row_number)
        windows.append(
            BammWindow(
                candidate_id=_required(row, "candidate_id", row_number),
                symbol=_normalize_symbol(_required(row, "symbol", row_number)),
                year=_normalize_text(row.get("year")) or _year_from_time(d_confirmation_time),
                b_break_time=b_break_time,
                d_confirmation_time=d_confirmation_time,
                b_break_ts=_parse_time(b_break_time),
                d_confirmation_ts=_parse_time(d_confirmation_time),
            )
        )
    return windows


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


def classify_quality(
    window: BammWindow,
    candidate: GeometryCandidate | None,
    reactions: Sequence[Reaction],
    deep_pullback_threshold: float,
) -> QualityOutcome:
    if candidate is None or window.b_break_ts is None or window.d_confirmation_ts is None:
        return QualityOutcome(window, candidate, "UNRESOLVED", None, None, None, None, None, None)
    if window.d_confirmation_ts <= window.b_break_ts:
        return QualityOutcome(window, candidate, "UNRESOLVED", None, None, None, None, None, None)

    b_break_to_d_boxes = candidate.ab_boxes - candidate.bc_boxes
    if b_break_to_d_boxes <= 0:
        return QualityOutcome(window, candidate, "UNRESOLVED", None, None, None, None, None, None)

    deep_boxes = b_break_to_d_boxes * deep_pullback_threshold
    invalidating_boxes = b_break_to_d_boxes
    adverse_direction = "DOWN" if candidate.cd_direction == "UP" else "UP"
    adverse_reactions = [
        reaction
        for reaction in reactions
        if window.b_break_ts < reaction.event_ts < window.d_confirmation_ts
        and reaction.direction == adverse_direction
    ]
    max_adverse = max((reaction.candidate_boxes for reaction in adverse_reactions), default=0.0)
    invalidating = next(
        (reaction for reaction in adverse_reactions if reaction.candidate_boxes >= invalidating_boxes),
        None,
    )
    deep_pullback = next(
        (reaction for reaction in adverse_reactions if reaction.candidate_boxes > deep_boxes),
        None,
    )

    if invalidating is not None:
        classification = "FAILED_BEFORE_D"
    elif deep_pullback is not None:
        classification = "REACHED_D_AFTER_DEEP_PULLBACK"
    else:
        classification = "REACHED_D_CLEAN"

    return QualityOutcome(
        window,
        candidate,
        classification,
        invalidating,
        deep_pullback,
        max_adverse,
        b_break_to_d_boxes,
        deep_boxes,
        invalidating_boxes,
    )


def _phase_4_answer(counts: Counter[str], total: int) -> str:
    reached = counts["REACHED_D_CLEAN"] + counts["REACHED_D_AFTER_DEEP_PULLBACK"]
    return "YES_RESEARCH_ONLY" if total and _pct_fraction(reached, total) >= 0.50 else "NO_RESEARCH_ONLY"


def summarize(outcomes: Sequence[QualityOutcome], deep_pullback_threshold: float) -> dict[str, Any]:
    counts = Counter(outcome.classification for outcome in outcomes)
    total = len(outcomes)
    return {
        "valid_bamm_windows_measured": total,
        "deep_pullback_threshold": _fmt(deep_pullback_threshold),
        "reached_d_clean_count": counts["REACHED_D_CLEAN"],
        "reached_d_clean_pct": _fmt_pct(counts["REACHED_D_CLEAN"], total),
        "reached_d_after_deep_pullback_count": counts["REACHED_D_AFTER_DEEP_PULLBACK"],
        "reached_d_after_deep_pullback_pct": _fmt_pct(counts["REACHED_D_AFTER_DEEP_PULLBACK"], total),
        "failed_before_d_count": counts["FAILED_BEFORE_D"],
        "failed_before_d_pct": _fmt_pct(counts["FAILED_BEFORE_D"], total),
        "unresolved_count": counts["UNRESOLVED"],
        "unresolved_pct": _fmt_pct(counts["UNRESOLVED"], total),
        "structural_quality_sufficient_for_phase_4_trade_model_research": _phase_4_answer(counts, total),
    }


def _sample_row(outcome: QualityOutcome) -> dict[str, Any]:
    candidate = outcome.candidate
    return {
        "candidate_id": outcome.window.candidate_id,
        "symbol": outcome.window.symbol,
        "year": outcome.window.year,
        "quality_classification": outcome.classification,
        "b_break_time": outcome.window.b_break_time,
        "d_confirmation_time": outcome.window.d_confirmation_time,
        "invalidating_reaction_time": outcome.invalidating_reaction.event_time if outcome.invalidating_reaction else "",
        "deep_pullback_time": outcome.deep_pullback_reaction.event_time if outcome.deep_pullback_reaction else "",
        "b_break_to_d_boxes": _fmt(outcome.b_break_to_d_boxes),
        "deep_pullback_boxes_threshold": _fmt(outcome.deep_pullback_boxes_threshold),
        "invalidating_boxes_threshold": _fmt(outcome.invalidating_boxes_threshold),
        "max_adverse_boxes_before_d": _fmt(outcome.max_adverse_boxes_before_d),
        "cd_direction": candidate.cd_direction if candidate else "",
        "AB_boxes": _fmt(candidate.ab_boxes) if candidate else "",
        "BC_boxes": _fmt(candidate.bc_boxes) if candidate else "",
        "CD_boxes": _fmt(candidate.cd_boxes) if candidate else "",
    }


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _group_rows(outcomes: Sequence[QualityOutcome], attr: str, deep_pullback_threshold: float) -> list[dict[str, Any]]:
    grouped: dict[str, list[QualityOutcome]] = defaultdict(list)
    for outcome in outcomes:
        grouped[getattr(outcome.window, attr)].append(outcome)
    return [
        {attr: group, **summarize(group_outcomes, deep_pullback_threshold)}
        for group, group_outcomes in sorted(grouped.items())
    ]


def _markdown_group_row(row: dict[str, Any], group_field: str) -> str:
    return (
        f"| {row[group_field]} | {row['valid_bamm_windows_measured']} | "
        f"{row['reached_d_clean_count']} | {row['reached_d_clean_pct']}% | "
        f"{row['reached_d_after_deep_pullback_count']} | {row['reached_d_after_deep_pullback_pct']}% | "
        f"{row['failed_before_d_count']} | {row['failed_before_d_pct']}% | "
        f"{row['unresolved_count']} | {row['unresolved_pct']}% | "
        f"{row['structural_quality_sufficient_for_phase_4_trade_model_research']} |"
    )


def _write_report(
    path: Path,
    *,
    summary: dict[str, Any],
    by_symbol: Sequence[dict[str, Any]],
    by_year: Sequence[dict[str, Any]],
) -> None:
    lines = [
        "# AB=CD BAMM Window Quality Audit",
        "",
        "## Scope and Guardrails",
        "",
        "- Research-only structural quality audit for VALID_BAMM_WINDOW rows only.",
        "- Uses existing BAMM-window output, geometry candidates, and validated harmonic reactions at runtime.",
        "- Does not run local data, inspect datasets, reconstruct ABCDs, use FAST artifacts, or create strategy/trading rules.",
        "- Does not create expectancy, PnL, entries, exits, stops, targets, profitability conclusions, trade recommendations, or strategy conclusions.",
        "",
        "## Classification Definitions",
        "",
        "- REACHED_D_CLEAN: projected D reached after B-break with no deep adverse pullback.",
        "- REACHED_D_AFTER_DEEP_PULLBACK: projected D reached, but adverse movement before D exceeds threshold.",
        "- FAILED_BEFORE_D: structure invalidates before projected D.",
        "- UNRESOLVED: insufficient later validated reaction data or incomplete runtime linkage.",
        "",
        "## Required Answers",
        "",
        f"1. Valid BAMM windows measured: {summary['valid_bamm_windows_measured']}",
        f"2. REACHED_D_CLEAN count and %: {summary['reached_d_clean_count']} ({summary['reached_d_clean_pct']}%)",
        f"3. REACHED_D_AFTER_DEEP_PULLBACK count and %: {summary['reached_d_after_deep_pullback_count']} ({summary['reached_d_after_deep_pullback_pct']}%)",
        f"4. FAILED_BEFORE_D count and %: {summary['failed_before_d_count']} ({summary['failed_before_d_pct']}%)",
        f"5. UNRESOLVED count and %: {summary['unresolved_count']} ({summary['unresolved_pct']}%)",
        "6. Stability across BTCUSDT / ETHUSDT / SOLUSDT is shown in the by-symbol table below.",
        "7. Stability across 2024 / 2025 / 2026 is shown in the by-year table below.",
        "8. Is there enough structural quality to justify Phase 4 trade-model research? "
        f"{summary['structural_quality_sufficient_for_phase_4_trade_model_research']}.",
        "",
        "## By Symbol",
        "",
        "| Symbol | Valid BAMM windows | Clean | Clean % | Deep pullback | Deep pullback % | Failed | Failed % | Unresolved | Unresolved % | Phase 4 research-only answer |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    lines.extend(_markdown_group_row(row, "symbol") for row in by_symbol)
    lines.extend([
        "",
        "## By Year",
        "",
        "| Year | Valid BAMM windows | Clean | Clean % | Deep pullback | Deep pullback % | Failed | Failed % | Unresolved | Unresolved % | Phase 4 research-only answer |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ])
    lines.extend(_markdown_group_row(row, "year") for row in by_year)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_audit(
    bamm_windows_path: Path,
    geometry_candidates_path: Path,
    reactions_path: Path,
    output_root: Path,
    expected_geometry_rows: int,
    expected_valid_bamm_windows: int,
    deep_pullback_threshold: float,
) -> None:
    if deep_pullback_threshold <= 0:
        raise ValueError("deep_pullback_threshold must be positive")
    candidates = load_geometry_candidates(geometry_candidates_path, expected_geometry_rows)
    windows = load_valid_bamm_windows(bamm_windows_path, expected_valid_bamm_windows)
    reactions_by_symbol = load_validated_reactions(reactions_path)
    outcomes = [
        classify_quality(
            window,
            candidates.get(window.candidate_id),
            reactions_by_symbol.get(window.symbol, ()),
            deep_pullback_threshold,
        )
        for window in windows
    ]

    summary = summarize(outcomes, deep_pullback_threshold)
    by_symbol = _group_rows(outcomes, "symbol", deep_pullback_threshold)
    by_year = _group_rows(outcomes, "year", deep_pullback_threshold)
    sample = [_sample_row(outcome) for outcome in outcomes[:100]]

    _write_csv(output_root / "abcd_bamm_window_quality_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_bamm_window_quality_by_symbol.csv", by_symbol, GROUP_FIELDS)
    _write_csv(output_root / "abcd_bamm_window_quality_by_year.csv", by_year, BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_bamm_window_quality_sample.csv", sample, SAMPLE_FIELDS)
    _write_report(output_root / "abcd_bamm_window_quality_report.md", summary=summary, by_symbol=by_symbol, by_year=by_year)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Research-only AB=CD BAMM window quality audit.")
    parser.add_argument("--bamm-windows", type=Path, default=DEFAULT_BAMM_WINDOWS)
    parser.add_argument("--geometry-candidates", type=Path, default=DEFAULT_GEOMETRY_CANDIDATES)
    parser.add_argument("--reactions", type=Path, default=DEFAULT_REACTIONS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--expected-geometry-rows", type=int, default=EXPECTED_GEOMETRY_ROWS)
    parser.add_argument("--expected-valid-bamm-windows", type=int, default=EXPECTED_VALID_BAMM_WINDOWS)
    parser.add_argument("--deep-pullback-threshold", type=float, default=DEFAULT_DEEP_PULLBACK_THRESHOLD)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_audit(
        args.bamm_windows,
        args.geometry_candidates,
        args.reactions,
        args.output_root,
        args.expected_geometry_rows,
        args.expected_valid_bamm_windows,
        args.deep_pullback_threshold,
    )


if __name__ == "__main__":
    main()
