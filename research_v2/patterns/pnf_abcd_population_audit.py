"""Phase 1 research-only population audit for PnF AB/CD structures.

This module implements only the population-counting phase described by
``pnf_abcd_symmetry_audit_design_v2.md``. It does not classify symmetry, measure
AB/CD ratios, use harmonic levels, use the 0.40 split, compute expectancy, create
detectors, scanners, strategies, signals, or touch production trading code.
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

TRUSTED_INPUT_ROOT = Path("research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v2")
DESIGN_DOC = Path("research_v2/patterns/pnf_abcd_symmetry_audit_design_v2.md")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_population_local_v1")
REACTIONS_FILENAME = "harmonic_reactions_by_threshold.csv"
THRESHOLD_NAME = "SLOW"
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
YEARS = (2024, 2025, 2026)
DIRECTIONS = {"UP", "DOWN"}

SUMMARY_FIELDS = [
    "symbol",
    "year",
    "ab_waiting_for_c",
    "abc_waiting_for_d",
    "invalidated_abc",
    "completed_abcd",
]
BY_SYMBOL_FIELDS = [
    "symbol",
    "ab_waiting_for_c",
    "abc_waiting_for_d",
    "invalidated_abc",
    "completed_abcd",
]
BY_YEAR_FIELDS = [
    "year",
    "ab_waiting_for_c",
    "abc_waiting_for_d",
    "invalidated_abc",
    "completed_abcd",
]
COUNT_KEYS = (
    "ab_waiting_for_c",
    "abc_waiting_for_d",
    "invalidated_abc",
    "completed_abcd",
)


@dataclass(frozen=True)
class Pivot:
    pivot_id: str
    source_row: int
    symbol: str
    candidate_direction: str
    candidate_boxes: float
    knowledge_time: str
    knowledge_ts: float
    completion_time: str
    completion_ts: float
    column_id: str
    column_sort: int


@dataclass(frozen=True)
class Event:
    state: str
    symbol: str
    year: int | None
    pivot_ids: tuple[str, ...]
    knowledge_time: str

    def key(self) -> tuple[Any, ...]:
        return (self.state, self.symbol, self.year, self.pivot_ids, self.knowledge_time)


def _empty_counts() -> dict[str, int]:
    return {key: 0 for key in COUNT_KEYS}


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().split(":")[-1]


def _parse_float(value: Any) -> float | None:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_time(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    numeric = _parse_float(text)
    if numeric is not None:
        return numeric / 1000.0 if numeric > 10_000_000_000 else numeric
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _year_from_ts(timestamp: float) -> int | None:
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).year
    except (OverflowError, OSError, ValueError):
        return None


def _column_sort(value: Any) -> int:
    parsed = _parse_float(value)
    return int(parsed) if parsed is not None else 0


def _trusted_reactions_path(input_root: Path) -> Path | None:
    candidates = (
        input_root / REACTIONS_FILENAME,
        input_root / "audit" / REACTIONS_FILENAME,
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_blocked_outputs(output_root: Path, *, reason: str, input_root: Path) -> None:
    _write_csv(output_root / "abcd_population_summary.csv", [], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_population_by_symbol.csv", [], BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_population_by_year.csv", [], BY_YEAR_FIELDS)
    report = [
        "# AB/CD Population Audit Report",
        "",
        "## Status",
        "BLOCKED — trusted validated input artifact is not available in this workspace.",
        "",
        "## Reason",
        reason,
        "",
        "## Trusted Input Requirement",
        f"- Required root: `{input_root.as_posix()}`",
        f"- Required file: `{REACTIONS_FILENAME}` under the trusted root or its `audit/` child.",
        "- No fallback to non-validated local artifacts was used.",
        "- No `completion_time` fallback was used for `knowledge_time`.",
        "",
        "## Answers",
        "1. **Total population sizes.** Not computed because the trusted validated input is missing.",
        "2. **Are completed ABCD structures rare or abundant?** Not determined.",
        "3. **Is population stable across symbols?** Not determined.",
        "4. **Is population stable across years?** Not determined.",
        "5. **Is sample size large enough to justify Phase 2 symmetry research?** Not determined from this workspace.",
        "",
        "## Research Guardrail",
        "Population audit only. No symmetry classification, AB/CD ratio measurement, harmonic-level usage, 0.40 split usage, expectancy, detector, scanner, strategy, entry/exit, trade, or production-code behavior was performed.",
        "",
    ]
    (output_root / "abcd_population_report.md").write_text("\n".join(report), encoding="utf-8")


def load_validated_pivots(input_root: Path) -> tuple[list[Pivot], dict[str, int], Path]:
    reactions_path = _trusted_reactions_path(input_root)
    if reactions_path is None:
        raise FileNotFoundError(
            f"missing trusted {REACTIONS_FILENAME} under {input_root} or {input_root / 'audit'}"
        )

    rejects = {
        "non_slow_or_non_confirming": 0,
        "untrusted_symbol": 0,
        "invalid_candidate_direction": 0,
        "missing_or_invalid_knowledge_time": 0,
        "missing_or_invalid_candidate_boxes": 0,
    }
    pivots: list[Pivot] = []

    with reactions_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{reactions_path}: expected CSV header")
        if "knowledge_time" not in reader.fieldnames:
            raise ValueError(
                f"{reactions_path}: missing required knowledge_time column; completion_time fallback is forbidden"
            )
        for row_number, row in enumerate(reader, start=2):
            threshold = str(row.get("threshold_name") or "").strip().upper()
            kind = str(row.get("reaction_kind") or "").strip().upper()
            if threshold != THRESHOLD_NAME or kind != "CONFIRMING":
                rejects["non_slow_or_non_confirming"] += 1
                continue

            symbol = _normalize_symbol(str(row.get("symbol") or ""))
            if symbol not in SYMBOLS:
                rejects["untrusted_symbol"] += 1
                continue

            direction = str(row.get("candidate_direction") or "").strip().upper()
            if direction not in DIRECTIONS:
                rejects["invalid_candidate_direction"] += 1
                continue

            knowledge_time = str(row.get("knowledge_time") or "").strip()
            knowledge_ts = _parse_time(knowledge_time)
            if knowledge_ts is None:
                rejects["missing_or_invalid_knowledge_time"] += 1
                continue

            boxes = _parse_float(row.get("candidate_boxes"))
            if boxes is None or boxes <= 0:
                rejects["missing_or_invalid_candidate_boxes"] += 1
                continue

            completion_time = str(row.get("completion_time") or "").strip()
            completion_ts = _parse_time(completion_time)
            column_id = str(row.get("column_id") or "").strip()
            pivot_id = f"{symbol}:{row_number}:{knowledge_time}:{column_id}:{direction}"
            pivots.append(
                Pivot(
                    pivot_id=pivot_id,
                    source_row=row_number,
                    symbol=symbol,
                    candidate_direction=direction,
                    candidate_boxes=boxes,
                    knowledge_time=knowledge_time,
                    knowledge_ts=knowledge_ts,
                    completion_time=completion_time,
                    completion_ts=completion_ts if completion_ts is not None else 0.0,
                    column_id=column_id,
                    column_sort=_column_sort(column_id),
                )
            )

    return pivots, rejects, reactions_path


def _sort_pivots(pivots: Iterable[Pivot]) -> list[Pivot]:
    return sorted(
        pivots,
        key=lambda pivot: (
            pivot.knowledge_ts,
            pivot.completion_ts,
            pivot.column_sort,
            pivot.source_row,
        ),
    )


def _valid_abc(a: Pivot, b: Pivot, c: Pivot) -> bool:
    del a
    return b.candidate_direction != c.candidate_direction


def _completed_abcd(a: Pivot, b: Pivot, c: Pivot, d: Pivot) -> bool:
    del a
    return (
        b.candidate_direction != c.candidate_direction
        and c.candidate_direction != d.candidate_direction
        and b.candidate_direction == d.candidate_direction
    )


def batch_events(pivots: Sequence[Pivot]) -> list[Event]:
    events: list[Event] = []
    by_symbol = {symbol: [] for symbol in SYMBOLS}
    for pivot in pivots:
        by_symbol.setdefault(pivot.symbol, []).append(pivot)

    for symbol in SYMBOLS:
        ordered = _sort_pivots(by_symbol.get(symbol, []))
        for index in range(len(ordered) - 1):
            a, b = ordered[index], ordered[index + 1]
            events.append(
                Event(
                    state="AB_WAITING_FOR_C",
                    symbol=symbol,
                    year=_year_from_ts(b.knowledge_ts),
                    pivot_ids=(a.pivot_id, b.pivot_id),
                    knowledge_time=b.knowledge_time,
                )
            )
        for index in range(len(ordered) - 2):
            a, b, c = ordered[index], ordered[index + 1], ordered[index + 2]
            if _valid_abc(a, b, c):
                events.append(
                    Event(
                        state="ABC_WAITING_FOR_D",
                        symbol=symbol,
                        year=_year_from_ts(c.knowledge_ts),
                        pivot_ids=(a.pivot_id, b.pivot_id, c.pivot_id),
                        knowledge_time=c.knowledge_time,
                    )
                )
        for index in range(len(ordered) - 3):
            a, b, c, d = (
                ordered[index],
                ordered[index + 1],
                ordered[index + 2],
                ordered[index + 3],
            )
            if not _valid_abc(a, b, c):
                continue
            state = "COMPLETED_ABCD" if _completed_abcd(a, b, c, d) else "INVALIDATED_ABC"
            events.append(
                Event(
                    state=state,
                    symbol=symbol,
                    year=_year_from_ts(d.knowledge_ts),
                    pivot_ids=(a.pivot_id, b.pivot_id, c.pivot_id, d.pivot_id),
                    knowledge_time=d.knowledge_time,
                )
            )
    return events


def incremental_events(pivots: Sequence[Pivot]) -> list[Event]:
    events: list[Event] = []
    by_symbol = {symbol: [] for symbol in SYMBOLS}
    for pivot in pivots:
        by_symbol.setdefault(pivot.symbol, []).append(pivot)

    for symbol in SYMBOLS:
        seen: list[Pivot] = []
        for pivot in _sort_pivots(by_symbol.get(symbol, [])):
            seen.append(pivot)
            if len(seen) >= 2:
                a, b = seen[-2], seen[-1]
                events.append(
                    Event(
                        state="AB_WAITING_FOR_C",
                        symbol=symbol,
                        year=_year_from_ts(b.knowledge_ts),
                        pivot_ids=(a.pivot_id, b.pivot_id),
                        knowledge_time=b.knowledge_time,
                    )
                )
            if len(seen) >= 3:
                a, b, c = seen[-3], seen[-2], seen[-1]
                if _valid_abc(a, b, c):
                    events.append(
                        Event(
                            state="ABC_WAITING_FOR_D",
                            symbol=symbol,
                            year=_year_from_ts(c.knowledge_ts),
                            pivot_ids=(a.pivot_id, b.pivot_id, c.pivot_id),
                            knowledge_time=c.knowledge_time,
                        )
                    )
            if len(seen) >= 4:
                a, b, c, d = seen[-4], seen[-3], seen[-2], seen[-1]
                if not _valid_abc(a, b, c):
                    continue
                state = "COMPLETED_ABCD" if _completed_abcd(a, b, c, d) else "INVALIDATED_ABC"
                events.append(
                    Event(
                        state=state,
                        symbol=symbol,
                        year=_year_from_ts(d.knowledge_ts),
                        pivot_ids=(a.pivot_id, b.pivot_id, c.pivot_id, d.pivot_id),
                        knowledge_time=d.knowledge_time,
                    )
                )
    return events


def _count_events(events: Sequence[Event]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    detail = {(symbol, year): _empty_counts() for symbol in SYMBOLS for year in YEARS}
    by_symbol = {symbol: _empty_counts() for symbol in SYMBOLS}
    by_year = {year: _empty_counts() for year in YEARS}
    state_to_key = {
        "AB_WAITING_FOR_C": "ab_waiting_for_c",
        "ABC_WAITING_FOR_D": "abc_waiting_for_d",
        "INVALIDATED_ABC": "invalidated_abc",
        "COMPLETED_ABCD": "completed_abcd",
    }
    for event in events:
        key = state_to_key[event.state]
        if event.symbol in SYMBOLS and event.year in YEARS:
            detail[(event.symbol, event.year)][key] += 1
            by_symbol[event.symbol][key] += 1
            by_year[event.year][key] += 1

    summary_rows = [
        {"symbol": symbol, "year": year, **detail[(symbol, year)]}
        for symbol in SYMBOLS
        for year in YEARS
    ]
    symbol_rows = [{"symbol": symbol, **by_symbol[symbol]} for symbol in SYMBOLS]
    year_rows = [{"year": year, **by_year[year]} for year in YEARS]
    return summary_rows, symbol_rows, year_rows


def _totals(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    return {key: sum(int(row[key]) for row in rows) for key in COUNT_KEYS}


def _stability_label(rows: Sequence[dict[str, Any]], group_key: str) -> str:
    completed = [int(row["completed_abcd"]) for row in rows]
    if not completed:
        return "not determined"
    if min(completed) == 0:
        return f"not stable across {group_key}: at least one group has zero completed ABCD structures"
    ratio = max(completed) / min(completed)
    if ratio <= 1.5:
        return f"broadly stable across {group_key} by completed-count scale (max/min={ratio:.2f})"
    if ratio <= 3.0:
        return f"moderately uneven across {group_key} by completed-count scale (max/min={ratio:.2f})"
    return f"highly uneven across {group_key} by completed-count scale (max/min={ratio:.2f})"


def _abundance_label(completed: int) -> str:
    if completed >= 1_000:
        return "abundant"
    if completed >= 100:
        return "moderate"
    return "rare/sparse"


def write_report(
    output_root: Path,
    *,
    reactions_path: Path,
    pivots: Sequence[Pivot],
    rejects: dict[str, int],
    summary_rows: Sequence[dict[str, Any]],
    symbol_rows: Sequence[dict[str, Any]],
    year_rows: Sequence[dict[str, Any]],
    batch_incremental_match: bool,
    mismatch_detail: str,
) -> None:
    totals = _totals(summary_rows)
    completed = totals["completed_abcd"]
    phase2_ready = completed >= 1_000 and all(int(row["completed_abcd"]) >= 30 for row in symbol_rows) and all(int(row["completed_abcd"]) >= 30 for row in year_rows)
    lines = [
        "# AB/CD Population Audit Report",
        "",
        "## Scope",
        "- Phase 1 population audit only.",
        f"- Trusted input: `{reactions_path.as_posix()}`.",
        f"- Causal design: `{DESIGN_DOC.as_posix()}`.",
        "- No symmetry classification, AB/CD ratio measurement, harmonic-level usage, 0.40 split usage, expectancy, detector, scanner, strategy, entry/exit, trade, or production-code behavior was performed.",
        "",
        "## Causal Controls",
        "- Pivot source: SLOW `CONFIRMING` reactions only.",
        "- Canonical direction source: `candidate_direction`.",
        "- Explicit `knowledge_time` required; no `completion_time` fallback is allowed or used.",
        "- State emissions use only the pivot that makes the state knowable: B for AB_WAITING_FOR_C, C for ABC_WAITING_FOR_D, D for INVALIDATED_ABC/COMPLETED_ABCD.",
        f"- Batch/incremental equivalence: {'PASS' if batch_incremental_match else 'FAIL'}.",
        f"- Equivalence detail: {mismatch_detail}",
        "",
        "## Dataset Coverage",
        f"- Accepted validated SLOW confirming pivots: {len(pivots)}",
        *[f"- Rejected/ignored rows — {key}: {value}" for key, value in rejects.items()],
        "",
        "## Total Population Sizes",
        f"- AB_WAITING_FOR_C: {totals['ab_waiting_for_c']}",
        f"- ABC_WAITING_FOR_D: {totals['abc_waiting_for_d']}",
        f"- INVALIDATED_ABC: {totals['invalidated_abc']}",
        f"- COMPLETED_ABCD: {totals['completed_abcd']}",
        "",
        "## Answers",
        f"1. **Total population sizes.** AB_WAITING_FOR_C={totals['ab_waiting_for_c']}; ABC_WAITING_FOR_D={totals['abc_waiting_for_d']}; INVALIDATED_ABC={totals['invalidated_abc']}; COMPLETED_ABCD={totals['completed_abcd']}.",
        f"2. **Are completed ABCD structures rare or abundant?** Completed ABCD structures are {_abundance_label(completed)} at {completed} completed structures.",
        f"3. **Is population stable across symbols?** {_stability_label(symbol_rows, 'symbols')}.",
        f"4. **Is population stable across years?** {_stability_label(year_rows, 'years')}.",
        f"5. **Is sample size large enough to justify Phase 2 symmetry research?** {'Yes, the Phase 1 population clears the completed-count floor for descriptive Phase 2 research.' if phase2_ready else 'No, the Phase 1 population does not clear the completed-count floor across all requested symbol/year groups.'}",
        "",
        "## By Symbol",
        "| Symbol | AB_WAITING_FOR_C | ABC_WAITING_FOR_D | INVALIDATED_ABC | COMPLETED_ABCD |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in symbol_rows:
        lines.append(
            f"| {row['symbol']} | {row['ab_waiting_for_c']} | {row['abc_waiting_for_d']} | {row['invalidated_abc']} | {row['completed_abcd']} |"
        )
    lines.extend([
        "",
        "## By Year",
        "| Year | AB_WAITING_FOR_C | ABC_WAITING_FOR_D | INVALIDATED_ABC | COMPLETED_ABCD |",
        "|---|---:|---:|---:|---:|",
    ])
    for row in year_rows:
        lines.append(
            f"| {row['year']} | {row['ab_waiting_for_c']} | {row['abc_waiting_for_d']} | {row['invalidated_abc']} | {row['completed_abcd']} |"
        )
    lines.extend([
        "",
        "## Research Guardrail",
        "This is a population audit only. It does not classify symmetry, measure AB/CD ratios, use harmonic levels, use the 0.40 split, compute expectancy, or define a detector, scanner, strategy, signal, entry/exit rule, trade rule, or production behavior.",
        "",
    ])
    (output_root / "abcd_population_report.md").write_text("\n".join(lines), encoding="utf-8")


def run_audit(
    *,
    input_root: str | Path = TRUSTED_INPUT_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> bool:
    input_path = Path(input_root)
    output_path = Path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)
    try:
        pivots, rejects, reactions_path = load_validated_pivots(input_path)
    except (FileNotFoundError, ValueError) as exc:
        _write_blocked_outputs(output_path, reason=str(exc), input_root=input_path)
        return False

    batch = batch_events(pivots)
    incremental = incremental_events(pivots)
    batch_keys = sorted(event.key() for event in batch)
    incremental_keys = sorted(event.key() for event in incremental)
    match = batch_keys == incremental_keys
    mismatch_detail = "matched deterministic event rows" if match else "batch and incremental deterministic event rows differ"

    summary_rows, symbol_rows, year_rows = _count_events(batch)
    _write_csv(output_path / "abcd_population_summary.csv", summary_rows, SUMMARY_FIELDS)
    _write_csv(output_path / "abcd_population_by_symbol.csv", symbol_rows, BY_SYMBOL_FIELDS)
    _write_csv(output_path / "abcd_population_by_year.csv", year_rows, BY_YEAR_FIELDS)
    write_report(
        output_path,
        reactions_path=reactions_path,
        pivots=pivots,
        rejects=rejects,
        summary_rows=summary_rows,
        symbol_rows=symbol_rows,
        year_rows=year_rows,
        batch_incremental_match=match,
        mismatch_detail=mismatch_detail,
    )
    return match


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, default=TRUSTED_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero when the trusted input is missing/invalid or equivalence fails",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    ok = run_audit(input_root=args.input_root, output_root=args.output_root)
    if args.strict and not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
