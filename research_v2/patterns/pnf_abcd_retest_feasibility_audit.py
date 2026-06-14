"""Research-only Model C retest feasibility audit for PRZ-confirmed AB=CD candidates.

This module consumes only existing local research artifacts to evaluate whether
PRZ_VALID_AND_CONFIRMED_13 candidates structurally show a later opposite-side
retrace/retest and possible continuation after that retrace.

It does not inspect raw datasets, reconstruct ABCDs, use FAST artifacts, create
strategy logic, define executable entries, stops, targets, RR, expectancy,
profitability, PnL, or trading recommendations.
"""

from __future__ import annotations

import argparse
import csv
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import (
    DIRECTIONS,
    SYMBOLS,
    YEARS,
    _column_sort,
    _fmt,
    _normalize_symbol,
    _parse_float,
    _parse_time,
)

CONFLUENCE_INPUT = Path(
    "research_v2/patterns/abcd_prz_confirmation_confluence_local_v1/"
    "abcd_prz_confirmation_confluence_candidates.csv"
)
D_MFE_INPUT = Path("research_v2/patterns/abcd_d_mfe_local_v1/abcd_d_mfe_candidates.csv")
REACTIONS_INPUT = Path(
    "research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/harmonic_reactions_by_threshold.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_retest_feasibility_local_v1")

EXPECTED_COHORT_COUNT = 1281
CONFIRMATION_THRESHOLD_BOXES = 13.0
THRESHOLD_NAME = "SLOW"
REACTION_KIND = "CONFIRMING"
DECISION_FEASIBLE = "RETEST_MODEL_C_FEASIBLE"
DECISION_NOT_FEASIBLE = "RETEST_MODEL_C_NOT_FEASIBLE"
RETRACE_CLASSES = (
    "NO_RETRACE",
    "SHALLOW_RETRACE",
    "NORMAL_RETRACE",
    "DEEP_RETRACE",
    "RETRACE_THEN_CONTINUATION",
)

CANDIDATE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "post_d_reaction_direction",
    "first_post_d_reaction_boxes",
    "has_retrace_after_confirmation",
    "retrace_boxes",
    "retrace_pct_of_first_reaction",
    "columns_to_retrace",
    "time_to_retrace",
    "has_continuation_after_retrace",
    "continuation_boxes_after_retrace",
    "retrace_class",
]
SUMMARY_FIELDS = [
    "measured_candidates",
    "retrace_after_confirmation_count",
    "retrace_after_confirmation_pct",
    "no_retrace_count",
    "no_retrace_pct",
    "shallow_retrace_count",
    "shallow_retrace_pct",
    "normal_retrace_count",
    "normal_retrace_pct",
    "deep_retrace_count",
    "deep_retrace_pct",
    "retrace_then_continuation_count",
    "retrace_then_continuation_pct",
    "median_retrace_pct_of_first_reaction",
    "median_columns_to_retrace",
    "median_continuation_boxes_after_retrace",
    "decision",
]
BY_SYMBOL_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]
BY_CLASS_FIELDS = ["retrace_class", "count", "pct_of_measured", "median_retrace_pct_of_first_reaction", "median_columns_to_retrace", "median_continuation_boxes_after_retrace"]


@dataclass(frozen=True)
class ReactionRow:
    symbol: str
    direction: str
    boxes: float
    knowledge_time: str
    knowledge_ts: float
    column_id: str
    column_sort: int


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _require_fields(path: Path, fieldnames: Sequence[str] | None, required: Iterable[str]) -> None:
    if not fieldnames:
        raise ValueError(f"{path}: expected CSV header")
    missing = [field for field in required if field not in fieldnames]
    if missing:
        raise ValueError(f"{path}: missing required fields: {', '.join(missing)}")


def _first(row: dict[str, Any], aliases: Sequence[str]) -> Any:
    for alias in aliases:
        value = row.get(alias)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _yes(value: Any) -> bool:
    return str(value or "").strip().upper() in {"1", "TRUE", "YES", "Y"}


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _median(values: Iterable[float]) -> str:
    clean = [value for value in values if value is not None]
    return _fmt(statistics.median(clean)) if clean else ""


def _opposite(direction: str) -> str:
    if direction == "UP":
        return "DOWN"
    if direction == "DOWN":
        return "UP"
    raise ValueError(f"unsupported direction: {direction}")


def _load_confluence_cohort(path: Path) -> set[str]:
    cohort: set[str] = set()
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("candidate_id", "PRZ_VALID_AND_CONFIRMED_13"))
        for row_number, row in enumerate(reader, start=2):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if not candidate_id:
                raise ValueError(f"{path}:{row_number}: missing candidate_id")
            if _yes(row.get("PRZ_VALID_AND_CONFIRMED_13")):
                cohort.add(candidate_id)
    if len(cohort) != EXPECTED_COHORT_COUNT:
        raise ValueError(
            "PRZ_VALID_AND_CONFIRMED_13 cohort count changed: "
            f"expected {EXPECTED_COHORT_COUNT}, observed {len(cohort)}"
        )
    return cohort


def _load_d_mfe(path: Path, cohort: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    required = ("candidate_id", "symbol", "year", "post_d_reaction_direction", "first_post_d_reaction_boxes")
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, required)
        for row_number, row in enumerate(reader, start=2):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if candidate_id not in cohort:
                continue
            symbol = _normalize_symbol(str(row.get("symbol") or ""))
            direction = str(row.get("post_d_reaction_direction") or "").strip().upper()
            first_boxes = _parse_float(row.get("first_post_d_reaction_boxes"))
            if symbol not in SYMBOLS or direction not in DIRECTIONS or first_boxes is None or first_boxes < CONFIRMATION_THRESHOLD_BOXES:
                raise ValueError(f"{path}:{row_number}: invalid confirmed cohort D-MFE row")
            rows.append({**row, "candidate_id": candidate_id, "symbol": symbol, "post_d_reaction_direction": direction, "first_post_d_reaction_boxes_value": first_boxes})
    if len(rows) != EXPECTED_COHORT_COUNT:
        raise ValueError(f"D-MFE cohort join changed: expected {EXPECTED_COHORT_COUNT}, observed {len(rows)}")
    return rows


def _load_reactions(path: Path) -> list[ReactionRow]:
    rows: list[ReactionRow] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("symbol", "candidate_direction", "knowledge_time", "candidate_boxes"))
        for row_number, row in enumerate(reader, start=2):
            if str(row.get("threshold_name") or "").strip().upper() != THRESHOLD_NAME:
                continue
            if str(row.get("reaction_kind") or "").strip().upper() != REACTION_KIND:
                continue
            symbol = _normalize_symbol(str(row.get("symbol") or ""))
            direction = str(row.get("candidate_direction") or "").strip().upper()
            boxes = _parse_float(_first(row, ("candidate_boxes", "reaction_boxes")))
            knowledge_time = str(row.get("knowledge_time") or "").strip()
            knowledge_ts = _parse_time(knowledge_time)
            if symbol not in SYMBOLS or direction not in DIRECTIONS or boxes is None or knowledge_ts is None:
                raise ValueError(f"{path}:{row_number}: invalid reaction row")
            rows.append(ReactionRow(symbol, direction, boxes, knowledge_time, knowledge_ts, str(row.get("column_id") or "").strip(), _column_sort(row.get("column_id"))))
    return sorted(rows, key=lambda row: (row.symbol, row.knowledge_ts, row.column_sort, row.direction, row.boxes))


def _d_match_values(row: dict[str, Any]) -> tuple[float | None, int, str]:
    time_text = str(_first(row, ("d_knowledge_time", "d_time", "candidate_knowledge_time")) or "").strip()
    return _parse_time(time_text), _column_sort(_first(row, ("d_column_id", "column_id"))), _opposite(row["post_d_reaction_direction"])


def _classify(has_retrace: bool, pct: float | None, has_continuation: bool) -> str:
    if not has_retrace:
        return "NO_RETRACE"
    if has_continuation:
        return "RETRACE_THEN_CONTINUATION"
    if pct is not None and pct < 0.382:
        return "SHALLOW_RETRACE"
    if pct is not None and pct <= 0.618:
        return "NORMAL_RETRACE"
    return "DEEP_RETRACE"


def measure(d_mfe_rows: Sequence[dict[str, Any]], reactions: Sequence[ReactionRow]) -> list[dict[str, Any]]:
    by_symbol = {symbol: [row for row in reactions if row.symbol == symbol] for symbol in SYMBOLS}
    output: list[dict[str, Any]] = []
    for candidate in d_mfe_rows:
        ordered = by_symbol[candidate["symbol"]]
        d_ts, d_col, cd_direction = _d_match_values(candidate)
        d_index = next((i for i, row in enumerate(ordered) if (row.knowledge_ts, row.column_sort, row.direction) == (d_ts, d_col, cd_direction)), None)
        if d_index is None:
            raise ValueError(f"could not match D pivot for candidate_id={candidate['candidate_id']}")
        post_direction = candidate["post_d_reaction_direction"]
        confirmation_index = next((i for i, row in enumerate(ordered[d_index + 1 :], start=d_index + 1) if row.direction == post_direction and row.boxes >= CONFIRMATION_THRESHOLD_BOXES), None)
        if confirmation_index is None:
            raise ValueError(f"could not match confirmation event for candidate_id={candidate['candidate_id']}")
        confirmation = ordered[confirmation_index]
        retrace_direction = _opposite(post_direction)
        retrace = next((row for row in ordered[confirmation_index + 1 :] if row.direction == retrace_direction), None)
        continuation_boxes: float | None = None
        if retrace is not None:
            continuation_values = [row.boxes for row in ordered[confirmation_index + 1 :] if row.direction == post_direction and row.knowledge_ts > retrace.knowledge_ts]
            continuation_boxes = max(continuation_values) if continuation_values else None
        first_boxes = candidate["first_post_d_reaction_boxes_value"]
        retrace_pct = (retrace.boxes / first_boxes) if retrace is not None and first_boxes else None
        has_continuation = continuation_boxes is not None
        output.append(
            {
                "candidate_id": candidate["candidate_id"],
                "symbol": candidate["symbol"],
                "year": str(candidate.get("year") or "").strip(),
                "post_d_reaction_direction": post_direction,
                "first_post_d_reaction_boxes": _fmt(first_boxes),
                "has_retrace_after_confirmation": "YES" if retrace else "NO",
                "retrace_boxes": _fmt(retrace.boxes) if retrace else "",
                "retrace_pct_of_first_reaction": _fmt(retrace_pct) if retrace_pct is not None else "",
                "columns_to_retrace": str(retrace.column_sort - confirmation.column_sort) if retrace else "",
                "time_to_retrace": str(int(round((retrace.knowledge_ts - confirmation.knowledge_ts) * 1000))) if retrace else "",
                "has_continuation_after_retrace": "YES" if has_continuation else "NO",
                "continuation_boxes_after_retrace": _fmt(continuation_boxes) if continuation_boxes is not None else "",
                "retrace_class": _classify(retrace is not None, retrace_pct, has_continuation),
            }
        )
    return output


def summarize(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    counts = {name: sum(1 for row in rows if row["retrace_class"] == name) for name in RETRACE_CLASSES}
    retrace_count = sum(1 for row in rows if row["has_retrace_after_confirmation"] == "YES")
    cont_values = [_parse_float(row.get("continuation_boxes_after_retrace")) for row in rows]
    row = {
        "measured_candidates": total,
        "retrace_after_confirmation_count": retrace_count,
        "retrace_after_confirmation_pct": _pct(retrace_count, total),
        "no_retrace_count": counts["NO_RETRACE"],
        "no_retrace_pct": _pct(counts["NO_RETRACE"], total),
        "shallow_retrace_count": counts["SHALLOW_RETRACE"],
        "shallow_retrace_pct": _pct(counts["SHALLOW_RETRACE"], total),
        "normal_retrace_count": counts["NORMAL_RETRACE"],
        "normal_retrace_pct": _pct(counts["NORMAL_RETRACE"], total),
        "deep_retrace_count": counts["DEEP_RETRACE"],
        "deep_retrace_pct": _pct(counts["DEEP_RETRACE"], total),
        "retrace_then_continuation_count": counts["RETRACE_THEN_CONTINUATION"],
        "retrace_then_continuation_pct": _pct(counts["RETRACE_THEN_CONTINUATION"], total),
        "median_retrace_pct_of_first_reaction": _median([v for row in rows if (v := _parse_float(row.get("retrace_pct_of_first_reaction"))) is not None]),
        "median_columns_to_retrace": _median([v for row in rows if (v := _parse_float(row.get("columns_to_retrace"))) is not None]),
        "median_continuation_boxes_after_retrace": _median([v for v in cont_values if v is not None]),
    }
    row["decision"] = DECISION_FEASIBLE if retrace_count and counts["RETRACE_THEN_CONTINUATION"] else DECISION_NOT_FEASIBLE
    return row


def by_class(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(rows)
    out = []
    for klass in RETRACE_CLASSES:
        scoped = [row for row in rows if row["retrace_class"] == klass]
        out.append({"retrace_class": klass, "count": len(scoped), "pct_of_measured": _pct(len(scoped), total), "median_retrace_pct_of_first_reaction": _median([v for row in scoped if (v := _parse_float(row.get("retrace_pct_of_first_reaction"))) is not None]), "median_columns_to_retrace": _median([v for row in scoped if (v := _parse_float(row.get("columns_to_retrace"))) is not None]), "median_continuation_boxes_after_retrace": _median([v for row in scoped if (v := _parse_float(row.get("continuation_boxes_after_retrace"))) is not None])})
    return out


def _scoped_summary(rows: Sequence[dict[str, Any]], field: str, values: Sequence[Any]) -> list[dict[str, Any]]:
    return [{field: str(value), **summarize([row for row in rows if str(row.get(field)) == str(value)])} for value in values]


def write_report(path: Path, summary: dict[str, Any], by_symbol_rows: Sequence[dict[str, Any]], by_year_rows: Sequence[dict[str, Any]]) -> None:
    lines = [
        "# AB=CD Retest Feasibility Audit — Model C",
        "",
        "Research-only structural feasibility audit. No strategy logic, executable entry, stop, target, RR, expectancy, profitability, PnL, or trade recommendation is defined.",
        "",
        "## Required Answers",
        f"1. PRZ_VALID_AND_CONFIRMED_13 candidates measured: {summary['measured_candidates']}",
        f"2. Retrace after confirmation: {summary['retrace_after_confirmation_count']} ({summary['retrace_after_confirmation_pct']})",
        f"3. NO_RETRACE: {summary['no_retrace_count']} ({summary['no_retrace_pct']})",
        f"4. SHALLOW_RETRACE: {summary['shallow_retrace_count']} ({summary['shallow_retrace_pct']})",
        f"5. NORMAL_RETRACE: {summary['normal_retrace_count']} ({summary['normal_retrace_pct']})",
        f"6. DEEP_RETRACE: {summary['deep_retrace_count']} ({summary['deep_retrace_pct']})",
        f"7. RETRACE_THEN_CONTINUATION: {summary['retrace_then_continuation_count']} ({summary['retrace_then_continuation_pct']})",
        f"8. Median retrace_pct_of_first_reaction: {summary['median_retrace_pct_of_first_reaction']}",
        f"9. Median columns_to_retrace: {summary['median_columns_to_retrace']}",
        f"10. Median continuation_boxes_after_retrace: {summary['median_continuation_boxes_after_retrace']}",
        "11. Stability across BTCUSDT / ETHUSDT / SOLUSDT: see abcd_retest_feasibility_by_symbol.csv.",
        "12. Stability across 2024 / 2025 / 2026: see abcd_retest_feasibility_by_year.csv.",
        "",
        "## Final Decision",
        str(summary["decision"]),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(confluence_input: Path, d_mfe_input: Path, reactions_input: Path, output_root: Path) -> dict[str, Any]:
    cohort = _load_confluence_cohort(confluence_input)
    d_mfe_rows = _load_d_mfe(d_mfe_input, cohort)
    rows = measure(d_mfe_rows, _load_reactions(reactions_input))
    summary = summarize(rows)
    by_symbol_rows = _scoped_summary(rows, "symbol", SYMBOLS)
    by_year_rows = _scoped_summary(rows, "year", YEARS)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "abcd_retest_feasibility_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_retest_feasibility_by_symbol.csv", by_symbol_rows, BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_retest_feasibility_by_year.csv", by_year_rows, BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_retest_feasibility_by_retrace_class.csv", by_class(rows), BY_CLASS_FIELDS)
    _write_csv(output_root / "abcd_retest_feasibility_candidates.csv", rows, CANDIDATE_FIELDS)
    write_report(output_root / "abcd_retest_feasibility_report.md", summary, by_symbol_rows, by_year_rows)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confluence-input", type=Path, default=CONFLUENCE_INPUT)
    parser.add_argument("--d-mfe-input", type=Path, default=D_MFE_INPUT)
    parser.add_argument("--reactions-input", type=Path, default=REACTIONS_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    summary = run(args.confluence_input, args.d_mfe_input, args.reactions_input, args.output_root)
    print(summary["decision"])


if __name__ == "__main__":
    main()
