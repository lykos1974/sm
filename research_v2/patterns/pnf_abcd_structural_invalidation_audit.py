"""Research-only AB=CD structural invalidation audit.

This module consumes only existing local research artifacts for the
PRZ_VALID_AND_CONFIRMED_13 cohort and measures post-confirmation/post-retest
structural adverse pivots. It does not inspect raw datasets, reconstruct ABCDs,
use FAST artifacts, create executable entries, targets, RR, expectancy,
profitability, PnL, or trade recommendations.
"""

from __future__ import annotations

import argparse
import csv
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import SYMBOLS, YEARS, _column_sort, _fmt, _normalize_symbol, _parse_float, _parse_time

CONFLUENCE_INPUT = Path("research_v2/patterns/abcd_prz_confirmation_confluence_local_v1/abcd_prz_confirmation_confluence_candidates.csv")
FEASIBILITY_INPUT = Path("research_v2/patterns/abcd_retest_feasibility_local_v1/abcd_retest_feasibility_candidates.csv")
ENTRY_LEVEL_INPUT = Path("research_v2/patterns/abcd_retest_entry_level_local_v1/abcd_retest_entry_level_candidates.csv")
REACTIONS_INPUT = Path("research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/harmonic_reactions_by_threshold.csv")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_structural_invalidation_local_v1")

EXPECTED_COHORT_COUNT = 1281
CONFIRMATION_THRESHOLD_BOXES = 13.0
THRESHOLD_NAME = "SLOW"
REACTION_KIND = "CONFIRMING"
READY = "STRUCTURAL_INVALIDATION_READY_FOR_PNL_PHASE"
NOT_READY = "STRUCTURAL_INVALIDATION_NOT_READY_FOR_PNL_PHASE"
STOP_MODELS = ("STOP_RETRACE_LOW_HIGH", "STOP_D_LEVEL", "STOP_FIRST_ADVERSE_PIVOT")

MEASURE_FIELDS = [
    "total_candidates", "measured_candidates", "unmatched_d_pivot_count", "unmeasured_count", "with_retrace_count", "with_retrace_pct", "with_first_adverse_pivot_count",
    "with_first_adverse_pivot_pct", "median_adverse_boxes_after_retest", "p75_adverse_boxes_after_retest",
    "p90_adverse_boxes_after_retest", "p95_adverse_boxes_after_retest", "median_adverse_pct_of_first_reaction",
    "median_adverse_pct_of_continuation", "median_columns_to_adverse_pivot", "median_time_to_adverse_pivot",
]
SUMMARY_FIELDS = [*MEASURE_FIELDS, "STOP_RETRACE_LOW_HIGH_feasibility", "STOP_D_LEVEL_feasibility", "STOP_FIRST_ADVERSE_PIVOT_feasibility", "decision"]
BY_SYMBOL_FIELDS = ["symbol", *MEASURE_FIELDS]
BY_YEAR_FIELDS = ["year", *MEASURE_FIELDS]
BY_STOP_MODEL_FIELDS = ["stop_model", "structurally_available_count", "structurally_available_pct", "structural_feasibility"]
CANDIDATE_FIELDS = [
    "candidate_id", "symbol", "year", "post_d_reaction_direction", "first_post_d_reaction_boxes",
    "retrace_boxes", "retrace_pct_of_first_reaction", "continuation_boxes_after_retrace", "first_adverse_pivot_direction",
    "first_adverse_pivot_boxes", "adverse_boxes_from_retrace_point", "adverse_pct_of_first_reaction",
    "adverse_pct_of_continuation", "columns_to_adverse_pivot", "time_to_adverse_pivot",
    "STOP_RETRACE_LOW_HIGH_structurally_available", "STOP_D_LEVEL_structurally_available", "STOP_FIRST_ADVERSE_PIVOT_structurally_available",
    "d_pivot_match_status", "structural_invalidation_status",
]


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


def _opposite(direction: str) -> str:
    if direction == "UP":
        return "DOWN"
    if direction == "DOWN":
        return "UP"
    raise ValueError(f"unsupported direction: {direction}")


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _median(values: Iterable[float]) -> str:
    clean = [value for value in values if value is not None]
    return _fmt(statistics.median(clean)) if clean else ""


def _quantile(values: Iterable[float], pct: float) -> str:
    clean = sorted(value for value in values if value is not None)
    if not clean:
        return ""
    index = (len(clean) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(clean) - 1)
    weight = index - lower
    return _fmt(clean[lower] * (1.0 - weight) + clean[upper] * weight)


def _load_confluence_cohort(path: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("candidate_id", "PRZ_VALID_AND_CONFIRMED_13"))
        for row_number, row in enumerate(reader, start=2):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if not candidate_id:
                raise ValueError(f"{path}:{row_number}: missing candidate_id")
            if _yes(row.get("PRZ_VALID_AND_CONFIRMED_13")):
                rows[candidate_id] = {key: str(value or "") for key, value in row.items()}
    if len(rows) != EXPECTED_COHORT_COUNT:
        raise ValueError(f"PRZ_VALID_AND_CONFIRMED_13 cohort count changed: expected {EXPECTED_COHORT_COUNT}, observed {len(rows)}")
    return rows


def _load_by_candidate(path: Path, required: Sequence[str], cohort: set[str]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, required)
        for row_number, row in enumerate(reader, start=2):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if candidate_id not in cohort:
                continue
            if candidate_id in rows:
                raise ValueError(f"{path}:{row_number}: duplicate candidate_id {candidate_id}")
            rows[candidate_id] = {key: str(value or "") for key, value in row.items()}
    if len(rows) != EXPECTED_COHORT_COUNT:
        raise ValueError(f"{path}: cohort join changed: expected {EXPECTED_COHORT_COUNT}, observed {len(rows)}")
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
            if symbol not in SYMBOLS or direction not in {"UP", "DOWN"} or boxes is None or knowledge_ts is None:
                raise ValueError(f"{path}:{row_number}: invalid reaction row")
            rows.append(ReactionRow(symbol, direction, boxes, knowledge_time, knowledge_ts, str(row.get("column_id") or "").strip(), _column_sort(row.get("column_id"))))
    return sorted(rows, key=lambda row: (row.symbol, row.knowledge_ts, row.column_sort, row.direction, row.boxes))


def _match_d_index(row: dict[str, Any], ordered: Sequence[ReactionRow]) -> int | None:
    post_direction = str(row.get("post_d_reaction_direction") or "").strip().upper()
    d_ts = _parse_time(_first(row, ("d_knowledge_time", "d_time", "candidate_knowledge_time")))
    d_col = _column_sort(_first(row, ("d_column_id", "column_id")))
    cd_direction = _opposite(post_direction)
    match = next((i for i, reaction in enumerate(ordered) if (reaction.knowledge_ts, reaction.column_sort, reaction.direction) == (d_ts, d_col, cd_direction)), None)
    return match


def _candidate_level_measurement(row: dict[str, Any], *, d_match_status: str, status: str) -> dict[str, Any]:
    post_direction = str(row.get("post_d_reaction_direction") or "").strip().upper()
    first_boxes = _parse_float(row.get("first_post_d_reaction_boxes"))
    retrace_boxes = _parse_float(row.get("retrace_boxes"))
    continuation_boxes = _parse_float(row.get("continuation_boxes_after_retrace"))
    return {
        "candidate_id": row["candidate_id"], "symbol": row["symbol"], "year": row.get("year", ""),
        "post_d_reaction_direction": post_direction, "first_post_d_reaction_boxes": _fmt(first_boxes) if first_boxes is not None else str(row.get("first_post_d_reaction_boxes") or ""),
        "retrace_boxes": _fmt(retrace_boxes) if retrace_boxes is not None else str(row.get("retrace_boxes") or ""),
        "retrace_pct_of_first_reaction": row.get("retrace_pct_of_first_reaction", ""),
        "continuation_boxes_after_retrace": _fmt(continuation_boxes) if continuation_boxes is not None else str(row.get("continuation_boxes_after_retrace") or ""),
        "first_adverse_pivot_direction": row.get("first_adverse_pivot_direction", ""),
        "first_adverse_pivot_boxes": row.get("first_adverse_pivot_boxes", ""),
        "adverse_boxes_from_retrace_point": row.get("adverse_boxes_from_retrace_point", ""),
        "adverse_pct_of_first_reaction": row.get("adverse_pct_of_first_reaction", ""),
        "adverse_pct_of_continuation": row.get("adverse_pct_of_continuation", ""),
        "columns_to_adverse_pivot": row.get("columns_to_adverse_pivot", ""),
        "time_to_adverse_pivot": row.get("time_to_adverse_pivot", ""),
        "STOP_RETRACE_LOW_HIGH_structurally_available": "",
        "STOP_D_LEVEL_structurally_available": "",
        "STOP_FIRST_ADVERSE_PIVOT_structurally_available": "",
        "d_pivot_match_status": d_match_status,
        "structural_invalidation_status": status,
    }


def _measure_candidate(row: dict[str, Any], ordered: Sequence[ReactionRow]) -> dict[str, Any]:
    post_direction = str(row.get("post_d_reaction_direction") or "").strip().upper()
    first_boxes = _parse_float(row.get("first_post_d_reaction_boxes"))
    retrace_boxes = _parse_float(row.get("retrace_boxes"))
    continuation_boxes = _parse_float(row.get("continuation_boxes_after_retrace"))
    d_index = _match_d_index(row, ordered)
    if d_index is None:
        return _candidate_level_measurement(row, d_match_status="UNMATCHED", status="UNMEASURED_D_MATCH")
    confirmation_index = next((i for i, reaction in enumerate(ordered[d_index + 1 :], start=d_index + 1) if reaction.direction == post_direction and reaction.boxes >= CONFIRMATION_THRESHOLD_BOXES), None)
    if confirmation_index is None:
        raise ValueError(f"could not match confirmation event for candidate_id={row['candidate_id']}")
    retrace_direction = _opposite(post_direction)
    retrace = next((reaction for reaction in ordered[confirmation_index + 1 :] if reaction.direction == retrace_direction), None)
    adverse = next((reaction for reaction in ordered[confirmation_index + 1 :] if retrace is not None and reaction.direction == retrace_direction and reaction.knowledge_ts > retrace.knowledge_ts), None)
    adverse_boxes = adverse.boxes if adverse is not None else None
    return {
        "candidate_id": row["candidate_id"], "symbol": row["symbol"], "year": row.get("year", ""),
        "post_d_reaction_direction": post_direction, "first_post_d_reaction_boxes": _fmt(first_boxes) if first_boxes is not None else "",
        "retrace_boxes": _fmt(retrace_boxes) if retrace_boxes is not None else "",
        "retrace_pct_of_first_reaction": row.get("retrace_pct_of_first_reaction", ""),
        "continuation_boxes_after_retrace": _fmt(continuation_boxes) if continuation_boxes is not None else "",
        "first_adverse_pivot_direction": adverse.direction if adverse is not None else "",
        "first_adverse_pivot_boxes": _fmt(adverse.boxes) if adverse is not None else "",
        "adverse_boxes_from_retrace_point": _fmt(adverse_boxes) if adverse_boxes is not None else "",
        "adverse_pct_of_first_reaction": _fmt(adverse_boxes / first_boxes) if adverse_boxes is not None and first_boxes else "",
        "adverse_pct_of_continuation": _fmt(adverse_boxes / continuation_boxes) if adverse_boxes is not None and continuation_boxes else "",
        "columns_to_adverse_pivot": str(adverse.column_sort - retrace.column_sort) if adverse is not None and retrace is not None else "",
        "time_to_adverse_pivot": str(int(round((adverse.knowledge_ts - retrace.knowledge_ts) * 1000))) if adverse is not None and retrace is not None else "",
        "STOP_RETRACE_LOW_HIGH_structurally_available": "YES" if retrace is not None else "NO",
        "STOP_D_LEVEL_structurally_available": "YES",
        "STOP_FIRST_ADVERSE_PIVOT_structurally_available": "YES" if adverse is not None else "NO",
        "d_pivot_match_status": "MATCHED",
        "structural_invalidation_status": "MEASURED",
    }


def measure(confluence: dict[str, dict[str, Any]], feasibility: dict[str, dict[str, Any]], entry: dict[str, dict[str, Any]], reactions: Sequence[ReactionRow]) -> list[dict[str, Any]]:
    by_symbol = {symbol: [reaction for reaction in reactions if reaction.symbol == symbol] for symbol in SYMBOLS}
    out: list[dict[str, Any]] = []
    for candidate_id in sorted(confluence):
        base = {**confluence[candidate_id], **feasibility[candidate_id], **entry[candidate_id], "candidate_id": candidate_id}
        symbol = _normalize_symbol(str(base.get("symbol") or ""))
        if symbol not in SYMBOLS:
            raise ValueError(f"candidate_id={candidate_id}: unsupported symbol {symbol}")
        base["symbol"] = symbol
        out.append(_measure_candidate(base, by_symbol[symbol]))
    return out


def _measured_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("structural_invalidation_status") == "MEASURED"]


def summarize(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    total_candidates = len(rows)
    unmatched = sum(1 for row in rows if row.get("d_pivot_match_status") == "UNMATCHED")
    unmeasured = sum(1 for row in rows if str(row.get("structural_invalidation_status") or "").startswith("UNMEASURED"))
    rows = _measured_rows(rows)
    total = len(rows)
    retraced = sum(1 for row in rows if row["STOP_RETRACE_LOW_HIGH_structurally_available"] == "YES")
    adverse = sum(1 for row in rows if row["STOP_FIRST_ADVERSE_PIVOT_structurally_available"] == "YES")
    adverse_boxes = [v for row in rows if (v := _parse_float(row.get("adverse_boxes_from_retrace_point"))) is not None]
    return {
        "total_candidates": total_candidates, "measured_candidates": total, "unmatched_d_pivot_count": unmatched, "unmeasured_count": unmeasured, "with_retrace_count": retraced, "with_retrace_pct": _pct(retraced, total),
        "with_first_adverse_pivot_count": adverse, "with_first_adverse_pivot_pct": _pct(adverse, total),
        "median_adverse_boxes_after_retest": _median(adverse_boxes), "p75_adverse_boxes_after_retest": _quantile(adverse_boxes, 0.75),
        "p90_adverse_boxes_after_retest": _quantile(adverse_boxes, 0.90), "p95_adverse_boxes_after_retest": _quantile(adverse_boxes, 0.95),
        "median_adverse_pct_of_first_reaction": _median(v for row in rows if (v := _parse_float(row.get("adverse_pct_of_first_reaction"))) is not None),
        "median_adverse_pct_of_continuation": _median(v for row in rows if (v := _parse_float(row.get("adverse_pct_of_continuation"))) is not None),
        "median_columns_to_adverse_pivot": _median(v for row in rows if (v := _parse_float(row.get("columns_to_adverse_pivot"))) is not None),
        "median_time_to_adverse_pivot": _median(v for row in rows if (v := _parse_float(row.get("time_to_adverse_pivot"))) is not None),
    }


def stop_model_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = _measured_rows(rows)
    total = len(rows)
    out = []
    for model in STOP_MODELS:
        field = f"{model}_structurally_available"
        count = sum(1 for row in rows if row.get(field) == "YES")
        out.append({"stop_model": model, "structurally_available_count": count, "structurally_available_pct": _pct(count, total), "structural_feasibility": "FEASIBLE" if count == total else "PARTIAL" if count else "NOT_FEASIBLE"})
    return out


def _scoped_summary(rows: Sequence[dict[str, Any]], field: str, values: Sequence[Any]) -> list[dict[str, Any]]:
    return [{field: str(value), **summarize([row for row in rows if str(row.get(field)) == str(value)])} for value in values]


def _decision(summary: dict[str, Any], stops: Sequence[dict[str, Any]]) -> str:
    if summary["total_candidates"] != EXPECTED_COHORT_COUNT or summary["unmeasured_count"]:
        return NOT_READY
    if not summary["median_adverse_boxes_after_retest"]:
        return NOT_READY
    return READY if any(row["structural_feasibility"] in {"FEASIBLE", "PARTIAL"} for row in stops) else NOT_READY


def write_report(path: Path, summary: dict[str, Any], stops: Sequence[dict[str, Any]]) -> None:
    stop_by_name = {row["stop_model"]: row for row in stops}
    lines = [
        "# AB=CD Structural Invalidation Audit",
        "",
        "Research-only structural audit. No raw dataset inspection, ABCD reconstruction, FAST artifacts, executable entries, targets, RR, expectancy, profitability, PnL, or trade recommendation is included.",
        "",
        "## Required Answers",
        f"1. Total candidates: {summary['total_candidates']}",
        f"2. Candidates measured: {summary['measured_candidates']}",
        f"3. Unmatched D pivots: {summary['unmatched_d_pivot_count']}",
        f"4. Unmeasured candidates: {summary['unmeasured_count']}",
        f"5. Median adverse boxes after retest: {summary['median_adverse_boxes_after_retest']}",
        f"6. p75 adverse boxes: {summary['p75_adverse_boxes_after_retest']}",
        f"7. p90 adverse boxes: {summary['p90_adverse_boxes_after_retest']}",
        f"8. p95 adverse boxes: {summary['p95_adverse_boxes_after_retest']}",
        f"9. Structural feasibility of STOP_RETRACE_LOW_HIGH: {stop_by_name['STOP_RETRACE_LOW_HIGH']['structural_feasibility']} ({stop_by_name['STOP_RETRACE_LOW_HIGH']['structurally_available_count']}, {stop_by_name['STOP_RETRACE_LOW_HIGH']['structurally_available_pct']})",
        f"10. Structural feasibility of STOP_D_LEVEL: {stop_by_name['STOP_D_LEVEL']['structural_feasibility']} ({stop_by_name['STOP_D_LEVEL']['structurally_available_count']}, {stop_by_name['STOP_D_LEVEL']['structurally_available_pct']})",
        f"11. Structural feasibility of STOP_FIRST_ADVERSE_PIVOT: {stop_by_name['STOP_FIRST_ADVERSE_PIVOT']['structural_feasibility']} ({stop_by_name['STOP_FIRST_ADVERSE_PIVOT']['structurally_available_count']}, {stop_by_name['STOP_FIRST_ADVERSE_PIVOT']['structurally_available_pct']})",
        "12. Stability across symbols: see abcd_structural_invalidation_by_symbol.csv.",
        "13. Stability across years: see abcd_structural_invalidation_by_year.csv.",
        "",
        "## Final Decision",
        str(summary["decision"]),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(confluence_input: Path, feasibility_input: Path, entry_level_input: Path, reactions_input: Path, output_root: Path) -> dict[str, Any]:
    confluence = _load_confluence_cohort(confluence_input)
    cohort = set(confluence)
    feasibility = _load_by_candidate(feasibility_input, ("candidate_id", "symbol", "year", "post_d_reaction_direction", "first_post_d_reaction_boxes", "retrace_boxes", "retrace_pct_of_first_reaction", "continuation_boxes_after_retrace"), cohort)
    entry = _load_by_candidate(entry_level_input, ("candidate_id",), cohort)
    rows = measure(confluence, feasibility, entry, _load_reactions(reactions_input))
    summary = summarize(rows)
    stops = stop_model_rows(rows)
    summary.update({f"{row['stop_model']}_feasibility": row["structural_feasibility"] for row in stops})
    summary["decision"] = _decision(summary, stops)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "abcd_structural_invalidation_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_structural_invalidation_by_symbol.csv", _scoped_summary(rows, "symbol", SYMBOLS), BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_structural_invalidation_by_year.csv", _scoped_summary(rows, "year", YEARS), BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_structural_invalidation_by_stop_model.csv", stops, BY_STOP_MODEL_FIELDS)
    _write_csv(output_root / "abcd_structural_invalidation_candidates.csv", rows, CANDIDATE_FIELDS)
    write_report(output_root / "abcd_structural_invalidation_report.md", summary, stops)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confluence-input", type=Path, default=CONFLUENCE_INPUT)
    parser.add_argument("--feasibility-input", type=Path, default=FEASIBILITY_INPUT)
    parser.add_argument("--entry-level-input", type=Path, default=ENTRY_LEVEL_INPUT)
    parser.add_argument("--reactions-input", type=Path, default=REACTIONS_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    summary = run(args.confluence_input, args.feasibility_input, args.entry_level_input, args.reactions_input, args.output_root)
    print(summary["decision"])


if __name__ == "__main__":
    main()
