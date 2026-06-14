"""Research-only AB=CD PRZ overshoot audit.

Measures structural extension beyond the theoretical AB=CD completion point
before the first confirmed post-D reaction appears. The audit consumes only
pre-existing local research artifacts; it does not inspect raw datasets,
reconstruct ABCDs, use FAST artifacts, or create entries, exits, stops,
targets, RR, expectancy, profitability, PnL, strategy logic, or a trading
model.
"""

from __future__ import annotations

import argparse
import csv
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

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

GEOMETRY_INPUT = Path("research_v2/patterns/abcd_geometry_local_v1/abcd_geometry_candidates.csv")
REACTIONS_INPUT = Path(
    "research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/harmonic_reactions_by_threshold.csv"
)
CONFLUENCE_INPUT = Path(
    "research_v2/patterns/abcd_prz_confirmation_confluence_local_v1/"
    "abcd_prz_confirmation_confluence_candidates.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_prz_overshoot_local_v1")

THRESHOLD_NAME = "SLOW"
REACTION_KIND = "CONFIRMING"
CONFIRMATION_THRESHOLD_BOXES = 13.0
VALID_PRZ_CLASSES = {"PRZ_TIGHT", "PRZ_ACCEPTABLE"}
EXPECTED_GEOMETRY_ROWS = 7823
SAMPLE_SIZE = 250

COHORT_ORDER = (
    "ALL",
    "PRZ_TIGHT",
    "PRZ_ACCEPTABLE",
    "PRZ_VALID",
    "CONFIRMED_13",
    "PRZ_VALID_AND_CONFIRMED_13",
)
DECISION_STABLE = "OVERSHOOT_STRUCTURE_STABLE_ENOUGH_FOR_NEXT_PHASE"
DECISION_NOT_STABLE = "OVERSHOOT_STRUCTURE_NOT_STABLE_ENOUGH_FOR_NEXT_PHASE"

STAT_FIELDS = [
    "scope",
    "cohort",
    "count",
    "median_overshoot_boxes",
    "avg_overshoot_boxes",
    "p50_overshoot_boxes",
    "p75_overshoot_boxes",
    "p90_overshoot_boxes",
    "p95_overshoot_boxes",
    "p99_overshoot_boxes",
    "median_overshoot_pct_of_AB",
    "p90_overshoot_pct_of_AB",
    "median_overshoot_pct_of_CD",
    "p90_overshoot_pct_of_CD",
]
BY_SYMBOL_FIELDS = ["symbol", *STAT_FIELDS]
BY_YEAR_FIELDS = ["year", *STAT_FIELDS]
SUMMARY_FIELDS = [
    "total_candidates_measured",
    "all_median_overshoot_boxes",
    "all_p90_overshoot_boxes",
    "all_p95_overshoot_boxes",
    "all_p99_overshoot_boxes",
    "all_median_overshoot_pct_of_AB",
    "all_p90_overshoot_pct_of_AB",
    "confirmed_13_median_overshoot_boxes",
    "prz_valid_and_confirmed_13_median_overshoot_boxes",
    "decision",
]
SAMPLE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "prz_class",
    "overshoot_boxes",
    "overshoot_pct_of_AB",
    "overshoot_pct_of_CD",
    "first_confirmed_reaction_boxes",
    "d_column_id",
    "d_time",
    "cd_direction",
    "first_confirmed_reaction_column_id",
    "first_confirmed_reaction_time",
]


@dataclass(frozen=True)
class GeometryRow:
    candidate_id: str
    symbol: str
    year: int | None
    d_time: str
    d_ts: float
    d_column_id: str
    d_column_sort: int
    cd_direction: str
    ab_boxes: float
    cd_boxes: float

    @property
    def reaction_direction(self) -> str:
        return "DOWN" if self.cd_direction == "UP" else "UP"


@dataclass(frozen=True)
class ReactionRow:
    symbol: str
    direction: str
    boxes: float
    knowledge_time: str
    knowledge_ts: float
    column_id: str
    column_sort: int


@dataclass(frozen=True)
class Measurement:
    candidate: GeometryRow
    prz_class: str
    overshoot_boxes: float
    first_confirmed_reaction: ReactionRow | None

    @property
    def first_confirmed_reaction_boxes(self) -> float:
        return self.first_confirmed_reaction.boxes if self.first_confirmed_reaction else 0.0

    @property
    def overshoot_pct_of_ab(self) -> float:
        return self.overshoot_boxes / self.candidate.ab_boxes if self.candidate.ab_boxes else 0.0

    @property
    def overshoot_pct_of_cd(self) -> float:
        return self.overshoot_boxes / self.candidate.cd_boxes if self.candidate.cd_boxes else 0.0


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


def _percentile(values: Sequence[float], percentile: float) -> str:
    if not values:
        return ""
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return _fmt(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)


def _median(values: Sequence[float]) -> str:
    return _fmt(statistics.median(values)) if values else ""


def _avg(values: Sequence[float]) -> str:
    return _fmt(sum(values) / len(values)) if values else ""


def _first(row: dict[str, Any], aliases: Sequence[str]) -> Any:
    for alias in aliases:
        value = row.get(alias)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _load_geometry(path: Path) -> list[GeometryRow]:
    rows: list[GeometryRow] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("candidate_id", "symbol", "d_time", "d_column_id", "cd_direction", "AB_boxes", "CD_boxes"))
        for row_number, row in enumerate(reader, start=2):
            symbol = _normalize_symbol(str(row.get("symbol") or ""))
            d_time = str(row.get("d_time") or row.get("candidate_knowledge_time") or "").strip()
            d_ts = _parse_time(d_time)
            cd_direction = str(row.get("cd_direction") or "").strip().upper()
            ab_boxes = _parse_float(row.get("AB_boxes"))
            cd_boxes = _parse_float(row.get("CD_boxes"))
            year_value = _parse_float(row.get("year"))
            if symbol not in SYMBOLS or d_ts is None or cd_direction not in DIRECTIONS or not ab_boxes or not cd_boxes:
                raise ValueError(f"{path}:{row_number}: invalid geometry candidate")
            rows.append(GeometryRow(str(row.get("candidate_id") or "").strip(), symbol, int(year_value) if year_value else None, d_time, d_ts, str(row.get("d_column_id") or "").strip(), _column_sort(row.get("d_column_id")), cd_direction, ab_boxes, cd_boxes))
    if len(rows) != EXPECTED_GEOMETRY_ROWS:
        raise ValueError(f"geometry row count changed: expected {EXPECTED_GEOMETRY_ROWS}, observed {len(rows)}")
    if len({row.candidate_id for row in rows}) != len(rows):
        raise ValueError(f"{path}: duplicate candidate_id values")
    return rows


def _load_reactions(path: Path) -> list[ReactionRow]:
    rows: list[ReactionRow] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("symbol", "candidate_direction"))
        for row_number, row in enumerate(reader, start=2):
            if str(row.get("threshold_name") or "").strip().upper() != THRESHOLD_NAME:
                continue
            if str(row.get("reaction_kind") or "").strip().upper() != REACTION_KIND:
                continue
            symbol = _normalize_symbol(str(row.get("symbol") or ""))
            direction = str(row.get("candidate_direction") or "").strip().upper()
            boxes = _parse_float(_first(row, ("candidate_boxes", "reaction_boxes")))
            knowledge_time = str(_first(row, ("knowledge_time", "completion_time", "completed_at", "end_ts")) or "").strip()
            knowledge_ts = _parse_time(knowledge_time)
            if symbol not in SYMBOLS or direction not in DIRECTIONS or boxes is None or knowledge_ts is None:
                raise ValueError(f"{path}:{row_number}: invalid validated reaction row")
            rows.append(ReactionRow(symbol, direction, boxes, knowledge_time, knowledge_ts, str(row.get("column_id") or "").strip(), _column_sort(row.get("column_id"))))
    return sorted(rows, key=lambda row: (row.symbol, row.knowledge_ts, row.column_sort, row.direction, row.boxes))


def _load_prz_classes(path: Path, candidate_ids: set[str]) -> dict[str, str]:
    if not path.is_file():
        raise ValueError(f"required PRZ/confluence artifact is missing: {path}")
    out: dict[str, str] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("candidate_id", "prz_class"))
        for row_number, row in enumerate(reader, start=2):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if not candidate_id:
                raise ValueError(f"{path}:{row_number}: missing candidate_id")
            if candidate_id in out:
                raise ValueError(f"{path}:{row_number}: duplicate candidate_id {candidate_id}")
            out[candidate_id] = str(row.get("prz_class") or "").strip().upper()
    if set(out) != candidate_ids:
        raise ValueError(f"candidate populations do not match for strict candidate_id join: geometry_count={len(candidate_ids)} prz_count={len(out)}")
    return out


def measure(candidates: Sequence[GeometryRow], reactions: Sequence[ReactionRow], prz_classes: dict[str, str]) -> list[Measurement]:
    by_symbol = {symbol: [row for row in reactions if row.symbol == symbol] for symbol in SYMBOLS}
    measurements: list[Measurement] = []
    for candidate in candidates:
        ordered = by_symbol.get(candidate.symbol, [])
        d_index = next((i for i, row in enumerate(ordered) if (row.knowledge_ts, row.column_sort, row.direction) == (candidate.d_ts, candidate.d_column_sort, candidate.cd_direction)), None)
        if d_index is None:
            raise ValueError(f"could not match D pivot in validated reaction chronology for {candidate.candidate_id}")
        max_cd_boxes = candidate.cd_boxes
        first_confirmed: ReactionRow | None = None
        for row in ordered[d_index + 1 :]:
            if row.direction == candidate.reaction_direction:
                first_confirmed = row
                break
            if row.direction == candidate.cd_direction:
                max_cd_boxes = max(max_cd_boxes, row.boxes)
        measurements.append(Measurement(candidate, prz_classes[candidate.candidate_id], max(0.0, max_cd_boxes - candidate.cd_boxes), first_confirmed))
    return measurements


def _cohort_predicates() -> dict[str, Callable[[Measurement], bool]]:
    return {
        "ALL": lambda row: True,
        "PRZ_TIGHT": lambda row: row.prz_class == "PRZ_TIGHT",
        "PRZ_ACCEPTABLE": lambda row: row.prz_class == "PRZ_ACCEPTABLE",
        "PRZ_VALID": lambda row: row.prz_class in VALID_PRZ_CLASSES,
        "CONFIRMED_13": lambda row: row.first_confirmed_reaction_boxes >= CONFIRMATION_THRESHOLD_BOXES,
        "PRZ_VALID_AND_CONFIRMED_13": lambda row: row.prz_class in VALID_PRZ_CLASSES and row.first_confirmed_reaction_boxes >= CONFIRMATION_THRESHOLD_BOXES,
    }


def summarize(rows: Sequence[Measurement], *, scope: str, cohort: str) -> dict[str, Any]:
    boxes = [row.overshoot_boxes for row in rows]
    pct_ab = [row.overshoot_pct_of_ab for row in rows]
    pct_cd = [row.overshoot_pct_of_cd for row in rows]
    return {
        "scope": scope,
        "cohort": cohort,
        "count": len(rows),
        "median_overshoot_boxes": _median(boxes),
        "avg_overshoot_boxes": _avg(boxes),
        "p50_overshoot_boxes": _percentile(boxes, 0.50),
        "p75_overshoot_boxes": _percentile(boxes, 0.75),
        "p90_overshoot_boxes": _percentile(boxes, 0.90),
        "p95_overshoot_boxes": _percentile(boxes, 0.95),
        "p99_overshoot_boxes": _percentile(boxes, 0.99),
        "median_overshoot_pct_of_AB": _median(pct_ab),
        "p90_overshoot_pct_of_AB": _percentile(pct_ab, 0.90),
        "median_overshoot_pct_of_CD": _median(pct_cd),
        "p90_overshoot_pct_of_CD": _percentile(pct_cd, 0.90),
    }


def _cohort_rows(rows: Sequence[Measurement], cohort: str) -> list[Measurement]:
    return [row for row in rows if _cohort_predicates()[cohort](row)]


def _all_summaries(rows: Sequence[Measurement], scope: str) -> list[dict[str, Any]]:
    return [summarize(_cohort_rows(rows, cohort), scope=scope, cohort=cohort) for cohort in COHORT_ORDER]


def _sample_rows(rows: Sequence[Measurement]) -> list[dict[str, Any]]:
    sample = sorted(rows, key=lambda row: (-row.overshoot_boxes, row.candidate.symbol, row.candidate.candidate_id))[:SAMPLE_SIZE]
    return [
        {
            "candidate_id": row.candidate.candidate_id,
            "symbol": row.candidate.symbol,
            "year": row.candidate.year or "",
            "prz_class": row.prz_class,
            "overshoot_boxes": _fmt(row.overshoot_boxes),
            "overshoot_pct_of_AB": _fmt(row.overshoot_pct_of_ab),
            "overshoot_pct_of_CD": _fmt(row.overshoot_pct_of_cd),
            "first_confirmed_reaction_boxes": _fmt(row.first_confirmed_reaction_boxes),
            "d_column_id": row.candidate.d_column_id,
            "d_time": row.candidate.d_time,
            "cd_direction": row.candidate.cd_direction,
            "first_confirmed_reaction_column_id": row.first_confirmed_reaction.column_id if row.first_confirmed_reaction else "",
            "first_confirmed_reaction_time": row.first_confirmed_reaction.knowledge_time if row.first_confirmed_reaction else "",
        }
        for row in sample
    ]


def _markdown_table(rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> str:
    lines = ["| " + " | ".join(fields) + " |", "| " + " | ".join("---" for _ in fields) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(field, "")) for field in fields) + " |")
    return "\n".join(lines)


def _float(row: dict[str, Any], field: str) -> float | None:
    return _parse_float(row.get(field))


def _decision(overall: dict[str, Any], by_symbol: Sequence[dict[str, Any]], by_year: Sequence[dict[str, Any]]) -> str:
    all_p90 = _float(overall, "p90_overshoot_boxes")
    if all_p90 is None:
        return DECISION_NOT_STABLE
    scoped = [row for row in (*by_symbol, *by_year) if row.get("cohort") == "ALL" and int(row.get("count") or 0) > 0]
    p90s = [_float(row, "p90_overshoot_boxes") for row in scoped]
    if not p90s or any(value is None for value in p90s):
        return DECISION_NOT_STABLE
    return DECISION_STABLE if max(p90s) <= max(1.0, all_p90 * 2.0) else DECISION_NOT_STABLE


def _summary_row(overall: dict[str, Any], by_cohort: Sequence[dict[str, Any]], decision: str) -> dict[str, Any]:
    indexed = {row["cohort"]: row for row in by_cohort if row["scope"] == "ALL_SYMBOLS_ALL_YEARS"}
    return {
        "total_candidates_measured": overall["count"],
        "all_median_overshoot_boxes": overall["median_overshoot_boxes"],
        "all_p90_overshoot_boxes": overall["p90_overshoot_boxes"],
        "all_p95_overshoot_boxes": overall["p95_overshoot_boxes"],
        "all_p99_overshoot_boxes": overall["p99_overshoot_boxes"],
        "all_median_overshoot_pct_of_AB": overall["median_overshoot_pct_of_AB"],
        "all_p90_overshoot_pct_of_AB": overall["p90_overshoot_pct_of_AB"],
        "confirmed_13_median_overshoot_boxes": indexed["CONFIRMED_13"]["median_overshoot_boxes"],
        "prz_valid_and_confirmed_13_median_overshoot_boxes": indexed["PRZ_VALID_AND_CONFIRMED_13"]["median_overshoot_boxes"],
        "decision": decision,
    }


def write_report(output_root: Path, summary: dict[str, Any], by_cohort: Sequence[dict[str, Any]], by_symbol: Sequence[dict[str, Any]], by_year: Sequence[dict[str, Any]]) -> None:
    overall = [row for row in by_cohort if row["scope"] == "ALL_SYMBOLS_ALL_YEARS"]
    lines = [
        "# AB=CD PRZ Overshoot Audit",
        "",
        "Research-only structural invalidation-distance audit using existing geometry, validated reaction chronology, and PRZ/confluence artifacts only. No raw datasets, ABCD reconstruction, FAST artifacts, entries, exits, stops, targets, RR, expectancy, profitability, PnL, or trading model are used.",
        "",
        "## Required answers",
        f"1. Total candidates measured: {summary['total_candidates_measured']}",
        f"2. Median overshoot boxes: {summary['all_median_overshoot_boxes']}",
        f"3. p90 overshoot boxes: {summary['all_p90_overshoot_boxes']}",
        f"4. p95 overshoot boxes: {summary['all_p95_overshoot_boxes']}",
        f"5. p99 overshoot boxes: {summary['all_p99_overshoot_boxes']}",
        f"6. Median overshoot % of AB: {summary['all_median_overshoot_pct_of_AB']}",
        f"7. p90 overshoot % of AB: {summary['all_p90_overshoot_pct_of_AB']}",
        "8. Overshoot comparison: see ALL, CONFIRMED_13, and PRZ_VALID_AND_CONFIRMED_13 rows in the cohort table below.",
        "9. Stability across symbols: see symbol table below.",
        "10. Stability across years: see year table below.",
        "",
        "## Core cohort comparison",
        _markdown_table([row for row in overall if row["cohort"] in {"ALL", "CONFIRMED_13", "PRZ_VALID_AND_CONFIRMED_13"}], STAT_FIELDS),
        "",
        "## All cohorts",
        _markdown_table(overall, STAT_FIELDS),
        "",
        "## Stability across symbols",
        _markdown_table([row for row in by_symbol if row["cohort"] == "ALL"], BY_SYMBOL_FIELDS),
        "",
        "## Stability across years",
        _markdown_table([row for row in by_year if row["cohort"] == "ALL"], BY_YEAR_FIELDS),
        "",
        "## Final decision",
        summary["decision"],
        "",
    ]
    (output_root / "abcd_prz_overshoot_report.md").write_text("\n".join(lines), encoding="utf-8")


def run_audit(geometry_input: Path, reactions_input: Path, confluence_input: Path, output_root: Path) -> None:
    candidates = _load_geometry(geometry_input)
    reactions = _load_reactions(reactions_input)
    prz_classes = _load_prz_classes(confluence_input, {row.candidate_id for row in candidates})
    measurements = measure(candidates, reactions, prz_classes)

    by_cohort = _all_summaries(measurements, "ALL_SYMBOLS_ALL_YEARS")
    by_symbol: list[dict[str, Any]] = []
    for symbol in SYMBOLS:
        scoped = [row for row in measurements if row.candidate.symbol == symbol]
        by_symbol.extend({"symbol": symbol, **row} for row in _all_summaries(scoped, f"SYMBOL_{symbol}"))
    by_year: list[dict[str, Any]] = []
    for year in YEARS:
        scoped = [row for row in measurements if row.candidate.year == year]
        by_year.extend({"year": year, **row} for row in _all_summaries(scoped, f"YEAR_{year}"))

    overall = next(row for row in by_cohort if row["cohort"] == "ALL")
    decision = _decision(overall, by_symbol, by_year)
    summary = _summary_row(overall, by_cohort, decision)

    _write_csv(output_root / "abcd_prz_overshoot_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_prz_overshoot_by_cohort.csv", by_cohort, STAT_FIELDS)
    _write_csv(output_root / "abcd_prz_overshoot_by_symbol.csv", by_symbol, BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_prz_overshoot_by_year.csv", by_year, BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_prz_overshoot_sample.csv", _sample_rows(measurements), SAMPLE_FIELDS)
    write_report(output_root, summary, by_cohort, by_symbol, by_year)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--geometry-input", type=Path, default=GEOMETRY_INPUT)
    parser.add_argument("--reactions-input", type=Path, default=REACTIONS_INPUT)
    parser.add_argument("--confluence-input", type=Path, default=CONFLUENCE_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_audit(args.geometry_input, args.reactions_input, args.confluence_input, args.output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
