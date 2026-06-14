"""Research-only AB=CD D-reaction audit.

This module consumes pre-existing local AB=CD geometry candidates and validated
SLOW/CONFIRMING harmonic reactions. It does not inspect raw datasets, does not
reconstruct ABCD structures, does not use FAST artifacts, and does not create a
strategy, entries, exits, stops, targets, PnL, expectancy, profitability, or
trading rules.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import (
    SYMBOLS,
    YEARS,
    _column_sort,
    _normalize_symbol,
    _parse_float,
    _parse_time,
    classify_cd_ab,
)

GEOMETRY_CANDIDATES = Path(
    "research_v2/patterns/abcd_geometry_local_v1/abcd_geometry_candidates.csv"
)
VALIDATED_REACTIONS = Path(
    "research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/"
    "harmonic_reactions_by_threshold.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_d_reaction_local_v1")
EXPECTED_GEOMETRY_ROWS = 7823
THRESHOLD_NAME = "SLOW"
REACTION_KIND = "CONFIRMING"
THRESHOLDS = (1, 2, 3, 5, 8)
COHORTS = ("ALL", "SYM_0_90_1_10", "EXT_1_20_1_35", "EXT_1_55_1_70", "OTHER")
DECISION_STRONG = "D_REACTION_STRONG_ENOUGH_FOR_NEXT_PHASE"
DECISION_WEAK = "D_REACTION_TOO_WEAK_STOP_BRANCH"

SUMMARY_FIELDS = [
    "scope",
    "cohort",
    "count",
    "median_post_d_reaction_boxes",
    "avg_post_d_reaction_boxes",
    "median_first_reaction_boxes",
    "avg_first_reaction_boxes",
    "median_columns_until_max_reaction",
    "avg_columns_until_max_reaction",
    "median_time_until_max_reaction_ms",
    "avg_time_until_max_reaction_ms",
    *(f"pct_reaction_gte_{threshold}_box" for threshold in THRESHOLDS),
]
BY_SYMBOL_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]
SAMPLE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "cohort",
    "d_time",
    "d_column_id",
    "cd_direction",
    "d_reaction_direction",
    "first_reaction_boxes",
    "first_reaction_time",
    "first_reaction_column_id",
    "max_reaction_boxes",
    "max_reaction_time",
    "max_reaction_column_id",
    "columns_until_max_reaction",
    "time_until_max_reaction_ms",
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
    cd_ab_ratio: float
    cohort: str

    @property
    def d_reaction_direction(self) -> str:
        if self.cd_direction == "DOWN":
            return "UP"
        if self.cd_direction == "UP":
            return "DOWN"
        raise ValueError(f"unsupported CD direction: {self.cd_direction!r}")


@dataclass(frozen=True)
class ReactionRow:
    symbol: str
    candidate_direction: str
    candidate_boxes: float
    knowledge_time: str
    knowledge_ts: float
    column_id: str
    column_sort: int


@dataclass(frozen=True)
class Measurement:
    candidate: GeometryRow
    first_reaction: ReactionRow | None
    max_reaction: ReactionRow | None

    @property
    def max_reaction_boxes(self) -> float:
        return self.max_reaction.candidate_boxes if self.max_reaction else 0.0

    @property
    def first_reaction_boxes(self) -> float:
        return self.first_reaction.candidate_boxes if self.first_reaction else 0.0

    @property
    def columns_until_max_reaction(self) -> int | None:
        if not self.max_reaction:
            return None
        return self.max_reaction.column_sort - self.candidate.d_column_sort

    @property
    def time_until_max_reaction_ms(self) -> int | None:
        if not self.max_reaction:
            return None
        return int(round((self.max_reaction.knowledge_ts - self.candidate.d_ts) * 1000))


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


def _pct(count: int, total: int) -> str:
    return _format_number((count / total) * 100.0 if total else math.nan)  # percentages


def _median(values: Sequence[float]) -> str:
    return _format_number(statistics.median(values) if values else math.nan)


def _avg(values: Sequence[float]) -> str:
    return _format_number(sum(values) / len(values) if values else math.nan)


def _first_value(row: dict[str, Any], aliases: Sequence[str]) -> Any:
    for alias in aliases:
        value = row.get(alias)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def load_geometry(path: Path) -> list[GeometryRow]:
    rows: list[GeometryRow] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{path}: expected CSV header")
        for row_number, row in enumerate(reader, start=2):
            symbol = _normalize_symbol(str(row.get("symbol") or ""))
            d_time = str(row.get("d_time") or row.get("candidate_knowledge_time") or "").strip()
            d_ts = _parse_time(d_time)
            cd_direction = str(row.get("cd_direction") or "").strip().upper()
            cd_ab_ratio = _parse_float(row.get("CD_AB_ratio"))
            if (
                symbol not in SYMBOLS
                or d_ts is None
                or cd_direction not in {"UP", "DOWN"}
                or cd_ab_ratio is None
            ):
                raise ValueError(f"{path}:{row_number}: invalid AB=CD geometry candidate")
            year_value = _parse_float(row.get("year"))
            rows.append(
                GeometryRow(
                    candidate_id=str(row.get("candidate_id") or f"geometry_row_{row_number}"),
                    symbol=symbol,
                    year=int(year_value) if year_value is not None else _year_from_ts(d_ts),
                    d_time=d_time,
                    d_ts=d_ts,
                    d_column_id=str(row.get("d_column_id") or "").strip(),
                    d_column_sort=_column_sort(row.get("d_column_id")),
                    cd_direction=cd_direction,
                    cd_ab_ratio=cd_ab_ratio,
                    cohort=classify_cd_ab(cd_ab_ratio),
                )
            )
    if len(rows) != EXPECTED_GEOMETRY_ROWS:
        raise ValueError(
            f"geometry row count changed: expected {EXPECTED_GEOMETRY_ROWS}, observed {len(rows)}"
        )
    return rows


def _year_from_ts(timestamp: float) -> int | None:
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).year
    except (OverflowError, OSError, ValueError):
        return None


def load_reactions(path: Path) -> list[ReactionRow]:
    rows: list[ReactionRow] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{path}: expected CSV header")
        for row_number, row in enumerate(reader, start=2):
            if str(row.get("threshold_name") or "").strip().upper() != THRESHOLD_NAME:
                continue
            if str(row.get("reaction_kind") or "").strip().upper() != REACTION_KIND:
                continue
            symbol = _normalize_symbol(str(row.get("symbol") or ""))
            direction = str(row.get("candidate_direction") or "").strip().upper()
            boxes = _parse_float(_first_value(row, ("candidate_boxes", "reaction_boxes")))
            knowledge_time = str(
                _first_value(row, ("knowledge_time", "completion_time", "completed_at", "end_ts")) or ""
            ).strip()
            knowledge_ts = _parse_time(knowledge_time)
            if (
                symbol not in SYMBOLS
                or direction not in {"UP", "DOWN"}
                or boxes is None
                or knowledge_ts is None
            ):
                raise ValueError(f"{path}:{row_number}: invalid SLOW CONFIRMING reaction row")
            rows.append(
                ReactionRow(
                    symbol=symbol,
                    candidate_direction=direction,
                    candidate_boxes=boxes,
                    knowledge_time=knowledge_time,
                    knowledge_ts=knowledge_ts,
                    column_id=str(row.get("column_id") or "").strip(),
                    column_sort=_column_sort(row.get("column_id")),
                )
            )
    return sorted(rows, key=lambda row: (row.symbol, row.knowledge_ts, row.column_sort))


def measure_reactions(candidates: Sequence[GeometryRow], reactions: Sequence[ReactionRow]) -> list[Measurement]:
    by_symbol = {symbol: [row for row in reactions if row.symbol == symbol] for symbol in SYMBOLS}
    measurements: list[Measurement] = []
    for candidate in candidates:
        post_d = [
            row
            for row in by_symbol.get(candidate.symbol, [])
            if row.candidate_direction == candidate.d_reaction_direction
            and (row.knowledge_ts, row.column_sort) > (candidate.d_ts, candidate.d_column_sort)
        ]
        first = post_d[0] if post_d else None
        max_reaction = max(post_d, key=lambda row: (row.candidate_boxes, -row.knowledge_ts)) if post_d else None
        measurements.append(Measurement(candidate=candidate, first_reaction=first, max_reaction=max_reaction))
    return measurements


def summarize(rows: Sequence[Measurement], *, scope: str, cohort: str = "ALL") -> dict[str, Any]:
    scoped = list(rows) if cohort == "ALL" else [row for row in rows if row.candidate.cohort == cohort]
    max_boxes = [row.max_reaction_boxes for row in scoped]
    first_boxes = [row.first_reaction_boxes for row in scoped]
    columns = [row.columns_until_max_reaction for row in scoped if row.columns_until_max_reaction is not None]
    times = [row.time_until_max_reaction_ms for row in scoped if row.time_until_max_reaction_ms is not None]
    out: dict[str, Any] = {
        "scope": scope,
        "cohort": cohort,
        "count": len(scoped),
        "median_post_d_reaction_boxes": _median(max_boxes),
        "avg_post_d_reaction_boxes": _avg(max_boxes),
        "median_first_reaction_boxes": _median(first_boxes),
        "avg_first_reaction_boxes": _avg(first_boxes),
        "median_columns_until_max_reaction": _median(columns),
        "avg_columns_until_max_reaction": _avg(columns),
        "median_time_until_max_reaction_ms": _median(times),
        "avg_time_until_max_reaction_ms": _avg(times),
    }
    for threshold in THRESHOLDS:
        out[f"pct_reaction_gte_{threshold}_box"] = _pct(
            sum(1 for value in max_boxes if value >= threshold), len(max_boxes)
        )
    return out


def _summary_rows(rows: Sequence[Measurement], scope: str) -> list[dict[str, Any]]:
    return [summarize(rows, scope=scope, cohort=cohort) for cohort in COHORTS]


def _sample_rows(rows: Sequence[Measurement], limit: int = 250) -> list[dict[str, Any]]:
    sample = sorted(
        rows,
        key=lambda row: (
            row.candidate.symbol,
            row.candidate.d_ts,
            row.candidate.candidate_id,
        ),
    )[:limit]
    out: list[dict[str, Any]] = []
    for row in sample:
        candidate = row.candidate
        out.append(
            {
                "candidate_id": candidate.candidate_id,
                "symbol": candidate.symbol,
                "year": candidate.year or "",
                "cohort": candidate.cohort,
                "d_time": candidate.d_time,
                "d_column_id": candidate.d_column_id,
                "cd_direction": candidate.cd_direction,
                "d_reaction_direction": candidate.d_reaction_direction,
                "first_reaction_boxes": _format_number(row.first_reaction_boxes),
                "first_reaction_time": row.first_reaction.knowledge_time if row.first_reaction else "",
                "first_reaction_column_id": row.first_reaction.column_id if row.first_reaction else "",
                "max_reaction_boxes": _format_number(row.max_reaction_boxes),
                "max_reaction_time": row.max_reaction.knowledge_time if row.max_reaction else "",
                "max_reaction_column_id": row.max_reaction.column_id if row.max_reaction else "",
                "columns_until_max_reaction": row.columns_until_max_reaction or "",
                "time_until_max_reaction_ms": row.time_until_max_reaction_ms or "",
            }
        )
    return out


def _markdown_table(rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> str:
    lines = ["| " + " | ".join(fields) + " |", "| " + " | ".join("---" for _ in fields) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(field, "")) for field in fields) + " |")
    return "\n".join(lines)


def write_report(output_root: Path, summary: dict[str, Any], by_symbol: Sequence[dict[str, Any]], by_year: Sequence[dict[str, Any]], by_cohort: Sequence[dict[str, Any]]) -> None:
    decision = DECISION_STRONG if float(summary.get("pct_reaction_gte_3_box") or 0) >= 50.0 else DECISION_WEAK
    lines = [
        "# AB=CD D-Reaction Audit",
        "",
        "Research-only audit. No strategy, entries, exits, stops, targets, PnL, expectancy, profitability, or trading rules are created or evaluated.",
        "",
        "## Required answers",
        f"1. Total D-completed AB=CD candidates measured: {summary['count']}",
        f"2. Median post-D reaction boxes: {summary['median_post_d_reaction_boxes']}",
        f"3. Average post-D reaction boxes: {summary['avg_post_d_reaction_boxes']}",
        f"4. % with reaction >= 1 box: {summary['pct_reaction_gte_1_box']}",
        f"5. % with reaction >= 2 boxes: {summary['pct_reaction_gte_2_box']}",
        f"6. % with reaction >= 3 boxes: {summary['pct_reaction_gte_3_box']}",
        f"7. % with reaction >= 5 boxes: {summary['pct_reaction_gte_5_box']}",
        f"8. % with reaction >= 8 boxes: {summary['pct_reaction_gte_8_box']}",
        "9. Stability across BTCUSDT / ETHUSDT / SOLUSDT: see symbol table below.",
        "10. Stability across 2024 / 2025 / 2026: see year table below.",
        "11. Reaction strength by cohort: see cohort table below.",
        "",
        "## By symbol",
        _markdown_table(
            by_symbol,
            [
                "symbol",
                "count",
                "median_post_d_reaction_boxes",
                "avg_post_d_reaction_boxes",
                "pct_reaction_gte_3_box",
                "pct_reaction_gte_5_box",
            ],
        ),
        "",
        "## By year",
        _markdown_table(
            by_year,
            [
                "year",
                "count",
                "median_post_d_reaction_boxes",
                "avg_post_d_reaction_boxes",
                "pct_reaction_gte_3_box",
                "pct_reaction_gte_5_box",
            ],
        ),
        "",
        "## By cohort",
        _markdown_table(
            by_cohort,
            [
                "cohort",
                "count",
                "median_post_d_reaction_boxes",
                "avg_post_d_reaction_boxes",
                "pct_reaction_gte_3_box",
                "pct_reaction_gte_5_box",
            ],
        ),
        "",
        "## Final research decision",
        decision,
        "",
    ]
    (output_root / "abcd_d_reaction_report.md").write_text("\n".join(lines), encoding="utf-8")


def run_audit(geometry_input: Path, reactions_input: Path, output_root: Path) -> None:
    candidates = load_geometry(geometry_input)
    reactions = load_reactions(reactions_input)
    measurements = measure_reactions(candidates, reactions)
    summary_rows = _summary_rows(measurements, "all")
    by_symbol = []
    for symbol in SYMBOLS:
        scoped = [row for row in measurements if row.candidate.symbol == symbol]
        by_symbol.extend({"symbol": symbol, **row} for row in _summary_rows(scoped, symbol))
    by_year = []
    for year in YEARS:
        scoped = [row for row in measurements if row.candidate.year == year]
        by_year.extend({"year": year, **row} for row in _summary_rows(scoped, str(year)))
    by_cohort = _summary_rows(measurements, "cohort")

    _write_csv(output_root / "abcd_d_reaction_summary.csv", summary_rows, SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_d_reaction_by_symbol.csv", by_symbol, BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_d_reaction_by_year.csv", by_year, BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_d_reaction_by_cohort.csv", by_cohort, SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_d_reaction_sample.csv", _sample_rows(measurements), SAMPLE_FIELDS)
    write_report(
        output_root,
        summary_rows[0],
        [row for row in by_symbol if row["cohort"] == "ALL"],
        [row for row in by_year if row["cohort"] == "ALL"],
        by_cohort,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--geometry-input", type=Path, default=GEOMETRY_CANDIDATES)
    parser.add_argument("--reactions-input", type=Path, default=VALIDATED_REACTIONS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_audit(args.geometry_input, args.reactions_input, args.output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
