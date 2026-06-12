"""Phase 2 research-only geometry audit for PnF AB=CD structures.

This module measures AB/CD geometry only for completed ABCD structures. It uses
only the Phase 2-approved research inputs, does not compute expectancy, does not
create a detector, does not create a strategy, does not create entries/exits, and
never uses future price outcomes.
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

TRUSTED_PIVOT_ROOT = Path("research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3")
TRUSTED_POPULATION_ROOT = Path("research_v2/patterns/abcd_population_local_v2")
DESIGN_DOC = Path("research_v2/patterns/pnf_abcd_symmetry_audit_design_v2.md")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_geometry_local_v1")
REACTIONS_FILENAME = "harmonic_reactions_by_threshold.csv"
THRESHOLD_NAME = "SLOW"
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
YEARS = (2024, 2025, 2026)
DIRECTIONS = {"UP", "DOWN"}

CANDIDATE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "candidate_knowledge_time",
    "a_time",
    "b_time",
    "c_time",
    "d_time",
    "a_column_id",
    "b_column_id",
    "c_column_id",
    "d_column_id",
    "ab_direction",
    "bc_direction",
    "cd_direction",
    "AB_boxes",
    "BC_boxes",
    "CD_boxes",
    "BC_AB_ratio",
    "CD_AB_ratio",
    "BC_AB_zone",
    "CD_AB_zone",
]
SUMMARY_FIELDS = [
    "scope",
    "completed_abcd",
    "sym_0_90_1_10_count",
    "sym_0_90_1_10_pct",
    "ext_1_20_1_35_count",
    "ext_1_20_1_35_pct",
    "ext_1_55_1_70_count",
    "ext_1_55_1_70_pct",
    "other_count",
    "other_pct",
    "shallow_lt_0_40_count",
    "mid_0_40_0_70_count",
    "deep_gt_0_70_count",
]
GROUP_FIELDS = [field for field in SUMMARY_FIELDS if field != "scope"]


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
class GeometryCandidate:
    candidate_id: str
    symbol: str
    year: int | None
    candidate_knowledge_time: str
    a_time: str
    b_time: str
    c_time: str
    d_time: str
    a_column_id: str
    b_column_id: str
    c_column_id: str
    d_column_id: str
    ab_direction: str
    bc_direction: str
    cd_direction: str
    ab_boxes: float
    bc_boxes: float
    cd_boxes: float
    bc_ab_ratio: float
    cd_ab_ratio: float
    bc_ab_zone: str
    cd_ab_zone: str

    def as_row(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "symbol": self.symbol,
            "year": self.year if self.year is not None else "",
            "candidate_knowledge_time": self.candidate_knowledge_time,
            "a_time": self.a_time,
            "b_time": self.b_time,
            "c_time": self.c_time,
            "d_time": self.d_time,
            "a_column_id": self.a_column_id,
            "b_column_id": self.b_column_id,
            "c_column_id": self.c_column_id,
            "d_column_id": self.d_column_id,
            "ab_direction": self.ab_direction,
            "bc_direction": self.bc_direction,
            "cd_direction": self.cd_direction,
            "AB_boxes": _fmt(self.ab_boxes),
            "BC_boxes": _fmt(self.bc_boxes),
            "CD_boxes": _fmt(self.cd_boxes),
            "BC_AB_ratio": _fmt(self.bc_ab_ratio),
            "CD_AB_ratio": _fmt(self.cd_ab_ratio),
            "BC_AB_zone": self.bc_ab_zone,
            "CD_AB_zone": self.cd_ab_zone,
        }


def _fmt(value: float) -> str:
    return f"{value:.10f}".rstrip("0").rstrip(".")


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


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _trusted_reactions_path(input_root: Path) -> Path | None:
    for candidate in (input_root / REACTIONS_FILENAME, input_root / "audit" / REACTIONS_FILENAME):
        if candidate.is_file():
            return candidate
    return None


def _population_candidate_path(population_root: Path) -> Path | None:
    names = (
        "abcd_population_candidates.csv",
        "abcd_candidates.csv",
        "abcd_candidate_population.csv",
    )
    for name in names:
        candidate = population_root / name
        if candidate.is_file():
            return candidate
    return None


def classify_cd_ab(ratio: float) -> str:
    if 0.90 <= ratio < 1.10:
        return "SYM_0_90_1_10"
    if 1.20 <= ratio < 1.35:
        return "EXT_1_20_1_35"
    if 1.55 <= ratio < 1.70:
        return "EXT_1_55_1_70"
    return "OTHER"


def classify_bc_ab(ratio: float) -> str:
    if ratio < 0.40:
        return "SHALLOW_LT_0_40"
    if ratio <= 0.70:
        return "MID_0_40_0_70"
    return "DEEP_GT_0_70"


def _valid_completed(a: Pivot, b: Pivot, c: Pivot, d: Pivot) -> bool:
    del a
    return (
        b.candidate_direction != c.candidate_direction
        and c.candidate_direction != d.candidate_direction
        and b.candidate_direction == d.candidate_direction
        and b.candidate_boxes > 0
        and c.candidate_boxes > 0
        and d.candidate_boxes > 0
    )


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


def _candidate_from_pivots(a: Pivot, b: Pivot, c: Pivot, d: Pivot) -> GeometryCandidate:
    bc_ab_ratio = c.candidate_boxes / b.candidate_boxes
    cd_ab_ratio = d.candidate_boxes / b.candidate_boxes
    year = _year_from_ts(d.knowledge_ts)
    candidate_id = "ABCD:" + ":".join([a.pivot_id, b.pivot_id, c.pivot_id, d.pivot_id])
    return GeometryCandidate(
        candidate_id=candidate_id,
        symbol=d.symbol,
        year=year,
        candidate_knowledge_time=d.knowledge_time,
        a_time=a.knowledge_time,
        b_time=b.knowledge_time,
        c_time=c.knowledge_time,
        d_time=d.knowledge_time,
        a_column_id=a.column_id,
        b_column_id=b.column_id,
        c_column_id=c.column_id,
        d_column_id=d.column_id,
        ab_direction=b.candidate_direction,
        bc_direction=c.candidate_direction,
        cd_direction=d.candidate_direction,
        ab_boxes=b.candidate_boxes,
        bc_boxes=c.candidate_boxes,
        cd_boxes=d.candidate_boxes,
        bc_ab_ratio=bc_ab_ratio,
        cd_ab_ratio=cd_ab_ratio,
        bc_ab_zone=classify_bc_ab(bc_ab_ratio),
        cd_ab_zone=classify_cd_ab(cd_ab_ratio),
    )


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


def geometry_from_pivots(pivots: Sequence[Pivot]) -> list[GeometryCandidate]:
    rows: list[GeometryCandidate] = []
    by_symbol = {symbol: [] for symbol in SYMBOLS}
    for pivot in pivots:
        by_symbol.setdefault(pivot.symbol, []).append(pivot)
    for symbol in SYMBOLS:
        ordered = _sort_pivots(by_symbol.get(symbol, []))
        for index in range(len(ordered) - 3):
            a, b, c, d = ordered[index], ordered[index + 1], ordered[index + 2], ordered[index + 3]
            if _valid_completed(a, b, c, d):
                rows.append(_candidate_from_pivots(a, b, c, d))
    return rows


def _row_value(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and str(row.get(name) or "").strip() != "":
            return row.get(name)
    return None


def geometry_from_population(path: Path) -> list[GeometryCandidate]:
    rows: list[GeometryCandidate] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{path}: expected CSV header")
        for row_number, row in enumerate(reader, start=2):
            state = str(_row_value(row, "state", "candidate_state") or "").strip().upper()
            if state and state != "COMPLETED_ABCD":
                continue
            symbol = _normalize_symbol(str(_row_value(row, "symbol") or ""))
            if symbol not in SYMBOLS:
                continue
            ab_boxes = _parse_float(_row_value(row, "AB_boxes", "ab_boxes"))
            bc_boxes = _parse_float(_row_value(row, "BC_boxes", "bc_boxes"))
            cd_boxes = _parse_float(_row_value(row, "CD_boxes", "cd_boxes"))
            if ab_boxes is None or bc_boxes is None or cd_boxes is None:
                raise ValueError(f"{path}:{row_number}: missing AB/BC/CD box geometry")
            if ab_boxes <= 0 or bc_boxes <= 0 or cd_boxes <= 0:
                raise ValueError(f"{path}:{row_number}: non-positive AB/BC/CD box geometry")
            d_time = str(_row_value(row, "d_time", "candidate_knowledge_time", "state_knowledge_time") or "").strip()
            d_ts = _parse_time(d_time)
            year = _year_from_ts(d_ts) if d_ts is not None else None
            bc_ab_ratio = bc_boxes / ab_boxes
            cd_ab_ratio = cd_boxes / ab_boxes
            rows.append(
                GeometryCandidate(
                    candidate_id=str(_row_value(row, "candidate_id", "state_id") or f"population_row_{row_number}"),
                    symbol=symbol,
                    year=year,
                    candidate_knowledge_time=str(_row_value(row, "candidate_knowledge_time", "state_knowledge_time", "d_time") or ""),
                    a_time=str(_row_value(row, "a_time") or ""),
                    b_time=str(_row_value(row, "b_time") or ""),
                    c_time=str(_row_value(row, "c_time") or ""),
                    d_time=d_time,
                    a_column_id=str(_row_value(row, "a_column_id") or ""),
                    b_column_id=str(_row_value(row, "b_column_id") or ""),
                    c_column_id=str(_row_value(row, "c_column_id") or ""),
                    d_column_id=str(_row_value(row, "d_column_id") or ""),
                    ab_direction=str(_row_value(row, "ab_direction") or ""),
                    bc_direction=str(_row_value(row, "bc_direction") or ""),
                    cd_direction=str(_row_value(row, "cd_direction") or _row_value(row, "ab_direction") or ""),
                    ab_boxes=ab_boxes,
                    bc_boxes=bc_boxes,
                    cd_boxes=cd_boxes,
                    bc_ab_ratio=bc_ab_ratio,
                    cd_ab_ratio=cd_ab_ratio,
                    bc_ab_zone=classify_bc_ab(bc_ab_ratio),
                    cd_ab_zone=classify_cd_ab(cd_ab_ratio),
                )
            )
    return rows


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else "0"


def summarize(candidates: Sequence[GeometryCandidate], scope: str | None = None) -> dict[str, Any]:
    total = len(candidates)
    cd_counts = {
        "SYM_0_90_1_10": 0,
        "EXT_1_20_1_35": 0,
        "EXT_1_55_1_70": 0,
        "OTHER": 0,
    }
    bc_counts = {
        "SHALLOW_LT_0_40": 0,
        "MID_0_40_0_70": 0,
        "DEEP_GT_0_70": 0,
    }
    for candidate in candidates:
        cd_counts[candidate.cd_ab_zone] += 1
        bc_counts[candidate.bc_ab_zone] += 1
    row: dict[str, Any] = {
        "completed_abcd": total,
        "sym_0_90_1_10_count": cd_counts["SYM_0_90_1_10"],
        "sym_0_90_1_10_pct": _pct(cd_counts["SYM_0_90_1_10"], total),
        "ext_1_20_1_35_count": cd_counts["EXT_1_20_1_35"],
        "ext_1_20_1_35_pct": _pct(cd_counts["EXT_1_20_1_35"], total),
        "ext_1_55_1_70_count": cd_counts["EXT_1_55_1_70"],
        "ext_1_55_1_70_pct": _pct(cd_counts["EXT_1_55_1_70"], total),
        "other_count": cd_counts["OTHER"],
        "other_pct": _pct(cd_counts["OTHER"], total),
        "shallow_lt_0_40_count": bc_counts["SHALLOW_LT_0_40"],
        "mid_0_40_0_70_count": bc_counts["MID_0_40_0_70"],
        "deep_gt_0_70_count": bc_counts["DEEP_GT_0_70"],
    }
    if scope is not None:
        row = {"scope": scope, **row}
    return row


def _group_rows(candidates: Sequence[GeometryCandidate], key_name: str) -> list[dict[str, Any]]:
    if key_name == "symbol":
        keys: Sequence[Any] = SYMBOLS
        getter = lambda candidate: candidate.symbol
    elif key_name == "year":
        keys = YEARS
        getter = lambda candidate: candidate.year
    else:
        raise ValueError(key_name)
    rows = []
    for key in keys:
        grouped = [candidate for candidate in candidates if getter(candidate) == key]
        rows.append({key_name: key, **summarize(grouped)})
    return rows


def _zone_stability(rows: Sequence[dict[str, Any]], label: str) -> str:
    populated = [row for row in rows if int(row["completed_abcd"]) > 0]
    if not populated:
        return f"Not determined; no completed candidates were available by {label}."
    all_present = all(
        int(row["sym_0_90_1_10_count"]) > 0
        and int(row["ext_1_20_1_35_count"]) > 0
        and int(row["ext_1_55_1_70_count"]) > 0
        for row in populated
    )
    if all_present:
        return f"Yes descriptively: all populated {label} contain all three requested CD/AB zones."
    return f"Mixed descriptively: at least one populated {label} is missing one or more requested CD/AB zones."


def write_report(
    output_root: Path,
    *,
    source_detail: str,
    candidates: Sequence[GeometryCandidate],
    summary: dict[str, Any],
    symbol_rows: Sequence[dict[str, Any]],
    year_rows: Sequence[dict[str, Any]],
) -> None:
    phase3_ok = int(summary["sym_0_90_1_10_count"]) >= 30
    lines = [
        "# AB=CD Phase 2 Geometry Audit Report",
        "",
        "## Scope",
        "- Research only: AB/CD geometry measurement for completed ABCD candidates.",
        f"- Approved source detail: {source_detail}.",
        f"- Causal design: `{DESIGN_DOC.as_posix()}`.",
        "- No expectancy, detector, strategy, entries/exits, or future price outcomes were computed or used.",
        "- The validated `0.40` split is used only as descriptive BC/AB context.",
        "",
        "## Total Geometry Counts",
        f"- Completed ABCD candidates measured: {summary['completed_abcd']}",
        f"- CD/AB `SYM_0_90_1_10`: {summary['sym_0_90_1_10_count']} ({summary['sym_0_90_1_10_pct']})",
        f"- CD/AB `EXT_1_20_1_35`: {summary['ext_1_20_1_35_count']} ({summary['ext_1_20_1_35_pct']})",
        f"- CD/AB `EXT_1_55_1_70`: {summary['ext_1_55_1_70_count']} ({summary['ext_1_55_1_70_pct']})",
        f"- CD/AB `OTHER`: {summary['other_count']} ({summary['other_pct']})",
        "",
        "## Answers",
        f"1. **Near CD/AB symmetry 0.90–1.10:** {summary['sym_0_90_1_10_count']} completed structures.",
        f"2. **Near extension 1.20–1.35:** {summary['ext_1_20_1_35_count']} completed structures.",
        f"3. **Near extension 1.55–1.70:** {summary['ext_1_55_1_70_count']} completed structures.",
        f"4. **Stable across BTC/ETH/SOL?** {_zone_stability(symbol_rows, 'symbols')}",
        f"5. **Stable across 2024/2025/2026?** {_zone_stability(year_rows, 'years')}",
        f"6. **Is AB=CD symmetry common enough to justify Phase 3 outcome research?** {'Yes for descriptive follow-up: the symmetry zone clears a minimal 30-structure research floor, but this report makes no expectancy claim.' if phase3_ok else 'No conclusion from this workspace: the symmetry zone does not clear a minimal 30-structure research floor or inputs were unavailable.'}",
        "",
        "## By Symbol",
        "| Symbol | Completed | SYM 0.90-1.10 | EXT 1.20-1.35 | EXT 1.55-1.70 | OTHER | Shallow BC<0.40 | Mid BC 0.40-0.70 | Deep BC>0.70 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in symbol_rows:
        lines.append(
            f"| {row['symbol']} | {row['completed_abcd']} | {row['sym_0_90_1_10_count']} | {row['ext_1_20_1_35_count']} | {row['ext_1_55_1_70_count']} | {row['other_count']} | {row['shallow_lt_0_40_count']} | {row['mid_0_40_0_70_count']} | {row['deep_gt_0_70_count']} |"
        )
    lines.extend([
        "",
        "## By Year",
        "| Year | Completed | SYM 0.90-1.10 | EXT 1.20-1.35 | EXT 1.55-1.70 | OTHER | Shallow BC<0.40 | Mid BC 0.40-0.70 | Deep BC>0.70 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in year_rows:
        lines.append(
            f"| {row['year']} | {row['completed_abcd']} | {row['sym_0_90_1_10_count']} | {row['ext_1_20_1_35_count']} | {row['ext_1_55_1_70_count']} | {row['other_count']} | {row['shallow_lt_0_40_count']} | {row['mid_0_40_0_70_count']} | {row['deep_gt_0_70_count']} |"
        )
    lines.extend([
        "",
        "## Research Guardrail",
        "This is a geometry audit only. It does not compute expectancy, define a detector, create a strategy, define entries/exits, use future price outcomes, or promote any trading rule.",
        "",
    ])
    (output_root / "abcd_geometry_report.md").write_text("\n".join(lines), encoding="utf-8")


def _write_blocked_outputs(output_root: Path, *, reason: str) -> None:
    _write_csv(output_root / "abcd_geometry_candidates.csv", [], CANDIDATE_FIELDS)
    _write_csv(output_root / "abcd_geometry_summary.csv", [summarize([], "ALL")], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_geometry_by_symbol.csv", _group_rows([], "symbol"), ["symbol", *GROUP_FIELDS])
    _write_csv(output_root / "abcd_geometry_by_year.csv", _group_rows([], "year"), ["year", *GROUP_FIELDS])
    report = [
        "# AB=CD Phase 2 Geometry Audit Report",
        "",
        "## Status",
        "BLOCKED — Phase 2-approved input artifacts are not available in this workspace.",
        "",
        "## Reason",
        reason,
        "",
        "## Approved Input Requirement",
        f"- Pivot root: `{TRUSTED_PIVOT_ROOT.as_posix()}`",
        f"- Population root: `{TRUSTED_POPULATION_ROOT.as_posix()}`",
        f"- Design: `{DESIGN_DOC.as_posix()}`",
        "- No fallback to other local artifacts was used.",
        "- No expectancy, detector, strategy, entries/exits, future price outcomes, or non-approved inputs were used.",
        "",
        "## Answers",
        "1. **Near CD/AB symmetry 0.90–1.10:** Not computed; approved input artifacts are missing.",
        "2. **Near extension 1.20–1.35:** Not computed; approved input artifacts are missing.",
        "3. **Near extension 1.55–1.70:** Not computed; approved input artifacts are missing.",
        "4. **Stable across BTC/ETH/SOL?** Not determined.",
        "5. **Stable across 2024/2025/2026?** Not determined.",
        "6. **Is AB=CD symmetry common enough to justify Phase 3 outcome research?** Not determined from this workspace.",
        "",
        "## Research Guardrail",
        "Geometry audit only. No expectancy / no trading / no detector.",
        "",
    ]
    (output_root / "abcd_geometry_report.md").write_text("\n".join(report), encoding="utf-8")


def run_audit(
    *,
    pivot_root: str | Path = TRUSTED_PIVOT_ROOT,
    population_root: str | Path = TRUSTED_POPULATION_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> bool:
    pivot_path = Path(pivot_root)
    population_path = Path(population_root)
    output_path = Path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)

    try:
        population_candidates = _population_candidate_path(population_path)
        if population_candidates is not None:
            candidates = geometry_from_population(population_candidates)
            source_detail = f"population candidates `{population_candidates.as_posix()}`"
        else:
            pivots, _rejects, reactions_path = load_validated_pivots(pivot_path)
            candidates = geometry_from_pivots(pivots)
            source_detail = f"rolling completed candidates from `{reactions_path.as_posix()}`"
    except (FileNotFoundError, ValueError) as exc:
        _write_blocked_outputs(output_path, reason=str(exc))
        return False

    candidate_rows = [candidate.as_row() for candidate in candidates]
    summary = summarize(candidates, "ALL")
    symbol_rows = _group_rows(candidates, "symbol")
    year_rows = _group_rows(candidates, "year")
    _write_csv(output_path / "abcd_geometry_candidates.csv", candidate_rows, CANDIDATE_FIELDS)
    _write_csv(output_path / "abcd_geometry_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_path / "abcd_geometry_by_symbol.csv", symbol_rows, ["symbol", *GROUP_FIELDS])
    _write_csv(output_path / "abcd_geometry_by_year.csv", year_rows, ["year", *GROUP_FIELDS])
    write_report(
        output_path,
        source_detail=source_detail,
        candidates=candidates,
        summary=summary,
        symbol_rows=symbol_rows,
        year_rows=year_rows,
    )
    return True


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pivot-root", type=Path, default=TRUSTED_PIVOT_ROOT)
    parser.add_argument("--population-root", type=Path, default=TRUSTED_POPULATION_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero when Phase 2-approved inputs are missing or invalid",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    ok = run_audit(
        pivot_root=args.pivot_root,
        population_root=args.population_root,
        output_root=args.output_root,
    )
    if args.strict and not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
