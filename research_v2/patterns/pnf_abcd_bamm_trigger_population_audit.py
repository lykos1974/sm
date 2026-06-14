"""Research-only AB=CD BAMM trigger population audit.

This script consumes an already-built AB=CD geometry candidate population and
the harmonic reaction file used to validate that each candidate's CD leg exists
in the trusted swing stream. It does not reconstruct ABCDs, does not use FAST
artifacts, does not create a new population, and does not evaluate outcomes,
expectancy, profitability, entries, exits, or strategy behavior.

BAMM trigger definition used here:

* AB=CD projected target has not yet been reached.
* CD leg is active and validated against the harmonic reactions input.
* B point is structurally broken in the CD direction.
* The projected D target remains beyond the B-break point.

For geometry-only candidates, the B-break and D-target checks are evaluated in
box-distance space:

* B is structurally broken during CD when ``CD_boxes > BC_boxes``.
* The projected AB=CD target is not yet reached at the B-break point when
  ``BC_boxes < AB_boxes``.

The harmonic reactions input is used only to verify that the candidate's CD leg
is present in the trusted reaction stream; it is not used to create additional
ABCD candidates.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import Counter
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
DEFAULT_OUTPUT_ROOT = Path(
    "research_v2/patterns/abcd_bamm_trigger_population_local_v1"
)
EXPECTED_GEOMETRY_ROWS = 7823

SUMMARY_FIELDS = [
    "geometry_candidate_rows_loaded",
    "bamm_trigger_count",
    "bamm_trigger_pct",
]
GROUP_FIELDS = [
    "group",
    "geometry_candidate_rows_loaded",
    "bamm_trigger_count",
    "bamm_trigger_pct",
]


@dataclass(frozen=True)
class GeometryCandidate:
    candidate_id: str
    symbol: str
    year: str
    c_time: str
    d_time: str
    c_column_id: str
    d_column_id: str
    cd_direction: str
    ab_boxes: float
    bc_boxes: float
    cd_boxes: float


@dataclass(frozen=True)
class ReactionKey:
    symbol: str
    direction: str
    completion_time: str
    column_id: str


def _parse_float(value: Any) -> float | None:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _fmt_pct(count: int, total: int) -> str:
    if total == 0:
        return "0"
    return f"{(count / total) * 100:.10f}".rstrip("0").rstrip(".")


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().split(":")[-1]


def _normalize_direction(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _candidate_year(row: dict[str, Any]) -> str:
    explicit_year = _normalize_text(row.get("year"))
    if explicit_year:
        return explicit_year

    for field in ("candidate_knowledge_time", "d_time", "c_time"):
        parsed = _parse_timestamp(row.get(field))
        if parsed is not None:
            return str(datetime.fromtimestamp(parsed, tz=timezone.utc).year)
    return ""


def _parse_timestamp(value: Any) -> float | None:
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


def _required(row: dict[str, Any], field: str, row_number: int) -> str:
    value = _normalize_text(row.get(field))
    if not value:
        raise ValueError(f"missing {field} on geometry row {row_number}")
    return value


def _required_float(row: dict[str, Any], field: str, row_number: int) -> float:
    parsed = _parse_float(row.get(field))
    if parsed is None:
        raise ValueError(f"missing or non-finite {field} on geometry row {row_number}")
    return parsed


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
        candidates.append(
            GeometryCandidate(
                candidate_id=_required(row, "candidate_id", row_number),
                symbol=_normalize_symbol(_required(row, "symbol", row_number)),
                year=_candidate_year(row),
                c_time=_required(row, "c_time", row_number),
                d_time=_required(row, "d_time", row_number),
                c_column_id=_required(row, "c_column_id", row_number),
                d_column_id=_required(row, "d_column_id", row_number),
                cd_direction=_normalize_direction(
                    _required(row, "cd_direction", row_number)
                ),
                ab_boxes=_required_float(row, "AB_boxes", row_number),
                bc_boxes=_required_float(row, "BC_boxes", row_number),
                cd_boxes=_required_float(row, "CD_boxes", row_number),
            )
        )
    return candidates


def _first_present(row: dict[str, Any], fields: Iterable[str]) -> str:
    for field in fields:
        value = _normalize_text(row.get(field))
        if value:
            return value
    return ""


def load_reaction_keys(path: Path) -> set[ReactionKey]:
    rows, _fields = _read_csv(path)
    keys: set[ReactionKey] = set()
    for row in rows:
        symbol = _normalize_symbol(row.get("symbol"))
        direction = _normalize_direction(
            _first_present(row, ("candidate_direction", "direction", "leg_direction"))
        )
        completion_time = _first_present(
            row,
            (
                "completion_time",
                "pivot_time",
                "knowledge_time",
                "confirmed_time",
                "time",
            ),
        )
        column_id = _first_present(
            row,
            (
                "column_id",
                "pivot_column_id",
                "completion_column_id",
                "confirmed_column_id",
            ),
        )
        if symbol and direction and (completion_time or column_id):
            keys.add(
                ReactionKey(
                    symbol=symbol,
                    direction=direction,
                    completion_time=completion_time,
                    column_id=column_id,
                )
            )
    return keys


def cd_leg_is_validated(candidate: GeometryCandidate, keys: set[ReactionKey]) -> bool:
    by_time = ReactionKey(
        symbol=candidate.symbol,
        direction=candidate.cd_direction,
        completion_time=candidate.d_time,
        column_id="",
    )
    by_column = ReactionKey(
        symbol=candidate.symbol,
        direction=candidate.cd_direction,
        completion_time="",
        column_id=candidate.d_column_id,
    )
    exact = ReactionKey(
        symbol=candidate.symbol,
        direction=candidate.cd_direction,
        completion_time=candidate.d_time,
        column_id=candidate.d_column_id,
    )
    return exact in keys or by_time in keys or by_column in keys


def has_bamm_trigger(candidate: GeometryCandidate, keys: set[ReactionKey]) -> bool:
    if not cd_leg_is_validated(candidate, keys):
        return False

    b_break_occurs_during_cd = candidate.cd_boxes > candidate.bc_boxes
    projected_target_unreached_at_b_break = candidate.bc_boxes < candidate.ab_boxes
    return b_break_occurs_during_cd and projected_target_unreached_at_b_break


def _group_rows(candidates: Sequence[GeometryCandidate], triggered_ids: set[str], attr: str) -> list[dict[str, Any]]:
    totals: Counter[str] = Counter()
    triggers: Counter[str] = Counter()
    for candidate in candidates:
        group = getattr(candidate, attr) or "UNKNOWN"
        totals[group] += 1
        if candidate.candidate_id in triggered_ids:
            triggers[group] += 1

    return [
        {
            "group": group,
            "geometry_candidate_rows_loaded": totals[group],
            "bamm_trigger_count": triggers[group],
            "bamm_trigger_pct": _fmt_pct(triggers[group], totals[group]),
        }
        for group in sorted(totals)
    ]


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_report(
    path: Path,
    geometry_count: int,
    trigger_count: int,
    by_symbol: Sequence[dict[str, Any]],
    by_year: Sequence[dict[str, Any]],
) -> None:
    lines = [
        f"Geometry candidate rows loaded: {geometry_count}",
        "",
        "# AB=CD BAMM Trigger Population Audit",
        "",
        "## Scope",
        "",
        "- Research-only population audit.",
        "- Loaded existing AB=CD geometry candidates only.",
        "- Used harmonic reactions only to validate candidate CD-leg presence.",
        "- No ABCD reconstruction.",
        "- No FAST artifacts.",
        "- No new population creation.",
        "- No outcome analysis, expectancy, profitability, or strategy logic.",
        "",
        "## Required Answers",
        "",
        f"1. Geometry candidate rows loaded: {geometry_count}",
        f"2. BAMM trigger count: {trigger_count}",
        f"3. BAMM trigger %: {_fmt_pct(trigger_count, geometry_count)}",
        "",
        "## By Symbol",
        "",
        "| Symbol | Geometry Candidates | BAMM Trigger Count | BAMM Trigger % |",
        "|---|---:|---:|---:|",
    ]
    lines.extend(
        f"| {row['group']} | {row['geometry_candidate_rows_loaded']} | "
        f"{row['bamm_trigger_count']} | {row['bamm_trigger_pct']} |"
        for row in by_symbol
    )
    lines.extend(
        [
            "",
            "## By Year",
            "",
            "| Year | Geometry Candidates | BAMM Trigger Count | BAMM Trigger % |",
            "|---|---:|---:|---:|",
        ]
    )
    lines.extend(
        f"| {row['group']} | {row['geometry_candidate_rows_loaded']} | "
        f"{row['bamm_trigger_count']} | {row['bamm_trigger_pct']} |"
        for row in by_year
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_audit(
    geometry_candidates_path: Path,
    reactions_path: Path,
    output_root: Path,
    expected_geometry_rows: int,
) -> None:
    candidates = load_geometry_candidates(geometry_candidates_path, expected_geometry_rows)
    reaction_keys = load_reaction_keys(reactions_path)

    triggered_ids = {
        candidate.candidate_id
        for candidate in candidates
        if has_bamm_trigger(candidate, reaction_keys)
    }
    trigger_count = len(triggered_ids)
    summary = [
        {
            "geometry_candidate_rows_loaded": len(candidates),
            "bamm_trigger_count": trigger_count,
            "bamm_trigger_pct": _fmt_pct(trigger_count, len(candidates)),
        }
    ]
    by_symbol = _group_rows(candidates, triggered_ids, "symbol")
    by_year = _group_rows(candidates, triggered_ids, "year")

    _write_csv(output_root / "abcd_bamm_trigger_summary.csv", summary, SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_bamm_trigger_by_symbol.csv", by_symbol, GROUP_FIELDS)
    _write_csv(output_root / "abcd_bamm_trigger_by_year.csv", by_year, GROUP_FIELDS)
    _write_report(
        output_root / "abcd_bamm_trigger_report.md",
        len(candidates),
        trigger_count,
        by_symbol,
        by_year,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Research-only AB=CD BAMM trigger population audit."
    )
    parser.add_argument(
        "--geometry-candidates",
        type=Path,
        default=DEFAULT_GEOMETRY_CANDIDATES,
        help="Existing abcd_geometry_candidates.csv input path.",
    )
    parser.add_argument(
        "--reactions",
        type=Path,
        default=DEFAULT_REACTIONS,
        help="Trusted harmonic_reactions_by_threshold.csv input path.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Directory where BAMM trigger audit outputs will be written.",
    )
    parser.add_argument(
        "--expected-geometry-rows",
        type=int,
        default=EXPECTED_GEOMETRY_ROWS,
        help="Required geometry candidate row count. The script exits if it differs.",
    )
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
