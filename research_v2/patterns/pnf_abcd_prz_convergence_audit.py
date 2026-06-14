"""Research-only AB=CD PRZ convergence audit.

This module consumes pre-existing local AB=CD geometry candidates and validated
SLOW/CONFIRMING harmonic reactions. It does not inspect raw datasets, does not
reconstruct ABCDs, does not use FAST artifacts, and does not create a strategy,
entries, exits, stops, targets, PnL, expectancy, profitability, or trading
rules.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import SYMBOLS, YEARS, _fmt, _parse_float

GEOMETRY_CANDIDATES = Path("research_v2/patterns/abcd_geometry_local_v1/abcd_geometry_candidates.csv")
VALIDATED_REACTIONS = Path(
    "research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/harmonic_reactions_by_threshold.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_prz_convergence_local_v1")
DEFAULT_D_REACTION_ROOT = Path("research_v2/patterns/abcd_d_mfe_local_v1")
EXPECTED_GEOMETRY_ROWS = 7823
THRESHOLD_NAME = "SLOW"
REACTION_KIND = "CONFIRMING"

RECIPROCAL_PRZ_MAP: tuple[tuple[float, tuple[float, ...]], ...] = (
    (0.382, (2.24, 2.618)),
    (0.500, (2.0,)),
    (0.618, (1.618,)),
    (0.707, (1.414,)),
    (0.786, (1.27,)),
    (0.886, (1.13,)),
)
PRZ_CLASSES = ("PRZ_TIGHT", "PRZ_ACCEPTABLE", "PRZ_LOOSE", "NO_VALID_PRZ_CONVERGENCE")
DECISION_NEXT = "PRZ_CONVERGENCE_WORTH_NEXT_PHASE"
DECISION_STOP = "PRZ_CONVERGENCE_NOT_USEFUL"
D_REACTION_FIELDS = (
    "first_post_d_reaction_boxes",
    "max_favorable_before_first_adverse_pivot",
    "columns_to_first_adverse_pivot",
    "time_to_first_adverse_pivot",
)

CANDIDATE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "d_time",
    "d_column_id",
    "BC_AB_ratio",
    "CD_AB_ratio",
    "AB_boxes",
    "BC_boxes",
    "CD_boxes",
    "bc_retracement_ratio",
    "nearest_expected_bc_retrace_ratio",
    "nearest_expected_bc_projection_ratio",
    "projected_bc_completion_distance",
    "abcd_completion_distance",
    "prz_distance",
    "prz_distance_pct_of_abcd",
    "prz_tightness_score",
    "prz_class",
    "d_reaction_join_available",
    *D_REACTION_FIELDS,
]
SUMMARY_FIELDS = [
    "scope",
    "count",
    "pct_of_total",
    "median_prz_distance_pct_of_abcd",
    "avg_prz_tightness_score",
    "d_reaction_rows",
    "median_max_favorable_before_first_adverse_pivot",
    "avg_max_favorable_before_first_adverse_pivot",
]
BY_SYMBOL_FIELDS = ["symbol", *SUMMARY_FIELDS]
BY_YEAR_FIELDS = ["year", *SUMMARY_FIELDS]
BY_CLASS_FIELDS = ["prz_class", *SUMMARY_FIELDS]


@dataclass(frozen=True)
class PrzCandidate:
    row: dict[str, Any]
    prz_class: str
    prz_distance_pct: float
    tightness_score: float


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _median(values: Sequence[float]) -> str:
    return _fmt(statistics.median(values)) if values else ""


def _avg(values: Sequence[float]) -> str:
    return _fmt(sum(values) / len(values)) if values else ""


def _nearest_projection(bc_retrace: float, implied_projection: float) -> tuple[float, float]:
    choices: list[tuple[float, float]] = []
    for retrace, projections in RECIPROCAL_PRZ_MAP:
        for projection in projections:
            choices.append((retrace, projection))
    return min(choices, key=lambda item: (abs(item[0] - bc_retrace), abs(item[1] - implied_projection)))


def _classify(distance_pct: float) -> str:
    if distance_pct <= 0.05:
        return "PRZ_TIGHT"
    if distance_pct <= 0.15:
        return "PRZ_ACCEPTABLE"
    if distance_pct <= 0.30:
        return "PRZ_LOOSE"
    return "NO_VALID_PRZ_CONVERGENCE"


def _validate_reaction_input(path: Path) -> int:
    count = 0
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{path}: expected CSV header")
        for row in reader:
            if str(row.get("threshold_name") or "").strip().upper() == THRESHOLD_NAME and str(
                row.get("reaction_kind") or ""
            ).strip().upper() == REACTION_KIND:
                count += 1
    return count


def _d_reaction_path(root: Path) -> Path | None:
    for name in ("abcd_d_mfe_candidates.csv", "abcd_d_mfe_full.csv", "abcd_d_mfe_rows.csv", "abcd_d_mfe_sample.csv"):
        candidate = root / name
        if candidate.exists():
            return candidate
    return None


def _load_d_reactions(root: Path) -> tuple[dict[str, dict[str, str]], str]:
    path = _d_reaction_path(root)
    if path is None:
        return {}, "unavailable: no full or sample bounded D-reaction output found"
    rows: dict[str, dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "candidate_id" not in reader.fieldnames:
            return {}, f"unavailable: {path.as_posix()} has no candidate_id field"
        for row in reader:
            candidate_id = str(row.get("candidate_id") or "").strip()
            if candidate_id:
                rows[candidate_id] = {field: str(row.get(field) or "") for field in D_REACTION_FIELDS}
    return rows, f"available: joined {len(rows)} rows from {path.as_posix()}"


def load_candidates(geometry_path: Path, d_reactions: dict[str, dict[str, str]]) -> list[PrzCandidate]:
    candidates: list[PrzCandidate] = []
    with geometry_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{geometry_path}: expected CSV header")
        for row_number, row in enumerate(reader, start=2):
            ab = _parse_float(row.get("AB_boxes"))
            bc = _parse_float(row.get("BC_boxes"))
            cd = _parse_float(row.get("CD_boxes"))
            bc_ratio = _parse_float(row.get("BC_AB_ratio"))
            if ab is None or bc is None or cd is None or bc_ratio is None or ab <= 0 or bc <= 0:
                raise ValueError(f"{geometry_path}:{row_number}: invalid geometry distances")
            implied_projection = ab / bc
            expected_retrace, expected_projection = _nearest_projection(bc_ratio, implied_projection)
            projected_bc_completion = bc * expected_projection
            abcd_completion = ab
            prz_distance = abs(abcd_completion - projected_bc_completion)
            distance_pct = prz_distance / abcd_completion if abcd_completion else math.inf
            tightness = max(0.0, 1.0 - distance_pct)
            prz_class = _classify(distance_pct)
            candidate_id = str(row.get("candidate_id") or f"geometry_row_{row_number}").strip()
            joined = d_reactions.get(candidate_id, {})
            out = {
                "candidate_id": candidate_id,
                "symbol": row.get("symbol", ""),
                "year": row.get("year", ""),
                "d_time": row.get("d_time", row.get("candidate_knowledge_time", "")),
                "d_column_id": row.get("d_column_id", ""),
                "BC_AB_ratio": _fmt(bc_ratio),
                "CD_AB_ratio": row.get("CD_AB_ratio", ""),
                "AB_boxes": _fmt(ab),
                "BC_boxes": _fmt(bc),
                "CD_boxes": _fmt(cd),
                "bc_retracement_ratio": _fmt(bc_ratio),
                "nearest_expected_bc_retrace_ratio": _fmt(expected_retrace),
                "nearest_expected_bc_projection_ratio": _fmt(expected_projection),
                "projected_bc_completion_distance": _fmt(projected_bc_completion),
                "abcd_completion_distance": _fmt(abcd_completion),
                "prz_distance": _fmt(prz_distance),
                "prz_distance_pct_of_abcd": _fmt(distance_pct),
                "prz_tightness_score": _fmt(tightness),
                "prz_class": prz_class,
                "d_reaction_join_available": "YES" if joined else "NO",
                **{field: joined.get(field, "") for field in D_REACTION_FIELDS},
            }
            candidates.append(PrzCandidate(out, prz_class, distance_pct, tightness))
    if len(candidates) != EXPECTED_GEOMETRY_ROWS:
        raise ValueError(f"geometry row count changed: expected {EXPECTED_GEOMETRY_ROWS}, observed {len(candidates)}")
    return candidates


def summarize(rows: Sequence[PrzCandidate], scope: str, total: int) -> dict[str, Any]:
    reaction_values = [
        value
        for value in (_parse_float(row.row.get("max_favorable_before_first_adverse_pivot")) for row in rows)
        if value is not None
    ]
    return {
        "scope": scope,
        "count": len(rows),
        "pct_of_total": _pct(len(rows), total),
        "median_prz_distance_pct_of_abcd": _median([row.prz_distance_pct for row in rows]),
        "avg_prz_tightness_score": _avg([row.tightness_score for row in rows]),
        "d_reaction_rows": len(reaction_values),
        "median_max_favorable_before_first_adverse_pivot": _median(reaction_values),
        "avg_max_favorable_before_first_adverse_pivot": _avg(reaction_values),
    }


def _stability(rows: Sequence[dict[str, Any]], key: str) -> str:
    parts = [f"{row[key]}={row['count']} ({row['pct_of_total']})" for row in rows]
    return "; ".join(parts)


def _markdown_table(rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> str:
    lines = ["| " + " | ".join(fields) + " |", "| " + " | ".join("---" for _ in fields) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(field, "")) for field in fields) + " |")
    return "\n".join(lines)


def write_report(output_root: Path, *, summary: dict[str, Any], by_symbol: Sequence[dict[str, Any]], by_year: Sequence[dict[str, Any]], by_class: Sequence[dict[str, Any]], reaction_detail: str) -> None:
    class_counts = {row["prz_class"]: row for row in by_class}
    tight = int(class_counts.get("PRZ_TIGHT", {}).get("count", 0))
    acceptable = int(class_counts.get("PRZ_ACCEPTABLE", {}).get("count", 0))
    decision = DECISION_NEXT if (tight + acceptable) / int(summary["count"] or 1) >= 0.25 else DECISION_STOP
    lines = [
        "# AB=CD PRZ Convergence Audit",
        "",
        "Research-only audit. No strategy, entries, exits, stops, targets, PnL, expectancy, profitability, trade model, ABCD reconstruction, dataset inspection, or FAST artifacts are used.",
        "",
        "## Inputs and validations",
        f"- Geometry candidates: `{GEOMETRY_CANDIDATES.as_posix()}`; hard row validation: {EXPECTED_GEOMETRY_ROWS} rows.",
        f"- Validated reactions: `{VALIDATED_REACTIONS.as_posix()}`; restricted to SLOW CONFIRMING rows for validation only.",
        f"- Bounded D-reaction join: {reaction_detail}.",
        "",
        "## Required answers",
        f"1. Total AB=CD candidates measured: {summary['count']}",
        f"2. Count and % PRZ_TIGHT: {class_counts['PRZ_TIGHT']['count']} ({class_counts['PRZ_TIGHT']['pct_of_total']})",
        f"3. Count and % PRZ_ACCEPTABLE: {class_counts['PRZ_ACCEPTABLE']['count']} ({class_counts['PRZ_ACCEPTABLE']['pct_of_total']})",
        f"4. Count and % PRZ_LOOSE: {class_counts['PRZ_LOOSE']['count']} ({class_counts['PRZ_LOOSE']['pct_of_total']})",
        f"5. Count and % NO_VALID_PRZ_CONVERGENCE: {class_counts['NO_VALID_PRZ_CONVERGENCE']['count']} ({class_counts['NO_VALID_PRZ_CONVERGENCE']['pct_of_total']})",
        f"6. Stability across BTCUSDT / ETHUSDT / SOLUSDT: {_stability(by_symbol, 'symbol')}",
        f"7. Stability across 2024 / 2025 / 2026: {_stability(by_year, 'year')}",
        "8. Bounded reaction strength by PRZ class: available in class table below." if "available:" in reaction_detail else "8. Bounded reaction strength by PRZ class: unavailable; no full or sample D-reaction candidate output was found.",
        f"9. Final research-only decision: {decision}",
        "",
        "## By PRZ class",
        _markdown_table(by_class, ["prz_class", "count", "pct_of_total", "median_prz_distance_pct_of_abcd", "avg_prz_tightness_score", "d_reaction_rows", "median_max_favorable_before_first_adverse_pivot"]),
        "",
        "## Guardrail",
        "This output is a convergence audit only and intentionally does not define or evaluate a trading model.",
        "",
    ]
    (output_root / "abcd_prz_convergence_report.md").write_text("\n".join(lines), encoding="utf-8")


def run_audit(geometry_input: Path, reactions_input: Path, output_root: Path, d_reaction_root: Path) -> None:
    slow_confirming_count = _validate_reaction_input(reactions_input)
    if slow_confirming_count <= 0:
        raise ValueError(f"{reactions_input}: expected at least one SLOW CONFIRMING reaction row")
    d_reactions, reaction_detail = _load_d_reactions(d_reaction_root)
    rows = load_candidates(geometry_input, d_reactions)
    total = len(rows)
    summary = summarize(rows, "ALL", total)
    by_symbol = [{"symbol": symbol, **summarize([row for row in rows if row.row["symbol"] == symbol], symbol, total)} for symbol in SYMBOLS]
    by_year = [{"year": year, **summarize([row for row in rows if str(row.row["year"]) == str(year)], str(year), total)} for year in YEARS]
    by_class = [{"prz_class": klass, **summarize([row for row in rows if row.prz_class == klass], klass, total)} for klass in PRZ_CLASSES]
    _write_csv(output_root / "abcd_prz_convergence_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_prz_convergence_by_symbol.csv", by_symbol, BY_SYMBOL_FIELDS)
    _write_csv(output_root / "abcd_prz_convergence_by_year.csv", by_year, BY_YEAR_FIELDS)
    _write_csv(output_root / "abcd_prz_convergence_by_prz_class.csv", by_class, BY_CLASS_FIELDS)
    _write_csv(output_root / "abcd_prz_convergence_candidates.csv", [row.row for row in rows], CANDIDATE_FIELDS)
    write_report(output_root, summary=summary, by_symbol=by_symbol, by_year=by_year, by_class=by_class, reaction_detail=reaction_detail)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--geometry-input", type=Path, default=GEOMETRY_CANDIDATES)
    parser.add_argument("--reactions-input", type=Path, default=VALIDATED_REACTIONS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--d-reaction-root", type=Path, default=DEFAULT_D_REACTION_ROOT)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_audit(args.geometry_input, args.reactions_input, args.output_root, args.d_reaction_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
