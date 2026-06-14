"""Research-only AB=CD D-confirmation audit.

This module consumes only two pre-existing local research artifacts:

- bounded D-reaction candidates from ``abcd_d_mfe_local_v1``
- geometry candidates from ``abcd_geometry_local_v1``

It validates both artifact populations, joins them by ``candidate_id``, and asks
whether early post-D confirmation strength separates bounded reaction quality.
It does not inspect raw datasets, reconstruct ABCDs, use FAST artifacts, create
entries/exits/stops/targets, define a strategy, or compute PnL, expectancy,
profitability, or trading rules.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_D_REACTION_CANDIDATES = Path(
    "research_v2/patterns/abcd_d_mfe_local_v1/abcd_d_mfe_candidates.csv"
)
DEFAULT_GEOMETRY_CANDIDATES = Path(
    "research_v2/patterns/abcd_geometry_local_v1/abcd_geometry_candidates.csv"
)
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_d_confirmation_local_v1")
EXPECTED_ROWS = 7823

DECISION_NEXT = "D_CONFIRMATION_WORTH_NEXT_PHASE"
DECISION_STOP = "D_CONFIRMATION_NOT_USEFUL"
QUALITY_ORDER = ("STRONG", "MEDIUM", "WEAK")

REACTION_FIELDS = [
    "first_post_d_reaction_boxes",
    "first_post_d_reaction_pct_of_AB",
    "first_post_d_reaction_pct_of_BC",
    "first_post_d_reaction_pct_of_CD",
    "columns_to_first_adverse_pivot",
    "time_to_first_adverse_pivot",
    "max_favorable_before_first_adverse_pivot",
]

CANDIDATE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "d_time",
    "d_column_id",
    "cd_direction",
    "AB_boxes",
    "BC_boxes",
    "CD_boxes",
    *REACTION_FIELDS,
    "bounded_reaction_quality",
]

SUMMARY_FIELDS = [
    "total_candidates_measured",
    "strong_count",
    "strong_pct",
    "medium_count",
    "medium_pct",
    "weak_count",
    "weak_pct",
    "median_first_post_d_reaction_boxes_strong",
    "median_first_post_d_reaction_boxes_medium",
    "median_first_post_d_reaction_boxes_weak",
    "median_first_post_d_reaction_pct_of_AB_strong",
    "median_first_post_d_reaction_pct_of_AB_medium",
    "median_first_post_d_reaction_pct_of_AB_weak",
    "median_first_post_d_reaction_pct_of_CD_strong",
    "median_first_post_d_reaction_pct_of_CD_medium",
    "median_first_post_d_reaction_pct_of_CD_weak",
    "strong_minus_weak_median_first_post_d_reaction_boxes",
    "strong_minus_weak_median_first_post_d_reaction_pct_of_AB",
    "strong_minus_weak_median_first_post_d_reaction_pct_of_CD",
    "early_post_d_confirmation_separates_strong_from_weak",
    "stable_across_symbols",
    "stable_across_years",
    "final_research_only_decision",
]

GROUP_FIELDS = [
    "group",
    "total_candidates_measured",
    "strong_count",
    "strong_pct",
    "medium_count",
    "medium_pct",
    "weak_count",
    "weak_pct",
    "median_first_post_d_reaction_boxes_strong",
    "median_first_post_d_reaction_boxes_medium",
    "median_first_post_d_reaction_boxes_weak",
    "median_first_post_d_reaction_pct_of_AB_strong",
    "median_first_post_d_reaction_pct_of_AB_medium",
    "median_first_post_d_reaction_pct_of_AB_weak",
    "median_first_post_d_reaction_pct_of_CD_strong",
    "median_first_post_d_reaction_pct_of_CD_medium",
    "median_first_post_d_reaction_pct_of_CD_weak",
    "early_post_d_confirmation_separates_strong_from_weak",
]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        parsed = float(_text(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _median(values: Iterable[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    return statistics.median(clean) if clean else None


def _read_csv_exact(path: Path, expected_rows: int, label: str) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{label} input {path.as_posix()} has no CSV header")
        rows = list(reader)
    if len(rows) != expected_rows:
        raise ValueError(
            f"{label} row count changed: expected {expected_rows}, observed {len(rows)} at {path.as_posix()}"
        )
    return rows


def _classify_quality(max_favorable: float | None) -> str:
    if max_favorable is None:
        raise ValueError("missing max_favorable_before_first_adverse_pivot")
    if max_favorable >= 13:
        return "STRONG"
    if max_favorable >= 8:
        return "MEDIUM"
    return "WEAK"


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def build_candidates(
    d_reaction_path: Path = DEFAULT_D_REACTION_CANDIDATES,
    geometry_path: Path = DEFAULT_GEOMETRY_CANDIDATES,
) -> list[dict[str, Any]]:
    """Load validated artifacts and compute D-confirmation candidate features."""
    reaction_rows = _read_csv_exact(d_reaction_path, EXPECTED_ROWS, "bounded D-reaction candidates")
    geometry_rows = _read_csv_exact(geometry_path, EXPECTED_ROWS, "geometry candidates")

    geometry_by_id: dict[str, dict[str, str]] = {}
    for row in geometry_rows:
        candidate_id = _text(row.get("candidate_id"))
        if not candidate_id:
            raise ValueError("geometry candidates contain a row without candidate_id")
        if candidate_id in geometry_by_id:
            raise ValueError(f"duplicate geometry candidate_id: {candidate_id}")
        geometry_by_id[candidate_id] = row

    candidates: list[dict[str, Any]] = []
    seen_reaction_ids: set[str] = set()
    for reaction in reaction_rows:
        candidate_id = _text(reaction.get("candidate_id"))
        if not candidate_id:
            raise ValueError("bounded D-reaction candidates contain a row without candidate_id")
        if candidate_id in seen_reaction_ids:
            raise ValueError(f"duplicate bounded D-reaction candidate_id: {candidate_id}")
        seen_reaction_ids.add(candidate_id)
        geometry = geometry_by_id.get(candidate_id)
        if geometry is None:
            raise ValueError(f"bounded D-reaction candidate_id not found in geometry candidates: {candidate_id}")

        first_boxes = _parse_float(reaction.get("first_post_d_reaction_boxes"))
        max_favorable = _parse_float(reaction.get("max_favorable_before_first_adverse_pivot"))
        ab_boxes = _parse_float(geometry.get("AB_boxes"))
        bc_boxes = _parse_float(geometry.get("BC_boxes"))
        cd_boxes = _parse_float(geometry.get("CD_boxes"))
        if ab_boxes is None or bc_boxes is None or cd_boxes is None:
            raise ValueError(f"candidate {candidate_id}: missing geometry box distance")

        candidates.append(
            {
                "candidate_id": candidate_id,
                "symbol": _text(geometry.get("symbol") or reaction.get("symbol")),
                "year": _text(geometry.get("year") or reaction.get("year")),
                "d_time": _text(
                    geometry.get("d_time")
                    or geometry.get("candidate_knowledge_time")
                    or reaction.get("d_knowledge_time")
                ),
                "d_column_id": _text(geometry.get("d_column_id") or reaction.get("d_column_id")),
                "cd_direction": _text(geometry.get("cd_direction") or reaction.get("cd_direction")),
                "AB_boxes": _fmt(ab_boxes),
                "BC_boxes": _fmt(bc_boxes),
                "CD_boxes": _fmt(cd_boxes),
                "first_post_d_reaction_boxes": _fmt(first_boxes),
                "first_post_d_reaction_pct_of_AB": _fmt(_safe_ratio(first_boxes, ab_boxes)),
                "first_post_d_reaction_pct_of_BC": _fmt(_safe_ratio(first_boxes, bc_boxes)),
                "first_post_d_reaction_pct_of_CD": _fmt(_safe_ratio(first_boxes, cd_boxes)),
                "columns_to_first_adverse_pivot": _text(reaction.get("columns_to_first_adverse_pivot")),
                "time_to_first_adverse_pivot": _text(reaction.get("time_to_first_adverse_pivot")),
                "max_favorable_before_first_adverse_pivot": _fmt(max_favorable),
                "bounded_reaction_quality": _classify_quality(max_favorable),
            }
        )

    missing_reactions = sorted(set(geometry_by_id) - seen_reaction_ids)
    if missing_reactions:
        raise ValueError(f"geometry candidates missing bounded D-reaction rows: {missing_reactions[:5]}")
    return candidates


def _quality_median(rows: Sequence[dict[str, Any]], quality: str, field: str) -> float | None:
    return _median(_parse_float(row.get(field)) for row in rows if row.get("bounded_reaction_quality") == quality)


def _separates(rows: Sequence[dict[str, Any]]) -> str:
    strong = _quality_median(rows, "STRONG", "first_post_d_reaction_boxes")
    weak = _quality_median(rows, "WEAK", "first_post_d_reaction_boxes")
    if strong is None or weak is None:
        return "NO"
    return "YES" if strong > weak else "NO"


def summarize(rows: Sequence[dict[str, Any]], include_stability: bool = False) -> dict[str, Any]:
    total = len(rows)
    counts = {
        quality: sum(1 for row in rows if row.get("bounded_reaction_quality") == quality)
        for quality in QUALITY_ORDER
    }
    summary: dict[str, Any] = {
        "total_candidates_measured": total,
        "strong_count": counts["STRONG"],
        "strong_pct": _pct(counts["STRONG"], total),
        "medium_count": counts["MEDIUM"],
        "medium_pct": _pct(counts["MEDIUM"], total),
        "weak_count": counts["WEAK"],
        "weak_pct": _pct(counts["WEAK"], total),
        "early_post_d_confirmation_separates_strong_from_weak": _separates(rows),
    }
    for field in (
        "first_post_d_reaction_boxes",
        "first_post_d_reaction_pct_of_AB",
        "first_post_d_reaction_pct_of_CD",
    ):
        for quality in QUALITY_ORDER:
            summary[f"median_{field}_{quality.lower()}"] = _fmt(_quality_median(rows, quality, field))
    summary["strong_minus_weak_median_first_post_d_reaction_boxes"] = _fmt(
        _diff_medians(rows, "first_post_d_reaction_boxes")
    )
    summary["strong_minus_weak_median_first_post_d_reaction_pct_of_AB"] = _fmt(
        _diff_medians(rows, "first_post_d_reaction_pct_of_AB")
    )
    summary["strong_minus_weak_median_first_post_d_reaction_pct_of_CD"] = _fmt(
        _diff_medians(rows, "first_post_d_reaction_pct_of_CD")
    )
    if include_stability:
        by_symbol = _group_rows(rows, "symbol")
        by_year = _group_rows(rows, "year")
        summary["stable_across_symbols"] = _stable_across_groups(by_symbol)
        summary["stable_across_years"] = _stable_across_groups(by_year)
        summary["final_research_only_decision"] = (
            DECISION_NEXT
            if summary["early_post_d_confirmation_separates_strong_from_weak"] == "YES"
            and summary["stable_across_symbols"] == "YES"
            and summary["stable_across_years"] == "YES"
            else DECISION_STOP
        )
    return {key: _fmt(value) for key, value in summary.items()}


def _diff_medians(rows: Sequence[dict[str, Any]], field: str) -> float | None:
    strong = _quality_median(rows, "STRONG", field)
    weak = _quality_median(rows, "WEAK", field)
    if strong is None or weak is None:
        return None
    return strong - weak


def _group_rows(rows: Sequence[dict[str, Any]], field: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_text(row.get(field))].append(row)
    return dict(grouped)


def _stable_across_groups(grouped: dict[str, list[dict[str, Any]]]) -> str:
    eligible = [
        group_rows
        for group_rows in grouped.values()
        if _quality_median(group_rows, "STRONG", "first_post_d_reaction_boxes") is not None
        and _quality_median(group_rows, "WEAK", "first_post_d_reaction_boxes") is not None
    ]
    if not eligible:
        return "NO"
    separating = sum(1 for group_rows in eligible if _separates(group_rows) == "YES")
    return "YES" if separating / len(eligible) >= 0.75 else "NO"


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _group_summary_rows(rows: Sequence[dict[str, Any]], field: str, group_label: str) -> list[dict[str, Any]]:
    output = []
    for group, group_rows in sorted(_group_rows(rows, field).items(), key=lambda item: item[0]):
        row = {"group": group, **summarize(group_rows)}
        if group_label != "group":
            row[group_label] = row.pop("group")
        output.append(row)
    return output


def _quality_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"group": quality, **summarize([row for row in rows if row.get("bounded_reaction_quality") == quality])}
        for quality in QUALITY_ORDER
    ]


def _report(summary: dict[str, Any]) -> str:
    return f"""# AB=CD D-Confirmation Audit\n\nResearch-only audit using bounded D-reaction candidate fields joined to existing geometry candidates. No raw datasets were inspected, no ABCDs were reconstructed, and no strategy, entries, exits, stops, targets, PnL, expectancy, profitability, or trading rules were created.\n\n## Required answers\n\n1. Total candidates measured: {summary['total_candidates_measured']}\n2. Count and % STRONG: {summary['strong_count']} ({summary['strong_pct']})\n3. Count and % MEDIUM: {summary['medium_count']} ({summary['medium_pct']})\n4. Count and % WEAK: {summary['weak_count']} ({summary['weak_pct']})\n5. Median first_post_d_reaction_boxes by quality: STRONG={summary['median_first_post_d_reaction_boxes_strong']}, MEDIUM={summary['median_first_post_d_reaction_boxes_medium']}, WEAK={summary['median_first_post_d_reaction_boxes_weak']}\n6. Median first_post_d_reaction_pct_of_AB by quality: STRONG={summary['median_first_post_d_reaction_pct_of_AB_strong']}, MEDIUM={summary['median_first_post_d_reaction_pct_of_AB_medium']}, WEAK={summary['median_first_post_d_reaction_pct_of_AB_weak']}\n7. Median first_post_d_reaction_pct_of_CD by quality: STRONG={summary['median_first_post_d_reaction_pct_of_CD_strong']}, MEDIUM={summary['median_first_post_d_reaction_pct_of_CD_medium']}, WEAK={summary['median_first_post_d_reaction_pct_of_CD_weak']}\n8. Does early post-D confirmation separate STRONG from WEAK? {summary['early_post_d_confirmation_separates_strong_from_weak']}\n9. Stable across symbols? {summary['stable_across_symbols']}\n10. Stable across years? {summary['stable_across_years']}\n11. Final research-only decision: {summary['final_research_only_decision']}\n\n## Validation\n\n- Geometry rows required: {EXPECTED_ROWS}\n- Bounded D-reaction candidate rows required: {EXPECTED_ROWS}\n"""


def run(
    d_reaction_candidates: Path = DEFAULT_D_REACTION_CANDIDATES,
    geometry_candidates: Path = DEFAULT_GEOMETRY_CANDIDATES,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    candidates = build_candidates(d_reaction_candidates, geometry_candidates)
    summary = summarize(candidates, include_stability=True)

    _write_csv(output_root / "abcd_d_confirmation_candidates.csv", candidates, CANDIDATE_FIELDS)
    _write_csv(output_root / "abcd_d_confirmation_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(
        output_root / "abcd_d_confirmation_by_symbol.csv",
        _group_summary_rows(candidates, "symbol", "symbol"),
        ["symbol", *GROUP_FIELDS[1:]],
    )
    _write_csv(
        output_root / "abcd_d_confirmation_by_year.csv",
        _group_summary_rows(candidates, "year", "year"),
        ["year", *GROUP_FIELDS[1:]],
    )
    _write_csv(output_root / "abcd_d_confirmation_by_quality.csv", _quality_rows(candidates), GROUP_FIELDS)
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "abcd_d_confirmation_report.md").write_text(_report(summary), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only AB=CD D-confirmation audit.")
    parser.add_argument("--d-reaction-candidates", type=Path, default=DEFAULT_D_REACTION_CANDIDATES)
    parser.add_argument("--geometry-candidates", type=Path, default=DEFAULT_GEOMETRY_CANDIDATES)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    run(args.d_reaction_candidates, args.geometry_candidates, args.output_root)


if __name__ == "__main__":
    main()
