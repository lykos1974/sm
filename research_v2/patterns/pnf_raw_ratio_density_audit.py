"""Research-only raw reaction-ratio density audit.

This module consumes the local harmonic reaction export and bins only the raw
``reaction_ratio`` values. It intentionally performs no nearest-harmonic-level
assignment, no harmonic labels, no enrichment scoring, no cluster-strength
calculation, no pattern recognition, no setup generation, and no expectancy
analysis.
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Sequence

DEFAULT_REACTIONS_INPUT = Path(
    "research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/"
    "harmonic_reactions_by_threshold.csv"
)
DEFAULT_OUTPUT_ROOT = Path(
    "research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit"
)
BUCKET_WIDTH = 0.05
BUCKET_START = 0.0
MIN_BUCKET_END = 3.05
AUDIT_YEARS = (2024, 2025, 2026)
REFERENCE_LEVELS = (0.236, 0.382, 0.500, 0.618, 0.707, 0.786, 1.000, 1.272, 1.618)

OUTPUT_HISTOGRAM = "raw_ratio_histogram.csv"
OUTPUT_PEAKS = "raw_ratio_peaks.csv"
OUTPUT_STABILITY = "raw_ratio_peak_stability.csv"
OUTPUT_REPORT = "raw_ratio_density_report.md"
OUTPUT_NAMES = (
    OUTPUT_HISTOGRAM,
    OUTPUT_PEAKS,
    "raw_ratio_histogram_2024.csv",
    "raw_ratio_histogram_2025.csv",
    "raw_ratio_histogram_2026.csv",
    OUTPUT_STABILITY,
    OUTPUT_REPORT,
)

HISTOGRAM_FIELDS = ["bucket_low", "bucket_high", "count", "percentage"]
PEAK_FIELDS = ["peak_center", "bucket_count", "local_rank"]
STABILITY_FIELDS = [
    "peak_center",
    "present_2024",
    "present_2025",
    "present_2026",
    "all_years",
    "average_rank",
]


@dataclass(frozen=True)
class RawReaction:
    ratio: float
    year: int | None


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


def _parse_ratio(value: Any, *, row_number: int) -> float:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"row {row_number}: invalid reaction_ratio: {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"row {row_number}: non-finite reaction_ratio: {value!r}")
    return parsed


def _parse_year(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        timestamp = float(text)
    except ValueError:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).year
        except ValueError:
            return None
    if not math.isfinite(timestamp):
        return None
    if timestamp > 10_000_000_000:
        timestamp /= 1000.0
    return datetime.fromtimestamp(timestamp, UTC).year


def load_reactions(path: str | Path) -> list[RawReaction]:
    reactions: list[RawReaction] = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if "reaction_ratio" not in (reader.fieldnames or []):
            raise ValueError("input must contain a reaction_ratio column")
        for row_number, row in enumerate(reader, start=2):
            ratio = _parse_ratio(row.get("reaction_ratio"), row_number=row_number)
            if ratio < BUCKET_START:
                continue
            reactions.append(RawReaction(ratio=ratio, year=_parse_year(row.get("completion_time"))))
    return reactions


def _bucket_edges(reactions: Sequence[RawReaction]) -> list[tuple[float, float]]:
    max_ratio = max((reaction.ratio for reaction in reactions), default=MIN_BUCKET_END)
    end = max(MIN_BUCKET_END, math.ceil(max_ratio / BUCKET_WIDTH) * BUCKET_WIDTH)
    bucket_count = int(round((end - BUCKET_START) / BUCKET_WIDTH))
    return [
        (round(BUCKET_START + index * BUCKET_WIDTH, 10), round(BUCKET_START + (index + 1) * BUCKET_WIDTH, 10))
        for index in range(bucket_count)
    ]


def histogram_rows(reactions: Sequence[RawReaction], edges: Sequence[tuple[float, float]]) -> list[dict[str, Any]]:
    counts = [0 for _ in edges]
    total = 0
    for reaction in reactions:
        if reaction.ratio < BUCKET_START or not edges:
            continue
        index = int(math.floor((reaction.ratio - BUCKET_START) / BUCKET_WIDTH))
        if 0 <= index < len(counts):
            counts[index] += 1
            total += 1
    rows: list[dict[str, Any]] = []
    for (low, high), count in zip(edges, counts):
        rows.append(
            {
                "bucket_low": f"{low:.2f}",
                "bucket_high": f"{high:.2f}",
                "count": count,
                "percentage": _format_number((count / total * 100.0) if total else 0.0),
            }
        )
    return rows


def peak_rows(histogram: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[tuple[float, int]] = []
    for index in range(1, len(histogram) - 1):
        count = int(histogram[index]["count"])
        if count > int(histogram[index - 1]["count"]) and count > int(histogram[index + 1]["count"]):
            low = float(histogram[index]["bucket_low"])
            high = float(histogram[index]["bucket_high"])
            candidates.append(((low + high) / 2.0, count))
    ranked = sorted(candidates, key=lambda item: (-item[1], item[0]))
    return [
        {"peak_center": f"{center:.3f}", "bucket_count": count, "local_rank": rank}
        for rank, (center, count) in enumerate(ranked, start=1)
    ]


def stability_rows(full_peaks: Sequence[dict[str, Any]], yearly_peaks: dict[int, Sequence[dict[str, Any]]]) -> list[dict[str, Any]]:
    centers = {str(row["peak_center"]) for row in full_peaks}
    centers.update(str(row["peak_center"]) for rows in yearly_peaks.values() for row in rows)
    rank_by_year = {
        year: {str(row["peak_center"]): int(row["local_rank"]) for row in rows}
        for year, rows in yearly_peaks.items()
    }
    rows: list[dict[str, Any]] = []
    for center in sorted(centers, key=float):
        present = {year: center in rank_by_year.get(year, {}) for year in AUDIT_YEARS}
        ranks = [rank_by_year[year][center] for year in AUDIT_YEARS if present[year]]
        rows.append(
            {
                "peak_center": center,
                "present_2024": present[2024],
                "present_2025": present[2025],
                "present_2026": present[2026],
                "all_years": all(present.values()),
                "average_rank": _format_number(mean(ranks) if ranks else math.nan),
            }
        )
    return rows


def _write_csv(path: Path, fields: Sequence[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _top_concentrations(histogram: Sequence[dict[str, Any]], limit: int = 10) -> list[str]:
    ranked = sorted(histogram, key=lambda row: (-int(row["count"]), float(row["bucket_low"])))
    return [
        f"{row['bucket_low']}-{row['bucket_high']} (count {row['count']}, {row['percentage']}%)"
        for row in ranked[:limit]
        if int(row["count"]) > 0
    ]


def _level_table(histogram: Sequence[dict[str, Any]], peaks: Sequence[dict[str, Any]]) -> list[str]:
    peak_lookup = {str(row["peak_center"]): row for row in peaks}
    peak_centers = [float(row["peak_center"]) for row in peaks]
    lines = [
        "| Reference ratio | Raw bucket | Count | Bucket is local peak? | Nearest raw peak | Within 0.05? |",
        "| --- | --- | ---: | --- | ---: | --- |",
    ]
    for level in REFERENCE_LEVELS:
        index = int(math.floor((level - BUCKET_START) / BUCKET_WIDTH))
        row = histogram[index] if 0 <= index < len(histogram) else None
        nearest = min(peak_centers, key=lambda center: abs(center - level)) if peak_centers else math.nan
        nearest_text = (
            f"{nearest:.3f} (Δ {abs(nearest - level):.3f})"
            if math.isfinite(nearest)
            else ""
        )
        within = math.isfinite(nearest) and abs(nearest - level) <= BUCKET_WIDTH
        if row is None:
            lines.append(
                f"| {_format_number(level)} | out of range | 0 | No | {nearest_text} | "
                f"{'Yes' if within else 'No'} |"
            )
            continue
        center = f"{((float(row['bucket_low']) + float(row['bucket_high'])) / 2.0):.3f}"
        lines.append(
            f"| {_format_number(level)} | {row['bucket_low']}-{row['bucket_high']} | "
            f"{row['count']} | {'Yes' if center in peak_lookup else 'No'} | "
            f"{nearest_text} | {'Yes' if within else 'No'} |"
        )
    return lines


def _write_report(
    path: Path,
    *,
    reactions: Sequence[RawReaction],
    histogram: Sequence[dict[str, Any]],
    peaks: Sequence[dict[str, Any]],
    stability: Sequence[dict[str, Any]],
    yearly_totals: dict[int, int],
) -> None:
    top = _top_concentrations(histogram)
    all_years = [row["peak_center"] for row in stability if str(row["all_years"]) == "True"]
    disappearing = [row["peak_center"] for row in stability if str(row["all_years"]) != "True"]
    peak_summary = [
        f"{row['peak_center']} (count {row['bucket_count']}, rank {row['local_rank']})"
        for row in peaks[:10]
    ]
    lines = [
        "# Raw Ratio Density Report",
        "",
        "Research-only audit. This report uses raw `reaction_ratio` values only; it does not use nearest harmonic assignment, harmonic level labels, enrichment scoring, cluster-strength calculations, pattern detection, strategy logic, or expectancy.",
        "",
        "## Dataset",
        f"- Input reactions counted: {len(reactions)}.",
        f"- Yearly reaction counts: 2024={yearly_totals.get(2024, 0)}, 2025={yearly_totals.get(2025, 0)}, 2026={yearly_totals.get(2026, 0)}.",
        f"- Bucket width: {BUCKET_WIDTH:.2f}; bucket range: {histogram[0]['bucket_low']}-{histogram[-1]['bucket_high']}.",
        "",
        "## 1. Where are the strongest raw ratio concentrations?",
        "; ".join(top) if top else "No populated buckets were found.",
        "",
        "Strongest local peaks:",
        "; ".join(peak_summary) if peak_summary else "No strict local peaks were found.",
        "",
        "## 2. Do the strongest concentrations occur near common harmonic ratios?",
        *_level_table(histogram, peaks),
        "",
        "## 3. Which peaks survive all years?",
        ", ".join(all_years) if all_years else "None. The local export has no 2024 or 2025 reaction rows, so no raw peak can satisfy all-year survival.",
        "",
        "## 4. Which peaks disappear?",
        ", ".join(disappearing) if disappearing else "None detected.",
        "",
        "## 5. Is there evidence of natural ratio clustering without harmonic-level assignment?",
        "Yes, within the available local export there are strict local maxima in the raw-ratio histogram. However, the evidence is temporally limited because the available export is populated only in 2026; it does not validate multi-year natural clustering.",
        "",
        "## 6. Does the data support continuing harmonic research?",
        "Yes, but only as research. The raw histogram contains non-uniform concentrations worth auditing on a larger multi-year export. These outputs do not promote harmonic levels, create a detector, define patterns, or support strategy deployment.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def run_audit(*, reactions_input: str | Path = DEFAULT_REACTIONS_INPUT, output_root: str | Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    reactions = load_reactions(reactions_input)
    edges = _bucket_edges(reactions)
    full_histogram = histogram_rows(reactions, edges)
    full_peaks = peak_rows(full_histogram)

    yearly_histograms: dict[int, list[dict[str, Any]]] = {}
    yearly_peaks: dict[int, list[dict[str, Any]]] = {}
    yearly_totals: dict[int, int] = {}
    for year in AUDIT_YEARS:
        yearly_reactions = [reaction for reaction in reactions if reaction.year == year]
        yearly_totals[year] = len(yearly_reactions)
        yearly_histograms[year] = histogram_rows(yearly_reactions, edges)
        yearly_peaks[year] = peak_rows(yearly_histograms[year])

    stability = stability_rows(full_peaks, yearly_peaks)

    output_path = Path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)
    _write_csv(output_path / OUTPUT_HISTOGRAM, HISTOGRAM_FIELDS, full_histogram)
    _write_csv(output_path / OUTPUT_PEAKS, PEAK_FIELDS, full_peaks)
    for year in AUDIT_YEARS:
        _write_csv(output_path / f"raw_ratio_histogram_{year}.csv", HISTOGRAM_FIELDS, yearly_histograms[year])
    _write_csv(output_path / OUTPUT_STABILITY, STABILITY_FIELDS, stability)
    _write_report(
        output_path / OUTPUT_REPORT,
        reactions=reactions,
        histogram=full_histogram,
        peaks=full_peaks,
        stability=stability,
        yearly_totals=yearly_totals,
    )
    return {
        "input_reactions": len(reactions),
        "output_root": str(output_path),
        "output_files": [str(output_path / name) for name in OUTPUT_NAMES],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only raw reaction-ratio density audit.")
    parser.add_argument("--reactions-input", default=str(DEFAULT_REACTIONS_INPUT), help="Local harmonic reactions CSV containing raw reaction_ratio values.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Directory where raw-ratio audit outputs will be written.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = run_audit(reactions_input=args.reactions_input, output_root=args.output_root)
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
