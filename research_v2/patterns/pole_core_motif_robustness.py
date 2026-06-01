from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

from research_v2.patterns.pole_directional_decomposition import (
    CONTINUATION_OUTCOMES,
    FAILURE_OUTCOME,
    _infer_direction,
    _row_expectancy,
)

EXPECTED_SYMBOLS = ("BTC", "ETH", "SOL", "SUI", "TAO", "ENA", "HYPE")
DIRECTIONS = ("BOTH", "LONG", "SHORT")
SIZE_BUCKETS = (
    ("small", "pole_boxes <= 8"),
    ("medium", "9 <= pole_boxes <= 16"),
    ("large", "pole_boxes >= 17"),
)
NA_VALUES = {"", "na", "none", "null", "nan"}
REQUIRED_COLUMNS = (
    "outcome_class",
    "opposing_pole_distance_columns",
    "enhanced_by_opposing_pole",
    "pole_boxes",
    "max_favorable_boxes",
    "max_adverse_boxes",
)
RANKING_FIELDS = [
    "rank",
    "symbol",
    "all_observations_sample_size",
    "all_observations_expectancy",
    "motif_sample_size",
    "motif_expectancy",
    "edge_lift",
    "support_status",
]
DIRECTIONAL_FIELDS = [
    "symbol",
    "direction",
    "all_observations_sample_size",
    "all_observations_expectancy",
    "motif_sample_size",
    "motif_expectancy",
    "edge_lift",
    "support_status",
]
SIZE_FIELDS = [
    "symbol",
    "pole_size_bucket",
    "bucket_definition",
    "all_observations_sample_size",
    "all_observations_expectancy",
    "motif_sample_size",
    "motif_expectancy",
    "edge_lift",
    "support_status",
]


@dataclass(frozen=True)
class PoleRow:
    symbol: str
    direction: str
    outcome: str
    distance: int
    enhanced: bool
    pole_boxes: float
    expectancy: float


@dataclass(frozen=True)
class SymbolAnalysis:
    symbol: str
    path: Path
    loaded_rows: int
    rows: list[PoleRow]
    excluded_reasons: dict[str, int]


def _clean(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in NA_VALUES else text


def _to_float(value: Any) -> float | None:
    text = _clean(value)
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    number = _to_float(value)
    return int(number) if number is not None and number.is_integer() else None


def _to_bool(value: Any) -> bool | None:
    text = _clean(value).lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _parse_symbol_input(value: str) -> tuple[str, Path]:
    symbol, separator, raw_path = value.partition("=")
    symbol = symbol.strip().upper()
    raw_path = raw_path.strip()
    if not separator or not symbol or not raw_path:
        raise argparse.ArgumentTypeError("expected SYMBOL=path/to/pole_labeled_outcomes.csv")
    return symbol, Path(raw_path)


def _load_symbol(symbol: str, path: Path) -> SymbolAnalysis:
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        missing = [column for column in REQUIRED_COLUMNS if column not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{symbol}: missing required labeled outcome columns: {', '.join(missing)}")
        raw_rows = list(reader)

    excluded = {
        "missing_direction": 0,
        "invalid_distance": 0,
        "invalid_enhanced": 0,
        "invalid_pole_boxes": 0,
        "invalid_max_favorable_boxes": 0,
        "invalid_max_adverse_boxes": 0,
    }
    rows: list[PoleRow] = []
    for raw in raw_rows:
        direction, _ = _infer_direction(raw)
        distance = _to_int(raw.get("opposing_pole_distance_columns"))
        enhanced = _to_bool(raw.get("enhanced_by_opposing_pole"))
        pole_boxes = _to_float(raw.get("pole_boxes"))
        favorable = _to_float(raw.get("max_favorable_boxes"))
        adverse = _to_float(raw.get("max_adverse_boxes"))
        checks = (
            (not direction, "missing_direction"),
            (distance is None, "invalid_distance"),
            (enhanced is None, "invalid_enhanced"),
            (pole_boxes is None, "invalid_pole_boxes"),
            (favorable is None, "invalid_max_favorable_boxes"),
            (adverse is None, "invalid_max_adverse_boxes"),
        )
        reason = next((name for failed, name in checks if failed), "")
        if reason:
            excluded[reason] += 1
            continue
        outcome = _clean(raw.get("outcome_class")).upper()
        assert distance is not None and enhanced is not None and pole_boxes is not None
        assert favorable is not None and adverse is not None
        rows.append(
            PoleRow(
                symbol=symbol,
                direction=direction,
                outcome=outcome,
                distance=distance,
                enhanced=enhanced,
                pole_boxes=pole_boxes,
                expectancy=_row_expectancy(outcome, favorable, adverse),
            )
        )
    return SymbolAnalysis(symbol=symbol, path=path, loaded_rows=len(raw_rows), rows=rows, excluded_reasons=excluded)


def _is_motif(row: PoleRow) -> bool:
    return row.distance == 3 and not row.enhanced


def _size_bucket(row: PoleRow) -> str:
    if row.pole_boxes <= 8:
        return "small"
    if row.pole_boxes <= 16:
        return "medium"
    return "large"


def _metrics(rows: list[PoleRow]) -> dict[str, int | float]:
    return {
        "sample_size": len(rows),
        "expectancy": round(mean(row.expectancy for row in rows), 6) if rows else 0.0,
        "continuation_pct": round(_safe_div(sum(row.outcome in CONTINUATION_OUTCOMES for row in rows), len(rows)), 6),
        "failure_pct": round(_safe_div(sum(row.outcome == FAILURE_OUTCOME for row in rows), len(rows)), 6),
    }


def _comparison(all_rows: list[PoleRow], motif_rows: list[PoleRow], min_sample: int) -> dict[str, int | float | str]:
    baseline = _metrics(all_rows)
    motif = _metrics(motif_rows)
    lift = round(float(motif["expectancy"]) - float(baseline["expectancy"]), 6)
    if int(motif["sample_size"]) < min_sample:
        status = "INSUFFICIENT_SAMPLE"
    elif lift > 0:
        status = "SUPPORTS"
    else:
        status = "CONTRADICTS"
    return {
        "all_observations_sample_size": baseline["sample_size"],
        "all_observations_expectancy": baseline["expectancy"],
        "motif_sample_size": motif["sample_size"],
        "motif_expectancy": motif["expectancy"],
        "edge_lift": lift,
        "support_status": status,
    }


def _directional_rows(rows: list[PoleRow], min_sample: int) -> list[dict[str, str | int | float]]:
    output: list[dict[str, str | int | float]] = []
    for symbol in (*EXPECTED_SYMBOLS, "ALL"):
        symbol_rows = rows if symbol == "ALL" else [row for row in rows if row.symbol == symbol]
        for direction in DIRECTIONS:
            baseline = symbol_rows if direction == "BOTH" else [row for row in symbol_rows if row.direction == direction]
            output.append({"symbol": symbol, "direction": direction, **_comparison(baseline, [row for row in baseline if _is_motif(row)], min_sample)})
    return output


def _size_rows(rows: list[PoleRow], min_sample: int) -> list[dict[str, str | int | float]]:
    output: list[dict[str, str | int | float]] = []
    for symbol in (*EXPECTED_SYMBOLS, "ALL"):
        symbol_rows = rows if symbol == "ALL" else [row for row in rows if row.symbol == symbol]
        for bucket, definition in SIZE_BUCKETS:
            baseline = [row for row in symbol_rows if _size_bucket(row) == bucket]
            output.append({"symbol": symbol, "pole_size_bucket": bucket, "bucket_definition": definition, **_comparison(baseline, [row for row in baseline if _is_motif(row)], min_sample)})
    return output


def _rankings(directional: list[dict[str, str | int | float]]) -> list[dict[str, str | int | float]]:
    combined = [row for row in directional if row["symbol"] != "ALL" and row["direction"] == "BOTH"]
    combined.sort(key=lambda row: (float(row["edge_lift"]), float(row["motif_expectancy"]), int(row["motif_sample_size"])), reverse=True)
    return [{"rank": rank, **{field: row[field] for field in RANKING_FIELDS if field != "rank"}} for rank, row in enumerate(combined, start=1)]


def _write_csv(path: Path, rows: list[dict[str, str | int | float]], fields: list[str]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _join(values: list[str]) -> str:
    return ", ".join(values) if values else "NONE"


def _write_summary(
    path: Path,
    analyses: list[SymbolAnalysis],
    rankings: list[dict[str, str | int | float]],
    directional: list[dict[str, str | int | float]],
    sizes: list[dict[str, str | int | float]],
    min_sample: int,
) -> None:
    combined = {str(row["symbol"]): row for row in directional if row["direction"] == "BOTH"}
    pooled_direction = {str(row["direction"]): row for row in directional if row["symbol"] == "ALL"}
    pooled_size = {str(row["pole_size_bucket"]): row for row in sizes if row["symbol"] == "ALL"}
    supporting = [symbol for symbol in EXPECTED_SYMBOLS if combined[symbol]["support_status"] == "SUPPORTS"]
    contradicting = [symbol for symbol in EXPECTED_SYMBOLS if combined[symbol]["support_status"] == "CONTRADICTS"]
    insufficient = [symbol for symbol in EXPECTED_SYMBOLS if combined[symbol]["support_status"] == "INSUFFICIENT_SAMPLE"]
    side_support = all(pooled_direction[direction]["support_status"] == "SUPPORTS" for direction in ("LONG", "SHORT"))
    size_support = all(pooled_size[bucket]["support_status"] == "SUPPORTS" for bucket, _ in SIZE_BUCKETS)
    structural_law = "YES" if len(supporting) >= 2 and not contradicting and side_support and size_support else "NO"
    if structural_law == "YES" and not insufficient:
        confidence = "HIGH"
    elif structural_law == "YES":
        confidence = "MEDIUM"
    else:
        confidence = "LOW"
    strongest = rankings[0] if rankings else None
    weakest = rankings[-1] if rankings else None

    with path.open("w") as f:
        f.write("# PnF Pole Research — Robustness Audit of the Cross-Symbol Core Motif\n\n")
        f.write("Research-only labeled-outcome validation. No strategy logic, execution logic, TP/SL, existing pattern definition, or threshold is changed. No execution simulation or motif discovery is performed.\n\n")
        f.write("## Fixed Motif Under Audit\n\n")
        f.write("`opposing_pole_distance_columns = 3 AND enhanced_by_opposing_pole = False`\n\n")
        f.write("`retrace_ratio` is ignored completely: it is neither required as an input column nor used to select, bucket, rank, or conclude on any row.\n\n")
        f.write("## A Priori Robustness Slices\n\n")
        f.write("- directions: `LONG`, `SHORT`, and pooled `BOTH`\n")
        f.write("- small poles: `pole_boxes <= 8`\n")
        f.write("- medium poles: `9 <= pole_boxes <= 16`\n")
        f.write("- large poles: `pole_boxes >= 17`\n")
        f.write(f"- descriptive minimum sample for support/contradiction classification: `{min_sample}` motif observations\n")
        f.write("- `edge_lift = expectancy(distance=3 AND enhanced=False) - expectancy(all observations)`\n\n")
        f.write("## Input Diagnostics\n\n")
        for analysis in analyses:
            excluded = ", ".join(f"{name}={count}" for name, count in analysis.excluded_reasons.items() if count) or "NONE"
            f.write(f"- {analysis.symbol}: path=`{analysis.path}`; loaded_rows={analysis.loaded_rows}; usable_rows={len(analysis.rows)}; excluded={excluded}\n")
        f.write("\n## Symbol Rankings by edge_lift\n\n")
        f.write("| Rank | Symbol | All n | All expectancy | Motif n | Motif expectancy | edge_lift | Status |\n")
        f.write("| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
        for row in rankings:
            f.write(f"| {row['rank']} | {row['symbol']} | {row['all_observations_sample_size']} | {row['all_observations_expectancy']} | {row['motif_sample_size']} | {row['motif_expectancy']} | {row['edge_lift']} | {row['support_status']} |\n")
        f.write("\n## Pooled Directional Robustness\n\n")
        f.write("| Direction | All n | All expectancy | Motif n | Motif expectancy | edge_lift | Status |\n")
        f.write("| --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
        for direction in DIRECTIONS:
            row = pooled_direction[direction]
            f.write(f"| {direction} | {row['all_observations_sample_size']} | {row['all_observations_expectancy']} | {row['motif_sample_size']} | {row['motif_expectancy']} | {row['edge_lift']} | {row['support_status']} |\n")
        f.write("\n## Pooled Size Robustness\n\n")
        f.write("| Pole size | Definition | All n | All expectancy | Motif n | Motif expectancy | edge_lift | Status |\n")
        f.write("| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
        for bucket, definition in SIZE_BUCKETS:
            row = pooled_size[bucket]
            f.write(f"| {bucket} | {definition} | {row['all_observations_sample_size']} | {row['all_observations_expectancy']} | {row['motif_sample_size']} | {row['motif_expectancy']} | {row['edge_lift']} | {row['support_status']} |\n")
        f.write("\n## Final Conclusions\n\n")
        f.write(f"- robust structural law: **{structural_law}**\n")
        f.write(f"- symbols supporting it: **{_join(supporting)}**\n")
        f.write(f"- symbols contradicting it: **{_join(contradicting)}**\n")
        f.write(f"- insufficient-sample symbols: **{_join(insufficient)}**\n")
        if strongest:
            f.write(f"- strongest supporting evidence: **{strongest['symbol']}** (`edge_lift={strongest['edge_lift']}`, `motif_n={strongest['motif_sample_size']}`)\n")
        if weakest:
            f.write(f"- weakest supporting evidence: **{weakest['symbol']}** (`edge_lift={weakest['edge_lift']}`, `motif_n={weakest['motif_sample_size']}`)\n")
        f.write(f"- confidence level: **{confidence}**\n\n")
        f.write("A symbol is classified as collapsing back to baseline expectancy when its adequately sampled motif has `edge_lift <= 0`; those symbols appear under `symbols contradicting it`. Insufficient samples remain explicitly inconclusive.\n\n")
        f.write("## Required Experiment Scorecard\n\n")
        f.write("The execution scorecard is intentionally not computed because this task prohibits execution simulation. Structural labeled-outcome expectancy is not a substitute for realized execution R.\n\n")
        for metric in ("candidate_rows_registered", "resolved_rows", "win_rate_non_ambiguous", "avg_realized_r_multiple", "total_realized_r_multiple", "TP1 -> TP2 conversion"):
            f.write(f"- `{metric}`: `NOT_COMPUTED (structural audit only; execution simulation prohibited)`\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit robustness of the fixed distance=3 AND enhanced=False cross-symbol pole motif (research-only).")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV", help="repeat for BTC, ETH, SOL, SUI, TAO, ENA, and HYPE")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--min-sample", type=int, default=20, help="descriptive minimum motif sample for support/contradiction classifications (default: 20)")
    args = parser.parse_args()
    if args.min_sample < 1:
        parser.error("--min-sample must be >= 1")
    symbol_inputs: list[tuple[str, Path]] = args.symbol_input
    symbols = [symbol for symbol, _ in symbol_inputs]
    if len(set(symbols)) != len(symbols):
        parser.error("each --symbol-input symbol must be unique")
    if set(symbols) != set(EXPECTED_SYMBOLS):
        parser.error(f"exactly these symbols are required: {', '.join(EXPECTED_SYMBOLS)}")
    missing_paths = [str(path) for _, path in symbol_inputs if not path.is_file()]
    if missing_paths:
        parser.error(f"labeled outcomes CSV path does not exist: {', '.join(missing_paths)}")

    by_symbol = dict(symbol_inputs)
    analyses = [_load_symbol(symbol, by_symbol[symbol]) for symbol in EXPECTED_SYMBOLS]
    rows = [row for analysis in analyses for row in analysis.rows]
    directional = _directional_rows(rows, args.min_sample)
    sizes = _size_rows(rows, args.min_sample)
    rankings = _rankings(directional)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "robustness_symbol_rankings.csv", rankings, RANKING_FIELDS)
    _write_csv(output_root / "robustness_directional.csv", directional, DIRECTIONAL_FIELDS)
    _write_csv(output_root / "robustness_size_buckets.csv", sizes, SIZE_FIELDS)
    _write_summary(output_root / "robustness_summary.md", analyses, rankings, directional, sizes, args.min_sample)
    print(f"symbols_loaded={','.join(EXPECTED_SYMBOLS)}")
    print(f"usable_rows={len(rows)}")
    print(f"outputs={output_root}")


if __name__ == "__main__":
    main()
