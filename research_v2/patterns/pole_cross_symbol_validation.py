from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

NA_VALUES = {"", "na", "none", "null", "nan"}
CONTINUATION_OUTCOMES = {"BULLISH_CONTINUATION", "BEARISH_CONTINUATION"}
FAILURE_OUTCOME = "FAILED_REVERSAL"
DIRECTIONS = ("BOTH", "LONG", "SHORT")
DIRECTION_COLUMNS = ("pattern_direction", "direction", "side")
REQUIRED_COLUMNS = (
    "outcome_class",
    "opposing_pole_distance_columns",
    "enhanced_by_opposing_pole",
    "retrace_ratio",
    "max_favorable_boxes",
    "max_adverse_boxes",
)
SCOPES = (
    ("A_all_observations", "all observations"),
    ("B_distance_3", "distance=3"),
    ("C_distance_3_enhanced_false", "distance=3 AND enhanced=False"),
    ("D_fixed_motif", "distance=3 AND enhanced=False AND retrace_ratio>1.0"),
)
METRIC_FIELDS = [
    "symbol",
    "scope",
    "scope_definition",
    "direction",
    "sample_size",
    "continuation_pct",
    "failure_pct",
    "expectancy_score",
    "asymmetry_score",
    "sample_insufficiency",
]
FLAG_FIELDS = ["symbol", "check_name", "result", "details"]


@dataclass(frozen=True)
class PoleRow:
    direction: str
    outcome: str
    distance: int
    enhanced: bool
    retrace_ratio: float
    expectancy: float


@dataclass(frozen=True)
class SymbolAnalysis:
    symbol: str
    path: Path
    loaded_rows: int
    usable_rows: int
    missing_required_columns: tuple[str, ...]
    unusable_row_reasons: dict[str, int]
    rows: list[PoleRow]


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


def _normalize_direction(value: Any) -> str:
    normalized = _clean(value).upper()
    if normalized in {"LONG", "BULLISH", "BULL", "UP", "LOW_POLE"}:
        return "LONG"
    if normalized in {"SHORT", "BEARISH", "BEAR", "DOWN", "HIGH_POLE"}:
        return "SHORT"
    return ""


def _infer_direction(row: dict[str, str]) -> tuple[str, str]:
    for column in DIRECTION_COLUMNS:
        direction = _normalize_direction(row.get(column))
        if direction:
            return direction, column
    direction = _normalize_direction(row.get("pattern_name"))
    return (direction, "pattern_name") if direction else ("", "")


def _row_expectancy(outcome: str, max_favorable: float, max_adverse: float) -> float:
    outcome_score = 1.0 if outcome in CONTINUATION_OUTCOMES else (-1.0 if outcome == FAILURE_OUTCOME else 0.0)
    asymmetry = _safe_div(max_favorable - max_adverse, max_favorable + max_adverse)
    return (0.7 * outcome_score) + (0.3 * asymmetry)


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
        columns = set(reader.fieldnames or [])
        raw_rows = list(reader)

    missing = [column for column in REQUIRED_COLUMNS if column not in columns]
    if not any(column in columns for column in DIRECTION_COLUMNS) and "pattern_name" not in columns:
        missing.append("direction|pattern_name")

    reason_counts = {
        "missing_direction": 0,
        "missing_outcome_class": 0,
        "invalid_distance": 0,
        "invalid_enhanced": 0,
        "invalid_retrace_ratio": 0,
        "invalid_max_favorable_boxes": 0,
        "invalid_max_adverse_boxes": 0,
    }
    rows: list[PoleRow] = []
    if not missing:
        for raw in raw_rows:
            direction, _ = _infer_direction(raw)
            outcome = _clean(raw.get("outcome_class")).upper()
            distance = _to_int(raw.get("opposing_pole_distance_columns"))
            enhanced = _to_bool(raw.get("enhanced_by_opposing_pole"))
            retrace_ratio = _to_float(raw.get("retrace_ratio"))
            max_favorable = _to_float(raw.get("max_favorable_boxes"))
            max_adverse = _to_float(raw.get("max_adverse_boxes"))
            invalid = False
            values = (
                (not direction, "missing_direction"),
                (not outcome, "missing_outcome_class"),
                (distance is None, "invalid_distance"),
                (enhanced is None, "invalid_enhanced"),
                (retrace_ratio is None, "invalid_retrace_ratio"),
                (max_favorable is None, "invalid_max_favorable_boxes"),
                (max_adverse is None, "invalid_max_adverse_boxes"),
            )
            for failed, reason in values:
                if failed:
                    reason_counts[reason] += 1
                    invalid = True
            if invalid:
                continue
            assert distance is not None and enhanced is not None and retrace_ratio is not None
            assert max_favorable is not None and max_adverse is not None
            rows.append(
                PoleRow(
                    direction=direction,
                    outcome=outcome,
                    distance=distance,
                    enhanced=enhanced,
                    retrace_ratio=retrace_ratio,
                    expectancy=_row_expectancy(outcome, max_favorable, max_adverse),
                )
            )

    return SymbolAnalysis(
        symbol=symbol,
        path=path,
        loaded_rows=len(raw_rows),
        usable_rows=len(rows),
        missing_required_columns=tuple(missing),
        unusable_row_reasons=reason_counts,
        rows=rows,
    )


def _scope_rows(rows: list[PoleRow], scope: str) -> list[PoleRow]:
    if scope == "A_all_observations":
        return rows
    distance_three = [row for row in rows if row.distance == 3]
    if scope == "B_distance_3":
        return distance_three
    unenhanced = [row for row in distance_three if not row.enhanced]
    if scope == "C_distance_3_enhanced_false":
        return unenhanced
    if scope == "D_fixed_motif":
        return [row for row in unenhanced if row.retrace_ratio > 1.0]
    raise ValueError(f"unknown scope: {scope}")


def _metrics(rows: list[PoleRow], min_sample: int) -> dict[str, str | int | float]:
    sample_size = len(rows)
    continuation_pct = _safe_div(sum(row.outcome in CONTINUATION_OUTCOMES for row in rows), sample_size)
    failure_pct = _safe_div(sum(row.outcome == FAILURE_OUTCOME for row in rows), sample_size)
    return {
        "sample_size": sample_size,
        "continuation_pct": round(continuation_pct, 6),
        "failure_pct": round(failure_pct, 6),
        "expectancy_score": round(mean(row.expectancy for row in rows), 6) if rows else 0.0,
        "asymmetry_score": round(continuation_pct - failure_pct, 6),
        "sample_insufficiency": "True" if sample_size < min_sample else "False",
    }


def _metric_rows(analyses: list[SymbolAnalysis], min_sample: int) -> list[dict[str, str | int | float]]:
    output: list[dict[str, str | int | float]] = []
    for analysis in analyses:
        for scope, definition in SCOPES:
            scoped = _scope_rows(analysis.rows, scope)
            for direction in DIRECTIONS:
                directional = scoped if direction == "BOTH" else [row for row in scoped if row.direction == direction]
                output.append(
                    {
                        "symbol": analysis.symbol,
                        "scope": scope,
                        "scope_definition": definition,
                        "direction": direction,
                        **_metrics(directional, min_sample),
                    }
                )
    return output


def _supports_motif(row: dict[str, str | int | float], min_sample: int) -> bool:
    return int(row["sample_size"]) >= min_sample and float(row["expectancy_score"]) > 0.0 and float(row["continuation_pct"]) > float(row["failure_pct"])


def _join(values: list[str]) -> str:
    return ", ".join(values) if values else "NONE"


def _conclusions(
    analyses: list[SymbolAnalysis],
    metrics: list[dict[str, str | int | float]],
    min_sample: int,
    symmetry_tolerance: float,
) -> tuple[dict[str, str], dict[str, str]]:
    index = {(str(row["symbol"]), str(row["scope"]), str(row["direction"])): row for row in metrics}
    motif = {analysis.symbol: index[(analysis.symbol, "D_fixed_motif", "BOTH")] for analysis in analyses}
    adequate = [symbol for symbol, row in motif.items() if int(row["sample_size"]) >= min_sample]
    supporting = [symbol for symbol in adequate if _supports_motif(motif[symbol], min_sample)]
    non_btc_adequate = [symbol for symbol in adequate if symbol != "BTC"]
    non_btc_supporting = [symbol for symbol in supporting if symbol != "BTC"]
    ranked = [symbol for symbol, row in motif.items() if int(row["sample_size"]) > 0]
    ranked.sort(key=lambda symbol: (float(motif[symbol]["expectancy_score"]), int(motif[symbol]["sample_size"])), reverse=True)

    if non_btc_adequate and len(non_btc_supporting) == len(non_btc_adequate):
        generalizes = "YES"
    elif non_btc_adequate and not non_btc_supporting:
        generalizes = "NO"
    else:
        generalizes = "PARTIAL"

    symmetry: dict[str, str] = {}
    for analysis in analyses:
        long_row = index[(analysis.symbol, "D_fixed_motif", "LONG")]
        short_row = index[(analysis.symbol, "D_fixed_motif", "SHORT")]
        long_n = int(long_row["sample_size"])
        short_n = int(short_row["sample_size"])
        difference = abs(float(long_row["expectancy_score"]) - float(short_row["expectancy_score"]))
        if long_n < min_sample or short_n < min_sample:
            symmetry[analysis.symbol] = f"INSUFFICIENT_SAMPLE (LONG={long_n}, SHORT={short_n}, min_each={min_sample})"
        elif _supports_motif(long_row, min_sample) and _supports_motif(short_row, min_sample) and difference <= symmetry_tolerance:
            symmetry[analysis.symbol] = f"YES (expectancy_delta={difference:.6f})"
        else:
            symmetry[analysis.symbol] = f"NO (expectancy_delta={difference:.6f})"

    retest = [analysis.symbol for analysis in analyses if analysis.symbol not in supporting]
    broad_based = "BROAD_BASED" if non_btc_supporting else ("BTC_UNIQUE_SO_FAR" if "BTC" in supporting else "NOT_ESTABLISHED")
    return (
        {
            "motif_generalizes": generalizes,
            "strongest_symbol": ranked[0] if ranked else "NONE",
            "weakest_symbol": ranked[-1] if ranked else "NONE",
            "symbols_recommended_for_next_phase": _join(supporting),
            "symbols_to_discard_or_retest": _join(retest),
            "btc_result_unique_or_broad_based": broad_based,
        },
        symmetry,
    )


def _flags(
    analyses: list[SymbolAnalysis],
    metrics: list[dict[str, str | int | float]],
    conclusions: dict[str, str],
    symmetry: dict[str, str],
    min_sample: int,
) -> list[dict[str, str]]:
    index = {(str(row["symbol"]), str(row["scope"]), str(row["direction"])): row for row in metrics}
    flags: list[dict[str, str]] = []
    for analysis in analyses:
        motif = index[(analysis.symbol, "D_fixed_motif", "BOTH")]
        reasons = ", ".join(f"{name}={count}" for name, count in analysis.unusable_row_reasons.items() if count) or "NONE"
        flags.extend(
            [
                {"symbol": analysis.symbol, "check_name": "input_path", "result": str(analysis.path), "details": "labeled outcomes CSV used for this symbol"},
                {"symbol": analysis.symbol, "check_name": "rows_loaded", "result": str(analysis.loaded_rows), "details": "rows read from labeled outcomes CSV"},
                {"symbol": analysis.symbol, "check_name": "usable_rows", "result": str(analysis.usable_rows), "details": f"unusable_row_reasons={reasons}"},
                {"symbol": analysis.symbol, "check_name": "missing_required_columns", "result": _join(list(analysis.missing_required_columns)), "details": "NONE means the labeled outcomes schema is usable"},
                {"symbol": analysis.symbol, "check_name": "fixed_motif_rows", "result": str(motif["sample_size"]), "details": "distance=3 AND enhanced=False AND retrace_ratio>1.0"},
                {"symbol": analysis.symbol, "check_name": "insufficient_sample", "result": "YES" if int(motif["sample_size"]) < min_sample else "NO", "details": f"fixed_motif_rows={motif['sample_size']}; min_sample={min_sample}"},
                {"symbol": analysis.symbol, "check_name": "long_short_symmetry", "result": symmetry[analysis.symbol], "details": "fixed motif only; both sides must independently meet the minimum sample"},
            ]
        )
    for check_name, result in conclusions.items():
        flags.append({"symbol": "ALL", "check_name": check_name, "result": result, "details": "fixed cross-symbol motif validation conclusion"})
    return flags


def _write_csv(path: Path, rows: list[dict[str, str | int | float]], fields: list[str]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_summary(
    path: Path,
    analyses: list[SymbolAnalysis],
    metrics: list[dict[str, str | int | float]],
    conclusions: dict[str, str],
    symmetry: dict[str, str],
    min_sample: int,
    symmetry_tolerance: float,
) -> None:
    index = {(str(row["symbol"]), str(row["scope"]), str(row["direction"])): row for row in metrics}
    with path.open("w") as f:
        f.write("# PnF Pole Cross-Symbol Motif Validation\n\n")
        f.write("Research-only validation. Rows are built only from labeled outcomes CSVs. No strategy changes, execution simulation, TP/SL logic, feature mining, per-symbol optimization, threshold changes, or symbol-specific rules are included.\n\n")
        f.write("## Fixed Motif\n\n")
        f.write("`opposing_pole_distance_columns = 3 AND enhanced_by_opposing_pole = False AND retrace_ratio > 1.0`\n\n")
        f.write("## Diagnostics\n\n")
        f.write(f"- symbols loaded: {_join([analysis.symbol for analysis in analyses])}\n")
        f.write(f"- minimum sample per evaluated population: {min_sample}\n")
        f.write(f"- LONG/SHORT symmetry expectancy tolerance: {symmetry_tolerance:.6f}\n")
        for analysis in analyses:
            motif = index[(analysis.symbol, "D_fixed_motif", "BOTH")]
            missing = _join(list(analysis.missing_required_columns))
            f.write(f"- {analysis.symbol}: rows={analysis.loaded_rows}; usable_rows={analysis.usable_rows}; missing_required_columns={missing}; fixed_motif_rows={motif['sample_size']}; insufficient_sample={'YES' if int(motif['sample_size']) < min_sample else 'NO'}\n")
        insufficient = [analysis.symbol for analysis in analyses if int(index[(analysis.symbol, "D_fixed_motif", "BOTH")]["sample_size"]) < min_sample]
        f.write(f"- insufficient sample symbols: {_join(insufficient)}\n\n")
        f.write("## Scope Metrics (BOTH Directions)\n\n")
        f.write("| Symbol | Scope | Sample | Continuation % | Failure % | Expectancy | Asymmetry | Insufficient |\n")
        f.write("| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
        for analysis in analyses:
            for scope, definition in SCOPES:
                row = index[(analysis.symbol, scope, "BOTH")]
                f.write(f"| {analysis.symbol} | {definition} | {row['sample_size']} | {row['continuation_pct']} | {row['failure_pct']} | {row['expectancy_score']} | {row['asymmetry_score']} | {row['sample_insufficiency']} |\n")
        f.write("\n## LONG/SHORT Symmetry by Symbol\n\n")
        for analysis in analyses:
            f.write(f"- {analysis.symbol}: {symmetry[analysis.symbol]}\n")
        f.write("\n## Conclusions\n\n")
        for name, result in conclusions.items():
            f.write(f"- {name}: **{result}**\n")
        f.write("\n`PARTIAL` includes the evidence-limited case where no non-BTC symbol has enough fixed-motif rows. Generate additional labeled outcomes before promoting or discarding any symbol from an insufficient sample.\n")
        f.write("\n## Governance Note\n\n")
        f.write("The execution scorecard (`candidate_rows_registered`, `resolved_rows`, win rate, realized R metrics, and TP1 -> TP2 conversion) is intentionally not produced because this validator performs structural labeled-outcome analysis only and does not run execution simulation.\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the fixed BTC pole motif across labeled symbol outcomes (research-only).")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV", help="repeat once per symbol, for example BTC=path/to/pole_labeled_outcomes.csv")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--min-sample", type=int, default=20, help="minimum fixed-motif sample for symbol and side conclusions (default: 20)")
    parser.add_argument("--symmetry-tolerance", type=float, default=0.15, help="maximum LONG/SHORT expectancy delta for symmetry (default: 0.15)")
    args = parser.parse_args()

    if args.min_sample < 1:
        parser.error("--min-sample must be >= 1")
    if args.symmetry_tolerance < 0:
        parser.error("--symmetry-tolerance must be >= 0")
    symbol_inputs: list[tuple[str, Path]] = args.symbol_input
    symbols = [symbol for symbol, _ in symbol_inputs]
    if len(set(symbols)) != len(symbols):
        parser.error("each --symbol-input symbol must be unique")
    missing_paths = [str(path) for _, path in symbol_inputs if not path.is_file()]
    if missing_paths:
        parser.error(f"labeled outcomes CSV path does not exist: {', '.join(missing_paths)}")

    analyses = [_load_symbol(symbol, path) for symbol, path in symbol_inputs]
    metrics = _metric_rows(analyses, args.min_sample)
    conclusions, symmetry = _conclusions(analyses, metrics, args.min_sample, args.symmetry_tolerance)
    flags = _flags(analyses, metrics, conclusions, symmetry, args.min_sample)

    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "pole_cross_symbol_metrics.csv", metrics, METRIC_FIELDS)
    _write_csv(output_root / "pole_cross_symbol_flags.csv", flags, FLAG_FIELDS)
    _write_summary(output_root / "pole_cross_symbol_summary.md", analyses, metrics, conclusions, symmetry, args.min_sample, args.symmetry_tolerance)

    print(f"symbols_loaded={_join(symbols)}")
    for analysis in analyses:
        motif_rows = next(row for row in metrics if row["symbol"] == analysis.symbol and row["scope"] == "D_fixed_motif" and row["direction"] == "BOTH")
        print(f"{analysis.symbol}: rows={analysis.loaded_rows} usable_rows={analysis.usable_rows} missing_required_columns={_join(list(analysis.missing_required_columns))} motif_rows={motif_rows['sample_size']} insufficient_sample={'YES' if int(motif_rows['sample_size']) < args.min_sample else 'NO'}")
    print(f"motif_generalizes={conclusions['motif_generalizes']}")
    print(f"outputs={output_root}")


if __name__ == "__main__":
    main()
