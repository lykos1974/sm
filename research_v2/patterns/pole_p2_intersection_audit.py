"""Research-only P+2 causal intersection audit.

This module audits pre-declared intersections of the already-causal P+2
``pole -> reversal -> confirmation`` motif.  It is diagnostic only: it does not
optimize filters, promote segments, import live traders, mutate production
strategy code, or change schemas.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import ENTRY_CANDIDATE
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import _parse_symbol_input, _round
from research_v2.patterns.pole_p2_causal_motif_audit import (
    BREAK_EVEN_TRIGGER_R,
    EXPECTED_SYMBOLS,
    MOTIF_NAME,
    TARGET_R,
    _validate_full_universe,
)
from research_v2.patterns.pole_p2_edge_segmentation_audit import (
    BASELINE_P2_EXPECTANCY,
    BASELINE_P2_TOTAL_R,
    BASELINE_P2_TRADES,
    UNIVERSE_EXPECTANCY_TOLERANCE,
    UNIVERSE_TOTAL_R_TOLERANCE,
    SegmentedOutcome,
    _build_segmented_outcomes,
    _universe_consistency,
    load_segmented_observations,
    summarize_outcomes,
)

OUTPUT_NAMES = (
    "p2_intersection_audit_summary.md",
    "p2_intersection_audit_results.csv",
    "p2_intersection_audit_flags.csv",
    "p2_intersection_audit_manifest.json",
)
INTERSECTION_FIELDS = [
    "intersection_id",
    "intersection_name",
    "symbol_scope",
    "direction_filter",
    "retrace_filter",
    "relative_pole_filter",
    "observations",
    "trades",
    "wins",
    "losses",
    "break_even_exits",
    "win_rate",
    "expectancy",
    "total_R",
    "baseline_expectancy",
    "expectancy_delta_vs_baseline",
    "expectancy_multiple_vs_baseline",
    "minimum_sample_warning",
    "rank_by_expectancy",
    "rank_by_total_R",
]
FLAG_FIELDS = ["scope", "flag", "details"]
TARGET_SYMBOL_GROUP = frozenset(("ENA", "HYPE", "TAO"))
DEEP_RETRACE = "DEEP_>0_618"
NEAR_RECENT_AVG = "NEAR_RECENT_AVG_0_75X_1_25X"
MIN_SAMPLE_TRADES = 100
BASELINE_EXPECTANCY = BASELINE_P2_EXPECTANCY
BASELINE_TRADES = BASELINE_P2_TRADES
BASELINE_TOTAL_R = BASELINE_P2_TOTAL_R


@dataclass(frozen=True)
class IntersectionDefinition:
    intersection_id: str
    intersection_name: str
    symbol_scope: str
    direction_filter: str
    retrace_filter: str
    relative_pole_filter: str
    predicate: Callable[[SegmentedOutcome], bool]


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _is_target_symbol(row: SegmentedOutcome) -> bool:
    return row.symbol in TARGET_SYMBOL_GROUP


def _is_short(row: SegmentedOutcome) -> bool:
    return row.direction == "SHORT"


def _is_deep(row: SegmentedOutcome) -> bool:
    return row.segments.get("retrace_quality") == DEEP_RETRACE


def _is_near_recent_avg(row: SegmentedOutcome) -> bool:
    return row.segments.get("relative_pole_size") == NEAR_RECENT_AVG


def _intersection_definitions() -> list[IntersectionDefinition]:
    return [
        IntersectionDefinition(
            "I01",
            "ENA/HYPE/TAO only",
            "ENA/HYPE/TAO",
            "ALL",
            "ALL",
            "ALL",
            _is_target_symbol,
        ),
        IntersectionDefinition(
            "I02",
            "ENA/HYPE/TAO + SHORT",
            "ENA/HYPE/TAO",
            "SHORT",
            "ALL",
            "ALL",
            lambda row: _is_target_symbol(row) and _is_short(row),
        ),
        IntersectionDefinition(
            "I03",
            "ENA/HYPE/TAO + DEEP retrace",
            "ENA/HYPE/TAO",
            "ALL",
            DEEP_RETRACE,
            "ALL",
            lambda row: _is_target_symbol(row) and _is_deep(row),
        ),
        IntersectionDefinition(
            "I04",
            "ENA/HYPE/TAO + SHORT + DEEP retrace",
            "ENA/HYPE/TAO",
            "SHORT",
            DEEP_RETRACE,
            "ALL",
            lambda row: _is_target_symbol(row) and _is_short(row) and _is_deep(row),
        ),
        IntersectionDefinition(
            "I05",
            "ENA/HYPE/TAO + SHORT + DEEP retrace + NEAR_RECENT_AVG",
            "ENA/HYPE/TAO",
            "SHORT",
            DEEP_RETRACE,
            NEAR_RECENT_AVG,
            lambda row: _is_target_symbol(row) and _is_short(row) and _is_deep(row) and _is_near_recent_avg(row),
        ),
        IntersectionDefinition(
            "I06",
            "All symbols + SHORT + DEEP retrace",
            "ALL_SYMBOLS",
            "SHORT",
            DEEP_RETRACE,
            "ALL",
            lambda row: _is_short(row) and _is_deep(row),
        ),
        IntersectionDefinition(
            "I07",
            "All symbols + SHORT + DEEP retrace + NEAR_RECENT_AVG",
            "ALL_SYMBOLS",
            "SHORT",
            DEEP_RETRACE,
            NEAR_RECENT_AVG,
            lambda row: _is_short(row) and _is_deep(row) and _is_near_recent_avg(row),
        ),
    ]


def _universe_status(all_row: dict[str, Any]) -> dict[str, Any]:
    universe = _universe_consistency(all_row)
    # Keep the exact required baseline values visible from this module too.
    universe["required_check"] = {
        "trades": BASELINE_TRADES,
        "expectancy": BASELINE_EXPECTANCY,
        "total_R": BASELINE_TOTAL_R,
        "expectancy_tolerance": UNIVERSE_EXPECTANCY_TOLERANCE,
        "total_R_tolerance": UNIVERSE_TOTAL_R_TOLERANCE,
    }
    return universe


def _comparison(expectancy: Any) -> tuple[Any, Any]:
    if expectancy == "":
        return "", ""
    delta = _round(float(expectancy) - BASELINE_EXPECTANCY)
    multiple = _round(float(expectancy) / BASELINE_EXPECTANCY) if BASELINE_EXPECTANCY else ""
    return delta, multiple


def _result_row(definition: IntersectionDefinition, outcomes: list[SegmentedOutcome]) -> dict[str, Any]:
    summary = summarize_outcomes(outcomes, "intersection", definition.intersection_id)
    expectancy_delta, expectancy_multiple = _comparison(summary["expectancy"])
    trades = int(summary["trades"] or 0)
    return {
        "intersection_id": definition.intersection_id,
        "intersection_name": definition.intersection_name,
        "symbol_scope": definition.symbol_scope,
        "direction_filter": definition.direction_filter,
        "retrace_filter": definition.retrace_filter,
        "relative_pole_filter": definition.relative_pole_filter,
        "observations": summary["observations"],
        "trades": trades,
        "wins": summary["wins"],
        "losses": summary["losses"],
        "break_even_exits": summary["break_even_exits"],
        "win_rate": summary["win_rate"],
        "expectancy": summary["expectancy"],
        "total_R": summary["total_R"],
        "baseline_expectancy": BASELINE_EXPECTANCY,
        "expectancy_delta_vs_baseline": expectancy_delta,
        "expectancy_multiple_vs_baseline": expectancy_multiple,
        "minimum_sample_warning": "MIN_SAMPLE_WARNING_TRADES_LT_100" if trades < MIN_SAMPLE_TRADES else "",
        "rank_by_expectancy": "",
        "rank_by_total_R": "",
    }


def build_intersection_rows(outcomes: list[SegmentedOutcome]) -> list[dict[str, Any]]:
    rows = [
        _result_row(definition, [row for row in outcomes if definition.predicate(row)])
        for definition in _intersection_definitions()
    ]
    expectancy_sorted = sorted(
        rows,
        key=lambda row: (row["expectancy"] != "", float(row["expectancy"] or "-inf"), row["trades"]),
        reverse=True,
    )
    total_r_sorted = sorted(rows, key=lambda row: (float(row["total_R"] or 0.0), row["trades"]), reverse=True)
    for rank, row in enumerate(expectancy_sorted, start=1):
        row["rank_by_expectancy"] = rank
    for rank, row in enumerate(total_r_sorted, start=1):
        row["rank_by_total_R"] = rank
    return rows


def _flag_rows(rows: list[dict[str, Any]], universe: dict[str, Any]) -> list[dict[str, str]]:
    flags: list[dict[str, str]] = []
    if universe["status"] != "UNIVERSE_MATCH":
        expected = universe["expected"]
        actual = universe["actual"]
        flags.append({
            "scope": "UNIVERSE",
            "flag": "UNIVERSE_MISMATCH",
            "details": (
                f"expected trades={expected['trades']}, expectancy={expected['expectancy']}, total_R={expected['total_R']}; "
                f"actual trades={actual['trades']}, expectancy={actual['expectancy']}, total_R={actual['total_R']}"
            ),
        })
    for row in rows:
        if row["minimum_sample_warning"]:
            flags.append({
                "scope": row["intersection_id"],
                "flag": "MIN_SAMPLE_WARNING",
                "details": f"{row['intersection_name']} has {row['trades']} trades; threshold is {MIN_SAMPLE_TRADES}",
            })
    return flags


def _write_summary(path: Path, all_row: dict[str, Any], rows: list[dict[str, Any]], flags: list[dict[str, str]], universe: dict[str, Any]) -> None:
    by_expectancy = sorted(rows, key=lambda row: int(row["rank_by_expectancy"]))
    by_total_r = sorted(rows, key=lambda row: int(row["rank_by_total_R"]))
    with path.open("x") as handle:
        handle.write("# P+2 causal intersection audit\n\n")
        handle.write("Research only. This is diagnostic intersection analysis, not optimization or strategy promotion.\n\n")
        handle.write("No live trader, production strategy, detector, or schema changes are made.\n\n")
        handle.write("## Universe consistency check\n\n")
        handle.write(f"- `status`: {universe['status']}\n")
        handle.write(f"- `required_trades`: {BASELINE_TRADES}\n")
        handle.write(f"- `required_expectancy`: {BASELINE_EXPECTANCY}\n")
        handle.write(f"- `required_total_R`: {BASELINE_TOTAL_R}\n")
        handle.write(f"- `actual_trades`: {universe['actual']['trades']}\n")
        handle.write(f"- `actual_expectancy`: {universe['actual']['expectancy']}\n")
        handle.write(f"- `actual_total_R`: {universe['actual']['total_R']}\n\n")
        handle.write("## Full P+2 baseline comparison\n\n")
        handle.write(f"- Full P+2 baseline expectancy: `{BASELINE_EXPECTANCY}` R/trade.\n")
        handle.write(f"- Full P+2 baseline trades: `{BASELINE_TRADES}`.\n")
        handle.write(f"- Full P+2 baseline total_R: `{BASELINE_TOTAL_R}`.\n")
        handle.write(f"- Loaded aggregate observations: `{all_row.get('observations', '')}`.\n")
        handle.write(f"- Loaded aggregate trades: `{all_row.get('trades', '')}`.\n")
        handle.write(f"- Loaded aggregate expectancy: `{all_row.get('expectancy', '')}`.\n")
        handle.write(f"- Loaded aggregate total_R: `{all_row.get('total_R', '')}`.\n\n")
        handle.write("## Required intersections\n\n")
        handle.write("| Intersection | Observations | Trades | Wins | Losses | BE exits | Win rate | Expectancy | Total_R | Delta vs baseline | Warning |\n")
        handle.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
        for row in rows:
            handle.write(
                f"| {row['intersection_name']} | {row['observations']} | {row['trades']} | {row['wins']} | "
                f"{row['losses']} | {row['break_even_exits']} | {row['win_rate']} | {row['expectancy']} | "
                f"{row['total_R']} | {row['expectancy_delta_vs_baseline']} | {row['minimum_sample_warning']} |\n"
            )
        handle.write("\n## Rank by expectancy\n\n")
        for row in by_expectancy:
            handle.write(f"{row['rank_by_expectancy']}. `{row['intersection_name']}` — expectancy `{row['expectancy']}`, trades `{row['trades']}`, total_R `{row['total_R']}`.\n")
        handle.write("\n## Rank by total_R\n\n")
        for row in by_total_r:
            handle.write(f"{row['rank_by_total_R']}. `{row['intersection_name']}` — total_R `{row['total_R']}`, expectancy `{row['expectancy']}`, trades `{row['trades']}`.\n")
        handle.write("\n## Flags\n\n")
        if not flags:
            handle.write("- No flags.\n")
        for flag in flags:
            handle.write(f"- `{flag['scope']}` `{flag['flag']}`: {flag['details']}\n")


def run(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    output_root: Path,
    candle_symbols: dict[str, str] | None = None,
    *,
    limit_rows_per_symbol: int | None = None,
    require_full_universe: bool = True,
) -> dict[str, Any]:
    symbols, segmented, candles_by_symbol, load_flags = load_segmented_observations(
        symbol_inputs,
        columns_inputs,
        candles_inputs,
        candle_symbols or {},
        limit_rows_per_symbol=limit_rows_per_symbol,
    )
    _validate_full_universe(symbols, require_full_universe)
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing P+2 intersection output(s): {', '.join(existing)}")

    outcomes = _build_segmented_outcomes(segmented, candles_by_symbol)
    all_row = summarize_outcomes(outcomes, "ALL", "ALL")
    universe = _universe_status(all_row)
    rows = build_intersection_rows(outcomes)
    flags = [
        {"scope": f"LOAD:{row['symbol']}:{row['row_number']}", "flag": row["result"], "details": f"{row['check_name']}: {row['details']}"}
        for row in load_flags
    ] + _flag_rows(rows, universe)

    _write_summary(output_root / OUTPUT_NAMES[0], all_row, rows, flags, universe)
    _write_csv(output_root / OUTPUT_NAMES[1], INTERSECTION_FIELDS, rows)
    _write_csv(output_root / OUTPUT_NAMES[2], FLAG_FIELDS, flags)

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_p2_intersection_audit",
        "research_only": True,
        "diagnostic_only": True,
        "optimization": False,
        "production_modifications": False,
        "live_trader_modifications": False,
        "detector_modifications": False,
        "schema_modifications": False,
        "strategy_promotion": False,
        "motif": MOTIF_NAME,
        "knowable_at": "P+2:pole->reversal->confirmation",
        "entry": ENTRY_CANDIDATE,
        "target_R": TARGET_R,
        "break_even_after_R": BREAK_EVEN_TRIGGER_R,
        "required_symbols": list(EXPECTED_SYMBOLS),
        "full_seven_market_universe": set(symbols) == set(EXPECTED_SYMBOLS),
        "symbols": symbols,
        "target_symbol_group": sorted(TARGET_SYMBOL_GROUP),
        "deep_retrace_bucket": DEEP_RETRACE,
        "near_recent_avg_bucket": NEAR_RECENT_AVG,
        "minimum_sample_trades": MIN_SAMPLE_TRADES,
        "baseline": {"trades": BASELINE_TRADES, "expectancy": BASELINE_EXPECTANCY, "total_R": BASELINE_TOTAL_R},
        "aggregate": all_row,
        "universe_consistency": universe,
        "intersections": rows,
        "flags": flags,
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[3]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only P+2 causal intersection audit")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--limit-rows-per-symbol", type=int, help="optional small-sample diagnostic cap after P+2 motif filtering")
    parser.add_argument("--allow-partial-universe", action="store_true", help="allow test/diagnostic runs outside the full BTC/ETH/SOL/ENA/HYPE/SUI/TAO universe")
    args = parser.parse_args()
    try:
        run(
            dict(args.symbol_input),
            dict(args.columns_input),
            dict(args.candles_input),
            args.output_root,
            dict(args.candle_symbol),
            limit_rows_per_symbol=args.limit_rows_per_symbol,
            require_full_universe=not args.allow_partial_universe,
        )
    except (FileExistsError, OSError, sqlite3.Error, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
