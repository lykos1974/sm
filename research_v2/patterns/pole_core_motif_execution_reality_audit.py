"""Research-only execution-reality audit for PnF pole core motif observations.

This module deliberately does not optimize entry, stop, target, or chronology rules. It
reuses the already validated NEXT_COLUMN_OPEN_ENTRY plus fixed three-box stop and asks a
narrow pre-execution question: how many distinct trade opportunities are represented by
motif observations after grouping overlapping observations from the same executable move?
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from research_v2.patterns.pole_core_motif_entry_timing_audit import (
    Candle,
    EntryTimingObservation,
    _classify,
)
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import (
    ENTRY_CANDIDATE,
    UNKNOWN,
    _load_observations,
)
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import R_TARGETS, _parse_symbol_input, _round

COMBINED = "COMBINED"
PRIMARY_TARGET_R = 2.5
ALLOWED_VERDICTS = ("EDGE_SURVIVES", "EDGE_WEAKENS", "EDGE_COLLAPSES", "INSUFFICIENT_DATA")
OUTPUT_NAMES = (
    "execution_reality_summary.md",
    "execution_reality_symbol_breakdown.csv",
    "execution_reality_opportunity_breakdown.csv",
    "execution_reality_overlap_report.csv",
    "execution_reality_flags.csv",
    "execution_reality_manifest.json",
)
OPPORTUNITY_GROUPING_METHOD = "symbol+direction+observable_entry_ts+entry_price+stop_price"
OPPORTUNITY_GROUPING_JUSTIFICATION = (
    "For NEXT_COLUMN_OPEN_ENTRY, a realistic executable trade opportunity is the first "
    "candle open after the immediate next PnF column starts. Observations with the same "
    "symbol, direction, observable entry timestamp, entry price, and fixed three-box stop "
    "represent the same order/risk event even if they came from multiple motif rows. Rows "
    "without observable execution geometry are not merged across missing anchors; each is "
    "kept as its own research-only opportunity so missing data cannot artificially improve "
    "de-duplicated expectancy."
)

SYMBOL_FIELDS = [
    "symbol", "total_observations", "unique_trade_opportunities", "deduplication_ratio",
    "observations_per_opportunity_median", "observations_per_opportunity_mean", "observations_per_opportunity_max",
    "opportunities_per_month", "opportunities_per_quarter", "opportunities_per_year",
    "observation_expected_R_1R", "opportunity_expected_R_1R",
    "observation_expected_R_1_5R", "opportunity_expected_R_1_5R",
    "observation_expected_R_2R", "opportunity_expected_R_2R",
    "observation_expected_R_2_5R", "opportunity_expected_R_2_5R",
    "observation_expected_R_3R", "opportunity_expected_R_3R",
]
OPPORTUNITY_FIELDS = [
    "opportunity_id", "symbol", "direction", "observable_entry_ts", "observable_entry_time_utc",
    "entry_price", "stop_price", "representative_row_number", "cluster_size", "member_row_numbers",
    "grouping_key", "geometry_status", "target_1R_classification", "target_1R_realized_R",
    "target_1_5R_classification", "target_1_5R_realized_R", "target_2R_classification", "target_2R_realized_R",
    "target_2_5R_classification", "target_2_5R_realized_R", "target_3R_classification", "target_3R_realized_R",
]
OVERLAP_FIELDS = [
    "observable_entry_ts", "observable_entry_time_utc", "symbol_count", "opportunity_count", "symbols", "opportunity_ids",
]
FLAG_FIELDS = ["scope", "symbol", "flag", "details"]


@dataclass(frozen=True)
class Opportunity:
    opportunity_id: str
    key: tuple[Any, ...]
    observations: tuple[EntryTimingObservation, ...]
    representative: EntryTimingObservation


def _target_label(target_r: float) -> str:
    return str(target_r).replace(".", "_").replace("_0", "")


def _ts_to_utc(ts: int | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(ts / 1000 if ts > 10_000_000_000 else ts, tz=UTC).isoformat()


def _price_key(value: float | None) -> str:
    return "" if value is None else f"{_round(value):.6f}"


def _opportunity_key(row: EntryTimingObservation) -> tuple[Any, ...]:
    if row.geometry_status != "OBSERVABLE" or row.observable_entry_ts is None or row.entry is None or row.stop is None:
        return ("UNOBSERVABLE", row.symbol, row.row_number)
    return (row.symbol, row.direction, row.observable_entry_ts, _price_key(row.entry), _price_key(row.stop))


def _build_opportunities(observations: list[EntryTimingObservation]) -> list[Opportunity]:
    grouped: dict[tuple[Any, ...], list[EntryTimingObservation]] = defaultdict(list)
    for row in observations:
        grouped[_opportunity_key(row)].append(row)
    opportunities: list[Opportunity] = []
    for ordinal, (key, rows) in enumerate(sorted(grouped.items(), key=lambda item: (str(item[0]), min(r.row_number for r in item[1]))), start=1):
        ordered = tuple(sorted(rows, key=lambda row: row.row_number))
        representative = ordered[0]
        opportunities.append(Opportunity(f"OPP-{ordinal:06d}", key, ordered, representative))
    return opportunities


def _classifications_for_observations(
    observations: Iterable[EntryTimingObservation], candles_by_symbol: dict[str, list[Candle]]
) -> dict[tuple[str, int, float], dict[str, Any]]:
    return {
        (row.symbol, row.row_number, target_r): _classify(row, candles_by_symbol[row.symbol], target_r)
        for row in observations
        for target_r in R_TARGETS
    }


def _classification_to_r(classification: str, target_r: float) -> float | str:
    if classification == "TARGET_FIRST":
        return target_r
    if classification == "STOP_FIRST":
        return -1.0
    return ""


def _expectancy_from_classifications(classifications: Iterable[str], target_r: float) -> dict[str, Any]:
    counts = Counter(classifications)
    wins, losses = counts["TARGET_FIRST"], counts["STOP_FIRST"]
    resolved = wins + losses
    return {
        "target_first": wins,
        "stop_first": losses,
        "ambiguous": counts["SAME_CANDLE_AMBIGUOUS"],
        "not_reached": counts["NOT_REACHED"],
        "unknown": sum(counts[name] for name in UNKNOWN),
        "resolved": resolved,
        "win_rate": _round(wins / resolved) if resolved else "",
        "loss_rate": _round(losses / resolved) if resolved else "",
        "expected_R": _round((wins * target_r - losses) / resolved) if resolved else "",
    }


def _opportunity_expectancy_rows(
    scope: str,
    symbol: str,
    opportunities: list[Opportunity],
    target_lookup: dict[tuple[str, int, float], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for target_r in R_TARGETS:
        classifications = [target_lookup[(opp.representative.symbol, opp.representative.row_number, target_r)]["classification"] for opp in opportunities]
        rows.append({"scope": scope, "symbol": symbol, "r_target": target_r, "opportunities": len(opportunities), **_expectancy_from_classifications(classifications, target_r)})
    return rows


def _observation_expectancy(
    observations: list[EntryTimingObservation], target_lookup: dict[tuple[str, int, float], dict[str, Any]], target_r: float
) -> dict[str, Any]:
    return _expectancy_from_classifications(
        (target_lookup[(row.symbol, row.row_number, target_r)]["classification"] for row in observations), target_r
    )


def _cluster_stats(opportunities: list[Opportunity]) -> tuple[float | str, float | str, int | str]:
    sizes = [len(opp.observations) for opp in opportunities]
    return (
        _round(median(sizes)) if sizes else "",
        _round(mean(sizes)) if sizes else "",
        max(sizes) if sizes else "",
    )


def _frequency(opportunities: list[Opportunity]) -> tuple[float | str, float | str, float | str]:
    timestamps = [opp.representative.observable_entry_ts for opp in opportunities if opp.representative.observable_entry_ts is not None]
    if len(timestamps) < 2:
        return ("", "", "")
    span_days = max((max(timestamps) - min(timestamps)) / 86_400, 1.0)
    if max(timestamps) > 10_000_000_000:
        span_days = max((max(timestamps) - min(timestamps)) / 86_400_000, 1.0)
    per_year = len(timestamps) / span_days * 365.25
    return (_round(per_year / 12), _round(per_year / 4), _round(per_year))


def _symbol_rows(
    symbols: list[str], observations: list[EntryTimingObservation], opportunities: list[Opportunity], target_lookup: dict[tuple[str, int, float], dict[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in [*symbols, COMBINED]:
        scoped_observations = observations if symbol == COMBINED else [row for row in observations if row.symbol == symbol]
        scoped_opportunities = opportunities if symbol == COMBINED else [opp for opp in opportunities if opp.representative.symbol == symbol]
        median_size, mean_size, max_size = _cluster_stats(scoped_opportunities)
        per_month, per_quarter, per_year = _frequency(scoped_opportunities)
        row: dict[str, Any] = {
            "symbol": symbol,
            "total_observations": len(scoped_observations),
            "unique_trade_opportunities": len(scoped_opportunities),
            "deduplication_ratio": _round(len(scoped_opportunities) / len(scoped_observations)) if scoped_observations else "",
            "observations_per_opportunity_median": median_size,
            "observations_per_opportunity_mean": mean_size,
            "observations_per_opportunity_max": max_size,
            "opportunities_per_month": per_month,
            "opportunities_per_quarter": per_quarter,
            "opportunities_per_year": per_year,
        }
        for target_r in R_TARGETS:
            label = _target_label(target_r)
            row[f"observation_expected_R_{label}R"] = _observation_expectancy(scoped_observations, target_lookup, target_r)["expected_R"]
            row[f"opportunity_expected_R_{label}R"] = _opportunity_expectancy_rows("SYMBOL", symbol, scoped_opportunities, target_lookup)[int(R_TARGETS.index(target_r))]["expected_R"]
        rows.append(row)
    return rows


def _opportunity_rows(opportunities: list[Opportunity], target_lookup: dict[tuple[str, int, float], dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for opp in opportunities:
        rep = opp.representative
        row: dict[str, Any] = {
            "opportunity_id": opp.opportunity_id,
            "symbol": rep.symbol,
            "direction": rep.direction,
            "observable_entry_ts": rep.observable_entry_ts or "",
            "observable_entry_time_utc": _ts_to_utc(rep.observable_entry_ts),
            "entry_price": "" if rep.entry is None else _round(rep.entry),
            "stop_price": "" if rep.stop is None else _round(rep.stop),
            "representative_row_number": rep.row_number,
            "cluster_size": len(opp.observations),
            "member_row_numbers": ";".join(str(row.row_number) for row in opp.observations),
            "grouping_key": "|".join(str(part) for part in opp.key),
            "geometry_status": rep.geometry_status,
        }
        for target_r in R_TARGETS:
            label = _target_label(target_r)
            classification = target_lookup[(rep.symbol, rep.row_number, target_r)]["classification"]
            row[f"target_{label}R_classification"] = classification
            row[f"target_{label}R_realized_R"] = _classification_to_r(classification, target_r)
        rows.append(row)
    return rows


def _overlap_rows(opportunities: list[Opportunity]) -> list[dict[str, Any]]:
    grouped: dict[int, list[Opportunity]] = defaultdict(list)
    for opp in opportunities:
        ts = opp.representative.observable_entry_ts
        if ts is not None and opp.representative.geometry_status == "OBSERVABLE":
            grouped[ts].append(opp)
    rows: list[dict[str, Any]] = []
    for ts, opps in sorted(grouped.items()):
        symbols = sorted({opp.representative.symbol for opp in opps})
        if len(symbols) < 2:
            continue
        rows.append({
            "observable_entry_ts": ts,
            "observable_entry_time_utc": _ts_to_utc(ts),
            "symbol_count": len(symbols),
            "opportunity_count": len(opps),
            "symbols": ";".join(symbols),
            "opportunity_ids": ";".join(opp.opportunity_id for opp in opps),
        })
    return rows


def _concentration_stats(opportunities: list[Opportunity], target_lookup: dict[tuple[str, int, float], dict[str, Any]], target_r: float = PRIMARY_TARGET_R) -> dict[str, Any]:
    realized: list[float] = []
    for opp in opportunities:
        classification = target_lookup[(opp.representative.symbol, opp.representative.row_number, target_r)]["classification"]
        value = _classification_to_r(classification, target_r)
        if value != "":
            realized.append(float(value))
    if not realized:
        return {"resolved_opportunities": 0, "total_realized_R": "", "top_10_share": "", "top_20_share": ""}
    total = sum(realized)
    ordered = sorted(realized, reverse=True)

    def share(percent: float) -> float | str:
        if total == 0:
            return ""
        count = max(1, math.ceil(len(ordered) * percent))
        return _round(sum(ordered[:count]) / total)

    return {
        "resolved_opportunities": len(realized),
        "total_realized_R": _round(total),
        "top_10_share": share(0.10),
        "top_20_share": share(0.20),
    }


def _edge_verdict(observation_expected: Any, opportunity_expected: Any, opportunity_count: int) -> tuple[str, str, float | str]:
    if opportunity_count < 10 or opportunity_expected == "" or observation_expected == "":
        return "INSUFFICIENT_DATA", "fewer than 10 de-duplicated opportunities or unresolved primary expectancy", ""
    degradation = "" if observation_expected == 0 else _round((observation_expected - opportunity_expected) / abs(observation_expected))
    if opportunity_expected <= 0:
        return "EDGE_COLLAPSES", "primary opportunity-level expectancy is non-positive after de-duplication", degradation
    if degradation != "" and degradation >= 0.25:
        return "EDGE_WEAKENS", "primary expectancy remains positive but decreases materially after de-duplication", degradation
    return "EDGE_SURVIVES", "primary expectancy remains positive with less than 25% de-duplication degradation", degradation


def _flags(
    opportunities: list[Opportunity], overlap_rows: list[dict[str, Any]], concentration: dict[str, Any], verdict: str
) -> list[dict[str, str]]:
    flags = [
        {"scope": "ALL", "symbol": COMBINED, "flag": "PRODUCTION_ISOLATION", "details": "research-only audit; no execution model or production strategy code is modified"},
        {"scope": "ALL", "symbol": COMBINED, "flag": "NO_OPTIMIZATION", "details": "entry, stop, target, and chronology logic are reused without parameter search"},
    ]
    clustered = [opp for opp in opportunities if len(opp.observations) > 1]
    if clustered:
        flags.append({"scope": "ALL", "symbol": COMBINED, "flag": "CLUSTERED_OBSERVATIONS_PRESENT", "details": f"{len(clustered)} opportunities contain more than one motif observation"})
    if overlap_rows:
        flags.append({"scope": "ALL", "symbol": COMBINED, "flag": "CROSS_MARKET_TIMESTAMP_OVERLAP", "details": f"{len(overlap_rows)} timestamps contain opportunities in multiple symbols"})
    if concentration["top_10_share"] != "" and concentration["top_10_share"] > 0.5:
        flags.append({"scope": "ALL", "symbol": COMBINED, "flag": "HIGH_TOP_10_CONCENTRATION", "details": "top 10% of resolved primary opportunities contribute more than half of total realized R"})
    flags.append({"scope": "ALL", "symbol": COMBINED, "flag": verdict, "details": "allowed verdict only; never PROMOTE"})
    return flags


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def run(
    symbol_inputs: dict[str, Path], columns_inputs: dict[str, Path], candles_inputs: dict[str, Path], output_root: Path,
    candle_symbols: dict[str, str] | None = None,
) -> None:
    symbols, observations, candles_by_symbol = _load_observations(symbol_inputs, columns_inputs, candles_inputs, candle_symbols or {})
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing execution-reality output(s): {', '.join(existing)}")

    opportunities = _build_opportunities(observations)
    target_lookup = _classifications_for_observations(observations, candles_by_symbol)
    symbol_rows = _symbol_rows(symbols, observations, opportunities, target_lookup)
    opportunity_rows = _opportunity_rows(opportunities, target_lookup)
    overlaps = _overlap_rows(opportunities)
    concentration = _concentration_stats(opportunities, target_lookup)

    combined_observation_25 = _observation_expectancy(observations, target_lookup, PRIMARY_TARGET_R)["expected_R"]
    combined_opportunity_25 = next(
        row["expected_R"] for row in _opportunity_expectancy_rows("COMBINED", COMBINED, opportunities, target_lookup) if row["r_target"] == PRIMARY_TARGET_R
    )
    verdict, reason, degradation = _edge_verdict(combined_observation_25, combined_opportunity_25, len(opportunities))
    flag_rows = _flags(opportunities, overlaps, concentration, verdict)

    with (output_root / OUTPUT_NAMES[0]).open("x") as handle:
        handle.write("# PnF pole motif execution-reality audit\n\n")
        handle.write("Research only. This pre-execution audit does not optimize parameters, construct TP1/TP2/BE logic, or modify existing research/production code. It never outputs `PROMOTE`.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n{reason}.\n\n")
        handle.write("## Opportunity grouping method\n\n")
        handle.write(f"**Method:** `{OPPORTUNITY_GROUPING_METHOD}`.\n\n{OPPORTUNITY_GROUPING_JUSTIFICATION}\n\n")
        handle.write("## Edge survival test\n\n")
        handle.write("| metric | value |\n|---|---:|\n")
        handle.write(f"| observations | {len(observations)} |\n")
        handle.write(f"| unique opportunities | {len(opportunities)} |\n")
        handle.write(f"| observation-level {PRIMARY_TARGET_R}R expectancy | {combined_observation_25} |\n")
        handle.write(f"| opportunity-level {PRIMARY_TARGET_R}R expectancy | {combined_opportunity_25} |\n")
        handle.write(f"| degradation percentage | {'' if degradation == '' else _round(degradation * 100)} |\n\n")
        handle.write("## Symbol opportunity breakdown\n\n")
        handle.write("| symbol | observations | opportunities | median cluster | mean cluster | max cluster | opps/month | opps/quarter | opps/year |\n")
        handle.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in symbol_rows:
            handle.write(f"| {row['symbol']} | {row['total_observations']} | {row['unique_trade_opportunities']} | {row['observations_per_opportunity_median']} | {row['observations_per_opportunity_mean']} | {row['observations_per_opportunity_max']} | {row['opportunities_per_month']} | {row['opportunities_per_quarter']} | {row['opportunities_per_year']} |\n")
        handle.write("\n## Cross-market independence indicators\n\n")
        overlapping_opps = sum(int(row["opportunity_count"]) for row in overlaps)
        handle.write(f"Exact overlapping observable-entry timestamps across symbols: {len(overlaps)}; opportunities involved: {overlapping_opps}. See `execution_reality_overlap_report.csv`.\n\n")
        handle.write("## Concentration risk at 2.5R\n\n")
        handle.write(f"Resolved opportunities: {concentration['resolved_opportunities']}; total realized R: {concentration['total_realized_R']}; top 10% share: {concentration['top_10_share']}; top 20% share: {concentration['top_20_share']}.\n")

    _write_csv(output_root / OUTPUT_NAMES[1], SYMBOL_FIELDS, symbol_rows)
    _write_csv(output_root / OUTPUT_NAMES[2], OPPORTUNITY_FIELDS, opportunity_rows)
    _write_csv(output_root / OUTPUT_NAMES[3], OVERLAP_FIELDS, overlaps)
    _write_csv(output_root / OUTPUT_NAMES[4], FLAG_FIELDS, flag_rows)
    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_core_motif_execution_reality_audit",
        "research_only": True,
        "execution_model_construction": False,
        "strategy_promotion": False,
        "entry_candidate": ENTRY_CANDIDATE,
        "stop_logic": "fixed_3_box_stop",
        "target_logic_modified": False,
        "chronology_logic_modified": False,
        "optimization_performed": False,
        "symbols": symbols,
        "r_targets": list(R_TARGETS),
        "primary_target_r": PRIMARY_TARGET_R,
        "opportunity_grouping_method": OPPORTUNITY_GROUPING_METHOD,
        "opportunity_grouping_justification": OPPORTUNITY_GROUPING_JUSTIFICATION,
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "verdict": verdict,
        "verdict_reason": reason,
        "observation_level_expectancy_2_5R": combined_observation_25,
        "opportunity_level_expectancy_2_5R": combined_opportunity_25,
        "degradation_percentage_2_5R": "" if degradation == "" else _round(degradation * 100),
        "concentration_2_5R": concentration,
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[5]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only PnF pole motif execution-reality audit")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    try:
        run(dict(args.symbol_input), dict(args.columns_input), dict(args.candles_input), args.output_root, dict(args.candle_symbol))
    except (FileExistsError, OSError, sqlite3.Error, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
