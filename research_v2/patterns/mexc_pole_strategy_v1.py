"""Research-only MEXC Pole Strategy v1 trade-plan runner.

This runner copies the validated Pole research baseline into a planning artifact only:
NEXT_COLUMN_OPEN_ENTRY, fixed three-box stop, fixed 2.5R target, +2R break-even
trigger, fixed-risk sizing, and one-position-per-symbol selection. It does not place
orders and intentionally does not touch production scanner or execution code.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
PNF_ROOT = REPO_ROOT / "pnf_mvp"
if str(PNF_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_ROOT))

from pnf_engine import PnFEngine, PnFProfile  # noqa: E402
from pnf_mvp.patterns.poles import detect_pole_patterns  # noqa: E402
from research_v2.patterns.pole_core_motif_entry_timing_audit import (  # noqa: E402
    Candle,
    EntryTimingObservation,
    _candidate_observation,
)
from research_v2.patterns.pole_core_motif_execution_reality_audit import (  # noqa: E402
    Opportunity,
    _build_opportunities,
)
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import (  # noqa: E402
    ENTRY_CANDIDATE,
    _load_observations,
)
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import TimedColumn, _parse_candle_symbol  # noqa: E402
from research_v2.patterns.pole_core_motif_sl_candidates import (  # noqa: E402
    _direction,
    _is_core_motif,
    _parse_symbol_input,
    _round,
)
from research_v2.patterns.pole_portfolio_reality_audit import (  # noqa: E402
    BREAK_EVEN_TRIGGER_R,
    TARGET_R,
    PortfolioTrade,
    _risk_sizing_values,
    _ts_to_utc,
)

TARGET_SYMBOLS = (
    "MEXC_FUT:BTCUSDT",
    "MEXC_FUT:ETHUSDT",
    "MEXC_FUT:SOLUSDT",
    "MEXC_FUT:SUIUSDT",
    "MEXC_FUT:ENAUSDT",
)
DEFAULT_DB = PNF_ROOT / "pnf_mvp.db"
DEFAULT_OUTPUT_ROOT = PNF_ROOT / "exports" / "mexc_pole_strategy_v1"
DEFAULT_HISTORICAL_OPPORTUNITIES = (
    PNF_ROOT
    / "exports"
    / "pole_core_motif_execution_reality_7markets_v1"
    / "execution_reality_opportunity_breakdown.csv"
)
DEFAULT_BOX_SIZES = {
    "MEXC_FUT:BTCUSDT": 100.0,
    "MEXC_FUT:ETHUSDT": 10.0,
    "MEXC_FUT:SOLUSDT": 1.0,
    "MEXC_FUT:SUIUSDT": 0.01,
    "MEXC_FUT:ENAUSDT": 0.001,
}
REVERSAL_BOXES = 3
OUTPUT_NAMES = (
    "mexc_pole_opportunities.csv",
    "mexc_pole_trade_plan.csv",
    "mexc_pole_summary.md",
    "mexc_pole_manifest.json",
)
PARITY_FIELDS = ["opportunity_id", "direction", "observable_entry_ts", "entry_price", "stop_price"]


@dataclass(frozen=True)
class ParityConfig:
    symbol_inputs: dict[str, Path]
    columns_inputs: dict[str, Path]
    candles_inputs: dict[str, Path]
    expected_opportunities: Path
    candle_symbols: dict[str, str]


TRADE_PLAN_FIELDS = [
    "symbol",
    "direction",
    "observable_entry_ts",
    "entry_time_utc",
    "entry_price",
    "stop_price",
    "risk_per_unit",
    "target_price",
    "break_even_trigger_price",
    "fixed_risk_usdt",
    "position_qty",
    "approximate_notional_usdt",
    "invalidation_rule",
    "source_opportunity_id",
    "source_row_reference",
]


def _normalize_parity_value(field: str, value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if field in {"entry_price", "stop_price"} and text:
        return f"{_round(float(text)):.6f}"
    if field == "observable_entry_ts" and text:
        return str(int(float(text)))
    return text


def _parity_rows_from_opportunities(opportunities: list[Opportunity]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for opportunity in opportunities:
        rep = opportunity.representative
        rows.append(
            {
                "opportunity_id": opportunity.opportunity_id,
                "direction": rep.direction,
                "observable_entry_ts": _normalize_parity_value("observable_entry_ts", rep.observable_entry_ts),
                "entry_price": _normalize_parity_value("entry_price", rep.entry),
                "stop_price": _normalize_parity_value("stop_price", rep.stop),
            }
        )
    return rows


def validate_historical_parity(config: ParityConfig) -> dict[str, Any]:
    if not config.expected_opportunities.is_file():
        raise ValueError(f"historical opportunity artifact is required for parity: {config.expected_opportunities}")
    symbols, observations, _candles_by_symbol = _load_observations(
        config.symbol_inputs,
        config.columns_inputs,
        config.candles_inputs,
        config.candle_symbols,
    )
    generated = _parity_rows_from_opportunities(_build_opportunities(observations))
    with config.expected_opportunities.open("r", newline="") as handle:
        expected = [
            {field: _normalize_parity_value(field, row.get(field)) for field in PARITY_FIELDS}
            for row in csv.DictReader(handle)
        ]
    if len(generated) != len(expected):
        raise ValueError(f"historical parity failed: opportunity count generated={len(generated)} expected={len(expected)}")
    generated_by_id = {row["opportunity_id"]: row for row in generated}
    expected_by_id = {row["opportunity_id"]: row for row in expected}
    if set(generated_by_id) != set(expected_by_id):
        missing = sorted(set(expected_by_id) - set(generated_by_id))[:10]
        extra = sorted(set(generated_by_id) - set(expected_by_id))[:10]
        raise ValueError(f"historical parity failed: opportunity ids diverge missing={missing} extra={extra}")
    divergences: list[str] = []
    for opportunity_id in sorted(expected_by_id):
        for field in PARITY_FIELDS[1:]:
            generated_value = generated_by_id[opportunity_id][field]
            expected_value = expected_by_id[opportunity_id][field]
            if generated_value != expected_value:
                divergences.append(f"{opportunity_id}.{field}: generated={generated_value} expected={expected_value}")
                break
    if divergences:
        raise ValueError("historical parity failed: " + "; ".join(divergences[:10]))
    return {
        "status": "PASS",
        "symbols": symbols,
        "opportunity_count": len(generated),
        "validated_fields": PARITY_FIELDS,
        "expected_opportunities": str(config.expected_opportunities),
    }


def _load_market_candles(db_path: Path, symbol: str) -> list[tuple[int, float, float, float, float]]:
    with sqlite3.connect(str(db_path)) as connection:
        rows = connection.execute(
            "SELECT close_time, open, high, low, close FROM candles WHERE symbol = ? ORDER BY close_time ASC",
            (symbol,),
        ).fetchall()
    return [(int(ts), float(open_), float(high), float(low), float(close)) for ts, open_, high, low, close in rows]


def _build_columns(symbol: str, candles: list[tuple[int, float, float, float, float]], box_size: float) -> list[TimedColumn]:
    profile = PnFProfile(name=f"{symbol}_bs{box_size:g}_rev{REVERSAL_BOXES}", box_size=box_size, reversal_boxes=REVERSAL_BOXES)
    engine = PnFEngine(profile=profile)
    for close_ts, _open, _high, _low, close in candles:
        engine.update_from_price(close_ts, close)
    return [TimedColumn(col.idx, col.kind, col.top, col.bottom, col.start_ts, col.end_ts) for col in engine.columns]


def _detect_core_observations(symbol: str, columns: list[TimedColumn], box_size: float, candles: list[tuple[int, float, float, float, float]]) -> list[EntryTimingObservation]:
    candle_objects = [Candle(ts, open_, high, low, close) for ts, open_, high, low, close in candles]
    columns_by_idx = {column.idx: column for column in columns}
    observations: list[EntryTimingObservation] = []
    for row_number, motif in enumerate(detect_pole_patterns(columns, box_size), start=2):
        if not _is_core_motif(motif):
            continue
        direction = _direction(motif.get("pattern_name"))
        if direction is None:
            continue
        pole = columns_by_idx.get(int(motif["pole_column_index"]))
        reversal = columns_by_idx.get(int(motif["reversal_column_index"]))
        if pole is None or reversal is None:
            continue
        expected = ("O", "X") if direction == "LONG" else ("X", "O")
        if (pole.kind, reversal.kind) != expected:
            continue
        observations.append(_candidate_observation(symbol, row_number, direction, ENTRY_CANDIDATE, pole, reversal, columns_by_idx.get(reversal.idx + 1), box_size, candle_objects))
    return observations


def _observable_opportunities(opportunities: list[Opportunity]) -> list[Opportunity]:
    return [opp for opp in opportunities if opp.representative.geometry_status == "OBSERVABLE" and opp.representative.entry is not None and opp.representative.stop is not None and opp.representative.observable_entry_ts is not None]


def _select_one_position_per_symbol(opportunities: list[Opportunity]) -> list[PortfolioTrade]:
    # This is a trade-plan runner, not an execution simulator, so open position state is
    # unknown. Enforce the baseline constraint conservatively by keeping only the first
    # observable plan per symbol in timestamp order. The PortfolioTrade shape mirrors the
    # portfolio audit output without inventing exits.
    selected: list[PortfolioTrade] = []
    planned_symbols: set[str] = set()
    for opportunity in sorted(
        opportunities,
        key=lambda opp: (
            int(opp.representative.observable_entry_ts),
            opp.representative.symbol,
            opp.opportunity_id,
        ),
    ):
        rep = opportunity.representative
        if rep.symbol in planned_symbols:
            continue
        selected.append(
            PortfolioTrade(
                trade_id=f"PLAN-{len(selected) + 1:06d}",
                opportunity_id=opportunity.opportunity_id,
                symbol=rep.symbol,
                direction=rep.direction,
                entry_ts=int(rep.observable_entry_ts),
                exit_ts=int(rep.observable_entry_ts),
                classification="TRADE_PLAN_ONLY",
                result_r=0.0,
                active_positions_at_entry=len(planned_symbols),
                active_risk_r_at_entry=float(len(planned_symbols)),
                entry_price=rep.entry,
                stop_price=rep.stop,
            )
        )
        planned_symbols.add(rep.symbol)
    return selected


def _trade_plan_rows(trades: list[PortfolioTrade], fixed_risk_usdt: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in sorted(trades, key=lambda row: (row.entry_ts, row.symbol, row.trade_id)):
        assert trade.entry_price is not None and trade.stop_price is not None
        risk = abs(trade.entry_price - trade.stop_price)
        sign = 1.0 if trade.direction == "LONG" else -1.0
        sizing = _risk_sizing_values(trade, fixed_risk_usdt)
        rows.append({
            "symbol": trade.symbol,
            "direction": trade.direction,
            "observable_entry_ts": trade.entry_ts,
            "entry_time_utc": _ts_to_utc(trade.entry_ts),
            "entry_price": _round(trade.entry_price),
            "stop_price": _round(trade.stop_price),
            "risk_per_unit": _round(risk),
            "target_price": _round(trade.entry_price + sign * TARGET_R * risk),
            "break_even_trigger_price": _round(trade.entry_price + sign * BREAK_EVEN_TRIGGER_R * risk),
            "fixed_risk_usdt": sizing["fixed_risk_usdt"],
            "position_qty": sizing["position_qty"],
            "approximate_notional_usdt": sizing["approximate_notional_usdt"],
            "invalidation_rule": "fixed_3_box_stop; move stop to entry only after +2R; no trailing/scaling/pyramiding",
            "source_opportunity_id": trade.opportunity_id,
            "source_row_reference": trade.opportunity_id,
        })
    return rows


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _parse_box_size(value: str) -> tuple[str, float]:
    symbol, sep, raw = value.partition("=")
    if not sep or not symbol.strip() or not raw.strip():
        raise argparse.ArgumentTypeError("expected SYMBOL=BOX_SIZE")
    return symbol.strip(), float(raw)


def run(db_path: Path = DEFAULT_DB, output_root: Path = DEFAULT_OUTPUT_ROOT, fixed_risk_usdt: float = 50.0, box_sizes: dict[str, float] | None = None, symbols: tuple[str, ...] = TARGET_SYMBOLS, parity_config: ParityConfig | None = None) -> None:
    if parity_config is None:
        raise ValueError("historical parity configuration is required; refusing to generate MEXC plans without proving 7-market baseline parity")
    parity_result = validate_historical_parity(parity_config)
    box_sizes = {**DEFAULT_BOX_SIZES, **(box_sizes or {})}
    output_root.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing MEXC Pole output(s): {', '.join(existing)}")
    all_observations: list[EntryTimingObservation] = []
    symbol_stats: dict[str, Any] = {}
    for symbol in symbols:
        candles = _load_market_candles(db_path, symbol)
        columns = _build_columns(symbol, candles, box_sizes[symbol]) if candles else []
        observations = _detect_core_observations(symbol, columns, box_sizes[symbol], candles) if candles else []
        all_observations.extend(observations)
        symbol_stats[symbol] = {"candles": len(candles), "columns": len(columns), "observations": len(observations), "box_size": box_sizes[symbol]}
    opportunities = _observable_opportunities(_build_opportunities(all_observations))
    trades = _select_one_position_per_symbol(opportunities)
    plan_rows = _trade_plan_rows(trades, fixed_risk_usdt)
    opportunity_fields = ["opportunity_id", "symbol", "direction", "observable_entry_ts", "observable_entry_time_utc", "entry_price", "stop_price", "representative_row_number", "cluster_size", "member_row_numbers", "grouping_key", "geometry_status"]
    _write_csv(output_root / OUTPUT_NAMES[0], opportunity_fields, _simple_opportunity_rows(opportunities))
    _write_csv(output_root / OUTPUT_NAMES[1], TRADE_PLAN_FIELDS, plan_rows)
    with (output_root / OUTPUT_NAMES[2]).open("x") as handle:
        handle.write("# MEXC Pole Strategy v1 trade plan\n\n")
        handle.write("Research only: no live orders, scanner changes, order execution changes, optimization, pyramiding, scaling, trailing, or indicator filters.\n\n")
        handle.write("Baseline copied: `NEXT_COLUMN_OPEN_ENTRY`, fixed 3-box stop, fixed 2.5R target, and +2R break-even trigger.\n\n")
        handle.write(f"Trade plans: {len(plan_rows)}.\n\n")
        handle.write("| symbol | candles | PnF columns | observations | box size |\n|---|---:|---:|---:|---:|\n")
        for symbol, stats in symbol_stats.items():
            handle.write(f"| {symbol} | {stats['candles']} | {stats['columns']} | {stats['observations']} | {stats['box_size']} |\n")
    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "mexc_pole_strategy_v1",
        "historical_parity": parity_result,
        "research_only": True,
        "live_orders": False,
        "production_modifications": False,
        "scanner_modified": False,
        "entry": ENTRY_CANDIDATE,
        "stop": "fixed_3_box_stop",
        "target_R": TARGET_R,
        "break_even_after_R": BREAK_EVEN_TRIGGER_R,
        "position_sizing_formula": "position_qty = fixed_risk_usdt / abs(entry_price - stop_price)",
        "fixed_risk_usdt": fixed_risk_usdt,
        "positioning": "one position per symbol; no pyramiding; no scaling; no trailing",
        "symbols": list(symbols),
        "symbol_stats": symbol_stats,
        "opportunities": len(opportunities),
        "trade_plans": len(plan_rows),
        "artifacts": list(OUTPUT_NAMES[:-1]),
    }
    with (output_root / OUTPUT_NAMES[3]).open("x") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _simple_opportunity_rows(opportunities: list[Opportunity]) -> list[dict[str, Any]]:
    rows = []
    for opp in opportunities:
        rep = opp.representative
        rows.append({
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
        })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only MEXC Pole Strategy v1 trade-plan runner")
    parser.add_argument("--candles-db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--fixed-risk-usdt", type=float, default=50.0)
    parser.add_argument("--box-size", action="append", type=_parse_box_size, default=[], metavar="SYMBOL=BOX_SIZE")
    parser.add_argument("--historical-opportunities", type=Path, default=DEFAULT_HISTORICAL_OPPORTUNITIES)
    parser.add_argument("--parity-symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--parity-columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--parity-candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--parity-candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    args = parser.parse_args()
    try:
        parity_config = ParityConfig(
            symbol_inputs=dict(args.parity_symbol_input),
            columns_inputs=dict(args.parity_columns_input),
            candles_inputs=dict(args.parity_candles_input),
            expected_opportunities=args.historical_opportunities,
            candle_symbols=dict(args.parity_candle_symbol),
        )
        run(args.candles_db_path, args.output_root, args.fixed_risk_usdt, dict(args.box_size), parity_config=parity_config)
    except (FileExistsError, OSError, sqlite3.Error, ValueError, KeyError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
