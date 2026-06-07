"""Research-only shadow forward validation for frozen P+2 survivor CAND-000053.

This module validates exactly one already-discovered candidate definition in
chronological expanding windows. It performs no optimization, parameter search,
retrospective ranking, detector change, strategy change, live trader change, or
candidate promotion.
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import statistics
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from research_v2.patterns.pole_be_research_audit import _be_classify
from research_v2.patterns.pole_core_motif_execution_reality_audit import _build_opportunities
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _parse_candle_symbol
from research_v2.patterns.pole_core_motif_sl_candidates import _parse_symbol_input, _round
from research_v2.patterns.pole_p2_candidate_sampler import SamplerOutcome, _metric_counts, _universe_consistency
from research_v2.patterns.pole_p2_causal_motif_audit import BREAK_EVEN_TRIGGER_R, EXPECTED_SYMBOLS, MOTIF_NAME, _validate_full_universe
from research_v2.patterns.pole_p2_edge_segmentation_audit import BASELINE_P2_EXPECTANCY, BASELINE_P2_TOTAL_R, BASELINE_P2_TRADES, _segment_map, load_segmented_observations
from research_v2.patterns.pole_p2_candidate_stability_audit import CandidateRule, _matches_rule, _parse_rule_definition

TARGET_CANDIDATE_ID = "CAND-000053"
FROZEN_RULE_DEFINITION = (
    "LONG(direction in [LONG]) + "
    "NEAR_RECENT_AVG_0_75X_1_25X(relative_pole_size in [NEAR_RECENT_AVG_0_75X_1_25X]) + "
    "NORMAL_REVERSAL_4_6_BOXES(reversal_boxes in [NORMAL_REVERSAL_4_6_BOXES])"
)
DEFAULT_MIN_FORWARD_WINDOWS = 1
OUTPUT_NAMES = (
    "p2_survivor_forward_validation_summary.md",
    "p2_survivor_forward_validation_windows.csv",
    "p2_survivor_forward_validation_metrics.csv",
    "p2_survivor_forward_validation_manifest.json",
)
ALLOWED_VERDICTS = (
    "FORWARD_EDGE_SURVIVES",
    "FORWARD_EDGE_WEAKENS",
    "FORWARD_EDGE_DISAPPEARS",
    "INSUFFICIENT_DATA",
)
WINDOW_FIELDS = [
    "candidate_id",
    "window_id",
    "train_start_quarter",
    "train_end_quarter",
    "train_quarters",
    "forward_quarter",
    "candidate_fixed_before_forward",
    "trades",
    "wins",
    "losses",
    "break_even_exits",
    "win_rate",
    "expectancy",
    "total_R",
]
METRIC_FIELDS = ["metric", "value"]


@dataclass(frozen=True)
class ForwardOutcome:
    outcome: SamplerOutcome
    entry_ts: int | None


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _staging_root(output_root: Path) -> Path:
    return output_root.parent / f".{output_root.name}.tmp-{uuid.uuid4().hex}"


def _ensure_complete_artifact_set(root: Path) -> None:
    missing = [name for name in OUTPUT_NAMES if not (root / name).is_file()]
    if missing:
        raise FileNotFoundError(f"staged CAND-000053 forward validation output is incomplete: {', '.join(missing)}")


def _publish_staged_outputs(staging_root: Path, output_root: Path) -> None:
    _ensure_complete_artifact_set(staging_root)
    if output_root.exists():
        if any(output_root.iterdir()):
            raise FileExistsError(f"refusing to publish into non-empty output root: {output_root}")
        output_root.rmdir()
    staging_root.replace(output_root)


def _quarter_sort_key(quarter: str) -> tuple[int, int, str]:
    try:
        year_text, quarter_text = quarter.split("-Q", 1)
        return int(year_text), int(quarter_text), quarter
    except ValueError:
        return (9999, 99, quarter)


def _frozen_rule() -> CandidateRule:
    return CandidateRule(
        candidate_id=TARGET_CANDIDATE_ID,
        source_rank=53,
        source_candidate_verdict="FROZEN_SURVIVOR",
        rule_width=3,
        rule_definition=FROZEN_RULE_DEFINITION,
        predicates=_parse_rule_definition(FROZEN_RULE_DEFINITION),
        source_metrics={
            "candidate_id": TARGET_CANDIDATE_ID,
            "trades": "304",
            "expectancy": "0.210526",
            "total_R": "64.0",
        },
    )


def _build_forward_outcomes(segmented: list[Any], candles_by_symbol: dict[str, list[Any]]) -> list[ForwardOutcome]:
    by_key = {(row.observation.symbol, row.observation.row_number): row for row in segmented}
    opportunities = _build_opportunities([row.observation for row in segmented])
    rows: list[ForwardOutcome] = []
    for opportunity in opportunities:
        rep = opportunity.representative
        segment_row = by_key[(rep.symbol, rep.row_number)]
        classification, realized_r, _ts, _details = _be_classify(rep, candles_by_symbol[rep.symbol], BREAK_EVEN_TRIGGER_R)
        segments = dict(_segment_map(segment_row))
        year, quarter = _ts_to_year_quarter(rep.observable_entry_ts)
        segments.update({"year": year, "quarter": quarter})
        outcome = SamplerOutcome(
            outcome_id=opportunity.opportunity_id,
            observation_count=len(opportunity.observations),
            symbol=rep.symbol,
            direction=rep.direction,
            classification=classification,
            realized_r=realized_r,
            segments=segments,
        )
        rows.append(ForwardOutcome(outcome=outcome, entry_ts=rep.observable_entry_ts))
    return sorted(rows, key=lambda row: ((row.entry_ts if row.entry_ts is not None else 10**30), row.outcome.outcome_id))


def _ts_to_year_quarter(ts: int | None) -> tuple[str, str]:
    if ts is None:
        return "UNKNOWN", "UNKNOWN"
    dt = datetime.fromtimestamp(ts / 1000 if ts > 10_000_000_000 else ts, tz=UTC)
    return str(dt.year), f"{dt.year}-Q{((dt.month - 1) // 3) + 1}"


def _trade_rows(rows: Iterable[ForwardOutcome]) -> list[ForwardOutcome]:
    return [row for row in rows if row.outcome.realized_r is not None]


def _max_drawdown(realized_rs: Iterable[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for realized_r in realized_rs:
        equity += realized_r
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return _round(max_dd)


def _dispersion(values: list[float]) -> float | str:
    if not values:
        return ""
    if len(values) == 1:
        return 0.0
    return _round(statistics.pstdev(values))


def _verdict(window_rows: list[dict[str, Any]], aggregate: dict[str, Any], min_forward_windows: int) -> str:
    if len(window_rows) < min_forward_windows or int(aggregate.get("total_trades") or 0) <= 0:
        return "INSUFFICIENT_DATA"
    total_expectancy = aggregate.get("total_expectancy")
    if total_expectancy == "" or float(total_expectancy) <= 0.0:
        return "FORWARD_EDGE_DISAPPEARS"
    if int(aggregate.get("negative_window_count") or 0) > int(aggregate.get("positive_window_count") or 0):
        return "FORWARD_EDGE_WEAKENS"
    if aggregate.get("median_window_expectancy") != "" and float(aggregate["median_window_expectancy"]) <= 0.0:
        return "FORWARD_EDGE_WEAKENS"
    return "FORWARD_EDGE_SURVIVES"


def _window_rows(selected: list[ForwardOutcome]) -> list[dict[str, Any]]:
    quarters = sorted(
        {row.outcome.segments.get("quarter", "UNKNOWN") for row in selected if row.outcome.segments.get("quarter", "UNKNOWN") != "UNKNOWN"},
        key=_quarter_sort_key,
    )
    rows: list[dict[str, Any]] = []
    for index in range(2, len(quarters)):
        train = quarters[:index]
        forward = quarters[index]
        forward_outcomes = [row.outcome for row in selected if row.outcome.segments.get("quarter") == forward]
        metrics = _metric_counts(forward_outcomes)
        rows.append({
            "candidate_id": TARGET_CANDIDATE_ID,
            "window_id": f"WF-{len(rows) + 1:03d}",
            "train_start_quarter": train[0],
            "train_end_quarter": train[-1],
            "train_quarters": ";".join(train),
            "forward_quarter": forward,
            "candidate_fixed_before_forward": "True",
            "trades": metrics.get("trades", ""),
            "wins": metrics.get("wins", ""),
            "losses": metrics.get("losses", ""),
            "break_even_exits": metrics.get("break_even_exits", ""),
            "win_rate": metrics.get("win_rate", ""),
            "expectancy": metrics.get("expectancy", ""),
            "total_R": metrics.get("total_R", ""),
        })
    return rows


def _aggregate_metrics(window_rows: list[dict[str, Any]], selected: list[ForwardOutcome]) -> dict[str, Any]:
    forward_quarters = {row["forward_quarter"] for row in window_rows}
    forward_trade_outcomes = [row.outcome for row in selected if row.outcome.segments.get("quarter") in forward_quarters]
    counts = _metric_counts(forward_trade_outcomes)
    expectancies = [float(row["expectancy"]) for row in window_rows if row.get("expectancy") != ""]
    realized_rs = [row.outcome.realized_r for row in selected if row.outcome.segments.get("quarter") in forward_quarters and row.outcome.realized_r is not None]
    positive = sum(1 for value in expectancies if value > 0.0)
    negative = sum(1 for value in expectancies if value < 0.0)
    flat = sum(1 for value in expectancies if value == 0.0)
    return {
        "total_trades": counts.get("trades", 0),
        "total_wins": counts.get("wins", 0),
        "total_losses": counts.get("losses", 0),
        "total_break_even_exits": counts.get("break_even_exits", 0),
        "total_win_rate": counts.get("win_rate", ""),
        "total_expectancy": counts.get("expectancy", ""),
        "total_R": counts.get("total_R", ""),
        "average_window_expectancy": _round(sum(expectancies) / len(expectancies)) if expectancies else "",
        "median_window_expectancy": _round(statistics.median(expectancies)) if expectancies else "",
        "positive_window_count": positive,
        "negative_window_count": negative,
        "flat_window_count": flat,
        "best_window_expectancy": _round(max(expectancies)) if expectancies else "",
        "worst_window_expectancy": _round(min(expectancies)) if expectancies else "",
        "expectancy_dispersion": _dispersion(expectancies),
        "max_drawdown_R": _max_drawdown(realized_rs),
    }


def _write_summary(path: Path, window_rows: list[dict[str, Any]], metrics: dict[str, Any], verdict: str, quarters: list[str]) -> None:
    with path.open("x", encoding="utf-8") as handle:
        handle.write("# CAND-000053 shadow forward validation\n\n")
        handle.write("Research only. No optimization, parameter search, GA, candidate modification, new filter, detector change, strategy change, production change, live trader change, or promotion was performed.\n\n")
        handle.write(f"## Verdict: **{verdict}**\n\n")
        handle.write("## Frozen candidate\n\n")
        handle.write(f"- `candidate_id`: {TARGET_CANDIDATE_ID}\n")
        handle.write(f"- `rule_definition`: {FROZEN_RULE_DEFINITION}\n")
        handle.write("- `candidate_fixed_from_beginning_to_end`: True\n")
        handle.write("- `retrospective_ranking`: False\n")
        handle.write("- `re_selection`: False\n\n")
        handle.write("## Chronological windows\n\n")
        handle.write(f"- available selected quarters: {'; '.join(quarters) if quarters else 'NONE'}\n")
        handle.write(f"- expanding forward windows emitted: {len(window_rows)}\n")
        handle.write("- rule: first two available quarters are treated as the initial training history; each later quarter is evaluated as the next fixed forward window.\n\n")
        handle.write("## Aggregate metrics across forward windows\n\n")
        for key in (
            "total_trades",
            "total_expectancy",
            "total_R",
            "average_window_expectancy",
            "median_window_expectancy",
            "positive_window_count",
            "negative_window_count",
            "best_window_expectancy",
            "worst_window_expectancy",
            "expectancy_dispersion",
            "max_drawdown_R",
        ):
            handle.write(f"- `{key}`: {metrics.get(key, '')}\n")
        handle.write("\n## Forward windows\n\n")
        for row in window_rows:
            handle.write(
                f"- `{row['window_id']}` train `{row['train_start_quarter']}` → `{row['train_end_quarter']}`, "
                f"forward `{row['forward_quarter']}`: trades={row['trades']}, wins={row['wins']}, "
                f"losses={row['losses']}, BE={row['break_even_exits']}, win_rate={row['win_rate']}, "
                f"expectancy={row['expectancy']}, total_R={row['total_R']}\n"
            )
        handle.write("\n## Output artifacts\n\n")
        for name in OUTPUT_NAMES[1:]:
            handle.write(f"- `{name}`\n")


def run(
    symbol_inputs: dict[str, Path],
    columns_inputs: dict[str, Path],
    candles_inputs: dict[str, Path],
    output_root: Path,
    candle_symbols: dict[str, str] | None = None,
    *,
    limit_rows_per_symbol: int | None = None,
    require_full_universe: bool = True,
    min_forward_windows: int = DEFAULT_MIN_FORWARD_WINDOWS,
) -> dict[str, Any]:
    output_root = output_root.resolve()
    output_root.parent.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_NAMES if (output_root / name).exists()]
    if existing:
        raise FileExistsError(f"refusing to overwrite existing CAND-000053 forward validation output(s): {', '.join(existing)}")

    symbols, segmented, candles_by_symbol, load_flags = load_segmented_observations(
        symbol_inputs,
        columns_inputs,
        candles_inputs,
        candle_symbols or {},
        limit_rows_per_symbol=limit_rows_per_symbol,
    )
    _validate_full_universe(symbols, require_full_universe)
    all_forward = _build_forward_outcomes(segmented, candles_by_symbol)
    universe = _universe_consistency(_metric_counts(row.outcome for row in all_forward))
    rule = _frozen_rule()
    selected = [row for row in all_forward if _matches_rule(row.outcome, rule)]
    selected_trade_rows = _trade_rows(selected)
    selected_quarters = sorted(
        {row.outcome.segments.get("quarter", "UNKNOWN") for row in selected if row.outcome.segments.get("quarter", "UNKNOWN") != "UNKNOWN"},
        key=_quarter_sort_key,
    )
    windows = _window_rows(selected)
    metrics = _aggregate_metrics(windows, selected)
    verdict = _verdict(windows, metrics, min_forward_windows)
    metric_rows = [{"metric": key, "value": value} for key, value in {"verdict": verdict, **metrics}.items()]

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "stage": "pole_p2_survivor_forward_validation",
        "research_only": True,
        "scope": "shadow_forward_validation_only",
        "candidate_id": TARGET_CANDIDATE_ID,
        "frozen_rule_definition": FROZEN_RULE_DEFINITION,
        "candidate_modifications": False,
        "new_filters": False,
        "production_modifications": False,
        "live_trader_modifications": False,
        "detector_modifications": False,
        "strategy_modifications": False,
        "strategy_promotion": False,
        "genetic_algorithm": False,
        "optimization": False,
        "parameter_search": False,
        "retrospective_ranking": False,
        "re_selection": False,
        "chronological_evaluation_only": True,
        "initial_train_quarters": 2,
        "motif": MOTIF_NAME,
        "required_symbols": list(EXPECTED_SYMBOLS),
        "full_seven_market_universe": set(symbols) == set(EXPECTED_SYMBOLS),
        "symbols": symbols,
        "selected_quarters": selected_quarters,
        "selected_trade_count": len(selected_trade_rows),
        "selected_classification_counts": dict(Counter(row.outcome.classification for row in selected)),
        "baseline_reference": {"trades": BASELINE_P2_TRADES, "expectancy": BASELINE_P2_EXPECTANCY, "total_R": BASELINE_P2_TOTAL_R},
        "universe_consistency": universe,
        "allowed_verdicts": list(ALLOWED_VERDICTS),
        "verdict": verdict,
        "artifacts": list(OUTPUT_NAMES[:-1]),
        "complete_artifact_set": list(OUTPUT_NAMES),
        "artifact_publish_mode": "staged_directory_replace",
        "artifact_write_completed": True,
        "load_flags": load_flags,
    }

    staging_root = _staging_root(output_root)
    try:
        staging_root.mkdir(parents=False, exist_ok=False)
        _write_csv(staging_root / OUTPUT_NAMES[1], WINDOW_FIELDS, windows)
        _write_csv(staging_root / OUTPUT_NAMES[2], METRIC_FIELDS, metric_rows)
        with (staging_root / OUTPUT_NAMES[3]).open("x", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
            handle.write("\n")
        _write_summary(staging_root / OUTPUT_NAMES[0], windows, metrics, verdict, selected_quarters)
        _publish_staged_outputs(staging_root, output_root)
    except Exception:
        shutil.rmtree(staging_root, ignore_errors=True)
        raise
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-only shadow forward validation for frozen CAND-000053")
    parser.add_argument("--symbol-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--columns-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV")
    parser.add_argument("--candles-input", action="append", required=True, type=_parse_symbol_input, metavar="SYMBOL=CSV_OR_DB")
    parser.add_argument("--candle-symbol", action="append", default=[], type=_parse_candle_symbol, metavar="SYMBOL=DB_SYMBOL")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--limit-rows-per-symbol", type=int, help="optional small-sample diagnostic cap after P+2 motif filtering")
    parser.add_argument("--allow-partial-universe", action="store_true", help="allow test/diagnostic runs outside the full BTC/ETH/SOL/ENA/HYPE/SUI/TAO universe")
    parser.add_argument("--min-forward-windows", type=int, default=DEFAULT_MIN_FORWARD_WINDOWS)
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
            min_forward_windows=args.min_forward_windows,
        )
    except (FileExistsError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
