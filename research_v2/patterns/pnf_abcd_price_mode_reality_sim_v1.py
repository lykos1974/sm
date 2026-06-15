"""Research-only true PRICE_MODE candle reality simulation for AB=CD Model C.

Consumes the execution-context artifact emitted by
``pnf_abcd_execution_context_v1.py`` and replays real OHLC candles after each
validated retest time. This module intentionally contains no structural proxy
mode, optimization, fees/slippage, leverage, production strategy logic, trade
recommendation, ABCD reconstruction, or FAST artifact dependency.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import SYMBOLS, YEARS, _fmt, _parse_float, _parse_time

DEFAULT_CONTEXT_INPUT = Path(
    "research_v2/patterns/abcd_execution_context_v1/abcd_execution_context_candidates.csv"
)
DEFAULT_CANDLES_INPUT = Path("pnf_mvp/data/pnf_mvp_research_clean.sqlite3")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_price_mode_reality_sim_v1")

EXPECTED_EXACT_CONTEXT_COUNT = 1281
EXACT_STATUS = "EXACT_PRICE_CONTEXT"
TARGETS = (1, 2, 3)
TARGET_FIRST = "TARGET_FIRST"
STOP_FIRST = "STOP_FIRST"
SAME_CANDLE_AMBIGUOUS = "SAME_CANDLE_AMBIGUOUS"
NOT_REACHED = "NOT_REACHED"
UNKNOWN_MISSING_CANDLES = "UNKNOWN_MISSING_CANDLES"
CLASSIFICATIONS = (
    TARGET_FIRST,
    STOP_FIRST,
    SAME_CANDLE_AMBIGUOUS,
    NOT_REACHED,
    UNKNOWN_MISSING_CANDLES,
)
FINAL_DECISION_YES = "PRICE_MODE_REALITY_JUSTIFIES_STRATEGY_RESEARCH"
FINAL_DECISION_NO = "PRICE_MODE_REALITY_REJECTS_MODEL_C"

CANDIDATE_FIELDS = [
    "candidate_id",
    "symbol",
    "candle_symbol_used",
    "year",
    "post_d_reaction_direction",
    "retest_time",
    "entry_price",
    "risk_price",
    "stop_price",
    "target_1R_price",
    "target_1R_classification",
    "target_1R_decision_event_time",
    "target_1R_target_hit_time",
    "target_1R_stop_hit_time",
    "target_1R_first_event_time",
    "target_2R_price",
    "target_2R_classification",
    "target_2R_decision_event_time",
    "target_2R_target_hit_time",
    "target_2R_stop_hit_time",
    "target_2R_first_event_time",
    "target_3R_price",
    "target_3R_classification",
    "target_3R_decision_event_time",
    "target_3R_target_hit_time",
    "target_3R_stop_hit_time",
    "target_3R_first_event_time",
    "has_candle_coverage",
    "details",
]
SUMMARY_FIELDS = [
    "total_candidates_loaded",
    "candidates_with_candle_coverage",
    "missing_candle_count",
    "same_candle_ambiguity_count",
    "not_reached_count",
    "decision_event_time_validation_failure_count",
    "target_1R_target_first_count",
    "target_1R_target_first_pct",
    "target_1R_stop_first_count",
    "target_1R_stop_first_pct",
    "target_2R_target_first_count",
    "target_2R_target_first_pct",
    "target_2R_stop_first_count",
    "target_2R_stop_first_pct",
    "target_3R_target_first_count",
    "target_3R_target_first_pct",
    "target_3R_stop_first_count",
    "target_3R_stop_first_pct",
    "final_decision",
]
BY_SCOPE_FIELDS = ["scope", "value", *SUMMARY_FIELDS]
BY_TARGET_FIELDS = [
    "target_r",
    "candidates",
    *[f"{name.lower()}_count" for name in CLASSIFICATIONS],
    *[f"{name.lower()}_pct" for name in CLASSIFICATIONS],
]


@dataclass(frozen=True)
class Candle:
    ts: float
    high: float
    low: float


def _text(value: Any) -> str:
    return str(value or "").strip()


def _require_fields(path: Path, fieldnames: Sequence[str] | None, required: Iterable[str]) -> None:
    if not fieldnames:
        raise ValueError(f"{path}: expected CSV header")
    missing = [field for field in required if field not in fieldnames]
    if missing:
        raise ValueError(f"{path}: missing required fields: {', '.join(missing)}")


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _fmt_optional(value: float | None) -> str:
    return "" if value is None else _fmt(value)


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _load_candidates(path: Path) -> list[dict[str, str]]:
    required = (
        "candidate_id",
        "symbol",
        "year",
        "post_d_reaction_direction",
        "retest_time",
        "entry_price",
        "risk_price",
        "target_1R_price",
        "target_2R_price",
        "target_3R_price",
        "execution_context_status",
    )
    rows: list[dict[str, str]] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, required)
        for row in reader:
            if _text(row.get("execution_context_status")) == EXACT_STATUS:
                rows.append({key: _text(value) for key, value in row.items()})
    if len(rows) != EXPECTED_EXACT_CONTEXT_COUNT:
        raise ValueError(
            f"{EXACT_STATUS} cohort count changed: expected "
            f"{EXPECTED_EXACT_CONTEXT_COUNT}, observed {len(rows)}"
        )
    return rows


def _normalize_ts(value: Any) -> float | None:
    return _parse_time(value)


def _load_candles(path: Path, symbol: str) -> list[Candle]:
    if path.suffix.lower() == ".csv":
        with path.open("r", newline="", encoding="utf-8") as handle:
            raw_rows = list(csv.DictReader(handle))
        rows = [
            Candle(
                ts=_normalize_ts(row.get("close_time") or row.get("close_ts") or row.get("timestamp") or row.get("ts")) or 0,
                high=float(row["high"]),
                low=float(row["low"]),
            )
            for row in raw_rows
            if _text(row.get("symbol", symbol)) == symbol
        ]
    else:
        with sqlite3.connect(str(path)) as connection:
            values = connection.execute(
                "SELECT close_time, high, low FROM candles WHERE symbol = ? ORDER BY close_time ASC",
                (symbol,),
            ).fetchall()
        rows = [Candle(_normalize_ts(ts) or 0, float(high), float(low)) for ts, high, low in values]
    return sorted((row for row in rows if row.ts), key=lambda candle: candle.ts)


def _candle_symbol_fallbacks(symbol: str) -> tuple[str, ...]:
    return (symbol, f"BINANCE_FUT:{symbol}", f"BINANCE:{symbol}")


def _load_candle_fallbacks(path: Path, symbols: Iterable[str]) -> dict[str, list[Candle]]:
    candle_symbols = dict.fromkeys(candidate for symbol in symbols for candidate in _candle_symbol_fallbacks(symbol))
    return {symbol: _load_candles(path, symbol) for symbol in candle_symbols}


def _select_candles(
    candles_by_symbol: dict[str, list[Candle]],
    symbol: str,
    retest_ts: float | None,
) -> tuple[str, list[Candle]]:
    loaded_symbol = ""
    loaded_candles: list[Candle] = []
    for candle_symbol in _candle_symbol_fallbacks(symbol):
        candles = candles_by_symbol.get(candle_symbol, [])
        if retest_ts is not None and any(candle.ts > retest_ts for candle in candles):
            return candle_symbol, candles
        if candles and not loaded_symbol:
            loaded_symbol = candle_symbol
            loaded_candles = candles
    return loaded_symbol, loaded_candles


def _direction(value: Any) -> str:
    direction = _text(value).upper()
    if direction in {"UP", "LONG", "BULL", "BULLISH"}:
        return "UP"
    if direction in {"DOWN", "SHORT", "BEAR", "BEARISH"}:
        return "DOWN"
    raise ValueError(f"missing/invalid post_d_reaction_direction: {value}")


def _stop_price(direction: str, entry_price: float, risk_price: float) -> float:
    return entry_price - risk_price if direction == "UP" else entry_price + risk_price


def _classify(
    candles: Sequence[Candle],
    retest_ts: float | None,
    direction: str,
    stop_price: float,
    target_price: float,
) -> tuple[str, str, str, str, str]:
    if retest_ts is None:
        return UNKNOWN_MISSING_CANDLES, "", "", "", "missing retest_time"
    replay = [candle for candle in candles if candle.ts > retest_ts]
    if not replay:
        return UNKNOWN_MISSING_CANDLES, "", "", "", "no candles after retest_time"
    for candle in replay:
        hit_target = candle.high >= target_price if direction == "UP" else candle.low <= target_price
        hit_stop = candle.low <= stop_price if direction == "UP" else candle.high >= stop_price
        if hit_target and hit_stop:
            event_time = _fmt(candle.ts)
            return SAME_CANDLE_AMBIGUOUS, event_time, event_time, event_time, "target and stop both inside same OHLC candle"
        if hit_target:
            event_time = _fmt(candle.ts)
            return TARGET_FIRST, event_time, event_time, "", "target reached before stop"
        if hit_stop:
            event_time = _fmt(candle.ts)
            return STOP_FIRST, event_time, "", event_time, "stop reached before target"
    return NOT_REACHED, "", "", "", "neither target nor stop reached in available candles"


def _decision_event_time_validation_failure_count(rows: Sequence[dict[str, Any]]) -> int:
    failures = 0
    for row in rows:
        for r in TARGETS:
            classification = row.get(f"target_{r}R_classification")
            decision_event_time = row.get(f"target_{r}R_decision_event_time")
            target_hit_time = row.get(f"target_{r}R_target_hit_time")
            stop_hit_time = row.get(f"target_{r}R_stop_hit_time")
            if classification == TARGET_FIRST and decision_event_time != target_hit_time:
                failures += 1
            elif classification == STOP_FIRST and decision_event_time != stop_hit_time:
                failures += 1
            elif (
                classification == SAME_CANDLE_AMBIGUOUS
                and not (target_hit_time == stop_hit_time == decision_event_time and decision_event_time)
            ):
                failures += 1
    return failures


def _summarize(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    covered = sum(1 for row in rows if row.get("has_candle_coverage") == "1")
    summary: dict[str, Any] = {
        "total_candidates_loaded": total,
        "candidates_with_candle_coverage": covered,
        "missing_candle_count": total - covered,
        "same_candle_ambiguity_count": sum(
            1 for row in rows for r in TARGETS if row.get(f"target_{r}R_classification") == SAME_CANDLE_AMBIGUOUS
        ),
        "not_reached_count": sum(1 for row in rows for r in TARGETS if row.get(f"target_{r}R_classification") == NOT_REACHED),
        "decision_event_time_validation_failure_count": _decision_event_time_validation_failure_count(rows),
    }
    for r in TARGETS:
        denom = sum(1 for row in rows if row.get(f"target_{r}R_classification") != UNKNOWN_MISSING_CANDLES)
        target_first = sum(1 for row in rows if row.get(f"target_{r}R_classification") == TARGET_FIRST)
        stop_first = sum(1 for row in rows if row.get(f"target_{r}R_classification") == STOP_FIRST)
        summary[f"target_{r}R_target_first_count"] = target_first
        summary[f"target_{r}R_target_first_pct"] = _pct(target_first, denom)
        summary[f"target_{r}R_stop_first_count"] = stop_first
        summary[f"target_{r}R_stop_first_pct"] = _pct(stop_first, denom)
    summary["final_decision"] = (
        FINAL_DECISION_YES
        if (_parse_float(summary["target_1R_target_first_pct"]) or 0.0)
        > (_parse_float(summary["target_1R_stop_first_pct"]) or 0.0)
        else FINAL_DECISION_NO
    )
    return summary


def _by_target(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for r in TARGETS:
        candidates = sum(1 for row in rows if row.get(f"target_{r}R_classification") != UNKNOWN_MISSING_CANDLES)
        item: dict[str, Any] = {"target_r": r, "candidates": candidates}
        for name in CLASSIFICATIONS:
            count = sum(1 for row in rows if row.get(f"target_{r}R_classification") == name)
            item[f"{name.lower()}_count"] = count
            item[f"{name.lower()}_pct"] = _pct(count, candidates) if name != UNKNOWN_MISSING_CANDLES else ""
        output.append(item)
    return output


def _scope(rows: Sequence[dict[str, Any]], field: str, values: Sequence[str]) -> list[dict[str, Any]]:
    return [{"scope": field, "value": value, **_summarize([row for row in rows if _text(row.get(field)) == value])} for value in values]


def _write_report(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# AB=CD Price Mode Reality Simulation — Model C",
        "",
        "Research-only true PRICE_MODE candle reality simulation using the execution-context artifact. No structural proxy mode, optimization, fees/slippage, leverage, production strategy, trade recommendation, ABCD reconstruction, or FAST artifacts are included.",
        "",
        "## Required Answers",
        f"1. Total candidates loaded: {summary['total_candidates_loaded']}",
        f"2. Candidates with candle coverage: {summary['candidates_with_candle_coverage']}",
        f"3. Missing candle count: {summary['missing_candle_count']}",
        f"4. 1R target-first count and %: {summary['target_1R_target_first_count']} ({summary['target_1R_target_first_pct']})",
        f"5. 1R stop-first count and %: {summary['target_1R_stop_first_count']} ({summary['target_1R_stop_first_pct']})",
        f"6. 2R target-first count and %: {summary['target_2R_target_first_count']} ({summary['target_2R_target_first_pct']})",
        f"7. 2R stop-first count and %: {summary['target_2R_stop_first_count']} ({summary['target_2R_stop_first_pct']})",
        f"8. 3R target-first count and %: {summary['target_3R_target_first_count']} ({summary['target_3R_target_first_pct']})",
        f"9. 3R stop-first count and %: {summary['target_3R_stop_first_count']} ({summary['target_3R_stop_first_pct']})",
        f"10. Same-candle ambiguity count: {summary['same_candle_ambiguity_count']}",
        f"11. Not reached count: {summary['not_reached_count']}",
        f"12. Decision event time validation failure count: {summary['decision_event_time_validation_failure_count']}",
        "13. Deprecated compatibility fields: target_1R_first_event_time, target_2R_first_event_time, and target_3R_first_event_time are retained as aliases of the corresponding decision_event_time fields; use the explicit decision_event_time, target_hit_time, and stop_hit_time fields for new analysis.",
        "14. Stability across BTCUSDT / ETHUSDT / SOLUSDT: see abcd_price_mode_reality_by_symbol.csv.",
        "15. Candle coverage by candle_symbol_used: see abcd_price_mode_reality_by_candle_symbol_used.csv.",
        "16. Stability across 2024 / 2025 / 2026: see abcd_price_mode_reality_by_year.csv.",
        "",
        "## Final Decision",
        str(summary["final_decision"]),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(context_input: Path, candles_input: Path, output_root: Path) -> dict[str, Any]:
    rows = _load_candidates(context_input)
    candles_by_symbol = _load_candle_fallbacks(candles_input, SYMBOLS)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        direction = _direction(row["post_d_reaction_direction"])
        symbol = row["symbol"]
        retest_ts = _normalize_ts(row["retest_time"])
        candle_symbol_used, candles = _select_candles(candles_by_symbol, symbol, retest_ts)
        entry_price = _parse_float(row["entry_price"])
        risk_price = _parse_float(row["risk_price"])
        targets = {r: _parse_float(row[f"target_{r}R_price"]) for r in TARGETS}
        if entry_price is None or risk_price is None or risk_price <= 0 or any(targets[r] is None for r in TARGETS):
            raise ValueError(f"candidate {row['candidate_id']}: invalid exact price context")
        stop = _stop_price(direction, entry_price, risk_price)
        has_coverage = bool(retest_ts is not None and any(candle.ts > retest_ts for candle in candles))
        item: dict[str, Any] = {
            "candidate_id": row["candidate_id"],
            "symbol": symbol,
            "candle_symbol_used": candle_symbol_used,
            "year": row["year"],
            "post_d_reaction_direction": direction,
            "retest_time": row["retest_time"],
            "entry_price": _fmt_optional(entry_price),
            "risk_price": _fmt_optional(risk_price),
            "stop_price": _fmt_optional(stop),
            "has_candle_coverage": "1" if has_coverage else "0",
        }
        details = []
        for r in TARGETS:
            classification, decision_event_time, target_hit_time, stop_hit_time, detail = _classify(
                candles, retest_ts, direction, stop, targets[r] or 0.0
            )
            item[f"target_{r}R_price"] = _fmt_optional(targets[r])
            item[f"target_{r}R_classification"] = classification
            item[f"target_{r}R_decision_event_time"] = decision_event_time
            item[f"target_{r}R_target_hit_time"] = target_hit_time
            item[f"target_{r}R_stop_hit_time"] = stop_hit_time
            item[f"target_{r}R_first_event_time"] = decision_event_time
            details.append(f"{r}R={detail}")
        item["details"] = "; ".join(details)
        candidates.append(item)
    summary = _summarize(candidates)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "abcd_price_mode_reality_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_price_mode_reality_by_target.csv", _by_target(candidates), BY_TARGET_FIELDS)
    _write_csv(output_root / "abcd_price_mode_reality_by_symbol.csv", _scope(candidates, "symbol", SYMBOLS), BY_SCOPE_FIELDS)
    candle_symbol_values = [symbol for symbol in candles_by_symbol if any(row.get("candle_symbol_used") == symbol for row in candidates)]
    candle_symbol_values.extend([""] if any(not row.get("candle_symbol_used") for row in candidates) else [])
    _write_csv(
        output_root / "abcd_price_mode_reality_by_candle_symbol_used.csv",
        _scope(candidates, "candle_symbol_used", candle_symbol_values),
        BY_SCOPE_FIELDS,
    )
    _write_csv(output_root / "abcd_price_mode_reality_by_year.csv", _scope(candidates, "year", [str(year) for year in YEARS]), BY_SCOPE_FIELDS)
    _write_csv(output_root / "abcd_price_mode_reality_candidates.csv", candidates, CANDIDATE_FIELDS)
    _write_report(output_root / "abcd_price_mode_reality_report.md", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context-input", type=Path, default=DEFAULT_CONTEXT_INPUT)
    parser.add_argument("--candles-input", type=Path, default=DEFAULT_CANDLES_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    summary = run(args.context_input, args.candles_input, args.output_root)
    print(summary["final_decision"])


if __name__ == "__main__":
    main()
