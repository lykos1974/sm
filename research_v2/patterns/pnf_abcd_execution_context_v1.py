"""Research-only AB=CD Model C execution-context artifact builder.

This module joins existing local research artifacts for the validated
PRZ_VALID_AND_CONFIRMED_13 cohort and emits a price-context artifact for the
ENTRY_RETRACE_382 model. It is intentionally limited to context preparation for
a later candle simulation: it does not compute PnL, classify target/stop
ordering, optimize parameters, model fees/slippage/leverage, reconstruct ABCDs,
use FAST artifacts, or create production strategy/trade recommendations.
"""

from __future__ import annotations

import argparse
import csv
import math
import sqlite3
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import (
    DIRECTIONS,
    SYMBOLS,
    YEARS,
    _column_sort,
    _fmt,
    _normalize_symbol,
    _parse_float,
    _parse_time,
)

CONFLUENCE_INPUT = Path(
    "research_v2/patterns/abcd_prz_confirmation_confluence_local_v1/"
    "abcd_prz_confirmation_confluence_candidates.csv"
)
FEASIBILITY_INPUT = Path(
    "research_v2/patterns/abcd_retest_feasibility_local_v1/"
    "abcd_retest_feasibility_candidates.csv"
)
ENTRY_INPUT = Path(
    "research_v2/patterns/abcd_retest_entry_level_local_v1/"
    "abcd_retest_entry_level_candidates.csv"
)
REACTIONS_INPUT = Path(
    "research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/"
    "harmonic_reactions_by_threshold.csv"
)
DEFAULT_DB_INPUT = Path("pnf_mvp/data/pnf_mvp_research_clean.sqlite3")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_execution_context_v1")

EXPECTED_COHORT_COUNT = 1281
CONFIRMATION_THRESHOLD_BOXES = 13.0
ENTRY_MODEL = "ENTRY_RETRACE_382"
ENTRY_RETRACE = 0.382
THRESHOLD_NAME = "SLOW"
REACTION_KIND = "CONFIRMING"
EXACT_STATUS = "EXACT_PRICE_CONTEXT"
APPROX_STATUS = "APPROXIMATED_RETRACE_PRICE"
MISSING_STATUS = "MISSING_PRICE_CONTEXT"
READY = "EXECUTION_CONTEXT_READY_FOR_PRICE_MODE"
NOT_READY = "EXECUTION_CONTEXT_NOT_READY_FOR_PRICE_MODE"
BOX_SIZE_BY_SYMBOL = {"BTCUSDT": 100.0, "ETHUSDT": 5.0, "SOLUSDT": 0.25}

CANDIDATE_FIELDS = [
    "candidate_id",
    "symbol",
    "year",
    "post_d_reaction_direction",
    "first_post_d_reaction_boxes",
    "retrace_pct_of_first_reaction",
    "box_size",
    "d_time",
    "d_column_id",
    "d_direction",
    "d_price",
    "confirmation_time",
    "confirmation_price",
    "retest_time",
    "retest_price",
    "entry_model",
    "entry_price",
    "risk_boxes",
    "risk_price",
    "target_1R_price",
    "target_2R_price",
    "target_3R_price",
    "execution_context_status",
]
SUMMARY_FIELDS = [
    "total_cohort_candidates",
    "exact_execution_context_count",
    "approximated_execution_context_count",
    "missing_execution_context_count",
    "median_risk_boxes",
    "median_risk_price",
    "price_mode_candle_simulation_ready",
    "final_decision",
]
BY_SCOPE_FIELDS = ["scope", "value", *SUMMARY_FIELDS]


@dataclass(frozen=True)
class ReactionRow:
    symbol: str
    direction: str
    boxes: float
    knowledge_time: str
    knowledge_ts: float
    completion_time: str
    completion_ts: float | None
    column_id: str
    column_sort: int
    price: float | None


@dataclass(frozen=True)
class AbcdPivot:
    symbol: str
    column_sort: int
    knowledge_time: str
    knowledge_ts: float
    column_id: str
    direction: str


@dataclass(frozen=True)
class Candle:
    symbol: str
    close_time: str
    close_ts: float
    high: float
    low: float
    close: float


def _text(value: Any) -> str:
    return str(value or "").strip()


def _yes(value: Any) -> bool:
    return _text(value).upper() in {"1", "TRUE", "YES", "Y"}


def _first(row: dict[str, Any], aliases: Sequence[str]) -> str:
    for alias in aliases:
        value = _text(row.get(alias))
        if value:
            return value
    return ""


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


def _load_keyed_csv(path: Path, required: Sequence[str]) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, required)
        for row_number, row in enumerate(reader, start=2):
            candidate_id = _text(row.get("candidate_id"))
            if not candidate_id:
                raise ValueError(f"{path}:{row_number}: missing candidate_id")
            if candidate_id in rows:
                raise ValueError(f"{path}:{row_number}: duplicate candidate_id {candidate_id}")
            rows[candidate_id] = {key: _text(value) for key, value in row.items()}
    return rows


def _load_joined_candidates(confluence_input: Path, feasibility_input: Path, entry_input: Path) -> list[dict[str, Any]]:
    confluence = _load_keyed_csv(
        confluence_input,
        ("candidate_id", "symbol", "year", "PRZ_VALID_AND_CONFIRMED_13", "first_post_d_reaction_boxes"),
    )
    feasibility = _load_keyed_csv(feasibility_input, ("candidate_id", "retrace_pct_of_first_reaction"))
    entry = _load_keyed_csv(entry_input, ("candidate_id", "qualifies_RETRACE_382"))
    cohort_ids = {candidate_id for candidate_id, row in confluence.items() if _yes(row.get("PRZ_VALID_AND_CONFIRMED_13"))}
    if len(cohort_ids) != EXPECTED_COHORT_COUNT:
        raise ValueError(f"PRZ_VALID_AND_CONFIRMED_13 cohort count changed: expected {EXPECTED_COHORT_COUNT}, observed {len(cohort_ids)}")
    missing = sorted((cohort_ids - set(feasibility)) | (cohort_ids - set(entry)))
    if missing:
        raise ValueError(f"strict candidate_id join failed; missing sample={missing[:10]}")
    rows: list[dict[str, Any]] = []
    for candidate_id in sorted(cohort_ids):
        row = {**confluence[candidate_id], **feasibility[candidate_id], **entry[candidate_id], "candidate_id": candidate_id}
        if not _yes(row.get("qualifies_RETRACE_382")):
            row["execution_context_status"] = MISSING_STATUS
        rows.append(row)
    return rows


def _opposite(direction: str) -> str:
    if direction == "UP":
        return "DOWN"
    if direction == "DOWN":
        return "UP"
    raise ValueError(f"unsupported direction: {direction}")


def _load_reactions(path: Path) -> list[ReactionRow]:
    rows: list[ReactionRow] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, ("symbol", "candidate_direction", "knowledge_time", "candidate_boxes"))
        for row_number, row in enumerate(reader, start=2):
            if _text(row.get("threshold_name")).upper() != THRESHOLD_NAME:
                continue
            if _text(row.get("reaction_kind")).upper() != REACTION_KIND:
                continue
            symbol = _normalize_symbol(_text(row.get("symbol")))
            direction = _text(row.get("candidate_direction")).upper()
            boxes = _parse_float(_first(row, ("candidate_boxes", "reaction_boxes")))
            knowledge_time = _text(row.get("knowledge_time"))
            knowledge_ts = _parse_time(knowledge_time)
            completion_time = _first(row, ("completion_time", "candidate_completion_time", "end_time", "knowledge_time"))
            price = _parse_float(_first(row, ("candidate_price", "completion_price", "pivot_price", "price", "close", "end_price")))
            if symbol not in SYMBOLS or direction not in DIRECTIONS or boxes is None or knowledge_ts is None:
                raise ValueError(f"{path}:{row_number}: invalid reaction row")
            rows.append(
                ReactionRow(
                    symbol=symbol,
                    direction=direction,
                    boxes=boxes,
                    knowledge_time=knowledge_time,
                    knowledge_ts=knowledge_ts,
                    completion_time=completion_time,
                    completion_ts=_parse_time(completion_time),
                    column_id=_text(row.get("column_id")),
                    column_sort=_column_sort(row.get("column_id")),
                    price=price,
                )
            )
    return sorted(rows, key=lambda row: (row.symbol, row.knowledge_ts, row.column_sort, row.direction, row.boxes))


def _load_box_sizes(db_input: Path) -> dict[str, float]:
    box_sizes = dict(BOX_SIZE_BY_SYMBOL)
    if not db_input.exists():
        return box_sizes
    try:
        with sqlite3.connect(str(db_input)) as connection:
            names = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")]
            for name in names:
                columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{name}")')]
                if "symbol" not in columns or "box_size" not in columns:
                    continue
                for symbol, box_size in connection.execute(f'SELECT symbol, box_size FROM "{name}" WHERE box_size IS NOT NULL'):
                    normalized = _normalize_symbol(_text(symbol))
                    parsed = _parse_float(box_size)
                    if normalized in SYMBOLS and parsed is not None and parsed > 0:
                        box_sizes[normalized] = parsed
    except sqlite3.Error:
        return box_sizes
    return box_sizes


def _parse_candidate_pivots(candidate_id: str) -> tuple[AbcdPivot, AbcdPivot, AbcdPivot, AbcdPivot] | None:
    parts = _text(candidate_id).split(":")
    if len(parts) < 21 or parts[0] != "ABCD":
        return None
    pivots: list[AbcdPivot] = []
    for offset in (1, 6, 11, 16):
        try:
            symbol = _normalize_symbol(parts[offset])
            column_sort = int(parts[offset + 1])
            knowledge_time = parts[offset + 2]
            knowledge_ts = _parse_time(knowledge_time)
            column_id = parts[offset + 3]
            direction = parts[offset + 4].upper()
        except (IndexError, ValueError):
            return None
        if symbol not in SYMBOLS or knowledge_ts is None or direction not in DIRECTIONS:
            return None
        pivots.append(AbcdPivot(symbol, column_sort, knowledge_time, knowledge_ts, column_id, direction))
    return (pivots[0], pivots[1], pivots[2], pivots[3])


def _d_pivot(row: dict[str, Any]) -> AbcdPivot | None:
    pivots = _parse_candidate_pivots(_text(row.get("candidate_id")))
    return pivots[3] if pivots else None


def _direction(row: dict[str, Any]) -> str:
    direction = _first(row, ("post_d_reaction_direction", "candidate_direction", "direction")).upper()
    if direction not in DIRECTIONS:
        raise ValueError(f"candidate {row.get('candidate_id')}: invalid post_d_reaction_direction")
    return direction


def _find_events(row: dict[str, Any], reactions: Sequence[ReactionRow]) -> tuple[ReactionRow | None, ReactionRow | None, ReactionRow | None]:
    symbol = _normalize_symbol(_text(row.get("symbol")))
    ordered = [reaction for reaction in reactions if reaction.symbol == symbol]
    d = _d_pivot(row)
    d_ts = d.knowledge_ts if d else _parse_time(_first(row, ("d_knowledge_time", "d_time", "candidate_knowledge_time")))
    d_col = _column_sort(d.column_id if d else _first(row, ("d_column_id", "column_id")))
    d_direction = d.direction if d else _opposite(_direction(row))
    d_index = next((i for i, reaction in enumerate(ordered) if (reaction.knowledge_ts, reaction.column_sort, reaction.direction) == (d_ts, d_col, d_direction)), None)
    if d_index is None:
        d_index = next((i for i, reaction in enumerate(ordered) if reaction.knowledge_ts >= (d_ts or 0) and reaction.direction == d_direction), None)
    if d_index is None:
        return None, None, None
    post_direction = _direction(row)
    confirmation_index = next((i for i, reaction in enumerate(ordered[d_index + 1 :], start=d_index + 1) if reaction.direction == post_direction and reaction.boxes >= CONFIRMATION_THRESHOLD_BOXES), None)
    if confirmation_index is None:
        confirmation_index = next((i for i, reaction in enumerate(ordered[d_index + 1 :], start=d_index + 1) if reaction.direction == post_direction), None)
    if confirmation_index is None:
        return ordered[d_index], None, None
    retrace_direction = _opposite(post_direction)
    retrace = next((reaction for reaction in ordered[confirmation_index + 1 :] if reaction.direction == retrace_direction), None)
    return ordered[d_index], ordered[confirmation_index], retrace


def _load_candles(db_input: Path) -> dict[str, list[Candle]]:
    candles: dict[str, list[Candle]] = {symbol: [] for symbol in SYMBOLS}
    if not db_input.exists():
        return candles
    try:
        with sqlite3.connect(str(db_input)) as connection:
            names = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")]
            for name in names:
                columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{name}")')]
                if not {"symbol", "high", "low", "close"}.issubset(columns):
                    continue
                time_col = next((col for col in ("close_time", "close_ts", "timestamp", "ts", "time") if col in columns), None)
                if time_col is None:
                    continue
                query = f'SELECT symbol, "{time_col}", high, low, close FROM "{name}" ORDER BY "{time_col}" ASC'
                for symbol, close_time, high, low, close in connection.execute(query):
                    normalized = _normalize_symbol(_text(symbol))
                    close_ts = _parse_time(close_time)
                    parsed_high = _parse_float(high)
                    parsed_low = _parse_float(low)
                    parsed_close = _parse_float(close)
                    if normalized in candles and close_ts is not None and parsed_high is not None and parsed_low is not None and parsed_close is not None:
                        candles[normalized].append(Candle(normalized, _text(close_time), close_ts, parsed_high, parsed_low, parsed_close))
                if any(candles.values()):
                    break
    except sqlite3.Error:
        return candles
    return {symbol: sorted(rows, key=lambda candle: candle.close_ts) for symbol, rows in candles.items()}


def _candle_at_or_after(candles: Sequence[Candle], ts: float | None) -> Candle | None:
    if ts is None:
        return None
    return next((candle for candle in candles if candle.close_ts >= ts), None)


def _directional_candle_price(candle: Candle | None, direction: str) -> float | None:
    if candle is None:
        return None
    return candle.high if direction == "UP" else candle.low


def _fmt_optional(value: float | None) -> str:
    return _fmt(value) if value is not None and math.isfinite(value) else ""


def _price_targets(direction: str, entry: float | None, risk_price: float | None) -> tuple[str, str, str]:
    if entry is None or risk_price is None:
        return "", "", ""
    sign = 1.0 if direction == "UP" else -1.0
    return tuple(_fmt(entry + sign * multiple * risk_price) for multiple in (1, 2, 3))  # type: ignore[return-value]


def _approx_retest_time(row: dict[str, Any], d: AbcdPivot | None) -> str:
    d_ts = d.knowledge_ts if d else _parse_time(_first(row, ("d_knowledge_time", "d_time", "candidate_knowledge_time")))
    time_to_retrace = _parse_float(_first(row, ("time_to_retrace", "time_to_retrace_ms", "retrace_time_delta_ms")))
    if d_ts is None or time_to_retrace is None:
        return ""
    return _fmt(d_ts + time_to_retrace)


def build_candidate_rows(
    candidates: Sequence[dict[str, Any]],
    reactions: Sequence[ReactionRow],
    box_sizes: dict[str, float],
    candles_by_symbol: dict[str, list[Candle]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in candidates:
        symbol = _normalize_symbol(_text(row.get("symbol")))
        direction = _direction(row)
        first_boxes = _parse_float(row.get("first_post_d_reaction_boxes"))
        retrace_pct = _parse_float(row.get("retrace_pct_of_first_reaction"))
        box_size = box_sizes.get(symbol)
        risk_boxes = ENTRY_RETRACE * first_boxes if first_boxes is not None else None
        risk_price = risk_boxes * box_size if risk_boxes is not None and box_size is not None else None
        d = _d_pivot(row)
        candles = candles_by_symbol.get(symbol, [])
        d_candle = _candle_at_or_after(candles, d.knowledge_ts if d else None)
        d_price = _directional_candle_price(d_candle, d.direction if d else _opposite(direction))
        d_event, confirmation, retest = _find_events(row, reactions)
        confirmation_candle = _candle_at_or_after(candles, confirmation.knowledge_ts if confirmation else None)
        confirmation_price = confirmation.price if confirmation and confirmation.price is not None else _directional_candle_price(confirmation_candle, direction)
        retest_candle = _candle_at_or_after(candles, retest.knowledge_ts if retest else None)
        exact_retest_price = retest.price if retest and retest.price is not None else _directional_candle_price(retest_candle, _opposite(direction))
        approximated_retest_price = None
        if exact_retest_price is None and confirmation_price is not None and risk_price is not None:
            approximated_retest_price = confirmation_price - risk_price if direction == "UP" else confirmation_price + risk_price
        retest_price = exact_retest_price if exact_retest_price is not None else approximated_retest_price
        status = EXACT_STATUS if d_price is not None and confirmation_price is not None and exact_retest_price is not None and risk_price is not None else APPROX_STATUS if d_price is not None and confirmation_price is not None and retest_price is not None and risk_price is not None else MISSING_STATUS
        target_1r, target_2r, target_3r = _price_targets(direction, retest_price, risk_price)
        out.append(
            {
                "candidate_id": row["candidate_id"],
                "symbol": symbol,
                "year": row.get("year", ""),
                "post_d_reaction_direction": direction,
                "first_post_d_reaction_boxes": _fmt_optional(first_boxes),
                "retrace_pct_of_first_reaction": _fmt_optional(retrace_pct),
                "box_size": _fmt_optional(box_size),
                "d_time": d.knowledge_time if d else (d_event.knowledge_time if d_event else _first(row, ("d_knowledge_time", "d_time"))),
                "d_column_id": d.column_id if d else "",
                "d_direction": d.direction if d else "",
                "d_price": _fmt_optional(d_price),
                "confirmation_time": confirmation.knowledge_time if confirmation else "",
                "confirmation_price": _fmt_optional(confirmation_price),
                "retest_time": retest.knowledge_time if retest else _approx_retest_time(row, d),
                "retest_price": _fmt_optional(retest_price),
                "entry_model": ENTRY_MODEL,
                "entry_price": _fmt_optional(retest_price),
                "risk_boxes": _fmt_optional(risk_boxes),
                "risk_price": _fmt_optional(risk_price),
                "target_1R_price": target_1r,
                "target_2R_price": target_2r,
                "target_3R_price": target_3r,
                "execution_context_status": status,
            }
        )
    return out


def _median(values: Iterable[float]) -> str:
    clean = [value for value in values if value is not None and math.isfinite(value)]
    return _fmt(statistics.median(clean)) if clean else ""


def summarize(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    exact = sum(1 for row in rows if row["execution_context_status"] == EXACT_STATUS)
    approximated = sum(1 for row in rows if row["execution_context_status"] == APPROX_STATUS)
    missing = sum(1 for row in rows if row["execution_context_status"] == MISSING_STATUS)
    ready = total == EXPECTED_COHORT_COUNT and missing == 0
    return {
        "total_cohort_candidates": total,
        "exact_execution_context_count": exact,
        "approximated_execution_context_count": approximated,
        "missing_execution_context_count": missing,
        "median_risk_boxes": _median(_parse_float(row.get("risk_boxes")) for row in rows),
        "median_risk_price": _median(_parse_float(row.get("risk_price")) for row in rows),
        "price_mode_candle_simulation_ready": "YES" if ready else "NO",
        "final_decision": READY if ready else NOT_READY,
    }


def _scope(rows: Sequence[dict[str, Any]], field: str, values: Sequence[Any]) -> list[dict[str, Any]]:
    scoped = []
    for value in values:
        subset = [row for row in rows if _text(row.get(field)) == str(value)]
        scoped.append({"scope": field, "value": str(value), **summarize(subset)})
    return scoped


def write_report(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# AB=CD Execution Context Artifact — Model C ENTRY_RETRACE_382",
        "",
        "Research-only execution-context artifact. No PnL, target/stop ordering, trade recommendation, production strategy, ABCD reconstruction, FAST artifacts, optimization, fees/slippage, or leverage is included.",
        "",
        "## Price Context Method",
        "candidate_id is parsed into the four encoded ABCD pivots and the fourth pivot is treated as D. D and confirmation prices are resolved from the first candle at or after the pivot knowledge_time, using candle high for UP pivots/reactions and candle low for DOWN pivots/reactions. ENTRY_RETRACE_382 retest/entry prices use confirmation_price retraced by 0.382 * first_post_d_reaction_boxes * box_size. If exact retrace pivot price is unavailable, the retrace price is approximated and the row is counted as APPROXIMATED_RETRACE_PRICE; if retrace time is unavailable, d_time + time_to_retrace is used when present.",
        "",
        "## Required Answers",
        f"1. Total cohort candidates: {summary['total_cohort_candidates']}",
        f"2. Exact execution context count: {summary['exact_execution_context_count']}",
        f"3. Approximated execution context count: {summary['approximated_execution_context_count']}",
        f"4. Missing execution context count: {summary['missing_execution_context_count']}",
        f"5. Median risk_boxes: {summary['median_risk_boxes']}",
        f"6. Median risk_price: {summary['median_risk_price']}",
        "7. Stability across BTCUSDT / ETHUSDT / SOLUSDT: see abcd_execution_context_by_symbol.csv.",
        "8. Stability across 2024 / 2025 / 2026: see abcd_execution_context_by_year.csv.",
        f"9. Is PRICE_MODE candle simulation ready? {summary['price_mode_candle_simulation_ready']}",
        "",
        "## Final Decision",
        str(summary["final_decision"]),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(confluence_input: Path, feasibility_input: Path, entry_input: Path, reactions_input: Path, db_input: Path, output_root: Path) -> dict[str, Any]:
    candidates = _load_joined_candidates(confluence_input, feasibility_input, entry_input)
    rows = build_candidate_rows(candidates, _load_reactions(reactions_input), _load_box_sizes(db_input), _load_candles(db_input))
    summary = summarize(rows)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "abcd_execution_context_candidates.csv", rows, CANDIDATE_FIELDS)
    _write_csv(output_root / "abcd_execution_context_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_execution_context_by_symbol.csv", _scope(rows, "symbol", SYMBOLS), BY_SCOPE_FIELDS)
    _write_csv(output_root / "abcd_execution_context_by_year.csv", _scope(rows, "year", [str(year) for year in YEARS]), BY_SCOPE_FIELDS)
    write_report(output_root / "abcd_execution_context_report.md", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confluence-input", type=Path, default=CONFLUENCE_INPUT)
    parser.add_argument("--feasibility-input", type=Path, default=FEASIBILITY_INPUT)
    parser.add_argument("--entry-input", type=Path, default=ENTRY_INPUT)
    parser.add_argument("--reactions-input", type=Path, default=REACTIONS_INPUT)
    parser.add_argument("--db-input", type=Path, default=DEFAULT_DB_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    summary = run(args.confluence_input, args.feasibility_input, args.entry_input, args.reactions_input, args.db_input, args.output_root)
    print(summary["final_decision"])


if __name__ == "__main__":
    main()
