"""Research-only candle reality simulation for AB=CD Model C retests.

This module consumes only existing local research artifacts and an existing clean
research candle database/CSV. It does not reconstruct ABCDs, use FAST artifacts,
create production strategy logic, optimize parameters, model fees/slippage,
leverage, or provide trade recommendations.
"""

from __future__ import annotations

import argparse
import csv
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from research_v2.patterns.pnf_abcd_geometry_audit import SYMBOLS, YEARS, _fmt, _parse_float, _parse_time

CONFLUENCE_INPUT = Path("research_v2/patterns/abcd_prz_confirmation_confluence_local_v1/abcd_prz_confirmation_confluence_candidates.csv")
FEASIBILITY_INPUT = Path("research_v2/patterns/abcd_retest_feasibility_local_v1/abcd_retest_feasibility_candidates.csv")
ENTRY_INPUT = Path("research_v2/patterns/abcd_retest_entry_level_local_v1/abcd_retest_entry_level_candidates.csv")
DEFAULT_CANDLES_INPUT = Path("pnf_mvp/data/pnf_mvp.sqlite3")
DEFAULT_OUTPUT_ROOT = Path("research_v2/patterns/abcd_candle_reality_sim_v1")

EXPECTED_COHORT_COUNT = 1281
ENTRY_MODEL = "ENTRY_RETRACE_382"
ENTRY_THRESHOLD = 0.382
TARGETS = (1, 2, 3)
FINAL_DECISION_YES = "CANDLE_REALITY_JUSTIFIES_STRATEGY_RESEARCH"
FINAL_DECISION_NO = "CANDLE_REALITY_REJECTS_PROXY_EDGE"
CLASSIFICATIONS = ("TARGET_FIRST", "STOP_FIRST", "SAME_CANDLE_AMBIGUOUS", "NOT_REACHED", "UNKNOWN_MISSING_CANDLES")

CANDIDATE_FIELDS = [
    "candidate_id", "symbol", "year", "post_d_reaction_direction", "first_post_d_reaction_boxes",
    "retest_event_time", "entry_model", "entry_price", "box_size", "risk_boxes", "risk_price",
    "target_1R_price", "target_1R_classification", "target_1R_first_event_time",
    "target_2R_price", "target_2R_classification", "target_2R_first_event_time",
    "target_3R_price", "target_3R_classification", "target_3R_first_event_time", "details",
]
SUMMARY_FIELDS = [
    "total_candidates_loaded", "candidates_with_candle_coverage", "unknown_missing_candle_count",
    "same_candle_ambiguity_count", "target_1R_target_first_count", "target_1R_target_first_pct",
    "target_1R_stop_first_count", "target_1R_stop_first_pct", "target_2R_target_first_count",
    "target_2R_target_first_pct", "target_2R_stop_first_count", "target_2R_stop_first_pct",
    "target_3R_target_first_count", "target_3R_target_first_pct", "target_3R_stop_first_count",
    "target_3R_stop_first_pct", "final_decision",
]
BY_SCOPE_FIELDS = ["scope", "value", *SUMMARY_FIELDS]
BY_TARGET_FIELDS = ["target_r", "candidates", *[f"{name.lower()}_count" for name in CLASSIFICATIONS], *[f"{name.lower()}_pct" for name in CLASSIFICATIONS]]

@dataclass(frozen=True)
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float


def _text(value: Any) -> str:
    return str(value or "").strip()


def _yes(value: Any) -> bool:
    return _text(value).upper() in {"1", "TRUE", "YES", "Y"}


def _require_fields(path: Path, fieldnames: Sequence[str] | None, required: Iterable[str]) -> None:
    if not fieldnames:
        raise ValueError(f"{path}: expected CSV header")
    missing = [field for field in required if field not in fieldnames]
    if missing:
        raise ValueError(f"{path}: missing required fields: {', '.join(missing)}")


def _first(row: dict[str, Any], aliases: Sequence[str]) -> str:
    for alias in aliases:
        value = _text(row.get(alias))
        if value:
            return value
    return ""


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _load_keyed_csv(path: Path, required: Sequence[str]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _require_fields(path, reader.fieldnames, required)
        for row_number, row in enumerate(reader, start=2):
            candidate_id = _text(row.get("candidate_id"))
            if not candidate_id:
                raise ValueError(f"{path}:{row_number}: missing candidate_id")
            if candidate_id in out:
                raise ValueError(f"{path}:{row_number}: duplicate candidate_id {candidate_id}")
            out[candidate_id] = {key: _text(value) for key, value in row.items()}
    return out


def _load_candidates(confluence_input: Path, feasibility_input: Path, entry_input: Path) -> list[dict[str, Any]]:
    confluence = _load_keyed_csv(confluence_input, ("candidate_id", "symbol", "year", "PRZ_VALID_AND_CONFIRMED_13", "first_post_d_reaction_boxes"))
    feasibility = _load_keyed_csv(feasibility_input, ("candidate_id", "retrace_pct_of_first_reaction"))
    entry = _load_keyed_csv(entry_input, ("candidate_id", "qualifies_RETRACE_382"))
    cohort_ids = {cid for cid, row in confluence.items() if _yes(row.get("PRZ_VALID_AND_CONFIRMED_13"))}
    if len(cohort_ids) != EXPECTED_COHORT_COUNT:
        raise ValueError(f"PRZ_VALID_AND_CONFIRMED_13 cohort count changed: expected {EXPECTED_COHORT_COUNT}, observed {len(cohort_ids)}")
    missing = sorted(cohort_ids - set(feasibility) | cohort_ids - set(entry))[:10]
    if missing:
        raise ValueError(f"strict candidate_id join failed for local artifacts; missing sample={missing}")
    rows: list[dict[str, Any]] = []
    for cid in sorted(cohort_ids):
        base = {**confluence[cid], **feasibility[cid], **entry[cid], "candidate_id": cid}
        if not _yes(base.get("qualifies_RETRACE_382")):
            continue
        first_boxes = _parse_float(base.get("first_post_d_reaction_boxes"))
        retrace_pct = _parse_float(base.get("retrace_pct_of_first_reaction"))
        if first_boxes is None or first_boxes < 13 or retrace_pct is None or retrace_pct < ENTRY_THRESHOLD:
            continue
        rows.append(base)
    return rows


def _candle_from_row(row: dict[str, Any]) -> Candle:
    ts = _first(row, ("close_time", "close_ts", "timestamp", "ts"))
    return Candle(int(float(ts)), float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]))


def _load_candles(path: Path, symbol: str) -> list[Candle]:
    if path.suffix.lower() == ".csv":
        with path.open("r", newline="", encoding="utf-8") as handle:
            rows = [_candle_from_row(row) for row in csv.DictReader(handle) if _text(row.get("symbol", symbol)) == symbol]
    else:
        with sqlite3.connect(str(path)) as connection:
            values = connection.execute("SELECT close_time, open, high, low, close FROM candles WHERE symbol = ? ORDER BY close_time ASC", (symbol,)).fetchall()
        rows = [Candle(int(ts), float(open_), float(high), float(low), float(close)) for ts, open_, high, low, close in values]
    return sorted(rows, key=lambda candle: candle.ts)


def _direction(row: dict[str, Any]) -> str:
    value = _first(row, ("post_d_reaction_direction", "candidate_direction", "direction")).upper()
    if value in {"UP", "LONG", "BULL", "BULLISH"}:
        return "UP"
    if value in {"DOWN", "SHORT", "BEAR", "BEARISH"}:
        return "DOWN"
    raise ValueError(f"candidate {row.get('candidate_id')}: missing/invalid post-D reaction direction")


def _event_ts(row: dict[str, Any]) -> int | None:
    value = _first(row, ("retest_event_time", "retrace_time", "retest_time", "retrace_knowledge_time", "retest_knowledge_time"))
    parsed = _parse_time(value)
    return int(parsed) if parsed is not None else None


def _entry_price(row: dict[str, Any], candles: Sequence[Candle], event_ts: int | None) -> float | None:
    direct = _parse_float(_first(row, ("ENTRY_RETRACE_382_entry_price", "entry_retrace_382_price", "entry_price", "retest_entry_price", "retrace_price")))
    if direct is not None:
        return direct
    matching = next((candle for candle in candles if event_ts is not None and candle.ts >= event_ts), None)
    return matching.close if matching is not None else None


def _classify(candles: Sequence[Candle], event_ts: int | None, direction: str, entry: float | None, risk_price: float, target_r: int) -> tuple[str, str, str]:
    if event_ts is None or entry is None or risk_price <= 0 or not candles:
        return "UNKNOWN_MISSING_CANDLES", "", "missing retest event, entry, risk, or symbol candles"
    replay = [candle for candle in candles if candle.ts > event_ts]
    if not replay:
        return "UNKNOWN_MISSING_CANDLES", "", "no candles after retest event"
    target = entry + target_r * risk_price if direction == "UP" else entry - target_r * risk_price
    stop = entry - risk_price if direction == "UP" else entry + risk_price
    for candle in replay:
        hit_target = candle.high >= target if direction == "UP" else candle.low <= target
        hit_stop = candle.low <= stop if direction == "UP" else candle.high >= stop
        if hit_target and hit_stop:
            return "SAME_CANDLE_AMBIGUOUS", str(candle.ts), "target and stop are both inside the same OHLC candle"
        if hit_target:
            return "TARGET_FIRST", str(candle.ts), "target reached before stop"
        if hit_stop:
            return "STOP_FIRST", str(candle.ts), "stop reached before target"
    return "NOT_REACHED", "", "neither target nor stop reached in available candles"


def _pct(count: int, total: int) -> str:
    return _fmt(count / total) if total else ""


def _summarize(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    covered = sum(1 for row in rows if all(row.get(f"target_{r}R_classification") != "UNKNOWN_MISSING_CANDLES" for r in TARGETS))
    unknown = sum(1 for row in rows if any(row.get(f"target_{r}R_classification") == "UNKNOWN_MISSING_CANDLES" for r in TARGETS))
    ambiguous = sum(1 for row in rows if any(row.get(f"target_{r}R_classification") == "SAME_CANDLE_AMBIGUOUS" for r in TARGETS))
    out: dict[str, Any] = {"total_candidates_loaded": total, "candidates_with_candle_coverage": covered, "unknown_missing_candle_count": unknown, "same_candle_ambiguity_count": ambiguous}
    for r in TARGETS:
        denom = total - sum(1 for row in rows if row.get(f"target_{r}R_classification") == "UNKNOWN_MISSING_CANDLES")
        target_first = sum(1 for row in rows if row.get(f"target_{r}R_classification") == "TARGET_FIRST")
        stop_first = sum(1 for row in rows if row.get(f"target_{r}R_classification") == "STOP_FIRST")
        out[f"target_{r}R_target_first_count"] = target_first
        out[f"target_{r}R_target_first_pct"] = _pct(target_first, denom)
        out[f"target_{r}R_stop_first_count"] = stop_first
        out[f"target_{r}R_stop_first_pct"] = _pct(stop_first, denom)
    out["final_decision"] = FINAL_DECISION_YES if (_parse_float(out.get("target_1R_target_first_pct")) or 0) > (_parse_float(out.get("target_1R_stop_first_pct")) or 0) else FINAL_DECISION_NO
    return out


def _by_target(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in TARGETS:
        counts = {name: sum(1 for row in rows if row.get(f"target_{r}R_classification") == name) for name in CLASSIFICATIONS}
        item = {"target_r": r, "candidates": len(rows)}
        for name in CLASSIFICATIONS:
            item[f"{name.lower()}_count"] = counts[name]
            item[f"{name.lower()}_pct"] = _pct(counts[name], len(rows))
        out.append(item)
    return out


def _scope(rows: Sequence[dict[str, Any]], field: str, values: Sequence[str]) -> list[dict[str, Any]]:
    return [{"scope": field, "value": value, **_summarize([row for row in rows if _text(row.get(field)) == value])} for value in values]


def _write_report(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# AB=CD Candle Reality Simulation — Model C ENTRY_RETRACE_382", "",
        "Research-only candle ordering check using existing local artifacts only. No ABCD reconstruction, FAST artifacts, optimization, fees/slippage, leverage, production strategy, live trading logic, or trade recommendation is included.", "",
        "## Required Answers",
        f"1. Total candidates loaded: {summary['total_candidates_loaded']}",
        f"2. Candidates with candle coverage: {summary['candidates_with_candle_coverage']}",
        f"3. Unknown/missing candle count: {summary['unknown_missing_candle_count']}",
        f"4. 1R target-first count and %: {summary['target_1R_target_first_count']} ({summary['target_1R_target_first_pct']})",
        f"5. 1R stop-first count and %: {summary['target_1R_stop_first_count']} ({summary['target_1R_stop_first_pct']})",
        f"6. 2R target-first count and %: {summary['target_2R_target_first_count']} ({summary['target_2R_target_first_pct']})",
        f"7. 2R stop-first count and %: {summary['target_2R_stop_first_count']} ({summary['target_2R_stop_first_pct']})",
        f"8. 3R target-first count and %: {summary['target_3R_target_first_count']} ({summary['target_3R_target_first_pct']})",
        f"9. 3R stop-first count and %: {summary['target_3R_stop_first_count']} ({summary['target_3R_stop_first_pct']})",
        f"10. Same-candle ambiguity count: {summary['same_candle_ambiguity_count']}",
        "11. Stability across BTCUSDT / ETHUSDT / SOLUSDT: see abcd_candle_reality_by_symbol.csv.",
        "12. Stability across 2024 / 2025 / 2026: see abcd_candle_reality_by_year.csv.", "",
        "## Final Decision", str(summary["final_decision"]),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(confluence_input: Path, feasibility_input: Path, entry_input: Path, candles_input: Path, output_root: Path) -> dict[str, Any]:
    rows = _load_candidates(confluence_input, feasibility_input, entry_input)
    candles_by_symbol = {symbol: _load_candles(candles_input, symbol) for symbol in SYMBOLS}
    candidates: list[dict[str, Any]] = []
    for row in rows:
        symbol = _text(row.get("symbol"))
        candles = candles_by_symbol.get(symbol, [])
        direction = _direction(row)
        event_ts = _event_ts(row)
        first_boxes = _parse_float(row.get("first_post_d_reaction_boxes")) or math.nan
        box_size = _parse_float(_first(row, ("box_size", "pnf_box_size", "box"))) or 1.0
        risk_boxes = ENTRY_THRESHOLD * first_boxes
        risk_price = risk_boxes * box_size if math.isfinite(risk_boxes) else math.nan
        entry = _entry_price(row, candles, event_ts)
        item = {"candidate_id": row["candidate_id"], "symbol": symbol, "year": row.get("year", ""), "post_d_reaction_direction": direction, "first_post_d_reaction_boxes": _fmt(first_boxes), "retest_event_time": event_ts or "", "entry_model": ENTRY_MODEL, "entry_price": _fmt(entry), "box_size": _fmt(box_size), "risk_boxes": _fmt(risk_boxes), "risk_price": _fmt(risk_price), "details": ""}
        details = []
        for r in TARGETS:
            target_price = None if entry is None or not math.isfinite(risk_price) else (entry + r * risk_price if direction == "UP" else entry - r * risk_price)
            classification, first_event, detail = _classify(candles, event_ts, direction, entry, risk_price, r)
            item[f"target_{r}R_price"] = _fmt(target_price)
            item[f"target_{r}R_classification"] = classification
            item[f"target_{r}R_first_event_time"] = first_event
            details.append(f"{r}R={detail}")
        item["details"] = "; ".join(details)
        candidates.append(item)
    summary = _summarize(candidates)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "abcd_candle_reality_summary.csv", [summary], SUMMARY_FIELDS)
    _write_csv(output_root / "abcd_candle_reality_by_symbol.csv", _scope(candidates, "symbol", SYMBOLS), BY_SCOPE_FIELDS)
    _write_csv(output_root / "abcd_candle_reality_by_year.csv", _scope(candidates, "year", [str(year) for year in YEARS]), BY_SCOPE_FIELDS)
    _write_csv(output_root / "abcd_candle_reality_by_target.csv", _by_target(candidates), BY_TARGET_FIELDS)
    _write_csv(output_root / "abcd_candle_reality_candidates.csv", candidates, CANDIDATE_FIELDS)
    _write_report(output_root / "abcd_candle_reality_report.md", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confluence-input", type=Path, default=CONFLUENCE_INPUT)
    parser.add_argument("--feasibility-input", type=Path, default=FEASIBILITY_INPUT)
    parser.add_argument("--entry-input", type=Path, default=ENTRY_INPUT)
    parser.add_argument("--candles-input", type=Path, default=DEFAULT_CANDLES_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    summary = run(args.confluence_input, args.feasibility_input, args.entry_input, args.candles_input, args.output_root)
    print(summary["final_decision"])


if __name__ == "__main__":
    main()
