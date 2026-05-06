#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
PNF_MVP_DIR = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_DIR) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_DIR))

from pnf_engine import PnFEngine, PnFProfile  # noqa: E402
from strategy_engine import evaluate_pullback_retest_long, evaluate_pullback_retest_short  # noqa: E402
from structure_engine import build_structure_state  # noqa: E402

VALIDATION_ELIGIBLE_STATUSES = {"CANDIDATE", "WATCH"}
DEFAULT_FUNNEL_CSV_PATH = "exports/shadow_research_scanner.csv"

FUNNEL_FIELD_ORDER = [
    "symbol",
    "reference_ts",
    "side",
    "status",
    "strategy",
    "reason",
    "reject_reason",
    "quality_score",
    "quality_grade",
    "trend_state",
    "trend_regime",
    "immediate_slope",
    "breakout_context",
    "market_state",
    "latest_signal_name",
    "is_extended_move",
    "active_leg_boxes",
    "zone_low",
    "zone_high",
    "ideal_entry",
    "invalidation",
    "risk",
    "tp1",
    "tp2",
    "rr1",
    "rr2",
    "decision_version",
    "decision_path",
    "watch_flags",
    "reject_flags",
    "promotion_checklist_pass_count",
    "promotion_checklist_failed_items",
    "entry_to_support_boxes",
    "invalidation_distance_boxes",
    "pullback_position_bucket",
    "breakout_context_rank",
    "extension_risk_score",
    "is_baseline_profile_match",
    "shadow_v4_version",
    "shadow_v4_status",
    "shadow_v4_reason",
    "shadow_v4_flags",
    "shadow_v4_rule_hit",
    "shadow_v4_status_delta",
    "shadow_v4_registration_eligible",
    "shadow_continuation_candidate",
    "shadow_continuation_trigger",
    "shadow_entry_price",
    "shadow_stop_price",
    "shadow_continuation_candidate_id",
    "shadow_krausz_short_candidate",
    "shadow_krausz_short_entry",
    "shadow_krausz_short_stop",
    "shadow_krausz_short_tp1",
    "shadow_krausz_short_tp2",
    "shadow_krausz_bounce_short_candidate",
    "shadow_krausz_bounce_short_entry",
    "shadow_krausz_bounce_short_stop",
    "shadow_krausz_bounce_short_tp1",
    "shadow_krausz_bounce_short_tp2",
    "shadow_reversal_long_candidate",
    "shadow_reversal_long_entry",
    "shadow_reversal_long_stop",
    "shadow_reversal_long_tp1",
    "shadow_reversal_long_tp2",
    "early_trend_candidate_flag",
    "blocked_by_existing_open_trade",
    "blocked_by_watch_cap",
    "registered_to_validation",
]


@dataclass(frozen=True)
class ShadowEventState:
    column_count: int
    current_column_kind: str | None
    current_column_top: float | None
    current_column_bottom: float | None
    latest_signal_name: str | None
    breakout_context: str | None
    active_leg_boxes: int | None


def load_settings(settings_path: str) -> dict:
    with open(settings_path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_settings_relative_path(settings_path: str, raw_path: str) -> str:
    path = Path(raw_path)
    if path.is_absolute():
        return str(path)
    settings_parent = Path(settings_path).resolve().parent
    candidate = settings_parent / path
    if candidate.exists():
        return str(candidate)
    return str((REPO_ROOT / path).resolve())


def build_profiles(settings: dict) -> Dict[str, PnFProfile]:
    profiles: Dict[str, PnFProfile] = {}
    for symbol in settings["symbols"]:
        p = settings["profiles"][symbol]
        profiles[symbol] = PnFProfile(
            name=symbol,
            box_size=float(p["box_size"]),
            reversal_boxes=int(p["reversal_boxes"]),
        )
    return profiles


def split_symbols(settings: dict, symbols_arg: str | None) -> List[str]:
    if not symbols_arg:
        return list(settings["symbols"])
    wanted = [s.strip() for s in symbols_arg.split(",") if s.strip()]
    return [s for s in settings["symbols"] if s in wanted]


def load_all_closed_candles(database_path: str, symbol: str, sample_limit: int | None = None) -> List[dict]:
    uri = f"file:{Path(database_path).resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        if sample_limit is None:
            cur = conn.execute(
                """
                SELECT close_time, close, high, low
                FROM candles
                WHERE symbol = ? AND interval = '1m'
                ORDER BY open_time ASC
                """,
                (symbol,),
            )
            candles = [dict(r) for r in cur.fetchall()]
        else:
            cur = conn.execute(
                """
                SELECT close_time, close, high, low
                FROM candles
                WHERE symbol = ? AND interval = '1m'
                ORDER BY open_time DESC
                LIMIT ?
                """,
                (symbol, int(sample_limit)),
            )
            candles = list(reversed([dict(r) for r in cur.fetchall()]))
    finally:
        conn.close()
    return candles[:-1] if len(candles) > 1 else []


def _coerce_bool_int(value: Any) -> int:
    return 1 if bool(value) else 0


def _current_column_parts(engine: PnFEngine) -> tuple[int, str | None, float | None, float | None]:
    current_col = engine.columns[-1] if engine.columns else None
    return (
        len(engine.columns),
        str(getattr(current_col, "kind", "")).upper() if current_col is not None else None,
        float(getattr(current_col, "top")) if current_col is not None else None,
        float(getattr(current_col, "bottom")) if current_col is not None else None,
    )


def _event_state_from_engine(engine: PnFEngine, structure: Dict[str, Any] | None = None) -> ShadowEventState:
    column_count, current_column_kind, current_column_top, current_column_bottom = _current_column_parts(engine)
    active_leg_boxes = None
    if structure is not None and structure.get("active_leg_boxes") is not None:
        active_leg_boxes = int(structure["active_leg_boxes"])
    return ShadowEventState(
        column_count=column_count,
        current_column_kind=current_column_kind,
        current_column_top=current_column_top,
        current_column_bottom=current_column_bottom,
        latest_signal_name=engine.latest_signal_name(),
        breakout_context=str(structure.get("breakout_context")) if structure and structure.get("breakout_context") is not None else None,
        active_leg_boxes=active_leg_boxes,
    )


def _basic_event_state_from_engine(engine: PnFEngine) -> tuple[int, str | None, float | None, float | None, str | None]:
    column_count, current_column_kind, current_column_top, current_column_bottom = _current_column_parts(engine)
    return (column_count, current_column_kind, current_column_top, current_column_bottom, engine.latest_signal_name())


def _compute_shadow_continuation_fields(
    *, setup: Dict[str, Any], structure: Dict[str, Any], columns: List[Any], profile: PnFProfile
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "shadow_continuation_candidate": 0,
        "shadow_continuation_trigger": 0,
        "shadow_entry_price": None,
        "shadow_stop_price": None,
        "shadow_continuation_candidate_id": None,
    }
    if str(setup.get("side") or "").upper() != "LONG":
        return out
    if str(structure.get("breakout_context") or "").upper() != "LATE_EXTENSION":
        return out
    if str(structure.get("trend_regime") or "").upper() != "BULLISH_REGIME":
        return out
    active_leg_boxes = structure.get("active_leg_boxes")
    if active_leg_boxes is None or int(active_leg_boxes) < 4:
        return out
    if not columns:
        return out
    box_size = float(profile.box_size)
    current_col = columns[-1]
    if str(getattr(current_col, "kind", "")).upper() == "O":
        pullback_depth_boxes = int(
            round(abs(float(getattr(current_col, "top", 0.0)) - float(getattr(current_col, "bottom", 0.0))) / box_size)
        )
        if pullback_depth_boxes <= 4:
            out["shadow_continuation_candidate"] = 1
    if len(columns) < 3:
        return out
    new_x = columns[-1]
    prev_o = columns[-2]
    if str(getattr(new_x, "kind", "")).upper() != "X" or str(getattr(prev_o, "kind", "")).upper() != "O":
        return out
    prev_x = None
    for col in reversed(columns[:-2]):
        if str(getattr(col, "kind", "")).upper() == "X":
            prev_x = col
            break
    if prev_x is None:
        return out
    prev_o_depth_boxes = int(
        round(abs(float(getattr(prev_o, "top", 0.0)) - float(getattr(prev_o, "bottom", 0.0))) / box_size)
    )
    if prev_o_depth_boxes > 4:
        return out
    if float(getattr(new_x, "top", 0.0)) <= float(getattr(prev_x, "top", 0.0)):
        return out
    out["shadow_continuation_trigger"] = 1
    out["shadow_entry_price"] = float(getattr(prev_x, "top", 0.0))
    out["shadow_stop_price"] = float(getattr(prev_o, "bottom", 0.0))
    return out


def _compute_shadow_krausz_short_fields(*, structure: Dict[str, Any], columns: List[Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "shadow_krausz_short_candidate": 0,
        "shadow_krausz_short_entry": None,
        "shadow_krausz_short_stop": None,
        "shadow_krausz_short_tp1": None,
        "shadow_krausz_short_tp2": None,
    }
    if str(structure.get("latest_signal_name") or "").upper() != "SELL":
        return out
    if str(structure.get("breakout_context") or "").upper() != "FRESH_BEARISH_BREAKDOWN":
        return out
    if not columns:
        return out
    current_col = columns[-1]
    if str(getattr(current_col, "kind", "")).upper() != "O":
        return out
    entry = float(getattr(current_col, "bottom", 0.0))
    stop = None
    for col in reversed(columns[:-1]):
        if str(getattr(col, "kind", "")).upper() == "X":
            stop = float(getattr(col, "top", 0.0))
            break
    if stop is None:
        resistance_level = structure.get("resistance_level")
        if isinstance(resistance_level, (int, float)):
            stop = float(resistance_level)
    if stop is None:
        return out
    risk = stop - entry
    if risk <= 0:
        return out
    out["shadow_krausz_short_candidate"] = 1
    out["shadow_krausz_short_entry"] = entry
    out["shadow_krausz_short_stop"] = stop
    out["shadow_krausz_short_tp1"] = entry - (2.0 * risk)
    out["shadow_krausz_short_tp2"] = entry - (3.0 * risk)
    return out


def _compute_shadow_krausz_bounce_short_fields(*, structure: Dict[str, Any], columns: List[Any], max_bounce_boxes: int = 2) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "shadow_krausz_bounce_short_candidate": 0,
        "shadow_krausz_bounce_short_entry": None,
        "shadow_krausz_bounce_short_stop": None,
        "shadow_krausz_bounce_short_tp1": None,
        "shadow_krausz_bounce_short_tp2": None,
    }
    if str(structure.get("latest_signal_name") or "").upper() != "SELL":
        return out
    if "BREAKDOWN" not in str(structure.get("breakout_context") or "").upper():
        return out
    if len(columns) < 4:
        return out
    resumed_o = columns[-1]
    bounce_x = columns[-2]
    breakdown_o = columns[-3]
    prior_x = columns[-4]
    if str(getattr(resumed_o, "kind", "")).upper() != "O":
        return out
    if str(getattr(bounce_x, "kind", "")).upper() != "X":
        return out
    if str(getattr(breakdown_o, "kind", "")).upper() != "O":
        return out
    if str(getattr(prior_x, "kind", "")).upper() != "X":
        return out
    bounce_size_boxes = int(round(abs(float(getattr(bounce_x, "top", 0.0)) - float(getattr(bounce_x, "bottom", 0.0)))))
    if bounce_size_boxes > max_bounce_boxes:
        return out
    breakdown_level = float(getattr(breakdown_o, "bottom", 0.0))
    failure_reclaim_level = float(getattr(prior_x, "top", 0.0))
    bounce_high = float(getattr(bounce_x, "top", 0.0))
    resumed_low = float(getattr(resumed_o, "bottom", 0.0))
    if bounce_high >= failure_reclaim_level:
        return out
    if resumed_low >= breakdown_level:
        return out
    entry = breakdown_level
    stop = bounce_high
    risk = stop - entry
    if risk <= 0:
        return out
    out["shadow_krausz_bounce_short_candidate"] = 1
    out["shadow_krausz_bounce_short_entry"] = entry
    out["shadow_krausz_bounce_short_stop"] = stop
    out["shadow_krausz_bounce_short_tp1"] = entry - (2.0 * risk)
    out["shadow_krausz_bounce_short_tp2"] = entry - (3.0 * risk)
    return out


def _compute_shadow_reversal_long_fields(*, structure: Dict[str, Any], columns: List[Any], lookahead_columns: int = 4) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "shadow_reversal_long_candidate": 0,
        "shadow_reversal_long_entry": None,
        "shadow_reversal_long_stop": None,
        "shadow_reversal_long_tp1": None,
        "shadow_reversal_long_tp2": None,
    }
    if str(structure.get("latest_signal_name") or "").upper() != "SELL":
        return out
    active_leg_boxes = structure.get("active_leg_boxes")
    active_leg_boxes_value = int(active_leg_boxes) if isinstance(active_leg_boxes, (int, float)) else None
    breakout_context = str(structure.get("breakout_context") or "").upper()
    if not (breakout_context == "LATE_EXTENSION" and active_leg_boxes_value is not None and active_leg_boxes_value >= 5):
        return out
    if not columns:
        return out
    breakdown_idx = None
    breakdown_level = None
    for idx in range(len(columns) - 1, -1, -1):
        col = columns[idx]
        if str(getattr(col, "kind", "")).upper() != "O":
            continue
        low = float(getattr(col, "bottom", 0.0))
        prev_o_lows = [
            float(getattr(prev, "bottom", 0.0))
            for prev in columns[:idx]
            if str(getattr(prev, "kind", "")).upper() == "O"
        ]
        if not prev_o_lows or low <= min(prev_o_lows):
            breakdown_idx = idx
            breakdown_level = low
            break
    if breakdown_idx is None or breakdown_level is None:
        return out
    end_idx = min(len(columns), breakdown_idx + lookahead_columns + 1)
    crossed_back_above = False
    for col in columns[breakdown_idx + 1 : end_idx]:
        if float(getattr(col, "top", 0.0)) > breakdown_level:
            crossed_back_above = True
            break
    if not crossed_back_above:
        return out
    lows_after_breakdown = [
        float(getattr(col, "bottom", 0.0))
        for col in columns[breakdown_idx:end_idx]
        if str(getattr(col, "kind", "")).upper() == "O"
    ]
    if not lows_after_breakdown:
        return out
    stop = min(lows_after_breakdown)
    entry = breakdown_level
    risk = entry - stop
    if risk <= 0:
        return out
    out["shadow_reversal_long_candidate"] = 1
    out["shadow_reversal_long_entry"] = entry
    out["shadow_reversal_long_stop"] = stop
    out["shadow_reversal_long_tp1"] = entry + (2.0 * risk)
    out["shadow_reversal_long_tp2"] = entry + (3.0 * risk)
    return out


def evaluate_setups(symbol: str, profile: PnFProfile, engine: PnFEngine, structure: Dict[str, Any]) -> List[dict]:
    setup_long = evaluate_pullback_retest_long(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )
    setup_short = evaluate_pullback_retest_short(
        symbol=symbol,
        profile=profile,
        columns=engine.columns,
        structure_state=structure,
    )
    return [s for s in (setup_long, setup_short) if s]


def build_funnel_row(
    *,
    symbol: str,
    reference_ts: int,
    setup: Dict[str, Any],
    structure: Dict[str, Any],
    columns: List[Any],
    profile: PnFProfile,
) -> Dict[str, Any]:
    shadow_v4_status = setup.get("shadow_v4_status", setup.get("status"))
    shadow_v4_watch_flags = str(setup.get("watch_flags") or "").strip()
    shadow_v4_reject_flags = str(setup.get("reject_flags") or "").strip()
    shadow_v4_flags = setup.get("shadow_v4_flags")
    if shadow_v4_flags is None:
        shadow_v4_flags = "|".join([x for x in (shadow_v4_watch_flags, shadow_v4_reject_flags) if x])
    shadow_v4_registration_eligible = setup.get("shadow_v4_registration_eligible")
    if shadow_v4_registration_eligible is None:
        shadow_v4_registration_eligible = 1 if str(shadow_v4_status or "").upper() in VALIDATION_ELIGIBLE_STATUSES else 0
    row = {
        "symbol": symbol,
        "reference_ts": int(reference_ts),
        "side": setup.get("side"),
        "status": setup.get("status"),
        "strategy": setup.get("strategy"),
        "reason": setup.get("reason"),
        "reject_reason": setup.get("reject_reason"),
        "quality_score": setup.get("quality_score"),
        "quality_grade": setup.get("quality_grade"),
        "trend_state": structure.get("trend_state"),
        "trend_regime": structure.get("trend_regime"),
        "immediate_slope": structure.get("immediate_slope"),
        "breakout_context": structure.get("breakout_context"),
        "market_state": structure.get("market_state"),
        "latest_signal_name": structure.get("latest_signal_name"),
        "is_extended_move": _coerce_bool_int(structure.get("is_extended_move", False)),
        "active_leg_boxes": structure.get("active_leg_boxes"),
        "zone_low": setup.get("zone_low"),
        "zone_high": setup.get("zone_high"),
        "ideal_entry": setup.get("ideal_entry"),
        "invalidation": setup.get("invalidation"),
        "risk": setup.get("risk"),
        "tp1": setup.get("tp1"),
        "tp2": setup.get("tp2"),
        "rr1": setup.get("rr1"),
        "rr2": setup.get("rr2"),
        "decision_version": setup.get("decision_version"),
        "decision_path": setup.get("decision_path"),
        "watch_flags": setup.get("watch_flags"),
        "reject_flags": setup.get("reject_flags"),
        "promotion_checklist_pass_count": setup.get("promotion_checklist_pass_count"),
        "promotion_checklist_failed_items": setup.get("promotion_checklist_failed_items"),
        "entry_to_support_boxes": setup.get("entry_to_support_boxes"),
        "invalidation_distance_boxes": setup.get("invalidation_distance_boxes"),
        "pullback_position_bucket": setup.get("pullback_position_bucket"),
        "breakout_context_rank": setup.get("breakout_context_rank"),
        "extension_risk_score": setup.get("extension_risk_score"),
        "is_baseline_profile_match": setup.get("is_baseline_profile_match"),
        "shadow_v4_version": setup.get("shadow_v4_version", setup.get("decision_version")),
        "shadow_v4_status": shadow_v4_status,
        "shadow_v4_reason": setup.get("shadow_v4_reason", setup.get("reason")),
        "shadow_v4_flags": shadow_v4_flags,
        "shadow_v4_rule_hit": setup.get("shadow_v4_rule_hit", setup.get("decision_path")),
        "shadow_v4_status_delta": setup.get("shadow_v4_status_delta"),
        "shadow_v4_registration_eligible": shadow_v4_registration_eligible,
        "shadow_continuation_candidate": 0,
        "shadow_continuation_trigger": 0,
        "shadow_entry_price": None,
        "shadow_stop_price": None,
        "shadow_continuation_candidate_id": None,
        "shadow_krausz_short_candidate": 0,
        "shadow_krausz_short_entry": None,
        "shadow_krausz_short_stop": None,
        "shadow_krausz_short_tp1": None,
        "shadow_krausz_short_tp2": None,
        "shadow_krausz_bounce_short_candidate": 0,
        "shadow_krausz_bounce_short_entry": None,
        "shadow_krausz_bounce_short_stop": None,
        "shadow_krausz_bounce_short_tp1": None,
        "shadow_krausz_bounce_short_tp2": None,
        "shadow_reversal_long_candidate": 0,
        "shadow_reversal_long_entry": None,
        "shadow_reversal_long_stop": None,
        "shadow_reversal_long_tp1": None,
        "shadow_reversal_long_tp2": None,
        "early_trend_candidate_flag": setup.get("early_trend_candidate_flag"),
        "blocked_by_existing_open_trade": 0,
        "blocked_by_watch_cap": 0,
        "registered_to_validation": 0,
    }
    row.update(_compute_shadow_continuation_fields(setup=setup, structure=structure, columns=columns, profile=profile))
    row.update(_compute_shadow_krausz_short_fields(structure=structure, columns=columns))
    row.update(_compute_shadow_krausz_bounce_short_fields(structure=structure, columns=columns))
    row.update(_compute_shadow_reversal_long_fields(structure=structure, columns=columns))
    return row


def _has_shadow_candidate_flag(row: Dict[str, Any]) -> bool:
    for key, value in row.items():
        if key.startswith("shadow_") and key.endswith("_candidate") and int(value or 0) == 1:
            return True
    return False


def write_funnel_csv(rows: List[Dict[str, Any]], csv_path: str) -> str:
    out_path = Path(csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FUNNEL_FIELD_ORDER, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return str(out_path.resolve())


def process_symbol(symbol: str, profile: PnFProfile, candles: List[dict]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    engine = PnFEngine(profile)
    rows: List[Dict[str, Any]] = []
    previous_basic_state: tuple[int, str | None, float | None, float | None, str | None] | None = None
    previous_event_state: ShadowEventState | None = None
    shadow_continuation_pending_candidate_id: str | None = None
    t_symbol = time.perf_counter()
    counters: Dict[str, Any] = {
        "candles_processed": 0,
        "events_processed": 0,
        "events_skipped": 0,
        "candidates_generated": 0,
    }

    for candle in candles:
        close_ts = int(candle["close_time"])
        close_price = float(candle["close"])
        engine.update_from_price(close_ts, close_price)
        counters["candles_processed"] += 1

        basic_state = _basic_event_state_from_engine(engine)
        if basic_state == previous_basic_state:
            counters["events_skipped"] += 1
            continue
        previous_basic_state = basic_state

        structure = build_structure_state(
            symbol=symbol,
            profile=profile,
            columns=engine.columns,
            latest_signal_name=engine.latest_signal_name(),
            market_state=engine.market_state(),
            last_price=getattr(engine, "last_price", None),
        )
        event_state = _event_state_from_engine(engine, structure)
        if event_state == previous_event_state:
            counters["events_skipped"] += 1
            continue
        previous_event_state = event_state
        counters["events_processed"] += 1

        setups = evaluate_setups(symbol, profile, engine, structure)
        for setup in setups:
            funnel_row = build_funnel_row(
                symbol=symbol,
                reference_ts=close_ts,
                setup=setup,
                structure=structure,
                columns=engine.columns,
                profile=profile,
            )
            if str(setup.get("side") or "").upper() == "LONG" and str(structure.get("breakout_context") or "").upper() == "LATE_EXTENSION":
                current_col = engine.columns[-1] if engine.columns else None
                current_col_kind = str(getattr(current_col, "kind", "")).upper() if current_col is not None else ""
                current_col_idx = getattr(current_col, "idx", None) if current_col is not None else None
                if int(funnel_row.get("shadow_continuation_candidate") or 0) == 1 and current_col_kind == "O" and current_col_idx is not None:
                    shadow_continuation_pending_candidate_id = f"{symbol}:{int(current_col_idx)}"
                    funnel_row["shadow_continuation_candidate_id"] = shadow_continuation_pending_candidate_id
                if int(funnel_row.get("shadow_continuation_trigger") or 0) == 1:
                    prev_o = engine.columns[-2] if len(engine.columns) >= 2 else None
                    prev_o_idx = getattr(prev_o, "idx", None) if prev_o is not None else None
                    expected_candidate_id = f"{symbol}:{int(prev_o_idx)}" if prev_o_idx is not None else None
                    if shadow_continuation_pending_candidate_id is None or expected_candidate_id != shadow_continuation_pending_candidate_id:
                        funnel_row["shadow_continuation_trigger"] = 0
                        funnel_row["shadow_entry_price"] = None
                        funnel_row["shadow_stop_price"] = None
                    else:
                        funnel_row["shadow_continuation_candidate_id"] = shadow_continuation_pending_candidate_id
                        shadow_continuation_pending_candidate_id = None
            if _has_shadow_candidate_flag(funnel_row):
                rows.append(funnel_row)
                counters["candidates_generated"] += 1

    counters["elapsed_s"] = time.perf_counter() - t_symbol
    candles_processed = int(counters["candles_processed"])
    counters["event_ratio"] = (float(counters["events_processed"]) / float(candles_processed)) if candles_processed else 0.0
    return rows, counters


def main() -> None:
    parser = argparse.ArgumentParser(description="Candidate-only shadow research scanner with event-driven PnF execution")
    parser.add_argument("--settings", default="pnf_mvp/settings.research_clean.json")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--funnel-csv", default=DEFAULT_FUNNEL_CSV_PATH)
    parser.add_argument("--sample-limit", type=int, default=None)
    args = parser.parse_args()

    settings = load_settings(args.settings)
    database_path = resolve_settings_relative_path(args.settings, settings["database_path"])
    profiles = build_profiles(settings)
    symbols = split_symbols(settings, args.symbols)

    all_rows: List[Dict[str, Any]] = []
    totals = {
        "candles_processed": 0,
        "events_processed": 0,
        "events_skipped": 0,
        "candidates_generated": 0,
        "elapsed_s": 0.0,
    }
    for symbol in symbols:
        candles = load_all_closed_candles(database_path, symbol, args.sample_limit)
        rows, counters = process_symbol(symbol, profiles[symbol], candles)
        all_rows.extend(rows)
        for key in totals:
            totals[key] += counters[key]
        print(
            f"[EVENT] symbol={symbol} elapsed_s={counters['elapsed_s']:.3f} "
            f"candles_processed={counters['candles_processed']} "
            f"events_processed={counters['events_processed']} "
            f"events_skipped={counters['events_skipped']} "
            f"candidates_generated={counters['candidates_generated']} "
            f"event_ratio={counters['event_ratio']:.6f}"
        )

    output_path = write_funnel_csv(all_rows, args.funnel_csv)
    total_candles = int(totals["candles_processed"])
    event_ratio = (float(totals["events_processed"]) / float(total_candles)) if total_candles else 0.0
    print(
        f"[EVENT_TOTAL] elapsed_s={totals['elapsed_s']:.3f} "
        f"candles_processed={totals['candles_processed']} "
        f"events_processed={totals['events_processed']} "
        f"events_skipped={totals['events_skipped']} "
        f"candidates_generated={totals['candidates_generated']} "
        f"event_ratio={event_ratio:.6f}"
    )
    print(f"Wrote {len(all_rows)} candidate rows to {output_path}")


if __name__ == "__main__":
    main()
