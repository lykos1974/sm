"""
trade_management_be.py

Standalone breakeven management layer for strategy validation / historical backfill.

Use
---
Import this module inside your existing trade-resolution loop in
strategy_historical_backfill.py (or whichever file resolves active trades).

Config
------
- BE_MODE = False  -> baseline behavior
- BE_MODE = True   -> enable breakeven logic
- BE_TRIGGER_R = 1.0 or 1.5
- BE_FEE_BUFFER_PCT = 0.02% by default, per your spec
- BE_TOUCH_MODE = "touch"

Why separate module
-------------------
This keeps the profitable strategy_engine baseline untouched and lets you
test trade management independently from setup selection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


BE_MODE = True
BE_TRIGGER_R = 1.5
BE_FEE_BUFFER_PCT = 0.0002   # 0.02%
BE_TOUCH_MODE = "touch"      # reserved for compatibility


@dataclass
class BEState:
    be_armed: bool = False
    be_price: Optional[float] = None
    be_trigger_price: Optional[float] = None
    be_trigger_r: float = 0.0


def _risk(entry: float, stop: float) -> float:
    return abs(float(entry) - float(stop))


def _breakeven_price(entry: float, side: str, fee_buffer_pct: float = BE_FEE_BUFFER_PCT) -> float:
    entry = float(entry)
    if side.upper() == "LONG":
        return entry * (1.0 + fee_buffer_pct)
    return entry * (1.0 - fee_buffer_pct)


def _trigger_price(entry: float, stop: float, side: str, trigger_r: float) -> float:
    r = _risk(entry, stop)
    if side.upper() == "LONG":
        return float(entry) + trigger_r * r
    return float(entry) - trigger_r * r


def arm_breakeven_if_needed(
    *,
    state: BEState,
    side: str,
    entry: float,
    stop: float,
    bar_high: float,
    bar_low: float,
    trigger_r: float = BE_TRIGGER_R,
    enabled: bool = BE_MODE,
) -> BEState:
    """
    Arms BE once price has progressed trigger_r * R in favor.
    This does not itself resolve the trade; it only updates state.
    """
    if not enabled or state.be_armed:
        return state

    trig = _trigger_price(entry, stop, side, trigger_r)

    if side.upper() == "LONG":
        progressed = float(bar_high) >= trig
    else:
        progressed = float(bar_low) <= trig

    if progressed:
        state.be_armed = True
        state.be_trigger_price = trig
        state.be_trigger_r = float(trigger_r)
        state.be_price = _breakeven_price(entry, side)
    return state


def check_be_stop_hit(
    *,
    state: BEState,
    side: str,
    bar_high: float,
    bar_low: float,
) -> bool:
    """
    Returns True if BE is armed and touched on this bar.
    """
    if not state.be_armed or state.be_price is None:
        return False

    if side.upper() == "LONG":
        return float(bar_low) <= float(state.be_price)
    return float(bar_high) >= float(state.be_price)


def realized_r_for_tp1_then_be(
    *,
    entry: float,
    stop: float,
    be_price: float,
    partial_size: float = 0.5,
) -> float:
    """
    Default assumption:
    - 50% off at TP1 (+2R)
    - remaining 50% exits at BE+fees
    """
    r = _risk(entry, stop)
    if r <= 0:
        return 0.0

    be_r = abs(float(be_price) - float(entry)) / r
    # directional sign
    if be_price < entry:
        be_r = -be_r

    return partial_size * 2.0 + (1.0 - partial_size) * be_r


def resolution_note_for_be(trigger_r: float) -> str:
    return f"tp1_partial_then_be_after_{trigger_r:.2f}R"


# -------------------------
# Suggested integration API
# -------------------------

def process_bar_for_long_trade(
    *,
    state: BEState,
    entry: float,
    stop: float,
    tp1: float,
    tp2: float,
    bar_high: float,
    bar_low: float,
    trigger_r: float = BE_TRIGGER_R,
    enabled: bool = BE_MODE,
):
    """
    Order of evaluation for LONG:
    1) check TP2
    2) arm BE if enough favorable progress
    3) check BE stop if armed
    4) check original stop

    Returns a dict with:
    - event: None | 'TP2' | 'BE_STOP' | 'STOP'
    - state: updated BEState
    """
    # TP2 first
    if float(bar_high) >= float(tp2):
        return {"event": "TP2", "state": state}

    state = arm_breakeven_if_needed(
        state=state,
        side="LONG",
        entry=entry,
        stop=stop,
        bar_high=bar_high,
        bar_low=bar_low,
        trigger_r=trigger_r,
        enabled=enabled,
    )

    if check_be_stop_hit(state=state, side="LONG", bar_high=bar_high, bar_low=bar_low):
        return {"event": "BE_STOP", "state": state}

    if float(bar_low) <= float(stop):
        return {"event": "STOP", "state": state}

    return {"event": None, "state": state}


def process_bar_for_short_trade(
    *,
    state: BEState,
    entry: float,
    stop: float,
    tp1: float,
    tp2: float,
    bar_high: float,
    bar_low: float,
    trigger_r: float = BE_TRIGGER_R,
    enabled: bool = BE_MODE,
):
    """
    Order of evaluation for SHORT:
    1) check TP2
    2) arm BE if enough favorable progress
    3) check BE stop if armed
    4) check original stop
    """
    if float(bar_low) <= float(tp2):
        return {"event": "TP2", "state": state}

    state = arm_breakeven_if_needed(
        state=state,
        side="SHORT",
        entry=entry,
        stop=stop,
        bar_high=bar_high,
        bar_low=bar_low,
        trigger_r=trigger_r,
        enabled=enabled,
    )

    if check_be_stop_hit(state=state, side="SHORT", bar_high=bar_high, bar_low=bar_low):
        return {"event": "BE_STOP", "state": state}

    if float(bar_high) >= float(stop):
        return {"event": "STOP", "state": state}

    return {"event": None, "state": state}
