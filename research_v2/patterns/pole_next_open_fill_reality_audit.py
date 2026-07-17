"""Research-only fill reality model for NEXT_COLUMN_OPEN_ENTRY.

The order is placed only after the signal is observable. A trade exists only if a
later candle trades through the requested entry price before the order expires.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle


@dataclass(frozen=True)
class FillRealityResult:
    status: str
    fill_ts: int | None
    exit_ts: int | None
    candles_waited: int
    result_r: float | None
    details: str


def evaluate_pending_entry(
    *,
    candles: Iterable[Candle],
    placement_ts: int,
    direction: str,
    entry: float,
    stop: float,
    target: float,
    expiry_candles: int,
) -> FillRealityResult:
    """Evaluate a pending limit order and, only after fill, its trade outcome.

    Candles at or before ``placement_ts`` are excluded. The order is cancelled
    after ``expiry_candles`` later candles if none trades through ``entry``.
    """
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")
    if expiry_candles <= 0:
        raise ValueError("expiry_candles must be positive")

    replay = [candle for candle in candles if candle.ts > placement_ts]
    pending_window = replay[:expiry_candles]

    fill_index: int | None = None
    for index, candle in enumerate(pending_window):
        if candle.low <= entry <= candle.high:
            fill_index = index
            break

    if fill_index is None:
        return FillRealityResult(
            status="CANCELLED_NOT_FILLED",
            fill_ts=None,
            exit_ts=None,
            candles_waited=len(pending_window),
            result_r=None,
            details=f"entry not traded within {expiry_candles} post-placement candles",
        )

    fill_candle = pending_window[fill_index]
    trade_replay = replay[fill_index:]
    for candle in trade_replay:
        hit_target = candle.high >= target if direction == "LONG" else candle.low <= target
        hit_stop = candle.low <= stop if direction == "LONG" else candle.high >= stop
        if hit_target and hit_stop:
            return FillRealityResult(
                status="FILLED_SAME_CANDLE_AMBIGUOUS",
                fill_ts=fill_candle.ts,
                exit_ts=candle.ts,
                candles_waited=fill_index + 1,
                result_r=None,
                details="target and stop are both inside one OHLC candle after fill",
            )
        if hit_target:
            risk = abs(entry - stop)
            result_r = abs(target - entry) / risk if risk else None
            return FillRealityResult(
                status="FILLED_TARGET_FIRST",
                fill_ts=fill_candle.ts,
                exit_ts=candle.ts,
                candles_waited=fill_index + 1,
                result_r=result_r,
                details="entry filled before expiry and target reached first",
            )
        if hit_stop:
            return FillRealityResult(
                status="FILLED_STOP_FIRST",
                fill_ts=fill_candle.ts,
                exit_ts=candle.ts,
                candles_waited=fill_index + 1,
                result_r=-1.0,
                details="entry filled before expiry and stop reached first",
            )

    return FillRealityResult(
        status="FILLED_NOT_RESOLVED",
        fill_ts=fill_candle.ts,
        exit_ts=None,
        candles_waited=fill_index + 1,
        result_r=None,
        details="entry filled but neither target nor stop was reached",
    )
