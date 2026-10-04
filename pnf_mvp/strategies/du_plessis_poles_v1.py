"""Default-off, decision-only Du Plessis pole profile. No execution imports.

Input is a close-confirmed P&F prefix, including its active final column.
Events are immutable; fill and position ownership belong to a separate layer.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Sequence

STRATEGY_ID = "du_plessis_poles_v1"
MODES = frozenset({"EARLY_ENTRY", "EXIT_ONLY"})


@dataclass(frozen=True)
class PoleEvent:
    event_id: str
    pole_type: str
    mode: str
    action: str
    pole_column_id: int
    retracement_column_id: int
    breakout_excess_boxes: int
    column_length_boxes: int
    retracement_boxes: int
    threshold_policy: str
    theoretical_trigger_level: str
    decision_ts: int
    status: str
    reason: str
    structural_label: bool
    context: str = "NONE"


def _integer(value: Any) -> int:
    if type(value) is not int:
        raise ValueError("invalid integer box coordinate")
    return value


def _grid(columns: Sequence[Any], box_size: Decimal) -> list[tuple[int, int]]:
    if not box_size.is_finite() or box_size <= 0:
        raise ValueError("constant positive box size required")
    out = []
    for col in columns:
        coords = []
        for edge in (col.bottom, col.top):
            d = Decimal(str(edge)) / box_size
            if not d.is_finite() or d != d.to_integral_value():
                raise ValueError("column off constant box grid; percentage/log grid needs explicit box indices")
            coords.append(_integer(int(d)))
        bottom, top = coords
        if bottom > top or col.kind not in ("X", "O"):
            raise ValueError("invalid column")
        out.append((bottom, top))
    return out


def _consolidated(grid: list[tuple[int, int]], pole_index: int) -> bool:
    """v1 assumption: previous three adjacent columns share >=1 grid level."""
    prior = grid[pole_index - 3:pole_index]
    return len(prior) == 3 and max(low for low, _ in prior) <= min(high for _, high in prior)


def evaluate_prefix(columns: Sequence[Any], *, box_size: str | Decimal,
                    reversal_boxes: int, mode: str = "EARLY_ENTRY",
                    enabled: bool = False, venue: str = "FUTURES",
                    owned_position: str | None = None) -> tuple[PoleEvent, ...]:
    """One decision per pole/mode. Replay a prefix and dedupe by event_id.

    owned_position is a strategy-owned LONG/SHORT, never another strategy's trade.
    This function emits candidates only and cannot create fills or orders.
    """
    if not enabled:
        return ()
    if mode not in MODES or venue not in ("SPOT", "FUTURES") or type(reversal_boxes) is not int or reversal_boxes < 1:
        raise ValueError("unsupported mode or reversal setting")
    if owned_position not in (None, "LONG", "SHORT"):
        raise ValueError("invalid position scope")
    box = Decimal(str(box_size))
    grid = _grid(columns, box)
    events: list[PoleEvent] = []
    for i in range(4, len(columns)):
        pole, retrace = columns[i - 1], columns[i]
        if (type(pole.idx) is not int or type(retrace.idx) is not int
                or retrace.idx != pole.idx + 1 or not _consolidated(grid, i - 1)):
            continue
        if (pole.kind, retrace.kind) not in (("X", "O"), ("O", "X")):
            continue
        prior_range = grid[i - 4:i - 1]
        low, high = grid[i - 1]
        rlow, rhigh = grid[i]
        length = high - low + 1
        retraced = rhigh - rlow + 1
        if (retraced < reversal_boxes or
                (pole.kind == "X" and rhigh != high - 1) or
                (pole.kind == "O" and rlow != low + 1) or
                type(retrace.end_ts) is not int or retrace.end_ts < pole.end_ts):
            continue
        excess = (high - max(top for _, top in prior_range) if pole.kind == "X"
                  else min(bottom for bottom, _ in prior_range) - low)
        if excess < 3 or 2 * retraced < length:
            continue
        kind = "HIGH_POLE" if pole.kind == "X" else "LOW_POLE"
        direction = "SHORT" if pole.kind == "X" else "LONG"
        action = ("EXIT_LONG" if pole.kind == "X" else "EXIT_SHORT") if mode == "EXIT_ONLY" else direction
        # Spot can display the bearish fact or close its own LONG; it cannot open SHORT.
        status = ("BLOCKED_SPOT_SHORT" if venue == "SPOT" and action in ("SHORT", "EXIT_SHORT")
                  else "POSITION_NOT_OWNED" if mode == "EXIT_ONLY" and owned_position != ("LONG" if pole.kind == "X" else "SHORT")
                  else "CANDIDATE")
        # Threshold is the first box that satisfies 2 * retraced >= length.
        threshold_count = (length + 1) // 2
        trigger_index = high - threshold_count if pole.kind == "X" else low + threshold_count
        trigger = str(box * trigger_index)
        event_id = f"{STRATEGY_ID}:{mode}:{pole.idx}:{retrace.idx}"
        context = "NONE"
        if events and events[-1].pole_type != kind and events[-1].retracement_column_id == pole.idx:
            context = "OPPOSING_POLE_SEQUENCE"
        events.append(PoleEvent(event_id, kind, mode, action, pole.idx, retrace.idx,
                                excess, length, retraced, ">=50%_BOX_COUNT", trigger,
                                _integer(retrace.end_ts), status,
                                "EARLY_RETRACEMENT" if status == "CANDIDATE" else status,
                                2 * retraced > length, context))
    return tuple(events)


class PoleDecisionLedger:
    """Persistent-by-replay event identity; callers may serialize events externally.

    A fill is never inferred. A caller may provide an explicitly simulated fill;
    absent that simulation there is no position and no reversal exit.
    """
    def __init__(self, *, mode: str = "EARLY_ENTRY", venue: str = "FUTURES",
                 prior_events: Sequence[PoleEvent] = ()):
        if mode not in MODES:
            raise ValueError("unsupported mode")
        self.mode, self.venue = mode, venue
        if any(e.mode != mode or not e.event_id.startswith(STRATEGY_ID + ":")
               for e in prior_events) or len({e.event_id for e in prior_events}) != len(prior_events):
            raise ValueError("invalid prior event lineage")
        self.events: dict[str, PoleEvent] = {e.event_id: e for e in prior_events}
        self.filled: dict[str, tuple[str, int]] = {}

    def ingest(self, columns: Sequence[Any], *, box_size: str | Decimal,
               reversal_boxes: int, enabled: bool = False,
               owned_position: str | None = None) -> tuple[PoleEvent, ...]:
        if not enabled:
            return ()
        fresh = []
        for candidate in evaluate_prefix(columns, box_size=box_size,
                                         reversal_boxes=reversal_boxes, mode=self.mode,
                                         enabled=True, venue=self.venue,
                                         owned_position=owned_position):
            if candidate.event_id not in self.events:
                self.events[candidate.event_id] = candidate
                fresh.append(candidate)
        if columns:
            active = columns[-1]
            for event_id, (direction, fill_ts) in tuple(self.filled.items()):
                original = self.events[event_id]
                target = "X" if direction == "SHORT" else "O"
                exit_id = event_id + ":REVERSAL_EXIT"
                if (active.idx > original.retracement_column_id
                        and active.kind == target and active.end_ts > fill_ts
                        and exit_id not in self.events):
                    exit_event = PoleEvent(exit_id, original.pole_type, self.mode,
                                           "CLOSE_" + direction, original.pole_column_id,
                                           active.idx, original.breakout_excess_boxes,
                                           original.column_length_boxes,
                                           original.retracement_boxes,
                                           original.threshold_policy, "", active.end_ts,
                                           "EXIT_CANDIDATE", "NEXT_VALID_REVERSAL",
                                           original.structural_label)
                    self.events[exit_id] = exit_event
                    fresh.append(exit_event)
        return tuple(fresh)

    def acknowledge_simulated_fill(self, event_id: str, *, direction: str,
                         exchange_fill_ts: int, evidence_id: str) -> None:
        event = self.events.get(event_id)
        if (event is None or event.status != "CANDIDATE"
                or event.mode != "EARLY_ENTRY" or event.action != direction
                or direction not in ("LONG", "SHORT") or type(exchange_fill_ts) is not int
                or exchange_fill_ts <= event.decision_ts or not evidence_id):
            raise ValueError("later simulated fill reference required")
        value = (direction, exchange_fill_ts)
        if event_id in self.filled and self.filled[event_id] != value:
            raise ValueError("conflicting fill")
        self.filled[event_id] = value
