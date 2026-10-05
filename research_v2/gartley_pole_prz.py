"""Offline, causal restricted Gartley PRZ annotations for Du Plessis pole events.

Research convention, not a trading strategy or a complete Carney implementation.
Only completed PnF column endpoints and closed candle prices are consumed.
No operational database, network, order, fill, or position dependency exists.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

SCHEMA = "restricted-gartley-pole-prz-v1"
ZERO = Decimal(0)


def number(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("invalid exact number")
    if isinstance(value, str) and (not value or len(value) > 80 or "\x00" in value):
        raise ValueError("invalid exact number")
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError("invalid exact number") from None
    if not result.is_finite() or abs(result.as_tuple().exponent) > 24:
        raise ValueError("invalid exact number")
    return result


def distance(price: Decimal, lower: Decimal, upper: Decimal) -> Decimal:
    return max(lower - price, ZERO, price - upper)


@dataclass(frozen=True)
class Pivot:
    pivot_id: str
    column_id: int
    kind: str
    price: Decimal
    extreme_at: int
    confirmed_at: int
    confirmation_sequence: int


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    direction: str
    pivot_ids: tuple[str, str, str, str]
    connected_column_id: int
    zone_known_at: int
    known_sequence: int
    x: Decimal
    b: Decimal
    r_b: Decimal
    r_c: Decimal
    k_bc: Decimal
    d_xa: Decimal
    d_abcd: Decimal
    d_bc: Decimal
    lower: Decimal
    upper: Decimal
    width: Decimal


def project(pivots: Sequence[Pivot], *, connected_column_id: int,
            box_size: Decimal = Decimal(100)) -> tuple[Candidate | None, str]:
    """Consecutive X/A/B/C only; returns a bounded reason for rejection."""
    if len(pivots) != 4 or box_size <= 0 or not box_size.is_finite():
        raise ValueError("four pivots and positive box required")
    x, a, b, c = pivots
    if (len({p.pivot_id for p in pivots}) != 4
            or any(p.column_id + 1 != q.column_id for p, q in zip(pivots, pivots[1:]))
            or any(p.confirmed_at > q.confirmed_at for p, q in zip(pivots, pivots[1:]))
            or c.column_id + 1 != connected_column_id):
        return None, "NONCONSECUTIVE_OR_UNCONFIRMED"
    kinds = tuple(p.kind for p in pivots)
    if kinds not in (("LOW", "HIGH", "LOW", "HIGH"), ("HIGH", "LOW", "HIGH", "LOW")):
        return None, "PIVOT_DIRECTION"
    bullish = kinds[0] == "LOW"
    if not ((x.price < b.price < a.price and b.price < c.price < a.price) if bullish
            else (x.price > b.price > a.price and b.price > c.price > a.price)):
        return None, "PIVOT_GEOMETRY"
    xa, ab, bc = abs(a.price - x.price), abs(b.price - a.price), abs(c.price - b.price)
    if min(xa, ab, bc) <= 0:
        return None, "ZERO_LEG"
    rb, rc = ab / xa, bc / ab
    if not Decimal("0.588") <= rb <= Decimal("0.648"):
        return None, "B_RATIO"
    if not Decimal("0.382") <= rc <= Decimal("0.886"):
        return None, "C_RATIO"
    kbc = 1 / rc
    if not Decimal("1.13") <= kbc <= Decimal("1.618"):
        return None, "BC_PROJECTION"
    dxa = a.price + Decimal("0.786") * (x.price - a.price)
    dabcd = c.price + (b.price - a.price)
    if (bullish and dabcd < dxa) or (not bullish and dabcd > dxa):
        return None, "EXCLUDED_VARIANT"
    low, high = sorted((dxa, dabcd))
    if high - low > box_size:
        return None, "ZONE_WIDTH"
    return Candidate(f"{SCHEMA}:{x.pivot_id}:{a.pivot_id}:{b.pivot_id}:{c.pivot_id}",
                     "LONG" if bullish else "SHORT", tuple(p.pivot_id for p in pivots),
                     connected_column_id, c.confirmed_at, c.confirmation_sequence,
                     x.price, b.price, rb, rc, kbc, dxa, dabcd, dabcd,
                     low, high, high-low), "CANDIDATE"


def _record(value: Any) -> dict:
    """JSON-safe, exact Decimal serialization; no float conversion."""
    if hasattr(value, "__dataclass_fields__"):
        value = asdict(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _record(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_record(item) for item in value]
    return value


class CausalPrzLedger:
    """Feed one close-confirmed PnF state per strictly increasing candle timestamp.

    The input engine's active final column is excluded from confirmed pivots.
    Events are append-only; duplicate identical updates return no new records.
    """
    def __init__(self, *, box_size: str | Decimal = "100") -> None:
        self.box_size = number(box_size)
        if self.box_size <= 0:
            raise ValueError("positive box required")
        self.pivots: list[Pivot] = []
        self.candidates: dict[str, Candidate] = {}
        self.states: dict[str, dict] = {}
        self.records: list[dict] = []
        self.closes: list[tuple[int, Decimal]] = []
        self.last_ts = -1
        self.last_fingerprint: tuple | None = None
        self.sequence = 0
        self.confirmed_fingerprints: dict[int, tuple] = {}
        self.seen_pole_events: set[str] = set()

    def ingest(self, columns: Sequence[Any], *, close_ts: int, close: Any,
               pole_events: Sequence[Any] = ()) -> list[dict]:
        if type(close_ts) is not int or close_ts < 0 or not columns:
            raise ValueError("invalid close-confirmed event")
        price = number(close)
        if price <= 0:
            raise ValueError("positive close required")
        fingerprint = (close_ts, price, tuple((c.idx, c.kind, str(number(c.top)),
                       str(number(c.bottom)), c.start_ts, c.end_ts) for c in columns),
                       tuple((e.event_id, e.decision_ts) for e in pole_events))
        if close_ts == self.last_ts and fingerprint == self.last_fingerprint:
            return []
        if close_ts <= self.last_ts:
            raise ValueError("nonmonotonic or conflicting close update")
        if any(type(c.idx) is not int or c.idx != n or c.kind not in ("X", "O")
               or type(c.end_ts) is not int or c.end_ts > close_ts
               for n, c in enumerate(columns)):
            raise ValueError("invalid PnF sequence")
        if ((not self.closes and len(columns) != 1)
                or len(columns)-1 > len(self.pivots)+1
                or len(columns)-1 < len(self.pivots)):
            raise ValueError("missing or reordered PnF event")
        for event in pole_events:
            if (type(event.decision_ts) is not int or event.decision_ts != close_ts
                    or event.event_id in self.seen_pole_events):
                raise ValueError("duplicate or mistimed pole decision")
        for idx, original in self.confirmed_fingerprints.items():
            if idx >= len(columns) or original != fingerprint[2][idx]:
                raise ValueError("confirmed pivot changed retrospectively")
        self.sequence += 1
        self.last_ts, self.last_fingerprint = close_ts, fingerprint
        fresh: list[dict] = []
        # A column is confirmed only after the immediately following column exists.
        while len(self.pivots) < len(columns)-1:
            col = columns[len(self.pivots)]
            pivot = Pivot(f"pnf:{col.idx}:{col.kind}", col.idx,
                          "HIGH" if col.kind == "X" else "LOW",
                          number(col.top if col.kind == "X" else col.bottom),
                          col.end_ts, close_ts, self.sequence)
            self.pivots.append(pivot)
            self.confirmed_fingerprints[col.idx] = fingerprint[2][col.idx]
            fresh.append({"type": "PIVOT_CONFIRMED", **_record(pivot)})
            if len(self.pivots) >= 4:
                four = self.pivots[-4:]
                candidate, reason = project(four, connected_column_id=col.idx+1,
                                            box_size=self.box_size)
                if candidate is not None:
                    if candidate.candidate_id in self.candidates:
                        raise ValueError("duplicate candidate")
                    self.candidates[candidate.candidate_id] = candidate
                    # At creation, the first close must still be on the approach side.
                    approach = (lambda p: p > candidate.upper) if candidate.direction == "LONG" else (lambda p: p < candidate.lower)
                    side_ok = approach(price) and all(approach(old_price) for old_ts, old_price in self.closes
                                                       if old_ts >= four[0].extreme_at)
                    state = {"phase": "WAITING" if side_ok else "UNAVAILABLE",
                             "reason": None if side_ok else "ZONE_TOUCHED_BEFORE_OR_AT_CREATION",
                             "first_zone_contact_at": None, "b_violated_at": None,
                             "full_zone_test_at": None,
                             "accepted_event_id": None}
                    self.states[candidate.candidate_id] = state
                    fresh.append({"type": "ZONE_CREATED", **_record(candidate),
                                  "state": state["phase"], "reason": state["reason"]})
                else:
                    fresh.append({"type": "PATTERN_EXCLUDED", "pivot_ids": [p.pivot_id for p in four],
                                  "reason": reason, "at": close_ts})
        self.closes.append((close_ts, price))
        for candidate in list(self.candidates.values()):
            state = self.states[candidate.candidate_id]
            if state["phase"] in ("UNAVAILABLE", "EXPIRED", "INVALIDATED", "MATCHED"):
                continue
            connected = candidate.connected_column_id
            if close_ts <= candidate.zone_known_at:
                continue
            invalid = price <= candidate.x if candidate.direction == "LONG" else price >= candidate.x
            if invalid:
                state.update(phase="INVALIDATED", reason="X_CROSSED_BEFORE_SIGNAL")
                fresh.append({"type": "ZONE_INVALIDATED", "candidate_id": candidate.candidate_id,
                              "at": close_ts, "reason": state["reason"]})
                continue
            contact = price <= candidate.upper if candidate.direction == "LONG" else price >= candidate.lower
            full = price <= candidate.lower if candidate.direction == "LONG" else price >= candidate.upper
            violated = price < candidate.b if candidate.direction == "LONG" else price > candidate.b
            if violated and state["b_violated_at"] is None:
                state["b_violated_at"] = close_ts
                fresh.append({"type": "B_VIOLATION", "candidate_id": candidate.candidate_id,
                              "at": close_ts})
            if contact and state["first_zone_contact_at"] is None:
                state["first_zone_contact_at"] = close_ts
                fresh.append({"type": "ZONE_CONTACT", "candidate_id": candidate.candidate_id, "at": close_ts})
            if full and state["full_zone_test_at"] is None:
                state["full_zone_test_at"] = close_ts
                state["phase"] = "TESTED"
                fresh.append({"type": "ZONE_FULL_TEST", "candidate_id": candidate.candidate_id,
                              "at": close_ts})
        for event in pole_events:
            self.seen_pole_events.add(event.event_id)
            matches = [c for c in self.candidates.values()
                       if c.connected_column_id == event.pole_column_id
                       and c.direction == event.action
                       and getattr(event, "pole_type", "LOW_POLE" if c.direction == "LONG" else "HIGH_POLE")
                       == ("LOW_POLE" if c.direction == "LONG" else "HIGH_POLE")
                       and getattr(event, "mode", "EARLY_ENTRY") == "EARLY_ENTRY"
                       and getattr(event, "status", "CANDIDATE") == "CANDIDATE"]
            matches.sort(key=lambda c: c.candidate_id)
            chosen = next((c for c in matches if self.states[c.candidate_id]["phase"] == "TESTED"
                           and self.states[c.candidate_id]["full_zone_test_at"] < close_ts
                           and self.states[c.candidate_id]["b_violated_at"] is not None
                           and self.states[c.candidate_id]["b_violated_at"] < close_ts), None)
            category = ("PRZ_MATCH" if chosen else "PRZ_AVAILABLE_NONMATCH" if matches
                        else "PRZ_UNAVAILABLE")
            if chosen:
                reason = "PRIOR_FULL_TEST"
            elif any(self.states[c.candidate_id]["full_zone_test_at"] == close_ts for c in matches):
                reason = "SAME_TIMESTAMP_FULL_TEST"
            elif matches:
                reason = self.states[matches[0].candidate_id]["reason"] or "NO_PRIOR_FULL_TEST"
            else:
                reason = "NO_CAUSAL_ZONE"
            if not chosen and matches and reason == "NO_PRIOR_FULL_TEST" and any(
                    self.states[c.candidate_id]["full_zone_test_at"] is not None
                    and self.states[c.candidate_id]["b_violated_at"] is None for c in matches):
                reason = "B_NOT_VIOLATED"
            extreme = (number(columns[chosen.connected_column_id].bottom) if chosen.direction == "LONG"
                       else number(columns[chosen.connected_column_id].top)) if chosen else None
            annotation = {"type": "POLE_ANNOTATION", "event_id": event.event_id,
                          "at": close_ts, "category": category, "reason": reason,
                          "candidate_id": chosen.candidate_id if chosen else None,
                          "signal_close_distance": str(distance(price, chosen.lower, chosen.upper)) if chosen else None,
                          "signal_close_distance_boxes": str(distance(price, chosen.lower, chosen.upper)/self.box_size) if chosen else None,
                          "known_pole_extreme_distance": str(distance(extreme, chosen.lower, chosen.upper)) if chosen else None,
                          "known_pole_extreme_distance_boxes": str(distance(extreme, chosen.lower, chosen.upper)/self.box_size) if chosen else None}
            if chosen:
                self.states[chosen.candidate_id].update(phase="MATCHED", accepted_event_id=event.event_id)
            fresh.append(annotation)
        # An event on the close that starts the next column still belongs to
        # the just-completed connected leg. Retire only after evaluating it.
        for candidate in self.candidates.values():
            state = self.states[candidate.candidate_id]
            if len(columns)-1 > candidate.connected_column_id and state["phase"] in ("WAITING", "TESTED"):
                state.update(phase="EXPIRED", reason="CONNECTED_LEG_ENDED")
                fresh.append({"type": "ZONE_EXPIRED", "candidate_id": candidate.candidate_id,
                              "at": close_ts, "reason": state["reason"]})
        self.records.extend(fresh)
        return fresh
