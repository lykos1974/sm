"""Default-OFF research annotation of PRZ test, Gartley X break, then pole.

An X break invalidates the standard Gartley. This version labels only a
historical context for a later pole on the same PnF leg. It neither changes
the v1 Gartley labels nor creates an entry, fill, exit, or order.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

PROFILE = "prz_x_break_pole_context_v2"


@dataclass(frozen=True)
class ContextAnnotation:
    schema: str
    event_id: str
    candidate_id: str | None
    direction: str
    category: str
    reason: str
    connected_column_id: int | None
    zone_known_at: int | None
    b_violated_at: int | None
    full_zone_test_at: int | None
    x_cross_at: int | None
    pole_signal_at: int


class XBreakPoleContext:
    """Consume immutable v1 research events in close-time order."""
    def __init__(self, *, enabled: bool = False) -> None:
        self.enabled = enabled
        self.candidates: dict[str, dict] = {}
        self.seen: set[str] = set()
        self.last_at = -1
        self.last_input: tuple | None = None
        self.annotations: list[ContextAnnotation] = []

    def ingest(self, facts: Sequence[dict], *, pole_events: Sequence[Any], at: int) -> list[ContextAnnotation]:
        if not self.enabled:
            return []
        if type(at) is not int or at < 0:
            raise ValueError("invalid close timestamp")
        fingerprint = (at, repr(facts), tuple((e.event_id, e.action, e.pole_column_id,
                       e.decision_ts, e.status) for e in pole_events))
        if at == self.last_at and fingerprint == self.last_input:
            return []
        if at <= self.last_at:
            raise ValueError("nonmonotonic or conflicting context update")
        for event in pole_events:
            if (type(event.decision_ts) is not int or event.decision_ts != at
                    or event.event_id in self.seen
                    or event.action not in ("LONG", "SHORT")
                    or getattr(event, "mode", "EARLY_ENTRY") != "EARLY_ENTRY"
                    or event.status != "CANDIDATE"
                    or getattr(event, "pole_type", "LOW_POLE" if event.action == "LONG" else "HIGH_POLE")
                    != ("LOW_POLE" if event.action == "LONG" else "HIGH_POLE")):
                raise ValueError("duplicate or mistimed pole")
        staging = {key: dict(value) for key, value in self.candidates.items()}
        for fact in facts:
            kind = fact["type"]
            if kind == "ZONE_CREATED":
                key = fact["candidate_id"]
                if (key in staging or type(fact["zone_known_at"]) is not int
                        or fact["zone_known_at"] != at or fact["direction"] not in ("LONG", "SHORT")
                        or type(fact["connected_column_id"]) is not int
                        or fact["state"] not in ("WAITING", "UNAVAILABLE")):
                    raise ValueError("duplicate or malformed zone")
                staging[key] = {
                    "direction": fact["direction"], "connected_column_id": fact["connected_column_id"],
                    "zone_known_at": fact["zone_known_at"], "available": fact["state"] == "WAITING",
                    "b_violated_at": None, "full_zone_test_at": None, "x_cross_at": None,
                    "accepted_event_id": None, "expired_at": None,
                }
            elif kind in ("B_VIOLATION", "ZONE_FULL_TEST", "ZONE_INVALIDATED"):
                key = fact["candidate_id"]
                if key not in staging or type(fact["at"]) is not int or fact["at"] != at:
                    raise ValueError("orphaned or mistimed zone fact")
                slot = ("b_violated_at" if kind == "B_VIOLATION" else
                        "full_zone_test_at" if kind == "ZONE_FULL_TEST" else "x_cross_at")
                if kind == "ZONE_INVALIDATED" and fact["reason"] != "X_CROSSED_BEFORE_SIGNAL":
                    continue
                if staging[key][slot] is not None:
                    raise ValueError("duplicate zone fact")
                staging[key][slot] = at
            elif kind == "ZONE_EXPIRED":
                key = fact["candidate_id"]
                if key not in staging or type(fact["at"]) is not int or fact["at"] != at:
                    raise ValueError("orphaned expiry")
                if staging[key]["expired_at"] is not None:
                    raise ValueError("duplicate expiry")
                staging[key]["expired_at"] = at
            elif kind not in ("PIVOT_CONFIRMED", "PATTERN_EXCLUDED", "ZONE_CONTACT",
                              "POLE_ANNOTATION"):
                raise ValueError("unknown zone fact")
        emitted = []
        for event in pole_events:
            candidates = sorted(((key, state) for key, state in staging.items()
                                 if state["connected_column_id"] == event.pole_column_id
                                 and state["direction"] == event.action), key=lambda item: item[0])
            accepted = next(((key, state) for key, state in candidates
                             if state["available"] and state["accepted_event_id"] is None
                             and (state["expired_at"] is None or state["expired_at"] == at)
                             and state["b_violated_at"] is not None
                             and state["full_zone_test_at"] is not None
                             and state["x_cross_at"] is not None
                             and state["zone_known_at"] < state["b_violated_at"]
                             and state["b_violated_at"] <= state["full_zone_test_at"]
                             and state["full_zone_test_at"] < state["x_cross_at"] < at), None)
            chosen = accepted or (candidates[0] if candidates else None)
            key, state = chosen if chosen else (None, None)
            if accepted:
                state["accepted_event_id"] = event.event_id
                category, reason = "CONTEXT_MATCH", "FULL_TEST_THEN_X_BREAK_THEN_POLE"
            elif not candidates:
                category, reason = "CONTEXT_UNAVAILABLE", "NO_CAUSAL_ZONE"
            elif not state["available"]:
                category, reason = "CONTEXT_UNAVAILABLE", "ZONE_UNAVAILABLE_AT_CREATION"
            elif state["expired_at"] is not None and state["expired_at"] < at:
                category, reason = "CONTEXT_AVAILABLE_NONMATCH", "CONNECTED_LEG_EXPIRED"
            elif state["full_zone_test_at"] is None:
                category, reason = "CONTEXT_AVAILABLE_NONMATCH", "NO_PRIOR_FULL_TEST"
            elif state["x_cross_at"] is None:
                category, reason = "CONTEXT_AVAILABLE_NONMATCH", "NO_PRIOR_X_BREAK"
            else:
                category, reason = "CONTEXT_AVAILABLE_NONMATCH", "ORDER_OR_OWNERSHIP_MISMATCH"
            item = ContextAnnotation(PROFILE, event.event_id, key, event.action,
                                     category, reason, state["connected_column_id"] if state else None,
                                     state["zone_known_at"] if state else None,
                                     state["b_violated_at"] if state else None,
                                     state["full_zone_test_at"] if state else None,
                                     state["x_cross_at"] if state else None, at)
            emitted.append(item)
        self.candidates = staging
        self.last_at, self.last_input = at, fingerprint
        self.seen.update(e.event_id for e in pole_events)
        self.annotations.extend(emitted)
        return emitted
