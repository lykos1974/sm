"""Versioned offline multi-column P&F support/resistance research facts.

These are two-test structural zones, never harmonic PRZ or trade signals.
The mechanical parameters were fixed before looking at trade outcomes:
3-box maximum level distance, 6-box intervening excursion, 80-column lookback.
"""
from __future__ import annotations

from decimal import Decimal

from research_v2.gartley_pole_prz import number

SCHEMA = "pnf-multicolumn-sr-v1"
BOX = Decimal(100)
MAX_DISTANCE = 3 * BOX
MIN_EXCURSION = 6 * BOX
LOOKBACK_COLUMNS = 80


def derive_zones(facts: list[dict]) -> list[dict]:
    """Use confirmed pivots in append order; report the second confirmation time.

    A prior level and an intervening swing must be known when the second
    pivot is confirmed. One most-recent qualifying pair per confirmation.
    Future pivots never alter already emitted zones.
    """
    if not isinstance(facts, list):
        raise ValueError("pivot facts required")
    pivots = []
    zones = []
    for fact in facts:
        if not isinstance(fact, dict) or fact.get("type") != "PIVOT_CONFIRMED":
            continue
        idx, at, extreme, seq = (fact.get(k) for k in
                                 ("column_id", "confirmed_at", "extreme_at", "confirmation_sequence"))
        kind, pid = fact.get("kind"), fact.get("pivot_id")
        if (type(idx) is not int or type(at) is not int or type(extreme) is not int
                or type(seq) is not int or idx < 0 or extreme < 0 or at < extreme
                or kind not in ("HIGH", "LOW")
                or pid != f"pnf:{idx}:{'X' if kind == 'HIGH' else 'O'}"
                or (pivots and (idx != pivots[-1]["idx"]+1
                                or at <= pivots[-1]["at"]
                                or seq <= pivots[-1]["sequence"]
                                or kind == pivots[-1]["kind"]))):
            raise ValueError("invalid confirmed pivot sequence")
        price = number(fact.get("price"))
        if price <= 0 or price % BOX:
            raise ValueError("off-grid pivot")
        current = {"idx": idx, "at": at, "sequence": seq, "kind": kind,
                   "price": price, "id": pid}
        # The confirmation of this pivot precedes every future pivot in
        # this scan. Exclude pairs without an intervening opposite pivot.
        for older in reversed(pivots):
            if idx-older["idx"] > LOOKBACK_COLUMNS:
                break
            if older["kind"] != kind or idx-older["idx"] < 2:
                continue
            if abs(older["price"]-price) > MAX_DISTANCE:
                continue
            between = [p["price"] for p in pivots
                       if older["idx"] < p["idx"] < idx and p["kind"] != kind]
            if not between:
                continue
            excursion = (min(older["price"],price)-min(between) if kind == "HIGH"
                         else max(between)-max(older["price"],price))
            if excursion < MIN_EXCURSION:
                continue
            low, high = sorted((older["price"],price))
            zones.append({"schema": SCHEMA, "id": f"{SCHEMA}:{older['idx']}:{idx}",
                          "type": "RESISTANCE" if kind == "HIGH" else "SUPPORT",
                          "first_pivot_id": older["id"], "second_pivot_id": pid,
                          "first_column": older["idx"], "second_column": idx,
                          "known_at": at, "known_column": idx+1,
                          "lower": str(low), "upper": str(high),
                          "excursion_boxes": str(excursion / BOX),
                          "column_separation": idx-older["idx"]})
            break
        pivots.append(current)
    return zones
