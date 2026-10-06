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
COARSE_SCHEMA = "pnf-multicolumn-sr-v2-coarse"


def derive_zones(facts: list[dict], *, box: Decimal = BOX,
                 max_distance_boxes: int = 3, min_excursion_boxes: int = 6,
                 lookback_columns: int = LOOKBACK_COLUMNS, schema: str = SCHEMA) -> list[dict]:
    """Use confirmed pivots in append order; report the second confirmation time.

    A prior level and an intervening swing must be known when the second
    pivot is confirmed. One most-recent qualifying pair per confirmation.
    Future pivots never alter already emitted zones.
    """
    if not isinstance(facts, list):
        raise ValueError("pivot facts required")
    if (box <= 0 or not box.is_finite() or type(max_distance_boxes) is not int
            or type(min_excursion_boxes) is not int or type(lookback_columns) is not int
            or min(max_distance_boxes, min_excursion_boxes, lookback_columns) <= 0):
        raise ValueError("invalid structural profile")
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
        if price <= 0 or price % box:
            raise ValueError("off-grid pivot")
        current = {"idx": idx, "at": at, "extreme_at": extreme,
                   "sequence": seq, "kind": kind,
                   "price": price, "id": pid}
        # The confirmation of this pivot precedes every future pivot in
        # this scan. Exclude pairs without an intervening opposite pivot.
        for older in reversed(pivots):
            if idx-older["idx"] > lookback_columns:
                break
            if older["kind"] != kind or idx-older["idx"] < 2:
                continue
            if abs(older["price"]-price) > max_distance_boxes * box:
                continue
            between = [p["price"] for p in pivots
                       if older["idx"] < p["idx"] < idx and p["kind"] != kind]
            if not between:
                continue
            excursion = (min(older["price"],price)-min(between) if kind == "HIGH"
                         else max(between)-max(older["price"],price))
            if excursion < min_excursion_boxes * box:
                continue
            low, high = sorted((older["price"],price))
            zones.append({"schema": schema, "id": f"{schema}:{older['idx']}:{idx}",
                          "type": "RESISTANCE" if kind == "HIGH" else "SUPPORT",
                          "first_pivot_id": older["id"], "second_pivot_id": pid,
                          "first_column": older["idx"], "second_column": idx,
                          "known_at": at, "known_column": idx+1,
                          "first_extreme_at": older["extreme_at"],
                          "second_extreme_at": extreme,
                          "lower": str(low), "upper": str(high),
                          "excursion_boxes": str(excursion / box),
                          "column_separation": idx-older["idx"]})
            break
        pivots.append(current)
    return zones


def derive_coarse_zones(facts: list[dict]) -> list[dict]:
    """Exploratory 10x price-scale P&F context, separate from the fine v1.

    The coarse reversal itself confirms at least three coarse boxes; a
    second extremum within one coarse box of the first is a repeated level.
    """
    return derive_zones(facts, box=Decimal(1000), max_distance_boxes=1,
                        min_excursion_boxes=3, lookback_columns=24,
                        schema=COARSE_SCHEMA)
