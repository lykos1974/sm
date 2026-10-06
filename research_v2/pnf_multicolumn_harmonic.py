"""Offline nonconsecutive coarse P&F Gartley projections; no execution logic.

The 24-column horizon, envelope test, and projection-at-C-close convention
are versioned research assumptions. The restricted ratios are reused from
gartley_pole_prz; these observations are distinct from its consecutive PRZ.
"""
from __future__ import annotations

from decimal import Decimal
from itertools import combinations

from research_v2.gartley_pole_prz import Pivot, number, project

SCHEMA = "pnf-multicolumn-gartley-v1"
BOX = Decimal(1000)
LOOKBACK = 24


def project_multicolumn(facts: list[dict]) -> list[dict]:
    parsed = []
    for i, fact in enumerate(facts):
        if (not isinstance(fact, dict) or fact.get("type") != "PIVOT_CONFIRMED"
                or type(fact.get("column_id")) is not int or fact["column_id"] != i
                or type(fact.get("extreme_at")) is not int
                or type(fact.get("confirmed_at")) is not int
                or type(fact.get("confirmation_sequence")) is not int
                or not 0 <= fact["extreme_at"] <= fact["confirmed_at"]
                or (parsed and (fact["confirmed_at"] <= parsed[-1]["confirmed_at"]
                                or fact["confirmation_sequence"] <= parsed[-1]["sequence"]))
                or fact.get("kind") != ("HIGH" if i % 2 else "LOW")
                or fact.get("pivot_id") != f"pnf:{i}:{'X' if i%2 else 'O'}"):
            raise ValueError("invalid coarse pivot chronology")
        price, close = number(fact.get("price")), number(fact.get("confirmation_close"))
        if price <= 0 or close <= 0 or price % BOX != 0:
            raise ValueError("invalid coarse pivot grid")
        parsed.append({"price": price, "close": close,
                       "confirmed_at": fact["confirmed_at"],
                       "extreme_at": fact["extreme_at"],
                       "sequence": fact["confirmation_sequence"],
                       "id": fact["pivot_id"], "kind": fact["kind"]})
    zones = []
    for c in range(3, len(parsed)):
        for x, a, b in combinations(range(max(0, c-LOOKBACK+1), c), 3):
            indices = (x, a, b, c)
            if all(v+1 == w for v, w in zip(indices, indices[1:])):
                continue
            selected = [parsed[j] for j in indices]
            if tuple(p["kind"] for p in selected) not in (
                    ("LOW", "HIGH", "LOW", "HIGH"),
                    ("HIGH", "LOW", "HIGH", "LOW")):
                continue
            # Skipped pivots may oscillate within a leg, but cannot redefine
            # its chosen endpoints. This is a causal structural convention.
            if any(not min(parsed[v]["price"], parsed[w]["price"]) <= parsed[k]["price"]
                   <= max(parsed[v]["price"], parsed[w]["price"])
                   for v, w in zip(indices, indices[1:]) for k in range(v+1, w)):
                continue
            virtual = [Pivot(p["id"], j, p["kind"], p["price"], p["extreme_at"],
                             p["confirmed_at"], p["sequence"])
                       for j, p in enumerate(selected)]
            candidate, _ = project(virtual, connected_column_id=4, box_size=BOX)
            if candidate is None:
                continue
            close = selected[-1]["close"]
            if not (close > candidate.upper if candidate.direction == "LONG"
                    else close < candidate.lower):
                continue
            zones.append({"id": f"{SCHEMA}:{':'.join(str(j) for j in indices)}",
                          "type": "NONCONSECUTIVE_GARTLEY_PROJECTION",
                          "direction": candidate.direction,
                          "pivot_columns": list(indices),
                          "pivots": [{"column": j, "price": str(parsed[j]["price"]),
                                      "extreme_at": parsed[j]["extreme_at"]}
                                     for j in indices],
                          "known_at": selected[-1]["confirmed_at"],
                          "lower": str(candidate.lower), "upper": str(candidate.upper),
                          "r_b": str(candidate.r_b), "r_c": str(candidate.r_c),
                          "k_bc": str(candidate.k_bc),
                          "d_xa": str(candidate.d_xa), "d_abcd": str(candidate.d_abcd)})
    return zones
