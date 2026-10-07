"""Illustrative cost sensitivity of the pinned BTC 2024 accepted trade cohort.

No fee schedule, exchange fills, funding, or true net P&L is asserted.
"""
from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

from research_v2.patterns.pole_miner_btc_preflight import (
    DECISION_HEADER, PINNED_COUNTS, PINNED_HASHES, TRADE_REQUIRED,
    _int, _json, _num, _rows, _sha,
)

DEFAULT_BPS = (Decimal(0), Decimal(2), Decimal(5), Decimal(10))


def _fmt(number: Decimal) -> str:
    return format(number.normalize(), "f")


def evaluate(decisions: Path, trades: Path, preflight: Path, output: Path, *,
             bps_grid: tuple[Decimal, ...] = DEFAULT_BPS,
             expected_counts: tuple[int, int] = PINNED_COUNTS,
             expected_hashes: tuple[str, str] = (PINNED_HASHES[0], PINNED_HASHES[2])) -> dict:
    decisions, trades, preflight, output = map(Path, (decisions, trades, preflight, output))
    if output.exists() or output.resolve() in {p.resolve() for p in (decisions, trades, preflight)}:
        raise FileExistsError("new report path required")
    if (not output.parent.is_dir() or not bps_grid or len(bps_grid) > 10
            or any(type(b) is not Decimal or not b.is_finite() or not 0 <= b <= 100
                   for b in bps_grid) or tuple(sorted(set(bps_grid))) != bps_grid):
        raise ValueError("invalid cost scenario")
    actual = (_sha(decisions), _sha(trades))
    if actual != tuple(v.lower() for v in expected_hashes):
        raise ValueError("source hash mismatch")
    prior = _json(preflight)
    if (prior.get("schema") != "pole-miner-btc-preflight-v1"
            or prior.get("status") != "GROSS_ONLY_NOT_MINER_INPUT"
            or prior.get("decisions") != expected_counts[0]
            or prior.get("linked_trades") != expected_counts[1]
            or prior.get("source_sha256", {}).get("decisions") != actual[0]
            or prior.get("source_sha256", {}).get("trades") != actual[1]):
        raise ValueError("preflight binding mismatch")
    drows, trows = _rows(decisions, DECISION_HEADER, True), _rows(trades, TRADE_REQUIRED, False)
    if (len(drows), len(trows)) != expected_counts:
        raise ValueError("cohort count mismatch")
    details, ids, gross = [], set(), Decimal(0)
    for row in trows:
        key = row["opportunity_id"]
        if key in ids or not key.startswith("OPP-") or not key[4:].isdigit():
            raise ValueError("duplicate or invalid opportunity identity")
        ids.add(key)
        index = int(key[4:])
        if (index < 1 or index > len(drows) or key != f"OPP-{index:06d}"
                or drows[index-1]["decision_id"] != f"DEC-{index:06d}"
                or row["symbol"] != "BTC" or row["direction"] != "LONG"):
            raise ValueError("opportunity identity mismatch")
        source = drows[index-1]
        entry, stop = _num(source["entry_price"]), _num(source["stop_price"])
        risk = entry - stop
        if entry <= 0 or risk <= 0:
            raise ValueError("invalid cost geometry")
        known, opened, closed = (_int(source[k]) for k in ("decision_known_at_ms",
                                  "entry_candle_open_ms", "entry_candle_close_ms"))
        entered, exited = _int(row["entry_timestamp"]), _int(row["exit_timestamp"])
        result = _num(row["result_R"])
        conservative = row["classification"] == "SAME_CANDLE_FILL_STOP_CONSERVATIVE" and result == -1
        if (not 10**12 <= known < opened <= closed <= entered <= exited
                or (entered == exited and not conservative)
                or (row["classification"] == "SAME_CANDLE_FILL_STOP_CONSERVATIVE"
                    and (entered != exited or not conservative))):
            raise ValueError("invalid trade chronology")
        gross += result
        details.append((result, Decimal(2) * entry / (Decimal(10000) * risk)))
    if abs(gross - _num(prior.get("gross_total_R"))) > Decimal("0.000001"):
        raise ValueError("gross cohort mismatch")
    cost_per_bps = sum((c for _, c in details), Decimal(0))
    scenarios = []
    for bps in bps_grid:
        realized = [r - bps * c for r, c in details]
        total = sum(realized, Decimal(0))
        running = peak = drawdown = Decimal(0)
        for r in realized:
            running += r
            peak = max(peak, running)
            drawdown = max(drawdown, peak - running)
        scenarios.append({"bps_per_side": _fmt(bps), "modeled_total_R": _fmt(total),
                          "modeled_mean_R": _fmt(total / len(details)),
                          "positive_trades": sum(r > 0 for r in realized),
                          "modeled_max_drawdown_R": _fmt(drawdown)})
    report = {"schema": "pole-miner-btc-cost-scenario-v1",
              "status": "ILLUSTRATIVE_COST_ONLY", "execution": "OFF",
              "source_sha256": {"decisions": actual[0], "trades": actual[1],
                                "preflight": _sha(preflight)},
              "accepted_trades": len(details), "gross_total_R": _fmt(gross),
              "break_even_bps_per_side": _fmt(gross / cost_per_bps) if cost_per_bps and gross > 0 else None,
              "scenarios": scenarios,
              "assumption": "2 * bps/10000 * planned entry price / planned entry-stop distance per accepted trade",
              "limitations": "Planned prices, gross OHLC fills and symmetric hypothetical costs; funding, exchange fills, spread variation and actual net P&L unavailable; 2024 already inspected"}
    with output.open("x", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
        f.write("\n")
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("decisions", "trades", "preflight", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    a = p.parse_args()
    print(json.dumps(evaluate(a.decisions, a.trades, a.preflight, a.output), indent=2))


if __name__ == "__main__":
    main()
