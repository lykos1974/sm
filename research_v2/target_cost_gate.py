"""One bounded, read-only cost stress for a completed BTC target sweep.

Costs use entry notional as a proxy on both sides. This cannot establish net
exchange P&L because the simulator did not record actual exit fill prices.
"""
from __future__ import annotations

import argparse
import csv
import json
import tkinter as tk
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tkinter import filedialog

from research_v2.backtest_workspace import _sha
from research_v2.target_comparison_chart import _decimal, _fmt, _integer, load_comparison

SCENARIOS = (Decimal("0"), Decimal("10"), Decimal("20"))  # all-in bps per side
FIELDS = ("target_R", "cost_bps_per_side", "trades", "gross_R", "cost_R",
          "adjusted_R", "max_drawdown_R", "worst_quarter_R", "quarters_with_trades",
          "quarterly_adjusted_R")


def _rows(path: Path, required: set[str]) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if (reader.fieldnames is None or len(reader.fieldnames) != len(set(reader.fieldnames))
                or not required <= set(reader.fieldnames)):
            raise ValueError("invalid research CSV schema")
        rows = list(reader)
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError("malformed research CSV")
    return rows


def analyze(root: Path) -> list[dict[str, str]]:
    root = root.resolve(strict=True)
    comparison = load_comparison(root)
    with (root / "target_sweep/comparison.csv").open(newline="", encoding="utf-8-sig") as stream:
        summaries = list(csv.DictReader(stream))
    with (root / "causal_manifest.json").open(encoding="utf-8") as stream:
        causal = json.load(stream)
    decision_file = root / "causal_decisions.csv"
    if causal.get("causal_decisions_sha256") != _sha(decision_file):
        raise ValueError("decision ledger hash mismatch")
    decisions = _rows(decision_file, {"decision_id", "direction", "entry_price", "stop_price",
                                    "entry_candle_close_ms"})
    by_id = {}
    for row in decisions:
        if row["decision_id"] in by_id or row["direction"] != "LONG":
            raise ValueError("ambiguous decision")
        by_id[row["decision_id"]] = row
    output = []
    for target, summary in zip(comparison, summaries, strict=True):
        directory = root / "portfolio" if target.target == Decimal("2.5") else (
            root / "target_sweep" / f"target_{_fmt(target.target)}R")
        manifest_path = directory / "portfolio_reality_manifest.json"
        if _sha(manifest_path) != summary["portfolio_manifest_sha256"]:
            raise ValueError("portfolio manifest hash mismatch")
        with manifest_path.open(encoding="utf-8") as stream:
            manifest = json.load(stream)
        if (_decimal(str(manifest.get("target_R"))) != target.target
                or _integer(summary["resolved_trades"]) != manifest["resolved_portfolio_trades"]):
            raise ValueError("portfolio summary mismatch")
        trades = _rows(directory / "portfolio_reality_trade_sequence.csv",
                       {"trade_id", "opportunity_id", "direction", "entry_timestamp",
                        "exit_timestamp", "result_R"})
        if len(trades) != _integer(summary["resolved_trades"]):
            raise ValueError("trade count mismatch")
        seen = set()
        validated = []
        for trade in trades:
            key = (trade["trade_id"], trade["opportunity_id"])
            if (key in seen or trade["direction"] != "LONG"
                    or not trade["opportunity_id"].startswith("OPP-")):
                raise ValueError("ambiguous trade")
            seen.add(key)
            decision = by_id.get("DEC-" + trade["opportunity_id"][4:])
            if decision is None:
                raise ValueError("missing causal decision")
            entry, stop = _decimal(decision["entry_price"]), _decimal(decision["stop_price"])
            entry_ms, exit_ms = _integer(trade["entry_timestamp"]), _integer(trade["exit_timestamp"])
            if (entry <= stop or stop <= 0 or entry_ms < _integer(decision["entry_candle_close_ms"])
                    or exit_ms < entry_ms or entry_ms < 10**12):
                raise ValueError("invalid causal trade geometry")
            validated.append((exit_ms, _decimal(trade["result_R"]),
                              entry / (entry - stop)))
        validated.sort(key=lambda item: item[0])
        if abs(sum((value for _, value, _ in validated), Decimal(0)) - target.profit) > Decimal("0.00001"):
            raise ValueError("trade gross total mismatch")
        for bps in SCENARIOS:
            cumulative = peak = worst = costs = Decimal(0)
            quarters = defaultdict(lambda: Decimal(0))
            for exit_ms, gross, notional_per_risk in validated:
                # Cost is per side on entry notional; exit notional is unknown.
                cost = 2 * bps / Decimal(10_000) * notional_per_risk
                costs += cost
                net = gross - cost
                cumulative += net
                peak = max(peak, cumulative)
                worst = max(worst, peak - cumulative)
                utc = datetime.fromtimestamp(exit_ms / 1000, timezone.utc)
                quarters[f"{utc.year}-Q{(utc.month - 1) // 3 + 1}"] += net
            if bps == 0 and abs(worst - target.drawdown) > Decimal("0.00001"):
                raise ValueError("gross drawdown mismatch")
            output.append(dict(zip(FIELDS, (str(target.target), str(bps), str(len(validated)),
                                            str(target.profit), str(costs), str(cumulative),
                                            str(worst), str(min(quarters.values())) if quarters else "",
                                            str(len(quarters)),
                                            json.dumps({period: str(quarters[period])
                                                        for period in sorted(quarters)}, sort_keys=True)),
                                    strict=True)))
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    root = args.result_dir
    if root is None:
        window = tk.Tk()
        window.withdraw()
        try:
            selected = filedialog.askdirectory(title="Επίλεξε ολοκληρωμένο BTC target-sweep results")
        finally:
            window.destroy()
        if not selected:
            parser.error("no result directory selected")
        root = Path(selected)
    output = args.output_dir or root / "bounded_cost_gate"
    if output.exists():
        parser.error("output directory already exists")
    rows = analyze(root)
    output.mkdir(parents=True, exist_ok=False)
    report = output / "bounded_cost_gate.csv"
    with report.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    manifest = {"schema": "bounded-cost-gate-v1", "research_only": True,
                "result_dir": str(root.resolve()),
                "comparison_sha256": _sha(root / "target_sweep/comparison.csv"),
                "decisions_sha256": _sha(root / "causal_decisions.csv"),
                "cost_bps_per_side": [str(x) for x in SCENARIOS],
                "cost_method": "entry notional proxy on both sides, divided by entry-stop price risk",
                "limitations": "No actual exit fill, fees, slippage, funding, margin or broker sizing; adjusted R is a sensitivity, not net P&L",
                "report_sha256": _sha(report)}
    (output / "bounded_cost_gate_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
