from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import (
    COMBINED,
    _percentile_90,
    _verdict,
)

OUTPUTS = {
    "next_open_expectancy_summary.md",
    "next_open_expectancy_symbol_breakdown.csv",
    "next_open_expectancy_targets.csv",
    "next_open_expectancy_risk_distribution.csv",
    "next_open_expectancy_manifest.json",
}


def _write_symbol(tmp_path: Path, symbol: str, entry: float, first_high: float, first_low: float, later_high: float) -> tuple[Path, Path, Path]:
    root = tmp_path / symbol
    root.mkdir()
    labels = root / "labels.csv"
    labels.write_text("pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\nLOW_POLE,0,1,3,false\n")
    columns = root / "columns.csv"
    columns.write_text("idx,kind,top,bottom,start_ts,end_ts,profile_name\n0,O,100,95,1,10,TEST_bs1_rev3\n1,X,100,96,11,20,TEST_bs1_rev3\n2,O,99,97,21,30,TEST_bs1_rev3\n")
    candles = root / "candles.csv"
    candles.write_text(f"close_time,open,high,low,close\n22,{entry},{first_high},{first_low},{entry}\n23,{entry},{later_high},{first_low},{entry}\n")
    return labels, columns, candles


def _args(tmp_path: Path) -> tuple[list[str], Path]:
    fixtures = {
        "BTC": _write_symbol(tmp_path, "BTC", 100, 106, 99, 109),
        "ETH": _write_symbol(tmp_path, "ETH", 200, 206, 199, 209),
        "SOL": _write_symbol(tmp_path, "SOL", 50, 53, 47, 53),
    }
    args: list[str] = []
    for symbol, (labels, columns, candles) in fixtures.items():
        args.extend(["--symbol-input", f"{symbol}={labels}", "--columns-input", f"{symbol}={columns}", "--candles-input", f"{symbol}={candles}"])
    output = tmp_path / "output"
    args.extend(["--output-root", str(output)])
    return args, output


def test_p90_uses_reproducible_nearest_rank() -> None:
    assert _percentile_90([1, 2, 3, 4, 5]) == 5
    assert _percentile_90([]) == ""


def test_cli_emits_only_next_open_research_outputs_with_risk_and_expectancy(tmp_path: Path) -> None:
    args, output = _args(tmp_path)
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_next_open_expectancy_audit", *args], check=True)
    assert {path.name for path in output.iterdir()} == OUTPUTS

    risks = list(csv.DictReader((output / "next_open_expectancy_risk_distribution.csv").open()))
    assert {row["entry_candidate"] for row in risks} == {"NEXT_COLUMN_OPEN_ENTRY"}
    assert {(row["symbol"], row["entry_price"], row["stop_price"], row["stop_distance_boxes"]) for row in risks} == {
        ("BTC", "100.0", "97.0", "3.0"), ("ETH", "200.0", "197.0", "3.0"), ("SOL", "50.0", "47.0", "3.0")
    }

    targets = list(csv.DictReader((output / "next_open_expectancy_targets.csv").open()))
    combined_two_r = next(row for row in targets if row["symbol"] == COMBINED and row["r_target"] == "2.0")
    assert combined_two_r == {
        "symbol": "COMBINED", "r_target": "2.0", "observations": "3", "target_first": "2", "stop_first": "1",
        "ambiguous": "0", "not_reached": "0", "unknown": "0", "resolved": "3", "win_rate": "0.666667",
        "loss_rate": "0.333333", "expected_R": "1.0",
    }
    combined_one_r = next(row for row in targets if row["symbol"] == COMBINED and row["r_target"] == "1.0")
    assert combined_one_r["ambiguous"] == "1"
    assert combined_one_r["resolved"] == "2"

    manifest = json.loads((output / "next_open_expectancy_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["strategy_promotion"] is False
    assert manifest["entry_candidate"] == "NEXT_COLUMN_OPEN_ENTRY"
    assert manifest["verdict"] == "HIGH_PRIORITY_RESEARCH"
    assert "PROMOTE" not in manifest["allowed_verdicts"]
    summary = (output / "next_open_expectancy_summary.md").read_text()
    assert "never outputs `PROMOTE`" in summary
    assert "Ambiguous, not-reached, and unknown rows" in summary


def test_verdict_can_discard_without_promoting() -> None:
    rows = [
        {"symbol": COMBINED, "r_target": 1.0, "resolved": 2, "expected_R": 0.0},
        {"symbol": COMBINED, "r_target": 2.0, "resolved": 2, "expected_R": 0.5},
    ]
    assert _verdict(rows)[0] == "DISCARD"


def test_existing_audits_and_production_strategy_are_not_modified() -> None:
    module = Path("research_v2/patterns/pole_core_motif_next_open_expectancy_audit.py").read_text()
    assert "evaluate_pullback_retest" not in module
    assert "NEXT_COLUMN_OPEN_ENTRY" in module
