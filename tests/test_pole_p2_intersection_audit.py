from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_p2_edge_segmentation_audit import SegmentedOutcome
from research_v2.patterns.pole_p2_intersection_audit import (
    BASELINE_EXPECTANCY,
    DEEP_RETRACE,
    NEAR_RECENT_AVG,
    build_intersection_rows,
)


def _outcome(symbol: str, direction: str, classification: str, realized_r: float, *, deep: bool = True, near: bool = True) -> SegmentedOutcome:
    return SegmentedOutcome(
        observation_count=1,
        symbol=symbol,
        direction=direction,
        classification=classification,
        realized_r=realized_r,
        segments={
            "symbol": symbol,
            "direction": direction,
            "retrace_quality": DEEP_RETRACE if deep else "NORMAL_0_382_0_618",
            "relative_pole_size": NEAR_RECENT_AVG if near else "ABOVE_RECENT_AVG_>1_25X",
        },
    )


def _write_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    labels = tmp_path / "ena_labels.csv"
    labels.write_text(
        "pattern_name,pole_column_index,reversal_column_index,retrace_ratio,market_regime,breakout_context\n"
        "HIGH_POLE,0,1,0.8,TREND,POST_BREAKDOWN\n"
        "LOW_POLE,2,3,0.4,TREND,POST_BREAKOUT\n"
    )
    columns = tmp_path / "ena_columns.csv"
    columns.write_text(
        "idx,kind,top,bottom,start_ts,end_ts,profile_name\n"
        "0,X,100,93,1577836800,1577840400,TEST_bs1_rev3\n"
        "1,O,99,96,1577840401,1577844000,TEST_bs1_rev3\n"
        "2,O,105,102,1577923200,1577926800,TEST_bs1_rev3\n"
        "3,X,104,102,1577926801,1577930400,TEST_bs1_rev3\n"
        "4,O,106,104,1577930401,1577934000,TEST_bs1_rev3\n"
    )
    candles = tmp_path / "ena_candles.csv"
    candles.write_text(
        "close_time,open,high,low,close\n"
        "1577844001,96,97,88,90\n"
        "1577930401,102,104,101,103\n"
        "1577934001,104,105,101,102\n"
    )
    return labels, columns, candles


def test_intersection_rows_rank_and_compare_baseline() -> None:
    outcomes = [
        _outcome("ENA", "SHORT", "TARGET_FIRST", 2.5),
        _outcome("HYPE", "SHORT", "STOP_FIRST", -1.0),
        _outcome("TAO", "LONG", "BREAK_EVEN_EXIT", 0.0),
        _outcome("BTC", "SHORT", "TARGET_FIRST", 2.5),
        _outcome("SOL", "SHORT", "STOP_FIRST", -1.0, near=False),
        _outcome("ETH", "LONG", "TARGET_FIRST", 2.5, deep=False),
    ]

    rows = build_intersection_rows(outcomes)

    assert [row["intersection_name"] for row in rows] == [
        "ENA/HYPE/TAO only",
        "ENA/HYPE/TAO + SHORT",
        "ENA/HYPE/TAO + DEEP retrace",
        "ENA/HYPE/TAO + SHORT + DEEP retrace",
        "ENA/HYPE/TAO + SHORT + DEEP retrace + NEAR_RECENT_AVG",
        "All symbols + SHORT + DEEP retrace",
        "All symbols + SHORT + DEEP retrace + NEAR_RECENT_AVG",
    ]
    target_short_deep_near = rows[4]
    assert target_short_deep_near["trades"] == 2
    assert target_short_deep_near["wins"] == 1
    assert target_short_deep_near["losses"] == 1
    assert target_short_deep_near["expectancy"] == 0.75
    assert target_short_deep_near["expectancy_delta_vs_baseline"] == round(0.75 - BASELINE_EXPECTANCY, 6)
    assert target_short_deep_near["minimum_sample_warning"] == "MIN_SAMPLE_WARNING_TRADES_LT_100"
    assert sorted(int(row["rank_by_expectancy"]) for row in rows) == list(range(1, 8))
    assert sorted(int(row["rank_by_total_R"]) for row in rows) == list(range(1, 8))


def test_cli_emits_research_only_intersection_outputs(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    output = tmp_path / "out"
    command = [
        sys.executable,
        "-m",
        "research_v2.patterns.pole_p2_intersection_audit",
        "--symbol-input",
        f"ENA={labels}",
        "--columns-input",
        f"ENA={columns}",
        "--candles-input",
        f"ENA={candles}",
        "--output-root",
        str(output),
        "--allow-partial-universe",
    ]

    subprocess.run(command, check=True)

    expected = {
        "p2_intersection_audit_summary.md",
        "p2_intersection_audit_results.csv",
        "p2_intersection_audit_flags.csv",
        "p2_intersection_audit_manifest.json",
    }
    assert expected == {path.name for path in output.iterdir()}

    results = list(csv.DictReader((output / "p2_intersection_audit_results.csv").open()))
    assert len(results) == 7
    assert results[0]["intersection_name"] == "ENA/HYPE/TAO only"
    assert results[0]["baseline_expectancy"] == "0.062516"
    assert "rank_by_expectancy" in results[0]
    assert any(row["minimum_sample_warning"] == "MIN_SAMPLE_WARNING_TRADES_LT_100" for row in results)

    flags = list(csv.DictReader((output / "p2_intersection_audit_flags.csv").open()))
    assert any(row["flag"] == "UNIVERSE_MISMATCH" for row in flags)
    assert any(row["flag"] == "MIN_SAMPLE_WARNING" for row in flags)

    summary = (output / "p2_intersection_audit_summary.md").read_text()
    assert "Research only" in summary
    assert "not optimization" in summary
    assert "`status`: UNIVERSE_MISMATCH" in summary
    assert "## Rank by expectancy" in summary
    assert "## Rank by total_R" in summary

    manifest = json.loads((output / "p2_intersection_audit_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["diagnostic_only"] is True
    assert manifest["optimization"] is False
    assert manifest["production_modifications"] is False
    assert manifest["live_trader_modifications"] is False
    assert manifest["detector_modifications"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["universe_consistency"]["expected"] == {"trades": 4023, "expectancy": 0.062516, "total_R": 251.5}
    assert len(manifest["intersections"]) == 7


def test_no_production_changes_and_no_live_trader_imports() -> None:
    source_path = Path("research_v2/patterns/pole_p2_intersection_audit.py")
    tree = ast.parse(source_path.read_text())
    imported_modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    assert all("live_" not in module and "forward_trader" not in module for module in imported_modules)
    assert all("pnf_mvp.strategy" not in module for module in imported_modules)
