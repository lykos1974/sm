from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_p2_edge_segmentation_audit import (
    _bucket_pole_boxes,
    _bucket_retrace,
    summarize_outcomes,
    SegmentedOutcome,
)


def _write_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    labels = tmp_path / "binance_labels.csv"
    labels.write_text(
        "pattern_name,pole_column_index,reversal_column_index,retrace_ratio,market_regime,breakout_context\n"
        "LOW_POLE,0,1,0.4,TREND,POST_BREAKOUT\n"
        "HIGH_POLE,2,3,0.8,RANGE,POST_BREAKDOWN\n"
        "LOW_POLE,4,5,0.2,TREND,POST_BREAKOUT\n"
    )
    columns = tmp_path / "binance_columns.csv"
    columns.write_text(
        "idx,kind,top,bottom,start_ts,end_ts,profile_name\n"
        "0,O,100,93,1577836800,1577840400,TEST_bs1_rev3\n"
        "1,X,99,96,1577840401,1577844000,TEST_bs1_rev3\n"
        "2,X,102,99,1577923200,1577926800,TEST_bs1_rev3\n"
        "3,O,102,100,1577926801,1577930400,TEST_bs1_rev3\n"
        "4,O,105,102,1583020800,1583024400,TEST_bs1_rev3\n"
        "5,X,104,102,1583024401,1583028000,TEST_bs1_rev3\n"
        "6,O,106,104,1583028001,1583031600,TEST_bs1_rev3\n"
    )
    candles = tmp_path / "binance_candles.csv"
    candles.write_text(
        "close_time,open,high,low,close\n"
        "1577844001,99,107,98,106\n"  # first long wins, entry 99 stop 96 target 106.5
        "1577930401,100,101,92,93\n"
        "1583028001,102,104,102,103\n"
        "1583031601,104,105,101,102\n"  # long BE exit after trigger? entry 104? stop 101 target 111.5 trigger 110 no. Not.
    )
    return labels, columns, candles


def test_segmentation_buckets() -> None:
    assert _bucket_pole_boxes(None) == "MISSING_POLE_BOXES"
    assert _bucket_pole_boxes(4) == "SMALL_<=4_BOXES"
    assert _bucket_pole_boxes(7) == "NORMAL_5_7_BOXES"
    assert _bucket_pole_boxes(8) == "LARGE_>=8_BOXES"
    assert _bucket_retrace(None) == "MISSING_RETRACE"
    assert _bucket_retrace(0.2) == "SHALLOW_<0_382"
    assert _bucket_retrace(0.5) == "NORMAL_0_382_0_618"
    assert _bucket_retrace(0.8) == "DEEP_>0_618"


def test_expectancy_math_and_be_accounting() -> None:
    rows = [
        SegmentedOutcome(2, "BTC", "LONG", "TARGET_FIRST", 2.5, {"symbol": "BTC"}),
        SegmentedOutcome(1, "BTC", "LONG", "STOP_FIRST", -1.0, {"symbol": "BTC"}),
        SegmentedOutcome(1, "BTC", "LONG", "BREAK_EVEN_EXIT", 0.0, {"symbol": "BTC"}),
        SegmentedOutcome(1, "BTC", "LONG", "NOT_REACHED", None, {"symbol": "BTC"}),
    ]

    summary = summarize_outcomes(rows, "symbol", "BTC")

    assert summary["observations"] == 5
    assert summary["unique_opportunities"] == 4
    assert summary["trades"] == 3
    assert summary["wins"] == 1
    assert summary["losses"] == 1
    assert summary["break_even_exits"] == 1
    assert summary["win_rate"] == 0.5
    assert summary["expectancy"] == 0.5
    assert summary["total_R"] == 1.5


def test_cli_emits_research_only_segmentation_outputs(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    output = tmp_path / "out"
    command = [
        sys.executable,
        "-m",
        "research_v2.patterns.pole_p2_edge_segmentation_audit",
        "--symbol-input",
        f"BTC={labels}",
        "--columns-input",
        f"BTC={columns}",
        "--candles-input",
        f"BTC={candles}",
        "--output-root",
        str(output),
        "--allow-partial-universe",
    ]

    subprocess.run(command, check=True)

    expected = {
        "p2_edge_segmentation_summary.md",
        "p2_edge_segmentation_by_symbol.csv",
        "p2_edge_segmentation_by_direction.csv",
        "p2_edge_segmentation_by_pole_strength.csv",
        "p2_edge_segmentation_by_retrace_quality.csv",
        "p2_edge_segmentation_by_market_family.csv",
        "p2_edge_segmentation_by_time.csv",
        "p2_edge_segmentation_flags.csv",
        "p2_edge_segmentation_manifest.json",
    }
    assert expected == {path.name for path in output.iterdir()}

    by_symbol = list(csv.DictReader((output / "p2_edge_segmentation_by_symbol.csv").open()))
    assert by_symbol[0]["segment"] == "BTC"
    assert by_symbol[0]["observations"] == "3"
    assert by_symbol[0]["unique_opportunities"] == "3"
    assert int(by_symbol[0]["trades"]) >= 1

    by_pole = list(csv.DictReader((output / "p2_edge_segmentation_by_pole_strength.csv").open()))
    assert {row["pole_metric"] for row in by_pole} == {"pole_boxes", "pole_duration", "pole_velocity", "relative_pole_size"}
    assert any(row["segment"] == "LARGE_>=8_BOXES" for row in by_pole)

    by_retrace = list(csv.DictReader((output / "p2_edge_segmentation_by_retrace_quality.csv").open()))
    assert any(row["segment"] == "NORMAL_0_382_0_618" for row in by_retrace)
    assert any(row["segment"] == "DEEP_>0_618" for row in by_retrace)

    flags = list(csv.DictReader((output / "p2_edge_segmentation_flags.csv").open()))
    assert any(row["flag"] == "UNIVERSE_MISMATCH" for row in flags)

    summary = (output / "p2_edge_segmentation_summary.md").read_text()
    assert "`unique_opportunities`" in summary
    assert "`status`: UNIVERSE_MISMATCH" in summary

    manifest = json.loads((output / "p2_edge_segmentation_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["production_modifications"] is False
    assert manifest["live_trader_modifications"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["verdict"] in manifest["allowed_verdicts"]
    assert manifest["universe_consistency"]["status"] == "UNIVERSE_MISMATCH"
    assert manifest["universe_consistency"]["expected"] == {"trades": 4023, "expectancy": 0.062516, "total_R": 251.5}
    assert "trend_regime" in manifest["segment_dimensions"]["market_regime"]


def test_no_production_changes_and_no_live_trader_imports() -> None:
    source_path = Path("research_v2/patterns/pole_p2_edge_segmentation_audit.py")
    tree = ast.parse(source_path.read_text())
    imported_modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    assert all("live_" not in module and "forward_trader" not in module for module in imported_modules)
    assert all("pnf_mvp.strategy" not in module for module in imported_modules)
