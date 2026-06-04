from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_p2_candidate_sampler import (
    ALLOWED_VERDICTS,
    _jaccard,
    run,
)


def _write_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    labels = tmp_path / "labels.csv"
    labels.write_text(
        "pattern_name,pole_column_index,reversal_column_index,retrace_ratio,market_regime,breakout_context,opposing_pole_distance_columns,enhanced_by_opposing_pole\n"
        "LOW_POLE,0,1,0.8,TREND,POST_BREAKOUT,999,true\n"
        "HIGH_POLE,3,4,0.7,RANGE,POST_BREAKDOWN,999,true\n"
        "LOW_POLE,6,7,0.4,TREND,POST_BREAKOUT,999,true\n"
        "HIGH_POLE,9,10,0.3,RANGE,POST_BREAKDOWN,999,true\n"
    )
    columns = tmp_path / "columns.csv"
    columns.write_text(
        "idx,kind,top,bottom,start_ts,end_ts,profile_name\n"
        "0,O,100,95,1,10,TEST_bs1_rev3\n"
        "1,X,100,96,11,20,TEST_bs1_rev3\n"
        "2,O,103,99,21,30,TEST_bs1_rev3\n"
        "3,X,110,105,101,110,TEST_bs1_rev3\n"
        "4,O,109,106,111,120,TEST_bs1_rev3\n"
        "5,X,108,104,121,130,TEST_bs1_rev3\n"
        "6,O,120,116,201,210,TEST_bs1_rev3\n"
        "7,X,120,117,211,220,TEST_bs1_rev3\n"
        "8,O,122,119,221,230,TEST_bs1_rev3\n"
        "9,X,130,126,301,310,TEST_bs1_rev3\n"
        "10,O,129,127,311,320,TEST_bs1_rev3\n"
        "11,X,131,128,321,330,TEST_bs1_rev3\n"
    )
    candles = tmp_path / "candles.csv"
    candles.write_text(
        "close_time,open,high,low,close\n"
        "22,100,107,99,106\n"   # long target first: entry 100 stop 97 target 107.5? Needs next candle.
        "23,106,108,105,107\n"
        "122,106,107,98,99\n"  # short target first: entry 106 stop 109 target 98.5
        "222,120,124,119,123\n"  # long stop first: entry 120 stop 117
        "322,127,130,126,130\n"  # short stop first: entry 127 stop 130
    )
    return labels, columns, candles


def test_jaccard_similarity_for_novelty_control() -> None:
    assert _jaccard(frozenset({"a", "b"}), frozenset({"b", "c"})) == 1 / 3
    assert _jaccard(frozenset({"a"}), frozenset({"a"})) == 1.0


def test_cli_emits_phase1_sampler_outputs(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    output = tmp_path / "out"
    command = [
        sys.executable,
        "-m",
        "research_v2.patterns.pole_p2_candidate_sampler",
        "--symbol-input",
        f"ENA={labels}",
        "--columns-input",
        f"ENA={columns}",
        "--candles-input",
        f"ENA={candles}",
        "--output-root",
        str(output),
        "--allow-partial-universe",
        "--min-trades",
        "1",
        "--min-wins",
        "0",
    ]

    subprocess.run(command, check=True)

    expected = {
        "p2_candidate_sampler_results.csv",
        "p2_candidate_sampler_top100.csv",
        "p2_candidate_sampler_summary.md",
        "p2_candidate_sampler_manifest.json",
    }
    assert expected == {path.name for path in output.iterdir()}

    results = list(csv.DictReader((output / "p2_candidate_sampler_results.csv").open()))
    assert results
    assert all(int(row["rule_width"]) <= 3 for row in results)
    assert {"candidate_id", "rule_definition", "expectancy", "total_R", "symbol_hhi", "year_hhi", "quarter_hhi"} <= set(results[0])

    top100 = list(csv.DictReader((output / "p2_candidate_sampler_top100.csv").open()))
    assert top100
    assert all(row["is_cluster_representative"] == "True" for row in top100)

    summary = (output / "p2_candidate_sampler_summary.md").read_text()
    assert "No live trader" in summary
    assert "genetic algorithm" in summary
    assert "Minimum sample requirements" in summary
    assert "Novelty control" in summary

    manifest = json.loads((output / "p2_candidate_sampler_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["production_modifications"] is False
    assert manifest["live_trader_modifications"] is False
    assert manifest["detector_modifications"] is False
    assert manifest["exchange_code_modifications"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["genetic_algorithm"] is False
    assert manifest["evolution"] is False
    assert manifest["mutation"] is False
    assert manifest["crossover"] is False
    assert manifest["max_rule_width"] == 3
    assert manifest["min_trades"] == 1
    assert manifest["min_wins"] == 0
    assert manifest["verdict"] in ALLOWED_VERDICTS
    assert manifest["universe_consistency"]["status"] == "UNIVERSE_MISMATCH"
    assert "opposing_pole_distance_columns" in manifest["forbidden_features"]
    assert "enhanced_by_opposing_pole" in manifest["forbidden_features"]


def test_run_rejects_partial_universe_by_default(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_p2_candidate_sampler",
            "--symbol-input",
            f"ENA={labels}",
            "--columns-input",
            f"ENA={columns}",
            "--candles-input",
            f"ENA={candles}",
            "--output-root",
            str(tmp_path / "out"),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "missing required seven-market symbols" in result.stderr


def test_no_production_changes_and_no_live_trader_imports() -> None:
    tree = ast.parse(Path("research_v2/patterns/pole_p2_candidate_sampler.py").read_text())
    imported_modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    assert all("live_" not in module and "forward_trader" not in module for module in imported_modules)
    assert all("pnf_mvp.strategy" not in module for module in imported_modules)
