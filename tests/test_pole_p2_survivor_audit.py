from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path


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
        "22,100,107,99,106\n"
        "23,106,108,105,107\n"
        "122,106,107,98,99\n"
        "222,120,124,119,123\n"
        "322,127,130,126,130\n"
    )
    return labels, columns, candles


from research_v2.patterns.pole_p2_survivor_audit import OUTPUT_NAMES, TARGET_CANDIDATE_IDS, _load_target_rules


def _write_candidates(path: Path) -> None:
    path.write_text(
        "candidate_id,candidate_verdict,is_cluster_representative,rule_width,rule_definition,expectancy,total_R\n"
        "CAND-000001,IGNORED,True,1,ETH(symbol in [ETH]),0.0,0.0\n"
        "CAND-000053,PROMISING_SUBPOPULATIONS_FOUND,True,1,ENA(symbol in [ENA]),0.1,1.0\n"
        "CAND-000065,PROMISING_SUBPOPULATIONS_FOUND,True,1,LONG(direction in [LONG]),0.2,2.0\n"
    )


def test_load_target_rules_requires_exact_survivor_scope(tmp_path: Path) -> None:
    candidate_input = tmp_path / "candidates.csv"
    _write_candidates(candidate_input)

    rules = _load_target_rules(candidate_input)

    assert [rule.candidate_id for rule in rules] == list(TARGET_CANDIDATE_IDS)
    assert [rule.source_rank for rule in rules] == [2, 3]
    assert rules[0].predicates == (("symbol", frozenset({"ENA"})),)


def test_cli_emits_focused_survivor_outputs(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    candidate_input = tmp_path / "candidates.csv"
    _write_candidates(candidate_input)
    output = tmp_path / "survivor"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_p2_survivor_audit",
            "--candidate-input",
            str(candidate_input),
            "--symbol-input",
            f"ENA={labels}",
            "--columns-input",
            f"ENA={columns}",
            "--candles-input",
            f"ENA={candles}",
            "--output-root",
            str(output),
            "--allow-partial-universe",
            "--min-candidate-trades",
            "1",
            "--min-removal-trades",
            "0",
        ],
        check=True,
    )

    assert set(OUTPUT_NAMES) == {path.name for path in output.iterdir()}

    equity = list(csv.DictReader((output / "p2_survivor_audit_equity_metrics.csv").open()))
    assert [row["candidate_id"] for row in equity] == list(TARGET_CANDIDATE_IDS)
    assert {"profit_factor", "max_drawdown_R", "longest_losing_streak", "candidate_verdict"} <= set(equity[0])

    symbol_breakdown = list(csv.DictReader((output / "p2_survivor_audit_symbol_breakdown.csv").open()))
    assert symbol_breakdown
    assert set(row["candidate_id"] for row in symbol_breakdown) <= set(TARGET_CANDIDATE_IDS)
    assert {"breakdown_type", "breakdown_value", "trade_share", "expectancy_delta_vs_base"} <= set(symbol_breakdown[0])

    leave_quarter = list(csv.DictReader((output / "p2_survivor_audit_leave_quarter_out.csv").open()))
    assert {"left_out_value", "remaining_expectancy", "expectancy_retention", "insufficient_after_leave_out"} <= set(leave_quarter[0])

    summary = (output / "p2_survivor_audit_summary.md").read_text()
    assert "CAND-000053 and CAND-000065" in summary
    assert "No GA, optimization, live trader" in summary
    assert "p2_survivor_audit_leave_year_out.csv" in summary

    manifest = json.loads((output / "p2_survivor_audit_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["scope"] == "focused_survivor_audit_only"
    assert manifest["candidate_ids"] == list(TARGET_CANDIDATE_IDS)
    assert manifest["production_modifications"] is False
    assert manifest["live_trader_modifications"] is False
    assert manifest["detector_modifications"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["genetic_algorithm"] is False
    assert manifest["optimization"] is False
    assert manifest["universe_consistency"]["status"] == "UNIVERSE_MISMATCH"


def test_cli_rejects_missing_survivor_candidate(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    candidate_input = tmp_path / "candidates.csv"
    candidate_input.write_text(
        "candidate_id,candidate_verdict,is_cluster_representative,rule_width,rule_definition,expectancy,total_R\n"
        "CAND-000053,PROMISING_SUBPOPULATIONS_FOUND,True,1,ENA(symbol in [ENA]),0.1,1.0\n"
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_p2_survivor_audit",
            "--candidate-input",
            str(candidate_input),
            "--symbol-input",
            f"ENA={labels}",
            "--columns-input",
            f"ENA={columns}",
            "--candles-input",
            f"ENA={candles}",
            "--output-root",
            str(tmp_path / "out"),
            "--allow-partial-universe",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "CAND-000065" in result.stderr


def test_no_production_changes_and_no_live_trader_imports() -> None:
    tree = ast.parse(Path("research_v2/patterns/pole_p2_survivor_audit.py").read_text())
    imported_modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    assert all("live_" not in module and "forward_trader" not in module for module in imported_modules)
    assert all("pnf_mvp.strategy" not in module for module in imported_modules)
