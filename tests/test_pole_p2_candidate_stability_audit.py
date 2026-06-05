import ast
import csv
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research_v2.patterns.pole_p2_candidate_stability_audit import ALLOWED_VERDICTS, _parse_rule_definition, run


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


def test_rule_definition_parser_handles_sampler_format() -> None:
    parsed = _parse_rule_definition("ENA(symbol in [ENA]) + POST_BREAKOUT(breakout_context in [POST_BREAKOUT])")
    assert parsed == (("symbol", frozenset({"ENA"})), ("breakout_context", frozenset({"POST_BREAKOUT"})))


def test_cli_emits_candidate_stability_outputs(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    sampler_output = tmp_path / "sampler"
    subprocess.run(
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
            str(sampler_output),
            "--allow-partial-universe",
            "--min-trades",
            "1",
            "--min-wins",
            "0",
        ],
        check=True,
    )

    output = tmp_path / "stability"
    command = [
        sys.executable,
        "-m",
        "research_v2.patterns.pole_p2_candidate_stability_audit",
        "--candidate-input",
        str(sampler_output / "p2_candidate_sampler_top100.csv"),
        "--symbol-input",
        f"ENA={labels}",
        "--columns-input",
        f"ENA={columns}",
        "--candles-input",
        f"ENA={candles}",
        "--output-root",
        str(output),
        "--allow-partial-universe",
        "--top-n",
        "3",
        "--min-candidate-trades",
        "1",
        "--min-removal-trades",
        "0",
    ]

    subprocess.run(command, check=True)

    expected = {
        "p2_candidate_stability_summary.md",
        "p2_candidate_stability_results.csv",
        "p2_candidate_stability_symbol_removal.csv",
        "p2_candidate_stability_quarter_removal.csv",
        "p2_candidate_stability_year_removal.csv",
        "p2_candidate_stability_market_removal.csv",
        "p2_candidate_stability_manifest.json",
    }
    assert expected == {path.name for path in output.iterdir()}

    results = list(csv.DictReader((output / "p2_candidate_stability_results.csv").open()))
    assert 1 <= len(results) <= 3
    assert results == sorted(results, key=lambda row: (float(row["stability_score"]), float(row["base_expectancy"]), float(row["base_total_R"])), reverse=True)
    assert {"dominant_symbol", "dominant_quarter", "dominant_year", "stability_score", "candidate_verdict"} <= set(results[0])
    assert all(row["candidate_verdict"] in ALLOWED_VERDICTS for row in results)

    symbol_rows = list(csv.DictReader((output / "p2_candidate_stability_symbol_removal.csv").open()))
    assert symbol_rows
    assert {"trades", "expectancy", "total_R", "delta_expectancy", "delta_total_R", "expectancy_retention"} <= set(symbol_rows[0])

    summary = (output / "p2_candidate_stability_summary.md").read_text()
    assert "No GA" in summary
    assert "ranked by stability score" in summary
    assert "No GA, optimization, live trader" in summary

    manifest = json.loads((output / "p2_candidate_stability_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["production_modifications"] is False
    assert manifest["live_trader_modifications"] is False
    assert manifest["detector_modifications"] is False
    assert manifest["exchange_code_modifications"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["genetic_algorithm"] is False
    assert manifest["optimization"] is False
    assert manifest["machine_learning"] is False
    assert manifest["ranking_logic"] == ["stability_score", "expectancy", "total_R"]
    assert manifest["top_n"] == 3
    assert manifest["universe_consistency"]["status"] == "UNIVERSE_MISMATCH"


def test_run_rejects_partial_universe_by_default(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    candidate_input = tmp_path / "candidates.csv"
    candidate_input.write_text(
        "candidate_id,candidate_verdict,is_cluster_representative,rule_width,rule_definition,expectancy,total_R\n"
        "CAND-000001,PROMISING_SUBPOPULATIONS_FOUND,True,1,ENA(symbol in [ENA]),0.1,1.0\n"
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_p2_candidate_stability_audit",
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
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "missing required seven-market symbols" in result.stderr


def test_no_production_changes_and_no_live_trader_imports() -> None:
    tree = ast.parse(Path("research_v2/patterns/pole_p2_candidate_stability_audit.py").read_text())
    imported_modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    assert all("live_" not in module and "forward_trader" not in module for module in imported_modules)
    assert all("pnf_mvp.strategy" not in module for module in imported_modules)
