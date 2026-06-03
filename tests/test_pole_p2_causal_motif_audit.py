from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_p2_causal_motif_audit import EXPECTED_SYMBOLS, load_p2_observations


def _write_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    labels = tmp_path / "labels.csv"
    labels.write_text(
        "pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\n"
        "LOW_POLE,0,1,99,true\n"
    )
    columns = tmp_path / "columns.csv"
    columns.write_text(
        "idx,kind,top,bottom,start_ts,end_ts,profile_name\n"
        "0,O,100,95,1,10,TEST_bs1_rev3\n"
        "1,X,100,96,11,20,TEST_bs1_rev3\n"
        "2,O,103,99,21,30,TEST_bs1_rev3\n"
    )
    candles = tmp_path / "candles.csv"
    candles.write_text(
        "close_time,open,high,low,close\n"
        "22,100,101,99,100\n"
        "23,100,105,99,105\n"
        "24,105,108,104,107\n"
    )
    return labels, columns, candles


def test_p2_loader_ignores_opposing_pole_fields_and_anchors_confirmation_next_open(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)

    _symbols, observations, _candles_by_symbol, flags = load_p2_observations(
        {"BTC": labels}, {"BTC": columns}, {"BTC": candles}, {}
    )

    assert flags == []
    assert len(observations) == 1
    obs = observations[0]
    assert obs.confirmation_idx == 2
    assert obs.observable_entry_ts == 22
    assert obs.entry == 100.0
    assert obs.stop == 97.0
    assert obs.geometry_status == "OBSERVABLE"


def test_cli_emits_full_seven_market_research_outputs_and_comparison_manifest(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    output = tmp_path / "out"
    command = [sys.executable, "-m", "research_v2.patterns.pole_p2_causal_motif_audit"]
    for symbol in EXPECTED_SYMBOLS:
        command.extend(["--symbol-input", f"{symbol}={labels}"])
        command.extend(["--columns-input", f"{symbol}={columns}"])
        command.extend(["--candles-input", f"{symbol}={candles}"])
    command.extend(["--output-root", str(output)])

    subprocess.run(command, check=True)

    observations = list(csv.DictReader((output / "p2_causal_motif_observations.csv").open()))
    assert len(observations) == len(EXPECTED_SYMBOLS)
    assert observations[0]["confirmation_column_index"] == "2"
    assert observations[0]["entry_price"] == "100.0"

    expectancy = list(csv.DictReader((output / "p2_causal_motif_expectancy.csv").open()))[0]
    assert expectancy["observations"] == str(len(EXPECTED_SYMBOLS))
    assert expectancy["target_first"] == str(len(EXPECTED_SYMBOLS))
    assert expectancy["expected_R"] == "2.5"

    break_even = list(csv.DictReader((output / "p2_causal_motif_break_even.csv").open()))[0]
    assert break_even["trades"] == str(len(EXPECTED_SYMBOLS))
    assert break_even["wins"] == str(len(EXPECTED_SYMBOLS))
    assert break_even["expectancy"] == "2.5"

    comparison = list(csv.DictReader((output / "p2_causal_motif_comparison.csv").open()))
    assert comparison[0]["baseline"] == "historical_non_causal_core_motif"
    assert comparison[0]["trade_count"] == "460"
    assert comparison[0]["expectancy"] == "1.654"
    assert comparison[1]["baseline"] == "causal_P4_true_birth_revalidation"
    assert comparison[1]["expectancy"] == "-0.714"

    manifest = json.loads((output / "p2_causal_motif_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["production_modifications"] is False
    assert manifest["live_trader_modifications"] is False
    assert manifest["full_seven_market_universe"] is True
    assert manifest["required_symbols"] == list(EXPECTED_SYMBOLS)
    assert manifest["ignored_fields"] == ["opposing_pole_distance_columns", "enhanced_by_opposing_pole"]
    assert manifest["knowable_at"] == "P+2:pole->reversal->confirmation"
    assert manifest["non_causal_core"] == {"trades": 460, "expectancy_R": 1.654}
    assert manifest["causal_P4_true_birth"] == {"expectancy_R": -0.714}
    assert manifest["comparison"] == [
        {"baseline": "historical_non_causal_core_motif", "trade_count": 460, "expectancy": 1.654, "delta_vs_p2_expectancy": -0.846},
        {"baseline": "causal_P4_true_birth_revalidation", "trade_count": "", "expectancy": -0.714, "delta_vs_p2_expectancy": -3.214},
    ]


def test_cli_rejects_partial_universe_by_default(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_p2_causal_motif_audit",
            "--symbol-input",
            f"BTC={labels}",
            "--columns-input",
            f"BTC={columns}",
            "--candles-input",
            f"BTC={candles}",
            "--output-root",
            str(tmp_path / "out"),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "missing required seven-market symbols" in result.stderr
