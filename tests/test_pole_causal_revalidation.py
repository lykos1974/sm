from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_causal_revalidation import load_causal_observations


def _write_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    labels = tmp_path / "labels.csv"
    labels.write_text(
        "pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\n"
        "LOW_POLE,0,1,3,false\n"
    )
    columns = tmp_path / "columns.csv"
    columns.write_text(
        "idx,kind,top,bottom,start_ts,end_ts,profile_name\n"
        "0,O,100,95,1,10,TEST_bs1_rev3\n"
        "1,X,100,96,11,20,TEST_bs1_rev3\n"
        "2,O,99,97,21,30,TEST_bs1_rev3\n"
        "3,X,104,98,31,40,TEST_bs1_rev3\n"
        "4,O,103,99,41,50,TEST_bs1_rev3\n"
    )
    candles = tmp_path / "candles.csv"
    candles.write_text(
        "close_time,open,high,low,close\n"
        "22,100,101,99,100\n"
        "42,120,128,121,127\n"
    )
    return labels, columns, candles


def test_causal_observation_anchors_next_open_after_true_birth_column(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)

    _symbols, observations, _candles_by_symbol, flags = load_causal_observations(
        {"BTC": labels}, {"BTC": columns}, {"BTC": candles}, {}
    )

    assert flags == []
    assert len(observations) == 1
    causal = observations[0]
    obs = causal.observation
    assert causal.legacy_confirmation_idx == 2
    assert causal.opposing_pole_idx == 3
    assert causal.motif_birth_idx == 4
    assert obs.confirmation_idx == 4
    assert obs.observable_entry_ts == 42
    assert obs.entry == 120.0
    assert obs.stop == 117.0
    assert obs.geometry_status == "OBSERVABLE"


def test_cli_revalidates_expectancy_execution_and_break_even_from_causal_birth(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    output = tmp_path / "out"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_causal_revalidation",
            "--symbol-input",
            f"BTC={labels}",
            "--columns-input",
            f"BTC={columns}",
            "--candles-input",
            f"BTC={candles}",
            "--output-root",
            str(output),
        ],
        check=True,
    )

    observations = list(csv.DictReader((output / "causal_revalidation_observations.csv").open()))
    assert observations[0]["legacy_confirmation_column_index"] == "2"
    assert observations[0]["motif_birth_column_index"] == "4"
    assert observations[0]["entry_price"] == "120.0"

    expectancy = list(csv.DictReader((output / "causal_revalidation_expectancy.csv").open()))[0]
    assert expectancy["observations"] == "1"
    assert expectancy["target_first"] == "1"
    assert expectancy["expected_R"] == "2.5"

    execution = list(csv.DictReader((output / "causal_revalidation_execution_model.csv").open()))[0]
    assert execution["unique_opportunities"] == "1"
    assert execution["total_R"] == "2.5"

    break_even = list(csv.DictReader((output / "causal_revalidation_break_even.csv").open()))[0]
    assert break_even["variant"] == "BREAK_EVEN_AFTER_2R"
    assert break_even["wins"] == "1"
    assert break_even["expectancy"] == "2.5"

    manifest = json.loads((output / "causal_revalidation_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["production_modifications"] is False
    assert manifest["motif_birth"] == "opposing_pole_discovery:first_pole_index+4"
    assert manifest["verdict"] == "EDGE_SURVIVES_CAUSAL_SAMPLE"
