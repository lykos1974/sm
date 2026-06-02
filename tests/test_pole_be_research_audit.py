import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, EntryTimingObservation
from research_v2.patterns.pole_core_motif_execution_reality_audit import _build_opportunities
from research_v2.patterns.pole_be_research_audit import (
    ALLOWED_VERDICTS,
    BASELINE_VARIANT,
    OUTPUT_NAMES,
    _apply_verdicts,
    _execute_variant,
    _summarize_variant,
)


def _obs(row_number: int, ts: int, entry: float = 100.0, stop: float = 97.0) -> EntryTimingObservation:
    return EntryTimingObservation(
        symbol="BTC",
        row_number=row_number,
        direction="LONG",
        entry_candidate="NEXT_COLUMN_OPEN_ENTRY",
        pole_idx=0,
        reversal_idx=1,
        confirmation_idx=2,
        box_size=1.0,
        entry=entry,
        stop=stop,
        observable_entry_ts=ts,
        replay_includes_anchor=True,
        candles_in_replay=1,
        geometry_status="OBSERVABLE",
        geometry_details="synthetic",
    )


def test_be_variants_measure_loss_reduction_and_destroyed_winners() -> None:
    observations = [_obs(2, 100), _obs(3, 200), _obs(4, 300)]
    opportunities = _build_opportunities(observations)
    candles = {
        "BTC": [
            Candle(100, 100, 103.2, 100.1, 101),
            Candle(101, 101, 104, 99, 100),
            Candle(102, 100, 108, 99, 107),
            Candle(200, 100, 104, 100.1, 101),
            Candle(201, 101, 102, 96, 97),
            Candle(300, 100, 101, 96, 97),
        ]
    }

    baseline_outcomes = _execute_variant(opportunities, candles, BASELINE_VARIANT, None)
    be_outcomes = _execute_variant(opportunities, candles, "B_BE_AFTER_1R", 1.0)
    baseline = _summarize_variant(BASELINE_VARIANT, None, baseline_outcomes, None, None)
    be_row = _summarize_variant("B_BE_AFTER_1R", 1.0, be_outcomes, baseline, baseline_outcomes)
    rows = [baseline, be_row]
    verdict, reason = _apply_verdicts(rows)

    assert baseline["trades"] == 3
    assert baseline["wins"] == 1
    assert baseline["losses"] == 2
    assert baseline["expectancy"] == 0.166667
    assert be_row["trades"] == 3
    assert be_row["wins"] == 0
    assert be_row["losses"] == 1
    assert be_row["break_even_exits"] == 2
    assert be_row["loss_reduction"] == 1
    assert be_row["win_destruction_count"] == 1
    assert be_row["expectancy_delta_vs_baseline"] == -0.5
    assert rows[1]["verdict"] == "BE_HURTS"
    assert verdict == "BE_HURTS"
    assert "PROMOTE" not in ALLOWED_VERDICTS
    assert "B_BE_AFTER_1R" in reason


def _write_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    label_path = tmp_path / "labels.csv"
    column_path = tmp_path / "columns.csv"
    candle_path = tmp_path / "candles.csv"
    label_path.write_text(
        "pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\n"
        "LOW_POLE,0,1,3,false\n"
        "LOW_POLE,3,4,3,false\n"
    )
    column_path.write_text(
        "idx,kind,top,bottom,start_ts,end_ts,profile_name\n"
        "0,O,100,95,1,10,TEST_bs1_rev3\n"
        "1,X,100,96,11,20,TEST_bs1_rev3\n"
        "2,O,99,97,21,30,TEST_bs1_rev3\n"
        "3,O,100,95,31,40,TEST_bs1_rev3\n"
        "4,X,100,96,41,50,TEST_bs1_rev3\n"
        "5,O,99,97,51,60,TEST_bs1_rev3\n"
    )
    candle_path.write_text(
        "close_time,open,high,low,close\n"
        "22,100,103.1,100.1,101\n"
        "23,101,102,99,100\n"
        "24,100,108,99,107\n"
        "52,100,101,96,97\n"
    )
    return label_path, column_path, candle_path


def test_cli_writes_break_even_research_artifacts_without_promotion(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    output = tmp_path / "output"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_be_research_audit",
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

    assert {path.name for path in output.iterdir()} == set(OUTPUT_NAMES)
    rows = list(csv.DictReader((output / "be_research_variant_breakdown.csv").open()))
    baseline = next(row for row in rows if row["variant"] == BASELINE_VARIANT)
    one_r = next(row for row in rows if row["variant"] == "B_BE_AFTER_1R")
    assert baseline["trades"] == "2"
    assert baseline["wins"] == "1"
    assert baseline["losses"] == "1"
    assert one_r["break_even_exits"] == "1"
    assert one_r["win_destruction_count"] == "1"

    manifest = json.loads((output / "be_research_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["production_modifications"] is False
    assert manifest["entry"] == "NEXT_COLUMN_OPEN_ENTRY"
    assert manifest["stop"] == "fixed_3_box_stop"
    assert manifest["target_R"] == 2.5
    assert "PROMOTE" not in manifest["allowed_verdicts"]
    assert manifest["management_rules"]["tp1"] is False
    assert manifest["management_rules"]["break_even_variants"] == [None, 1.0, 1.5, 2.0]
