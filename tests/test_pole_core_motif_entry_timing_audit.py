from __future__ import annotations

import csv
import sqlite3
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, EntryTimingObservation, _classify


def _observation(candidate: str = "REVERSAL_EXTREME_TOUCH_ENTRY", anchor: int | None = 10, status: str = "OBSERVABLE") -> EntryTimingObservation:
    return EntryTimingObservation("TEST", 2, "LONG", candidate, 0, 1, 2, 1.0, 100.0, 97.0, anchor, False, 1, status, "fixture")


def test_touch_entry_target_first() -> None:
    assert _classify(_observation(), [Candle(11, 100, 103, 99, 102)], 1.0)["classification"] == "TARGET_FIRST"


def test_touch_entry_stop_first() -> None:
    assert _classify(_observation(), [Candle(11, 100, 101, 97, 98)], 1.0)["classification"] == "STOP_FIRST"


def test_same_candle_ambiguous() -> None:
    assert _classify(_observation(), [Candle(11, 100, 103, 97, 101)], 1.0)["classification"] == "SAME_CANDLE_AMBIGUOUS"


def test_current_confirmation_entry_reproduces_late_entry_failure() -> None:
    row = _observation("CURRENT_CONFIRMATION_ENTRY", anchor=20)
    candles = [Candle(19, 100, 104, 99, 103), Candle(21, 100, 101, 97, 98)]
    assert _classify(row, candles, 1.0)["classification"] == "STOP_FIRST"


def test_missing_candle_mapping_is_unknown() -> None:
    assert _classify(_observation(), [], 1.0)["classification"] == "UNKNOWN_MISSING_CANDLES"


def _write_inputs(tmp_path: Path) -> tuple[Path, Path]:
    labels = tmp_path / "labels.csv"
    labels.write_text("pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\nLOW_POLE,0,1,3,false\n")
    columns = tmp_path / "columns.csv"
    columns.write_text("idx,kind,top,bottom,start_ts,end_ts,profile_name\n0,O,100,95,1,10,TEST_bs1_rev3\n1,X,100,96,11,20,TEST_bs1_rev3\n2,O,99,97,21,30,TEST_bs1_rev3\n")
    return labels, columns


def test_output_keeps_compact_symbol_while_querying_mapped_db_symbol(tmp_path: Path) -> None:
    labels, columns = _write_inputs(tmp_path); db = tmp_path / "candles.db"; output = tmp_path / "out"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE candles (symbol TEXT, close_time INTEGER, open REAL, high REAL, low REAL, close REAL)")
        connection.executemany("INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?)", [("BINANCE_FUT:BTCUSDT", 20, 99, 100, 98, 100), ("BINANCE_FUT:BTCUSDT", 21, 100, 101, 99, 100), ("BINANCE_FUT:BTCUSDT", 22, 100, 103, 99, 102)])
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_entry_timing_audit", "--symbol-input", f"BTC={labels}", "--columns-input", f"BTC={columns}", "--candles-input", f"BTC={db}", "--candle-symbol", "BTC=BINANCE_FUT:BTCUSDT", "--output-root", str(output)], check=True)
    targets = list(csv.DictReader((output / "entry_timing_targets.csv").open()))
    assert {row["symbol"] for row in targets} == {"BTC"}
    assert {path.name for path in output.iterdir()} == {"entry_timing_summary.md", "entry_timing_observations.csv", "entry_timing_targets.csv", "entry_timing_candidate_breakdown.csv", "entry_timing_symbol_breakdown.csv", "entry_timing_flags.csv", "entry_timing_manifest.json"}


def test_production_files_untouched() -> None:
    # This research audit imports shared loaders but is isolated from production strategy modules.
    module = Path("research_v2/patterns/pole_core_motif_entry_timing_audit.py").read_text()
    assert "evaluate_pullback_retest" not in module
