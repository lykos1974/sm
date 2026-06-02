from __future__ import annotations

import csv
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

OUTPUTS = {
    "sl_c_candle_chronology_summary.md", "sl_c_candle_chronology_observations.csv",
    "sl_c_candle_chronology_symbol_breakdown.csv", "sl_c_candle_chronology_targets.csv",
    "sl_c_candle_chronology_flags.csv", "sl_c_candle_chronology_manifest.json",
}


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_labels(path: Path) -> None:
    _write_csv(path, ["pattern_name", "pole_column_index", "reversal_column_index", "opposing_pole_distance_columns", "enhanced_by_opposing_pole"], [
        {"pattern_name": "LOW_POLE", "pole_column_index": "0", "reversal_column_index": "1", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False"},
        {"pattern_name": "HIGH_POLE", "pole_column_index": "3", "reversal_column_index": "4", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False"},
        {"pattern_name": "LOW_POLE", "pole_column_index": "6", "reversal_column_index": "7", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False"},
        {"pattern_name": "LOW_POLE", "pole_column_index": "9", "reversal_column_index": "10", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False"},
    ])


def _write_columns(path: Path) -> None:
    fields = ["profile_name", "idx", "kind", "top", "bottom", "start_ts", "end_ts"]
    rows = [
        (0, "O", 13, 9, 10), (1, "X", 15, 10, 20), (2, "O", 14, 12, 30),
        (3, "X", 20, 16, 40), (4, "O", 19, 14, 50), (5, "X", 17, 15, 60),
        (6, "O", 13, 9, 70), (7, "X", 15, 10, 80), (8, "O", 14, 12, 90),
        (9, "O", 13, 9, 100), (10, "X", 15, 10, 110),
    ]
    _write_csv(path, fields, [{"profile_name": "TEST_bs1_rev3", "idx": str(idx), "kind": kind, "top": str(top), "bottom": str(bottom), "start_ts": str(ts), "end_ts": str(ts)} for idx, kind, top, bottom, ts in rows])


def _write_candles(path: Path) -> None:
    # Row 2 long: target first. Row 3 short: stop first. Row 4 long: same candle.
    # Row 5 has no confirmation column and is ANCHOR_NOT_OBSERVABLE.
    _write_csv(path, ["close_time", "high", "low"], [
        {"close_time": "31", "high": "18", "low": "14"},
        {"close_time": "61", "high": "18", "low": "14"},
        {"close_time": "91", "high": "18", "low": "11"},
    ])


def _args(tmp_path: Path, *, candles: Path | None = None) -> tuple[list[str], Path]:
    labels, columns = tmp_path / "labels.csv", tmp_path / "columns.csv"
    candles = candles or tmp_path / "candles.csv"
    _write_labels(labels)
    _write_columns(columns)
    if not candles.exists() and candles.name == "candles.csv":
        _write_candles(candles)
    output = tmp_path / "output"
    return ["--symbol-input", f"TEST={labels}", "--columns-input", f"TEST={columns}", "--candles-input", f"TEST={candles}", "--output-root", str(output)], output


def test_generates_outputs_and_classifies_chronological_ordering(tmp_path: Path) -> None:
    args, output = _args(tmp_path)
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_sl_c_candle_chronology", *args], check=True)
    assert {path.name for path in output.iterdir()} == OUTPUTS
    targets = list(csv.DictReader((output / "sl_c_candle_chronology_targets.csv").open()))
    one_r = {(row["row_number"], row["r_target"]): row["classification"] for row in targets}
    assert one_r[("2", "1.0")] == "TARGET_FIRST"
    assert one_r[("3", "1.0")] == "STOP_FIRST"
    assert one_r[("4", "1.0")] == "SAME_CANDLE_AMBIGUOUS"
    assert one_r[("5", "1.0")] == "ANCHOR_NOT_OBSERVABLE"
    summary = (output / "sl_c_candle_chronology_summary.md").read_text()
    assert "**DISCARD_SL_C_TRADABLE_INTERPRETATION**" in summary
    assert "Never `PROMOTE`" in summary
    manifest = json.loads((output / "sl_c_candle_chronology_manifest.json").read_text())
    assert manifest["research_only"] is True


def test_missing_user_path_fails_cleanly_without_outputs(tmp_path: Path) -> None:
    missing = tmp_path / "missing.csv"
    args, output = _args(tmp_path, candles=missing)
    result = subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_sl_c_candle_chronology", *args], text=True, capture_output=True)
    assert result.returncode != 0
    assert "missing candles-input path(s)" in result.stderr
    assert not output.exists()


def test_refuses_to_overwrite_existing_outputs(tmp_path: Path) -> None:
    args, output = _args(tmp_path)
    output.mkdir()
    (output / "sl_c_candle_chronology_targets.csv").write_text("keep me\n")
    result = subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_sl_c_candle_chronology", *args], text=True, capture_output=True)
    assert result.returncode != 0
    assert "refusing to overwrite existing chronology output" in result.stderr
    assert (output / "sl_c_candle_chronology_targets.csv").read_text() == "keep me\n"


def test_unknown_and_not_reached_classifications_are_deterministic() -> None:
    from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import Candle, ChronologyObservation, _classify

    def observation(status: str, anchor: int | None = 10) -> ChronologyObservation:
        return ChronologyObservation("TEST", 2, "LONG", 0, 1, 2, 1.0, 100.0, 97.0, anchor, 0, status, "fixture status")

    assert _classify(observation("OBSERVABLE"), [Candle(11, 101.0, 99.0)], 1.0)["classification"] == "NOT_REACHED"
    assert _classify(observation("OBSERVABLE"), [], 1.0)["classification"] == "UNKNOWN_MISSING_CANDLES"
    assert _classify(observation("UNKNOWN_UNMAPPABLE_COLUMN_TIME", None), [], 1.0)["classification"] == "UNKNOWN_UNMAPPABLE_COLUMN_TIME"


def test_loads_explicit_sqlite_candles_input(tmp_path: Path) -> None:
    from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import _load_candles

    db_path = tmp_path / "candles.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE candles (symbol TEXT, close_time INTEGER, high REAL, low REAL)")
        connection.executemany("INSERT INTO candles VALUES (?, ?, ?, ?)", [("OTHER", 1, 9, 8), ("TEST", 3, 12, 10), ("TEST", 2, 11, 9)])
    candles = _load_candles(db_path, "TEST")
    assert [(candle.ts, candle.high, candle.low) for candle in candles] == [(2, 11.0, 9.0), (3, 12.0, 10.0)]


def _write_symbol_db(path: Path, symbol: str) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE candles (symbol TEXT, close_time INTEGER, high REAL, low REAL)")
        connection.executemany("INSERT INTO candles VALUES (?, ?, ?, ?)", [
            (symbol, 31, 18, 14),
            (symbol, 61, 18, 14),
            (symbol, 91, 18, 11),
        ])


def _db_args(tmp_path: Path, db_path: Path) -> tuple[list[str], Path]:
    labels, columns = tmp_path / "labels.csv", tmp_path / "columns.csv"
    _write_labels(labels)
    _write_columns(columns)
    output = tmp_path / "output"
    return ["--symbol-input", f"BTC={labels}", "--columns-input", f"BTC={columns}", "--candles-input", f"BTC={db_path}", "--output-root", str(output)], output


def test_candle_symbol_mapping_loads_db_symbol_without_changing_research_symbol(tmp_path: Path) -> None:
    db_path = tmp_path / "candles.db"
    _write_symbol_db(db_path, "BINANCE_FUT:BTCUSDT")
    args, output = _db_args(tmp_path, db_path)
    subprocess.run([
        sys.executable, "-m", "research_v2.patterns.pole_core_motif_sl_c_candle_chronology",
        *args, "--candle-symbol", "BTC=BINANCE_FUT:BTCUSDT",
    ], check=True)
    targets = list(csv.DictReader((output / "sl_c_candle_chronology_targets.csv").open()))
    assert {row["symbol"] for row in targets} == {"BTC"}
    assert next(row for row in targets if row["row_number"] == "2" and row["r_target"] == "1.0")["classification"] == "TARGET_FIRST"


def test_missing_candle_symbol_mapping_preserves_unknown_missing_candles(tmp_path: Path) -> None:
    db_path = tmp_path / "candles.db"
    _write_symbol_db(db_path, "BINANCE_FUT:BTCUSDT")
    args, output = _db_args(tmp_path, db_path)
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_sl_c_candle_chronology", *args], check=True)
    targets = list(csv.DictReader((output / "sl_c_candle_chronology_targets.csv").open()))
    assert next(row for row in targets if row["row_number"] == "2" and row["r_target"] == "1.0")["classification"] == "UNKNOWN_MISSING_CANDLES"
