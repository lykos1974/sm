from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_next_open_robustness_audit import (
    ALLOWED_VERDICTS,
    COMBINED,
    OUTPUT_NAMES,
    _build_rows,
    _split_halves,
    _split_quartiles,
    _verdict,
)
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import _load_observations


def _write_symbol(tmp_path: Path, symbol: str, outcomes: list[str]) -> tuple[Path, Path, Path]:
    root = tmp_path / symbol
    root.mkdir()
    labels = root / "labels.csv"
    columns = root / "columns.csv"
    candles = root / "candles.csv"
    labels.write_text(
        "pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\n"
        + "".join(f"LOW_POLE,{index * 3},{index * 3 + 1},3,false\n" for index in range(len(outcomes)))
    )
    column_rows = ["idx,kind,top,bottom,start_ts,end_ts,profile_name\n"]
    candle_rows = ["close_time,open,high,low,close\n"]
    for index, outcome in enumerate(outcomes):
        base_idx = index * 3
        base_ts = 100 + index * 10
        column_rows.extend(
            [
                f"{base_idx},O,100,95,{base_ts},{base_ts + 1},TEST_bs1_rev3\n",
                f"{base_idx + 1},X,100,96,{base_ts + 2},{base_ts + 3},TEST_bs1_rev3\n",
                f"{base_idx + 2},O,99,97,{base_ts + 4},{base_ts + 5},TEST_bs1_rev3\n",
            ]
        )
        if outcome == "W":
            high, low = 109, 100
        elif outcome == "L":
            high, low = 101, 97
        else:
            high, low = 101, 99
        candle_rows.append(f"{base_ts + 5},100,100,100,100\n")
        candle_rows.append(f"{base_ts + 6},100,{high},{low},100\n")
    columns.write_text("".join(column_rows))
    candles.write_text("".join(candle_rows))
    return labels, columns, candles


def _inputs(tmp_path: Path, outcome_map: dict[str, list[str]]) -> tuple[dict[str, Path], dict[str, Path], dict[str, Path]]:
    symbol_inputs: dict[str, Path] = {}
    columns_inputs: dict[str, Path] = {}
    candles_inputs: dict[str, Path] = {}
    for symbol, outcomes in outcome_map.items():
        labels, columns, candles = _write_symbol(tmp_path, symbol, outcomes)
        symbol_inputs[symbol] = labels
        columns_inputs[symbol] = columns
        candles_inputs[symbol] = candles
    return symbol_inputs, columns_inputs, candles_inputs


def _built(tmp_path: Path, outcome_map: dict[str, list[str]]):
    symbol_inputs, columns_inputs, candles_inputs = _inputs(tmp_path, outcome_map)
    symbols, observations, candles_by_symbol = _load_observations(symbol_inputs, columns_inputs, candles_inputs, {})
    return symbols, *_build_rows(symbols, observations, candles_by_symbol), observations


def _primary(rows: list[dict[str, object]], scope: str, symbol: str, segment: str, r_target: float = 2.0) -> dict[str, object]:
    return next(row for row in rows if row["scope"] == scope and row["symbol"] == symbol and row["segment"] == segment and row["r_target"] == r_target)


def test_robust_multi_segment_positive_edge() -> None:
    outcomes = {"BTC": ["W"] * 40, "ETH": ["W"] * 40, "SOL": ["W"] * 40}
    with __import__("tempfile").TemporaryDirectory() as directory:
        symbols, symbol_rows, half_rows, quartile_rows, _flags, _observations = _built(Path(directory), outcomes)
    verdict, reason = _verdict(symbol_rows, half_rows, quartile_rows, symbols)
    assert verdict == "ROBUST"
    assert "most symbols" in reason
    assert set(ALLOWED_VERDICTS) == {"FRAGILE", "MODERATE", "ROBUST", "INSUFFICIENT_DATA"}


def test_fragile_single_symbol_dependence() -> None:
    outcomes = {"BTC": ["W"] * 20, "ETH": ["L"] * 20, "SOL": ["L"] * 20}
    with __import__("tempfile").TemporaryDirectory() as directory:
        symbols, symbol_rows, half_rows, quartile_rows, _flags, _observations = _built(Path(directory), outcomes)
    verdict, reason = _verdict(symbol_rows, half_rows, quartile_rows, symbols)
    assert verdict == "FRAGILE"
    assert "combined full-sample" in reason


def test_chronology_split_calculations(tmp_path: Path) -> None:
    symbols, _symbol_rows, half_rows, _quartile_rows, _flags, observations = _built(tmp_path, {"BTC": ["W", "W", "L", "L"]})
    assert symbols == ["BTC"]
    split = _split_halves([row for row in observations if row.symbol == "BTC"])
    assert [row.row_number for row in split["EARLY_HALF"]] == [2, 3]
    assert [row.row_number for row in split["LATE_HALF"]] == [4, 5]
    early = _primary(half_rows, "HALF", "BTC", "EARLY_HALF", 2.0)
    late = _primary(half_rows, "HALF", "BTC", "LATE_HALF", 2.0)
    assert early["observations"] == 2
    assert early["win_rate"] == 1.0
    assert early["expected_R"] == 2.0
    assert late["win_rate"] == 0.0
    assert late["expected_R"] == -1.0


def test_quartile_calculations(tmp_path: Path) -> None:
    _symbols, _symbol_rows, _half_rows, quartile_rows, _flags, observations = _built(tmp_path, {"BTC": ["W", "L", "W", "L", "W", "L", "W", "L"]})
    split = _split_quartiles([row for row in observations if row.symbol == "BTC"])
    assert {segment: [row.row_number for row in rows] for segment, rows in split.items()} == {
        "Q1": [2, 3],
        "Q2": [4, 5],
        "Q3": [6, 7],
        "Q4": [8, 9],
    }
    q1 = _primary(quartile_rows, "QUARTILE", "BTC", "Q1", 2.0)
    assert q1["observations"] == 2
    assert q1["target_first"] == 1
    assert q1["stop_first"] == 1
    assert q1["win_rate"] == 0.5
    assert q1["expected_R"] == 0.5


def test_insufficient_data_detection(tmp_path: Path) -> None:
    symbols, symbol_rows, half_rows, quartile_rows, flags, _observations = _built(tmp_path, {"BTC": ["W", "W", "W", "W"]})
    verdict, reason = _verdict(symbol_rows, half_rows, quartile_rows, symbols)
    assert verdict == "INSUFFICIENT_DATA"
    assert "very low observation counts" in reason
    assert any(row["flag"] == "VERY_LOW_OBSERVATION_COUNT" for row in flags)


def test_cli_output_generation(tmp_path: Path) -> None:
    symbol_inputs, columns_inputs, candles_inputs = _inputs(tmp_path, {"BTC": ["W"] * 6, "ETH": ["W"] * 6, "SOL": ["L"] * 6})
    output = tmp_path / "output"
    args: list[str] = []
    for symbol in ("BTC", "ETH", "SOL"):
        args.extend(["--symbol-input", f"{symbol}={symbol_inputs[symbol]}", "--columns-input", f"{symbol}={columns_inputs[symbol]}", "--candles-input", f"{symbol}={candles_inputs[symbol]}"])
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_next_open_robustness_audit", *args, "--output-root", str(output)], check=True)
    assert {path.name for path in output.iterdir()} == set(OUTPUT_NAMES)
    symbol_rows = list(csv.DictReader((output / "robustness_symbol_breakdown.csv").open()))
    btc_two_r = next(row for row in symbol_rows if row["symbol"] == "BTC" and row["r_target"] == "2.0")
    assert btc_two_r["observations"] == "6"
    assert btc_two_r["expected_R"] == "2.0"
    manifest = json.loads((output / "robustness_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["strategy_promotion"] is False
    assert manifest["optimization"] is False
    assert manifest["entry_candidate"] == "NEXT_COLUMN_OPEN_ENTRY"
    assert "PROMOTE" not in manifest["allowed_verdicts"]


def test_production_isolation() -> None:
    module = Path("research_v2/patterns/pole_core_motif_next_open_robustness_audit.py").read_text()
    assert "evaluate_pullback_retest" not in module
    assert "StrategyValidationStore" not in module
    assert "NEXT_COLUMN_OPEN_ENTRY" in module
    assert "optimization\": False" in module
