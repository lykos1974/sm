from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, EntryTimingObservation
from research_v2.patterns.pole_core_motif_execution_reality_audit import _build_opportunities
from research_v2.patterns.pole_execution_model_v1_audit import (
    ALLOWED_VERDICTS,
    COMBINED,
    OUTPUT_NAMES,
    _concurrency_rows,
    _duration_rows,
    _execute_opportunities,
    _percentile_90,
    _trade_stat_rows,
    _verdict,
)


def _obs(symbol: str, row_number: int, ts: int, entry: float = 100.0, stop: float = 97.0) -> EntryTimingObservation:
    return EntryTimingObservation(
        symbol=symbol,
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


def test_trade_stats_duration_and_cross_symbol_risk_for_fixed_2_5r_model() -> None:
    observations = [_obs("BTC", 2, 100), _obs("BTC", 3, 200), _obs("ETH", 2, 100)]
    opportunities = _build_opportunities(observations)
    candles = {
        "BTC": [
            Candle(100, 100, 101, 99, 100),
            Candle(101, 100, 108, 99, 100),
            Candle(200, 100, 101, 99, 100),
            Candle(201, 100, 101, 96, 100),
        ],
        "ETH": [Candle(100, 100, 108, 99, 100)],
    }
    executed = _execute_opportunities(opportunities, candles)

    combined = next(row for row in _trade_stat_rows(["BTC", "ETH"], executed) if row["symbol"] == COMBINED)
    assert combined["trades"] == 3
    assert combined["wins"] == 2
    assert combined["losses"] == 1
    assert combined["win_rate"] == 0.666667
    assert combined["expectancy"] == 1.333333
    assert combined["total_R"] == 4.0

    duration = next(row for row in _duration_rows(["BTC", "ETH"], executed) if row["symbol"] == COMBINED)
    assert duration["median_bars_in_trade"] == 2.0
    assert duration["mean_bars_in_trade"] == 1.666667
    assert duration["p90_bars_in_trade"] == 2.0
    assert duration["max_bars_in_trade"] == 2

    concurrency = _concurrency_rows(["BTC", "ETH"], executed)
    btc = next(row for row in concurrency if row["symbol"] == "BTC")
    combined_concurrency = next(row for row in concurrency if row["symbol"] == COMBINED)
    assert btc["maximum_simultaneous_trades"] == 1
    assert btc["one_position_per_symbol_feasible"] is True
    assert combined_concurrency["max_concurrent_positions"] == 2
    assert combined_concurrency["peak_active_risk_R"] == 2.0


def test_verdicts_are_execution_only_and_never_promote() -> None:
    assert "PROMOTE" not in ALLOWED_VERDICTS
    ready_trade_rows = [{"symbol": COMBINED, "trades": 2, "ambiguous": 0, "not_reached": 0, "unknown": 0}]
    ready_concurrency = [{"scope": "SYMBOL", "symbol": "BTC", "maximum_simultaneous_trades": 1}]
    assert _verdict(ready_trade_rows, ready_concurrency)[0] == "EXECUTION_READY"

    complex_concurrency = [{"scope": "SYMBOL", "symbol": "BTC", "maximum_simultaneous_trades": 2}]
    assert _verdict(ready_trade_rows, complex_concurrency)[0] == "EXECUTION_COMPLEX"

    uncertain_trade_rows = [{"symbol": COMBINED, "trades": 2, "ambiguous": 1, "not_reached": 0, "unknown": 0}]
    assert _verdict(uncertain_trade_rows, ready_concurrency)[0] == "EXECUTION_UNCERTAIN"


def test_p90_uses_nearest_rank() -> None:
    assert _percentile_90([1, 2, 3, 4, 5]) == 5.0
    assert _percentile_90([]) == ""


def _write_fixture(tmp_path: Path, symbol: str, high: float, low: float) -> tuple[Path, Path, Path]:
    root = tmp_path / symbol
    root.mkdir()
    label_path = root / "labels.csv"
    column_path = root / "columns.csv"
    candle_path = root / "candles.csv"
    label_path.write_text("pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\nLOW_POLE,0,1,3,false\n")
    column_path.write_text("idx,kind,top,bottom,start_ts,end_ts,profile_name\n0,O,100,95,1,10,TEST_bs1_rev3\n1,X,100,96,11,20,TEST_bs1_rev3\n2,O,99,97,21,30,TEST_bs1_rev3\n")
    candle_path.write_text(f"close_time,open,high,low,close\n22,100,{high},{low},100\n")
    return label_path, column_path, candle_path


def test_cli_writes_execution_model_v1_artifacts_without_strategy_changes(tmp_path: Path) -> None:
    fixtures = {"BTC": _write_fixture(tmp_path, "BTC", 108, 99), "ETH": _write_fixture(tmp_path, "ETH", 101, 96)}
    args: list[str] = []
    for symbol, (labels, columns, candles) in fixtures.items():
        args.extend(["--symbol-input", f"{symbol}={labels}", "--columns-input", f"{symbol}={columns}", "--candles-input", f"{symbol}={candles}"])
    output = tmp_path / "output"
    args.extend(["--output-root", str(output)])

    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_execution_model_v1_audit", *args], check=True)

    assert {path.name for path in output.iterdir()} == set(OUTPUT_NAMES)
    trade_rows = list(csv.DictReader((output / "execution_model_v1_trade_stats.csv").open()))
    combined = next(row for row in trade_rows if row["symbol"] == COMBINED)
    assert combined["trades"] == "2"
    assert combined["wins"] == "1"
    assert combined["losses"] == "1"
    assert combined["expectancy"] == "0.75"

    concurrency_rows = list(csv.DictReader((output / "execution_model_v1_concurrency_stats.csv").open()))
    combined_concurrency = next(row for row in concurrency_rows if row["symbol"] == COMBINED)
    assert combined_concurrency["peak_active_risk_R"] == "2.0"

    manifest = json.loads((output / "execution_model_v1_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["optimization_performed"] is False
    assert manifest["entry"] == "NEXT_COLUMN_OPEN_ENTRY"
    assert manifest["target_R"] == 2.5
    assert "PROMOTE" not in manifest["allowed_verdicts"]

    module_text = Path("research_v2/patterns/pole_execution_model_v1_audit.py").read_text()
    assert "evaluate_pullback_retest" not in module_text
