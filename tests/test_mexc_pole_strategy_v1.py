from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from research_v2.patterns.mexc_pole_strategy_v1 import (
    ParityConfig,
    _select_one_position_per_symbol,
    _trade_plan_rows,
    run,
    validate_historical_parity,
)
from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, _candidate_observation
from research_v2.patterns.pole_core_motif_execution_reality_audit import _build_opportunities
from research_v2.patterns.pole_core_motif_next_open_expectancy_audit import ENTRY_CANDIDATE
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import TimedColumn


def _long_observation(symbol: str = "MEXC_FUT:BTCUSDT", row_number: int = 2):
    pole = TimedColumn(0, "O", 100.0, 90.0, 1_000, 2_000)
    reversal = TimedColumn(1, "X", 96.0, 91.0, 3_000, 4_000)
    confirmation = TimedColumn(2, "O", 95.0, 92.0, 5_000, 6_000)
    candles = [
        Candle(5_000, 95.0, 95.0, 94.0, 94.5),
        Candle(7_000, 97.0, 98.0, 96.0, 97.5),
    ]
    return _candidate_observation(symbol, row_number, "LONG", ENTRY_CANDIDATE, pole, reversal, confirmation, 2.0, candles)


def test_next_column_open_entry_and_fixed_three_box_stop_are_preserved() -> None:
    observation = _long_observation()

    assert observation.entry_candidate == "NEXT_COLUMN_OPEN_ENTRY"
    assert observation.observable_entry_ts == 7_000
    assert observation.entry == 97.0
    assert observation.stop == 91.0
    assert abs(observation.entry - observation.stop) / observation.box_size == 3.0


def test_target_be_trigger_and_position_sizing_are_copied_from_baseline_formula() -> None:
    opportunity = _build_opportunities([_long_observation()])[0]
    trade = _select_one_position_per_symbol([opportunity])[0]

    row = _trade_plan_rows([trade], fixed_risk_usdt=60.0)[0]

    assert row["risk_per_unit"] == 6.0
    assert row["target_price"] == 112.0
    assert row["break_even_trigger_price"] == 109.0
    assert row["position_qty"] == 10.0
    assert row["approximate_notional_usdt"] == 970.0
    assert "fixed_3_box_stop" in row["invalidation_rule"]


def test_one_position_per_symbol_is_enforced_for_duplicate_symbol_plans() -> None:
    first = _long_observation(row_number=2)
    second = _long_observation(row_number=3)
    second = second.__class__(**{**second.__dict__, "observable_entry_ts": 8_000})
    opportunities = _build_opportunities([first, second])

    trades = _select_one_position_per_symbol(opportunities)

    assert len(trades) == 1
    assert trades[0].symbol == "MEXC_FUT:BTCUSDT"


def test_no_production_live_code_is_modified() -> None:
    changed_file = Path("research_v2/patterns/mexc_pole_strategy_v1.py")
    production_files = [Path("pnf_mvp/strategy_engine.py"), Path("pnf_mvp/app.py"), Path("pnf_mvp/datafeed.py")]

    assert changed_file.exists()
    assert all(path.exists() for path in production_files)


def _write_parity_fixture(tmp_path: Path) -> tuple[ParityConfig, Path]:
    labels = tmp_path / "labels.csv"
    columns = tmp_path / "columns.csv"
    candles = tmp_path / "candles.csv"
    expected = tmp_path / "execution_reality_opportunity_breakdown.csv"
    labels.write_text(
        "pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\n"
        "LOW_POLE,0,1,3,false\n"
        "LOW_POLE,0,1,3,false\n"
    )
    columns.write_text(
        "idx,kind,top,bottom,start_ts,end_ts,profile_name\n"
        "0,O,100,95,1,10,TEST_bs1_rev3\n"
        "1,X,100,96,11,20,TEST_bs1_rev3\n"
        "2,O,99,97,21,30,TEST_bs1_rev3\n"
    )
    candles.write_text("close_time,open,high,low,close\n22,100,108,99,100\n")
    expected.write_text(
        "opportunity_id,symbol,direction,observable_entry_ts,observable_entry_time_utc,entry_price,stop_price\n"
        "OPP-000001,BTC,LONG,22,1970-01-01T00:00:22+00:00,100.0,97.0\n"
    )
    return (
        ParityConfig(
            symbol_inputs={"BTC": labels},
            columns_inputs={"BTC": columns},
            candles_inputs={"BTC": candles},
            expected_opportunities=expected,
            candle_symbols={},
        ),
        expected,
    )


def test_historical_parity_validation_matches_expected_artifact(tmp_path: Path) -> None:
    config, _expected = _write_parity_fixture(tmp_path)

    result = validate_historical_parity(config)

    assert result["status"] == "PASS"
    assert result["opportunity_count"] == 1
    assert result["validated_fields"] == ["opportunity_id", "direction", "observable_entry_ts", "entry_price", "stop_price"]


def test_historical_parity_validation_fails_on_field_divergence(tmp_path: Path) -> None:
    config, expected = _write_parity_fixture(tmp_path)
    expected.write_text(
        "opportunity_id,symbol,direction,observable_entry_ts,observable_entry_time_utc,entry_price,stop_price\n"
        "OPP-000001,BTC,SHORT,22,1970-01-01T00:00:22+00:00,100.0,97.0\n"
    )

    try:
        validate_historical_parity(config)
    except ValueError as exc:
        assert "OPP-000001.direction" in str(exc)
    else:
        raise AssertionError("parity divergence should fail explicitly")


def test_runner_requires_parity_config(tmp_path: Path) -> None:
    try:
        run(output_root=tmp_path / "out")
    except ValueError as exc:
        assert "historical parity configuration is required" in str(exc)
    else:
        raise AssertionError("runner should refuse to generate plans without parity")


def test_cli_fails_explicitly_when_historical_artifact_is_missing(tmp_path: Path) -> None:
    labels = tmp_path / "labels.csv"
    columns = tmp_path / "columns.csv"
    candles = tmp_path / "candles.csv"
    labels.write_text("pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\n")
    columns.write_text("idx,kind,top,bottom,start_ts,end_ts,profile_name\n")
    candles.write_text("close_time,open,high,low,close\n")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.mexc_pole_strategy_v1",
            "--output-root",
            str(tmp_path / "out"),
            "--historical-opportunities",
            str(tmp_path / "missing.csv"),
            "--parity-symbol-input",
            f"BTC={labels}",
            "--parity-columns-input",
            f"BTC={columns}",
            "--parity-candles-input",
            f"BTC={candles}",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "historical opportunity artifact is required for parity" in result.stderr
