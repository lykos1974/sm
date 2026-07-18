from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle, EntryTimingObservation
from research_v2.patterns.pole_next_open_limit_fill_reality_audit import (
    ALLOWED_VERDICTS,
    COMBINED,
    EXPIRY_CANDLES,
    OUTPUT_NAMES,
    _limit_fill_result,
    _symbol_summary,
)


def _obs(direction: str = "LONG", entry: float = 100.0, stop: float | None = None) -> EntryTimingObservation:
    resolved_stop = stop if stop is not None else (97.0 if direction == "LONG" else 103.0)
    return EntryTimingObservation(
        symbol="BTC",
        row_number=2,
        direction=direction,
        entry_candidate="NEXT_COLUMN_OPEN_ENTRY",
        pole_idx=0,
        reversal_idx=1,
        confirmation_idx=2,
        box_size=1.0,
        entry=entry,
        stop=resolved_stop,
        observable_entry_ts=22,
        replay_includes_anchor=True,
        candles_in_replay=4,
        geometry_status="OBSERVABLE",
        geometry_details="synthetic",
    )


def test_long_pending_limit_fills_only_when_candle_trades_through_entry() -> None:
    result = _limit_fill_result(
        _obs("LONG"),
        [
            Candle(22, 102, 103, 101, 102),
            Candle(23, 103, 104, 99, 101),
            Candle(24, 101, 108, 101, 108),
        ],
        expiry_candles=2,
        target_r=2.0,
    )
    assert result.fill_status == "FILLED"
    assert result.fill_ts == 23
    assert result.outcome_classification == "TARGET_FIRST"
    assert result.realized_r == 2.0


def test_short_pending_limit_uses_symmetric_trade_through_and_outcome() -> None:
    result = _limit_fill_result(
        _obs("SHORT"),
        [
            Candle(22, 98, 99, 97, 98),
            Candle(23, 97, 101, 96, 99),
            Candle(24, 99, 99, 93, 93),
        ],
        expiry_candles=2,
        target_r=2.0,
    )
    assert result.fill_status == "FILLED"
    assert result.fill_ts == 23
    assert result.outcome_classification == "TARGET_FIRST"
    assert result.realized_r == 2.0


def test_expired_order_is_cancelled_and_never_fills_later() -> None:
    one_candle = _limit_fill_result(
        _obs("LONG"),
        [
            Candle(22, 102, 103, 101, 102),
            Candle(23, 103, 104, 100, 101),
            Candle(24, 101, 108, 100, 108),
        ],
        expiry_candles=1,
        target_r=2.0,
    )
    two_candles = _limit_fill_result(
        _obs("LONG"),
        [
            Candle(22, 102, 103, 101, 102),
            Candle(23, 103, 104, 101, 103),
            Candle(24, 101, 108, 100, 108),
        ],
        expiry_candles=2,
        target_r=2.0,
    )
    three_candles = _limit_fill_result(
        _obs("LONG"),
        [
            Candle(22, 102, 103, 101, 102),
            Candle(23, 103, 104, 101, 103),
            Candle(24, 101, 102, 99, 101),
        ],
        expiry_candles=3,
        target_r=2.0,
    )
    assert one_candle.fill_status == "CANCELLED_EXPIRED"
    assert one_candle.fill_ts is None
    assert one_candle.outcome_classification == "MISSED_LIMIT_FILL"
    assert two_candles.fill_status == "CANCELLED_EXPIRED"
    assert two_candles.fill_ts is None
    assert three_candles.fill_status == "FILLED"
    assert three_candles.fill_ts == 24


def test_same_candle_fill_plus_stop_is_conservative_and_explicit() -> None:
    result = _limit_fill_result(
        _obs("LONG"),
        [Candle(22, 102, 108, 96, 101), Candle(23, 101, 110, 100, 109)],
        expiry_candles=1,
        target_r=2.0,
    )
    assert result.fill_status == "FILLED"
    assert result.fill_ts == 22
    assert result.outcome_classification == "SAME_CANDLE_FILL_STOP_CONSERVATIVE"
    assert result.realized_r == -1.0


def test_same_candle_fill_plus_target_without_stop_is_not_counted_as_win() -> None:
    result = _limit_fill_result(
        _obs("LONG"),
        [Candle(22, 102, 107, 99, 106), Candle(23, 106, 108, 105, 107)],
        expiry_candles=1,
        target_r=2.0,
    )
    assert result.fill_status == "FILLED"
    assert result.outcome_classification == "SAME_CANDLE_FILL_TARGET_AMBIGUOUS"
    assert result.realized_r == ""


def test_symbol_summary_reports_required_scorecard_fields_by_expiry() -> None:
    filled = _limit_fill_result(
        _obs(),
        [Candle(22, 102, 103, 99, 101), Candle(23, 101, 108, 101, 108)],
        expiry_candles=1,
        target_r=2.0,
    )
    cancelled = _limit_fill_result(
        _obs(),
        [Candle(22, 103, 104, 101, 103), Candle(23, 103, 104, 101, 103)],
        expiry_candles=1,
        target_r=2.0,
    )
    other_expiry = _limit_fill_result(
        _obs(),
        [Candle(22, 103, 104, 101, 103), Candle(23, 103, 104, 100, 101)],
        expiry_candles=2,
        target_r=2.0,
    )
    summary = _symbol_summary(1, COMBINED, [filled, cancelled, other_expiry])
    assert summary["expiry_candles"] == 1
    assert summary["observations"] == 2
    assert summary["filled_rows"] == 1
    assert summary["cancelled_rows"] == 1
    assert summary["resolved_rows"] == 1
    assert summary["win_rate_non_ambiguous"] == 1.0
    assert summary["avg_realized_R"] == 2.0
    assert summary["total_realized_R"] == 2.0


def test_cli_outputs_three_expiry_audit_without_promotion(tmp_path: Path) -> None:
    labels = tmp_path / "labels.csv"
    columns = tmp_path / "columns.csv"
    candles = tmp_path / "candles.csv"
    labels.write_text("pattern_name,pole_column_index,reversal_column_index,opposing_pole_distance_columns,enhanced_by_opposing_pole\nLOW_POLE,0,1,3,false\n")
    columns.write_text("idx,kind,top,bottom,start_ts,end_ts,profile_name\n0,O,100,95,1,10,TEST_bs1_rev3\n1,X,100,96,11,20,TEST_bs1_rev3\n2,O,99,97,21,30,TEST_bs1_rev3\n")
    candles.write_text("close_time,open,high,low,close\n22,102,103,102,102\n23,103,104,99,101\n24,101,108,101,108\n")
    output = tmp_path / "output"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_next_open_limit_fill_reality_audit",
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
    rows = list(csv.DictReader((output / "next_open_limit_fill_reality_rows.csv").open()))
    assert {row["expiry_candles"] for row in rows} == {str(expiry) for expiry in EXPIRY_CANDLES}
    assert next(row for row in rows if row["expiry_candles"] == "1")["fill_status"] == "CANCELLED_EXPIRED"
    assert next(row for row in rows if row["expiry_candles"] == "2")["fill_status"] == "FILLED"
    symbol_rows = list(csv.DictReader((output / "next_open_limit_fill_reality_symbol_breakdown.csv").open()))
    assert {row["expiry_candles"] for row in symbol_rows if row["symbol"] == COMBINED} == {"1", "2", "3"}
    manifest = json.loads((output / "next_open_limit_fill_reality_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["live_trading_logic_modified"] is False
    assert manifest["protected_strategy_baseline_modified"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["optimization_performed"] is False
    assert manifest["expiry_candles"] == list(EXPIRY_CANDLES)
    assert manifest["allowed_verdicts"] == list(ALLOWED_VERDICTS)
    assert "PROMOTE" not in manifest["allowed_verdicts"]
    summary = (output / "next_open_limit_fill_reality_summary.md").read_text()
    assert "never outputs `PROMOTE`" in summary
    assert "If no fill occurs within 1, 2, or 3 candles" in summary
