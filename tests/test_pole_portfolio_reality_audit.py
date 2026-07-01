import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_portfolio_reality_audit import (
    ALLOWED_VERDICTS,
    OUTPUT_NAMES,
    PortfolioTrade,
    _apply_one_position_per_symbol,
    _equity_curve,
    _exposure_summary,
    _money_equity_curve,
    _monthly_money_rows,
    _max_streak,
    _period_rows,
    _risk_flags,
    _symbol_rows,
    _verdict,
)


def _trade(
    symbol: str,
    entry: int,
    exit_: int,
    result: float,
    active: int = 0,
    trade_id: str | None = None,
) -> PortfolioTrade:
    return PortfolioTrade(
        trade_id=trade_id or f"{symbol}-{entry}",
        opportunity_id=f"OPP-{symbol}-{entry}",
        symbol=symbol,
        direction="LONG",
        entry_ts=entry,
        exit_ts=exit_,
        classification="TARGET_FIRST" if result > 0 else "BREAK_EVEN_EXIT" if result == 0 else "STOP_FIRST",
        result_r=result,
        active_positions_at_entry=active,
        active_risk_r_at_entry=float(active),
    )


def test_equity_curve_construction_and_max_drawdown() -> None:
    trades = [_trade("BTC", 1, 10, 2.5), _trade("ETH", 2, 20, -1.0), _trade("SOL", 3, 30, -1.0), _trade("BTC", 31, 40, 2.5)]

    rows, summary = _equity_curve(trades)

    assert [row["cumulative_R"] for row in rows] == [2.5, 1.5, 0.5, 3.0]
    assert [row["drawdown_R"] for row in rows] == [0.0, 1.0, 2.0, 0.0]
    assert summary["total_R"] == 3.0
    assert summary["average_R_per_trade"] == 0.75
    assert summary["median_R_per_trade"] == 0.75
    assert summary["max_drawdown_R"] == 2.0
    assert summary["max_drawdown_percent_of_peak_R"] == 66.666667
    assert summary["recovery_time_after_drawdown"] == 30


def test_optional_money_equity_curve_uses_existing_trade_sequence() -> None:
    trades = [_trade("BTC", 1, 10, 2.5), _trade("ETH", 2, 20, -1.0), _trade("SOL", 3, 30, 0.0)]

    rows, summary = _money_equity_curve(trades, initial_capital_usdt=1000.0, fixed_position_size_usdt=50.0)

    assert [row["pnl_usdt"] for row in rows] == [125.0, -50.0, 0.0]
    assert [row["equity_usdt"] for row in rows] == [1125.0, 1075.0, 1075.0]
    assert [row["drawdown_usdt"] for row in rows] == [0.0, 50.0, 50.0]
    assert summary == {
        "initial_capital_usdt": 1000.0,
        "fixed_position_size_usdt": 50.0,
        "final_equity_usdt": 1075.0,
        "total_pnl_usdt": 75.0,
        "max_drawdown_usdt": 50.0,
        "max_drawdown_percent": 4.444444,
    }


def test_optional_monthly_money_returns_follow_money_equity_rows() -> None:
    jan = 1704067200
    feb = 1706745600
    trades = [_trade("BTC", jan - 1, jan, 2.5), _trade("ETH", feb - 1, feb, -1.0)]
    money_rows, _ = _money_equity_curve(trades, initial_capital_usdt=1000.0, fixed_position_size_usdt=50.0)

    assert _monthly_money_rows(money_rows, initial_capital_usdt=1000.0) == [
        {
            "month": "2024-01",
            "starting_equity_usdt": 1000.0,
            "ending_equity_usdt": 1125.0,
            "pnl_usdt": 125.0,
            "return_percent": 12.5,
            "trades": 1,
        },
        {
            "month": "2024-02",
            "starting_equity_usdt": 1125.0,
            "ending_equity_usdt": 1075.0,
            "pnl_usdt": -50.0,
            "return_percent": -4.444444,
            "trades": 1,
        },
    ]


def test_losing_flat_and_non_winning_streaks() -> None:
    results = [2.5, -1.0, -1.0, 0.0, 0.0, -1.0, 2.5]

    assert _max_streak(results, lambda value: value < 0) == 2
    assert _max_streak(results, lambda value: value == 0) == 2
    assert _max_streak(results, lambda value: value <= 0) == 5


def test_concurrent_exposure_and_one_position_per_symbol_filter() -> None:
    outcomes = [
        {"opportunity": type("Opp", (), {"opportunity_id": "OPP-1"})(), "symbol": "BTC", "direction": "LONG", "entry_ts": 10, "exit_ts": 30, "classification": "TARGET_FIRST", "result_r": 2.5},
        {"opportunity": type("Opp", (), {"opportunity_id": "OPP-2"})(), "symbol": "ETH", "direction": "LONG", "entry_ts": 20, "exit_ts": 40, "classification": "STOP_FIRST", "result_r": -1.0},
        {"opportunity": type("Opp", (), {"opportunity_id": "OPP-3"})(), "symbol": "BTC", "direction": "LONG", "entry_ts": 25, "exit_ts": 50, "classification": "TARGET_FIRST", "result_r": 2.5},
    ]

    trades, flags = _apply_one_position_per_symbol(outcomes)
    exposure = _exposure_summary(trades)

    assert [trade.symbol for trade in trades] == ["BTC", "ETH"]
    assert trades[0].active_positions_at_entry == 0
    assert trades[1].active_positions_at_entry == 1
    assert exposure["median_concurrent_positions"] == 1.5
    assert exposure["p90_concurrent_positions"] == 2.0
    assert exposure["max_concurrent_positions"] == 2
    assert exposure["average_active_risk_R"] == 1.5
    assert exposure["peak_active_risk_R"] == 2.0
    assert flags[0]["flag"] == "SAME_SYMBOL_OVERLAP_SKIPPED"


def test_symbol_contribution() -> None:
    trades = [_trade("BTC", 1, 10, 2.5), _trade("BTC", 11, 20, 0.0), _trade("ETH", 2, 12, -1.0)]

    rows = _symbol_rows(trades, total_r=1.5)
    btc = next(row for row in rows if row["symbol"] == "BTC")
    eth = next(row for row in rows if row["symbol"] == "ETH")

    assert btc["trades"] == 2
    assert btc["wins"] == 1
    assert btc["losses"] == 0
    assert btc["BE_exits"] == 1
    assert btc["total_R"] == 2.5
    assert btc["expectancy_R"] == 1.25
    assert btc["contribution_percentage"] == 166.666667
    assert eth["losses"] == 1


def test_monthly_and_quarterly_grouping() -> None:
    jan = 1704067200  # 2024-01-01T00:00:00Z
    apr = 1711929600  # 2024-04-01T00:00:00Z
    trades = [_trade("BTC", jan - 1, jan, 2.5), _trade("ETH", jan, jan + 10, -1.0), _trade("SOL", apr - 1, apr, 0.0)]

    monthly = _period_rows(trades, "month")
    quarterly = _period_rows(trades, "quarter")

    assert monthly == [
        {"period": "2024-01", "trades": 2, "total_R": 1.5, "win_rate": 0.5, "max_losing_streak": 1},
        {"period": "2024-04", "trades": 1, "total_R": 0.0, "win_rate": 0.0, "max_losing_streak": 0},
    ]
    assert quarterly == [
        {"period": "2024-Q1", "trades": 2, "total_R": 1.5, "win_rate": 0.5, "max_losing_streak": 1},
        {"period": "2024-Q2", "trades": 1, "total_R": 0.0, "win_rate": 0.0, "max_losing_streak": 0},
    ]


def test_verdict_logic_allows_only_portfolio_research_verdicts() -> None:
    assert "PROMOTE" not in ALLOWED_VERDICTS
    few_trades = [_trade("BTC", 1, 2, 2.5)]
    _, few_equity = _equity_curve(few_trades)
    assert _verdict(few_trades, few_equity, []) == ("INSUFFICIENT_DATA", "fewer than 30 resolved portfolio trades are available")

    robust = [_trade("BTC", i, i + 1, 2.5 if i % 2 == 0 else -1.0) for i in range(40)]
    _, robust_equity = _equity_curve(robust)
    assert _verdict(robust, robust_equity, [])[0] == "PORTFOLIO_READY_RESEARCH"

    fragile = [_trade("BTC", i, i + 1, -1.0) for i in range(40)]
    _, fragile_equity = _equity_curve(fragile)
    assert _verdict(fragile, fragile_equity, [])[0] == "PORTFOLIO_FRAGILE"

    risky_flags = [{"flag": "FREQUENT_EXPOSURE_OVER_2R", "severity": "MEDIUM", "details": "synthetic"}]
    assert _verdict(robust, robust_equity, risky_flags)[0] == "PORTFOLIO_PROMISING_BUT_RISKY"


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
        "22,100,106.1,100.1,101\n"
        "23,101,108,101,107\n"
        "24,106,108,100,107\n"
        "52,100,106.1,100.1,101\n"
        "53,101,102,100,100\n"
    )
    return label_path, column_path, candle_path


def test_cli_outputs_research_only_artifacts_and_preserves_production_isolation(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    output = tmp_path / "output"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_portfolio_reality_audit",
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
    assert not (output / "equity_curve_usdt.csv").exists()
    assert not (output / "monthly_returns_usdt.csv").exists()
    trades = list(csv.DictReader((output / "portfolio_reality_trade_sequence.csv").open()))
    assert len(trades) == 2
    assert trades[0]["result_R"] == "2.5"
    assert trades[1]["result_R"] == "0.0"

    manifest = json.loads((output / "portfolio_reality_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["not_production"] is True
    assert manifest["production_modifications"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["entry"] == "NEXT_COLUMN_OPEN_ENTRY"
    assert manifest["stop"] == "fixed_3_box_stop"
    assert manifest["target_R"] == 2.5
    assert manifest["break_even_after_R"] == 2.0
    assert manifest["management_rules"] == {"pyramiding": False, "scaling": False, "tp1": False, "tp2": False, "trailing": False}
    assert "PROMOTE" not in manifest["allowed_verdicts"]

    source = Path("research_v2/patterns/pole_portfolio_reality_audit.py").read_text()
    assert "live_binance_forward_trader" not in source
    assert "strategy_historical_backfill" not in source


def test_cli_money_simulation_adds_usdt_artifacts_without_changing_research_outputs(tmp_path: Path) -> None:
    labels, columns, candles = _write_fixture(tmp_path)
    output = tmp_path / "output"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_portfolio_reality_audit",
            "--symbol-input",
            f"BTC={labels}",
            "--columns-input",
            f"BTC={columns}",
            "--candles-input",
            f"BTC={candles}",
            "--output-root",
            str(output),
            "--initial-capital",
            "1000",
            "--fixed-position-size",
            "50",
        ],
        check=True,
    )

    assert {path.name for path in output.iterdir()} == {*OUTPUT_NAMES, "equity_curve_usdt.csv", "monthly_returns_usdt.csv"}
    summary = (output / "portfolio_reality_summary.md").read_text()
    assert "- `final_equity_usdt`: 1125.0" in summary
    assert "- `total_pnl_usdt`: 125.0" in summary
    assert "- `max_drawdown_usdt`: 0.0" in summary
    assert "- `max_drawdown_percent`: 0.0" in summary

    money_rows = list(csv.DictReader((output / "equity_curve_usdt.csv").open()))
    assert [row["pnl_usdt"] for row in money_rows] == ["125.0", "0.0"]
    assert [row["equity_usdt"] for row in money_rows] == ["1125.0", "1125.0"]

    monthly_rows = list(csv.DictReader((output / "monthly_returns_usdt.csv").open()))
    assert monthly_rows == [
        {
            "month": "1970-01",
            "starting_equity_usdt": "1000.0",
            "ending_equity_usdt": "1125.0",
            "pnl_usdt": "125.0",
            "return_percent": "12.5",
            "trades": "2",
        }
    ]

    manifest = json.loads((output / "portfolio_reality_manifest.json").read_text())
    assert manifest["verdict"] == "INSUFFICIENT_DATA"
    assert manifest["money_simulation"] == {
        "enabled": True,
        "initial_capital_usdt": 1000.0,
        "fixed_position_size_usdt": 50.0,
        "summary": {
            "final_equity_usdt": 1125.0,
            "total_pnl_usdt": 125.0,
            "max_drawdown_usdt": 0.0,
            "max_drawdown_percent": 0.0,
        },
        "artifacts": ["equity_curve_usdt.csv", "monthly_returns_usdt.csv"],
    }
