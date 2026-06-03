import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_baseline_stability_audit import (
    ALLOWED_VERDICTS,
    OUTPUT_NAMES,
    StabilityTrade,
    _concentration,
    _drawdown,
    _group_rows,
    _load_trades,
    _rolling_rows,
    _verdict,
)


def _trade(symbol: str, market: str, exit_ts: int, result_r: float, trade_id: str | None = None) -> StabilityTrade:
    return StabilityTrade(
        trade_id=trade_id or f"T-{symbol}-{exit_ts}",
        symbol=symbol,
        market_family=market,
        entry_ts=exit_ts - 1,
        exit_ts=exit_ts,
        classification="TARGET_FIRST" if result_r > 0 else "BREAK_EVEN_EXIT" if result_r == 0 else "STOP_FIRST",
        result_r=result_r,
    )


def test_market_grouping_uses_explicit_market_and_symbol_prefix(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    path.write_text(
        "trade_id,symbol,market_family,entry_timestamp,exit_timestamp,classification,result_R\n"
        "1,BTC,BINANCE,1,2,TARGET_FIRST,2.5\n"
        "2,MEXC:ETH,,3,4,STOP_FIRST,-1\n"
        "3,SOL,MEXC,5,6,BREAK_EVEN_EXIT,0\n"
    )

    trades = _load_trades(path)
    rows = _group_rows(trades, "market")

    assert [row["market_family"] for row in rows] == ["BINANCE", "MEXC"]
    assert rows[0]["trades"] == 1
    assert rows[1]["trades"] == 2
    assert rows[1]["BE_exits"] == 1


def test_yearly_grouping() -> None:
    trades = [
        _trade("BTC", "BINANCE", 1704067200, 2.5),  # 2024
        _trade("ETH", "MEXC", 1735689600, -1.0),  # 2025
        _trade("SOL", "MEXC", 1735689610, 0.0),
    ]

    rows = _group_rows(trades, "year")

    assert rows == [
        {"period": "2024", "trades": 1, "wins": 1, "losses": 0, "BE_exits": 0, "expectancy_R": 2.5, "total_R": 2.5, "win_rate": 1.0, "BE_rate": 0.0, "max_drawdown_R": 0.0, "longest_losing_streak": 0},
        {"period": "2025", "trades": 2, "wins": 0, "losses": 1, "BE_exits": 1, "expectancy_R": -0.5, "total_R": -1.0, "win_rate": 0.0, "BE_rate": 0.5, "max_drawdown_R": 1.0, "longest_losing_streak": 1},
    ]


def test_quarterly_grouping() -> None:
    trades = [
        _trade("BTC", "BINANCE", 1704067200, 2.5),  # 2024-Q1
        _trade("ETH", "MEXC", 1711929600, -1.0),  # 2024-Q2
        _trade("SOL", "MEXC", 1711929610, 2.5),
    ]

    rows = _group_rows(trades, "quarter")

    assert [row["period"] for row in rows] == ["2024-Q1", "2024-Q2"]
    assert rows[0]["expectancy_R"] == 2.5
    assert rows[1]["trades"] == 2
    assert rows[1]["total_R"] == 1.5
    assert rows[1]["win_rate"] == 0.5


def test_rolling_metrics() -> None:
    trades = [_trade("BTC", "BINANCE", i, result) for i, result in enumerate([2.5, -1.0, 0.0, 2.5], start=1)]

    rows = _rolling_rows(trades, 2)

    assert rows[0]["window_start_sequence"] == 1
    assert rows[0]["window_end_sequence"] == 2
    assert rows[0]["expectancy_R"] == 0.75
    assert rows[1]["total_R"] == -1.0
    assert rows[2]["win_rate"] == 0.5


def test_drawdown_calculation() -> None:
    trades = [_trade("BTC", "BINANCE", i, result) for i, result in enumerate([2.5, -1.0, -1.0, 2.5, -1.0], start=1)]

    assert _drawdown(trades) == 2.0


def test_concentration_calculation() -> None:
    rows = [
        {"symbol": "BTC", "total_R": 70.0},
        {"symbol": "ETH", "total_R": 20.0},
        {"symbol": "SOL", "total_R": 10.0},
    ]

    assert _concentration(100.0, rows, "symbol", "symbol") == {
        "scope": "symbol",
        "top": "BTC",
        "top_contribution_percentage": 70.0,
        "classification": "highly_concentrated",
    }


def test_verdict_logic_allowed_outputs_only() -> None:
    assert "PROMOTE" not in ALLOWED_VERDICTS
    trades = [_trade("BTC", "BINANCE", i, 2.5) for i in range(30)]

    assert _verdict(trades, [{"flag": "RESEARCH_ONLY", "severity": "INFO"}])[0] == "STABLE_BASELINE"
    assert _verdict(trades, [{"flag": "MISSING_MEXC_GROUP", "severity": "MEDIUM"}])[0] == "MOSTLY_STABLE"
    assert _verdict(trades, [{"flag": "ROLLING_EXPECTANCY_COLLAPSE", "severity": "HIGH"}])[0] == "FRAGILE_OVER_TIME"
    assert _verdict(trades, [{"flag": "HIGH_CONCENTRATION", "severity": "HIGH"}])[0] == "CONCENTRATED_EDGE"
    assert _verdict(trades[:3], [])[0] == "INSUFFICIENT_DATA"


def test_cli_outputs_research_only_artifacts_and_preserves_production_isolation(tmp_path: Path) -> None:
    trade_path = tmp_path / "portfolio_reality_trade_sequence.csv"
    with trade_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["trade_id", "symbol", "market_family", "entry_timestamp", "exit_timestamp", "classification", "result_R"])
        writer.writeheader()
        for i in range(36):
            market = "BINANCE" if i % 2 == 0 else "MEXC"
            symbol = ["BTC", "ETH", "SOL", "ENA"][i % 4]
            result = 2.5 if i % 3 != 0 else -1.0
            writer.writerow({"trade_id": f"T{i}", "symbol": symbol, "market_family": market, "entry_timestamp": 1704067200 + i, "exit_timestamp": 1704068200 + i, "classification": "TARGET_FIRST" if result > 0 else "STOP_FIRST", "result_R": result})
    log_path = tmp_path / "research_results_log.md"
    log_path.write_text("# PnF Strategy Research Results Log\n\n---\n")
    output = tmp_path / "output"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_baseline_stability_audit",
            "--trade-sequence",
            str(trade_path),
            "--output-root",
            str(output),
            "--rolling-window",
            "6",
            "--research-log",
            str(log_path),
        ],
        check=True,
    )

    assert {path.name for path in output.iterdir()} == set(OUTPUT_NAMES)
    manifest = json.loads((output / "baseline_stability_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["not_production"] is True
    assert manifest["production_modifications"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["research_log_modified"] is False
    assert manifest["entry"] == "NEXT_COLUMN_OPEN_ENTRY"
    assert manifest["stop"] == "fixed_3_box_stop"
    assert manifest["target_R"] == 2.5
    assert manifest["break_even_after_R"] == 2.0
    assert manifest["management_rules"] == {"pyramiding": False, "scaling": False, "tp1": False, "tp2": False, "trailing": False}
    assert "PROMOTE" not in manifest["allowed_verdicts"]
    assert log_path.read_text() == "# PnF Strategy Research Results Log\n\n---\n"

    source = Path("research_v2/patterns/pole_baseline_stability_audit.py").read_text()
    assert "strategy_historical_backfill" not in source
    assert "live_binance_forward_trader" not in source
