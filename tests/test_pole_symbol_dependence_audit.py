import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_symbol_dependence_audit import (
    ALLOWED_VERDICTS,
    OUTPUT_NAMES,
    AuditTrade,
    _contribution_normalized_expectancy,
    _equal_weight_rows,
    _metrics,
    _removal_rows,
    _symbol_rows,
    _verdict,
)


def _trade(symbol: str, result: float, exit_ts: int = 1) -> AuditTrade:
    return AuditTrade(trade_id=f"{symbol}-{exit_ts}", symbol=symbol, exit_ts=exit_ts, result_r=result)


def test_symbol_removal_and_without_btc_expectancy() -> None:
    trades = [_trade("BTC", 2.5, 1), _trade("BTC", 2.5, 2), _trade("ETH", -1.0, 3), _trade("SOL", 0.0, 4)]

    rows = _removal_rows(trades, ["BTC", "ETH", "SOL"])
    without_btc = next(row for row in rows if row["removed_symbol"] == "BTC")
    without_eth = next(row for row in rows if row["removed_symbol"] == "ETH")

    assert without_btc["trades"] == 2
    assert without_btc["expectancy_R"] == -0.5
    assert without_btc["total_R"] == -1.0
    assert without_eth["trades"] == 3
    assert without_eth["expectancy_R"] == 1.666667


def test_equal_weight_caps_each_symbol_to_same_trade_count() -> None:
    trades = [
        _trade("BTC", 2.5, 1),
        _trade("BTC", -1.0, 2),
        _trade("BTC", 2.5, 3),
        _trade("ETH", -1.0, 4),
        _trade("SOL", 0.0, 5),
        _trade("SOL", 2.5, 6),
    ]

    rows, portfolio = _equal_weight_rows(trades, ["BTC", "ETH", "SOL"])

    assert {row["sampled_trades"] for row in rows} == {1}
    assert portfolio["trades"] == 3
    assert portfolio["total_R"] == 1.5
    assert portfolio["expectancy_R"] == 0.5


def test_contribution_normalized_expectancy_averages_symbols_not_trades() -> None:
    trades = [_trade("BTC", 2.5, 1), _trade("BTC", 2.5, 2), _trade("ETH", -1.0, 3)]
    symbol_rows = _symbol_rows(trades)

    assert _metrics(trades)["expectancy_R"] == 1.333333
    assert _contribution_normalized_expectancy(symbol_rows) == 0.75


def test_verdicts_are_limited_and_detect_btc_dependence() -> None:
    assert "PROMOTE" not in ALLOWED_VERDICTS
    trades = [_trade("BTC", 2.5, i) for i in range(20)]
    trades += [_trade("ETH", -1.0, 20 + i) for i in range(10)]
    trades += [_trade("SOL", 0.0, 30 + i) for i in range(10)]

    verdict = _verdict(trades, _symbol_rows(trades))

    assert verdict == "EDGE_BTC_DEPENDENT"
    assert verdict in ALLOWED_VERDICTS


def test_cli_outputs_research_only_symbol_dependence_artifacts(tmp_path: Path) -> None:
    trade_sequence = tmp_path / "portfolio_reality_trade_sequence.csv"
    with trade_sequence.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["trade_id", "symbol", "exit_timestamp", "result_R"])
        writer.writeheader()
        for idx in range(40):
            symbol = "BTCUSDT" if idx < 20 else "ETHUSDT" if idx < 30 else "SOLUSDT"
            result = 2.5 if idx < 20 else -1.0 if idx < 30 else 0.0
            writer.writerow({"trade_id": f"T-{idx}", "symbol": symbol, "exit_timestamp": idx, "result_R": result})
    output = tmp_path / "out"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_symbol_dependence_audit",
            "--trade-sequence",
            str(trade_sequence),
            "--output-root",
            str(output),
            "--symbol",
            "BTC",
            "--symbol",
            "ETH",
            "--symbol",
            "SOL",
        ],
        check=True,
    )

    assert {path.name for path in output.iterdir()} == set(OUTPUT_NAMES)
    manifest = json.loads((output / "pole_symbol_dependence_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["not_production"] is True
    assert manifest["production_modifications"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["entry"] == "NEXT_COLUMN_OPEN_ENTRY"
    assert manifest["stop"] == "fixed_3_box_stop"
    assert manifest["target_R"] == 2.5
    assert manifest["break_even_after_R"] == 2.0
    assert manifest["portfolio_without_BTC"]["expectancy_R"] == -0.5
    assert manifest["verdict"] == "EDGE_BTC_DEPENDENT"
