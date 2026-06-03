import csv
import json
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_baseline_stability_market_fix_audit import (
    MARKET_FIX_OUTPUT_NAMES,
    inspect_trade_sequence,
    load_corrected_trades,
    market_from_symbol,
    recover_market_family,
    run,
    _corrected_verdict,
)
from research_v2.patterns.pole_baseline_stability_audit import StabilityTrade, _group_rows


def _trade(symbol: str, market: str, result_r: float, exit_ts: int) -> StabilityTrade:
    return StabilityTrade(
        trade_id=f"{symbol}-{exit_ts}",
        symbol=symbol,
        market_family=market,
        entry_ts=exit_ts - 1,
        exit_ts=exit_ts,
        classification="TARGET_FIRST" if result_r > 0 else "BREAK_EVEN_EXIT" if result_r == 0 else "STOP_FIRST",
        result_r=result_r,
    )


def _write_trade_sequence(path: Path, *, include_market: bool = False, rows: int = 36) -> None:
    symbols = ["BTC", "ETH", "SOL", "ENA", "HYPE", "SUI", "TAO"]
    fields = ["trade_id", "symbol", "entry_timestamp", "exit_timestamp", "classification", "result_R"]
    if include_market:
        fields.insert(2, "market_family")
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for i in range(rows):
            symbol = symbols[i % len(symbols)]
            result = 2.5 if i % 4 != 0 else -1.0
            row = {
                "trade_id": f"T{i:03d}",
                "symbol": symbol,
                "entry_timestamp": 1704067200 + i,
                "exit_timestamp": 1704068200 + i,
                "classification": "TARGET_FIRST" if result > 0 else "STOP_FIRST",
                "result_R": result,
            }
            if include_market:
                row["market_family"] = "UNKNOWN"
            writer.writerow(row)


def test_symbol_to_market_mapping() -> None:
    assert market_from_symbol("BTC") == "BINANCE"
    assert market_from_symbol("ETHUSDT") == "BINANCE"
    assert market_from_symbol("SOL") == "BINANCE"
    assert market_from_symbol("ENA") == "MEXC"
    assert market_from_symbol("HYPEUSDT") == "MEXC"
    assert market_from_symbol("SUI") == "MEXC"
    assert market_from_symbol("TAO") == "MEXC"
    assert market_from_symbol("DOGE") == "UNKNOWN"


def test_missing_market_recovery_prefers_mapping_for_unknown_metadata() -> None:
    assert recover_market_family({"market_family": "UNKNOWN"}, "BTC") == ("BINANCE", "symbol_mapping_recovery")
    assert recover_market_family({"exchange": "MEXC"}, "BTC") == ("MEXC", "explicit_metadata")
    assert recover_market_family({}, "DOGE") == ("UNKNOWN", "unmapped_symbol")


def test_trade_sequence_inspection_documents_available_fields(tmp_path: Path) -> None:
    path = tmp_path / "portfolio_reality_trade_sequence.csv"
    path.write_text("trade_id,symbol,source,result_R\n1,BTC,portfolio,2.5\n")

    findings = inspect_trade_sequence(path)

    assert findings["symbol_exists"] is True
    assert findings["market_exists"] is False
    assert findings["exchange_exists"] is False
    assert findings["source_exists"] is True
    assert findings["columns"] == ["trade_id", "symbol", "source", "result_R"]


def test_market_grouping_uses_recovered_families(tmp_path: Path) -> None:
    path = tmp_path / "portfolio_reality_trade_sequence.csv"
    path.write_text(
        "trade_id,symbol,entry_timestamp,exit_timestamp,classification,result_R\n"
        "1,BTC,1,2,TARGET_FIRST,2.5\n"
        "2,ETHUSDT,3,4,STOP_FIRST,-1\n"
        "3,HYPE,5,6,TARGET_FIRST,2.5\n"
        "4,TAOUSDT,7,8,BREAK_EVEN_EXIT,0\n"
    )

    trades, recovery_counts = load_corrected_trades(path)
    market_rows = _group_rows(trades, "market")

    assert recovery_counts == {"explicit_metadata": 0, "symbol_mapping_recovery": 4, "unmapped_symbol": 0}
    assert [row["market_family"] for row in market_rows] == ["BINANCE", "MEXC"]
    assert market_rows[0]["trades"] == 2
    assert market_rows[1]["BE_exits"] == 1


def test_corrected_verdict_logic_removes_unknown_market_concentration_when_both_markets_positive() -> None:
    trades = [_trade("BTC", "BINANCE", 2.5 if i % 3 else -1.0, i) for i in range(1, 19)]
    trades += [_trade("ENA", "MEXC", 2.5 if i % 3 else -1.0, i + 100) for i in range(1, 19)]
    market_rows = _group_rows(trades, "market")
    concentration_rows = [
        {"scope": "market", "classification": "diversified"},
        {"scope": "symbol", "classification": "diversified"},
        {"scope": "time", "classification": "diversified"},
    ]

    verdict, reason = _corrected_verdict(trades, [{"flag": "RESEARCH_ONLY", "severity": "INFO", "scope": "test", "details": ""}], concentration_rows, market_rows)

    assert verdict == "STABLE_BASELINE"
    assert "both market families" in reason


def test_corrected_verdict_preserves_concentrated_edge_for_symbol_concentration() -> None:
    trades = [_trade("BTC", "BINANCE", 2.5, i) for i in range(1, 31)]
    trades += [_trade("ENA", "MEXC", 2.5, i + 100) for i in range(1, 6)]
    concentration_rows = [
        {"scope": "market", "classification": "diversified"},
        {"scope": "symbol", "classification": "highly_concentrated"},
        {"scope": "time", "classification": "diversified"},
    ]

    verdict, reason = _corrected_verdict(trades, [], concentration_rows, _group_rows(trades, "market"))

    assert verdict == "CONCENTRATED_EDGE"
    assert "symbol" in reason


def test_cli_outputs_market_fix_artifacts_and_preserves_production_isolation(tmp_path: Path) -> None:
    trade_path = tmp_path / "portfolio_reality_trade_sequence.csv"
    _write_trade_sequence(trade_path)
    output = tmp_path / "market_fix"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_baseline_stability_market_fix_audit",
            "--trade-sequence",
            str(trade_path),
            "--output-root",
            str(output),
            "--rolling-window",
            "50",
        ],
        check=True,
    )

    assert {path.name for path in output.iterdir()} == set(MARKET_FIX_OUTPUT_NAMES)
    breakdown = list(csv.DictReader((output / "baseline_stability_market_fix_breakdown.csv").open()))
    assert [row["market_family"] for row in breakdown] == ["BINANCE", "MEXC"]
    manifest = json.loads((output / "baseline_stability_market_fix_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["not_production"] is True
    assert manifest["production_modifications"] is False
    assert manifest["strategy_modifications"] is False
    assert manifest["parameter_modifications"] is False
    assert manifest["research_log_modified"] is False
    assert manifest["reran_chronology"] is False
    assert manifest["reran_expectancy"] is False
    assert manifest["reran_execution"] is False
    assert manifest["reran_portfolio"] is False
    assert manifest["reran_break_even"] is False
    assert manifest["reran_symbol_dependence"] is False
    assert manifest["recovery_counts"]["symbol_mapping_recovery"] == 36
    assert "PROMOTE" not in manifest["allowed_verdicts"]

    source = Path("research_v2/patterns/pole_baseline_stability_market_fix_audit.py").read_text()
    assert "strategy_historical_backfill" not in source
    assert "live_binance_forward_trader" not in source


def test_run_refuses_to_modify_input_trade_records(tmp_path: Path) -> None:
    trade_path = tmp_path / "portfolio_reality_trade_sequence.csv"
    _write_trade_sequence(trade_path, include_market=True, rows=8)
    before = trade_path.read_text()

    run(trade_path, tmp_path / "out", rolling_window=50)

    assert trade_path.read_text() == before
