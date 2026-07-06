import sqlite3

import mexc_pole_live_replay_parity as replay


def test_replay_summary_passes_for_empty_research_clean_shape(tmp_path):
    db_path = tmp_path / "pnf_mvp_research_clean.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE candles(symbol TEXT, close_time INTEGER, open REAL, high REAL, low REAL, close REAL)")
    summary_path = tmp_path / "replay_summary.md"

    summary = replay.run_replay(db_path, summary_path)

    assert summary["status"] == "PASS"
    assert summary["trades_generated"] == 0
    text = summary_path.read_text()
    assert "opportunities detected" in text
    assert "PASS only if live trade sequence == research trade sequence." in text


def test_replay_trade_normalization_matches_live_and_research_rows():
    live_plan = replay.live.TradePlan(
        symbol="MEXC_FUT:BTCUSDT",
        direction="LONG",
        opportunity_id="opp-1",
        entry_price=replay.Decimal("100"),
        stop_price=replay.Decimal("97"),
        target_price=replay.Decimal("107.5"),
        break_even_trigger_price=replay.Decimal("106"),
        risk_per_unit=replay.Decimal("3"),
        position_qty=replay.Decimal("1"),
        notional_usdt=replay.Decimal("100"),
        observable_entry_ts=123,
    )
    research_row = {
        "source_opportunity_id": "opp-1",
        "symbol": "MEXC_FUT:BTCUSDT",
        "direction": "LONG",
        "observable_entry_ts": 123,
        "entry_price": 100,
        "stop_price": 97,
        "target_price": 107.5,
        "break_even_trigger_price": 106,
    }

    assert replay.ReplayTrade.from_live_plan(live_plan) == replay.ReplayTrade.from_research_row(research_row)
