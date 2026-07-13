from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

import mexc_pole_live_trader as trader
import mexc_pole_missed_signal_audit as audit


BASE = 1_700_000_000
HOUR = 3600


def dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, UTC)


def cfg(tmp_path, **kw):
    base = dict(
        candles_db_path=tmp_path / "candles.sqlite3",
        state_db_path=tmp_path / "live_state.sqlite3",
        decisions_log_path=tmp_path / "live_decisions.log",
        orders_log_path=tmp_path / "live_orders.log",
        trade_plan_csv_path=tmp_path / "current_trade_plan.csv",
        allowed_symbols=("MEXC_FUT:BTCUSDT",),
    )
    base.update(kw)
    return trader.LiveConfig(**base)


def plan(ts: int, opportunity_id: str = "opp-1") -> trader.TradePlan:
    return trader.TradePlan(
        symbol="MEXC_FUT:BTCUSDT",
        direction="LONG",
        opportunity_id=opportunity_id,
        entry_price=Decimal("100"),
        stop_price=Decimal("99"),
        target_price=Decimal("102.5"),
        break_even_trigger_price=Decimal("102"),
        risk_per_unit=Decimal("1"),
        position_qty=Decimal("1"),
        notional_usdt=Decimal("100"),
        observable_entry_ts=ts,
    )


def write_candles(db_path, times, symbol="MEXC_FUT:BTCUSDT"):
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS candles (symbol TEXT, close_time INTEGER, open REAL, high REAL, low REAL, close REAL)")
        conn.executemany(
            "INSERT INTO candles(symbol, close_time, open, high, low, close) VALUES (?,?,?,?,?,?)",
            [(symbol, ts, 1.0, 1.0, 1.0, 1.0) for ts in times],
        )


def log_event(path, ts: int, event: str, **payload):
    row = {"ts": dt(ts).isoformat(), "event": event, **payload}
    with path.open("a") as handle:
        handle.write(json.dumps(row) + "\n")


def test_explicit_from_to_range_replays_selected_interval(tmp_path, monkeypatch):
    c = cfg(tmp_path)
    times = [BASE + HOUR * i for i in range(5)]
    write_candles(c.candles_db_path, times)
    audit_range = audit.resolve_audit_range(c, from_utc=dt(times[1]), to_utc=dt(times[3]))

    monkeypatch.setattr(audit, "live_plans_through", lambda config, close_time: [plan(close_time, f"opp-{close_time}")])

    rows = audit.run_audit(c, tmp_path / "missed.csv", audit_range)

    assert [row["opportunity_id"] for row in rows] == [f"opp-{times[2]}", f"opp-{times[3]}"]
    assert rows[-1]["signal_time_utc"] == dt(times[3]).isoformat()


def test_last_hours_range_uses_to_or_latest_closed_candle(tmp_path):
    c = cfg(tmp_path)
    times = [BASE + HOUR * i for i in range(6)]
    write_candles(c.candles_db_path, times)

    explicit_to = audit.resolve_audit_range(c, to_utc=dt(times[4]), last_hours=2)
    latest_to = audit.resolve_audit_range(c, last_hours=1)

    assert explicit_to.start == dt(times[2])
    assert explicit_to.end == dt(times[4])
    assert latest_to.start == dt(times[4])
    assert latest_to.end == dt(times[5])


def test_mutually_exclusive_argument_rejection():
    parser = audit.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--from-utc", "2026-07-13T00:00:00Z", "--last-hours", "2"])


def test_reversed_range_rejection(tmp_path):
    c = cfg(tmp_path)
    write_candles(c.candles_db_path, [BASE, BASE + HOUR])

    with pytest.raises(ValueError, match="start .* must be before end"):
        audit.resolve_audit_range(c, from_utc=dt(BASE + HOUR), to_utc=dt(BASE))


def test_insufficient_candle_data_failure(tmp_path):
    c = cfg(tmp_path)
    write_candles(c.candles_db_path, [BASE, BASE + HOUR, BASE + 3 * HOUR])
    audit_range = audit.AuditRange(dt(BASE), dt(BASE + 3 * HOUR))

    with pytest.raises(RuntimeError, match="insufficient local candle data: .*candle gap"):
        audit.run_audit(c, tmp_path / "missed.csv", audit_range)


def test_full_gap_replay_not_latest_snapshot_only(tmp_path, monkeypatch):
    c = cfg(tmp_path)
    times = [BASE + HOUR * i for i in range(4)]
    write_candles(c.candles_db_path, times)
    audit_range = audit.AuditRange(dt(times[0]), dt(times[3]))
    observed_snapshot_maxima = []

    def fake_live_generate_trade_plans(config, client):
        with sqlite3.connect(config.candles_db_path) as conn:
            max_ts = conn.execute("SELECT MAX(close_time) FROM candles").fetchone()[0]
        observed_snapshot_maxima.append(max_ts)
        return [plan(int(max_ts), f"opp-{max_ts}")]

    monkeypatch.setattr(audit.live, "generate_trade_plans", fake_live_generate_trade_plans)

    rows = audit.run_audit(c, tmp_path / "missed.csv", audit_range)

    assert observed_snapshot_maxima == times[1:]
    assert [row["opportunity_id"] for row in rows] == [f"opp-{ts}" for ts in times[1:]]


def test_audit_uses_exact_live_generate_trade_plans_and_preserves_live_files(tmp_path, monkeypatch):
    c = cfg(tmp_path, box_sizes={"MEXC_FUT:BTCUSDT": 100.0}, fixed_risk_usdt=Decimal("7"))
    times = [BASE + HOUR * i for i in range(3)]
    write_candles(c.candles_db_path, times)
    log_event(c.decisions_log_path, times[0], "NO_VALID_SIGNAL")
    c.state_db_path.write_text("state-sentinel")
    c.orders_log_path.write_text("orders-sentinel")
    c.trade_plan_csv_path.write_text("plan-sentinel")
    original_decisions = c.decisions_log_path.read_text()
    calls = []

    def fake_generate_trade_plans(config, client):
        calls.append((config, client))
        assert config is not c
        assert config.allowed_symbols == c.allowed_symbols
        assert config.box_sizes == c.box_sizes
        assert config.fixed_risk_usdt == c.fixed_risk_usdt
        assert isinstance(client, audit.ReadOnlySpecClient)
        with sqlite3.connect(config.candles_db_path) as conn:
            max_ts = conn.execute("SELECT MAX(close_time) FROM candles").fetchone()[0]
        return [plan(int(max_ts), f"opp-{max_ts}")]

    monkeypatch.setattr(audit.live, "generate_trade_plans", fake_generate_trade_plans)

    rows = audit.run_audit(c, tmp_path / "missed.csv")

    assert [row["opportunity_id"] for row in rows] == [f"opp-{times[1]}", f"opp-{times[2]}"]
    assert len(calls) == 2
    assert c.state_db_path.read_text() == "state-sentinel"
    assert c.orders_log_path.read_text() == "orders-sentinel"
    assert c.trade_plan_csv_path.read_text() == "plan-sentinel"
    assert c.decisions_log_path.read_text() == original_decisions


def test_read_only_spec_client_rejects_exchange_order_methods():
    client = audit.ReadOnlySpecClient()

    for method_name in ("place_entry", "place_stop", "place_target", "replace_stop_to_break_even", "cancel_order", "modify_order"):
        with pytest.raises(AssertionError, match="audit must not call exchange order method"):
            getattr(client, method_name)(plan(1))


def test_reconcile_blocked_reason_when_bot_was_running_near_signal(tmp_path, monkeypatch):
    c = cfg(tmp_path)
    times = [BASE + HOUR * i for i in range(2)]
    write_candles(c.candles_db_path, times)
    log_event(c.decisions_log_path, times[0], "NO_VALID_SIGNAL")
    log_event(c.decisions_log_path, times[1], "TRADING_BLOCKED", reason="EXCHANGE_RECONCILE_ERROR")

    monkeypatch.setattr(audit, "live_plans_through", lambda config, close_time: [plan(close_time, f"opp-{close_time}")])

    rows = audit.run_audit(c, tmp_path / "missed.csv")

    assert rows == [
        {
            "signal_time_utc": dt(times[1]).isoformat(),
            "symbol": "MEXC_FUT:BTCUSDT",
            "direction": "LONG",
            "observable_entry_time_utc": dt(times[1]).isoformat(),
            "entry": "100",
            "stop": "99",
            "target": "102.5",
            "opportunity_id": f"opp-{times[1]}",
            "bot_running_at_time": "True",
            "not_executed_reason": "RECONCILE_BLOCKED",
        }
    ]


def test_validate_local_candle_coverage_accepts_second_timestamps(tmp_path):
    c = cfg(tmp_path)
    times = [BASE + HOUR * i for i in range(4)]
    write_candles(c.candles_db_path, times)
    audit_range = audit.AuditRange(dt(times[0]), dt(times[-1]))

    audit.validate_local_candle_coverage(c.candles_db_path, c.allowed_symbols, audit_range)


def test_validate_local_candle_coverage_accepts_millisecond_timestamps(tmp_path):
    c = cfg(tmp_path)
    times = [BASE + HOUR * i for i in range(4)]
    write_candles(c.candles_db_path, [ts * 1000 for ts in times])
    audit_range = audit.AuditRange(dt(times[0]), dt(times[-1]))

    audit.validate_local_candle_coverage(c.candles_db_path, c.allowed_symbols, audit_range)
    assert audit.resolve_audit_range(c, last_hours=1).end == dt(times[-1])
