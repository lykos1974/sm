from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

import mexc_pole_live_trader as trader
import mexc_pole_missed_signal_audit as audit


BASE = 1_700_000_000
ALIGNED_BASE = ((BASE + 59) // 60) * 60
HOUR = 60


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

    assert explicit_to.start == dt(times[4] - 2 * 3600)
    assert explicit_to.end == dt(times[4])
    assert latest_to.start == dt(times[5] - 3600)
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


def test_detect_candle_interval_requires_min1_from_millisecond_timestamps(tmp_path):
    c = cfg(tmp_path)
    times = [BASE + 60 * i for i in range(4)]
    write_candles(c.candles_db_path, [ts * 1000 for ts in times])

    interval = audit.detect_candle_interval(c.candles_db_path, c.allowed_symbols)

    assert interval.seconds == 60
    assert interval.mexc_name == "Min1"
    assert interval.storage_scales["MEXC_FUT:BTCUSDT"] == 1000


def test_min1_api_requests_and_btcusdt_endpoint_mapping(monkeypatch):
    requested = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return json.dumps({"success": True, "data": {"time": [ALIGNED_BASE + 60], "open": ["1"], "high": ["2"], "low": ["0.5"], "close": ["1.5"]}}).encode()

    def fake_urlopen(url, timeout):
        requested.append(url)
        return Response()

    monkeypatch.setattr(audit.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: end_ts)

    rows = audit.fetch_mexc_public_candles("https://contract.mexc.com", "MEXC_FUT:BTCUSDT", ALIGNED_BASE + 60, ALIGNED_BASE + 60)

    assert rows == [(ALIGNED_BASE + 60, 1.0, 2.0, 0.5, 1.5)]
    assert "/api/v1/contract/kline/BTC_USDT?" in requested[0]
    assert "interval=Min1" in requested[0]


def test_minute_level_missing_range_detection(tmp_path, monkeypatch):
    c = cfg(tmp_path)
    times = [BASE, BASE + 60, BASE + 180]
    write_candles(c.candles_db_path, times)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: end_ts)

    ranges = audit.missing_candle_ranges([c.candles_db_path], c.allowed_symbols, audit.AuditRange(dt(BASE), dt(BASE + 180)), 60)

    assert ranges == {"MEXC_FUT:BTCUSDT": [(BASE + 120, BASE + 120)]}


def test_store_audit_candles_preserves_millisecond_cache_storage(tmp_path):
    cache = tmp_path / "audit.sqlite3"

    audit.store_audit_candles(cache, "MEXC_FUT:BTCUSDT", [(BASE + 60, 1.0, 2.0, 0.5, 1.5)], storage_scale=1000)

    with sqlite3.connect(cache) as conn:
        stored = conn.execute("SELECT symbol, close_time FROM candles").fetchone()
    assert stored == ("MEXC_FUT:BTCUSDT", (BASE + 60) * 1000)


def test_complete_contiguous_replay_coverage_from_primary_and_cache(tmp_path, monkeypatch):
    c = cfg(tmp_path)
    primary_times = [BASE, BASE + 60, BASE + 180]
    write_candles(c.candles_db_path, [ts * 1000 for ts in primary_times])
    cache = tmp_path / "audit_cache.sqlite3"
    audit.store_audit_candles(cache, "MEXC_FUT:BTCUSDT", [(BASE + 120, 1.0, 1.0, 1.0, 1.0)], storage_scale=1000)
    observed = []

    def fake_live_generate_trade_plans(config, client):
        with sqlite3.connect(config.candles_db_path) as conn:
            max_ts = conn.execute("SELECT MAX(close_time) FROM candles").fetchone()[0]
        observed.append(max_ts)
        return [plan(max_ts // 1000, f"opp-{max_ts}")]

    monkeypatch.setattr(audit.live, "generate_trade_plans", fake_live_generate_trade_plans)
    monkeypatch.setattr(audit, "ensure_audit_candle_coverage", lambda config, audit_range: cache)

    rows = audit.run_audit(c, tmp_path / "missed.csv", audit.AuditRange(dt(BASE), dt(BASE + 180)))

    assert [row["opportunity_id"] for row in rows] == [f"opp-{ts * 1000}" for ts in (BASE + 60, BASE + 120, BASE + 180)]
    assert observed == [(BASE + 60) * 1000, (BASE + 120) * 1000, (BASE + 180) * 1000]


def test_mexc_missing_candle_error_includes_focused_diagnostics_without_payload(monkeypatch):
    requested = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            payload = {
                "success": True,
                "data": {
                    "time": [ALIGNED_BASE, ALIGNED_BASE + 120],
                    "open": ["1", "3"],
                    "high": ["2", "4"],
                    "low": ["0.5", "2.5"],
                    "close": ["1.5", "3.5"],
                    "huge_payload_marker": "must-not-leak",
                },
            }
            return json.dumps(payload).encode()

    def fake_urlopen(url, timeout):
        requested.append(url)
        return Response()

    monkeypatch.setattr(audit.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: end_ts)

    with pytest.raises(RuntimeError) as excinfo:
        audit.fetch_mexc_public_candles("https://contract.mexc.com", "MEXC_FUT:BTCUSDT", ALIGNED_BASE, ALIGNED_BASE + 120)

    message = str(excinfo.value)
    assert "MEXC kline response missing candle" in message
    assert f"symbol=MEXC_FUT:BTCUSDT" in message
    assert f"requested_start_ts={ALIGNED_BASE}" in message
    assert f"requested_end_ts={ALIGNED_BASE + 120}" in message
    assert f"first_returned_ts={ALIGNED_BASE}" in message
    assert f"last_returned_ts={ALIGNED_BASE + 120}" in message
    assert "expected_candle_count=3" in message
    assert "returned_unique_candle_count=2" in message
    assert f"first_missing_ts={ALIGNED_BASE + 60}" in message
    assert f"previous_returned_ts={ALIGNED_BASE}" in message
    assert f"next_returned_ts={ALIGNED_BASE + 120}" in message
    assert f"current_page_start_ts={ALIGNED_BASE}" in message
    assert f"current_page_end_ts={ALIGNED_BASE + 120}" in message
    assert "pages_fetched=1" in message
    assert "must-not-leak" not in message
    assert requested


def test_mexc_fetch_aligns_unaligned_bounds_and_expected_timestamps(monkeypatch):
    calls = []

    def fake_page(base_url, symbol, start_ts, end_ts, interval):
        calls.append((start_ts, end_ts))
        return ([(ts, 1.0, 2.0, 0.5, 1.5) for ts in range(start_ts, end_ts + 1, 60)], {"success": True})

    monkeypatch.setattr(audit, "_fetch_mexc_public_candle_page", fake_page)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: (end_ts // interval_seconds) * interval_seconds)

    rows = audit.fetch_mexc_public_candles("https://contract.mexc.com", "MEXC_FUT:BTCUSDT", ALIGNED_BASE + 1, ALIGNED_BASE + 179)

    assert [row[0] for row in rows] == [ALIGNED_BASE + 60, ALIGNED_BASE + 120]
    assert calls == [(ALIGNED_BASE + 60, ALIGNED_BASE + 120)]


def test_mexc_fetch_exact_5000_aligned_candle_range(monkeypatch):
    calls = []
    start = ALIGNED_BASE + 60
    end = start + 4999 * 60

    def fake_page(base_url, symbol, start_ts, end_ts, interval):
        calls.append((start_ts, end_ts))
        return ([(ts, 1.0, 2.0, 0.5, 1.5) for ts in range(start_ts, end_ts + 1, 60)], {"success": True})

    monkeypatch.setattr(audit, "_fetch_mexc_public_candle_page", fake_page)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: end_ts)

    rows = audit.fetch_mexc_public_candles("https://contract.mexc.com", "MEXC_FUT:BTCUSDT", start, end)

    assert len(rows) == 5000
    assert rows[0][0] == start
    assert rows[-1][0] == end
    assert calls == [
        (start, start + 1999 * 60),
        (start + 2000 * 60, start + 3999 * 60),
        (start + 4000 * 60, end),
    ]


def test_mexc_fetch_multi_page_advances_from_last_returned_without_boundary_gap(monkeypatch):
    calls = []
    start = ALIGNED_BASE + 60
    end = start + 2001 * 60

    def fake_page(base_url, symbol, start_ts, end_ts, interval):
        calls.append((start_ts, end_ts))
        page_limit = start_ts + 1998 * 60 if len(calls) == 1 else end_ts
        return ([(ts, 1.0, 2.0, 0.5, 1.5) for ts in range(start_ts, page_limit + 1, 60)], {"success": True})

    monkeypatch.setattr(audit, "_fetch_mexc_public_candle_page", fake_page)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: end_ts)

    rows = audit.fetch_mexc_public_candles("https://contract.mexc.com", "MEXC_FUT:BTCUSDT", start, end)

    assert len(rows) == 2002
    assert [row[0] for row in rows] == list(range(start, end + 1, 60))
    assert calls == [(start, start + 1999 * 60), (start + 1999 * 60, end)]


def test_mexc_fetch_deduplicates_and_keeps_only_closed_aligned_candles(monkeypatch):
    start = ALIGNED_BASE + 60
    end = ALIGNED_BASE + 240

    def fake_page(base_url, symbol, start_ts, end_ts, interval):
        return ([
            (start, 1.0, 2.0, 0.5, 1.5),
            (start, 9.0, 9.0, 9.0, 9.0),
            (start + 30, 3.0, 3.0, 3.0, 3.0),
            (start + 60, 2.0, 3.0, 1.5, 2.5),
        ], {"success": True})

    monkeypatch.setattr(audit, "_fetch_mexc_public_candle_page", fake_page)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: start + 60)

    rows = audit.fetch_mexc_public_candles("https://contract.mexc.com", "MEXC_FUT:BTCUSDT", start, end)

    assert rows == [(start, 9.0, 9.0, 9.0, 9.0), (start + 60, 2.0, 3.0, 1.5, 2.5)]


def test_mexc_fetch_no_progress_protection_reports_missing(monkeypatch):
    calls = []
    start = ALIGNED_BASE + 60
    end = ALIGNED_BASE + 180

    def fake_page(base_url, symbol, start_ts, end_ts, interval):
        calls.append((start_ts, end_ts))
        return ([], {"success": True})

    monkeypatch.setattr(audit, "_fetch_mexc_public_candle_page", fake_page)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: end_ts)

    with pytest.raises(RuntimeError, match="MEXC kline response missing candle") as excinfo:
        audit.fetch_mexc_public_candles("https://contract.mexc.com", "MEXC_FUT:BTCUSDT", start, end)

    assert calls == [(start, end)]
    assert "pages_fetched=1" in str(excinfo.value)
