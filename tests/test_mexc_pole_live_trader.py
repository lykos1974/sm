from __future__ import annotations

from decimal import Decimal
import http.client
import json
import sqlite3

import pytest

import mexc_pole_live_trader as trader

ORIGINAL_REFRESH_LIVE_CANDLES = trader.refresh_live_candles
ORIGINAL_ENSURE_LIVE_CANDLE_WARMUP = trader.ensure_live_candle_warmup


class FakeClient:
    def __init__(self, mark=Decimal("100"), fail_entry=False, fail_stop=False, stop_exists=True, target_exists=True, positions=None, open_orders=None):
        self.spec = trader.ContractSpec("BTCUSDT", qty_step=Decimal("0.001"), min_qty=Decimal("0.001"))
        self.orders = []
        self.mark = Decimal(mark)
        self.fail_entry = fail_entry
        self.fail_stop = fail_stop
        self.stop_exists = stop_exists
        self.target_exists = target_exists
        self.positions = positions if positions is not None else [{"positionId": "pos-1", "symbol": "BTCUSDT", "state": 1, "holdVol": "1", "holdAvgPrice": "100"}]
        self.open_orders = open_orders if open_orders is not None else []
        self.query_counts = {"order": 0, "position": 0, "open_orders": 0, "plan_orders": 0}
        self.queried_symbols = []
        self.be_moves = 0

    def get_contract_spec(self, venue_symbol): return self.spec
    def place_entry(self, plan, order_type):
        if self.fail_entry: raise RuntimeError("entry failed")
        self.orders.append(("entry", plan.opportunity_id)); return {"order_id": "entry-1"}
    def place_stop(self, plan):
        if self.fail_stop: raise RuntimeError("stop failed")
        self.orders.append(("stop", plan.opportunity_id)); return {"order_id": "stop-1"}
    def place_target(self, plan): self.orders.append(("target", plan.opportunity_id)); return {"order_id": "target-1"}
    def query_position(self, symbol):
        self.query_counts["position"] += 1
        self.queried_symbols.append(("position", symbol))
        return self.positions
    def query_open_orders(self, symbol):
        self.query_counts["open_orders"] += 1
        self.queried_symbols.append(("open_orders", symbol))
        return self.open_orders
    def query_order(self, order_id):
        self.query_counts["order"] += 1
        return {"orderId": order_id, "state": 3, "dealVol": "1"}
    def query_plan_orders(self, symbol):
        self.query_counts["plan_orders"] += 1
        self.queried_symbols.append(("plan_orders", symbol))
        rows = []
        if self.stop_exists:
            rows.append({"id": "stop-1", "symbol": "BTCUSDT", "state": 1, "stopLossPrice": "99", "vol": "1"})
        if self.target_exists:
            rows.append({"id": "target-1", "symbol": "BTCUSDT", "state": 1, "takeProfitPrice": "102.5", "vol": "1"})
        return rows
    def replace_stop_to_break_even(self, trade_id, plan): self.be_moves += 1; return {"order_id": "be-1"}
    def get_mark_price(self, venue_symbol): return self.mark
    def sync_trade(self, row): return {"status": row["status"]}


def cfg(tmp_path, **kw):
    base = dict(state_db_path=tmp_path / "live_state.sqlite3", decisions_log_path=tmp_path / "live_decisions.log", orders_log_path=tmp_path / "live_orders.log", trade_plan_csv_path=tmp_path / "current_trade_plan.csv")
    base.update(kw)
    return trader.LiveConfig(**base)


def plan(**kw):
    base = dict(symbol="MEXC_FUT:BTCUSDT", direction="LONG", opportunity_id="opp-1", entry_price=Decimal("100"), stop_price=Decimal("99"), target_price=Decimal("102.5"), break_even_trigger_price=Decimal("102"), risk_per_unit=Decimal("1"), position_qty=Decimal("1"), notional_usdt=Decimal("100"), observable_entry_ts=1)
    base.update(kw)
    return trader.TradePlan(**base)


@pytest.fixture(autouse=True)
def parity_matches(monkeypatch):
    monkeypatch.setattr(trader, "recompute_research_plan_for_live", lambda live_plan, config, client: live_plan)
    monkeypatch.setattr(trader, "refresh_live_candles", lambda config: {})
    monkeypatch.setattr(trader, "ensure_live_candle_warmup", lambda config: [])


def test_mexc_request_incomplete_read_is_runtime_error(monkeypatch):
    client = trader.MexcFuturesClient("api-key", "api-secret", "https://contract.mexc.com")

    def raise_incomplete_read(*args, **kwargs):
        raise http.client.IncompleteRead(b'{"success":', 32)

    monkeypatch.setattr(trader.urllib.request, "urlopen", raise_incomplete_read)

    with pytest.raises(RuntimeError, match="MEXC request failed:") as excinfo:
        client._request("GET", "/api/v1/private/account/assets", signed=True)

    message = str(excinfo.value)
    assert "api-key" not in message
    assert "api-secret" not in message


def test_reconcile_query_error_logs_and_does_not_kill_without_unprotected_position(tmp_path):
    c = cfg(tmp_path, allowed_symbols=("MEXC_FUT:BTCUSDT",))
    client = FakeClient(positions=[])

    def fail_open_orders(symbol):
        raise RuntimeError("MEXC request failed: IncompleteRead(10 bytes read)")

    client.query_open_orders = fail_open_orders

    assert trader.reconcile_from_exchange(c, client) == "EXCHANGE_RECONCILE_ERROR"
    assert not trader.is_killed(c.state_db_path)
    payload = json.loads(c.decisions_log_path.read_text().splitlines()[0])
    assert payload["event"] == "EXCHANGE_RECONCILE_ERROR"
    assert payload["symbol"] == "MEXC_FUT:BTCUSDT"
    assert payload["step"] == "query_open_orders"
    assert payload["query"] == "query_open_orders"
    assert "IncompleteRead" in payload["error"]


def test_live_reconcile_error_blocks_trade_execution(tmp_path, monkeypatch):
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "k")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "s")
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, max_notional_usdt=Decimal("1000"), allowed_symbols=("MEXC_FUT:BTCUSDT",))
    client = FakeClient(positions=[])

    def fail_plan_orders(symbol):
        raise RuntimeError("MEXC request failed: IncompleteRead(10 bytes read)")

    client.query_plan_orders = fail_plan_orders
    monkeypatch.setattr(trader, "generate_trade_plans", lambda config, exchange_client: [plan()])

    assert trader.run_once(c, client) == ["EXCHANGE_RECONCILE_ERROR"]
    assert client.orders == []
    assert not trader.is_killed(c.state_db_path)
    log = c.decisions_log_path.read_text()
    assert "EXCHANGE_RECONCILE_ERROR" in log
    assert "TRADING_BLOCKED" in log


def test_config_symbols_key_loads_all_configured_symbols(tmp_path):
    config_path = tmp_path / "config.json"
    configured = ["MEXC_FUT:BTCUSDT", "MEXC_FUT:ETHUSDT", "MEXC_FUT:SOLUSDT"]
    config_path.write_text(json.dumps({"symbols": configured}))

    c = trader.LiveConfig.from_json(config_path)

    assert c.allowed_symbols == tuple(configured)


def test_config_allowed_symbols_fallback_still_works(tmp_path):
    config_path = tmp_path / "config.json"
    configured = ["MEXC_FUT:SUIUSDT", "MEXC_FUT:ENAUSDT"]
    config_path.write_text(json.dumps({"allowed_symbols": configured}))

    c = trader.LiveConfig.from_json(config_path)

    assert c.allowed_symbols == tuple(configured)


def test_config_conflicting_symbols_and_allowed_symbols_raises(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"symbols": ["MEXC_FUT:BTCUSDT"], "allowed_symbols": ["MEXC_FUT:ENAUSDT"]}))

    with pytest.raises(ValueError, match="symbols and allowed_symbols both exist but differ"):
        trader.LiveConfig.from_json(config_path)


def test_reconcile_iterates_loaded_symbols(tmp_path):
    symbols = ("MEXC_FUT:SUIUSDT", "MEXC_FUT:ENAUSDT")
    c = cfg(tmp_path, allowed_symbols=symbols)
    client = FakeClient(positions=[])

    trader.reconcile_from_exchange(c, client)

    assert client.queried_symbols == [
        ("position", "MEXC_FUT:SUIUSDT"),
        ("open_orders", "MEXC_FUT:SUIUSDT"),
        ("plan_orders", "MEXC_FUT:SUIUSDT"),
        ("position", "MEXC_FUT:ENAUSDT"),
        ("open_orders", "MEXC_FUT:ENAUSDT"),
        ("plan_orders", "MEXC_FUT:ENAUSDT"),
    ]


def test_run_once_audits_loaded_symbol_universe(tmp_path, monkeypatch):
    symbols = ("MEXC_FUT:SUIUSDT", "MEXC_FUT:ENAUSDT")
    c = cfg(tmp_path, allowed_symbols=symbols)
    monkeypatch.setattr(trader, "generate_trade_plans", lambda config, client: [])

    trader.run_once(c, FakeClient())

    first_line = c.decisions_log_path.read_text().splitlines()[0]
    payload = json.loads(first_line)
    assert payload["event"] == "SYMBOL_UNIVERSE_LOADED"
    assert payload["symbols"] == list(symbols)


def test_dry_run_cannot_place_real_orders(tmp_path):
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=True)
    client = FakeClient()
    assert trader.execute_plan(plan(), c, client) == "DRY_RUN"
    assert client.orders == []


def test_live_flag_required(tmp_path, monkeypatch):
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "k")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "s")
    c = cfg(tmp_path, live_trading_enabled=False, dry_run=False)
    client = FakeClient()
    assert trader.execute_plan(plan(), c, client) == "DRY_RUN"
    assert client.orders == []


def test_sizing_with_fixed_1_usdt_risk():
    spec = trader.ContractSpec("BTCUSDT", qty_step=Decimal("0.001"), min_qty=Decimal("0.001"))
    assert trader.compute_qty(Decimal("100"), Decimal("99"), Decimal("1.0"), spec) == Decimal("1")
    assert trader.compute_qty(Decimal("100"), Decimal("99.75"), Decimal("1.0"), spec) == Decimal("4")


def test_max_notional_blocks_oversized_trades(tmp_path):
    c = cfg(tmp_path, max_notional_usdt=Decimal("99"))
    ok, reason = trader.can_open(plan(notional_usdt=Decimal("100")), c)
    assert not ok and reason == "MAX_NOTIONAL"


def test_max_daily_loss_blocks_trading(tmp_path):
    c = cfg(tmp_path, max_daily_loss_usdt=Decimal("3"))
    trader.init_state(c.state_db_path)
    import sqlite3
    with sqlite3.connect(c.state_db_path) as conn:
        conn.execute("INSERT INTO trades(opportunity_id,symbol,status,entry_price,stop_price,target_price,be_trigger_price,qty,notional_usdt,closed_at,realized_pnl_usdt) VALUES ('x','MEXC_FUT:ETHUSDT','CLOSED','1','1','1','1','1','1',datetime('now'),'-3.0')")
    ok, reason = trader.can_open(plan(), c)
    assert not ok and reason == "MAX_DAILY_LOSS"


def test_duplicate_opportunity_does_not_place_second_order(tmp_path, monkeypatch):
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "k")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "s")
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, max_notional_usdt=Decimal("1000"))
    client = FakeClient()
    assert trader.execute_plan(plan(), c, client) == "OPEN"
    assert trader.execute_plan(plan(), c, client) in {"SYMBOL_POSITION_ALREADY_OPEN", "DUPLICATE_OPPORTUNITY"}
    assert [o[0] for o in client.orders].count("entry") == 1


def test_be_move_only_after_plus_2r(tmp_path):
    c = cfg(tmp_path)
    trader.init_state(c.state_db_path)
    import sqlite3
    with sqlite3.connect(c.state_db_path) as conn:
        conn.execute("INSERT INTO trades(opportunity_id,symbol,status,entry_price,stop_price,target_price,be_trigger_price,qty,notional_usdt,opened_at) VALUES ('opp','MEXC_FUT:BTCUSDT','OPEN','100','99','102.5','102','1','100',datetime('now'))")
    low = FakeClient(mark=Decimal("101.99"))
    trader.sync_open_trades(c, low)
    assert low.be_moves == 0
    high = FakeClient(mark=Decimal("102"))
    trader.sync_open_trades(c, high)
    assert high.be_moves == 1


def test_kill_switch_blocks_trading(tmp_path):
    c = cfg(tmp_path)
    trader.trigger_kill_switch(c.state_db_path, "manual")
    ok, reason = trader.can_open(plan(), c)
    assert not ok and reason == "KILL_SWITCH_ACTIVE"


def test_restart_reconciliation_from_exchange(tmp_path):
    c = cfg(tmp_path)
    client = FakeClient(
        positions=[{"positionId": "restart-1", "symbol": "BTCUSDT", "state": 1, "holdVol": "2", "holdAvgPrice": "100"}],
        open_orders=[{"orderId": "entry-open", "symbol": "BTCUSDT", "state": 2}],
    )
    trader.reconcile_from_exchange(c, client)
    import sqlite3
    with sqlite3.connect(c.state_db_path) as conn:
        row = conn.execute("SELECT opportunity_id,status,qty FROM trades WHERE symbol='MEXC_FUT:BTCUSDT'").fetchone()
    assert row == ("EXCHANGE-MEXC_FUT:BTCUSDT-restart-1", "OPEN", "2")
    assert client.query_counts["position"] >= 1
    assert client.query_counts["open_orders"] >= 1
    assert client.query_counts["plan_orders"] >= 1


def test_missing_stop_triggers_kill_switch(tmp_path, monkeypatch):
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "k")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "s")
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, max_notional_usdt=Decimal("1000"))
    result = trader.execute_plan(plan(), c, FakeClient(stop_exists=False))
    assert result == "UNPROTECTED_POSITION"
    assert trader.is_killed(c.state_db_path)
    assert "UNPROTECTED_POSITION" in c.decisions_log_path.read_text()


def test_entry_filled_but_stop_missing_blocks_trading(tmp_path, monkeypatch):
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "k")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "s")
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, max_notional_usdt=Decimal("1000"))
    assert trader.execute_plan(plan(), c, FakeClient(stop_exists=False)) == "UNPROTECTED_POSITION"
    ok, reason = trader.can_open(plan(opportunity_id="opp-2", symbol="MEXC_FUT:ETHUSDT"), c)
    assert not ok and reason == "KILL_SWITCH_ACTIVE"


def test_open_orders_are_verified_after_submit(tmp_path, monkeypatch):
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "k")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "s")
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, max_notional_usdt=Decimal("1000"))
    client = FakeClient()
    assert trader.execute_plan(plan(), c, client) == "OPEN"
    assert client.query_counts["order"] == 1
    assert client.query_counts["plan_orders"] >= 2


def test_stop_unverified_blocks_all_new_trades(tmp_path):
    c = cfg(tmp_path, max_open_positions=99)
    trader.init_state(c.state_db_path)
    import sqlite3
    with sqlite3.connect(c.state_db_path) as conn:
        conn.execute("INSERT INTO trades(opportunity_id,symbol,status,entry_price,stop_price,target_price,be_trigger_price,qty,notional_usdt,opened_at) VALUES ('bad','MEXC_FUT:BTCUSDT','STOP_UNVERIFIED','100','99','102.5','102','1','100',datetime('now'))")
    ok, reason = trader.can_open(plan(opportunity_id="new", symbol="MEXC_FUT:ETHUSDT"), c)
    assert not ok and reason == "STOP_UNVERIFIED_BLOCK"


def test_mexc_credentials_file_takes_priority_over_env(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "env-key")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "env-secret")
    (tmp_path / "mexc_credentials.json").write_text('{"api_key":"file-key","api_secret":"file-secret"}')

    api_key, api_secret, source = trader.load_mexc_credentials()

    assert (api_key, api_secret) == ("file-key", "file-secret")
    assert source == "mexc_credentials.json"


def test_mexc_credentials_env_fallback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "env-key")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "env-secret")

    api_key, api_secret, source = trader.load_mexc_credentials()

    assert (api_key, api_secret) == ("env-key", "env-secret")
    assert source == "environment"


def test_mexc_credentials_missing_reports_no_secret_source(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(trader.MEXC_API_KEY_ENV, raising=False)
    monkeypatch.delenv(trader.MEXC_API_SECRET_ENV, raising=False)

    assert trader.load_mexc_credentials() == (None, None, "missing")


def test_live_orders_allowed_with_credentials_file_without_env(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(trader.MEXC_API_KEY_ENV, raising=False)
    monkeypatch.delenv(trader.MEXC_API_SECRET_ENV, raising=False)
    (tmp_path / "mexc_credentials.json").write_text('{"api_key":"file-key","api_secret":"file-secret"}')
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, max_notional_usdt=Decimal("1000"))
    client = FakeClient()

    assert trader.execute_plan(plan(), c, client) == "OPEN"
    assert [o[0] for o in client.orders] == ["entry", "stop", "target"]


def test_matching_live_research_plan_passes(tmp_path, monkeypatch):
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "k")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "s")
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, max_notional_usdt=Decimal("1000"))
    client = FakeClient()

    assert trader.execute_plan(plan(), c, client) == "OPEN"
    assert [order[0] for order in client.orders] == ["entry", "stop", "target"]
    assert "PARITY_PASSED" in c.decisions_log_path.read_text()


def test_mismatched_entry_blocks_execution(tmp_path, monkeypatch):
    monkeypatch.setattr(
        trader, "recompute_research_plan_for_live", lambda live_plan, config, client: plan(entry_price=Decimal("100.01"))
    )
    c = cfg(tmp_path, live_trading_enabled=False, dry_run=True)
    client = FakeClient()

    assert trader.execute_plan(plan(), c, client) == "PARITY_FAILED"
    assert client.orders == []
    assert "PARITY_FAILED" in c.decisions_log_path.read_text()


def test_mismatched_stop_blocks_execution(tmp_path, monkeypatch):
    monkeypatch.setattr(
        trader, "recompute_research_plan_for_live", lambda live_plan, config, client: plan(stop_price=Decimal("98.99"))
    )
    c = cfg(tmp_path, live_trading_enabled=False, dry_run=True)
    client = FakeClient()

    assert trader.execute_plan(plan(), c, client) == "PARITY_FAILED"
    assert client.orders == []


def test_parity_unavailable_blocks_execution(tmp_path, monkeypatch):
    monkeypatch.setattr(trader, "recompute_research_plan_for_live", lambda live_plan, config, client: None)
    c = cfg(tmp_path, live_trading_enabled=False, dry_run=True)
    client = FakeClient()

    assert trader.execute_plan(plan(), c, client) == "PARITY_UNAVAILABLE"
    assert client.orders == []
    assert "PARITY_UNAVAILABLE" in c.decisions_log_path.read_text()


def test_live_mode_parity_failure_triggers_kill_switch(tmp_path, monkeypatch):
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "k")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "s")
    monkeypatch.setattr(
        trader, "recompute_research_plan_for_live", lambda live_plan, config, client: plan(entry_price=Decimal("100.01"))
    )
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, max_notional_usdt=Decimal("1000"))
    client = FakeClient()

    assert trader.execute_plan(plan(), c, client) == "PARITY_FAILED"
    assert trader.is_killed(c.state_db_path)
    assert client.orders == []


def test_dry_run_parity_failure_does_not_place_orders(tmp_path, monkeypatch):
    monkeypatch.setattr(
        trader, "recompute_research_plan_for_live", lambda live_plan, config, client: plan(stop_price=Decimal("98.99"))
    )
    c = cfg(tmp_path, live_trading_enabled=False, dry_run=True)
    client = FakeClient()

    assert trader.execute_plan(plan(), c, client) == "PARITY_FAILED"
    assert client.orders == []
    assert not trader.is_killed(c.state_db_path)


class FakeHealthClient:
    def __init__(self, fail_step=None):
        self.fail_step = fail_step
        self.calls = []

    def _call(self, name, payload):
        self.calls.append(name)
        if self.fail_step == name:
            raise RuntimeError(f"{name} failed")
        return payload

    def authenticate(self):
        return self._call("authenticate", {"ok": True})

    def query_account(self):
        return self._call("query_account", {"balance": []})

    def query_all_positions(self):
        return self._call("query_all_positions", [])

    def query_all_open_orders(self):
        return self._call("query_all_open_orders", [])


def test_health_check_ready_is_read_only_and_reports_all_steps(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "mexc_credentials.json").write_text('{"api_key":"file-key","api_secret":"file-secret"}')
    client = FakeHealthClient()

    assert trader.run_health_check(cfg(tmp_path), client) is True

    assert client.calls == ["authenticate", "query_account", "query_all_positions", "query_all_open_orders"]
    assert capsys.readouterr().out.splitlines() == [
        "PASS Credentials",
        "PASS Authentication",
        "PASS Account",
        "PASS Positions",
        "PASS Open Orders",
        "OVERALL: READY",
    ]
    assert not (tmp_path / "live_state.sqlite3").exists()
    assert not (tmp_path / "live_decisions.log").exists()
    assert not (tmp_path / "live_orders.log").exists()


def test_health_check_uses_env_credentials_fallback(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(trader.MEXC_API_KEY_ENV, "env-key")
    monkeypatch.setenv(trader.MEXC_API_SECRET_ENV, "env-secret")
    client = FakeHealthClient()

    assert trader.run_health_check(cfg(tmp_path), client) is True

    assert client.calls == ["authenticate", "query_account", "query_all_positions", "query_all_open_orders"]
    assert "OVERALL: READY" in capsys.readouterr().out


def test_health_check_missing_credentials_not_ready(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(trader.MEXC_API_KEY_ENV, raising=False)
    monkeypatch.delenv(trader.MEXC_API_SECRET_ENV, raising=False)
    client = FakeHealthClient()

    assert trader.run_health_check(cfg(tmp_path), client) is False

    assert client.calls == []
    assert capsys.readouterr().out.splitlines() == [
        "FAIL Credentials",
        "Reason: missing MEXC futures API credentials",
        "OVERALL: NOT_READY",
    ]


def test_health_check_reports_failing_step(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "mexc_credentials.json").write_text('{"api_key":"file-key","api_secret":"file-secret"}')
    client = FakeHealthClient(fail_step="query_all_open_orders")

    assert trader.run_health_check(cfg(tmp_path), client) is False

    assert client.calls == ["authenticate", "query_account", "query_all_positions", "query_all_open_orders"]
    assert capsys.readouterr().out.splitlines() == [
        "PASS Credentials",
        "PASS Authentication",
        "PASS Account",
        "PASS Positions",
        "FAIL Open Orders",
        "Reason: query_all_open_orders failed",
        "OVERALL: NOT_READY",
    ]


def test_health_check_cli_exit_code_ready(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "mexc_credentials.json").write_text('{"api_key":"file-key","api_secret":"file-secret"}')
    config_path = tmp_path / "config.json"
    config_path.write_text("{}")
    monkeypatch.setattr(trader, "MexcFuturesClient", lambda api_key, api_secret, base_url: FakeHealthClient())
    monkeypatch.setattr("sys.argv", ["mexc_pole_live_trader.py", "--config", str(config_path), "--health-check"])

    with pytest.raises(SystemExit) as excinfo:
        trader.main()

    assert excinfo.value.code == 0
    assert "OVERALL: READY" in capsys.readouterr().out


def test_health_check_cli_exit_code_not_ready(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.json"
    config_path.write_text("{}")
    monkeypatch.delenv(trader.MEXC_API_KEY_ENV, raising=False)
    monkeypatch.delenv(trader.MEXC_API_SECRET_ENV, raising=False)
    monkeypatch.setattr("sys.argv", ["mexc_pole_live_trader.py", "--config", str(config_path), "--health-check"])

    with pytest.raises(SystemExit) as excinfo:
        trader.main()

    assert excinfo.value.code == 1
    assert "OVERALL: NOT_READY" in capsys.readouterr().out


def test_refresh_live_candles_fetches_missing_incrementally_and_preserves_symbols(tmp_path, monkeypatch):
    import mexc_pole_missed_signal_audit as audit

    monkeypatch.setattr(trader, "refresh_live_candles", ORIGINAL_REFRESH_LIVE_CANDLES)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: 180)
    calls = []

    def fake_fetch(base_url, symbol, start_ts, end_ts, interval, interval_seconds):
        calls.append((base_url, symbol, audit.mexc_contract_symbol(symbol), start_ts, end_ts, interval, interval_seconds))
        return [(start_ts, 1.0, 2.0, 0.5, 1.5)] if start_ts <= end_ts else []

    monkeypatch.setattr(audit, "fetch_mexc_public_candles", fake_fetch)
    c = cfg(tmp_path, candles_db_path=tmp_path / "mexc_live_candles.db", allowed_symbols=("MEXC_FUT:BTCUSDT",), live_candle_backfill_minutes=1)
    trader.init_live_candle_db(c.candles_db_path)
    with sqlite3.connect(c.candles_db_path) as conn:
        conn.execute("INSERT INTO candles(symbol, close_time, open, high, low, close) VALUES (?,?,?,?,?,?)", ("MEXC_FUT:BTCUSDT", 60, 1, 2, 0.5, 1.5))

    assert trader.refresh_live_candles(c) == {"MEXC_FUT:BTCUSDT": 1}

    assert calls == [("https://contract.mexc.com", "MEXC_FUT:BTCUSDT", "BTC_USDT", 120, 180, "Min1", 60)]
    with sqlite3.connect(c.candles_db_path) as conn:
        rows = conn.execute("SELECT symbol, close_time FROM candles ORDER BY close_time").fetchall()
    assert rows == [("MEXC_FUT:BTCUSDT", 60), ("MEXC_FUT:BTCUSDT", 120)]


def test_refresh_live_candles_does_not_duplicate_existing_rows(tmp_path, monkeypatch):
    import mexc_pole_missed_signal_audit as audit

    monkeypatch.setattr(trader, "refresh_live_candles", ORIGINAL_REFRESH_LIVE_CANDLES)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: 120)
    fetch_calls = []

    def fake_fetch(base_url, symbol, start_ts, end_ts, interval, interval_seconds):
        fetch_calls.append((start_ts, end_ts))
        return [(60, 1.0, 2.0, 0.5, 1.5), (120, 1.5, 2.5, 1.0, 2.0)]

    monkeypatch.setattr(audit, "fetch_mexc_public_candles", fake_fetch)
    c = cfg(tmp_path, candles_db_path=tmp_path / "mexc_live_candles.db", allowed_symbols=("MEXC_FUT:ETHUSDT",), live_candle_backfill_minutes=2)

    assert trader.refresh_live_candles(c) == {"MEXC_FUT:ETHUSDT": 2}
    assert trader.refresh_live_candles(c) == {"MEXC_FUT:ETHUSDT": 0}

    assert fetch_calls == [(60, 120)]
    with sqlite3.connect(c.candles_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM candles WHERE symbol='MEXC_FUT:ETHUSDT'").fetchone()[0] == 2


def test_refresh_live_candles_uses_only_fully_closed_end(tmp_path, monkeypatch):
    import mexc_pole_missed_signal_audit as audit

    monkeypatch.setattr(trader, "refresh_live_candles", ORIGINAL_REFRESH_LIVE_CANDLES)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: 240)
    observed = {}

    def fake_fetch(base_url, symbol, start_ts, end_ts, interval, interval_seconds):
        observed["range"] = (start_ts, end_ts)
        return [(start_ts, 1.0, 2.0, 0.5, 1.5)]

    monkeypatch.setattr(audit, "fetch_mexc_public_candles", fake_fetch)
    c = cfg(tmp_path, candles_db_path=tmp_path / "mexc_live_candles.db", allowed_symbols=("MEXC_FUT:SOLUSDT",), live_candle_backfill_minutes=1)

    trader.refresh_live_candles(c)

    assert observed["range"] == (240, 240)


def test_run_once_fail_closed_when_candle_refresh_fails(tmp_path, monkeypatch):
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, allowed_symbols=("MEXC_FUT:BTCUSDT",))
    client = FakeClient(positions=[])
    called = {"plans": False}

    def fail_refresh(config):
        raise RuntimeError("mexc outage")

    def forbidden_generate(config, exchange_client):
        called["plans"] = True
        return [plan()]

    monkeypatch.setattr(trader, "refresh_live_candles", fail_refresh)
    monkeypatch.setattr(trader, "generate_trade_plans", forbidden_generate)

    with pytest.raises(RuntimeError, match="mexc outage"):
        trader.run_once(c, client)
    assert called["plans"] is False
    assert client.orders == []
    events = [json.loads(line) for line in c.decisions_log_path.read_text().splitlines()]
    assert [event["event"] for event in events][-2:] == ["CANDLE_REFRESH_ERROR", "TRADING_BLOCKED"]
    assert events[-1]["reason"] == "CANDLE_REFRESH_ERROR"



def _write_warmup_candles(db_path, symbol="MEXC_FUT:BTCUSDT", start=60, gap_at=None, flat_count=None):
    trader.init_live_candle_db(db_path)
    if flat_count is not None:
        rows = [(symbol, start + i * 60, 10000.0, 10000.0, 10000.0, 10000.0) for i in range(flat_count)]
    else:
        closes = [10000.0, 10100.0, 9800.0, 10200.0, 9500.0, 9900.0, 9600.0, 9700.0]
        rows = []
        for i, close in enumerate(closes):
            ts = start + i * 60
            if gap_at is not None and ts == gap_at:
                continue
            rows.append((symbol, ts, close, close, close, close))
    with sqlite3.connect(db_path) as conn:
        conn.executemany("INSERT OR IGNORE INTO candles(symbol, close_time, open, high, low, close) VALUES (?,?,?,?,?,?)", rows)
    return rows


def test_warmup_empty_db_becomes_sufficient_after_refresh(tmp_path, monkeypatch):
    import mexc_pole_missed_signal_audit as audit

    monkeypatch.setattr(trader, "refresh_live_candles", ORIGINAL_REFRESH_LIVE_CANDLES)
    monkeypatch.setattr(trader, "ensure_live_candle_warmup", ORIGINAL_ENSURE_LIVE_CANDLE_WARMUP)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: 480)

    def fake_fetch(base_url, symbol, start_ts, end_ts, interval, interval_seconds):
        closes = [10000.0, 10100.0, 9800.0, 10200.0, 9500.0, 9900.0, 9600.0, 9700.0]
        first = end_ts - (len(closes) - 1) * interval_seconds
        return [(first + i * interval_seconds, close, close, close, close) for i, close in enumerate(closes)]

    monkeypatch.setattr(audit, "fetch_mexc_public_candles", fake_fetch)
    monkeypatch.setattr(trader, "generate_trade_plans", lambda config, exchange_client: [])
    c = cfg(tmp_path, candles_db_path=tmp_path / "mexc_live_candles.db", allowed_symbols=("MEXC_FUT:BTCUSDT",), live_candle_backfill_minutes=8)

    assert trader.run_once(c, FakeClient()) == []

    events = [json.loads(line) for line in c.decisions_log_path.read_text().splitlines()]
    warmup = [event for event in events if event["event"] == "CANDLE_WARMUP_STATUS"][-1]
    assert warmup["warmup_status"] == "OK"
    assert warmup["available_candles"] == 8
    assert warmup["required_candles"] == 7


def test_warmup_iteratively_fetches_until_structural_columns_complete(tmp_path, monkeypatch):
    import mexc_pole_missed_signal_audit as audit

    monkeypatch.setattr(trader, "ensure_live_candle_warmup", ORIGINAL_ENSURE_LIVE_CANDLE_WARMUP)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: 480)
    older_calls = []

    def fake_fetch(base_url, symbol, start_ts, end_ts, interval, interval_seconds):
        older_calls.append((start_ts, end_ts))
        if len(older_calls) == 1:
            closes = [10000.0] * 8
        else:
            closes = [10000.0, 10100.0, 9800.0, 10200.0, 9500.0, 9900.0, 9600.0, 9700.0]
        return [(start_ts + i * interval_seconds, close, close, close, close) for i, close in enumerate(closes)]

    monkeypatch.setattr(audit, "fetch_mexc_public_candles", fake_fetch)
    c = cfg(
        tmp_path,
        candles_db_path=tmp_path / "mexc_live_candles.db",
        allowed_symbols=("MEXC_FUT:BTCUSDT",),
        live_candle_backfill_minutes=8,
        max_live_backfill_minutes=16,
        max_live_history_windows=2,
    )
    _write_warmup_candles(c.candles_db_path, flat_count=8)

    statuses = trader.ensure_live_candle_warmup(c)

    assert older_calls == [(-420, 0), (-900, -480)]
    assert statuses[0].ok
    assert statuses[0].available_candles == 24
    assert statuses[0].pnf_columns >= statuses[0].required_pnf_columns


def test_warmup_stops_at_configured_structural_backfill_limits(tmp_path, monkeypatch):
    import mexc_pole_missed_signal_audit as audit

    monkeypatch.setattr(trader, "ensure_live_candle_warmup", ORIGINAL_ENSURE_LIVE_CANDLE_WARMUP)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: 480)
    older_calls = []

    def fake_fetch(base_url, symbol, start_ts, end_ts, interval, interval_seconds):
        older_calls.append((start_ts, end_ts))
        return [(start_ts + i * interval_seconds, 10000.0, 10000.0, 10000.0, 10000.0) for i in range((end_ts - start_ts) // interval_seconds + 1)]

    monkeypatch.setattr(audit, "fetch_mexc_public_candles", fake_fetch)
    c = cfg(
        tmp_path,
        candles_db_path=tmp_path / "mexc_live_candles.db",
        allowed_symbols=("MEXC_FUT:BTCUSDT",),
        live_candle_backfill_minutes=8,
        max_live_backfill_minutes=16,
        max_live_history_windows=2,
    )
    _write_warmup_candles(c.candles_db_path, flat_count=8)

    statuses = trader.ensure_live_candle_warmup(c)

    assert older_calls == [(-420, 0), (-900, -480)]
    assert statuses[0].available_candles == 24
    assert statuses[0].pnf_columns < statuses[0].required_pnf_columns
    assert not statuses[0].ok


def test_warmup_sufficient_history_passes_without_fetching_more(tmp_path, monkeypatch):
    import mexc_pole_missed_signal_audit as audit

    monkeypatch.setattr(trader, "ensure_live_candle_warmup", ORIGINAL_ENSURE_LIVE_CANDLE_WARMUP)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: 480)
    monkeypatch.setattr(audit, "fetch_mexc_public_candles", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not fetch older history")))
    c = cfg(tmp_path, candles_db_path=tmp_path / "mexc_live_candles.db", allowed_symbols=("MEXC_FUT:BTCUSDT",))
    _write_warmup_candles(c.candles_db_path)

    statuses = trader.ensure_live_candle_warmup(c)

    assert len(statuses) == 1
    assert statuses[0].ok
    assert statuses[0].available_candles == 8
    assert statuses[0].pnf_columns >= statuses[0].required_pnf_columns


def test_warmup_gap_in_history_fails_even_after_older_fetch(tmp_path, monkeypatch):
    import mexc_pole_missed_signal_audit as audit

    monkeypatch.setattr(trader, "ensure_live_candle_warmup", ORIGINAL_ENSURE_LIVE_CANDLE_WARMUP)
    monkeypatch.setattr(audit, "_closed_audit_end", lambda end_ts, interval_seconds=60: 480)
    monkeypatch.setattr(audit, "fetch_mexc_public_candles", lambda *args, **kwargs: [])
    c = cfg(tmp_path, candles_db_path=tmp_path / "mexc_live_candles.db", allowed_symbols=("MEXC_FUT:BTCUSDT",))
    _write_warmup_candles(c.candles_db_path, gap_at=240)

    statuses = trader.ensure_live_candle_warmup(c)

    assert not statuses[0].contiguous
    assert not statuses[0].ok


def test_run_once_fail_closed_when_warmup_is_insufficient(tmp_path, monkeypatch):
    c = cfg(tmp_path, live_trading_enabled=True, dry_run=False, allowed_symbols=("MEXC_FUT:BTCUSDT",))
    client = FakeClient(positions=[])
    called = {"plans": False}
    status = trader.CandleWarmupStatus(
        symbol="MEXC_FUT:BTCUSDT",
        available_candles=5000,
        required_candles=7,
        earliest_ts=60,
        latest_ts=300000,
        contiguous=True,
        pnf_columns=1,
        required_pnf_columns=6,
        latest_required_ts=300000,
    )

    def forbidden_generate(config, exchange_client):
        called["plans"] = True
        return [plan()]

    monkeypatch.setattr(trader, "ensure_live_candle_warmup", lambda config: [status])
    monkeypatch.setattr(trader, "generate_trade_plans", forbidden_generate)

    assert trader.run_once(c, client) == ["CANDLE_WARMUP_INSUFFICIENT"]
    assert called["plans"] is False
    assert client.orders == []
    events = [json.loads(line) for line in c.decisions_log_path.read_text().splitlines()]
    assert "CANDLE_WARMUP_STATUS" in [event["event"] for event in events]
    assert [event["event"] for event in events][-2:] == ["CANDLE_WARMUP_INSUFFICIENT", "TRADING_BLOCKED"]
    assert events[-1]["reason"] == "CANDLE_WARMUP_INSUFFICIENT"
