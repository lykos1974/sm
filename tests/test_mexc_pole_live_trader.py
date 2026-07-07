from __future__ import annotations

from decimal import Decimal
import http.client
import json

import pytest

import mexc_pole_live_trader as trader


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
