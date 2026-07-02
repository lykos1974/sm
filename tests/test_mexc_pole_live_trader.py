from __future__ import annotations

from decimal import Decimal

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
        return self.positions
    def query_open_orders(self, symbol):
        self.query_counts["open_orders"] += 1
        return self.open_orders
    def query_order(self, order_id):
        self.query_counts["order"] += 1
        return {"orderId": order_id, "state": 3, "dealVol": "1"}
    def query_plan_orders(self, symbol):
        self.query_counts["plan_orders"] += 1
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
