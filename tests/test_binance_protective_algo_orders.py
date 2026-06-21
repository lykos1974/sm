import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import live_binance_forward_trader as trader


def sample_signal(side="LONG"):
    return trader.TriangleSignal(
        symbol="BINANCE_FUT:SOLUSDT",
        pattern="bullish_triangle" if side == "LONG" else "bearish_triangle",
        side=side,
        trigger_ts=123456,
        entry_price=Decimal("100"),
        stop_price=Decimal("99") if side == "LONG" else Decimal("101"),
        tp1_price=Decimal("102") if side == "LONG" else Decimal("98"),
        tp2_price=Decimal("103") if side == "LONG" else Decimal("97"),
        trigger_column_idx=7,
        support_level=Decimal("99"),
        resistance_level=Decimal("100"),
        break_distance_boxes=Decimal("1"),
        pattern_quality="TEST",
    )


class CapturingBinanceClient(trader.BinanceFuturesClient):
    def __init__(self):
        super().__init__("key", "secret", base_url=trader.BINANCE_DEMO_BASE_URL)
        self.calls = []

    def _request_json(self, method, path, *, params=None, signed=False):
        self.calls.append({"method": method, "path": path, "params": params, "signed": signed})
        return {"algoId": 123, "clientAlgoId": params.get("clientAlgoId")}

    def get_symbol_spec(self, symbol):
        return trader.SymbolSpec(
            symbol=symbol,
            status="TRADING",
            base_asset=symbol.removesuffix("USDT"),
            quote_asset="USDT",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            max_qty=Decimal("100000"),
            min_notional=Decimal("1"),
            price_precision=2,
            quantity_precision=3,
        )


def setup_trade(signal):
    conn = sqlite3.connect(":memory:")
    trader.init_live_tables(conn)
    trader.record_trade(
        conn,
        signal,
        notional_usdt=Decimal("1"),
        exchange_order_id="entry-1",
        status="POSITION_OPEN",
        dry_run=False,
        decision="ORDER_SENT",
        raw_order_response={"order_request": {"quantity": "0.01"}},
    )
    trade_id = conn.execute("SELECT id FROM live_trades_binance").fetchone()[0]
    return conn, trade_id


def test_submit_algo_order_uses_fapi_algo_order_endpoint():
    client = CapturingBinanceClient()
    response = client.submit_algo_order({"symbol": "SOLUSDT", "clientAlgoId": "abc"})

    assert response["algoId"] == 123
    assert client.calls == [
        {
            "method": "POST",
            "path": "/fapi/v1/algoOrder",
            "params": {"symbol": "SOLUSDT", "clientAlgoId": "abc"},
            "signed": True,
        }
    ]


def test_long_protective_orders_use_sell_long():
    stop_order, tp_order = trader.build_protective_algo_orders(trade_id=7, signal=sample_signal("LONG"))

    assert stop_order["side"] == "SELL"
    assert stop_order["positionSide"] == "LONG"
    assert stop_order["type"] == "STOP_MARKET"
    assert stop_order["triggerPrice"] == "99"
    assert tp_order["side"] == "SELL"
    assert tp_order["positionSide"] == "LONG"
    assert tp_order["type"] == "TAKE_PROFIT_MARKET"
    assert tp_order["triggerPrice"] == "103"


def test_short_protective_orders_use_buy_short():
    stop_order, tp_order = trader.build_protective_algo_orders(trade_id=7, signal=sample_signal("SHORT"))

    assert stop_order["side"] == "BUY"
    assert stop_order["positionSide"] == "SHORT"
    assert stop_order["type"] == "STOP_MARKET"
    assert stop_order["triggerPrice"] == "101"
    assert tp_order["side"] == "BUY"
    assert tp_order["positionSide"] == "SHORT"
    assert tp_order["type"] == "TAKE_PROFIT_MARKET"
    assert tp_order["triggerPrice"] == "97"


def test_close_position_orders_send_no_quantity_or_reduce_only():
    stop_order, tp_order = trader.build_protective_algo_orders(trade_id=7, signal=sample_signal("LONG"))

    for order in (stop_order, tp_order):
        assert order["algoType"] == "CONDITIONAL"
        assert order["closePosition"] == "true"
        assert order["workingType"] == "MARK_PRICE"
        assert "quantity" not in order
        assert "reduceOnly" not in order


def test_fully_attached_ids_skip_everything_and_mark_attached():
    conn, trade_id = setup_trade(sample_signal("LONG"))
    conn.execute(
        """
        UPDATE live_trades_binance
        SET stop_algo_id = 'sl-existing', tp_algo_id = 'tp-existing', protective_orders_status = 'ATTACH_FAILED'
        WHERE id = ?
        """,
        (trade_id,),
    )
    conn.commit()
    client = CapturingBinanceClient()

    trader.attach_protective_algo_orders(conn, client, trade_id, sample_signal("LONG"))

    assert client.calls == []
    row = conn.execute(
        "SELECT stop_algo_id, tp_algo_id, protective_orders_status FROM live_trades_binance WHERE id = ?",
        (trade_id,),
    ).fetchone()
    assert row == ("sl-existing", "tp-existing", "ATTACHED")


def test_attached_status_skips_everything_even_if_ids_missing():
    conn, trade_id = setup_trade(sample_signal("LONG"))
    conn.execute(
        """
        UPDATE live_trades_binance
        SET stop_algo_id = NULL, tp_algo_id = NULL, protective_orders_status = 'ATTACHED'
        WHERE id = ?
        """,
        (trade_id,),
    )
    conn.commit()
    client = CapturingBinanceClient()

    trader.attach_protective_algo_orders(conn, client, trade_id, sample_signal("LONG"))

    assert client.calls == []
    row = conn.execute(
        "SELECT stop_algo_id, tp_algo_id, protective_orders_status FROM live_trades_binance WHERE id = ?",
        (trade_id,),
    ).fetchone()
    assert row == (None, None, "ATTACHED")


def test_failed_tp_attach_preserves_stop_algo_id_and_marks_failed():
    class StopThenFailClient(CapturingBinanceClient):
        def _request_json(self, method, path, *, params=None, signed=False):
            self.calls.append({"method": method, "path": path, "params": params, "signed": signed})
            if params["type"] == "STOP_MARKET":
                return {"algoId": "stop-123", "clientAlgoId": params["clientAlgoId"]}
            raise RuntimeError("tp attach failed")

    signal = sample_signal("LONG")
    conn, trade_id = setup_trade(signal)
    client = StopThenFailClient()

    trader.attach_protective_algo_orders(conn, client, trade_id, signal)

    row = conn.execute(
        "SELECT stop_algo_id, tp_algo_id, protective_orders_status, protective_orders_error FROM live_trades_binance WHERE id = ?",
        (trade_id,),
    ).fetchone()
    assert row[0] == "stop-123"
    assert row[1] is None
    assert row[2] == "ATTACH_FAILED"
    assert "tp attach failed" in row[3]


def test_retry_after_stop_success_submits_only_missing_tp():
    conn, trade_id = setup_trade(sample_signal("LONG"))
    conn.execute(
        """
        UPDATE live_trades_binance
        SET stop_algo_id = 'stop-existing', tp_algo_id = NULL, protective_orders_status = 'ATTACH_FAILED'
        WHERE id = ?
        """,
        (trade_id,),
    )
    conn.commit()
    client = CapturingBinanceClient()

    trader.attach_protective_algo_orders(conn, client, trade_id, sample_signal("LONG"))

    assert [call["params"]["type"] for call in client.calls] == ["TAKE_PROFIT_MARKET"]
    row = conn.execute(
        "SELECT stop_algo_id, tp_algo_id, protective_orders_status, protective_orders_error FROM live_trades_binance WHERE id = ?",
        (trade_id,),
    ).fetchone()
    assert row[0] == "stop-existing"
    assert row[1] == "123"
    assert row[2] == "ATTACHED"
    assert row[3] is None


def test_retry_after_tp_success_submits_only_missing_stop():
    conn, trade_id = setup_trade(sample_signal("LONG"))
    conn.execute(
        """
        UPDATE live_trades_binance
        SET stop_algo_id = NULL, tp_algo_id = 'tp-existing', protective_orders_status = 'ATTACH_FAILED'
        WHERE id = ?
        """,
        (trade_id,),
    )
    conn.commit()
    client = CapturingBinanceClient()

    trader.attach_protective_algo_orders(conn, client, trade_id, sample_signal("LONG"))

    assert [call["params"]["type"] for call in client.calls] == ["STOP_MARKET"]
    row = conn.execute(
        "SELECT stop_algo_id, tp_algo_id, protective_orders_status, protective_orders_error FROM live_trades_binance WHERE id = ?",
        (trade_id,),
    ).fetchone()
    assert row[0] == "123"
    assert row[1] == "tp-existing"
    assert row[2] == "ATTACHED"
    assert row[3] is None


def test_retry_missing_tp_failure_preserves_existing_stop_id():
    class FailTpClient(CapturingBinanceClient):
        def _request_json(self, method, path, *, params=None, signed=False):
            self.calls.append({"method": method, "path": path, "params": params, "signed": signed})
            raise RuntimeError("tp retry failed")

    conn, trade_id = setup_trade(sample_signal("LONG"))
    conn.execute(
        """
        UPDATE live_trades_binance
        SET stop_algo_id = 'stop-existing', tp_algo_id = NULL, protective_orders_status = 'ATTACH_FAILED'
        WHERE id = ?
        """,
        (trade_id,),
    )
    conn.commit()
    client = FailTpClient()

    trader.attach_protective_algo_orders(conn, client, trade_id, sample_signal("LONG"))

    assert [call["params"]["type"] for call in client.calls] == ["TAKE_PROFIT_MARKET"]
    row = conn.execute(
        "SELECT stop_algo_id, tp_algo_id, protective_orders_status, protective_orders_error FROM live_trades_binance WHERE id = ?",
        (trade_id,),
    ).fetchone()
    assert row[0] == "stop-existing"
    assert row[1] is None
    assert row[2] == "ATTACH_FAILED"
    assert "tp retry failed" in row[3]


def test_xrp_like_floating_trigger_prices_are_quantized_before_submission():
    signal = trader.TriangleSignal(
        symbol="BINANCE_FUT:XRPUSDT",
        pattern="execution_intent",
        side="LONG",
        trigger_ts=123456,
        entry_price=Decimal("1.1500"),
        stop_price=Decimal("1.1300000000000001"),
        tp1_price=Decimal("1.1600"),
        tp2_price=Decimal("1.1699999999999993"),
        trigger_column_idx=7,
        support_level=Decimal("1.13"),
        resistance_level=Decimal("1.15"),
        break_distance_boxes=Decimal("1"),
        pattern_quality="TEST",
    )

    class XrpSpecClient(CapturingBinanceClient):
        def get_symbol_spec(self, symbol):
            return trader.SymbolSpec(
                symbol=symbol,
                status="TRADING",
                base_asset="XRP",
                quote_asset="USDT",
                tick_size=Decimal("0.0001"),
                step_size=Decimal("0.1"),
                min_qty=Decimal("0.1"),
                max_qty=Decimal("1000000"),
                min_notional=Decimal("1"),
                price_precision=4,
                quantity_precision=1,
            )

    conn, trade_id = setup_trade(signal)
    client = XrpSpecClient()

    trader.attach_protective_algo_orders(conn, client, trade_id, signal)

    submitted = [call["params"] for call in client.calls]
    assert [order["triggerPrice"] for order in submitted] == ["1.1300", "1.1700"]
    assert all("000000000000" not in order["triggerPrice"] for order in submitted)
