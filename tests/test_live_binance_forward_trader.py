import argparse
import os
import sqlite3
import tempfile
import unittest
from decimal import Decimal

import live_binance_forward_trader as trader


EXCHANGE_INFO = {
    "symbols": [
        {
            "symbol": "SOLUSDT",
            "status": "TRADING",
            "baseAsset": "SOL",
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.0100"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "0.50"},
            ],
        }
    ]
}


def sample_signal(trigger_ts=123456):
    return trader.TriangleSignal(
        symbol="BINANCE_FUT:SOLUSDT",
        pattern="bullish_triangle",
        side="LONG",
        trigger_ts=trigger_ts,
        entry_price=Decimal("100"),
        stop_price=Decimal("99"),
        tp1_price=Decimal("102"),
        tp2_price=Decimal("103"),
        trigger_column_idx=7,
        support_level=Decimal("99"),
        resistance_level=Decimal("100"),
        break_distance_boxes=Decimal("1"),
        pattern_quality="STRICT_CONSECUTIVE_5_COL_TRIANGLE_UP_BREAK",
    )


class StaticClient:
    has_credentials = False

    def get_symbol_spec(self, symbol):
        return trader.parse_symbol_spec(EXCHANGE_INFO, symbol)


class LiveClient(StaticClient):
    has_credentials = True

    def get_position_mode(self):
        return {"dualSidePosition": False}


class BinanceForwardTraderTests(unittest.TestCase):
    def test_order_signing_uses_timestamp_and_hmac_sha256(self):
        client = trader.BinanceFuturesClient("key", "secret")
        signed = client._signed_params({"symbol": "SOLUSDT", "side": "BUY"}, timestamp=1700000000000)
        self.assertEqual(signed["timestamp"], 1700000000000)
        self.assertIn("signature", signed)
        query = "symbol=SOLUSDT&side=BUY&recvWindow=5000&timestamp=1700000000000"
        expected = trader.hmac.new(b"secret", query.encode("utf-8"), trader.hashlib.sha256).hexdigest()
        self.assertEqual(signed["signature"], expected)

    def test_exchange_info_filter_parsing(self):
        spec = trader.parse_symbol_spec(EXCHANGE_INFO, "SOLUSDT")
        self.assertEqual(spec.tick_size, Decimal("0.0100"))
        self.assertEqual(spec.step_size, Decimal("0.001"))
        self.assertEqual(spec.min_qty, Decimal("0.001"))
        self.assertEqual(spec.min_notional, Decimal("0.50"))
        self.assertEqual(spec.status, "TRADING")

    def test_dry_run_db_state_duplicate(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        signal = sample_signal()
        spec, order, reason = trader.validate_guards(
            conn, StaticClient(), signal, notional_usdt=Decimal("1"), live_enabled=False
        )
        self.assertIsNone(reason)
        self.assertEqual(order["type"], "LIMIT")
        trader.record_signal(conn, signal, decision="DRY_RUN", block_reason=None, dry_run=True, notional_usdt=Decimal("1"))
        self.assertTrue(trader.signal_exists(conn, signal))
        _spec2, _order2, reason2 = trader.validate_guards(
            conn, StaticClient(), signal, notional_usdt=Decimal("1"), live_enabled=False
        )
        self.assertEqual(reason2, "duplicate signal for same symbol/pattern/trigger timestamp")
        trader.set_last_processed_close_time(conn, signal.symbol, 999)
        self.assertEqual(trader.get_last_processed_close_time(conn, signal.symbol), 999)

    def test_order_construction_safety_guards(self):
        signal = sample_signal()
        spec = trader.parse_symbol_spec(EXCHANGE_INFO, "SOLUSDT")
        order, reason = trader.build_entry_order(signal, spec, Decimal("1"))
        self.assertIsNone(reason)
        self.assertEqual(order["type"], "LIMIT")
        self.assertNotIn("stopPrice", order)
        self.assertNotIn("reduceOnly", order)
        self.assertLessEqual(Decimal(order["quantity"]) * Decimal(order["price"]), Decimal("1"))

        close, close_reason = trader.build_reduce_only_close_order(
            trade_id=1,
            symbol=signal.symbol,
            side="LONG",
            exit_price=Decimal("103"),
            entry_order=order,
            spec=spec,
        )
        self.assertIsNone(close_reason)
        self.assertEqual(close["type"], "LIMIT")
        self.assertEqual(close["side"], "SELL")
        self.assertEqual(close["reduceOnly"], "true")

        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        os.environ.pop("LIVE_TRADING_ENABLED", None)
        client = trader.BinanceFuturesClient(None, None)
        _spec, _order, live_reason = trader.validate_guards(
            conn, client, sample_signal(trigger_ts=789), notional_usdt=Decimal("1"), live_enabled=True
        )
        self.assertEqual(live_reason, "API credentials missing")

    def test_min_notional_blocks_one_usdt_cap(self):
        fixture = {"symbols": [{**EXCHANGE_INFO["symbols"][0], "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
            {"filterType": "MIN_NOTIONAL", "notional": "5"},
        ]}]}
        spec = trader.parse_symbol_spec(fixture, "SOLUSDT")
        order, reason = trader.build_entry_order(sample_signal(), spec, Decimal("1"))
        self.assertIsNone(order)
        self.assertIn("min order notional cannot support 1 USDT cap", reason)

    def test_self_test_signal_records_once_and_duplicate_blocks_second(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)

        first = trader.process_self_test_signal(
            conn, StaticClient(), notional_usdt=Decimal("1"), symbol="BINANCE_FUT:SOLUSDT"
        )
        second = trader.process_self_test_signal(
            conn, StaticClient(), notional_usdt=Decimal("1"), symbol="BINANCE_FUT:SOLUSDT"
        )

        self.assertTrue(first)
        self.assertFalse(second)
        signal_rows = conn.execute(
            "SELECT decision, notes FROM live_signals_binance WHERE symbol = 'BINANCE_FUT:SOLUSDT'"
        ).fetchall()
        trade_rows = conn.execute(
            "SELECT status, notes FROM live_trades_binance WHERE symbol = 'BINANCE_FUT:SOLUSDT'"
        ).fetchall()
        self.assertEqual(signal_rows, [("DRY_RUN", "SELF_TEST_SIGNAL")])
        self.assertEqual(trade_rows, [("DRY_RUN", "SELF_TEST_SIGNAL")])

    def test_self_test_refuses_live_env_without_dry_run_flag(self):
        original = os.environ.get("LIVE_TRADING_ENABLED")
        os.environ["LIVE_TRADING_ENABLED"] = "1"
        with tempfile.NamedTemporaryFile(suffix=".sqlite3") as handle:
            args = argparse.Namespace(
                db_path=handle.name,
                settings="unused-for-self-test.json",
                dry_run=False,
                notional_usdt="1",
                self_test_signal=True,
            )
            try:
                trader.process_once(args)
                conn = sqlite3.connect(handle.name)
                signal_count = conn.execute("SELECT COUNT(*) FROM live_signals_binance").fetchone()[0]
                trade_count = conn.execute("SELECT COUNT(*) FROM live_trades_binance").fetchone()[0]
            finally:
                if original is None:
                    os.environ.pop("LIVE_TRADING_ENABLED", None)
                else:
                    os.environ["LIVE_TRADING_ENABLED"] = original

        self.assertEqual(signal_count, 0)
        self.assertEqual(trade_count, 0)


if __name__ == "__main__":
    unittest.main()
