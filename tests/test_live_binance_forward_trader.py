import argparse
import io
from contextlib import closing, redirect_stdout
import json
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


def sample_double_signal(trigger_ts=123456, pattern="double_top_breakout"):
    if pattern == "double_bottom_breakdown":
        return trader.TriangleSignal(
            symbol="BINANCE_FUT:SOLUSDT",
            pattern=pattern,
            side="SHORT",
            trigger_ts=trigger_ts,
            entry_price=Decimal("100"),
            stop_price=Decimal("101"),
            tp1_price=Decimal("98"),
            tp2_price=Decimal("97"),
            trigger_column_idx=7,
            support_level=Decimal("100"),
            resistance_level=Decimal("101"),
            break_distance_boxes=Decimal("1"),
            pattern_quality="STRICT_CONSECUTIVE_3_COL_DOUBLE_BOTTOM_BREAKDOWN",
        )
    return trader.TriangleSignal(
        symbol="BINANCE_FUT:SOLUSDT",
        pattern=pattern,
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
        pattern_quality="STRICT_CONSECUTIVE_3_COL_DOUBLE_TOP_BREAKOUT",
    )


class FakeColumn:
    def __init__(self, idx, kind, top, bottom):
        self.idx = idx
        self.kind = kind
        self.top = top
        self.bottom = bottom


class FakePnFEngine:
    columns = []
    signal_name = None

    def __init__(self, profile):
        self.columns = list(self.__class__.columns)

    def update_from_price(self, close_time, close):
        return None

    def latest_signal_name(self):
        return self.__class__.signal_name


class StaticClient:
    has_credentials = False

    def get_symbol_spec(self, symbol):
        return trader.parse_symbol_spec(EXCHANGE_INFO, symbol)


class LiveClient(StaticClient):
    has_credentials = True

    def get_position_mode(self):
        return {"dualSidePosition": False}


class LifecycleClient(LiveClient):
    def __init__(self, status="FILLED"):
        self.submitted_orders = []
        self.status = status

    def submit_order(self, order):
        self.submitted_orders.append(order)
        return {"orderId": 42, "clientOrderId": order["newClientOrderId"], "status": "NEW"}

    def get_order(self, symbol, *, order_id=None, client_order_id=None):
        return {
            "symbol": symbol,
            "orderId": int(order_id),
            "clientOrderId": client_order_id,
            "status": self.status,
            "avgPrice": "100.50" if self.status == "FILLED" else "0",
            "executedQty": "0.009" if self.status in {"FILLED", "PARTIALLY_FILLED"} else "0",
            "updateTime": 1700000000001,
        }

    def get_user_trades(self, symbol, *, order_id=None):
        return [{"orderId": int(order_id), "commission": "0.00001", "commissionAsset": "USDT"}]


class SubmittingClient(LifecycleClient):
    instances = []

    def __init__(self, api_key=None, api_secret=None, *, base_url=trader.BINANCE_BASE_URL):
        super().__init__("NEW")
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.has_credentials = bool(api_key and api_secret)
        SubmittingClient.instances.append(self)

    def get_position_risk(self, symbol):
        return [{"symbol": symbol, "positionAmt": "0"}]


class DemoInitClient:
    instances = []
    has_credentials = False

    def __init__(self, api_key, api_secret, *, base_url=trader.BINANCE_BASE_URL):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        DemoInitClient.instances.append(self)

    def get_symbol_spec(self, symbol):
        return trader.parse_symbol_spec(EXCHANGE_INFO, symbol)


class BinanceForwardTraderTests(unittest.TestCase):

    def test_verbose_market_logs_emit_market_signal_and_order_details(self):
        original_client = trader.BinanceFuturesClient
        original_triangle = trader.detect_latest_strict_triangle
        try:
            trader.BinanceFuturesClient = DemoInitClient
            trader.detect_latest_strict_triangle = lambda symbol, profile, candles: sample_signal(trigger_ts=1)
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = os.path.join(temp_dir, "binance.sqlite3")
                settings_path = os.path.join(temp_dir, "settings.json")
                with open(settings_path, "w", encoding="utf-8") as fh:
                    json.dump({
                        "symbols": ["BINANCE_FUT:SOLUSDT"],
                        "profiles": {"BINANCE_FUT:SOLUSDT": {"name": "t", "box_size": 1.0, "reversal_boxes": 3}},
                    }, fh)
                with closing(sqlite3.connect(db_path)) as conn:
                    conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BINANCE_FUT:SOLUSDT", "1m", 1, 101, 101, 101))
                    conn.commit()

                args = argparse.Namespace(
                    db_path=db_path,
                    settings=settings_path,
                    dry_run=True,
                    demo=True,
                    enable_demo_doubles=False,
                    notional_usdt="1",
                    history_bars=5000,
                    self_test_signal=False,
                    verbose_market_logs=True,
                )
                output = io.StringIO()
                with redirect_stdout(output):
                    trader.process_once(args)
        finally:
            trader.BinanceFuturesClient = original_client
            trader.detect_latest_strict_triangle = original_triangle

        lines = output.getvalue().splitlines()
        self.assertTrue(any(" MARKET BINANCE_FUT:SOLUSDT " in line and '"last_close": 101.0' in line for line in lines))
        self.assertTrue(any(" SIGNAL_DETAIL BINANCE_FUT:SOLUSDT " in line and '"tp2": 103.0' in line for line in lines))
        self.assertTrue(any(" ORDER_DETAIL BINANCE_FUT:SOLUSDT " in line and '"reduce_only": false' in line for line in lines))

    def test_verbose_lifecycle_logs_position_open_detail_on_fill(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        signal = sample_signal()
        spec = trader.parse_symbol_spec(EXCHANGE_INFO, "SOLUSDT")
        order, reason = trader.build_entry_order(signal, spec, Decimal("1"))
        self.assertIsNone(reason)

        output = io.StringIO()
        with redirect_stdout(output):
            trader.record_submitted_entry_order(
                conn,
                LifecycleClient("FILLED"),
                signal,
                order=order,
                notional_usdt=Decimal("1"),
                verbose_market_logs=True,
            )

        lines = output.getvalue().splitlines()
        self.assertTrue(any(" POSITION_OPEN_DETAIL " in line and '"avg_fill_price": 100.5' in line for line in lines))
        self.assertTrue(any('"requested_entry": 100.0' in line and '"position_side": "LONG"' in line for line in lines))

    def test_process_once_logs_loop_scan_no_signal_without_startup(self):
        original_triangle = trader.detect_latest_strict_triangle
        try:
            trader.detect_latest_strict_triangle = lambda symbol, profile, candles: None
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = os.path.join(temp_dir, "binance.sqlite3")
                settings_path = os.path.join(temp_dir, "settings.json")
                with open(settings_path, "w", encoding="utf-8") as fh:
                    json.dump({
                        "symbols": ["BINANCE_FUT:SOLUSDT"],
                        "profiles": {
                            "BINANCE_FUT:SOLUSDT": {"name": "t", "box_size": 1.0, "reversal_boxes": 3}
                        },
                    }, fh)
                with closing(sqlite3.connect(db_path)) as conn:
                    conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BINANCE_FUT:SOLUSDT", "1m", 1, 101, 101, 101))
                    conn.commit()

                args = argparse.Namespace(
                    db_path=db_path,
                    settings=settings_path,
                    dry_run=True,
                    demo=False,
                    enable_demo_doubles=False,
                    notional_usdt="1",
                    history_bars=5000,
                    self_test_signal=False,
                )
                output = io.StringIO()
                with redirect_stdout(output):
                    trader.process_once(args, iteration=17)
                    trader.process_once(args, iteration=18)
        finally:
            trader.detect_latest_strict_triangle = original_triangle

        lines = output.getvalue().splitlines()
        self.assertEqual(sum(" STARTUP " in line for line in lines), 0)
        self.assertTrue(any(" LOOP_BEGIN iteration=17" in line for line in lines))
        self.assertTrue(any(" LOOP_BEGIN iteration=18" in line for line in lines))
        self.assertEqual(sum(" SCAN BINANCE_FUT:SOLUSDT" in line for line in lines), 2)
        self.assertEqual(sum(" NO_SIGNAL BINANCE_FUT:SOLUSDT" in line for line in lines), 2)

    def test_main_logs_startup_once_across_loop_iterations(self):
        original_parse_args = trader.parse_args
        original_process_once = trader.process_once
        original_sleep = trader.time.sleep
        calls = []
        args = argparse.Namespace(
            db_path="unused.sqlite3",
            settings="unused.json",
            dry_run=True,
            demo=True,
            enable_demo_doubles=True,
            notional_usdt="1",
            history_bars=5000,
            self_test_signal=False,
            loop=True,
            poll_seconds=0,
        )

        def fake_process_once(parsed_args, *, iteration=None):
            calls.append(iteration)
            if len(calls) >= 2:
                raise SystemExit

        try:
            trader.parse_args = lambda: args
            trader.process_once = fake_process_once
            trader.time.sleep = lambda seconds: None
            output = io.StringIO()
            with redirect_stdout(output):
                with self.assertRaises(SystemExit):
                    trader.main()
        finally:
            trader.parse_args = original_parse_args
            trader.process_once = original_process_once
            trader.time.sleep = original_sleep

        events = output.getvalue().splitlines()
        self.assertEqual(calls, [1, 2])
        self.assertEqual(sum(" STARTUP " in line for line in events), 1)
        self.assertIn("STARTUP mode=DEMO_DRY_RUN", events[0])

    def test_detect_latest_strict_double_top_breakout(self):
        original_engine = trader.PnFEngine
        try:
            FakePnFEngine.columns = [
                FakeColumn(5, "X", "100", "98"),
                FakeColumn(6, "O", "99", "97"),
                FakeColumn(7, "X", "101", "99"),
            ]
            FakePnFEngine.signal_name = "BUY"
            trader.PnFEngine = FakePnFEngine
            signal = trader.detect_latest_strict_double(
                "BINANCE_FUT:SOLUSDT",
                trader.PnFProfile("test", 1.0, 3),
                [trader.Candle(111, 101.0, 101.0, 101.0)],
            )
        finally:
            trader.PnFEngine = original_engine

        self.assertIsNotNone(signal)
        self.assertEqual(signal.pattern, "double_top_breakout")
        self.assertEqual(signal.side, "LONG")
        self.assertEqual(signal.entry_price, Decimal("100"))
        self.assertEqual(signal.stop_price, Decimal("99.0"))
        self.assertEqual(signal.tp1_price, Decimal("102.0"))
        self.assertEqual(signal.tp2_price, Decimal("103.0"))

    def test_detect_latest_strict_double_bottom_breakdown(self):
        original_engine = trader.PnFEngine
        try:
            FakePnFEngine.columns = [
                FakeColumn(5, "O", "102", "100"),
                FakeColumn(6, "X", "103", "101"),
                FakeColumn(7, "O", "101", "99"),
            ]
            FakePnFEngine.signal_name = "SELL"
            trader.PnFEngine = FakePnFEngine
            signal = trader.detect_latest_strict_double(
                "BINANCE_FUT:SOLUSDT",
                trader.PnFProfile("test", 1.0, 3),
                [trader.Candle(222, 99.0, 99.0, 99.0)],
            )
        finally:
            trader.PnFEngine = original_engine

        self.assertIsNotNone(signal)
        self.assertEqual(signal.pattern, "double_bottom_breakdown")
        self.assertEqual(signal.side, "SHORT")
        self.assertEqual(signal.entry_price, Decimal("100"))
        self.assertEqual(signal.stop_price, Decimal("101.0"))
        self.assertEqual(signal.tp1_price, Decimal("98.0"))
        self.assertEqual(signal.tp2_price, Decimal("97.0"))

    def test_enable_demo_doubles_only_works_with_demo(self):
        original_env = os.environ.copy()
        original_client = trader.BinanceFuturesClient
        original_triangle = trader.detect_latest_strict_triangle
        original_double = trader.detect_latest_strict_double
        SubmittingClient.instances = []
        try:
            os.environ.clear()
            os.environ.update({
                "LIVE_TRADING_ENABLED": "1",
                "BINANCE_FUTURES_API_KEY": "prod-key",
                "BINANCE_FUTURES_API_SECRET": "prod-secret",
            })
            trader.BinanceFuturesClient = SubmittingClient
            trader.detect_latest_strict_triangle = lambda symbol, profile, candles: None
            trader.detect_latest_strict_double = lambda symbol, profile, candles: sample_double_signal(trigger_ts=1)
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = os.path.join(temp_dir, "binance.sqlite3")
                settings_path = os.path.join(temp_dir, "settings.json")
                with open(settings_path, "w", encoding="utf-8") as fh:
                    json.dump({"symbols": ["BINANCE_FUT:SOLUSDT"], "profiles": {"BINANCE_FUT:SOLUSDT": {"name": "t", "box_size": 1.0, "reversal_boxes": 3}}}, fh)
                with closing(sqlite3.connect(db_path)) as conn:
                    conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BINANCE_FUT:SOLUSDT", "1m", 1, 101, 101, 101))
                    conn.commit()
                args = argparse.Namespace(db_path=db_path, settings=settings_path, dry_run=False, demo=False, enable_demo_doubles=True, notional_usdt="1", history_bars=5000, self_test_signal=False)
                trader.process_once(args)
                with closing(sqlite3.connect(db_path)) as conn:
                    signal_count = conn.execute("SELECT COUNT(*) FROM live_signals_binance").fetchone()[0]
        finally:
            trader.BinanceFuturesClient = original_client
            trader.detect_latest_strict_triangle = original_triangle
            trader.detect_latest_strict_double = original_double
            os.environ.clear()
            os.environ.update(original_env)

        self.assertEqual(signal_count, 0)
        self.assertEqual(SubmittingClient.instances[-1].submitted_orders, [])

    def test_production_mode_still_blocks_doubles(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        _spec, order, reason = trader.validate_guards(
            conn, LiveClient(), sample_double_signal(), notional_usdt=Decimal("1"), live_enabled=True
        )
        self.assertIsNone(order)
        self.assertEqual(reason, "pattern outside live allowlist")

        _spec2, order2, reason2 = trader.validate_guards(
            conn, LiveClient(), sample_double_signal(), notional_usdt=Decimal("1"), live_enabled=True, demo=False, allow_demo_doubles=True
        )
        self.assertIsNone(order2)
        self.assertEqual(reason2, "pattern outside live allowlist")


    def test_demo_doubles_dry_run_sends_no_orders(self):
        original_env = os.environ.copy()
        original_client = trader.BinanceFuturesClient
        original_triangle = trader.detect_latest_strict_triangle
        original_double = trader.detect_latest_strict_double
        SubmittingClient.instances = []
        try:
            os.environ.clear()
            os.environ.update({
                "LIVE_TRADING_ENABLED": "1",
                "BINANCE_DEMO_FUTURES_API_KEY": "demo-key",
                "BINANCE_DEMO_FUTURES_API_SECRET": "demo-secret",
            })
            trader.BinanceFuturesClient = SubmittingClient
            trader.detect_latest_strict_triangle = lambda symbol, profile, candles: None
            trader.detect_latest_strict_double = lambda symbol, profile, candles: sample_double_signal(trigger_ts=1)
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = os.path.join(temp_dir, "binance.sqlite3")
                settings_path = os.path.join(temp_dir, "settings.json")
                with open(settings_path, "w", encoding="utf-8") as fh:
                    json.dump({"symbols": ["BINANCE_FUT:SOLUSDT"], "profiles": {"BINANCE_FUT:SOLUSDT": {"name": "t", "box_size": 1.0, "reversal_boxes": 3}}}, fh)
                with closing(sqlite3.connect(db_path)) as conn:
                    conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BINANCE_FUT:SOLUSDT", "1m", 1, 101, 101, 101))
                    conn.commit()
                args = argparse.Namespace(db_path=db_path, settings=settings_path, dry_run=True, demo=True, enable_demo_doubles=True, notional_usdt="1", history_bars=5000, self_test_signal=False)
                trader.process_once(args)
                with closing(sqlite3.connect(db_path)) as conn:
                    signal_count = conn.execute("SELECT COUNT(*) FROM live_signals_binance").fetchone()[0]
        finally:
            trader.BinanceFuturesClient = original_client
            trader.detect_latest_strict_triangle = original_triangle
            trader.detect_latest_strict_double = original_double
            os.environ.clear()
            os.environ.update(original_env)

        self.assertEqual(signal_count, 0)
        self.assertEqual(SubmittingClient.instances[-1].submitted_orders, [])

    def test_demo_live_double_path_builds_limit_order_and_notes(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        signal = sample_double_signal()
        _spec, order, reason = trader.validate_guards(
            conn, LiveClient(), signal, notional_usdt=Decimal("1"), live_enabled=True, demo=True, allow_demo_doubles=True
        )
        self.assertIsNone(reason)
        self.assertEqual(order["type"], "LIMIT")
        self.assertEqual(order["side"], "BUY")
        result = trader.record_submitted_entry_order(conn, LifecycleClient("NEW"), signal, order=order, notional_usdt=Decimal("1"))
        self.assertEqual(result["lifecycle"]["entry_order_status"], "NEW")
        row = conn.execute("SELECT pattern, notes FROM live_signals_binance").fetchone()
        self.assertEqual(row[0], "double_top_breakout")
        self.assertIn("DEMO_DOUBLE_SMOKE_TEST", row[1])

    def test_demo_double_duplicate_guard_works(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        signal = sample_double_signal()
        _spec, _order, reason = trader.validate_guards(
            conn, LiveClient(), signal, notional_usdt=Decimal("1"), live_enabled=True, demo=True, allow_demo_doubles=True
        )
        self.assertIsNone(reason)
        trader.record_signal(conn, signal, decision="ORDER_SENT", block_reason=None, dry_run=False, notional_usdt=Decimal("1"))
        _spec2, order2, reason2 = trader.validate_guards(
            conn, LiveClient(), signal, notional_usdt=Decimal("1"), live_enabled=True, demo=True, allow_demo_doubles=True
        )
        self.assertIsNone(order2)
        self.assertEqual(reason2, "duplicate signal for same symbol/pattern/trigger timestamp")

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
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "binance.sqlite3")
            args = argparse.Namespace(
                db_path=db_path,
                settings="unused-for-self-test.json",
                dry_run=False,
                notional_usdt="1",
                self_test_signal=True,
            )
            try:
                trader.process_once(args)
                with closing(sqlite3.connect(db_path)) as conn:
                    signal_count = conn.execute("SELECT COUNT(*) FROM live_signals_binance").fetchone()[0]
                    trade_count = conn.execute("SELECT COUNT(*) FROM live_trades_binance").fetchone()[0]
            finally:
                if original is None:
                    os.environ.pop("LIVE_TRADING_ENABLED", None)
                else:
                    os.environ["LIVE_TRADING_ENABLED"] = original

        self.assertEqual(signal_count, 0)
        self.assertEqual(trade_count, 0)


    def test_demo_mode_uses_demo_base_url_and_env_vars(self):
        original_env = os.environ.copy()
        original_client = trader.BinanceFuturesClient
        DemoInitClient.instances = []
        try:
            os.environ.clear()
            os.environ.update({
                "BINANCE_FUTURES_API_KEY": "prod-key",
                "BINANCE_FUTURES_API_SECRET": "prod-secret",
                "BINANCE_DEMO_FUTURES_API_KEY": "demo-key",
                "BINANCE_DEMO_FUTURES_API_SECRET": "demo-secret",
            })
            trader.BinanceFuturesClient = DemoInitClient
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = os.path.join(temp_dir, "binance.sqlite3")
                args = argparse.Namespace(
                    db_path=db_path,
                    settings="unused-for-self-test.json",
                    dry_run=True,
                    demo=True,
                    notional_usdt="1",
                    self_test_signal=True,
                )
                trader.process_once(args)
        finally:
            trader.BinanceFuturesClient = original_client
            os.environ.clear()
            os.environ.update(original_env)

        self.assertEqual(DemoInitClient.instances[-1].base_url, trader.BINANCE_DEMO_BASE_URL)
        self.assertEqual(DemoInitClient.instances[-1].api_key, "demo-key")
        self.assertEqual(DemoInitClient.instances[-1].api_secret, "demo-secret")

    def test_dry_run_self_test_sends_no_orders(self):
        class NoOrdersClient(StaticClient):
            def submit_order(self, order):
                raise AssertionError("dry-run must not submit orders")

        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        self.assertTrue(trader.process_self_test_signal(conn, NoOrdersClient(), notional_usdt=Decimal("1")))

    def test_filled_entry_order_marks_position_open_and_records_fill(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        signal = sample_signal()
        spec = trader.parse_symbol_spec(EXCHANGE_INFO, "SOLUSDT")
        order, reason = trader.build_entry_order(signal, spec, Decimal("1"))
        self.assertIsNone(reason)
        result = trader.record_submitted_entry_order(conn, LifecycleClient("FILLED"), signal, order=order, notional_usdt=Decimal("1"))

        row = conn.execute(
            "SELECT status, entry_order_status, avg_fill_price, executed_qty, entry_commission, entry_slippage FROM live_trades_binance"
        ).fetchone()
        self.assertEqual(row[0], "POSITION_OPEN")
        self.assertEqual(row[1], "FILLED")
        self.assertEqual(row[2], 100.5)
        self.assertEqual(row[3], 0.009)
        self.assertEqual(row[4], 0.00001)
        self.assertEqual(row[5], 0.5)
        self.assertEqual(result["lifecycle"]["entry_order_status"], "FILLED")

    def test_new_or_partially_filled_entry_does_not_trigger_exits(self):
        for status in ("NEW", "PARTIALLY_FILLED"):
            conn = sqlite3.connect(":memory:")
            trader.init_live_tables(conn)
            signal = sample_signal(trigger_ts=123456 if status == "NEW" else 123457)
            spec = trader.parse_symbol_spec(EXCHANGE_INFO, "SOLUSDT")
            order, reason = trader.build_entry_order(signal, spec, Decimal("1"))
            self.assertIsNone(reason)
            client = LifecycleClient(status)
            trader.record_submitted_entry_order(conn, client, signal, order=order, notional_usdt=Decimal("1"))
            conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)")
            conn.execute(
                "INSERT INTO candles(symbol, interval, close_time, high, low) VALUES(?,?,?,?,?)",
                (signal.symbol, "1m", signal.trigger_ts + 60_000, 104, 98),
            )
            conn.commit()
            trader.update_open_trade_exits(conn, client, live_enabled=True)
            row = conn.execute("SELECT status, exit_time FROM live_trades_binance").fetchone()
            self.assertEqual(row[0], "ORDER_SENT")
            self.assertIsNone(row[1])

    def test_reduce_only_close_order_remains_reduce_only_true(self):
        spec = trader.parse_symbol_spec(EXCHANGE_INFO, "SOLUSDT")
        close, reason = trader.build_reduce_only_close_order(
            trade_id=99,
            symbol="BINANCE_FUT:SOLUSDT",
            side="LONG",
            exit_price=Decimal("103"),
            entry_order={"quantity": "0.009"},
            spec=spec,
        )
        self.assertIsNone(reason)
        self.assertEqual(close["reduceOnly"], "true")


if __name__ == "__main__":
    unittest.main()
