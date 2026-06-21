import argparse
import io
from contextlib import closing, redirect_stdout
from datetime import datetime, timedelta, timezone
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


def sample_pole_motif_signal(trigger_ts=123456):
    return trader.TriangleSignal(
        symbol="BINANCE_FUT:SOLUSDT",
        pattern="pole_motif_low",
        side="LONG",
        trigger_ts=trigger_ts,
        entry_price=Decimal("105"),
        stop_price=Decimal("102"),
        tp1_price=Decimal("111"),
        tp2_price=Decimal("112.5"),
        trigger_column_idx=7,
        support_level=Decimal("102"),
        resistance_level=Decimal("105"),
        break_distance_boxes=Decimal("3"),
        pattern_quality="POLE_MOTIF_DEMO_FORWARD|NEXT_COLUMN_OPEN_ENTRY",
    )


def replace_signal(signal, **overrides):
    values = dict(signal.__dict__)
    values.update(overrides)
    return trader.TriangleSignal(**values)


def create_strategy_setups_db(path, rows):
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE strategy_setups (
            setup_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            status TEXT NOT NULL,
            reference_ts INTEGER NOT NULL,
            breakout_context TEXT,
            pullback_quality TEXT,
            active_leg_boxes INTEGER,
            is_extended_move INTEGER,
            resolution_status TEXT NOT NULL,
            ideal_entry REAL,
            invalidation REAL,
            tp1 REAL,
            tp2 REAL,
            rr1 REAL,
            rr2 REAL
        )
        """
    )
    for row in rows:
        payload = {
            "setup_id": "setup-1",
            "symbol": "BINANCE_FUT:ETHUSDT",
            "side": "LONG",
            "status": "CANDIDATE",
            "reference_ts": 1_700_000_000,
            "breakout_context": "POST_BREAKOUT_PULLBACK",
            "pullback_quality": "HEALTHY",
            "active_leg_boxes": 2,
            "is_extended_move": 0,
            "resolution_status": "PENDING",
            "ideal_entry": 100.0,
            "invalidation": 95.0,
            "tp1": 105.0,
            "tp2": 110.0,
            "rr1": 1.0,
            "rr2": 2.0,
            **row,
        }
        conn.execute(
            """
            INSERT INTO strategy_setups(
                setup_id, symbol, side, status, reference_ts, breakout_context,
                pullback_quality, active_leg_boxes, is_extended_move, resolution_status,
                ideal_entry, invalidation, tp1, tp2, rr1, rr2
            ) VALUES(
                :setup_id, :symbol, :side, :status, :reference_ts, :breakout_context,
                :pullback_quality, :active_leg_boxes, :is_extended_move, :resolution_status,
                :ideal_entry, :invalidation, :tp1, :tp2, :rr1, :rr2
            )
            """,
            payload,
        )
    conn.commit()
    conn.close()


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
        self.cancelled_orders = []
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

    def cancel_order(self, symbol, *, order_id=None, orig_client_order_id=None):
        response = {
            "symbol": symbol,
            "orderId": int(order_id),
            "clientOrderId": orig_client_order_id,
            "status": "CANCELED",
        }
        self.cancelled_orders.append(response)
        return response


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


class ForceDemoClient(DemoInitClient):
    has_credentials = False

    def __init__(self, api_key, api_secret, *, base_url=trader.BINANCE_BASE_URL):
        super().__init__(api_key, api_secret, base_url=base_url)
        self.has_credentials = bool(api_key and api_secret)


class BinanceForwardTraderTests(unittest.TestCase):
    def test_setup_consumer_baseline_filter_and_candidate_logging(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "strategy_validation.db")
            create_strategy_setups_db(
                db_path,
                [
                    {"setup_id": "valid-setup"},
                    {"setup_id": "watch-row", "status": "WATCH"},
                    {"setup_id": "short-row", "side": "SHORT"},
                    {"setup_id": "extended-row", "is_extended_move": 1},
                    {"setup_id": "wrong-leg-row", "active_leg_boxes": 3},
                    {"setup_id": "resolved-row", "resolution_status": "TP2"},
                ],
            )
            args = argparse.Namespace(strategy_setups_db=db_path, state_db_path=os.path.join(temp_dir, "live_state.db"), demo=True)
            output = io.StringIO()
            with redirect_stdout(output):
                trader.process_setup_execution_once(args, iteration=3)

        logs = output.getvalue()
        self.assertIn("SETUP_CONSUMER_MODE ENABLED", logs)
        self.assertIn("SETUP_ROWS_FOUND 1", logs)
        self.assertIn("SETUP_EXECUTION_CANDIDATE", logs)
        self.assertIn('"setup_id": "valid-setup"', logs)
        self.assertIn('"entry": "100.0"', logs)
        self.assertNotIn("watch-row", logs)
        self.assertNotIn("short-row", logs)

    def test_setup_consumer_rejects_invalid_long_risk_levels(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "strategy_validation.db")
            create_strategy_setups_db(
                db_path,
                [
                    {"setup_id": "bad-risk", "ideal_entry": 100.0, "invalidation": 101.0, "tp1": 105.0, "tp2": 110.0},
                ],
            )
            output = io.StringIO()
            with redirect_stdout(output):
                trader.process_setup_execution_once(argparse.Namespace(strategy_setups_db=db_path, state_db_path=os.path.join(temp_dir, "live_state.db"), demo=True), iteration=1)

        logs = output.getvalue()
        self.assertIn("SETUP_ROWS_FOUND 1", logs)
        self.assertIn("SETUP_EXECUTION_REJECTED", logs)
        self.assertIn("invalid LONG risk levels", logs)
        self.assertIn("NO_EXECUTABLE_SETUPS", logs)

    def test_setup_consumer_opens_strategy_setups_db_read_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "strategy_validation.db")
            create_strategy_setups_db(db_path, [{"setup_id": "valid-setup"}])
            with closing(trader.connect_strategy_setups_db_readonly(db_path)) as conn:
                self.assertEqual(len(trader.load_executable_strategy_setups(conn)), 1)
                with self.assertRaises(sqlite3.OperationalError):
                    conn.execute("UPDATE strategy_setups SET status = 'WATCH' WHERE setup_id = 'valid-setup'")

    def test_setup_consumer_does_not_submit_orders_or_initialize_binance_client(self):
        class ForbiddenClient:
            def __init__(self, *args, **kwargs):
                raise AssertionError("Binance client must not be initialized in read-only setup consumer mode")

        original_client = trader.BinanceFuturesClient
        try:
            trader.BinanceFuturesClient = ForbiddenClient
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = os.path.join(temp_dir, "strategy_validation.db")
                create_strategy_setups_db(db_path, [{"setup_id": "valid-setup"}])
                output = io.StringIO()
                with redirect_stdout(output):
                    trader.process_setup_execution_once(argparse.Namespace(strategy_setups_db=db_path, state_db_path=os.path.join(temp_dir, "live_state.db"), demo=True), iteration=1)
        finally:
            trader.BinanceFuturesClient = original_client

        logs = output.getvalue()
        self.assertIn("SETUP_EXECUTION_CANDIDATE", logs)
        self.assertIn("EXECUTION_INTENT_CREATED", logs)

    def test_setup_consumer_binance_demo_accepts_binance_symbol_universe(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "strategy_validation.db")
            state_db_path = os.path.join(temp_dir, "live_state.db")
            create_strategy_setups_db(
                db_path,
                [
                    {"setup_id": "btc-raw", "symbol": "BTCUSDT"},
                    {"setup_id": "eth-raw", "symbol": "ETHUSDT"},
                    {"setup_id": "btc-prefixed", "symbol": "BINANCE_FUT:BTCUSDT"},
                ],
            )
            output = io.StringIO()
            with redirect_stdout(output):
                trader.process_setup_execution_once(
                    argparse.Namespace(strategy_setups_db=db_path, state_db_path=state_db_path, demo=True),
                    iteration=1,
                )

        logs = output.getvalue()
        self.assertIn("SETUP_ROWS_FOUND 3", logs)
        self.assertIn("SETUP_ROWS_ACCEPTED 3", logs)
        self.assertIn("SETUP_ROWS_REJECTED 0", logs)
        self.assertEqual(logs.count("SETUP_EXECUTION_CANDIDATE"), 3)
        self.assertIn('"symbol": "BTCUSDT"', logs)
        self.assertIn('"symbol": "ETHUSDT"', logs)
        self.assertIn('"symbol": "BINANCE_FUT:BTCUSDT"', logs)

    def test_setup_consumer_binance_demo_rejects_mexc_symbols_with_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "strategy_validation.db")
            state_db_path = os.path.join(temp_dir, "live_state.db")
            create_strategy_setups_db(
                db_path,
                [
                    {"setup_id": "mexc-hype", "symbol": "MEXC_FUT:HYPEUSDT"},
                    {"setup_id": "mexc-eth", "symbol": "MEXC_FUT:ETHUSDT"},
                    {"setup_id": "raw-btc", "symbol": "BTCUSDT"},
                ],
            )
            output = io.StringIO()
            with redirect_stdout(output):
                trader.process_setup_execution_once(
                    argparse.Namespace(strategy_setups_db=db_path, state_db_path=state_db_path, demo=True),
                    iteration=1,
                )

        logs = output.getvalue()
        self.assertIn("SETUP_ROWS_FOUND 3", logs)
        self.assertIn("SETUP_ROWS_ACCEPTED 1", logs)
        self.assertIn("SETUP_ROWS_REJECTED 2", logs)
        self.assertEqual(logs.count("SETUP_EXECUTION_CANDIDATE"), 1)
        self.assertEqual(logs.count("SETUP_EXECUTION_REJECTED"), 2)
        self.assertIn('"reason": "UNSUPPORTED_EXECUTION_VENUE"', logs)
        self.assertIn('"setup_id": "mexc-hype"', logs)
        self.assertIn('"setup_id": "mexc-eth"', logs)

    def test_setup_consumer_tracks_seen_setup_ids_without_duplicate_candidates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "strategy_validation.db")
            state_db_path = os.path.join(temp_dir, "live_state.db")
            create_strategy_setups_db(db_path, [{"setup_id": "same-setup", "symbol": "BTCUSDT"}])
            args = argparse.Namespace(strategy_setups_db=db_path, state_db_path=state_db_path, demo=True)
            first_output = io.StringIO()
            with redirect_stdout(first_output):
                trader.process_setup_execution_once(args, iteration=1)
            second_output = io.StringIO()
            with redirect_stdout(second_output):
                trader.process_setup_execution_once(args, iteration=2)

            with closing(sqlite3.connect(state_db_path)) as conn:
                rows = conn.execute("SELECT setup_id, first_seen_ts, last_seen_ts FROM executed_setup_candidates").fetchall()
                intent_rows = conn.execute(
                    """
                    SELECT setup_id, symbol, side, entry, stop, tp1, tp2, rr1, rr2, reference_ts, intent_status
                    FROM execution_intents
                    """
                ).fetchall()

        first_logs = first_output.getvalue()
        self.assertIn("SETUP_EXECUTION_CANDIDATE", first_logs)
        self.assertIn("EXECUTION_INTENT_CREATED", first_logs)
        self.assertIn("INTENTS_CREATED 1", first_logs)
        self.assertIn("INTENTS_SKIPPED 0", first_logs)
        second_logs = second_output.getvalue()
        self.assertNotIn("SETUP_EXECUTION_CANDIDATE", second_logs)
        self.assertIn("SETUP_ALREADY_SEEN", second_logs)
        self.assertIn("EXECUTION_INTENT_ALREADY_EXISTS", second_logs)
        self.assertIn("INTENTS_CREATED 0", second_logs)
        self.assertIn("INTENTS_SKIPPED 1", second_logs)
        self.assertIn('"setup_id": "same-setup"', second_logs)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "same-setup")
        self.assertEqual(len(intent_rows), 1)
        self.assertEqual(intent_rows[0], ("same-setup", "BTCUSDT", "LONG", "100.0", "95.0", "105.0", "110.0", "1.0", "2.0", 1_700_000_000, "NEW"))


    def test_execution_intent_state_db_survives_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "strategy_validation.db")
            state_db_path = os.path.join(temp_dir, "live_state.db")
            create_strategy_setups_db(db_path, [{"setup_id": "restart-setup", "symbol": "BTCUSDT"}])
            args = argparse.Namespace(strategy_setups_db=db_path, state_db_path=state_db_path, demo=True)

            with redirect_stdout(io.StringIO()):
                trader.process_setup_execution_once(args, iteration=1)

            with closing(sqlite3.connect(state_db_path)) as restarted_conn:
                rows = restarted_conn.execute(
                    "SELECT setup_id, symbol, intent_status FROM execution_intents WHERE setup_id = ?",
                    ("restart-setup",),
                ).fetchall()

        self.assertEqual(rows, [("restart-setup", "BTCUSDT", "NEW")])

    def test_main_dispatches_to_setup_consumer_only_when_flag_is_enabled(self):
        original_parse_args = trader.parse_args
        original_process_once = trader.process_once
        original_setup_once = trader.process_setup_execution_once
        try:
            calls = []
            trader.process_once = lambda parsed_args, *, iteration=None: calls.append(("signal", iteration))
            trader.process_setup_execution_once = lambda parsed_args, *, iteration=None: calls.append(("setup", iteration))
            trader.parse_args = lambda: argparse.Namespace(
                db_path="unused-market.db",
                state_db_path="unused-state.db",
                settings="unused-settings.json",
                dry_run=True,
                demo=True,
                export_trade_journal=None,
                reconcile_positions=False,
                consume_strategy_setups=False,
                loop=False,
                poll_seconds=0,
            )
            with redirect_stdout(io.StringIO()):
                trader.main()
            trader.parse_args = lambda: argparse.Namespace(
                db_path="unused-market.db",
                state_db_path="unused-state.db",
                settings="unused-settings.json",
                dry_run=True,
                demo=True,
                export_trade_journal=None,
                reconcile_positions=False,
                consume_strategy_setups=True,
                loop=False,
                poll_seconds=0,
            )
            with redirect_stdout(io.StringIO()):
                trader.main()
        finally:
            trader.parse_args = original_parse_args
            trader.process_once = original_process_once
            trader.process_setup_execution_once = original_setup_once

        self.assertEqual(calls, [("signal", 1), ("setup", 1)])

    def test_research_rule_matching_eth_long_watch_setup_passes(self):
        rule = {
            "rule_id": "eth-long-cont-persist-v1",
            "symbol": "BINANCE_FUT:ETHUSDT",
            "side": "LONG",
            "status": "WATCH",
            "breakout_context": "LATE_EXTENSION",
            "pullback_quality": "HEALTHY",
            "trend_regime": "BULLISH_REGIME",
            "continuation_execution_class": "CONTINUATION_EXTENDED_REJECT",
            "entry_distance_bucket": "BELOW_BREAKOUT",
            "active_leg_boxes": {"min": 4, "max": 6},
            "quality_score": {"min": 40, "max": 90},
        }
        setup = {
            "symbol": "BINANCE_FUT:ETHUSDT",
            "side": "LONG",
            "status": "WATCH",
            "breakout_context": "LATE_EXTENSION",
            "pullback_quality": "HEALTHY",
            "trend_regime": "BULLISH_REGIME",
            "continuation_execution_class": "CONTINUATION_EXTENDED_REJECT",
            "entry_distance_bucket": "BELOW_BREAKOUT",
            "active_leg_boxes": 5,
            "quality_score": 70,
        }
        matched, reason = trader.evaluate_research_rule(setup, rule)
        self.assertTrue(matched)
        self.assertIsNone(reason)

    def test_research_rule_rejects_btc_short_wrong_breakout_and_missing_field(self):
        rule = {
            "symbol": "BINANCE_FUT:ETHUSDT",
            "side": "LONG",
            "status": "WATCH",
            "breakout_context": "LATE_EXTENSION",
            "pullback_quality": "HEALTHY",
            "trend_regime": "BULLISH_REGIME",
            "continuation_execution_class": "CONTINUATION_EXTENDED_REJECT",
            "entry_distance_bucket": "BELOW_BREAKOUT",
            "active_leg_boxes": {"min": 4, "max": 6},
            "quality_score": {"min": 40, "max": 90},
        }
        bad_symbol = {"symbol": "BINANCE_FUT:BTCUSDT", "side": "LONG", "status": "WATCH", "breakout_context": "LATE_EXTENSION", "pullback_quality": "HEALTHY", "trend_regime": "BULLISH_REGIME", "continuation_execution_class": "CONTINUATION_EXTENDED_REJECT", "entry_distance_bucket": "BELOW_BREAKOUT", "active_leg_boxes": 5, "quality_score": 70}
        bad_side = {**bad_symbol, "symbol": "BINANCE_FUT:ETHUSDT", "side": "SHORT"}
        bad_context = {**bad_symbol, "symbol": "BINANCE_FUT:ETHUSDT", "breakout_context": "POST_BREAKOUT_PULLBACK"}
        missing_field = dict(bad_symbol)
        missing_field.pop("trend_regime")
        self.assertFalse(trader.evaluate_research_rule(bad_symbol, rule)[0])
        self.assertFalse(trader.evaluate_research_rule(bad_side, rule)[0])
        self.assertFalse(trader.evaluate_research_rule(bad_context, rule)[0])
        matched, reason = trader.evaluate_research_rule(missing_field, rule)
        self.assertFalse(matched)
        self.assertIn("missing required setup field", str(reason))

    def test_research_rule_json_refuses_without_demo_or_dry_run(self):
        args = argparse.Namespace(
            db_path="unused.sqlite3",
            state_db_path="unused-state.sqlite3",
            settings="unused-settings.json",
            dry_run=False,
            demo=False,
            enable_demo_doubles=False,
            notional_usdt="1",
            demo_max_notional_usdt="1",
            history_bars=5000,
            self_test_signal=False,
            verbose_market_logs=False,
            reconcile_positions=False,
            research_rule_json="/tmp/rule.json",
        )
        original_loader = trader.load_research_rule
        original_client = trader.BinanceFuturesClient
        try:
            trader.load_research_rule = lambda _path: {"rule_id": "x"}
            trader.BinanceFuturesClient = DemoInitClient
            with self.assertRaisesRegex(RuntimeError, "requires --demo or --dry-run"):
                trader.process_once(args)
        finally:
            trader.load_research_rule = original_loader
            trader.BinanceFuturesClient = original_client

    def test_research_rule_supports_integer_filters_for_active_leg_boxes(self):
        rule = {
            "symbol": "BINANCE_FUT:ETHUSDT",
            "side": "LONG",
            "status": "WATCH",
            "breakout_context": "LATE_EXTENSION",
            "pullback_quality": "HEALTHY",
            "trend_regime": "BULLISH_REGIME",
            "continuation_execution_class": "CONTINUATION_EXTENDED_REJECT",
            "entry_distance_bucket": "BELOW_BREAKOUT",
            "integer_filters": {"active_leg_boxes": {"allowed": [4, 5, 6]}},
            "numeric_thresholds": {"quality_score": {"min": 40, "max": 90}},
        }
        setup_ok = {
            "symbol": "BINANCE_FUT:ETHUSDT",
            "side": "LONG",
            "status": "WATCH",
            "breakout_context": "LATE_EXTENSION",
            "pullback_quality": "HEALTHY",
            "trend_regime": "BULLISH_REGIME",
            "continuation_execution_class": "CONTINUATION_EXTENDED_REJECT",
            "entry_distance_bucket": "BELOW_BREAKOUT",
            "active_leg_boxes": 5,
            "quality_score": 80,
        }
        setup_bad = dict(setup_ok)
        setup_bad["active_leg_boxes"] = 7
        self.assertTrue(trader.evaluate_research_rule(setup_ok, rule)[0])
        self.assertFalse(trader.evaluate_research_rule(setup_bad, rule)[0])


    def test_runtime_symbol_uses_normalized_binance_symbol_for_candle_lookup(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
        conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BTCUSDT", "1m", 1700000000000, 50000.25, 50001.0, 49999.0))
        conn.commit()

        output = io.StringIO()
        with redirect_stdout(output):
            candles = trader.load_candles(conn, "BINANCE_FUT:BTCUSDT", 5000)

        lines = output.getvalue().splitlines()
        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[-1].close, 50000.25)
        self.assertTrue(any('"runtime_symbol": "BINANCE_FUT:BTCUSDT"' in line for line in lines))
        self.assertTrue(any('"normalized_symbol": "BTCUSDT"' in line for line in lines))
        self.assertTrue(any('"primary_query_symbol": "BINANCE_FUT:BTCUSDT"' in line for line in lines))
        self.assertTrue(any('"fallback_query_symbol": "BTCUSDT"' in line for line in lines))
        self.assertTrue(any("CANDLE_RESULT" in line and '"symbol": "BTCUSDT"' in line and '"rows": 1' in line for line in lines))
    def test_process_once_logs_runtime_compare_with_normalized_db_candles(self):
        original_triangle = trader.detect_latest_strict_triangle
        try:
            trader.detect_latest_strict_triangle = lambda symbol, profile, candles: None
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = os.path.join(temp_dir, "binance.sqlite3")
                settings_path = os.path.join(temp_dir, "settings.json")
                with open(settings_path, "w", encoding="utf-8") as fh:
                    json.dump({
                        "symbols": ["BINANCE_FUT:BTCUSDT"],
                        "profiles": {"BINANCE_FUT:BTCUSDT": {"name": "t", "box_size": 100.0, "reversal_boxes": 3}},
                    }, fh)
                with closing(sqlite3.connect(db_path)) as conn:
                    conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BTCUSDT", "1m", 1700000000000, 50000.25, 50001.0, 49999.0))
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
                    verbose_market_logs=True,
                )
                output = io.StringIO()
                with redirect_stdout(output):
                    trader.process_once(args)
        finally:
            trader.detect_latest_strict_triangle = original_triangle

        lines = output.getvalue().splitlines()
        self.assertTrue(any("CANDLE_QUERY" in line and '"runtime_symbol": "BINANCE_FUT:BTCUSDT"' in line and '"primary_query_symbol": "BINANCE_FUT:BTCUSDT"' in line for line in lines))
        self.assertTrue(any("CANDLE_RESULT" in line and '"symbol": "BTCUSDT"' in line and '"rows": 1' in line and '"last_close": 50000.25' in line for line in lines))
        self.assertTrue(any("MARKET_RUNTIME_COMPARE" in line and "BINANCE_FUT:BTCUSDT" in line and '"latest_candle_close": 50000.25' in line for line in lines))

    def test_load_candles_prefers_prefixed_runtime_symbol(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
        conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BINANCE_FUT:ETHUSDT", "1m", 1, 101.0, 102.0, 100.0))
        conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("ETHUSDT", "1m", 1, 201.0, 202.0, 200.0))
        conn.commit()
        output = io.StringIO()
        with redirect_stdout(output):
            candles = trader.load_candles(conn, "BINANCE_FUT:ETHUSDT", 10)
        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0].close, 101.0)
        self.assertTrue(any("CANDLE_RESULT" in line and '"symbol": "BINANCE_FUT:ETHUSDT"' in line and '"rows": 1' in line for line in output.getvalue().splitlines()))

    def test_load_candles_falls_back_to_unprefixed_symbol(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
        conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("ETHUSDT", "1m", 1, 201.0, 202.0, 200.0))
        conn.commit()
        output = io.StringIO()
        with redirect_stdout(output):
            candles = trader.load_candles(conn, "BINANCE_FUT:ETHUSDT", 10)
        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0].close, 201.0)
        lines = output.getvalue().splitlines()
        self.assertTrue(any("CANDLE_QUERY" in line and '"primary_query_symbol": "BINANCE_FUT:ETHUSDT"' in line and '"fallback_query_symbol": "ETHUSDT"' in line for line in lines))
        self.assertTrue(any("CANDLE_RESULT" in line and '"symbol": "ETHUSDT"' in line and '"rows": 1' in line for line in lines))

    def test_load_candles_no_rows_returns_empty_and_logs_rows_zero(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
        conn.commit()
        output = io.StringIO()
        with redirect_stdout(output):
            candles = trader.load_candles(conn, "BINANCE_FUT:ETHUSDT", 10)
        self.assertEqual(candles, [])
        self.assertTrue(any("CANDLE_RESULT" in line and '"rows": 0' in line for line in output.getvalue().splitlines()))

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
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("SOLUSDT", "1m", 1, 101, 101, 101))
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

    def test_pole_motif_signal_detail_includes_staleness_diagnostics(self):
        profile = trader.PnFProfile("POLE_DEMO_bs1_rev3", 1.0, 3)
        signal = sample_pole_motif_signal(trigger_ts=1700000000000)
        output = io.StringIO()

        with redirect_stdout(output):
            trader.log_signal_detail(signal, profile, 100.0, latest_candle_close_time=1700000120000)

        line = output.getvalue()
        self.assertIn(" SIGNAL_DETAIL BINANCE_FUT:SOLUSDT ", line)
        self.assertIn('"CURRENT_PRICE": 100.0', line)
        self.assertIn('"ENTRY_PRICE": 105.0', line)
        self.assertIn('"ENTRY_DISTANCE_PERCENT": 5.0', line)
        self.assertIn('"TRIGGER_TIMESTAMP": 1700000000000', line)
        self.assertIn('"LATEST_CANDLE_CLOSE_TIME": 1700000120000', line)
        self.assertIn('"TRIGGER_AGE_SECONDS": 120.0', line)
        self.assertIn(f'"ENTRY_MODEL": "{trader.POLE_MOTIF_ENTRY_MODEL}"', line)

    def test_pole_motif_setup_detected_and_signal_detail_emit_diagnostics_without_verbose(self):
        original_client = trader.BinanceFuturesClient
        original_triangle = trader.detect_latest_strict_triangle
        original_pole = trader.detect_latest_pole_motif_demo_signal
        original_env = os.environ.copy()
        try:
            trader.BinanceFuturesClient = DemoInitClient
            trader.detect_latest_strict_triangle = lambda symbol, profile, candles: None
            trader.detect_latest_pole_motif_demo_signal = lambda symbol, profile, candles: sample_pole_motif_signal(
                trigger_ts=1700000000000
            )
            os.environ["LIVE_TRADING_ENABLED"] = "1"
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = os.path.join(temp_dir, "binance.sqlite3")
                state_db_path = os.path.join(temp_dir, "state.sqlite3")
                settings_path = os.path.join(temp_dir, "settings.json")
                with open(settings_path, "w", encoding="utf-8") as fh:
                    json.dump(
                        {
                            "symbols": ["BINANCE_FUT:SOLUSDT"],
                            "profiles": {"BINANCE_FUT:SOLUSDT": {"name": "t", "box_size": 1.0, "reversal_boxes": 3}},
                        },
                        fh,
                    )
                with closing(sqlite3.connect(db_path)) as conn:
                    conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("SOLUSDT", "1m", 1700000120000, 100, 100, 100))
                    conn.commit()

                args = argparse.Namespace(
                    db_path=db_path,
                    state_db_path=state_db_path,
                    settings=settings_path,
                    dry_run=False,
                    demo=True,
                    enable_demo_doubles=False,
                    enable_demo_pole_motif=True,
                    notional_usdt="1",
                    demo_max_notional_usdt="1",
                    history_bars=5000,
                    self_test_signal=False,
                    force_demo_order=False,
                    reconcile_positions=False,
                    verbose_market_logs=False,
                    research_rule_json=None,
                )
                output = io.StringIO()
                with redirect_stdout(output):
                    trader.process_once(args)
        finally:
            trader.BinanceFuturesClient = original_client
            trader.detect_latest_strict_triangle = original_triangle
            trader.detect_latest_pole_motif_demo_signal = original_pole
            os.environ.clear()
            os.environ.update(original_env)

        lines = output.getvalue().splitlines()
        self.assertTrue(any(" SETUP_DETECTED BINANCE_FUT:SOLUSDT pole_motif_low LONG " in line for line in lines))
        self.assertTrue(any(" SIGNAL_DETAIL BINANCE_FUT:SOLUSDT " in line for line in lines))
        for event in ("SETUP_DETECTED", "SIGNAL_DETAIL"):
            line = next(line for line in lines if f" {event} " in line)
            self.assertIn('"CURRENT_PRICE": 100.0', line)
            self.assertIn('"ENTRY_PRICE": 105.0', line)
            self.assertIn('"ENTRY_DISTANCE_PERCENT": 5.0', line)
            self.assertIn('"TRIGGER_TIMESTAMP": 1700000000000', line)
            self.assertIn('"LATEST_CANDLE_CLOSE_TIME": 1700000120000', line)
            self.assertIn('"TRIGGER_AGE_SECONDS": 120.0', line)
            self.assertIn(f'"ENTRY_MODEL": "{trader.POLE_MOTIF_ENTRY_MODEL}"', line)

    def test_process_once_keeps_candle_db_read_only_and_writes_state_db(self):
        original_client = trader.BinanceFuturesClient
        original_triangle = trader.detect_latest_strict_triangle
        try:
            trader.BinanceFuturesClient = DemoInitClient
            trader.detect_latest_strict_triangle = lambda symbol, profile, candles: sample_signal(trigger_ts=1)
            with tempfile.TemporaryDirectory() as temp_dir:
                candle_db_path = os.path.join(temp_dir, "market_data.db")
                state_db_path = os.path.join(temp_dir, "binance_state.db")
                settings_path = os.path.join(temp_dir, "settings.json")
                with open(settings_path, "w", encoding="utf-8") as fh:
                    json.dump({
                        "symbols": ["BINANCE_FUT:SOLUSDT"],
                        "profiles": {"BINANCE_FUT:SOLUSDT": {"name": "t", "box_size": 1.0, "reversal_boxes": 3}},
                    }, fh)
                with closing(sqlite3.connect(candle_db_path)) as conn:
                    conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("SOLUSDT", "1m", 1, 101, 101, 101))
                    conn.commit()

                args = argparse.Namespace(
                    db_path=candle_db_path,
                    state_db_path=state_db_path,
                    settings=settings_path,
                    dry_run=True,
                    demo=False,
                    enable_demo_doubles=False,
                    notional_usdt="1",
                    demo_max_notional_usdt="1",
                    history_bars=5000,
                    self_test_signal=False,
                    verbose_market_logs=False,
                )
                trader.process_once(args)

                with closing(sqlite3.connect(candle_db_path)) as candle_conn:
                    candle_tables = {row[0] for row in candle_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                with closing(sqlite3.connect(state_db_path)) as state_conn:
                    state_tables = {row[0] for row in state_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    signal_rows = state_conn.execute("SELECT decision FROM live_signals_binance").fetchall()
                    trade_rows = state_conn.execute("SELECT decision FROM live_trades_binance").fetchall()
                    last_processed = state_conn.execute("SELECT last_processed_close_time FROM live_binance_trader_state").fetchone()[0]
        finally:
            trader.BinanceFuturesClient = original_client
            trader.detect_latest_strict_triangle = original_triangle

        self.assertEqual(candle_tables, {"candles"})
        self.assertTrue({"live_signals_binance", "live_trades_binance", "live_binance_trader_state"}.issubset(state_tables))
        self.assertEqual(signal_rows, [("DRY_RUN",)])
        self.assertEqual(trade_rows, [("DRY_RUN",)])
        self.assertEqual(last_processed, 1)

    def test_update_open_trade_exits_reads_candle_db_and_writes_state_db(self):
        state_conn = sqlite3.connect(":memory:")
        candle_conn = sqlite3.connect(":memory:")
        try:
            trader.init_live_tables(state_conn)
            signal = sample_signal(trigger_ts=1)
            trader.record_trade(
                state_conn,
                signal,
                notional_usdt=Decimal("1"),
                exchange_order_id=None,
                status="POSITION_OPEN",
                dry_run=True,
                decision="DRY_RUN",
            )
            candle_conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)")
            candle_conn.execute(
                "INSERT INTO candles(symbol, interval, close_time, high, low) VALUES(?,?,?,?,?)",
                ("SOLUSDT", "1m", signal.trigger_ts + 60_000, 104, 100),
            )
            candle_conn.commit()

            trader.update_open_trade_exits(state_conn, candle_conn, LifecycleClient("FILLED"), live_enabled=False)
            row = state_conn.execute("SELECT status, exit_price, realized_r FROM live_trades_binance").fetchone()
            candle_tables = {row[0] for row in candle_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            state_conn.close()
            candle_conn.close()

        self.assertEqual(row, ("POSITION_CLOSED", 103.0, 3.0))
        self.assertEqual(candle_tables, {"candles"})

    def test_demo_live_notional_cap_can_be_explicitly_raised_only_for_demo_live(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        signal = replace_signal(sample_signal(), entry_price=Decimal("10"), stop_price=Decimal("9"), tp1_price=Decimal("12"), tp2_price=Decimal("13"))
        _spec, order, reason = trader.validate_guards(
            conn,
            LiveClient(),
            signal,
            notional_usdt=Decimal("5"),
            live_enabled=True,
            demo=True,
            demo_max_notional_usdt=Decimal("5"),
        )
        self.assertIsNone(reason)
        self.assertEqual(Decimal(order["quantity"]) * Decimal(order["price"]), Decimal("5.000"))

        _spec2, order2, reason2 = trader.validate_guards(
            conn,
            LiveClient(),
            sample_signal(trigger_ts=999),
            notional_usdt=Decimal("5"),
            live_enabled=False,
            demo=True,
            demo_max_notional_usdt=Decimal("5"),
        )
        self.assertIsNone(order2)
        self.assertIn("notional exceeds effective cap", reason2)

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
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("SOLUSDT", "1m", 1, 101, 101, 101))
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
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("SOLUSDT", "1m", 1, 101, 101, 101))
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
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("SOLUSDT", "1m", 1, 101, 101, 101))
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

    def test_invalid_long_risk_levels_are_blocked_before_order_build(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        signal = replace_signal(sample_signal(), stop_price=Decimal("101"))
        spec = trader.parse_symbol_spec(EXCHANGE_INFO, "SOLUSDT")

        built_order, build_reason = trader.build_entry_order(signal, spec, Decimal("1"))
        _spec, guarded_order, guard_reason = trader.validate_guards(
            conn, StaticClient(), signal, notional_usdt=Decimal("1"), live_enabled=False
        )

        self.assertIsNone(built_order)
        self.assertEqual(build_reason, "invalid risk levels")
        self.assertIsNone(guarded_order)
        self.assertEqual(guard_reason, "invalid risk levels")

    def test_invalid_short_risk_levels_are_blocked_before_order_build(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        signal = replace_signal(
            sample_signal(),
            pattern="bearish_triangle",
            side="SHORT",
            stop_price=Decimal("99"),
            tp1_price=Decimal("98"),
            tp2_price=Decimal("97"),
        )
        spec = trader.parse_symbol_spec(EXCHANGE_INFO, "SOLUSDT")

        built_order, build_reason = trader.build_entry_order(signal, spec, Decimal("1"))
        _spec, guarded_order, guard_reason = trader.validate_guards(
            conn, StaticClient(), signal, notional_usdt=Decimal("1"), live_enabled=False
        )

        self.assertIsNone(built_order)
        self.assertEqual(build_reason, "invalid risk levels")
        self.assertIsNone(guarded_order)
        self.assertEqual(guard_reason, "invalid risk levels")

    def test_valid_long_and_short_risk_levels_pass(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        long_signal = sample_signal(trigger_ts=1001)
        short_signal = replace_signal(
            sample_signal(trigger_ts=1002),
            pattern="bearish_triangle",
            side="SHORT",
            stop_price=Decimal("101"),
            tp1_price=Decimal("98"),
            tp2_price=Decimal("97"),
        )

        _long_spec, long_order, long_reason = trader.validate_guards(
            conn, StaticClient(), long_signal, notional_usdt=Decimal("1"), live_enabled=False
        )
        _short_spec, short_order, short_reason = trader.validate_guards(
            conn, StaticClient(), short_signal, notional_usdt=Decimal("1"), live_enabled=False
        )

        self.assertIsNone(long_reason)
        self.assertEqual(long_order["side"], "BUY")
        self.assertIsNone(short_reason)
        self.assertEqual(short_order["side"], "SELL")
    def test_process_once_records_invalid_risk_levels_and_submits_no_order(self):
        original_client = trader.BinanceFuturesClient
        original_triangle = trader.detect_latest_strict_triangle
        original_env = os.environ.copy()
        SubmittingClient.instances.clear()
        try:
            os.environ["LIVE_TRADING_ENABLED"] = "1"
            os.environ[trader.BINANCE_DEMO_API_KEY_ENV] = "key"
            os.environ[trader.BINANCE_DEMO_API_SECRET_ENV] = "secret"
            trader.BinanceFuturesClient = SubmittingClient
            trader.detect_latest_strict_triangle = lambda symbol, profile, candles: replace_signal(
                sample_signal(trigger_ts=1), stop_price=Decimal("101")
            )
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = os.path.join(temp_dir, "binance.sqlite3")
                settings_path = os.path.join(temp_dir, "settings.json")
                with open(settings_path, "w", encoding="utf-8") as fh:
                    json.dump({"symbols": ["BINANCE_FUT:SOLUSDT"], "profiles": {"BINANCE_FUT:SOLUSDT": {"name": "t", "box_size": 1.0, "reversal_boxes": 3}}}, fh)
                with closing(sqlite3.connect(db_path)) as conn:
                    conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("SOLUSDT", "1m", 1, 101, 101, 101))
                    conn.commit()
                args = argparse.Namespace(db_path=db_path, settings=settings_path, dry_run=False, demo=True, enable_demo_doubles=False, notional_usdt="1", history_bars=5000, self_test_signal=False)
                trader.process_once(args)
                with closing(sqlite3.connect(db_path)) as conn:
                    signal_rows = conn.execute("SELECT decision, block_reason FROM live_signals_binance").fetchall()
                    trade_count = conn.execute("SELECT COUNT(*) FROM live_trades_binance").fetchone()[0]
        finally:
            trader.BinanceFuturesClient = original_client
            trader.detect_latest_strict_triangle = original_triangle
            os.environ.clear()
            os.environ.update(original_env)

        self.assertEqual(signal_rows, [("BLOCKED", "invalid risk levels")])
        self.assertEqual(trade_count, 0)
        self.assertEqual(SubmittingClient.instances[-1].submitted_orders, [])

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

    def test_force_demo_order_refuses_without_demo(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            market_db = os.path.join(temp_dir, "market.sqlite3")
            state_db = os.path.join(temp_dir, "state.sqlite3")
            with sqlite3.connect(market_db) as conn:
                conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BINANCE_FUT:SOLUSDT", "1m", 1700000000000, 100.0, 101.0, 99.0))
                conn.commit()
            args = argparse.Namespace(
                db_path=market_db, state_db_path=state_db, settings="unused-settings.json",
                dry_run=True, demo=False, enable_demo_doubles=False, notional_usdt="1", demo_max_notional_usdt="1", history_bars=5000,
                self_test_signal=False, force_demo_order=True, force_demo_symbol="BINANCE_FUT:SOLUSDT", force_demo_side="LONG", force_demo_notional_usdt="1.0",
                verbose_market_logs=False, reconcile_positions=False, research_rule_json=None,
            )
            with self.assertRaisesRegex(RuntimeError, "requires --demo"):
                trader.process_once(args)

    def test_force_demo_order_dry_run_writes_signal_row(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            market_db = os.path.join(temp_dir, "market.sqlite3")
            state_db = os.path.join(temp_dir, "state.sqlite3")
            with sqlite3.connect(market_db) as conn:
                conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BINANCE_FUT:SOLUSDT", "1m", 1700000000000, 100.0, 101.0, 99.0))
                conn.commit()
            args = argparse.Namespace(
                db_path=market_db, state_db_path=state_db, settings="unused-settings.json",
                dry_run=True, demo=True, enable_demo_doubles=False, notional_usdt="1", demo_max_notional_usdt="1", history_bars=5000,
                self_test_signal=False, force_demo_order=True, force_demo_symbol="BINANCE_FUT:SOLUSDT", force_demo_side="LONG", force_demo_notional_usdt="1.0",
                verbose_market_logs=False, reconcile_positions=False, research_rule_json=None,
            )
            original_client = trader.BinanceFuturesClient
            trader.BinanceFuturesClient = ForceDemoClient
            try:
                trader.process_once(args)
            finally:
                trader.BinanceFuturesClient = original_client
            with sqlite3.connect(state_db) as conn:
                row = conn.execute("SELECT decision, notes FROM live_signals_binance").fetchone()
                self.assertEqual(row, ("FORCE_DEMO_DRY_RUN", "forced demo self-test"))

    def test_force_demo_order_uses_override_max_notional_cap(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            market_db = os.path.join(temp_dir, "market.sqlite3")
            state_db = os.path.join(temp_dir, "state.sqlite3")
            with sqlite3.connect(market_db) as conn:
                conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BINANCE_FUT:SOLUSDT", "1m", 1700000000000, 100.0, 101.0, 99.0))
                conn.commit()
            args = argparse.Namespace(
                db_path=market_db, state_db_path=state_db, settings="unused-settings.json",
                dry_run=True, demo=True, enable_demo_doubles=False, notional_usdt="1", demo_max_notional_usdt="1", history_bars=5000,
                self_test_signal=False, force_demo_order=True, force_demo_symbol="BINANCE_FUT:SOLUSDT", force_demo_side="LONG", force_demo_notional_usdt="100.0",
                force_demo_max_notional_usdt="100.0",
                verbose_market_logs=False, reconcile_positions=False, research_rule_json=None,
            )
            original_client = trader.BinanceFuturesClient
            trader.BinanceFuturesClient = ForceDemoClient
            try:
                trader.process_once(args)
            finally:
                trader.BinanceFuturesClient = original_client
            with sqlite3.connect(state_db) as conn:
                row = conn.execute("SELECT decision, block_reason FROM live_signals_binance").fetchone()
                self.assertEqual(row, ("FORCE_DEMO_DRY_RUN", None))

    def test_force_demo_order_non_demo_still_refuses_with_override(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            market_db = os.path.join(temp_dir, "market.sqlite3")
            state_db = os.path.join(temp_dir, "state.sqlite3")
            with sqlite3.connect(market_db) as conn:
                conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("BINANCE_FUT:SOLUSDT", "1m", 1700000000000, 100.0, 101.0, 99.0))
                conn.commit()
            args = argparse.Namespace(
                db_path=market_db, state_db_path=state_db, settings="unused-settings.json",
                dry_run=True, demo=False, enable_demo_doubles=False, notional_usdt="1", demo_max_notional_usdt="1", history_bars=5000,
                self_test_signal=False, force_demo_order=True, force_demo_symbol="BINANCE_FUT:SOLUSDT", force_demo_side="LONG", force_demo_notional_usdt="100.0",
                force_demo_max_notional_usdt="100.0",
                verbose_market_logs=False, reconcile_positions=False, research_rule_json=None,
            )
            with self.assertRaisesRegex(RuntimeError, "requires --demo"):
                trader.process_once(args)

    def test_build_forced_demo_signal_long_price_order(self):
        signal = trader.build_forced_demo_signal("BINANCE_FUT:SOLUSDT", "LONG", trader.Candle(close_time=1, close=100.0, high=101.0, low=99.0))
        self.assertLess(signal.stop_price, signal.entry_price)
        self.assertLess(signal.entry_price, signal.tp1_price)
        self.assertLess(signal.tp1_price, signal.tp2_price)

    def test_build_forced_demo_signal_short_price_order(self):
        signal = trader.build_forced_demo_signal("BINANCE_FUT:SOLUSDT", "SHORT", trader.Candle(close_time=1, close=100.0, high=101.0, low=99.0))
        self.assertGreater(signal.stop_price, signal.entry_price)
        self.assertGreater(signal.entry_price, signal.tp1_price)
        self.assertGreater(signal.tp1_price, signal.tp2_price)

    def test_normal_mode_unaffected_when_force_demo_order_disabled(self):
        args = argparse.Namespace(
            db_path="unused.sqlite3", state_db_path="unused-state.sqlite3", settings="unused-settings.json",
            dry_run=False, demo=False, enable_demo_doubles=False, notional_usdt="1", demo_max_notional_usdt="1", history_bars=5000,
            self_test_signal=True, force_demo_order=False, force_demo_symbol="BINANCE_FUT:SOLUSDT", force_demo_side="LONG", force_demo_notional_usdt="1.0",
            verbose_market_logs=False, reconcile_positions=False, research_rule_json=None,
        )
        original = trader.build_forced_demo_signal
        try:
            def _boom(*_args, **_kwargs):
                raise AssertionError("force-demo path must be inactive")
            trader.build_forced_demo_signal = _boom
            trader.process_once(args)
        finally:
            trader.build_forced_demo_signal = original

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

    def _record_pending_order(self, conn, *, created_at, status="NEW"):
        signal = sample_signal()
        order = {"newClientOrderId": "entry-test-1"}
        raw_response = {"orderId": 42, "clientOrderId": "entry-test-1", "status": "NEW"}
        trader.record_trade(
            conn,
            signal,
            notional_usdt=Decimal("1"),
            exchange_order_id="42",
            status="ORDER_SENT",
            dry_run=False,
            decision="ORDER_SENT",
            raw_order_response={"order_request": order, "order_response": raw_response},
            notes="pending entry",
        )
        conn.execute("UPDATE live_trades_binance SET created_at = ?", (created_at,))
        conn.commit()
        return LifecycleClient(status)

    def test_stale_pending_new_order_is_cancelled_and_marked_entry_expired(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        client = self._record_pending_order(conn, created_at="2000-01-01T00:00:00Z", status="NEW")

        trader.poll_pending_entry_orders(conn, client, live_enabled=True, max_pending_entry_minutes=1)

        row = conn.execute("SELECT status, block_reason, notes, raw_order_response FROM live_trades_binance").fetchone()
        self.assertEqual(row[0], "ENTRY_EXPIRED")
        self.assertEqual(row[1], "MAX_PENDING_ENTRY_AGE_EXCEEDED")
        self.assertIn("MAX_PENDING_ENTRY_AGE_EXCEEDED", row[2])
        self.assertIn("cancel_response", row[3])
        self.assertEqual(len(client.cancelled_orders), 1)

    def test_young_pending_new_order_is_not_cancelled(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        client = self._record_pending_order(conn, created_at=trader.now_iso(), status="NEW")

        trader.poll_pending_entry_orders(conn, client, live_enabled=True, max_pending_entry_minutes=60)

        row = conn.execute("SELECT status, entry_order_status FROM live_trades_binance").fetchone()
        self.assertEqual(row[0], "ORDER_SENT")
        self.assertEqual(row[1], "NEW")
        self.assertEqual(client.cancelled_orders, [])

    def test_stale_filled_order_is_not_cancelled_and_opens_position(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        client = self._record_pending_order(conn, created_at="2000-01-01T00:00:00Z", status="FILLED")

        trader.poll_pending_entry_orders(conn, client, live_enabled=True, max_pending_entry_minutes=1)

        row = conn.execute("SELECT status, entry_order_status FROM live_trades_binance").fetchone()
        self.assertEqual(row[0], "POSITION_OPEN")
        self.assertEqual(row[1], "FILLED")
        self.assertEqual(client.cancelled_orders, [])

    def test_stale_exchange_canceled_order_updates_local_terminal_status(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        client = self._record_pending_order(conn, created_at="2000-01-01T00:00:00Z", status="CANCELED")

        trader.poll_pending_entry_orders(conn, client, live_enabled=True, max_pending_entry_minutes=1)

        row = conn.execute("SELECT status, entry_order_status, notes FROM live_trades_binance").fetchone()
        self.assertEqual(row[0], "CANCELED")
        self.assertEqual(row[1], "CANCELED")
        self.assertIn("terminal on exchange", row[2])
        self.assertEqual(client.cancelled_orders, [])

    def test_disabled_max_pending_option_preserves_current_pending_behavior(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        client = self._record_pending_order(conn, created_at="2000-01-01T00:00:00Z", status="NEW")

        trader.poll_pending_entry_orders(conn, client, live_enabled=True)

        row = conn.execute("SELECT status, entry_order_status FROM live_trades_binance").fetchone()
        self.assertEqual(row[0], "ORDER_SENT")
        self.assertEqual(row[1], "NEW")
        self.assertEqual(client.cancelled_orders, [])

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



    def _reconcile_conn_with_position_open_trade(
        self,
        *,
        executed_qty="0.009",
        avg_fill_price="100.50",
        status="POSITION_OPEN",
        side="LONG",
        trigger_ts=123456,
    ):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        trader.init_live_tables(conn)
        signal = replace_signal(
            sample_signal(trigger_ts=trigger_ts),
            side=side,
            pattern="bearish_triangle" if side == "SHORT" else "bullish_triangle",
            stop_price=Decimal("101") if side == "SHORT" else Decimal("99"),
            tp1_price=Decimal("98") if side == "SHORT" else Decimal("102"),
            tp2_price=Decimal("97") if side == "SHORT" else Decimal("103"),
        )
        trader.record_trade(
            conn,
            signal,
            notional_usdt=Decimal("1"),
            exchange_order_id="42",
            status=status,
            dry_run=False,
            decision="ORDER_SENT",
            raw_order_response={"order_request": {"quantity": executed_qty}},
        )
        conn.execute(
            """
            UPDATE live_trades_binance
            SET avg_fill_price = ?, executed_qty = ?, entry_order_status = 'FILLED'
            WHERE symbol = ? AND trigger_timestamp = ?
            """,
            (avg_fill_price, executed_qty, "BINANCE_FUT:SOLUSDT", trigger_ts),
        )
        conn.commit()
        return conn

    def _binance_sol_position(self, *, qty="0.009", entry="100.50", amt=None):
        amount = amt if amt is not None else qty
        return trader.parse_binance_open_positions(
            [{"symbol": "SOLUSDT", "positionAmt": amount, "entryPrice": entry, "positionSide": "BOTH"}],
            "BINANCE_FUT:SOLUSDT",
        )

    def test_reconcile_positions_matching_position_and_state_row(self):
        conn = self._reconcile_conn_with_position_open_trade()

        logs = trader.build_position_reconciliation_logs(
            self._binance_sol_position(),
            trader.load_local_open_trades_for_reconciliation(conn),
        )
        binance_log = next(log for log in logs if log["source"] == "BINANCE")
        local_log = next(log for log in logs if log["source"] == "LOCAL")

        self.assertEqual(binance_log["reconcile_status"], "MATCHED")
        self.assertEqual(local_log["reconcile_status"], "MATCHED")
        self.assertTrue(binance_log["has_matching_local_state_row"])
        self.assertTrue(local_log["has_matching_binance_position"])
        self.assertEqual(binance_log["mismatch_warnings"], [])
        self.assertEqual(local_log["mismatch_warnings"], [])
        self.assertEqual(local_log["stop_price"], 99.0)
        self.assertEqual(local_log["tp2_price"], 103.0)

    def test_reconcile_positions_binance_position_without_local_row(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        trader.init_live_tables(conn)

        logs = trader.build_position_reconciliation_logs(
            self._binance_sol_position(),
            trader.load_local_open_trades_for_reconciliation(conn),
        )

        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["source"], "BINANCE")
        self.assertEqual(logs[0]["reconcile_status"], "BINANCE_ONLY")
        self.assertFalse(logs[0]["has_matching_local_state_row"])
        self.assertEqual(logs[0]["mismatch_warnings"], ["BINANCE_ONLY"])

    def test_reconcile_positions_local_row_without_binance_position(self):
        conn = self._reconcile_conn_with_position_open_trade()

        logs = trader.build_position_reconciliation_logs(
            [],
            trader.load_local_open_trades_for_reconciliation(conn),
        )

        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["source"], "LOCAL")
        self.assertEqual(logs[0]["reconcile_status"], "LOCAL_ONLY")
        self.assertFalse(logs[0]["has_matching_binance_position"])
        self.assertEqual(logs[0]["mismatch_warnings"], ["LOCAL_ONLY"])

    def test_reconcile_positions_duplicate_local_open_rows_warning(self):
        conn = self._reconcile_conn_with_position_open_trade(trigger_ts=123456)
        trader.record_trade(
            conn,
            sample_signal(trigger_ts=123457),
            notional_usdt=Decimal("1"),
            exchange_order_id="43",
            status="POSITION_OPEN",
            dry_run=False,
            decision="ORDER_SENT",
            raw_order_response={"order_request": {"quantity": "0.010"}},
        )
        conn.execute(
            "UPDATE live_trades_binance SET avg_fill_price = 100.50, executed_qty = 0.010 WHERE trigger_timestamp = 123457"
        )
        conn.commit()

        logs = trader.build_position_reconciliation_logs(
            self._binance_sol_position(qty="0.009"),
            trader.load_local_open_trades_for_reconciliation(conn),
        )

        self.assertTrue(any("DUPLICATE_LOCAL_OPEN_ROWS" in log["mismatch_warnings"] for log in logs))
        self.assertEqual(sum(log["source"] == "LOCAL" for log in logs), 2)

    def test_reconcile_positions_side_mismatch_warning(self):
        conn = self._reconcile_conn_with_position_open_trade(side="SHORT")

        logs = trader.build_position_reconciliation_logs(
            self._binance_sol_position(amt="0.009"),
            trader.load_local_open_trades_for_reconciliation(conn),
        )

        self.assertTrue(all(log["reconcile_status"] == "SIDE_MISMATCH" for log in logs))
        self.assertTrue(all("SIDE_MISMATCH" in log["mismatch_warnings"] for log in logs))

    def test_reconcile_positions_status_mismatch_warning(self):
        conn = self._reconcile_conn_with_position_open_trade(status="ORDER_SENT")

        logs = trader.build_position_reconciliation_logs(
            self._binance_sol_position(),
            trader.load_local_open_trades_for_reconciliation(conn),
        )

        self.assertTrue(all(log["reconcile_status"] == "STATUS_MISMATCH" for log in logs))
        self.assertTrue(all("STATUS_MISMATCH" in log["mismatch_warnings"] for log in logs))

    def test_reconcile_positions_qty_mismatch_label(self):
        conn = self._reconcile_conn_with_position_open_trade(executed_qty="0.008")

        logs = trader.build_position_reconciliation_logs(
            self._binance_sol_position(qty="0.009"),
            trader.load_local_open_trades_for_reconciliation(conn),
        )

        self.assertTrue(all(log["reconcile_status"] == "QTY_MISMATCH" for log in logs))
        self.assertTrue(all("QTY_MISMATCH" in log["mismatch_warnings"] for log in logs))

    def test_reconcile_positions_main_loop_exits_once_and_does_not_call_process_once(self):
        class ReconcileOnlyClient:
            instances = []

            def __init__(self, api_key=None, api_secret=None, *, base_url=trader.BINANCE_BASE_URL):
                self.api_key = api_key
                self.api_secret = api_secret
                self.base_url = base_url
                self.has_credentials = True
                self.position_risk_calls = []
                self.submitted_orders = []
                ReconcileOnlyClient.instances.append(self)

            def get_position_risk(self, symbol):
                self.position_risk_calls.append(symbol)
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]

            def submit_order(self, order):
                self.submitted_orders.append(order)
                raise AssertionError("reconciliation must not submit orders")

        original_client = trader.BinanceFuturesClient
        original_env = os.environ.copy()
        original_parse_args = trader.parse_args
        original_process_once = trader.process_once
        try:
            os.environ.clear()
            os.environ.update({
                "BINANCE_FUTURES_API_KEY": "prod-key",
                "BINANCE_FUTURES_API_SECRET": "prod-secret",
            })
            trader.BinanceFuturesClient = ReconcileOnlyClient
            with tempfile.TemporaryDirectory() as temp_dir:
                state_db_path = os.path.join(temp_dir, "state.db")
                with closing(sqlite3.connect(state_db_path)) as conn:
                    trader.init_live_tables(conn)
                args = argparse.Namespace(
                    db_path=os.path.join(temp_dir, "missing-market-data.db"),
                    state_db_path=state_db_path,
                    settings=os.path.join(temp_dir, "missing-settings.json"),
                    dry_run=True,
                    demo=False,
                    enable_demo_doubles=False,
                    notional_usdt="1",
                    demo_max_notional_usdt="1",
                    history_bars=5000,
                    self_test_signal=False,
                    verbose_market_logs=False,
                    reconcile_positions=True,
                    loop=True,
                    poll_seconds=0,
                )
                trader.parse_args = lambda: args
                trader.process_once = lambda parsed_args, *, iteration=None: (_ for _ in ()).throw(
                    AssertionError("process_once must not be called in reconcile mode")
                )
                output = io.StringIO()
                with redirect_stdout(output):
                    trader.main()
        finally:
            trader.BinanceFuturesClient = original_client
            trader.parse_args = original_parse_args
            trader.process_once = original_process_once
            os.environ.clear()
            os.environ.update(original_env)

        self.assertIn("LOOP_IGNORED", output.getvalue())
        self.assertIn("RECONCILE_POSITION", output.getvalue())
        self.assertNotIn("SCAN", output.getvalue())
        self.assertEqual(ReconcileOnlyClient.instances[-1].submitted_orders, [])
        self.assertEqual(sorted(ReconcileOnlyClient.instances[-1].position_risk_calls), ["BNBUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"])

    def test_reconcile_positions_calls_no_write_helpers_or_candle_db(self):
        class ReconcileOnlyClient:
            def __init__(self, api_key=None, api_secret=None, *, base_url=trader.BINANCE_BASE_URL):
                self.has_credentials = True

            def get_position_risk(self, symbol):
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]

        original_client = trader.BinanceFuturesClient
        original_env = os.environ.copy()
        original_parse_args = trader.parse_args
        original_init = trader.init_live_tables
        original_poll = trader.poll_pending_entry_orders
        original_update = trader.update_open_trade_exits
        original_candle = trader.connect_candle_db_readonly
        try:
            os.environ.clear()
            os.environ.update({
                "BINANCE_FUTURES_API_KEY": "prod-key",
                "BINANCE_FUTURES_API_SECRET": "prod-secret",
            })
            trader.BinanceFuturesClient = ReconcileOnlyClient
            trader.init_live_tables = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("init_live_tables called"))
            trader.poll_pending_entry_orders = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("poll called"))
            trader.update_open_trade_exits = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("update exits called"))
            trader.connect_candle_db_readonly = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("candle DB opened"))
            with tempfile.TemporaryDirectory() as temp_dir:
                state_db_path = os.path.join(temp_dir, "state.db")
                with closing(sqlite3.connect(state_db_path)) as conn:
                    original_init(conn)
                trader.parse_args = lambda: argparse.Namespace(
                    db_path=os.path.join(temp_dir, "missing-market-data.db"),
                    state_db_path=state_db_path,
                    settings=os.path.join(temp_dir, "missing-settings.json"),
                    dry_run=True,
                    demo=False,
                    enable_demo_doubles=False,
                    notional_usdt="1",
                    demo_max_notional_usdt="1",
                    history_bars=5000,
                    self_test_signal=False,
                    verbose_market_logs=False,
                    reconcile_positions=True,
                    loop=False,
                    poll_seconds=0,
                )
                with redirect_stdout(io.StringIO()):
                    trader.main()
        finally:
            trader.BinanceFuturesClient = original_client
            trader.parse_args = original_parse_args
            trader.init_live_tables = original_init
            trader.poll_pending_entry_orders = original_poll
            trader.update_open_trade_exits = original_update
            trader.connect_candle_db_readonly = original_candle
            os.environ.clear()
            os.environ.update(original_env)

    def test_export_trade_journal_writes_human_readable_csv(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db_path = os.path.join(temp_dir, "state.db")
            export_path = os.path.join(temp_dir, "journal", "trades.csv")
            with closing(sqlite3.connect(state_db_path)) as conn:
                trader.init_live_tables(conn)
                conn.execute(
                    """
                    INSERT INTO live_trades_binance(
                        created_at, symbol, pattern, strategy_id, candidate_id, side,
                        trigger_timestamp, entry_price, stop_price, tp1_price, tp2_price,
                        notional_usdt, decision, status, block_reason, dry_run,
                        exchange_order_id, entry_order_status, avg_fill_price, executed_qty,
                        stop_algo_id, tp_algo_id, protective_orders_status, exit_time,
                        exit_price, realized_r, fees, raw_order_response, notes
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        "2026-06-16T00:00:00+00:00",
                        "BINANCE_FUT:SOLUSDT",
                        "bullish_triangle",
                        "P2_SURVIVOR_V1",
                        "CAND-000053",
                        "LONG",
                        123456,
                        100.0,
                        99.0,
                        102.0,
                        103.0,
                        1.0,
                        "ORDER_SENT",
                        "POSITION_OPEN",
                        None,
                        0,
                        "abc123",
                        "FILLED",
                        100.1,
                        0.01,
                        "sl-1",
                        "tp-1",
                        "ATTACHED",
                        123999,
                        103.0,
                        3.0,
                        0.001,
                        "{}",
                        "entry order filled; POSITION_OPEN",
                    ),
                )
                conn.commit()

            with closing(trader.connect_state_db_readonly(state_db_path)) as conn:
                row_count = trader.export_trade_journal(conn, export_path)

            self.assertEqual(row_count, 1)
            with open(export_path, "r", encoding="utf-8") as fh:
                lines = fh.read().splitlines()
            self.assertEqual(lines[0].split(","), list(trader.TRADE_JOURNAL_COLUMNS))
            self.assertIn("BINANCE_FUT:SOLUSDT", lines[1])
            self.assertIn("P2_SURVIVOR_V1", lines[1])
            self.assertIn("ATTACHED", lines[1])

    def test_main_export_trade_journal_is_read_only_and_skips_runtime(self):
        original_parse_args = trader.parse_args
        original_process_once = trader.process_once
        original_reconcile = trader.run_reconciliation_once
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                state_db_path = os.path.join(temp_dir, "state.db")
                export_path = os.path.join(temp_dir, "journal.csv")
                with closing(sqlite3.connect(state_db_path)) as conn:
                    trader.init_live_tables(conn)
                trader.parse_args = lambda: argparse.Namespace(
                    db_path=os.path.join(temp_dir, "missing-market-data.db"),
                    state_db_path=state_db_path,
                    settings=os.path.join(temp_dir, "missing-settings.json"),
                    dry_run=True,
                    demo=True,
                    enable_demo_doubles=False,
                    notional_usdt="1",
                    demo_max_notional_usdt="1",
                    history_bars=5000,
                    self_test_signal=False,
                    verbose_market_logs=False,
                    reconcile_positions=False,
                    export_trade_journal=export_path,
                    loop=False,
                    poll_seconds=0,
                )
                trader.process_once = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("process_once called"))
                trader.run_reconciliation_once = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("reconcile called"))
                output = io.StringIO()
                with redirect_stdout(output):
                    trader.main()
                self.assertTrue(os.path.exists(export_path))
        finally:
            trader.parse_args = original_parse_args
            trader.process_once = original_process_once
            trader.run_reconciliation_once = original_reconcile

        self.assertIn("TRADE_JOURNAL_EXPORT", output.getvalue())


    def test_build_pole_motif_signal_uses_next_column_open_fixed_risk_chain(self):
        pattern = {
            "pattern_name": "LOW_POLE",
            "pole_column_index": 5,
            "reversal_column_index": 6,
        }
        profile = trader.PnFProfile("POLE_DEMO_bs0.25_rev3", 0.25, 3)
        signal = trader.build_pole_motif_signal(
            symbol="BINANCE_FUT:SOLUSDT",
            profile=profile,
            pattern=pattern,
            entry_candle=trader.Candle(close_time=1700000000000, close=100.5, high=101.0, low=99.0, open=100.0),
            confirmation_idx=7,
        ).to_triangle_signal()

        self.assertEqual(signal.pattern, "pole_motif_low")
        self.assertEqual(signal.side, "LONG")
        self.assertEqual(signal.entry_price, Decimal("100.0"))
        self.assertEqual(signal.stop_price, Decimal("99.25"))
        self.assertEqual(signal.tp2_price, Decimal("101.875"))
        self.assertEqual(signal.tp1_price, Decimal("101.50"))
        self.assertIn("NEXT_COLUMN_OPEN_ENTRY", signal.pattern_quality)

    def test_without_p2_flag_process_once_does_not_call_p2_detector(self):
        original_client = trader.BinanceFuturesClient
        original_triangle = trader.detect_latest_strict_triangle
        original_p2 = trader.detect_latest_p2_survivor_demo_signal
        try:
            trader.BinanceFuturesClient = DemoInitClient
            trader.detect_latest_strict_triangle = lambda symbol, profile, candles: None

            def fail_p2(*args, **kwargs):
                raise AssertionError("P2 detector must not be called without --enable-demo-p2-survivor-v1")

            trader.detect_latest_p2_survivor_demo_signal = fail_p2
            with tempfile.TemporaryDirectory() as temp_dir:
                candle_db_path = os.path.join(temp_dir, "market_data.db")
                state_db_path = os.path.join(temp_dir, "binance_state.db")
                settings_path = os.path.join(temp_dir, "settings.json")
                with open(settings_path, "w", encoding="utf-8") as fh:
                    json.dump({
                        "symbols": ["BINANCE_FUT:SOLUSDT"],
                        "profiles": {"BINANCE_FUT:SOLUSDT": {"name": "t", "box_size": 1.0, "reversal_boxes": 3}},
                    }, fh)
                with closing(sqlite3.connect(candle_db_path)) as conn:
                    conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, close REAL, high REAL, low REAL)")
                    conn.execute("INSERT INTO candles VALUES(?,?,?,?,?,?)", ("SOLUSDT", "1m", 1, 101, 101, 101))
                    conn.commit()

                args = argparse.Namespace(
                    db_path=candle_db_path,
                    state_db_path=state_db_path,
                    settings=settings_path,
                    dry_run=True,
                    demo=True,
                    enable_demo_doubles=False,
                    enable_demo_pole_motif=False,
                    enable_demo_p2_survivor_v1=False,
                    notional_usdt="1",
                    demo_max_notional_usdt="1",
                    history_bars=5000,
                    self_test_signal=False,
                    force_demo_order=False,
                    verbose_market_logs=False,
                    reconcile_positions=False,
                    research_rule_json=None,
                )
                trader.process_once(args)
        finally:
            trader.BinanceFuturesClient = original_client
            trader.detect_latest_strict_triangle = original_triangle
            trader.detect_latest_p2_survivor_demo_signal = original_p2

    def test_existing_order_sizing_exact_output_unchanged(self):
        spec = trader.parse_symbol_spec(EXCHANGE_INFO, "SOLUSDT")
        order, reason = trader.build_entry_order(sample_signal(trigger_ts=123456), spec, Decimal("1"))

        self.assertIsNone(reason)
        self.assertEqual(order, {
            "symbol": "SOLUSDT",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": "0.010",
            "price": "100.00",
            "newClientOrderId": "pnf-bull-123456-L",
        })

    def test_existing_duplicate_guard_remains_symbol_pattern_timestamp(self):
        conn = sqlite3.connect(":memory:")
        try:
            trader.init_live_tables(conn)
            signal = sample_signal(trigger_ts=654321)
            trader.record_signal(conn, signal, decision="ORDER_SENT", block_reason=None, dry_run=False, notional_usdt=Decimal("1"))
            _spec, _order, reason = trader.validate_guards(
                conn,
                LiveClient(),
                signal,
                notional_usdt=Decimal("1"),
                live_enabled=True,
            )
            different_pattern = replace_signal(signal, pattern="bearish_triangle", side="SHORT", stop_price=Decimal("101"), tp1_price=Decimal("98"), tp2_price=Decimal("97"))
            _spec2, _order2, reason2 = trader.validate_guards(
                conn,
                LiveClient(),
                different_pattern,
                notional_usdt=Decimal("1"),
                live_enabled=True,
            )
        finally:
            conn.close()

        self.assertEqual(reason, "duplicate signal for same symbol/pattern/trigger timestamp")
        self.assertNotEqual(reason2, "duplicate signal for same symbol/pattern/trigger timestamp")

    def test_duplicate_setup_cooldown_blocks_same_entry_stop_after_close(self):
        conn = sqlite3.connect(":memory:")
        try:
            trader.init_live_tables(conn)
            original = sample_signal(trigger_ts=1001)
            trader.record_trade(
                conn,
                original,
                notional_usdt=Decimal("1"),
                exchange_order_id="closed-1",
                status="POSITION_CLOSED",
                dry_run=False,
            )
            repeat = replace_signal(original, trigger_ts=1002)
            _spec, order, reason = trader.validate_guards(
                conn,
                LiveClient(),
                repeat,
                notional_usdt=Decimal("1"),
                live_enabled=True,
            )
        finally:
            conn.close()

        self.assertIsNone(order)
        self.assertEqual(reason, "DUPLICATE_SETUP_COOLDOWN")

    def test_duplicate_setup_cooldown_allows_after_twelve_hours(self):
        conn = sqlite3.connect(":memory:")
        try:
            trader.init_live_tables(conn)
            original = sample_signal(trigger_ts=1001)
            trader.record_trade(
                conn,
                original,
                notional_usdt=Decimal("1"),
                exchange_order_id="closed-1",
                status="POSITION_CLOSED",
                dry_run=False,
            )
            old_created_at = (datetime.now(timezone.utc) - timedelta(hours=13)).isoformat(timespec="seconds")
            conn.execute("UPDATE live_trades_binance SET created_at = ?", (old_created_at,))
            conn.commit()
            repeat = replace_signal(original, trigger_ts=1002)
            _spec, order, reason = trader.validate_guards(
                conn,
                LiveClient(),
                repeat,
                notional_usdt=Decimal("1"),
                live_enabled=True,
            )
        finally:
            conn.close()

        self.assertIsNone(reason)
        self.assertIsNotNone(order)

    def test_strategy_candidate_metadata_only_for_p2_signals(self):
        conn = sqlite3.connect(":memory:")
        try:
            trader.init_live_tables(conn)
            regular = sample_signal(trigger_ts=2001)
            p2 = replace_signal(sample_pole_motif_signal(trigger_ts=2002), pattern=trader.P2_SURVIVOR_PATTERN)
            trader.record_signal(conn, regular, decision="DRY_RUN", block_reason=None, dry_run=True, notional_usdt=Decimal("1"))
            trader.record_trade(conn, regular, notional_usdt=Decimal("1"), exchange_order_id=None, status="DRY_RUN", dry_run=True)
            trader.record_signal(conn, p2, decision="DRY_RUN", block_reason=None, dry_run=True, notional_usdt=Decimal("1"))
            trader.record_trade(conn, p2, notional_usdt=Decimal("1"), exchange_order_id=None, status="DRY_RUN", dry_run=True)
            signal_rows = conn.execute("SELECT pattern, strategy_id, candidate_id FROM live_signals_binance ORDER BY trigger_timestamp").fetchall()
            trade_rows = conn.execute("SELECT pattern, strategy_id, candidate_id FROM live_trades_binance ORDER BY trigger_timestamp").fetchall()
        finally:
            conn.close()

        self.assertEqual(signal_rows[0], ("bullish_triangle", None, None))
        self.assertEqual(trade_rows[0], ("bullish_triangle", None, None))
        self.assertEqual(signal_rows[1], (trader.P2_SURVIVOR_PATTERN, trader.P2_SURVIVOR_STRATEGY_ID, trader.P2_SURVIVOR_CANDIDATE_ID))
        self.assertEqual(trade_rows[1], (trader.P2_SURVIVOR_PATTERN, trader.P2_SURVIVOR_STRATEGY_ID, trader.P2_SURVIVOR_CANDIDATE_ID))

    def test_p2_flag_requires_demo_in_process_once(self):
        args = argparse.Namespace(
            db_path="unused.sqlite3",
            state_db_path="unused-state.sqlite3",
            settings="unused-settings.json",
            dry_run=True,
            demo=False,
            enable_demo_doubles=False,
            enable_demo_pole_motif=False,
            enable_demo_p2_survivor_v1=True,
            notional_usdt="1",
            demo_max_notional_usdt="1",
            history_bars=5000,
            self_test_signal=False,
            force_demo_order=False,
            verbose_market_logs=False,
            reconcile_positions=False,
            research_rule_json=None,
        )

        with self.assertRaisesRegex(RuntimeError, "--enable-demo-p2-survivor-v1 requires --demo"):
            trader.process_once(args)

    def test_pole_motif_guard_is_demo_live_only(self):
        conn = sqlite3.connect(":memory:")
        trader.init_live_tables(conn)
        signal = trader.TriangleSignal(
            symbol="BINANCE_FUT:SOLUSDT",
            pattern="pole_motif_low",
            side="LONG",
            trigger_ts=123,
            entry_price=Decimal("100"),
            stop_price=Decimal("99"),
            tp1_price=Decimal("102"),
            tp2_price=Decimal("102.5"),
            trigger_column_idx=7,
            support_level=Decimal("99"),
            resistance_level=Decimal("100"),
            break_distance_boxes=Decimal("3"),
            pattern_quality="POLE_MOTIF_DEMO_FORWARD",
        )

        _spec, order, reason = trader.validate_guards(
            conn,
            LiveClient(),
            signal,
            notional_usdt=Decimal("1"),
            live_enabled=True,
            demo=True,
            allow_demo_pole_motif=True,
        )
        self.assertIsNone(reason)
        self.assertIsNotNone(order)

        _spec2, order2, reason2 = trader.validate_guards(
            conn,
            LiveClient(),
            replace_signal(signal, trigger_ts=124),
            notional_usdt=Decimal("1"),
            live_enabled=True,
            demo=False,
            allow_demo_pole_motif=True,
        )
        self.assertIsNone(order2)
        self.assertIn("pattern outside live allowlist", reason2)

    def test_pole_motif_break_even_lifecycle_logs_and_closes_at_be_stop(self):
        state_conn = sqlite3.connect(":memory:")
        candle_conn = sqlite3.connect(":memory:")
        try:
            trader.init_live_tables(state_conn)
            signal = trader.TriangleSignal(
                symbol="BINANCE_FUT:SOLUSDT",
                pattern="pole_motif_low",
                side="LONG",
                trigger_ts=1,
                entry_price=Decimal("100"),
                stop_price=Decimal("99"),
                tp1_price=Decimal("102"),
                tp2_price=Decimal("102.5"),
                trigger_column_idx=7,
                support_level=Decimal("99"),
                resistance_level=Decimal("100"),
                break_distance_boxes=Decimal("3"),
                pattern_quality="POLE_MOTIF_DEMO_FORWARD",
            )
            trader.record_trade(
                state_conn,
                signal,
                notional_usdt=Decimal("1"),
                exchange_order_id=None,
                status="POSITION_OPEN",
                dry_run=True,
                decision="DRY_RUN",
            )
            candle_conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)")
            candle_conn.execute("INSERT INTO candles VALUES(?,?,?,?,?)", ("SOLUSDT", "1m", 2, 102.1, 100.1))
            candle_conn.execute("INSERT INTO candles VALUES(?,?,?,?,?)", ("SOLUSDT", "1m", 3, 101.0, 99.9))
            candle_conn.commit()

            output = io.StringIO()
            with redirect_stdout(output):
                trader.update_open_trade_exits(state_conn, candle_conn, LifecycleClient("FILLED"), live_enabled=False)
            row = state_conn.execute("SELECT status, break_even_armed, active_stop_price, exit_price, realized_r FROM live_trades_binance").fetchone()
        finally:
            state_conn.close()
            candle_conn.close()

        self.assertEqual(row, ("POSITION_CLOSED", 1, 100.0, 100.0, 0.0))
        text = output.getvalue()
        self.assertIn("BE_ARMED", text)
        self.assertIn("STOP_HIT", text)
        self.assertIn("POSITION_CLOSED", text)

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


    def test_p2_survivor_guard_requires_demo_flag_and_records_ids(self):
        conn = sqlite3.connect(":memory:")
        try:
            trader.init_live_tables(conn)
            signal = replace_signal(
                sample_pole_motif_signal(),
                pattern=trader.P2_SURVIVOR_PATTERN,
                pattern_quality="P2_SURVIVOR_V1|candidate_id=CAND-000053",
            )
            _spec, blocked_order, blocked_reason = trader.validate_guards(
                conn,
                LiveClient(),
                signal,
                notional_usdt=Decimal("1"),
                live_enabled=True,
                demo=False,
                allow_demo_p2_survivor_v1=True,
            )
            _spec_flag, flag_order, flag_reason = trader.validate_guards(
                conn,
                LiveClient(),
                signal,
                notional_usdt=Decimal("1"),
                live_enabled=True,
                demo=True,
                allow_demo_p2_survivor_v1=False,
            )
            _spec2, order, reason = trader.validate_guards(
                conn,
                LiveClient(),
                signal,
                notional_usdt=Decimal("1"),
                live_enabled=True,
                demo=True,
                allow_demo_p2_survivor_v1=True,
            )
            trader.record_signal(conn, signal, decision="DRY_RUN", block_reason=None, dry_run=True, notional_usdt=Decimal("1"))
            trader.record_trade(conn, signal, notional_usdt=Decimal("1"), exchange_order_id=None, status="DRY_RUN", dry_run=True)
            signal_row = conn.execute("SELECT pattern, strategy_id, candidate_id FROM live_signals_binance").fetchone()
            trade_row = conn.execute("SELECT pattern, strategy_id, candidate_id FROM live_trades_binance").fetchone()
        finally:
            conn.close()

        self.assertIsNone(blocked_order)
        self.assertEqual(blocked_reason, "pattern outside live allowlist")
        self.assertIsNone(flag_order)
        self.assertEqual(flag_reason, "pattern outside live allowlist")
        self.assertIsNone(reason)
        self.assertEqual(order["type"], "LIMIT")
        self.assertEqual(signal_row, (trader.P2_SURVIVOR_PATTERN, trader.P2_SURVIVOR_STRATEGY_ID, trader.P2_SURVIVOR_CANDIDATE_ID))
        self.assertEqual(trade_row, (trader.P2_SURVIVOR_PATTERN, trader.P2_SURVIVOR_STRATEGY_ID, trader.P2_SURVIVOR_CANDIDATE_ID))

    def test_detect_latest_p2_survivor_uses_causal_p2_buckets_not_rejected_core_fields(self):
        class P2Column:
            def __init__(self, idx, kind, top, bottom, start_ts=None):
                self.idx = idx
                self.kind = kind
                self.top = top
                self.bottom = bottom
                self.start_ts = start_ts

        columns = [
            P2Column(0, "X", 4, 0),
            P2Column(1, "O", 4, 0),
            P2Column(2, "X", 4, 0),
            P2Column(3, "O", 4, 0),
            P2Column(4, "X", 4, 0),
            P2Column(5, "O", 4, 0),
            P2Column(6, "O", 14, 10),
            P2Column(7, "X", 18, 14),
            P2Column(8, "O", 18, 16, start_ts=1_000),
        ]

        class P2Engine:
            def __init__(self, profile):
                self.columns = columns

            def update_from_price(self, close_time, close):
                return None

        original_engine = trader.PnFEngine
        original_detect = trader.detect_pole_patterns
        try:
            trader.PnFEngine = P2Engine
            trader.detect_pole_patterns = lambda engine_columns, box_size: [
                {
                    "pattern_name": "LOW_POLE",
                    "pole_column_index": 6,
                    "reversal_column_index": 7,
                    "opposing_pole_distance_columns": 3,
                    "enhanced_by_opposing_pole": True,
                }
            ]
            signal = trader.detect_latest_p2_survivor_demo_signal(
                "BINANCE_FUT:SOLUSDT",
                trader.PnFProfile("t", 1.0, 3),
                [trader.Candle(500, 100, 100, 100), trader.Candle(1_500, 101, 101, 101, 101)],
            )
        finally:
            trader.PnFEngine = original_engine
            trader.detect_pole_patterns = original_detect

        self.assertIsNotNone(signal)
        self.assertEqual(signal.pattern, trader.P2_SURVIVOR_PATTERN)
        self.assertEqual(signal.side, "LONG")
        self.assertIn(trader.P2_SURVIVOR_STRATEGY_ID, signal.pattern_quality)
        self.assertIn(f"candidate_id={trader.P2_SURVIVOR_CANDIDATE_ID}", signal.pattern_quality)
        self.assertIn(f"relative_pole_size={trader.P2_SURVIVOR_RELATIVE_POLE_SIZE}", signal.pattern_quality)
        self.assertIn(f"reversal_boxes={trader.P2_SURVIVOR_REVERSAL_BOXES}", signal.pattern_quality)


if __name__ == "__main__":
    unittest.main()
