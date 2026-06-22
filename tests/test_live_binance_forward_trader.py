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

    def get_user_trades(self, symbol, *, order_id=None, start_time=None, end_time=None, limit=None):
        return [{"orderId": int(order_id), "commission": "0.00001", "commissionAsset": "USDT"}]

    def get_position_risk(self, symbol):
        return [{"symbol": symbol, "positionAmt": "0.009", "entryPrice": "100.50", "positionSide": "BOTH"}]

    def get_algo_order(self, symbol, *, algo_id=None, client_algo_id=None):
        return {"symbol": symbol, "algoId": algo_id or client_algo_id, "status": "NEW"}

    def submit_algo_order(self, order):
        self.submitted_orders.append(order)
        return {"algoId": f"algo-{len(self.submitted_orders)}", "clientAlgoId": order.get("clientAlgoId"), "status": "NEW"}

    def get_mark_price(self, symbol):
        return Decimal("100.00")

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

    def test_inspect_execution_intents_empty_table_returns_zero_counts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db_path = os.path.join(temp_dir, "live_state.db")
            with closing(sqlite3.connect(state_db_path)) as conn:
                trader.init_execution_intents_table(conn)

            output = io.StringIO()
            with redirect_stdout(output):
                trader.inspect_execution_intents_once(argparse.Namespace(state_db_path=state_db_path))
            with closing(sqlite3.connect(state_db_path)) as conn:
                table_exists = trader.execution_intents_table_exists(conn)

        logs = output.getvalue()
        self.assertIn("INTENTS_TOTAL 0", logs)
        self.assertIn("INTENTS_NEW 0", logs)
        self.assertIn("INTENTS_READY 0", logs)
        self.assertIn("INTENTS_CANCELLED 0", logs)
        self.assertNotIn("INTENT_ROW", logs)
        self.assertTrue(table_exists)

    def test_inspect_execution_intents_populated_db_prints_expected_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db_path = os.path.join(temp_dir, "live_state.db")
            with closing(sqlite3.connect(state_db_path)) as conn:
                trader.init_execution_intents_table(conn)
                conn.executemany(
                    """
                    INSERT INTO execution_intents(
                        intent_id, setup_id, symbol, side, entry, stop, tp1, tp2, rr1, rr2,
                        reference_ts, created_ts, intent_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        ("intent-a", "setup-a", "BTCUSDT", "LONG", "100", "95", "105", "110", "1", "2", 1700000000, 1700000001, "NEW"),
                        ("intent-b", "setup-b", "ETHUSDT", "LONG", "200", "190", "210", "220", "1", "2", 1700000100, 1700000101, "READY"),
                        ("intent-c", "setup-c", "SOLUSDT", "SHORT", "50", "55", "45", "40", None, None, 1700000200, 1700000201, "CANCELLED"),
                    ],
                )
                conn.commit()

            output = io.StringIO()
            with redirect_stdout(output):
                trader.inspect_execution_intents_once(argparse.Namespace(state_db_path=state_db_path))
            with closing(sqlite3.connect(state_db_path)) as conn:
                statuses = conn.execute(
                    "SELECT setup_id, intent_status FROM execution_intents ORDER BY setup_id"
                ).fetchall()

        logs = output.getvalue()
        self.assertIn("INTENTS_TOTAL 3", logs)
        self.assertIn("INTENTS_NEW 1", logs)
        self.assertIn("INTENTS_READY 1", logs)
        self.assertIn("INTENTS_CANCELLED 1", logs)
        self.assertEqual(logs.count("INTENT_ROW"), 3)
        self.assertIn('"intent_id": "intent-a"', logs)
        self.assertIn('"setup_id": "setup-b"', logs)
        self.assertIn('"intent_status": "CANCELLED"', logs)
        self.assertIn('"rr1": null', logs)
        self.assertEqual(statuses, [("setup-a", "NEW"), ("setup-b", "READY"), ("setup-c", "CANCELLED")])

    def test_inspect_execution_intents_missing_table_exits_cleanly(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db_path = os.path.join(temp_dir, "live_state.db")
            sqlite3.connect(state_db_path).close()

            output = io.StringIO()
            with redirect_stdout(output):
                trader.inspect_execution_intents_once(argparse.Namespace(state_db_path=state_db_path))
            with closing(sqlite3.connect(state_db_path)) as conn:
                table_exists = trader.execution_intents_table_exists(conn)

        logs = output.getvalue()
        self.assertIn("INTENTS_TOTAL 0", logs)
        self.assertIn("INTENTS_NEW 0", logs)
        self.assertIn("INTENTS_READY 0", logs)
        self.assertIn("INTENTS_CANCELLED 0", logs)
        self.assertFalse(table_exists)

    def test_seed_mexc_dry_run_intents_requires_double_confirmation_flags(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db_path = os.path.join(temp_dir, "live_state.db")
            with self.assertRaises(SystemExit):
                trader.seed_mexc_dry_run_intents_once(
                    argparse.Namespace(
                        state_db_path=state_db_path,
                        seed_mexc_dry_run_intents=True,
                        allow_test_seed=False,
                    )
                )
            self.assertFalse(os.path.exists(state_db_path))

    def test_seed_mexc_dry_run_intents_creates_new_mexc_intents(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db_path = os.path.join(temp_dir, "live_state.db")
            output = io.StringIO()
            with redirect_stdout(output):
                trader.seed_mexc_dry_run_intents_once(
                    argparse.Namespace(
                        state_db_path=state_db_path,
                        seed_mexc_dry_run_intents=True,
                        allow_test_seed=True,
                    )
                )
            with closing(sqlite3.connect(state_db_path)) as conn:
                rows = conn.execute(
                    """
                    SELECT setup_id, intent_id, symbol, side, entry, stop, tp1, tp2, rr1, rr2, intent_status
                    FROM execution_intents
                    ORDER BY setup_id
                    """
                ).fetchall()
                strategy_table = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'strategy_setups'"
                ).fetchone()

        logs = output.getvalue()
        self.assertIn("MEXC_DRY_RUN_INTENTS_SEEDED 7", logs)
        self.assertEqual(logs.count("MEXC_DRY_RUN_INTENT_SEEDED"), 7)
        self.assertEqual(len(rows), 7)
        self.assertIsNone(strategy_table)
        for setup_id, intent_id, symbol, side, entry, stop, tp1, tp2, rr1, rr2, status in rows:
            self.assertTrue(setup_id.startswith("test-mexc-dry-run-"))
            self.assertEqual(intent_id, trader.execution_intent_id(setup_id))
            self.assertTrue(symbol.startswith("MEXC_FUT:"))
            self.assertEqual(side, "LONG")
            self.assertLess(Decimal(stop), Decimal(entry))
            self.assertLess(Decimal(entry), Decimal(tp1))
            self.assertLess(Decimal(tp1), Decimal(tp2))
            self.assertEqual((rr1, rr2, status), ("2", "3", "NEW"))
        self.assertIn(("test-mexc-dry-run-taousdt", trader.execution_intent_id("test-mexc-dry-run-taousdt"), "MEXC_FUT:TAOUSDT", "LONG", "100", "99", "102", "103", "2", "3", "NEW"), rows)
        self.assertIn(("test-mexc-dry-run-hypeusdt", trader.execution_intent_id("test-mexc-dry-run-hypeusdt"), "MEXC_FUT:HYPEUSDT", "LONG", "100", "99", "102", "103", "2", "3", "NEW"), rows)

    def test_seed_mexc_dry_run_intents_skips_duplicates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db_path = os.path.join(temp_dir, "live_state.db")
            args = argparse.Namespace(state_db_path=state_db_path, seed_mexc_dry_run_intents=True, allow_test_seed=True)
            with redirect_stdout(io.StringIO()):
                trader.seed_mexc_dry_run_intents_once(args)
            output = io.StringIO()
            with redirect_stdout(output):
                trader.seed_mexc_dry_run_intents_once(args)
            with closing(sqlite3.connect(state_db_path)) as conn:
                count = conn.execute("SELECT COUNT(*) FROM execution_intents").fetchone()[0]

        logs = output.getvalue()
        self.assertEqual(count, 7)
        self.assertIn("MEXC_DRY_RUN_INTENTS_SEEDED 0", logs)
        self.assertEqual(logs.count("MEXC_DRY_RUN_INTENT_ALREADY_EXISTS"), 7)

    def test_seed_mexc_dry_run_intents_does_not_alter_existing_binance_ready_intents(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db_path = os.path.join(temp_dir, "live_state.db")
            with closing(sqlite3.connect(state_db_path)) as conn:
                trader.init_execution_intents_table(conn)
                conn.execute(
                    """
                    INSERT INTO execution_intents(
                        intent_id, setup_id, symbol, side, entry, stop, tp1, tp2, rr1, rr2,
                        reference_ts, created_ts, intent_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("intent-binance-ready", "binance-ready", "BINANCE_FUT:BTCUSDT", "LONG", "100", "99", "102", "103", "2", "3", 1, 2, "READY"),
                )
                conn.commit()
            with redirect_stdout(io.StringIO()):
                trader.seed_mexc_dry_run_intents_once(
                    argparse.Namespace(state_db_path=state_db_path, seed_mexc_dry_run_intents=True, allow_test_seed=True)
                )
            with closing(sqlite3.connect(state_db_path)) as conn:
                ready = conn.execute(
                    "SELECT intent_id, setup_id, symbol, intent_status FROM execution_intents WHERE setup_id = ?",
                    ("binance-ready",),
                ).fetchone()
                mexc_count = conn.execute("SELECT COUNT(*) FROM execution_intents WHERE symbol LIKE 'MEXC_FUT:%'").fetchone()[0]

        self.assertEqual(ready, ("intent-binance-ready", "binance-ready", "BINANCE_FUT:BTCUSDT", "READY"))
        self.assertEqual(mexc_count, 7)

    def test_seed_mexc_dry_run_intents_does_not_initialize_clients_or_submit_orders(self):
        class ForbiddenClient:
            def __init__(self, *args, **kwargs):
                raise AssertionError("client must not be initialized while seeding MEXC dry-run intents")

        original_binance_client = trader.BinanceFuturesClient
        original_mexc_client = trader.MexcFuturesExecutionClient
        try:
            trader.BinanceFuturesClient = ForbiddenClient
            trader.MexcFuturesExecutionClient = ForbiddenClient
            with tempfile.TemporaryDirectory() as temp_dir:
                state_db_path = os.path.join(temp_dir, "live_state.db")
                output = io.StringIO()
                with redirect_stdout(output):
                    trader.seed_mexc_dry_run_intents_once(
                        argparse.Namespace(state_db_path=state_db_path, seed_mexc_dry_run_intents=True, allow_test_seed=True)
                    )
        finally:
            trader.BinanceFuturesClient = original_binance_client
            trader.MexcFuturesExecutionClient = original_mexc_client

        self.assertIn("MEXC_DRY_RUN_INTENTS_SEEDED 7", output.getvalue())

    def test_inspect_execution_intents_does_not_initialize_binance_client(self):
        class ForbiddenClient:
            def __init__(self, *args, **kwargs):
                raise AssertionError("Binance client must not be initialized in inspection mode")

        original_client = trader.BinanceFuturesClient
        try:
            trader.BinanceFuturesClient = ForbiddenClient
            with tempfile.TemporaryDirectory() as temp_dir:
                state_db_path = os.path.join(temp_dir, "live_state.db")
                with closing(sqlite3.connect(state_db_path)) as conn:
                    trader.init_execution_intents_table(conn)
                output = io.StringIO()
                with redirect_stdout(output):
                    trader.inspect_execution_intents_once(argparse.Namespace(state_db_path=state_db_path))
        finally:
            trader.BinanceFuturesClient = original_client

        self.assertIn("INTENTS_TOTAL 0", output.getvalue())

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

    def test_mexc_dry_run_startup_log_uses_mexc_execution_context(self):
        args = argparse.Namespace(
            db_path=None,
            state_db_path="data/mexc_phase_a_state.db",
            dry_run=True,
            demo=False,
            execute_mexc_intents=True,
            mexc_demo_or_live_mode_name_if_supported="DRY_RUN",
            enable_demo_doubles=False,
            enable_demo_pole_motif=False,
            demo_max_notional_usdt=str(trader.MAX_NOTIONAL_USDT),
        )
        output = io.StringIO()
        with redirect_stdout(output):
            trader.log_startup(args)

        startup_line = next(line for line in output.getvalue().splitlines() if " STARTUP " in line)
        self.assertIn("STARTUP mode=MEXC_DRY_RUN_PHASE_A", startup_line)
        self.assertIn('"venue": "MEXC_FUT"', startup_line)
        self.assertIn('"execution": "DRY_RUN"', startup_line)
        self.assertIn('"base_url": "MEXC_FUTURES_BASE_URL"', startup_line)
        self.assertIn('"api_key_env": "MEXC_FUTURES_API_KEY"', startup_line)
        self.assertNotIn("PRODUCTION_LIVE", startup_line)
        self.assertNotIn("BINANCE_FUTURES_API_KEY", startup_line)
        self.assertNotIn('"execution": "LIVE"', startup_line)

    def test_binance_production_live_startup_log_remains_unchanged(self):
        old_live = os.environ.get("LIVE_TRADING_ENABLED")
        os.environ["LIVE_TRADING_ENABLED"] = "1"
        try:
            args = argparse.Namespace(
                db_path="data/binance_state.db",
                state_db_path=None,
                dry_run=False,
                demo=False,
                execute_mexc_intents=False,
                enable_demo_doubles=False,
                enable_demo_pole_motif=False,
                demo_max_notional_usdt=str(trader.MAX_NOTIONAL_USDT),
            )
            output = io.StringIO()
            with redirect_stdout(output):
                trader.log_startup(args)
        finally:
            if old_live is None:
                os.environ.pop("LIVE_TRADING_ENABLED", None)
            else:
                os.environ["LIVE_TRADING_ENABLED"] = old_live

        startup_line = next(line for line in output.getvalue().splitlines() if " STARTUP " in line)
        self.assertIn("STARTUP mode=PRODUCTION_LIVE", startup_line)
        self.assertIn('"venue": "PRODUCTION"', startup_line)
        self.assertIn('"execution": "LIVE"', startup_line)
        self.assertIn(f'"base_url": "{trader.BINANCE_BASE_URL}"', startup_line)
        self.assertIn('"api_key_env": "BINANCE_FUTURES_API_KEY"', startup_line)

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
        query = "symbol=SOLUSDT&side=BUY&recvWindow=10000&timestamp=1700000000000"
        expected = trader.hmac.new(b"secret", query.encode("utf-8"), trader.hashlib.sha256).hexdigest()
        self.assertEqual(signed["signature"], expected)

    def test_order_signing_syncs_binance_server_time_offset(self):
        class OffsetClient(trader.BinanceFuturesClient):
            def get_server_time(self):
                return 1700000007500

        client = OffsetClient("key", "secret")
        original_time = trader.time.time
        output = io.StringIO()
        try:
            trader.time.time = lambda: 1700000000.0
            with redirect_stdout(output):
                signed = client._signed_params({"symbol": "SOLUSDT", "side": "BUY"})
        finally:
            trader.time.time = original_time

        self.assertEqual(signed["timestamp"], 1700000006000)
        self.assertLess(signed["timestamp"], 1700000007500)
        self.assertEqual(signed["recvWindow"], 10000)
        self.assertIn("BINANCE_TIME_SYNC", output.getvalue())
        self.assertIn('"server_time": 1700000007500', output.getvalue())
        self.assertIn('"local_time": 1700000000000', output.getvalue())
        self.assertIn('"offset_ms": 7500', output.getvalue())


    def test_signed_timestamp_uses_safety_margin_not_ahead_of_server(self):
        class OffsetClient(trader.BinanceFuturesClient):
            def get_server_time(self):
                return 1700000014061

        client = OffsetClient("key", "secret")
        original_time = trader.time.time
        try:
            trader.time.time = lambda: 1700000000.0
            signed = client._signed_params({"symbol": "SOLUSDT"})
        finally:
            trader.time.time = original_time

        self.assertEqual(signed["timestamp"], 1700000012561)
        self.assertLessEqual(signed["timestamp"], 1700000014061 - 1500)

    def test_signed_request_timestamp_error_resyncs_and_retries_once(self):
        class FakeResponse:
            def __init__(self, payload):
                self.payload = payload
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
            def read(self):
                return self.payload

        calls = []
        signed_order_urls = []

        def fake_urlopen(request, timeout=15):
            calls.append(request.full_url)
            if "/fapi/v1/time" in request.full_url:
                return FakeResponse(b'{"serverTime":1700000005000}')
            signed_order_urls.append(request.full_url)
            if len(signed_order_urls) == 1:
                raise trader.urllib.error.HTTPError(request.full_url, 400, "Bad Request", {}, io.BytesIO(b'{"code":-1021,"msg":"Timestamp ahead"}'))
            return FakeResponse(b'{"orderId":123}')

        client = trader.BinanceFuturesClient("key", "secret")
        original_urlopen = trader.urllib.request.urlopen
        output = io.StringIO()
        try:
            trader.urllib.request.urlopen = fake_urlopen
            with redirect_stdout(output):
                result = client._request_json("GET", "/fapi/v1/order", params={"symbol": "SOLUSDT"}, signed=True)
        finally:
            trader.urllib.request.urlopen = original_urlopen

        self.assertEqual(result, {"orderId": 123})
        self.assertEqual(len(signed_order_urls), 2)
        self.assertEqual(sum(1 for url in calls if "/fapi/v1/time" in url), 2)
        self.assertIn("BINANCE_SIGNED_RETRY_TIMESTAMP", output.getvalue())

    def test_signed_request_non_timestamp_error_is_not_retried(self):
        class FakeResponse:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
            def read(self):
                return b'{"serverTime":1700000005000}'

        signed_order_calls = 0

        def fake_urlopen(request, timeout=15):
            nonlocal signed_order_calls
            if "/fapi/v1/time" in request.full_url:
                return FakeResponse()
            signed_order_calls += 1
            raise trader.urllib.error.HTTPError(request.full_url, 400, "Bad Request", {}, io.BytesIO(b'{"code":-2015,"msg":"Invalid API-key"}'))

        client = trader.BinanceFuturesClient("key", "secret")
        original_urlopen = trader.urllib.request.urlopen
        try:
            trader.urllib.request.urlopen = fake_urlopen
            with self.assertRaisesRegex(RuntimeError, "-2015"):
                client._request_json("GET", "/fapi/v1/order", params={"symbol": "SOLUSDT"}, signed=True)
        finally:
            trader.urllib.request.urlopen = original_urlopen

        self.assertEqual(signed_order_calls, 1)

    def test_signed_request_retry_success_does_not_duplicate_logical_operation(self):
        class FakeResponse:
            def __init__(self, payload):
                self.payload = payload
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
            def read(self):
                return self.payload

        posted_bodies = []

        def fake_urlopen(request, timeout=15):
            if "/fapi/v1/time" in request.full_url:
                return FakeResponse(b'{"serverTime":1700000005000}')
            posted_bodies.append(request.data.decode("utf-8"))
            if len(posted_bodies) == 1:
                raise trader.urllib.error.HTTPError(request.full_url, 400, "Bad Request", {}, io.BytesIO(b'{"code":-1021,"msg":"Timestamp ahead"}'))
            return FakeResponse(b'{"orderId":456,"clientOrderId":"abc"}')

        client = trader.BinanceFuturesClient("key", "secret")
        original_urlopen = trader.urllib.request.urlopen
        try:
            trader.urllib.request.urlopen = fake_urlopen
            result = client._request_json("POST", "/fapi/v1/order", params={"symbol": "SOLUSDT", "newClientOrderId": "abc"}, signed=True)
        finally:
            trader.urllib.request.urlopen = original_urlopen

        self.assertEqual(result["orderId"], 456)
        self.assertEqual(len(posted_bodies), 2)
        self.assertTrue(all(body.count("newClientOrderId=abc") == 1 for body in posted_bodies))

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

    def _create_intent_state_db(self, path, **overrides):
        payload = {
            "intent_id": "intent-test",
            "setup_id": "setup-test",
            "symbol": "BINANCE_FUT:SOLUSDT",
            "side": "LONG",
            "entry": "100",
            "stop": "99",
            "tp1": "102",
            "tp2": "103",
            "rr1": "2",
            "rr2": "3",
            "reference_ts": 1700000000,
            "created_ts": 1700000001,
            "intent_status": "NEW",
            **overrides,
        }
        with closing(sqlite3.connect(path)) as conn:
            trader.init_live_tables(conn)
            trader.init_execution_intents_table(conn)
            conn.execute(
                """
                INSERT INTO execution_intents(
                    intent_id, setup_id, symbol, side, entry, stop, tp1, tp2, rr1, rr2,
                    reference_ts, created_ts, intent_status
                ) VALUES(
                    :intent_id, :setup_id, :symbol, :side, :entry, :stop, :tp1, :tp2, :rr1, :rr2,
                    :reference_ts, :created_ts, :intent_status
                )
                """,
                payload,
            )
            conn.commit()

    def _run_intent_executor(self, state_db_path, *, demo=True, dry_run=False, live_env="1", notional="1", client_cls=SubmittingClient):
        original_client = trader.BinanceFuturesClient
        old_live = os.environ.get("LIVE_TRADING_ENABLED")
        old_key = os.environ.get(trader.BINANCE_DEMO_API_KEY_ENV)
        old_secret = os.environ.get(trader.BINANCE_DEMO_API_SECRET_ENV)
        SubmittingClient.instances = []
        try:
            trader.BinanceFuturesClient = client_cls
            if live_env is None:
                os.environ.pop("LIVE_TRADING_ENABLED", None)
            else:
                os.environ["LIVE_TRADING_ENABLED"] = live_env
            os.environ[trader.BINANCE_DEMO_API_KEY_ENV] = "key"
            os.environ[trader.BINANCE_DEMO_API_SECRET_ENV] = "secret"
            output = io.StringIO()
            with redirect_stdout(output):
                trader.process_execution_intents_once(
                    argparse.Namespace(
                        state_db_path=state_db_path,
                        demo=demo,
                        dry_run=dry_run,
                        notional_usdt=notional,
                    ),
                    iteration=1,
                )
            return output.getvalue(), list(SubmittingClient.instances)
        finally:
            trader.BinanceFuturesClient = original_client
            if old_live is None:
                os.environ.pop("LIVE_TRADING_ENABLED", None)
            else:
                os.environ["LIVE_TRADING_ENABLED"] = old_live
            if old_key is None:
                os.environ.pop(trader.BINANCE_DEMO_API_KEY_ENV, None)
            else:
                os.environ[trader.BINANCE_DEMO_API_KEY_ENV] = old_key
            if old_secret is None:
                os.environ.pop(trader.BINANCE_DEMO_API_SECRET_ENV, None)
            else:
                os.environ[trader.BINANCE_DEMO_API_SECRET_ENV] = old_secret

    def test_execution_intents_demo_only_enforcement(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state)
            logs, clients = self._run_intent_executor(state, demo=False)
        self.assertIn("EXECUTION_INTENT_REJECTED", logs)
        self.assertIn("requires --demo", logs)
        self.assertEqual(clients[0].submitted_orders, [])

    def test_execution_intents_dry_run_blocks_execution(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state)
            logs, clients = self._run_intent_executor(state, dry_run=True)
        self.assertIn("blocked by --dry-run", logs)
        self.assertEqual(clients[0].submitted_orders, [])

    def test_execution_intents_missing_live_trading_enabled_blocks_execution(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state)
            logs, clients = self._run_intent_executor(state, live_env=None)
        self.assertIn("LIVE_TRADING_ENABLED is not 1", logs)
        self.assertEqual(clients[0].submitted_orders, [])

    def test_execution_intents_short_blocked(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state, side="SHORT", stop="101", tp1="98", tp2="97")
            logs, clients = self._run_intent_executor(state)
        self.assertIn("SHORT execution intents are blocked", logs)
        self.assertEqual(clients[0].submitted_orders, [])

    def test_execution_intents_mexc_fut_blocked(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state, symbol="MEXC_FUT:HYPEUSDT")
            logs, clients = self._run_intent_executor(state)
        self.assertIn(trader.UNSUPPORTED_EXECUTION_VENUE, logs)
        self.assertEqual(clients[0].submitted_orders, [])

    def test_execution_intents_invalid_risk_ordering_blocked(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state, stop="101")
            logs, clients = self._run_intent_executor(state)
        self.assertIn("invalid risk levels", logs)
        self.assertEqual(clients[0].submitted_orders, [])

    def test_execution_intents_demo_notional_cap_is_100_usdt(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state)
            ok_logs, ok_clients = self._run_intent_executor(state, notional="100")
            self.assertIn("EXECUTION_INTENT_ORDER_SENT", ok_logs)
            self.assertEqual(len(ok_clients[0].submitted_orders), 1)

            state2 = os.path.join(temp_dir, "state2.db")
            self._create_intent_state_db(state2, intent_id="intent-test-2", setup_id="setup-test-2")
            bad_logs, bad_clients = self._run_intent_executor(state2, notional="101")
        self.assertIn("cap=100", bad_logs)
        self.assertEqual(bad_clients[0].submitted_orders, [])

    def test_execution_intents_successful_fake_order_marks_ready(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state)
            logs, clients = self._run_intent_executor(state)
            with closing(sqlite3.connect(state)) as conn:
                status = conn.execute("SELECT intent_status FROM execution_intents").fetchone()[0]
                trade = conn.execute("SELECT setup_id, intent_id, status FROM live_trades_binance").fetchone()
        self.assertIn("EXECUTION_INTENT_ORDER_SENT", logs)
        self.assertEqual(status, "READY")
        self.assertEqual(trade, ("setup-test", "intent-test", "ORDER_SENT"))
        self.assertEqual(len(clients[0].submitted_orders), 1)

    def _run_mexc_intent_executor(self, state_db_path, *, key="key", secret="secret"):
        old_key = os.environ.get(trader.MEXC_FUTURES_API_KEY_ENV)
        old_secret = os.environ.get(trader.MEXC_FUTURES_API_SECRET_ENV)
        try:
            if key is None:
                os.environ.pop(trader.MEXC_FUTURES_API_KEY_ENV, None)
            else:
                os.environ[trader.MEXC_FUTURES_API_KEY_ENV] = key
            if secret is None:
                os.environ.pop(trader.MEXC_FUTURES_API_SECRET_ENV, None)
            else:
                os.environ[trader.MEXC_FUTURES_API_SECRET_ENV] = secret
            output = io.StringIO()
            with redirect_stdout(output):
                trader.process_mexc_execution_intents_once(
                    argparse.Namespace(
                        state_db_path=state_db_path,
                        mexc_demo_or_live_mode_name_if_supported="DRY_RUN",
                    ),
                    iteration=1,
                )
            return output.getvalue()
        finally:
            if old_key is None:
                os.environ.pop(trader.MEXC_FUTURES_API_KEY_ENV, None)
            else:
                os.environ[trader.MEXC_FUTURES_API_KEY_ENV] = old_key
            if old_secret is None:
                os.environ.pop(trader.MEXC_FUTURES_API_SECRET_ENV, None)
            else:
                os.environ[trader.MEXC_FUTURES_API_SECRET_ENV] = old_secret

    def test_mexc_allowed_symbols_accepted_and_tao_hype_rejected(self):
        self.assertTrue(trader.is_mexc_futures_symbol_allowed("MEXC_FUT:BTCUSDT"))
        self.assertTrue(trader.is_mexc_futures_symbol_allowed("ENAUSDT"))
        self.assertFalse(trader.is_mexc_futures_symbol_allowed("MEXC_FUT:TAOUSDT"))
        self.assertFalse(trader.is_mexc_futures_symbol_allowed("MEXC_FUT:HYPEUSDT"))

    def test_mexc_contract_response_parses_allowed_symbols_and_marks_tao_hype_unsupported(self):
        response = {
            "success": True,
            "data": [
                {
                    "symbol": "BTC_USDT",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "priceUnit": "0.1",
                    "priceScale": 1,
                    "volScale": 3,
                    "minVol": "0.001",
                    "minNotional": "5",
                    "contractSize": "0.0001",
                    "maxLeverage": 200,
                    "orderTypes": ["LIMIT", "MARKET"],
                    "state": 0,
                },
                {"symbol": "ETH_USDT", "baseCoin": "ETH", "quoteCoin": "USDT", "priceUnit": "0.01", "volScale": 3, "minVol": "0.001", "contractSize": "0.01", "maxLeverage": 200, "state": 0},
                {"symbol": "SOL_USDT", "baseCoin": "SOL", "quoteCoin": "USDT", "priceUnit": "0.001", "volScale": 1, "minVol": "0.1", "contractSize": "1", "maxLeverage": 100, "state": 0},
                {"symbol": "SUI_USDT", "baseCoin": "SUI", "quoteCoin": "USDT", "priceUnit": "0.0001", "volScale": 1, "minVol": "1", "contractSize": "1", "maxLeverage": 50, "state": 0},
                {"symbol": "ENA_USDT", "baseCoin": "ENA", "quoteCoin": "USDT", "priceUnit": "0.0001", "volScale": 1, "minVol": "1", "contractSize": "1", "maxLeverage": 50, "state": 0},
                {"symbol": "TAO_USDT", "baseCoin": "TAO", "quoteCoin": "USDT", "priceUnit": "0.01", "minVol": "0.01", "contractSize": "0.01", "maxLeverage": 50, "state": 0},
                {"symbol": "HYPE_USDT", "baseCoin": "HYPE", "quoteCoin": "USDT", "priceUnit": "0.001", "minVol": "0.1", "contractSize": "0.1", "maxLeverage": 50, "state": 0},
            ],
        }
        specs = trader.parse_mexc_contract_specs(response)
        by_symbol = {spec["symbol"]: spec for spec in specs}
        self.assertTrue(by_symbol["BTCUSDT"]["supported"])
        self.assertEqual(by_symbol["BTCUSDT"]["base_asset"], "BTC")
        self.assertEqual(by_symbol["BTCUSDT"]["tick_size"], "0.1")
        self.assertEqual(by_symbol["BTCUSDT"]["minimum_notional"], "5")
        self.assertEqual(by_symbol["BTCUSDT"]["supported_order_types"], ["LIMIT", "MARKET"])
        self.assertFalse(by_symbol["TAOUSDT"]["supported"])
        self.assertEqual(by_symbol["TAOUSDT"]["unsupported_reason"], "NOT_ENABLED_FOR_MEXC_EXECUTION")
        self.assertFalse(by_symbol["HYPEUSDT"]["supported"])
        self.assertEqual(sum(1 for spec in specs if spec["supported"]), 5)

    def test_mexc_contract_missing_symbol_handled_cleanly(self):
        specs = trader.parse_mexc_contract_specs({"success": True, "data": [{"symbol": "BTC_USDT"}]})
        by_symbol = {spec["symbol"]: spec for spec in specs}
        self.assertEqual(by_symbol["ETHUSDT"]["unsupported_reason"], "MISSING_FROM_EXCHANGE_RESPONSE")
        self.assertFalse(by_symbol["ETHUSDT"]["supported"])

    def test_mexc_contract_malformed_response_handled_cleanly(self):
        with self.assertRaisesRegex(RuntimeError, "missing data"):
            trader.parse_mexc_contract_specs({"success": True, "data": None})

    def test_inspect_mexc_contracts_prints_summary_and_does_not_initialize_order_clients(self):
        class FakePublicClient:
            def __init__(self, *, base_url):
                self.base_url = base_url

            def get_contract_details(self):
                return {
                    "success": True,
                    "data": [
                        {"symbol": "BTC_USDT", "baseCoin": "BTC", "quoteCoin": "USDT", "priceUnit": "0.1", "minVol": "0.001", "contractSize": "0.0001", "maxLeverage": 200, "state": 0},
                        {"symbol": "ETH_USDT", "baseCoin": "ETH", "quoteCoin": "USDT", "priceUnit": "0.01", "minVol": "0.001", "contractSize": "0.01", "maxLeverage": 200, "state": 0},
                        {"symbol": "SOL_USDT", "baseCoin": "SOL", "quoteCoin": "USDT", "priceUnit": "0.001", "minVol": "0.1", "contractSize": "1", "maxLeverage": 100, "state": 0},
                        {"symbol": "SUI_USDT", "baseCoin": "SUI", "quoteCoin": "USDT", "priceUnit": "0.0001", "minVol": "1", "contractSize": "1", "maxLeverage": 50, "state": 0},
                        {"symbol": "ENA_USDT", "baseCoin": "ENA", "quoteCoin": "USDT", "priceUnit": "0.0001", "minVol": "1", "contractSize": "1", "maxLeverage": 50, "state": 0},
                        {"symbol": "TAO_USDT", "baseCoin": "TAO", "quoteCoin": "USDT", "state": 0},
                        {"symbol": "HYPE_USDT", "baseCoin": "HYPE", "quoteCoin": "USDT", "state": 0},
                    ],
                }

        class ForbiddenOrderClient:
            def __init__(self, *args, **kwargs):
                raise AssertionError("order/execution clients must not initialize during MEXC contract inspection")

        original_public = trader.MexcFuturesPublicClient
        original_binance = trader.BinanceFuturesClient
        original_mexc_execution = trader.MexcFuturesExecutionClient
        try:
            trader.MexcFuturesPublicClient = FakePublicClient
            trader.BinanceFuturesClient = ForbiddenOrderClient
            trader.MexcFuturesExecutionClient = ForbiddenOrderClient
            output = io.StringIO()
            with redirect_stdout(output):
                trader.inspect_mexc_contracts_once(argparse.Namespace(mexc_futures_base_url="https://example.invalid"))
        finally:
            trader.MexcFuturesPublicClient = original_public
            trader.BinanceFuturesClient = original_binance
            trader.MexcFuturesExecutionClient = original_mexc_execution
        logs = output.getvalue()
        self.assertIn("MEXC_CONTRACT_SPEC", logs)
        self.assertIn('"symbol": "BTCUSDT"', logs)
        self.assertIn('"unsupported_reason": "NOT_ENABLED_FOR_MEXC_EXECUTION"', logs)
        self.assertIn("MEXC_CONTRACTS_FOUND 7", logs)
        self.assertIn("MEXC_CONTRACTS_SUPPORTED 5", logs)
        self.assertIn("MEXC_CONTRACTS_UNSUPPORTED 2", logs)

    def test_mexc_position_size_enforces_bankroll_leverage_and_risk_caps(self):
        signal = trader.TriangleSignal(
            symbol="MEXC_FUT:BTCUSDT",
            pattern="execution_intent",
            side="LONG",
            trigger_ts=1,
            entry_price=Decimal("100"),
            stop_price=Decimal("99"),
            tp1_price=Decimal("102"),
            tp2_price=Decimal("103"),
            trigger_column_idx=0,
            support_level=Decimal("99"),
            resistance_level=Decimal("100"),
            break_distance_boxes=Decimal("0"),
            pattern_quality="test",
        )
        plan, reason = trader.calculate_mexc_futures_position_size(signal)
        self.assertIsNone(reason)
        self.assertLessEqual(plan.risk_usdt, Decimal("0.20"))
        self.assertLessEqual(plan.notional_usdt, Decimal("20"))
        self.assertEqual(plan.leverage, Decimal("5"))

    def test_mexc_order_payload_uses_btc_contract_size_symbol_tick_and_integer_vol(self):
        signal = trader.TriangleSignal(
            symbol="MEXC_FUT:BTCUSDT", pattern="execution_intent", side="LONG", trigger_ts=1,
            entry_price=Decimal("100.06"), stop_price=Decimal("99.99"), tp1_price=Decimal("101"), tp2_price=Decimal("102"),
            trigger_column_idx=0, support_level=Decimal("99.99"), resistance_level=Decimal("100.06"),
            break_distance_boxes=Decimal("0"), pattern_quality="test",
        )
        plan, reason = trader.calculate_mexc_futures_position_size(signal, contract_size=Decimal("0.0001"), tick_size=Decimal("0.1"))
        self.assertIsNone(reason)
        self.assertEqual(plan.mexc_symbol, "BTC_USDT")
        self.assertEqual(plan.rounded_entry, Decimal("100.0"))
        self.assertEqual(plan.vol, 2000)
        self.assertIsInstance(plan.vol, int)
        order = trader.mexc_order_from_plan({"intent_id": "intent-btc-test"}, plan)
        self.assertEqual(order["symbol"], "BTC_USDT")
        self.assertEqual(order["price"], "100.0")
        self.assertEqual(order["vol"], 2000)
        self.assertEqual(order["side"], 1)
        self.assertEqual(order["type"], 1)
        self.assertEqual(order["openType"], 1)
        self.assertEqual(order["leverage"], 5)
        self.assertEqual(order["externalOid"], "pnf-mexc-btc-test")

    def test_mexc_order_payload_uses_ena_contract_size(self):
        signal = trader.TriangleSignal(
            symbol="MEXC_FUT:ENAUSDT", pattern="execution_intent", side="LONG", trigger_ts=1,
            entry_price=Decimal("0.50009"), stop_price=Decimal("0.49999"), tp1_price=Decimal("0.51"), tp2_price=Decimal("0.52"),
            trigger_column_idx=0, support_level=Decimal("0.49999"), resistance_level=Decimal("0.50009"),
            break_distance_boxes=Decimal("0"), pattern_quality="test",
        )
        plan, reason = trader.calculate_mexc_futures_position_size(signal, contract_size=Decimal("1"), tick_size=Decimal("0.00001"), price_precision=5)
        self.assertIsNone(reason)
        self.assertEqual(plan.mexc_symbol, "ENA_USDT")
        self.assertEqual(plan.rounded_entry, Decimal("0.50009"))
        order = trader.mexc_order_from_plan({"intent_id": "intent-ena-test"}, plan)
        self.assertEqual(order["price"], "0.50009")
        self.assertEqual(plan.vol, 39)
        self.assertEqual(plan.quantity, Decimal("39"))


    def test_mexc_order_payload_formats_symbol_price_precision(self):
        cases = [
            ("BTCUSDT", Decimal("100.06"), "100.0"),
            ("ETHUSDT", Decimal("100.019"), "100.01"),
            ("SOLUSDT", Decimal("150.123"), "150.12"),
            ("SUIUSDT", Decimal("3.50009"), "3.5000"),
            ("ENAUSDT", Decimal("0.500099"), "0.50009"),
        ]
        for symbol, entry, expected_price in cases:
            with self.subTest(symbol=symbol):
                signal = trader.TriangleSignal(
                    symbol=f"MEXC_FUT:{symbol}", pattern="execution_intent", side="LONG", trigger_ts=1,
                    entry_price=entry, stop_price=entry - Decimal("0.00001"), tp1_price=entry + Decimal("1"), tp2_price=entry + Decimal("2"),
                    trigger_column_idx=0, support_level=entry - Decimal("0.00001"), resistance_level=entry,
                    break_distance_boxes=Decimal("0"), pattern_quality="test",
                )
                plan, reason = trader.calculate_mexc_futures_position_size(signal)
                self.assertIsNone(reason)
                order = trader.mexc_order_from_plan({"intent_id": f"intent-{symbol.lower()}"}, plan)
                self.assertEqual(order["price"], expected_price)

    def test_mexc_order_builder_enforces_notional_and_risk_caps(self):
        signal = trader.TriangleSignal(
            symbol="MEXC_FUT:BTCUSDT", pattern="execution_intent", side="LONG", trigger_ts=1,
            entry_price=Decimal("100"), stop_price=Decimal("99"), tp1_price=Decimal("101"), tp2_price=Decimal("102"),
            trigger_column_idx=0, support_level=Decimal("99"), resistance_level=Decimal("100"),
            break_distance_boxes=Decimal("0"), pattern_quality="test",
        )
        _plan, reason = trader.calculate_mexc_futures_position_size(signal, contract_size=Decimal("0.0001"), tick_size=Decimal("0.1"), notional_usdt=Decimal("20.01"))
        self.assertIn("20 USDT", reason)
        risky = replace_signal(signal, stop_price=Decimal("90"), support_level=Decimal("90"))
        _plan, reason = trader.calculate_mexc_futures_position_size(risky, contract_size=Decimal("0.0001"), tick_size=Decimal("0.1"))
        self.assertEqual(reason, "0.20 USDT risk cap exceeded")

    def test_mexc_max_open_position_blocks_second_trade(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state, symbol="MEXC_FUT:BTCUSDT")
            with closing(sqlite3.connect(state)) as conn:
                conn.execute(
                    """
                    INSERT INTO live_trades_binance(
                        created_at, symbol, pattern, side, trigger_timestamp, entry_price, stop_price,
                        tp1_price, tp2_price, notional_usdt, decision, status, dry_run
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (trader.now_iso(), "MEXC_FUT:ETHUSDT", "execution_intent", "LONG", 1, 100, 99, 102, 103, 20, "ORDER_SENT", "ORDER_SENT", 1),
                )
                conn.commit()
            logs = self._run_mexc_intent_executor(state)
        self.assertIn("max open MEXC positions reached", logs)

    def test_mexc_missing_credentials_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state, symbol="MEXC_FUT:BTCUSDT")
            logs = self._run_mexc_intent_executor(state, key=None, secret=None)
            with closing(sqlite3.connect(state)) as conn:
                trade_count = conn.execute("SELECT COUNT(*) FROM live_trades_binance").fetchone()[0]
        self.assertIn("API credentials missing", logs)
        self.assertEqual(trade_count, 0)

    def test_mexc_dry_run_records_venue_tagged_trade_without_binance_submit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state, symbol="MEXC_FUT:SOLUSDT")
            logs = self._run_mexc_intent_executor(state)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT symbol, status, dry_run, raw_order_response FROM live_trades_binance").fetchone()
        self.assertIn("MEXC_ORDER_PAYLOAD_DRY_RUN", logs)
        self.assertEqual(row[0], "MEXC_FUT:SOLUSDT")
        self.assertEqual(row[1], "DRY_RUN")
        self.assertEqual(row[2], 1)
        self.assertIn('"venue": "MEXC_FUT"', row[3])

    def test_execution_intents_timestamp_rejection_leaves_intent_new(self):
        class TimestampRejectingClient(SubmittingClient):
            def submit_order(self, order):
                self.submitted_orders.append(order)
                raise RuntimeError('Binance HTTP 400: {"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}')

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state)
            logs, clients = self._run_intent_executor(state, client_cls=TimestampRejectingClient)
            with closing(sqlite3.connect(state)) as conn:
                status = conn.execute("SELECT intent_status FROM execution_intents").fetchone()[0]
                trade_count = conn.execute("SELECT COUNT(*) FROM live_trades_binance").fetchone()[0]
        self.assertIn("EXECUTION_INTENT_ORDER_FAILED", logs)
        self.assertIn("-1021", logs)
        self.assertEqual(status, "NEW")
        self.assertEqual(trade_count, 0)
        self.assertEqual(len(clients[0].submitted_orders), 1)

    def test_execution_intents_rerun_does_not_duplicate_ready_intent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state)
            self._run_intent_executor(state)
            logs, clients = self._run_intent_executor(state)
            with closing(sqlite3.connect(state)) as conn:
                count = conn.execute("SELECT COUNT(*) FROM live_trades_binance").fetchone()[0]
        self.assertIn("intent_status is not NEW: READY", logs)
        self.assertEqual(count, 1)
        self.assertEqual(clients[0].submitted_orders, [])

    def test_execution_intents_existing_open_trade_blocks_same_symbol(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_intent_state_db(state)
            with closing(sqlite3.connect(state)) as conn:
                conn.execute(
                    """
                    INSERT INTO live_trades_binance(
                        created_at, symbol, pattern, side, trigger_timestamp, entry_price, stop_price,
                        tp1_price, tp2_price, notional_usdt, status, dry_run
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (trader.now_iso(), "BINANCE_FUT:SOLUSDT", "x", "LONG", 1, 100, 99, 102, 103, 1, "ORDER_SENT", 0),
                )
                conn.commit()
            logs, clients = self._run_intent_executor(state)
        self.assertIn("existing open live trade on symbol", logs)
        self.assertEqual(clients[0].submitted_orders, [])

    def test_execution_intents_strategy_setups_is_never_mutated(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            setup_db = os.path.join(temp_dir, "strategy_validation.db")
            state = os.path.join(temp_dir, "state.db")
            create_strategy_setups_db(setup_db, [{"setup_id": "setup-test", "symbol": "BINANCE_FUT:SOLUSDT"}])
            self._create_intent_state_db(state)
            with closing(sqlite3.connect(setup_db)) as conn:
                before = conn.execute("SELECT * FROM strategy_setups").fetchall()
            self._run_intent_executor(state)
            with closing(sqlite3.connect(setup_db)) as conn:
                after = conn.execute("SELECT * FROM strategy_setups").fetchall()
        self.assertEqual(before, after)

    def _create_order_sent_trade_state_db(self, state_db_path, *, entry_status="NEW"):
        with closing(sqlite3.connect(state_db_path)) as conn:
            trader.init_live_tables(conn)
            order_request = {"symbol": "SOLUSDT", "newClientOrderId": "entry-client", "side": "BUY"}
            order_response = {"orderId": 42, "clientOrderId": "entry-client", "status": "NEW"}
            conn.execute(
                """
                INSERT INTO live_trades_binance(
                    created_at, symbol, pattern, side, trigger_timestamp, entry_price, stop_price,
                    tp1_price, tp2_price, notional_usdt, decision, status, dry_run, exchange_order_id,
                    raw_order_response, entry_order_status, protective_orders_status, setup_id, intent_id,
                    active_stop_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trader.now_iso(),
                    "BINANCE_FUT:SOLUSDT",
                    "execution_intent",
                    "LONG",
                    1700000000,
                    100,
                    99,
                    102,
                    103,
                    1,
                    "ORDER_SENT",
                    "ORDER_SENT",
                    0,
                    "42",
                    json.dumps({"order_request": order_request, "order_response": order_response}),
                    entry_status,
                    "PENDING_ENTRY_FILL",
                    "setup-test",
                    "intent-test",
                    99,
                ),
            )
            conn.commit()

    def _run_execution_trade_sync(self, state_db_path, *, demo=True, dry_run=False, live_env="1", client_cls=LifecycleClient, status="FILLED"):
        class SyncClient(client_cls):
            instances = []
            def __init__(self, api_key=None, api_secret=None, *, base_url=trader.BINANCE_BASE_URL):
                super().__init__(status=status)
                self.api_key = api_key
                self.api_secret = api_secret
                self.base_url = base_url
                self.has_credentials = bool(api_key and api_secret)
                SyncClient.instances.append(self)

        original_client = trader.BinanceFuturesClient
        old_live = os.environ.get("LIVE_TRADING_ENABLED")
        old_key = os.environ.get(trader.BINANCE_DEMO_API_KEY_ENV)
        old_secret = os.environ.get(trader.BINANCE_DEMO_API_SECRET_ENV)
        try:
            trader.BinanceFuturesClient = SyncClient
            if live_env is None:
                os.environ.pop("LIVE_TRADING_ENABLED", None)
            else:
                os.environ["LIVE_TRADING_ENABLED"] = live_env
            os.environ[trader.BINANCE_DEMO_API_KEY_ENV] = "key"
            os.environ[trader.BINANCE_DEMO_API_SECRET_ENV] = "secret"
            output = io.StringIO()
            with redirect_stdout(output):
                trader.process_sync_execution_trades_once(
                    argparse.Namespace(state_db_path=state_db_path, demo=demo, dry_run=dry_run),
                    iteration=1,
                )
            return output.getvalue(), list(SyncClient.instances)
        finally:
            trader.BinanceFuturesClient = original_client
            if old_live is None:
                os.environ.pop("LIVE_TRADING_ENABLED", None)
            else:
                os.environ["LIVE_TRADING_ENABLED"] = old_live
            if old_key is None:
                os.environ.pop(trader.BINANCE_DEMO_API_KEY_ENV, None)
            else:
                os.environ[trader.BINANCE_DEMO_API_KEY_ENV] = old_key
            if old_secret is None:
                os.environ.pop(trader.BINANCE_DEMO_API_SECRET_ENV, None)
            else:
                os.environ[trader.BINANCE_DEMO_API_SECRET_ENV] = old_secret

    def test_sync_execution_trades_non_demo_blocked(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            with self.assertRaisesRegex(RuntimeError, "requires --demo"):
                self._run_execution_trade_sync(state, demo=False)

    def test_sync_execution_trades_dry_run_blocked(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            with self.assertRaisesRegex(RuntimeError, "blocked by --dry-run"):
                self._run_execution_trade_sync(state, dry_run=True)

    def test_sync_execution_trades_missing_live_env_blocked(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            with self.assertRaisesRegex(RuntimeError, "LIVE_TRADING_ENABLED=1"):
                self._run_execution_trade_sync(state, live_env=None)

    def test_sync_execution_trades_entry_new_no_protective_orders(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state, entry_status="NEW")
            logs, clients = self._run_execution_trade_sync(state, status="NEW")
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, entry_order_status, protective_orders_status FROM live_trades_binance").fetchone()
        self.assertIn("ENTRY_NOT_FILLED", logs)
        self.assertEqual(row, ("ORDER_SENT", "NEW", "PENDING_ENTRY_FILL"))
        self.assertEqual(clients[0].submitted_orders, [])

    def test_sync_execution_trades_entry_filled_attaches_tp_sl(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            logs, clients = self._run_execution_trade_sync(state, status="FILLED")
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, entry_order_status, avg_fill_price, executed_qty, protective_orders_status, stop_algo_id, tp_algo_id FROM live_trades_binance").fetchone()
        self.assertIn("PROTECTIVE_ORDERS_ATTACHED", logs)
        self.assertEqual(row[0:5], ("POSITION_OPEN", "FILLED", 100.50, 0.009, "ATTACHED"))
        self.assertTrue(row[5])
        self.assertTrue(row[6])
        self.assertEqual([order["type"] for order in clients[0].submitted_orders], ["STOP_MARKET", "TAKE_PROFIT_MARKET"])

    def test_sync_execution_trades_duplicate_sync_does_not_attach_twice(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            logs, clients = self._run_execution_trade_sync(state, status="FILLED")
            logs2, clients2 = self._run_execution_trade_sync(state, status="FILLED")
        self.assertIn("PROTECTIVE_ORDERS_ATTACHED", logs)
        self.assertIn("EXECUTION_TRADES_SYNC_FOUND 0", logs2)
        self.assertEqual(len(clients[0].submitted_orders), 2)
        self.assertEqual(len(clients2[0].submitted_orders), 0)

    def test_sync_execution_trades_retries_unprotected_and_does_not_duplicate_after_success(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            with closing(sqlite3.connect(state)) as conn:
                conn.execute(
                    """
                    UPDATE live_trades_binance
                    SET status = 'POSITION_OPEN_UNPROTECTED', entry_order_status = 'FILLED',
                        protective_orders_status = 'ATTACH_FAILED', notes = 'previous attach failed'
                    """
                )
                conn.commit()

            logs, clients = self._run_execution_trade_sync(state, status="FILLED")
            logs2, clients2 = self._run_execution_trade_sync(state, status="FILLED")

            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute(
                    "SELECT status, protective_orders_status, stop_algo_id, tp_algo_id FROM live_trades_binance"
                ).fetchone()

        self.assertIn("UNPROTECTED_TRADES_SYNC_FOUND 1", logs)
        self.assertIn("PROTECTIVE_ORDERS_ATTACHED", logs)
        self.assertIn("UNPROTECTED_TRADES_SYNC_FOUND 0", logs2)
        self.assertEqual(row[0:2], ("POSITION_OPEN", "ATTACHED"))
        self.assertTrue(row[2])
        self.assertTrue(row[3])
        self.assertEqual(len(clients[0].submitted_orders), 2)
        self.assertEqual(len(clients2[0].submitted_orders), 0)

    def test_sync_execution_trades_protective_failure_marks_unprotected(self):
        class FailingProtectiveClient(LifecycleClient):
            def submit_algo_order(self, order):
                self.submitted_orders.append(order)
                raise RuntimeError("protective boom")

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            logs, clients = self._run_execution_trade_sync(state, status="FILLED", client_cls=FailingProtectiveClient)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, protective_orders_status, protective_orders_error FROM live_trades_binance").fetchone()
        self.assertIn("PROTECTIVE_ORDER_FAILED", logs)
        self.assertEqual(row[0], "POSITION_OPEN_UNPROTECTED")
        self.assertEqual(row[1], "ATTACH_FAILED")
        self.assertIn("protective boom", row[2])
        self.assertEqual(len(clients[0].submitted_orders), 1)

    def test_sync_execution_trades_long_unprotected_past_stop_emergency_closes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            with closing(sqlite3.connect(state)) as conn:
                conn.execute(
                    """
                    UPDATE live_trades_binance
                    SET status = 'POSITION_OPEN_UNPROTECTED', entry_order_status = 'FILLED',
                        executed_qty = 0.009, active_stop_price = 101,
                        protective_orders_status = 'BLOCKED_IMMEDIATE_TRIGGER'
                    """
                )
                conn.commit()

            logs, clients = self._run_execution_trade_sync(state, status="FILLED")
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute(
                    "SELECT status, protective_orders_status, protective_orders_raw_response FROM live_trades_binance"
                ).fetchone()

        self.assertIn("EMERGENCY_CLOSE_TRIGGERED", logs)
        self.assertIn("EMERGENCY_CLOSE_ORDER_SENT", logs)
        self.assertEqual(row[0:2], ("EMERGENCY_CLOSE_SENT", "EMERGENCY_CLOSE_SENT"))
        self.assertEqual(len(clients[0].submitted_orders), 1)
        close_order = clients[0].submitted_orders[0]
        self.assertEqual(close_order["type"], "MARKET")
        self.assertEqual(close_order["side"], "SELL")
        self.assertEqual(close_order["reduceOnly"], "true")
        self.assertEqual(close_order["quantity"], "0.009")
        self.assertNotIn("STOP_MARKET", row[2])

    def test_sync_execution_trades_long_unprotected_above_stop_does_not_emergency_close(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            with closing(sqlite3.connect(state)) as conn:
                conn.execute(
                    """
                    UPDATE live_trades_binance
                    SET status = 'POSITION_OPEN_UNPROTECTED', entry_order_status = 'FILLED',
                        executed_qty = 0.009, active_stop_price = 99,
                        protective_orders_status = 'ATTACH_FAILED'
                    """
                )
                conn.commit()

            logs, clients = self._run_execution_trade_sync(state, status="FILLED")
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, protective_orders_status FROM live_trades_binance").fetchone()

        self.assertNotIn("EMERGENCY_CLOSE_ORDER_SENT", logs)
        self.assertEqual(row, ("POSITION_OPEN", "ATTACHED"))
        self.assertEqual([order["type"] for order in clients[0].submitted_orders], ["STOP_MARKET", "TAKE_PROFIT_MARKET"])

    def test_sync_execution_trades_emergency_close_duplicate_rerun_does_not_close_twice(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            with closing(sqlite3.connect(state)) as conn:
                conn.execute(
                    """
                    UPDATE live_trades_binance
                    SET status = 'POSITION_OPEN_UNPROTECTED', entry_order_status = 'FILLED',
                        executed_qty = 0.009, active_stop_price = 101,
                        protective_orders_status = 'BLOCKED_IMMEDIATE_TRIGGER'
                    """
                )
                conn.commit()

            logs, clients = self._run_execution_trade_sync(state, status="FILLED")
            logs2, clients2 = self._run_execution_trade_sync(state, status="FILLED")

        self.assertIn("EMERGENCY_CLOSE_ORDER_SENT", logs)
        self.assertIn("UNPROTECTED_TRADES_SYNC_FOUND 0", logs2)
        self.assertEqual(len(clients[0].submitted_orders), 1)
        self.assertEqual(len(clients2[0].submitted_orders), 0)

    def test_sync_execution_trades_emergency_close_submission_failure_keeps_unprotected(self):
        class FailingCloseClient(LifecycleClient):
            def submit_order(self, order):
                self.submitted_orders.append(order)
                raise RuntimeError("emergency close boom")

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_order_sent_trade_state_db(state)
            with closing(sqlite3.connect(state)) as conn:
                conn.execute(
                    """
                    UPDATE live_trades_binance
                    SET status = 'POSITION_OPEN_UNPROTECTED', entry_order_status = 'FILLED',
                        executed_qty = 0.009, active_stop_price = 101,
                        protective_orders_status = 'BLOCKED_IMMEDIATE_TRIGGER'
                    """
                )
                conn.commit()

            logs, clients = self._run_execution_trade_sync(state, status="FILLED", client_cls=FailingCloseClient)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, protective_orders_status, notes FROM live_trades_binance").fetchone()

        self.assertIn("EMERGENCY_CLOSE_FAILED", logs)
        self.assertEqual(row[0:2], ("POSITION_OPEN_UNPROTECTED", "BLOCKED_IMMEDIATE_TRIGGER"))
        self.assertIn("emergency close boom", row[2])
        self.assertEqual(len(clients[0].submitted_orders), 1)


    def _create_open_execution_trade_state_db(self, state_db_path, *, status="POSITION_OPEN"):
        self._create_order_sent_trade_state_db(state_db_path)
        with closing(sqlite3.connect(state_db_path)) as conn:
            conn.execute(
                """
                UPDATE live_trades_binance
                SET status = ?, entry_order_status = 'FILLED', avg_fill_price = 100.50,
                    executed_qty = 0.009, entry_order_update_time = 1700000000000,
                    protective_orders_status = 'ATTACHED', stop_algo_id = 'stop-1', tp_algo_id = 'tp-1'
                """,
                (status,),
            )
            conn.commit()

    def test_sync_execution_trades_tp_closure_detected(self):
        class ClosedTpClient(LifecycleClient):
            def get_position_risk(self, symbol):
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]
            def get_algo_order(self, symbol, *, algo_id=None, client_algo_id=None):
                return {"status": "FILLED" if algo_id == "tp-1" else "NEW"}
            def get_user_trades(self, symbol, *, order_id=None, start_time=None, end_time=None, limit=None):
                self.last_user_trade_request = {"order_id": order_id, "start_time": start_time, "end_time": end_time, "limit": limit}
                return [{"orderId": order_id, "side": "SELL", "price": "103", "qty": "0.009", "realizedPnl": "0.0225", "time": 1700000001000}]

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_open_execution_trade_state_db(state)
            logs, _clients = self._run_execution_trade_sync(state, client_cls=ClosedTpClient)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, exit_ts, exit_price, realized_pnl, realized_pnl_pct, close_reason FROM live_trades_binance").fetchone()
        self.assertIn("POSITION_CLOSED_TP", logs)
        self.assertEqual(row[0], "POSITION_CLOSED_TP")
        self.assertEqual(row[1], 1700000001000)
        self.assertEqual(row[2], 103.0)
        self.assertEqual(row[3], 0.0225)
        self.assertEqual(row[5], "POSITION_CLOSED_TP")

    def test_sync_execution_trades_stop_closure_detected(self):
        class ClosedStopClient(LifecycleClient):
            def get_position_risk(self, symbol):
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]
            def get_algo_order(self, symbol, *, algo_id=None, client_algo_id=None):
                return {"status": "FILLED" if algo_id == "stop-1" else "NEW"}
            def get_user_trades(self, symbol, *, order_id=None, start_time=None, end_time=None, limit=None):
                return [{"orderId": order_id, "side": "SELL", "price": "99", "qty": "0.009", "realizedPnl": "-0.0135", "time": 1700000002000}]

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_open_execution_trade_state_db(state)
            logs, _clients = self._run_execution_trade_sync(state, client_cls=ClosedStopClient)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, exit_price, realized_pnl, close_reason FROM live_trades_binance").fetchone()
        self.assertIn("POSITION_CLOSED_STOP", logs)
        self.assertEqual(row, ("POSITION_CLOSED_STOP", 99.0, -0.0135, "POSITION_CLOSED_STOP"))

    def test_sync_execution_trades_manual_closure_detected(self):
        class ClosedManualClient(LifecycleClient):
            def get_position_risk(self, symbol):
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]
            def get_user_trades(self, symbol, *, order_id=None, start_time=None, end_time=None, limit=None):
                return [{"side": "SELL", "price": "101", "qty": "0.009", "realizedPnl": "0.0045", "time": 1700000003000}]

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_open_execution_trade_state_db(state)
            logs, _clients = self._run_execution_trade_sync(state, client_cls=ClosedManualClient)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, exit_price, realized_pnl, close_reason FROM live_trades_binance").fetchone()
        self.assertIn("POSITION_CLOSED_MANUAL", logs)
        self.assertEqual(row, ("POSITION_CLOSED_MANUAL", 101.0, 0.0045, "POSITION_CLOSED_MANUAL"))


    def test_sync_execution_trades_stale_tp_execution_falls_back_to_manual(self):
        class StaleTpClient(LifecycleClient):
            def get_position_risk(self, symbol):
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]
            def get_algo_order(self, symbol, *, algo_id=None, client_algo_id=None):
                return {"status": "FILLED" if algo_id == "tp-1" else "NEW"}
            def get_user_trades(self, symbol, *, order_id=None, start_time=None, end_time=None, limit=None):
                if order_id == "tp-1":
                    return [{"orderId": order_id, "side": "SELL", "price": "103", "qty": "0.009", "realizedPnl": "0.0225", "time": 1699999999000}]
                return [{"side": "SELL", "price": "101", "qty": "0.009", "realizedPnl": "0.0045", "time": 1700000003000}]

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_open_execution_trade_state_db(state)
            logs, _clients = self._run_execution_trade_sync(state, client_cls=StaleTpClient)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, close_reason, exit_price, realized_pnl FROM live_trades_binance").fetchone()
        self.assertIn("CLOSE_CLASSIFICATION_MANUAL", logs)
        self.assertEqual(row, ("POSITION_CLOSED_MANUAL", "POSITION_CLOSED_MANUAL", 101.0, 0.0045))

    def test_sync_execution_trades_stale_stop_execution_falls_back_to_manual(self):
        class StaleStopClient(LifecycleClient):
            def get_position_risk(self, symbol):
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]
            def get_algo_order(self, symbol, *, algo_id=None, client_algo_id=None):
                return {"status": "FILLED" if algo_id == "stop-1" else "NEW"}
            def get_user_trades(self, symbol, *, order_id=None, start_time=None, end_time=None, limit=None):
                if order_id == "stop-1":
                    return [{"orderId": order_id, "side": "SELL", "price": "99", "qty": "0.009", "realizedPnl": "-0.0135", "time": 1699999999000}]
                return [{"side": "SELL", "price": "101", "qty": "0.009", "realizedPnl": "0.0045", "time": 1700000003000}]

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_open_execution_trade_state_db(state)
            logs, _clients = self._run_execution_trade_sync(state, client_cls=StaleStopClient)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, close_reason, exit_price, realized_pnl FROM live_trades_binance").fetchone()
        self.assertIn("CLOSE_CLASSIFICATION_MANUAL", logs)
        self.assertEqual(row, ("POSITION_CLOSED_MANUAL", "POSITION_CLOSED_MANUAL", 101.0, 0.0045))

    def test_sync_execution_trades_tp_stop_ambiguity_falls_back_to_manual(self):
        class AmbiguousClient(LifecycleClient):
            def get_position_risk(self, symbol):
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]
            def get_algo_order(self, symbol, *, algo_id=None, client_algo_id=None):
                return {"status": "FILLED"}
            def get_user_trades(self, symbol, *, order_id=None, start_time=None, end_time=None, limit=None):
                if order_id in {"tp-1", "stop-1"}:
                    return [{"orderId": order_id, "side": "SELL", "price": "100", "qty": "0.009", "realizedPnl": "0", "time": 1700000001000}]
                return [{"side": "SELL", "price": "101", "qty": "0.009", "realizedPnl": "0.0045", "time": 1700000003000}]

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_open_execution_trade_state_db(state)
            logs, _clients = self._run_execution_trade_sync(state, client_cls=AmbiguousClient)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, close_reason FROM live_trades_binance").fetchone()
        self.assertIn("ambiguous_tp_and_stop_causal_executions", logs)
        self.assertEqual(row, ("POSITION_CLOSED_MANUAL", "POSITION_CLOSED_MANUAL"))

    def test_sync_execution_trades_manual_close_metrics_isolation(self):
        class ManualIsolationClient(LifecycleClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.manual_requests = []
            def get_position_risk(self, symbol):
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]
            def get_user_trades(self, symbol, *, order_id=None, start_time=None, end_time=None, limit=None):
                if order_id is None:
                    self.manual_requests.append({"start_time": start_time, "end_time": end_time, "limit": limit})
                return [
                    {"side": "SELL", "price": "90", "qty": "0.009", "realizedPnl": "-0.1", "time": 1699999999000},
                    {"side": "BUY", "price": "105", "qty": "0.009", "realizedPnl": "0.1", "time": 1700000001000},
                    {"side": "SELL", "price": "101", "qty": "0.004", "realizedPnl": "0.002", "time": 1700000002000},
                    {"side": "SELL", "price": "102", "qty": "0.005", "realizedPnl": "0.005", "time": 1700000003000},
                    {"side": "SELL", "price": "110", "qty": "0.009", "realizedPnl": "1.0", "time": 1700000004000},
                ]

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_open_execution_trade_state_db(state)
            logs, clients = self._run_execution_trade_sync(state, client_cls=ManualIsolationClient)
            with closing(sqlite3.connect(state)) as conn:
                row = conn.execute("SELECT status, exit_price, realized_pnl, exit_ts FROM live_trades_binance").fetchone()
        self.assertIn("CLOSE_CLASSIFICATION_MANUAL", logs)
        self.assertEqual(clients[0].manual_requests[0]["start_time"], 1700000000000)
        self.assertEqual(clients[0].manual_requests[0]["limit"], 1000)
        self.assertEqual(row[0], "POSITION_CLOSED_MANUAL")
        self.assertAlmostEqual(row[1], ((101 * 0.004) + (102 * 0.005)) / 0.009)
        self.assertEqual(row[2], 0.007)
        self.assertEqual(row[3], 1700000003000)

    def test_sync_execution_trades_closure_duplicate_rerun_noop_and_closed_skipped(self):
        class CountingClosedClient(LifecycleClient):
            risk_calls = 0
            def get_position_risk(self, symbol):
                type(self).risk_calls += 1
                return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]
            def get_algo_order(self, symbol, *, algo_id=None, client_algo_id=None):
                return {"status": "FILLED" if algo_id == "tp-1" else "NEW"}
            def get_user_trades(self, symbol, *, order_id=None, start_time=None, end_time=None, limit=None):
                return [{"orderId": order_id, "side": "SELL", "price": "103", "qty": "0.009", "realizedPnl": "0.0225", "time": 1700000001000}]

        with tempfile.TemporaryDirectory() as temp_dir:
            state = os.path.join(temp_dir, "state.db")
            self._create_open_execution_trade_state_db(state)
            CountingClosedClient.risk_calls = 0
            logs, _ = self._run_execution_trade_sync(state, client_cls=CountingClosedClient)
            logs2, _ = self._run_execution_trade_sync(state, client_cls=CountingClosedClient)
            with closing(sqlite3.connect(state)) as conn:
                rows = conn.execute("SELECT status FROM live_trades_binance").fetchall()
        self.assertIn("POSITION_CLOSED_TP", logs)
        self.assertIn("OPEN_POSITION_CLOSURE_SYNC_FOUND 0", logs2)
        self.assertEqual(rows, [("POSITION_CLOSED_TP",)])

    def test_sync_execution_trades_does_not_mutate_strategy_setups(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            setup_db = os.path.join(temp_dir, "strategy_validation.db")
            state = os.path.join(temp_dir, "state.db")
            create_strategy_setups_db(setup_db, [{"setup_id": "setup-test", "symbol": "BINANCE_FUT:SOLUSDT"}])
            self._create_order_sent_trade_state_db(state)
            with closing(sqlite3.connect(setup_db)) as conn:
                before = conn.execute("SELECT * FROM strategy_setups").fetchall()
            self._run_execution_trade_sync(state, status="FILLED")
            with closing(sqlite3.connect(setup_db)) as conn:
                after = conn.execute("SELECT * FROM strategy_setups").fetchall()
        self.assertEqual(before, after)

    def test_execution_intents_no_path_runs_unless_flag_supplied(self):
        original_parse_args = trader.parse_args
        original_process_once = trader.process_once
        original_execute_once = trader.process_execution_intents_once
        try:
            calls = []
            trader.process_once = lambda parsed_args, *, iteration=None: calls.append(("signal", iteration))
            trader.process_execution_intents_once = lambda parsed_args, *, iteration=None: calls.append(("intents", iteration))
            trader.parse_args = lambda: argparse.Namespace(
                db_path="unused-market.db",
                state_db_path="unused-state.db",
                settings="unused-settings.json",
                dry_run=True,
                demo=True,
                export_trade_journal=None,
                reconcile_positions=False,
                consume_strategy_setups=False,
                execute_execution_intents=False,
                loop=False,
                poll_seconds=0,
                inspect_execution_intents=False,
                sync_execution_trades=False,
            )
            with redirect_stdout(io.StringIO()):
                trader.main()
        finally:
            trader.parse_args = original_parse_args
            trader.process_once = original_process_once
            trader.process_execution_intents_once = original_execute_once
        self.assertEqual(calls, [("signal", 1)])


if __name__ == "__main__":
    unittest.main()
