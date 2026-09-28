import sqlite3
import json
import tempfile
import unittest
from pathlib import Path

import live_mexc_forward_trader as trader
from tests.test_live_mexc_fill_reconciliation import Adapter, evidence


class NoExchangeCalls:
    has_credentials = True

    def __getattr__(self, name):
        raise AssertionError(f"unexpected exchange access: {name}")


class OrderSentExitTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        trader.init_live_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_confirmed_fill_boundary_both_sides_and_restart(self):
        for side, pre_high, pre_low, fill_high, fill_low, later_high, later_low in (
            ("LONG", 121, 89, 121, 89, 101, 89),
            ("SHORT", 111, 79, 111, 79, 111, 99),
        ):
            with self.subTest(side=side), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "trade.db"
                conn = sqlite3.connect(path)
                trader.init_live_tables(conn)
                conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)")
                conn.executemany("INSERT INTO candles VALUES('MEXC_FUT:BTCUSDT','1m',?,?,?)", (
                    (120000, pre_high, pre_low), (180000, fill_high, fill_low),
                ))
                conn.execute("""INSERT INTO live_trades(created_at,symbol,pattern,side,entry_time,entry_price,stop_price,tp1_price,tp2_price,notional_usdt,exchange_order_id,status,raw_order_response)
                    VALUES('now','MEXC_FUT:BTCUSDT','triangle',?,60000,100,?,?,?,1,'oid','ORDER_SENT',?)""",
                    (side, 90 if side == 'LONG' else 110, 105 if side == 'LONG' else 95, 120 if side == 'LONG' else 80, json.dumps({'order_request': {'symbol': 'BTC_USDT', 'side': 1 if side == 'LONG' else 3, 'vol': '2'}})))
                conn.commit()
                self.assertEqual(trader.reconcile_exchange_fills(conn, Adapter(evidence(side)), enabled=True), 1)
                self.assertEqual(trader.reconcile_exchange_fills(conn, Adapter(evidence(side)), enabled=True), 0)
                conn.close()
                conn = sqlite3.connect(path)
                try:
                    before = conn.total_changes
                    trader.update_open_trade_exits(conn, NoExchangeCalls(), live_enabled=False)
                    trader.update_open_trade_exits(conn, NoExchangeCalls(), live_enabled=False)
                    self.assertEqual(conn.total_changes, before)
                    self.assertEqual(conn.execute("SELECT status,exit_time,realized_r FROM live_trades").fetchone(), ("FILLED", None, None))
                    conn.execute("INSERT INTO candles VALUES('MEXC_FUT:BTCUSDT','1m',240000,?,?)", (later_high, later_low))
                    conn.commit()
                    trader.update_open_trade_exits(conn, NoExchangeCalls(), live_enabled=False)
                    self.assertEqual(conn.execute("SELECT status,exit_time,realized_r FROM live_trades").fetchone(), ("POSITION_CLOSED", 240000, -1.0))
                    before = conn.total_changes
                    trader.update_open_trade_exits(conn, NoExchangeCalls(), live_enabled=False)
                    self.assertEqual(conn.total_changes, before)
                finally:
                    conn.close()

    def test_missing_malformed_and_equal_fill_timestamp_fail_closed(self):
        self.conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)")
        self.conn.execute("INSERT INTO candles VALUES('MEXC_FUT:BTCUSDT','1m',180000,121,89)")
        for timestamp in (None, 'garbage', 0, 180000):
            self.conn.execute("""INSERT INTO live_trades(created_at,symbol,pattern,side,entry_time,entry_price,stop_price,tp1_price,tp2_price,notional_usdt,status,confirmed_fill_ts)
                VALUES('now','MEXC_FUT:BTCUSDT','triangle','LONG',60000,100,90,105,120,1,'FILLED',?)""", (timestamp,))
        self.conn.commit()
        before = self.conn.total_changes
        trader.update_open_trade_exits(self.conn, NoExchangeCalls(), live_enabled=False)
        self.assertEqual(self.conn.total_changes, before)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM live_trades WHERE realized_r IS NOT NULL").fetchone()[0], 0)

    def test_timestamp_only_confirmation_bypass_absent(self):
        self.assertFalse(hasattr(trader, 'confirm_exchange_fill'))

    def test_legacy_schema_migration_preserves_unverified_fill(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'legacy.db'
            conn = sqlite3.connect(path)
            conn.execute("""CREATE TABLE live_trades(id INTEGER PRIMARY KEY, created_at TEXT, symbol TEXT, pattern TEXT, side TEXT,
                entry_time INTEGER, entry_price REAL, stop_price REAL, tp1_price REAL, tp2_price REAL, notional_usdt REAL,
                exchange_order_id TEXT, status TEXT, exit_time INTEGER, exit_price REAL, realized_r REAL, fees REAL,
                raw_order_response TEXT, notes TEXT)""")
            conn.execute("""INSERT INTO live_trades(created_at,symbol,pattern,side,entry_time,entry_price,stop_price,tp1_price,tp2_price,notional_usdt,status)
                VALUES('now','MEXC_FUT:BTCUSDT','triangle','LONG',60000,100,90,105,120,1,'FILLED')""")
            conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)")
            conn.execute("INSERT INTO candles VALUES('MEXC_FUT:BTCUSDT','1m',240000,121,89)")
            conn.commit()
            trader.init_live_tables(conn)
            before = conn.total_changes
            trader.update_open_trade_exits(conn, NoExchangeCalls(), live_enabled=False)
            self.assertEqual(conn.total_changes, before)
            self.assertEqual(conn.execute('SELECT status,confirmed_fill_ts,realized_r FROM live_trades').fetchone(), ('FILLED', None, None))
            conn.close()

    def test_unfilled_stop_crossing_is_write_free_for_both_sides_and_restart(self):
        for side, high, low in (("LONG", 101, 89), ("SHORT", 111, 99)):
            with self.subTest(side=side):
                self.conn.execute("DELETE FROM live_trades")
                self.conn.execute("CREATE TABLE IF NOT EXISTS candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)")
                self.conn.execute("DELETE FROM candles")
                self.conn.execute("INSERT INTO candles VALUES('MEXC_FUT:BTCUSDT','1m',2,?,?)", (high, low))
                self.conn.execute("""INSERT INTO live_trades(created_at,symbol,pattern,side,entry_time,entry_price,stop_price,tp1_price,tp2_price,notional_usdt,exchange_order_id,status,raw_order_response)
                    VALUES('now','MEXC_FUT:BTCUSDT','triangle',?,1,100,?,?,?,1,'123','ORDER_SENT','{}')""",
                    (side, 90 if side == "LONG" else 110, 105 if side == "LONG" else 95, 120 if side == "LONG" else 80))
                self.conn.commit()
                before = self.conn.total_changes
                for _ in range(2):
                    trader.update_open_trade_exits(self.conn, NoExchangeCalls(), live_enabled=True)
                self.assertEqual(self.conn.total_changes, before)
                self.assertEqual(self.conn.execute("SELECT status,exit_time,realized_r FROM live_trades").fetchone(), ("ORDER_SENT", None, None))

    def test_only_persisted_fill_state_is_exit_eligible(self):
        self.conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)")
        self.conn.execute("INSERT INTO candles VALUES('MEXC_FUT:BTCUSDT','1m',2,101,89)")
        for status in ("ORDER_SENT", "PARTIAL_FILL", "REJECTED", "CANCELLED", "EXPIRED", "UNKNOWN", "API_ERROR", "OPEN", "EXIT_PENDING"):
            self.conn.execute("""INSERT INTO live_trades(created_at,symbol,pattern,side,entry_time,entry_price,stop_price,tp1_price,tp2_price,notional_usdt,status)
                VALUES('now','MEXC_FUT:BTCUSDT','triangle','LONG',1,100,90,105,120,1,?)""", (status,))
        self.conn.commit()
        before = self.conn.total_changes
        trader.update_open_trade_exits(self.conn, NoExchangeCalls(), live_enabled=False)
        self.assertEqual(self.conn.total_changes, before)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM live_trades WHERE realized_r IS NOT NULL").fetchone()[0], 0)
        self.conn.execute("UPDATE live_trades SET status = 'FILLED' WHERE status = 'ORDER_SENT'")
        self.conn.commit()
        trader.update_open_trade_exits(self.conn, NoExchangeCalls(), live_enabled=False)
        self.assertEqual(self.conn.execute("SELECT status, realized_r FROM live_trades WHERE id = 1").fetchone(), ("FILLED", None))
        before = self.conn.total_changes
        trader.update_open_trade_exits(self.conn, NoExchangeCalls(), live_enabled=False)
        self.assertEqual(self.conn.total_changes, before)

    def test_order_sent_remains_unfilled_after_database_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "live.db"
            conn = sqlite3.connect(path)
            trader.init_live_tables(conn)
            conn.execute("CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)")
            conn.execute("INSERT INTO candles VALUES('MEXC_FUT:BTCUSDT','1m',2,101,89)")
            conn.execute("""INSERT INTO live_trades(created_at,symbol,pattern,side,entry_time,entry_price,stop_price,tp1_price,tp2_price,notional_usdt,exchange_order_id,status)
                VALUES('now','MEXC_FUT:BTCUSDT','triangle','LONG',1,100,90,105,120,1,'123','ORDER_SENT')""")
            conn.commit()
            conn.close()
            conn = sqlite3.connect(path)
            try:
                trader.update_open_trade_exits(conn, NoExchangeCalls(), live_enabled=True)
                self.assertEqual(conn.execute("SELECT status, exit_time, realized_r FROM live_trades").fetchone(), ("ORDER_SENT", None, None))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
