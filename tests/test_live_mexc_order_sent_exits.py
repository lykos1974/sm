import sqlite3
import tempfile
import unittest
from pathlib import Path

import live_mexc_forward_trader as trader


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
        self.assertEqual(self.conn.execute("SELECT status, realized_r FROM live_trades WHERE id = 1").fetchone(), ("POSITION_CLOSED", -1.0))
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
