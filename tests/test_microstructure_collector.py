import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "microstructure", ROOT / "market_collector" / "microstructure_collector.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def raw(stream, data):
    return json.dumps({"stream": stream, "data": data}, separators=(",", ":"))


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "micro.db"
        self.store = MODULE.Store(self.path)
        self.session = self.store.start_session("test")

    def tearDown(self):
        try:
            self.store.end_session(self.session, "complete")
            self.store.close()
        finally:
            self.tmp.cleanup()

    def message(self, stream, data):
        with patch.object(MODULE.time, "time_ns", return_value=11), patch.object(
            MODULE.time, "monotonic_ns", return_value=22
        ):
            return MODULE.parse_combined(raw(stream, data))

    def quote(self, update=7):
        return {"u": update, "s": "BTCUSDT", "b": "100.10", "B": "1.2",
                "a": "100.20", "A": "2.3"}

    def trade(self, agg_id):
        return {"E": 10, "s": "BTCUSDT", "a": agg_id, "p": "99.9",
                "q": "0.4", "f": agg_id, "l": agg_id, "T": 9, "m": True}

    def test_quote_exact_values_and_receive_times(self):
        self.store.queue(self.session, self.message("btcusdt@bookTicker", self.quote()))
        self.store.flush()
        row = self.store.conn.execute(
            "SELECT update_id,receive_wall_ns,receive_monotonic_ns,bid_price,ask_price FROM book_ticker"
        ).fetchone()
        self.assertEqual(row, (7, 11, 22, "100.10", "100.20"))

    def test_trade_deduplicates_and_records_duplicate_anomaly(self):
        message = self.message("btcusdt@aggTrade", self.trade(50))
        self.store.queue(self.session, message)
        self.store.flush()
        self.store.queue(self.session, message)
        self.store.flush()
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM agg_trades").fetchone()[0], 1)
        self.assertEqual(
            self.store.conn.execute("SELECT anomaly_type FROM stream_anomalies").fetchone()[0],
            "DUPLICATE_OR_OUT_OF_ORDER_AGG_TRADE",
        )

    def test_trade_gap_but_no_false_book_update_gap(self):
        for agg_id in (100, 103):
            self.store.queue(self.session, self.message("btcusdt@aggTrade", self.trade(agg_id)))
        for update in (10, 1000):
            self.store.queue(self.session, self.message("btcusdt@bookTicker", self.quote(update)))
        self.store.flush()
        rows = self.store.conn.execute(
            "SELECT anomaly_type,previous_id,current_id,details FROM stream_anomalies"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][:3], ("POSSIBLE_AGG_TRADE_ID_GAP", 100, 103))
        self.assertEqual(json.loads(rows[0][3]), {"missing_first": 101, "missing_last": 102})

    def test_failed_flush_is_retryable_and_atomic(self):
        self.store.queue(self.session, self.message("btcusdt@bookTicker", self.quote()))
        self.store.conn.execute(
            "CREATE TRIGGER fail BEFORE INSERT ON book_ticker BEGIN SELECT RAISE(ABORT,'fault'); END"
        )
        self.store.conn.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.flush()
        self.assertEqual(len(self.store.pending), 1)
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM book_ticker").fetchone()[0], 0)
        self.store.conn.execute("DROP TRIGGER fail")
        self.store.conn.commit()
        self.store.flush()
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM book_ticker").fetchone()[0], 1)

    def test_restart_continues_gap_detection(self):
        self.store.queue(self.session, self.message("btcusdt@aggTrade", self.trade(10)))
        self.store.flush()
        self.store.end_session(self.session, "restart")
        self.store.close()
        self.store = MODULE.Store(self.path)
        self.session = self.store.start_session("second")
        self.store.queue(self.session, self.message("btcusdt@aggTrade", self.trade(12)))
        self.store.flush()
        self.assertEqual(
            self.store.conn.execute("SELECT previous_id,current_id FROM stream_anomalies").fetchone(),
            (10, 12),
        )

    def test_invalid_quote_and_symbol_do_not_write(self):
        bad = self.quote()
        bad["b"] = "102"
        with self.assertRaises(ValueError):
            self.store.queue(self.session, self.message("btcusdt@bookTicker", bad))
        with self.assertRaises(ValueError):
            MODULE.parse_combined(raw("ethusdt@bookTicker", {**bad, "s": "ETHUSDT"}))
        self.assertEqual(self.store.counts(), (0, 0, 0))

    def test_session_closure_and_public_url(self):
        self.store.end_session(self.session, "network test")
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status,end_reason FROM sessions WHERE session_id=?", (self.session,)
            ).fetchone(),
            ("CLOSED", "network test"),
        )
        url = MODULE.stream_url()
        self.assertEqual(
            url,
            "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade",
        )
        self.assertNotIn("apiKey", url)

    def test_stale_trade_records_socket_wait_diagnostic(self):
        data = self.trade(60)
        data["E"] = 1
        with patch.object(MODULE.time, "time_ns", return_value=1_000_000_000):
            message = MODULE.parse_combined(
                raw("btcusdt@aggTrade", data), socket_wait_ms=0.25
            )
        self.store.queue(self.session, message)
        self.store.flush()
        row = self.store.conn.execute(
            "SELECT diagnostic_type,socket_wait_ms FROM runtime_diagnostics"
        ).fetchone()
        self.assertEqual(row, ("STALE_AGG_TRADE", 0.25))


if __name__ == "__main__":
    unittest.main()
