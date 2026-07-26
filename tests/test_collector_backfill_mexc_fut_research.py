from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from pnf_mvp import collector_backfill_mexc_fut_research as collector
from pnf_mvp.storage import Storage


START_MS = 1_700_000_040_000  # minute aligned
END_MS = START_MS + 5 * collector.INTERVAL_MS
FIRST_CLOSE_S = START_MS // 1000 + collector.INTERVAL_SECONDS


def candle(ts, open_=10.0, high=12.0, low=9.0, close=11.0):
    return (ts, open_, high, low, close)


class MexcBackfillTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "research.sqlite3"
        self.storage = Storage(str(self.db))

    def tearDown(self):
        self.storage.conn.close()
        self.tmp.cleanup()

    @staticmethod
    def page_with_first(first):
        def fetch(_base, _symbol, start, end, _interval):
            return ([candle(first)] if start <= first <= end else []), {}
        return fetch

    def test_symbol_conversion(self):
        self.assertEqual(collector.normalize_symbol("mexc_fut:btcusdt"), "MEXC_FUT:BTCUSDT")
        self.assertEqual(collector.mexc_contract_symbol("MEXC_FUT:BTCUSDT"), "BTC_USDT")

    def test_seconds_to_storage_milliseconds(self):
        open_ms, close_ms = collector.storage_times(FIRST_CLOSE_S)
        self.assertEqual(open_ms, START_MS)
        self.assertEqual(close_ms, START_MS + collector.INTERVAL_MS)
        with self.assertRaises(ValueError):
            collector.storage_times((START_MS + collector.INTERVAL_MS))

    def test_page_continuation_when_contract_not_on_first_page(self):
        calls = []
        later = FIRST_CLOSE_S + collector.INTERVAL_SECONDS * collector.MEXC_KLINE_PAGE_LIMIT

        def page(_base, _symbol, start, end, _interval):
            calls.append((start, end))
            return ([candle(later)] if start <= later <= end else []), {}

        found = collector.find_first_available("MEXC_FUT:BTCUSDT", FIRST_CLOSE_S, later, page)
        self.assertEqual(found, later)
        self.assertEqual(len(calls), 2)

    def test_gap_only_resume(self):
        self.storage.insert_candle("MEXC_FUT:BTCUSDT", "1m", START_MS, START_MS + 60_000, 10, 12, 9, 11, 0)
        self.storage.insert_candle("MEXC_FUT:BTCUSDT", "1m", START_MS + 120_000, START_MS + 180_000, 10, 12, 9, 11, 0)
        requested = []

        def fetch(_base, _symbol, start, end, _interval, _seconds):
            requested.append((start, end))
            return [candle(ts) for ts in range(start, end + 1, 60)]

        result = collector.backfill_symbol(
            self.storage, "MEXC_FUT:BTCUSDT", START_MS, END_MS, 0,
            fetcher=fetch, page_fetcher=self.page_with_first(FIRST_CLOSE_S),
        )
        self.assertEqual(requested, [(FIRST_CLOSE_S + 60, FIRST_CLOSE_S + 60), (FIRST_CLOSE_S + 180, FIRST_CLOSE_S + 240)])
        self.assertTrue(result.continuous)
        self.assertEqual(result.count, 5)

    def test_idempotent_rerun(self):
        calls = []

        def fetch(_base, _symbol, start, end, _interval, _seconds):
            calls.append((start, end))
            return [candle(ts) for ts in range(start, end + 1, 60)]

        kwargs = dict(fetcher=fetch, page_fetcher=self.page_with_first(FIRST_CLOSE_S))
        collector.backfill_symbol(self.storage, "MEXC_FUT:BTCUSDT", START_MS, END_MS, 0, **kwargs)
        collector.backfill_symbol(self.storage, "MEXC_FUT:BTCUSDT", START_MS, END_MS, 0, **kwargs)
        self.assertEqual(calls, [(FIRST_CLOSE_S, END_MS // 1000)])
        self.assertEqual(self.storage.conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0], 5)

    def test_contract_listing_later_than_requested_start(self):
        listed = FIRST_CLOSE_S + 120

        def fetch(_base, _symbol, start, end, _interval, _seconds):
            return [candle(ts) for ts in range(start, end + 1, 60)]

        result = collector.backfill_symbol(
            self.storage, "MEXC_FUT:ENAUSDT", START_MS, END_MS, 0,
            fetcher=fetch, page_fetcher=self.page_with_first(listed),
        )
        self.assertEqual(result.first_available_ms, listed * 1000)
        self.assertEqual(result.missing, 0)
        self.assertEqual(result.count, 3)

    def test_continuity_failure(self):
        self.storage.insert_candle("MEXC_FUT:BTCUSDT", "1m", START_MS, START_MS + 60_000, 10, 12, 9, 11, 0)
        result = collector.backfill_symbol(
            self.storage, "MEXC_FUT:BTCUSDT", START_MS, END_MS, 0, verify_only=True,
            page_fetcher=self.page_with_first(FIRST_CLOSE_S),
        )
        self.assertFalse(result.continuous)
        self.assertEqual(result.missing, 4)

    def test_all_nine_default_symbols(self):
        self.assertEqual(len(collector.DEFAULT_SYMBOLS), 9)
        self.assertEqual(
            collector.DEFAULT_SYMBOLS,
            ("MEXC_FUT:BTCUSDT", "MEXC_FUT:ETHUSDT", "MEXC_FUT:BNBUSDT", "MEXC_FUT:SOLUSDT",
             "MEXC_FUT:XRPUSDT", "MEXC_FUT:SUIUSDT", "MEXC_FUT:TAOUSDT", "MEXC_FUT:HYPEUSDT",
             "MEXC_FUT:ENAUSDT"),
        )

    def test_no_live_db_mutation(self):
        live = Path(self.tmp.name) / "mexc_live_candles.db"
        live.write_bytes(b"unchanged")
        with self.assertRaises(ValueError):
            collector.run(str(live), ["MEXC_FUT:BTCUSDT"], START_MS, END_MS, 0)
        self.assertEqual(live.read_bytes(), b"unchanged")


if __name__ == "__main__":
    unittest.main()
