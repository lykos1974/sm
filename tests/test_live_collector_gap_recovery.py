"""Offline coverage of bounded MEXC candle recovery."""
import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


storage = load('gap_storage', ROOT / 'market_collector/storage.py')
with patch.dict(sys.modules, {'storage': storage}):
    collector = load('gap_collector', ROOT / 'market_collector/collector.py')


def bar(open_ms, close=101):
    return [open_ms, 100, max(100, close), min(100, close), close, 2, open_ms + 59999]


class GapRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = storage.Storage(str(Path(self.tmp.name) / 'research.db'))
        self.logs = []
        self.c = collector.MexcFuturesCollector(
            'https://unused.invalid', 'Min1', 10, ['BTCUSDT'], self.store, self.logs.append,
            symbol_prefix='MEXC_FUT:',
        )
        self.now = 1_800_000_310  # 1m-aligned, five minutes and ten seconds.
        self.end = (int(self.now * 1000 - 5000) // 60000) * 60000 - 60000

    def rows(self):
        with self.store._connect() as conn:
            return [tuple(r) for r in conn.execute(
                'SELECT open_time,close_time,open,high,low,close,volume FROM candles '
                'WHERE symbol=? ORDER BY open_time', ('MEXC_FUT:BTCUSDT',))]

    def test_repairs_interior_gap_once_and_preserves_existing_row(self):
        first = self.end - 2 * 60000
        self.store.upsert_candle('MEXC_FUT:BTCUSDT', '1m', first, first+59999, 100, 100, 100, 100, 2)
        self.store.upsert_candle('MEXC_FUT:BTCUSDT', '1m', self.end, self.end+59999, 100, 102, 100, 102, 2)
        existing = self.rows()[-1]
        calls = []
        def fetch(*args, **kwargs):
            calls.append(kwargs)
            return [bar(first, 100), bar(first+60000), bar(self.end, 999)]
        self.c.fetch_klines = fetch
        with patch.object(collector.time, 'time', return_value=self.now), patch.object(collector, 'urlopen', side_effect=AssertionError('network')):
            self.c.recover_recent_symbol('BTCUSDT')
            self.c.recover_recent_symbol('BTCUSDT')
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.rows()[-1], existing)
        self.assertIn(first+60000, [row[0] for row in self.rows()])

    def test_empty_response_never_claims_complete_and_retries_after_restart(self):
        self.c.fetch_klines = lambda *a, **kw: []
        with patch.object(collector.time, 'time', return_value=self.now):
            self.c.recover_recent_symbol('BTCUSDT')
        self.assertFalse(self.rows())
        self.assertTrue(any('UNAVAILABLE' in line for line in self.logs))
        second = collector.MexcFuturesCollector('https://unused.invalid', 'Min1', 10, ['BTCUSDT'], self.store,
                                                self.logs.append, symbol_prefix='MEXC_FUT:')
        second.fetch_klines = lambda *a, **kw: [bar(self.end)]
        with patch.object(collector.time, 'time', return_value=self.now):
            second.recover_recent_symbol('BTCUSDT')
        self.assertEqual(len(self.rows()), 1)

    def test_invalid_or_failed_batch_is_atomic(self):
        first = self.end - 60000
        self.c.fetch_klines = lambda *a, **kw: [bar(first), bar(self.end, -1)]
        with patch.object(collector.time, 'time', return_value=self.now):
            with self.assertRaises(ValueError):
                self.c.recover_recent_symbol('BTCUSDT')
        self.assertFalse(self.rows())
        self.c.fetch_klines = lambda *a, **kw: [bar(first), bar(self.end)]
        with self.store._connect() as conn:
            conn.execute('CREATE TRIGGER fail_second BEFORE INSERT ON candles '
                         f'WHEN NEW.open_time={self.end} BEGIN SELECT RAISE(ABORT, "injected"); END')
        with patch.object(collector.time, 'time', return_value=self.now):
            with self.assertRaises(sqlite3.Error):
                self.c.recover_recent_symbol('BTCUSDT')
        self.assertFalse(self.rows())

    def test_rejects_truncated_arrays_and_duplicate_timestamps(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self): return b'{"success":true,"data":{"time":[1800000000,1800000000],"open":[1],"high":[1],"low":[1],"close":[1],"vol":[1]}}'
        with patch.object(collector, 'urlopen', return_value=Response()):
            with self.assertRaises(ValueError):
                self.c.fetch_klines('BTCUSDT', 2, start_time=1800000000, end_time=1800000060)

    def test_poll_recovers_interior_gap_automatically_and_rate_limits(self):
        first = self.end - 2 * 60000
        self.store.upsert_candle('MEXC_FUT:BTCUSDT', '1m', first, first+59999, 100, 100, 100, 100, 2)
        self.store.upsert_candle('MEXC_FUT:BTCUSDT', '1m', self.end, self.end+59999, 100, 101, 100, 101, 2)
        calls = []
        def fetch(*args, **kw):
            calls.append(kw)
            if kw['start_time'] == first // 1000:
                return [bar(first, 100), bar(self.end)]
            return [bar(first, 100), bar(first+60000), bar(self.end)]
        self.c.fetch_klines = fetch
        with patch.object(collector.time, 'time', return_value=self.now):
            self.c.poll_symbol_once('BTCUSDT')
            self.c.poll_symbol_once('BTCUSDT')
        self.assertEqual(len([call for call in calls if call['limit'] == 1440]), 1)
        self.assertIn(first+60000, [row[0] for row in self.rows()])
        self.assertTrue(any('PARTIAL_API_COVERAGE' in line for line in self.logs))

    def test_complete_coverage_requires_every_minute(self):
        first = self.end - 2 * 60000
        self.c.recovery_window_minutes = 3
        self.c.fetch_klines = lambda *a, **kw: [bar(first), bar(first+60000), bar(self.end)]
        with patch.object(collector.time, 'time', return_value=self.now):
            self.c.recover_recent_symbol('BTCUSDT')
        self.assertTrue(any('recovery COMPLETE returned=3' in line and
                            'missing_local=3' in line and 'recovered=3' in line and
                            'not_returned=0 still_missing=0' in line for line in self.logs))

    def test_competing_writer_is_not_reported_as_remaining_gap(self):
        self.c.fetch_klines = lambda *a, **kw: [bar(self.end)]
        insert = self.store.insert_missing_candles
        def concurrent_insert(rows):
            self.store.upsert_candle('MEXC_FUT:BTCUSDT', '1m', self.end,
                                     self.end+59999, 100, 101, 100, 101, 2)
            return insert(rows)
        with patch.object(self.store, 'insert_missing_candles', side_effect=concurrent_insert), \
             patch.object(collector.time, 'time', return_value=self.now):
            self.c.recover_recent_symbol('BTCUSDT')
        self.assertEqual(len(self.rows()), 1)
        self.assertTrue(any('still_missing=0' in line for line in self.logs))

    def test_duplicate_keys_and_invalid_numbers_rejected_before_storage(self):
        class Response:
            def __init__(self, body): self.body = body
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self): return self.body
        bodies = (
            b'{"success":true,"success":true,"code":0,"data":{}}',
            b'{"success":true,"code":0,"data":{"time":[],"time":[]}}',
            b'{"success":true,"code":0,"data":{"time":[1800000000],"open":[NaN],"high":[1],"low":[1],"close":[1],"vol":[1]}}',
        )
        for body in bodies:
            with self.subTest(body=body[:40]), patch.object(collector, 'urlopen', return_value=Response(body)):
                with self.assertRaises(ValueError):
                    self.c.fetch_klines('BTCUSDT', 1, start_time=1800000000, end_time=1800000000)
        self.assertFalse(self.rows())


if __name__ == '__main__':
    unittest.main()
