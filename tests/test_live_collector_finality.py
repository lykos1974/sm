"""Offline regressions for the standalone collector; temporary SQLite only."""
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
storage = load('standalone_storage', ROOT / 'market_collector/storage.py')
with patch.dict(sys.modules, {'storage': storage}):
    collector = load('standalone_collector', ROOT / 'market_collector/collector.py')

def bar(open_ms, close):
    return [open_ms, 100, max(100, close), min(100, close), close, 1, open_ms + 59999]

class FinalityTests(unittest.TestCase):
    def exercise(self, callback):
        for cls in (collector.BinanceCollector, collector.MexcFuturesCollector):
            with self.subTest(exchange=cls.exchange_name), tempfile.TemporaryDirectory() as tmp:
                store = storage.Storage(str(Path(tmp) / 'market.db'))
                instance = cls('https://unused.invalid', 'Min1' if cls is collector.MexcFuturesCollector else '1m', 10, ['BTCUSDT'], store, lambda _: None)
                with patch.object(collector, 'urlopen', side_effect=AssertionError('Network forbidden')):
                    callback(instance, store)

    def test_bootstrap_rejects_open_then_poll_stores_final(self):
        def case(c, s):
            c.fetch_klines = lambda *a, **kw: [bar(0, 100), bar(60000, 101)]
            with patch.object(collector.time, 'time', return_value=90):
                c.bootstrap_symbol('BTCUSDT', 2)
            self.assertEqual(s.get_last_open_time('BTCUSDT', '1m'), 0)
            c.fetch_klines = lambda *a, **kw: [bar(0, 100), bar(60000, 109), bar(120000, 110)]
            with patch.object(collector.time, 'time', return_value=125):
                c.poll_symbol_once('BTCUSDT')
            self.assertEqual(s.get_last_open_time('BTCUSDT', '1m'), 60000)
            self.assertEqual(s.get_latest_close('BTCUSDT', '1m'), 109)
            self.assertEqual(s.count_candles('BTCUSDT', '1m'), 2)
        self.exercise(case)

    def test_overlap_repairs_latest_legacy_row_and_repeated_poll_is_unique(self):
        def case(c, s):
            s.upsert_candle('BTCUSDT', '1m', 60000, 119999, 100, 101, 99, 101, 1)
            requests = []
            def fetch(*a, **kw):
                requests.append(kw)
                return [bar(60000, 109), bar(120000, 110)]
            c.fetch_klines = fetch
            with patch.object(collector.time, 'time', return_value=125):
                c.poll_symbol_once('BTCUSDT')
                c.poll_symbol_once('BTCUSDT')
            expected_start = 60 if isinstance(c, collector.MexcFuturesCollector) else 60000
            self.assertEqual(requests[0]['start_time'], expected_start)
            self.assertEqual(s.get_latest_close('BTCUSDT', '1m'), 109)
            self.assertEqual(s.count_candles('BTCUSDT', '1m'), 1)
        self.exercise(case)

    def test_grace_boundary_and_no_progress_preserve_state(self):
        def case(c, s):
            c.fetch_klines = lambda *a, **kw: [bar(60000, 109)]
            with patch.object(collector.time, 'time', return_value=124.998):
                c.poll_symbol_once('BTCUSDT')
            self.assertEqual(s.count_candles('BTCUSDT', '1m'), 0)
            with patch.object(collector.time, 'time', return_value=125):
                c.poll_symbol_once('BTCUSDT')
            c.fetch_klines = lambda *a, **kw: []
            with patch.object(collector.time, 'time', return_value=130):
                c.poll_symbol_once('BTCUSDT')
            with s._connect() as db:
                row = db.execute('SELECT last_open_time,last_close_time FROM collector_state').fetchone()
            self.assertEqual(tuple(row), (60000, 119999))
        self.exercise(case)

    def test_slow_response_cannot_age_provisional_data_into_final(self):
        def case(c, s):
            clock = [90]
            def fetch(*a, **kw):
                clock[0] = 130
                return [bar(60000, 101)]
            c.fetch_klines = fetch
            with patch.object(collector.time, 'time', side_effect=lambda: clock[0]):
                c.poll_symbol_once('BTCUSDT')
            self.assertEqual(s.count_candles('BTCUSDT', '1m'), 0)
        self.exercise(case)

    def test_full_page_overlap_still_advances(self):
        def case(c, s):
            c.max_kline_limit = 2
            s.upsert_candle('BTCUSDT', '1m', 0, 59999, 100, 100, 100, 100, 1)
            calls = []
            def fetch(*a, **kw):
                calls.append(kw['start_time'])
                return [bar(0, 100), bar(60000, 101)] if len(calls) == 1 else [bar(120000, 102)]
            c.fetch_klines = fetch
            with patch.object(collector.time, 'time', return_value=250):
                c.poll_symbol_once('BTCUSDT')
            scale = 1000 if isinstance(c, collector.MexcFuturesCollector) else 1
            self.assertEqual(calls, [0, 120000 // scale])
            self.assertEqual(s.count_candles('BTCUSDT', '1m'), 3)
        self.exercise(case)

if __name__ == '__main__':
    unittest.main()
