import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


COLLECTOR = load("microstructure_for_check", ROOT / "market_collector" / "microstructure_collector.py")
CHECKER = load("microstructure_checker", ROOT / "market_collector" / "check_microstructure_db.py")


class CheckerTests(unittest.TestCase):
    def test_valid_closed_database_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "evidence.db"
            store = COLLECTOR.Store(path)
            session = store.start_session("test")
            trade = {"E": 1_000, "s": "BTCUSDT", "a": 7, "p": "100", "q": "1",
                     "f": 7, "l": 7, "T": 999, "m": False}
            quote = {"u": 8, "s": "BTCUSDT", "b": "99", "B": "2", "a": "101", "A": "3"}
            with patch.object(COLLECTOR.time, "time_ns", return_value=1_100_000_000):
                for stream, data in (("btcusdt@aggTrade", trade), ("btcusdt@bookTicker", quote)):
                    raw = json.dumps({"stream": stream, "data": data})
                    store.queue(session, COLLECTOR.parse_combined(raw))
            store.end_session(session, "normal_stop")
            store.close()
            report, failures = CHECKER.audit(path)
            self.assertEqual(failures, [])
            self.assertEqual(report["integrity"], "ok")
            self.assertEqual(report["counts"]["agg_trades"], 1)

    def test_restart_gap_is_not_mislabeled_as_live_loss(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "evidence.db"
            store = COLLECTOR.Store(path)
            first = store.start_session("first")
            self._queue_quote(store, first)
            self._queue_trade(store, first, 10)
            store.end_session(first, "normal_stop")
            second = store.start_session("second")
            self._queue_trade(store, second, 15)
            store.end_session(second, "normal_stop")
            store.close()
            report, failures = CHECKER.audit(path)
            self.assertEqual(failures, [])
            self.assertEqual(report["anomalies"]["session_start_gaps"], 1)
            self.assertEqual(report["anomalies"]["in_session"], 0)

    def test_gap_inside_session_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "evidence.db"
            store = COLLECTOR.Store(path)
            session = store.start_session("test")
            self._queue_quote(store, session)
            self._queue_trade(store, session, 20)
            self._queue_trade(store, session, 22)
            store.end_session(session, "normal_stop")
            store.close()
            report, failures = CHECKER.audit(path)
            self.assertEqual(report["anomalies"]["in_session"], 1)
            self.assertTrue(failures)

    @staticmethod
    def _queue_trade(store, session, agg_id):
        data = {"E": 1_000, "s": "BTCUSDT", "a": agg_id, "p": "100", "q": "1",
                "f": agg_id, "l": agg_id, "T": 999, "m": False}
        raw = json.dumps({"stream": "btcusdt@aggTrade", "data": data})
        with patch.object(COLLECTOR.time, "time_ns", return_value=1_100_000_000):
            store.queue(session, COLLECTOR.parse_combined(raw))
        store.flush()

    @staticmethod
    def _queue_quote(store, session):
        data = {"u": 1, "s": "BTCUSDT", "b": "99", "B": "1", "a": "101", "A": "1"}
        raw = json.dumps({"stream": "btcusdt@bookTicker", "data": data})
        with patch.object(COLLECTOR.time, "time_ns", return_value=1_100_000_000):
            store.queue(session, COLLECTOR.parse_combined(raw))
        store.flush()


if __name__ == "__main__":
    unittest.main()
