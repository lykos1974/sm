import importlib.util
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

OBSERVER = load("strategy_setup_observer_tested", ROOT / "pnf_mvp" / "strategy_setup_observer.py")

def candidate(score=80):
    return {"strategy": "pullback_retest", "side": "LONG", "status": "CANDIDATE",
            "quality_score": score, "ideal_entry": 100.0, "invalidation": 98.0,
            "tp1": 104.0, "tp2": 106.0}

def live_clock():
    close_ms = time.time_ns() // 1_000_000
    return close_ms, close_ms * 1_000_000 + 1_000

class StrategySetupObserverTests(unittest.TestCase):
    def test_lifecycle_is_deduplicated(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.db"
            observer = OBSERVER.StrategySetupObserver(path, ["BTCUSDT"], ["CANDIDATE"], 65)
            structure = {"current_column_index": 7}
            close_ms, wall_ns = live_clock()
            self.assertEqual(observer.observe("BTCUSDT", close_ms, [candidate()], structure, wall_ns, 500),
                             {"opened": 1, "continued": 0, "withdrawn": 0, "expired": 0})
            self.assertEqual(observer.observe("BTCUSDT", close_ms + 60_000, [candidate()], structure, wall_ns + 1, 600),
                             {"opened": 0, "continued": 1, "withdrawn": 0, "expired": 0})
            self.assertEqual(observer.observe("BTCUSDT", close_ms + 120_000, [], structure, wall_ns + 2, 700),
                             {"opened": 0, "continued": 0, "withdrawn": 1, "expired": 0})
            observer.close()
            conn = sqlite3.connect(path)
            row = conn.execute("SELECT available_wall_ns,expires_wall_ns,lifecycle,last_seen_close_ms "
                               "FROM setup_occurrences").fetchone()
            self.assertEqual(row, (wall_ns, wall_ns + 2, "WITHDRAWN", close_ms + 60_000))
            conn.close()

    def test_normal_stop_interrupts_open_and_low_score_is_filtered(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.db"
            observer = OBSERVER.StrategySetupObserver(path, ["BTCUSDT"], ["CANDIDATE"], 65)
            structure = {"current_column_index": 7}
            close_ms, wall_ns = live_clock()
            self.assertEqual(observer.observe("BTCUSDT", close_ms, [candidate(60)], structure, wall_ns, 500)["opened"], 0)
            observer.observe("BTCUSDT", close_ms, [candidate()], structure, wall_ns, 600)
            observer.close()
            conn = sqlite3.connect(path)
            lifecycle, reason, expires = conn.execute(
                "SELECT lifecycle,close_reason,expires_wall_ns FROM setup_occurrences").fetchone()
            self.assertEqual((lifecycle, reason), ("INTERRUPTED", "OBSERVER_STOP"))
            self.assertIsNotNone(expires)
            conn.close()

    def test_next_start_interrupts_occurrence_left_open_by_crash(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.db"
            crashed = OBSERVER.StrategySetupObserver(path, ["BTCUSDT"], ["CANDIDATE"], 65)
            close_ms, wall_ns = live_clock()
            crashed.observe(
                "BTCUSDT", close_ms, [candidate()], {"current_column_index": 7}, wall_ns, 500
            )
            restarted = OBSERVER.StrategySetupObserver(path, ["BTCUSDT"], ["CANDIDATE"], 65)
            conn = sqlite3.connect(path)
            lifecycle, reason = conn.execute(
                "SELECT lifecycle,close_reason FROM setup_occurrences"
            ).fetchone()
            self.assertEqual((lifecycle, reason), ("INTERRUPTED", "OBSERVER_RESTART"))
            conn.close()
            restarted.close()
            crashed.close()

    def test_three_candle_expiry_is_exact_and_same_key_never_reopens(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.db"
            observer = OBSERVER.StrategySetupObserver(path, ["BTCUSDT"], ["CANDIDATE"], 65)
            structure = {"current_column_index": 7}
            close_ms, wall_ns = live_clock()
            scheduled = (close_ms + 180_000) * 1_000_000
            observer.observe("BTCUSDT", close_ms, [candidate()], structure, wall_ns, 1)
            result = observer.observe(
                "BTCUSDT", close_ms + 180_000, [candidate()], structure, scheduled, 2
            )
            self.assertEqual(result, {"opened": 0, "continued": 0,
                                      "withdrawn": 0, "expired": 1})
            result = observer.observe(
                "BTCUSDT", close_ms + 240_000, [candidate()], structure,
                scheduled + 60_000_000_000, 3,
            )
            self.assertEqual(result["opened"], 0)
            observer.close()
            conn = sqlite3.connect(path)
            rows = conn.execute(
                "SELECT lifecycle,expires_wall_ns,close_reason FROM setup_occurrences"
            ).fetchall()
            self.assertEqual(rows, [("EXPIRED", scheduled, "THREE_CANDLE_EXPIRY")])
            conn.close()

if __name__ == "__main__":
    unittest.main()
