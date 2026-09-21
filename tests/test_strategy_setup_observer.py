import importlib.util
import sqlite3
import sys
import tempfile
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

class StrategySetupObserverTests(unittest.TestCase):
    def test_lifecycle_is_deduplicated(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.db"
            observer = OBSERVER.StrategySetupObserver(path, ["BTCUSDT"], ["CANDIDATE"], 65)
            structure = {"current_column_index": 7}
            self.assertEqual(observer.observe("BTCUSDT", 1000, [candidate()], structure, 10_000, 500),
                             {"opened": 1, "continued": 0, "withdrawn": 0})
            self.assertEqual(observer.observe("BTCUSDT", 2000, [candidate()], structure, 20_000, 600),
                             {"opened": 0, "continued": 1, "withdrawn": 0})
            self.assertEqual(observer.observe("BTCUSDT", 3000, [], structure, 30_000, 700),
                             {"opened": 0, "continued": 0, "withdrawn": 1})
            observer.close()
            conn = sqlite3.connect(path)
            row = conn.execute("SELECT available_wall_ns,expires_wall_ns,lifecycle,last_seen_close_ms "
                               "FROM setup_occurrences").fetchone()
            self.assertEqual(row, (10_000, 30_000, "WITHDRAWN", 2000))
            conn.close()

    def test_normal_stop_interrupts_open_and_low_score_is_filtered(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.db"
            observer = OBSERVER.StrategySetupObserver(path, ["BTCUSDT"], ["CANDIDATE"], 65)
            structure = {"current_column_index": 7}
            self.assertEqual(observer.observe("BTCUSDT", 1000, [candidate(60)], structure, 10_000, 500)["opened"], 0)
            observer.observe("BTCUSDT", 2000, [candidate()], structure, 20_000, 600)
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
            crashed.observe(
                "BTCUSDT", 1000, [candidate()], {"current_column_index": 7}, 10_000, 500
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

if __name__ == "__main__":
    unittest.main()
