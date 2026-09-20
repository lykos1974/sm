import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.path.insert(0, str(path.parent))
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


COLLECTOR = load("collector_for_daily", ROOT / "market_collector" / "microstructure_collector.py")
RUNNER = load("microstructure_daily", ROOT / "market_collector" / "microstructure_daily_runner.py")


class DailyRunnerTests(unittest.TestCase):
    def test_utc_boundary_math(self):
        now = datetime(2026, 9, 20, 23, 59, 50, tzinfo=timezone.utc)
        self.assertEqual(RUNNER.utc_day(now), "2026-09-20")
        self.assertEqual(RUNNER.seconds_until_next_utc_day(now), 10)

    def test_unclean_open_session_becomes_aborted_not_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "day.db"
            store = COLLECTOR.CompactStore(path)
            session = store.start_session("test")
            store.conn.close()
            self.assertEqual(RUNNER.recover_aborted_sessions(path), 1)
            conn = sqlite3.connect(path)
            try:
                row = conn.execute(
                    "SELECT status,end_reason,ended_wall_ns FROM sessions WHERE session_key=?",
                    (session,),
                ).fetchone()
                self.assertEqual(row[0], "ABORTED")
                self.assertEqual(row[1], "unclean_process_exit_detected_on_restart")
                self.assertIsNotNone(row[2])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
