import importlib.util
import json
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

    def test_bounded_midnight_rollover_archives_verifies_and_retains_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = root / "live" / "BTCUSDT" / "2026-09-20.db"
            archive_root = root / "archive"
            store = COLLECTOR.CompactStore(database)
            session = store.start_session("test")
            boundary_ns = int(
                datetime(2026, 9, 21, tzinfo=timezone.utc).timestamp() * 1_000_000_000
            )
            quote = {"u": 1, "s": "BTCUSDT", "b": "99.10", "B": "1.20",
                     "a": "99.20", "A": "2.30"}
            for update_id, wall_ns in ((1, boundary_ns - 1), (2, boundary_ns + 200_000_000)):
                payload = {**quote, "u": update_id}
                raw = json.dumps(
                    {"stream": "btcusdt@bookTicker", "data": payload},
                    separators=(",", ":"),
                )
                store.queue(session, COLLECTOR.ReceivedMessage(
                    wall_ns, update_id, "btcusdt@bookTicker", payload, raw, 0.0,
                ))
            store.write_batch(store.detach_pending())
            store.end_session(session, "normal_stop")
            store.close()

            result = RUNNER.archive_and_verify(database, archive_root, 1.0)

            self.assertTrue(result["verified"])
            self.assertTrue(result["source_retained"])
            self.assertTrue(database.is_file())
            self.assertTrue((Path(result["archive"]) / "manifest.json").is_file())


if __name__ == "__main__":
    unittest.main()
