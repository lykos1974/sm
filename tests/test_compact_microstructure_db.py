import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
import uuid
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


COLLECTOR = load("collector_for_compaction", ROOT / "market_collector" / "microstructure_collector.py")
COMPACTOR = load("microstructure_compactor", ROOT / "market_collector" / "compact_microstructure_db.py")


class CompactionTests(unittest.TestCase):
    def test_compaction_preserves_structured_evidence_and_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, destination = Path(tmp) / "source.db", Path(tmp) / "compact.db"
            store = COLLECTOR.Store(source)
            session = store.start_session("test")
            quote = {"u": 1, "s": "BTCUSDT", "b": "99.10", "B": "1.20",
                     "a": "99.20", "A": "2.30"}
            trade = {"E": 10, "s": "BTCUSDT", "a": 5, "p": "99.15", "q": "0.40",
                     "f": 5, "l": 5, "T": 9, "m": True}
            for stream, data in (("btcusdt@bookTicker", quote), ("btcusdt@aggTrade", trade)):
                raw = json.dumps({"stream": stream, "data": data}, separators=(",", ":"))
                store.queue(session, COLLECTOR.parse_combined(raw))
            store.end_session(session, "normal_stop")
            store.close()
            before = source.read_bytes()
            report = COMPACTOR.compact(source, destination)
            self.assertEqual(source.read_bytes(), before)
            self.assertEqual(report["counts"]["book_ticker"], 1)
            self.assertTrue(destination.is_file())
            with self.assertRaises(FileExistsError):
                COMPACTOR.compact(source, destination)

    def test_direct_compact_writer_matches_legacy_conversion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy_path = root / "legacy.db"
            converted_path = root / "converted.db"
            direct_path = root / "direct.db"
            fixed_uuid = uuid.UUID(int=1)
            with patch.object(COLLECTOR.uuid, "uuid4", return_value=fixed_uuid), patch.object(
                COLLECTOR.time, "time_ns", return_value=1_800_000_000_000_000_000
            ), patch.object(COLLECTOR.time, "monotonic_ns", return_value=500):
                legacy = COLLECTOR.Store(legacy_path)
                direct = COLLECTOR.CompactStore(direct_path)
                legacy_session = legacy.start_session("test")
                direct_session = direct.start_session("test")
                quote = {"u": 1, "s": "BTCUSDT", "b": "99.10", "B": "1.20",
                         "a": "99.20", "A": "2.30"}
                trade = {"E": 1_800_000_000_000, "s": "BTCUSDT", "a": 5,
                         "p": "99.15", "q": "0.40", "f": 5, "l": 5,
                         "T": 1_800_000_000_000, "m": True}
                for stream, data in (("btcusdt@bookTicker", quote), ("btcusdt@aggTrade", trade)):
                    raw = json.dumps({"stream": stream, "data": data}, separators=(",", ":"))
                    message = COLLECTOR.parse_combined(raw)
                    legacy.queue(legacy_session, message)
                    direct.queue(direct_session, message)
                legacy.end_session(legacy_session, "normal_stop")
                direct.end_session(direct_session, "normal_stop")
                legacy.close()
                direct.close()
            COMPACTOR.compact(legacy_path, converted_path)
            converted = sqlite3.connect(converted_path)
            direct = sqlite3.connect(direct_path)
            try:
                self.assertEqual(
                    COMPACTOR._destination_hashes(converted),
                    COMPACTOR._destination_hashes(direct),
                )
            finally:
                converted.close()
                direct.close()


if __name__ == "__main__":
    unittest.main()
