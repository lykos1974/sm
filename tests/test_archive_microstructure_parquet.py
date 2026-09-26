import importlib.util
import json
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
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


COLLECTOR = load("collector_for_archive", ROOT / "market_collector" / "microstructure_collector.py")
COMPACTOR = load("compactor_for_archive", ROOT / "market_collector" / "compact_microstructure_db.py")
ARCHIVER = load("microstructure_archiver", ROOT / "market_collector" / "archive_microstructure_parquet.py")
VERIFIER = load("microstructure_verifier", ROOT / "market_collector" / "verify_microstructure_archive.py")


class ArchiveTests(unittest.TestCase):
    def test_small_midnight_spill_requires_explicit_tolerance_and_is_manifested(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "spill.db"
            store = COLLECTOR.CompactStore(source)
            session = store.start_session("test")
            boundary_ns = int(
                datetime(2026, 9, 21, tzinfo=timezone.utc).timestamp() * 1_000_000_000
            )
            quote = {"u": 1, "s": "BTCUSDT", "b": "99.10", "B": "1.20",
                     "a": "99.20", "A": "2.30"}
            store.queue(session, COLLECTOR.ReceivedMessage(
                boundary_ns - 1, 1, "btcusdt@bookTicker", quote, "{}", 0.0,
            ))
            store.queue(session, COLLECTOR.ReceivedMessage(
                boundary_ns + 250_000_000, 2, "btcusdt@bookTicker",
                {**quote, "u": 2}, "{}", 0.0,
            ))
            store.write_batch(store.detach_pending())
            store.end_session(session, "normal_stop")
            store.close()
            with self.assertRaisesRegex(RuntimeError, "boundary_spill_ms=250.000"):
                ARCHIVER.archive(source, root / "refused")
            report = ARCHIVER.archive(
                source, root / "accepted", max_boundary_spill_seconds=1.0
            )
            self.assertEqual(report["utc_date"], "2026-09-20")
            self.assertTrue(report["boundary_spill"]["present"])
            self.assertEqual(report["boundary_spill"]["rows"]["book_ticker"], 1)
            self.assertEqual(report["boundary_spill"]["rows"]["agg_trades"], 0)

    def test_verified_parquet_round_trip_is_non_destructive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, compact = root / "source.db", root / "compact.db"
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
            COMPACTOR.compact(source, compact)
            before = compact.read_bytes()
            report = ARCHIVER.archive(compact, root / "archive", chunk_rows=1)
            self.assertEqual(compact.read_bytes(), before)
            self.assertEqual(report["tables"]["book_ticker"]["rows"], 1)
            self.assertEqual(report["tables"]["agg_trades"]["rows"], 1)
            self.assertEqual(report["tables"]["quality_intervals"]["rows"], 1)
            quality = report["quality"]["sessions"][session]
            self.assertEqual(quality["quality_status"], "CONTAINS_INDETERMINATE_INTERVALS")
            self.assertTrue(quality["requires_interval_filtering"])
            self.assertTrue((Path(report["archive"]) / "manifest.json").is_file())
            self.assertTrue((Path(report["archive"]) / "manifest.sha256").is_file())
            verified = VERIFIER.verify(report["archive"], report["manifest_sha256"])
            self.assertTrue(verified["manifest_sealed"])
            self.assertEqual(verified["verified_tables"]["book_ticker"]["rows"], 1)
            self.assertEqual(verified["latest_trade"]["agg_trade_id"], 5)
            self.assertEqual(verified["verified_tables"]["quality_intervals"]["rows"], 1)
            self.assertEqual(
                verified["quality"]["sessions"][session]["quality_status"],
                "CONTAINS_INDETERMINATE_INTERVALS",
            )
            with self.assertRaises(FileExistsError):
                ARCHIVER.archive(compact, root / "archive", chunk_rows=1)


if __name__ == "__main__":
    unittest.main()
