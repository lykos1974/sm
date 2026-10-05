"""Short deterministic annual-wrapper tests; the real annual CSV is not loaded."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_v2 import prz_x_break_pole_annual as annual


class AnnualLedgerTests(unittest.TestCase):
    def fixture(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        path = Path(root.name) / "candles.csv"
        prices = (2700,3000,2000,2600,2100,2400,2800,3200,2600,2500,3300,3200)
        first = 1704067259999
        path.write_text("close_time,open,close\n" + "".join(
            f"{first+i*60000},{p},{p}\n" for i, p in enumerate(prices)))
        return path, Path(root.name)/"report.json", hashlib.sha256(path.read_bytes()).hexdigest(), first, len(prices)

    def test_complete_labeled_ledger_and_existing_report_protection(self):
        path, output, digest, first, count = self.fixture()
        with (patch.object(annual, "SOURCE_SHA256", digest),
              patch.object(annual, "SOURCE_MINUTES", count),
              patch.object(annual, "FIRST_CLOSE_MS", first),
              patch.object(annual, "LAST_CLOSE_MS", first+(count-1)*60000)):
            summary = annual.run(path, output=output, expected_sha256=digest)
            report = json.loads(output.read_text())
            self.assertEqual(summary["signal_categories"]["CONTEXT_MATCH"], 1)
            self.assertEqual(report["source_sha256"], digest)
            self.assertEqual(report["summary"]["interpretation"],
                             "observed subset of unchanged baseline; not a filtered replay or net profit")
            self.assertTrue(report["decision_facts"])
            self.assertEqual(report["signal_annotations"][0]["category"], "CONTEXT_MATCH")
            saved = output.read_bytes()
            with self.assertRaisesRegex(ValueError, "new output"):
                annual.run(path, output=output, expected_sha256=digest)
            self.assertEqual(output.read_bytes(), saved)

    def test_hash_mismatch_fails_without_report(self):
        path, output, digest, first, count = self.fixture()
        with (patch.object(annual, "SOURCE_SHA256", "0"*64),
              patch.object(annual, "SOURCE_MINUTES", count)):
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                annual.run(path, output=output, expected_sha256="0"*64)
        self.assertFalse(output.exists())

    def test_publication_failure_cleans_temporary_and_keeps_existing_bytes(self):
        path, output, digest, first, count = self.fixture()
        with patch.object(annual.os, "link", side_effect=PermissionError("synthetic")):
            with self.assertRaises(PermissionError):
                annual._atomic_new_json(output, {"safe": True})
        self.assertFalse(output.exists())
        self.assertEqual(list(output.parent.glob(".prz_context_*.tmp")), [])
        output.write_bytes(b"original report")
        with self.assertRaisesRegex(ValueError, "existing report"):
            annual._atomic_new_json(output, {"safe": False})
        self.assertEqual(output.read_bytes(), b"original report")


if __name__ == "__main__":
    unittest.main()
