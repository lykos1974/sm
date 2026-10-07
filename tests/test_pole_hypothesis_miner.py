"""Offline contract for the bounded, research-only hypothesis miner."""
import csv
import json
import tempfile
import unittest
from pathlib import Path

from research_v2.patterns.pole_hypothesis_miner import mine


class MinerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "opportunities.csv"
        self.output = self.root / "report.json"

    def rows(self, n=40):
        return [dict(opportunity_id=f"o{i}", decision_ts=str(1700000000000+i*86400000),
                     known_at=str(1700000000000+i*86400000), symbol="BTCUSDT",
                     side="LONG" if i % 2 else "SHORT", pole_boxes=str(3+i%3),
                     reversal_boxes="3", retrace_ratio="0.5", relative_pole_size="NEAR",
                     net_r="1" if i % 3 else "-1") for i in range(n)]

    def write(self, rows):
        with self.source.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def run_miner(self, rows=None, **kwargs):
        self.write(rows if rows is not None else self.rows())
        return mine(self.source, self.output, min_train=4, min_validation=2,
                    min_test=2, **kwargs)

    def test_report_is_reproducible_and_test_is_untouched_during_selection(self):
        report = self.run_miner()
        self.assertEqual(report["schema"], "pole-hypothesis-miner-v1")
        self.assertTrue(report["research_only"])
        self.assertEqual(report["universe_count"], 40)
        self.assertEqual(report["candidate_count"], 21)
        self.assertIn("test", report["finalist"])
        self.assertEqual(json.loads(self.output.read_text()), report)
        first = report["finalist"]["rule_id"]
        changed = self.rows()
        for row in changed[32:]:
            row["net_r"] = "-999"
        self.output.unlink()
        self.assertEqual(self.run_miner(changed)["finalist"]["rule_id"], first)

    def test_late_feature_fails_closed(self):
        rows = self.rows()
        rows[0]["known_at"] = str(int(rows[0]["decision_ts"])+1)
        with self.assertRaisesRegex(ValueError, "future feature"):
            self.run_miner(rows)
        self.assertFalse(self.output.exists())

    def test_unknown_column_and_nonfinite_outcome_rejected(self):
        rows = self.rows()
        rows[0]["future_max_price"] = "100"
        for row in rows[1:]:
            row["future_max_price"] = "100"
        with self.assertRaisesRegex(ValueError, "schema"):
            self.run_miner(rows)
        rows = self.rows()
        rows[0]["net_r"] = "NaN"
        with self.assertRaisesRegex(ValueError, "numeric"):
            self.run_miner(rows)

    def test_duplicates_time_order_and_decimal_types_rejected(self):
        rows = self.rows()
        rows[1]["opportunity_id"] = rows[0]["opportunity_id"]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.run_miner(rows)
        rows = self.rows()
        rows[1]["decision_ts"] = rows[0]["decision_ts"]
        with self.assertRaisesRegex(ValueError, "chronology"):
            self.run_miner(rows)
        rows = self.rows()
        rows[0]["retrace_ratio"] = "Infinity"
        with self.assertRaisesRegex(ValueError, "numeric"):
            self.run_miner(rows)

    def test_minimum_sample_blocks_lucky_small_subsets(self):
        self.write(self.rows(20))
        with self.assertRaisesRegex(ValueError, "insufficient"):
            mine(self.source, self.output)
        self.assertFalse(self.output.exists())

    def test_existing_report_never_overwritten(self):
        self.output.write_bytes(b"existing")
        with self.assertRaises(FileExistsError):
            self.run_miner()
        self.assertEqual(self.output.read_bytes(), b"existing")


if __name__ == "__main__":
    unittest.main()
