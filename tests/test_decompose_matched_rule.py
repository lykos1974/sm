from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from research_v2.optimizers.decompose_matched_rule import decompose_matched_rows


class DecomposeMatchedRuleTests(unittest.TestCase):
    def _write_fixture(self, path: Path) -> None:
        fields = [
            "symbol",
            "side",
            "status",
            "breakout_context",
            "pullback_quality",
            "trend_regime",
            "continuation_execution_class",
            "entry_distance_bucket",
            "active_leg_boxes",
            "quality_score",
            "resolution_status",
            "realized_r_multiple",
        ]
        rows = [
            {"symbol": "BTCUSDT", "side": "LONG", "status": "WATCH", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "continuation_execution_class": "A", "entry_distance_bucket": "NEAR", "active_leg_boxes": "2", "quality_score": "0.5", "resolution_status": "TP2", "realized_r_multiple": "2.0"},
            {"symbol": "BTCUSDT", "side": "LONG", "status": "WATCH", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "continuation_execution_class": "A", "entry_distance_bucket": "NEAR", "active_leg_boxes": "2", "quality_score": "0.5", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0"},
            {"symbol": "BTCUSDT", "side": "LONG", "status": "WATCH", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "continuation_execution_class": "B", "entry_distance_bucket": "MID", "active_leg_boxes": "2", "quality_score": "0.5", "resolution_status": "EXPIRED", "realized_r_multiple": "0.0"},
            {"symbol": "ETHUSDT", "side": "LONG", "status": "CANDIDATE", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "continuation_execution_class": "A", "entry_distance_bucket": "FAR", "active_leg_boxes": "3", "quality_score": "0.5", "resolution_status": "STOPPED", "realized_r_multiple": "0.0"},
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def test_decomposition_outputs_and_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            matched = base / "matched_rows.csv"
            self._write_fixture(matched)
            out = base / "out"

            result = decompose_matched_rows(matched_rows_csv=str(matched), output_root=str(out), rule_id="random_04277")

            self.assertTrue(Path(result["summary_path"]).exists())
            self.assertTrue(Path(result["tables_path"]).exists())
            self.assertTrue(Path(result["positive_path"]).exists())
            self.assertTrue(Path(result["negative_path"]).exists())

            summary = Path(result["summary_path"]).read_text(encoding="utf-8")
            self.assertIn("TP2: 1", summary)
            self.assertIn("STOPPED: 2", summary)
            self.assertIn("EXPIRED: 1", summary)
            self.assertIn("Edge concentration warning", summary)
            self.assertIn("WATCH rows (3) exceed CANDIDATE rows (1)", summary)
            self.assertIn("quality_score appears constant", summary)

            with Path(result["tables_path"]).open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            symbol_btc = next(r for r in rows if r["dimension"] == "symbol" and r["bucket"] == "BTCUSDT")
            self.assertAlmostEqual(float(symbol_btc["sum_realized_r_multiple"]), 1.0, places=8)


if __name__ == "__main__":
    unittest.main()
