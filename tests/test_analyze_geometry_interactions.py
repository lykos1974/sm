from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from research_v2.optimizers.analyze_geometry_interactions import analyze_geometry_interactions


class AnalyzeGeometryInteractionsTests(unittest.TestCase):
    def _write_rows(self, path: Path) -> None:
        fields = [
            "row_identity", "recurring_match_count", "symbol", "side", "pullback_quality", "trend_regime", "entry_distance_bucket", "active_leg_boxes", "resolution_status", "realized_r_multiple",
        ]
        rows = [
            {"row_identity": "1", "recurring_match_count": "2", "symbol": "ETHUSDT", "side": "LONG", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "NEAR", "active_leg_boxes": "2", "resolution_status": "TP2", "realized_r_multiple": "2.0"},
            {"row_identity": "2", "recurring_match_count": "2", "symbol": "ETHUSDT", "side": "LONG", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "NEAR", "active_leg_boxes": "2", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0"},
            {"row_identity": "3", "recurring_match_count": "3", "symbol": "BTCUSDT", "side": "SHORT", "pullback_quality": "WEAK", "trend_regime": "DOWN", "entry_distance_bucket": "FAR", "active_leg_boxes": "1", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0"},
            {"row_identity": "4", "recurring_match_count": "3", "symbol": "BTCUSDT", "side": "SHORT", "pullback_quality": "WEAK", "trend_regime": "DOWN", "entry_distance_bucket": "FAR", "active_leg_boxes": "1", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0"},
            {"row_identity": "5", "recurring_match_count": "1", "symbol": "SOLUSDT", "side": "LONG", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "MID", "active_leg_boxes": "2", "resolution_status": "TP2", "realized_r_multiple": "1.5"},
            {"row_identity": "6", "recurring_match_count": "2", "symbol": "SOLUSDT", "side": "LONG", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "MID", "active_leg_boxes": "2", "resolution_status": "TP1_ONLY", "realized_r_multiple": "0.3"},
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def test_grouping_and_lift_and_ranking(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            csv_path = base / "recurring_rows.csv"
            self._write_rows(csv_path)
            out = analyze_geometry_interactions(
                recurring_rows_csv=str(csv_path),
                output_root=str(base / "out"),
                min_recurring_count=2,
                min_cluster_size=2,
            )

            with Path(out["geometry_interactions_csv"]).open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 2)
            top = rows[0]
            self.assertEqual(top["active_leg_boxes"], "2")
            self.assertEqual(top["entry_distance_bucket"], "NEAR")
            self.assertAlmostEqual(float(top["tp2_ratio"]), 0.5, places=6)
            self.assertAlmostEqual(float(top["tp2_lift"]), 0.25, places=6)

            with Path(out["strongest_failure_clusters_csv"]).open("r", encoding="utf-8") as handle:
                failure = list(csv.DictReader(handle))
            self.assertEqual(failure[0]["symbol"], "BTCUSDT")

            summary = Path(out["geometry_interaction_summary_md"]).read_text(encoding="utf-8")
            self.assertIn("low-sample TP2-lift clusters", summary)
            self.assertIn("LONG vs SHORT geometry comparison", summary)


if __name__ == "__main__":
    unittest.main()
