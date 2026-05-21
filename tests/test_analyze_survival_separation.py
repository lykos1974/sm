from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from research_v2.optimizers.analyze_survival_separation import analyze_survival_separation


class AnalyzeSurvivalSeparationTests(unittest.TestCase):
    def _write_recurring_rows(self, path: Path) -> None:
        fields = [
            "row_identity", "recurring_match_count", "matched_rule_ids", "realized_r_multiple", "symbol", "side", "status",
            "breakout_context", "pullback_quality", "trend_regime", "continuation_execution_class", "active_leg_boxes",
            "entry_distance_bucket", "quality_score", "resolution_status",
        ]
        rows = [
            {"row_identity": "row_id:r1", "recurring_match_count": "4", "matched_rule_ids": "a,b", "realized_r_multiple": "2.0", "symbol": "ETHUSDT", "side": "LONG", "status": "WATCH", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "continuation_execution_class": "A", "active_leg_boxes": "2", "entry_distance_bucket": "NEAR", "quality_score": "0.92", "resolution_status": "TP2"},
            {"row_identity": "row_id:r2", "recurring_match_count": "3", "matched_rule_ids": "a,c", "realized_r_multiple": "1.7", "symbol": "ETHUSDT", "side": "LONG", "status": "WATCH", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "continuation_execution_class": "A", "active_leg_boxes": "2", "entry_distance_bucket": "NEAR", "quality_score": "0.88", "resolution_status": "TP2"},
            {"row_identity": "row_id:r3", "recurring_match_count": "2", "matched_rule_ids": "b,c", "realized_r_multiple": "-1.0", "symbol": "BTCUSDT", "side": "LONG", "status": "WATCH", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "continuation_execution_class": "A", "active_leg_boxes": "2", "entry_distance_bucket": "MID", "quality_score": "0.55", "resolution_status": "STOPPED"},
            {"row_identity": "row_id:r4", "recurring_match_count": "4", "matched_rule_ids": "b,d", "realized_r_multiple": "-1.0", "symbol": "SOLUSDT", "side": "LONG", "status": "WATCH", "breakout_context": "LATE_EXTENSION", "pullback_quality": "WEAK", "trend_regime": "UP", "continuation_execution_class": "B", "active_leg_boxes": "1", "entry_distance_bucket": "FAR", "quality_score": "0.35", "resolution_status": "STOPPED"},
            {"row_identity": "row_id:r5", "recurring_match_count": "5", "matched_rule_ids": "d,e", "realized_r_multiple": "0.3", "symbol": "ETHUSDT", "side": "LONG", "status": "CANDIDATE", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "continuation_execution_class": "A", "active_leg_boxes": "2", "entry_distance_bucket": "MID", "quality_score": "0.74", "resolution_status": "TP1_ONLY"},
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def test_enrichment_and_lift_calculations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            recurring = base / "recurring_rows.csv"
            self._write_recurring_rows(recurring)
            out = analyze_survival_separation(
                recurring_rows_csv=str(recurring),
                output_root=str(base / "out"),
                min_recurring_count=2,
            )
            self.assertEqual(out["input_rows"], 5)
            self.assertEqual(out["filtered_rows"], 5)
            self.assertEqual(out["included_survival_rows"], 4)
            self.assertEqual(out["excluded_non_survival_rows"], 1)
            self.assertAlmostEqual(out["baseline_tp2_ratio"], 0.5)
            self.assertAlmostEqual(out["baseline_stopped_ratio"], 0.5)

            with Path(out["feature_comparison_table_csv"]).open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            symbol_eth = next(r for r in rows if r["feature"] == "symbol" and r["value"] == "ETHUSDT")
            self.assertEqual(int(symbol_eth["tp2_count"]), 2)
            self.assertEqual(int(symbol_eth["stopped_count"]), 0)
            self.assertAlmostEqual(float(symbol_eth["tp2_lift_vs_baseline"]), 2.0)

            rec_r4 = next(r for r in rows if r["feature"] == "recurring_match_count" and r["value"] == "R4")
            self.assertEqual(int(rec_r4["count"]), 2)

    def test_min_recurring_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            recurring = base / "recurring_rows.csv"
            self._write_recurring_rows(recurring)
            out = analyze_survival_separation(
                recurring_rows_csv=str(recurring),
                output_root=str(base / "out"),
                min_recurring_count=4,
            )
            self.assertEqual(out["filtered_rows"], 3)
            self.assertEqual(out["included_survival_rows"], 2)
            self.assertEqual(out["excluded_non_survival_rows"], 1)


if __name__ == "__main__":
    unittest.main()
