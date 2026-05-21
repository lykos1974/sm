from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from research_v2.optimizers.analyze_recurring_rows import analyze_recurring_rows


class AnalyzeRecurringRowsTests(unittest.TestCase):
    def _write_dataset(self, path: Path, *, duplicate_row_id: bool = False) -> None:
        fields = [
            "row_id", "setup_id", "reference_ts", "symbol", "status", "side", "breakout_context", "pullback_quality", "trend_regime",
            "entry_distance_bucket", "continuation_execution_class", "active_leg_boxes", "quality_score", "resolution_status", "realized_r_multiple", "strategy",
        ]
        rows = [
            {"row_id": "r-1", "setup_id": "s-1", "reference_ts": "1", "symbol": "ETHUSDT", "status": "WATCH", "side": "LONG", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "NEAR", "continuation_execution_class": "A", "active_leg_boxes": "2", "quality_score": "0.91", "resolution_status": "TP2", "realized_r_multiple": "2.0", "strategy": "S1"},
            {"row_id": "r-2", "setup_id": "s-2", "reference_ts": "2", "symbol": "ETHUSDT", "status": "WATCH", "side": "LONG", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "NEAR", "continuation_execution_class": "A", "active_leg_boxes": "2", "quality_score": "0.86", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0", "strategy": "S1"},
            {"row_id": "r-3", "setup_id": "s-3", "reference_ts": "3", "symbol": "BTCUSDT", "status": "CANDIDATE", "side": "LONG", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "MID", "continuation_execution_class": "B", "active_leg_boxes": "2", "quality_score": "0.72", "resolution_status": "TP1_ONLY", "realized_r_multiple": "0.5", "strategy": "S1"},
            {"row_id": "r-4", "setup_id": "s-4", "reference_ts": "4", "symbol": "ETHUSDT", "status": "WATCH", "side": "SHORT", "breakout_context": "LATE_EXTENSION", "pullback_quality": "WEAK", "trend_regime": "DOWN", "entry_distance_bucket": "FAR", "continuation_execution_class": "C", "active_leg_boxes": "1", "quality_score": "0.30", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0", "strategy": "S1"},
        ]
        if duplicate_row_id:
            rows[1]["row_id"] = rows[0]["row_id"]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def _write_ranked_and_rules(self, base: Path) -> tuple[Path, Path]:
        ranked = base / "ranked_rules.csv"
        top_dir = base / "top_rules"
        top_dir.mkdir()
        rules = [
            {"rule_id": "r1", "categorical_filters": {"status": {"mode": "include", "values": ["WATCH"]}, "side": {"mode": "include", "values": ["LONG"]}}},
            {"rule_id": "r2", "categorical_filters": {"symbol": {"mode": "include", "values": ["ETHUSDT"]}, "status": {"mode": "include", "values": ["WATCH"]}}},
            {"rule_id": "r3", "categorical_filters": {"status": {"mode": "include", "values": ["WATCH"]}}, "numeric_thresholds": {"quality_score": {"min": 0.85, "max": 1.0}}},
        ]
        with ranked.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["rule_id", "rule_json"])
            writer.writeheader()
            for rule in rules:
                writer.writerow({"rule_id": rule["rule_id"], "rule_json": json.dumps(rule)})
        for i, rule in enumerate(rules, start=1):
            (top_dir / f"{i:02d}_{rule['rule_id']}.json").write_text(json.dumps(rule), encoding="utf-8")
        return ranked, top_dir

    def test_recurrence_counting_and_tp2_detection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            dataset = base / "labels.csv"
            self._write_dataset(dataset)
            ranked, top_dir = self._write_ranked_and_rules(base)
            out = analyze_recurring_rows(
                ranked_rules_csv=str(ranked),
                top_rules_dir=str(top_dir),
                labeled_dataset_path=str(dataset),
                top_n=3,
                output_root=str(base / "out"),
            )

            with Path(out["recurring_rows_csv"]).open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(rows[0]["row_identity"], "row_id:r-1")
            self.assertEqual(int(rows[0]["recurring_match_count"]), 3)
            self.assertEqual(rows[0]["resolution_status"], "TP2")

            summary = Path(out["recurring_rows_summary_md"]).read_text(encoding="utf-8")
            self.assertIn("Top recurring TP2 rows", summary)
            self.assertIn("Recurrence Distribution", summary)
            self.assertIn("rediscovered setup population", summary)

    def test_duplicate_identity_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            dataset = base / "labels.csv"
            self._write_dataset(dataset, duplicate_row_id=True)
            ranked, top_dir = self._write_ranked_and_rules(base)
            with self.assertRaisesRegex(ValueError, "not unique"):
                analyze_recurring_rows(
                    ranked_rules_csv=str(ranked),
                    top_rules_dir=str(top_dir),
                    labeled_dataset_path=str(dataset),
                    top_n=3,
                    output_root=str(base / "out"),
                )


if __name__ == "__main__":
    unittest.main()
