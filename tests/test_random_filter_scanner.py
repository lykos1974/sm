from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from research_v2.optimizers.random_filter_scanner import scan_rules


class RandomFilterScannerTests(unittest.TestCase):
    def _write_fixture(self, path: Path) -> None:
        fields = [
            "reference_ts","status","side","breakout_context","pullback_quality","trend_regime",
            "entry_distance_bucket","continuation_execution_class","pattern_family","quality_grade",
            "is_extended_move","is_baseline_profile_match","active_leg_boxes","quality_score",
            "entry_distance_boxes","continuation_quality_score","extension_penalty","resolution_status","realized_r_multiple",
        ]
        rows = []
        # early train-positive for short overfit
        for i in range(1, 31):
            rows.append({**{f: "" for f in fields}, "reference_ts": i, "status": "CANDIDATE", "side": "SHORT", "resolution_status": "TP2", "realized_r_multiple": "2.0", "active_leg_boxes": "1", "is_extended_move": "1", "quality_score": "0.2"})
        # long profitable across all periods
        for i in range(31, 121):
            rows.append({**{f: "" for f in fields}, "reference_ts": i, "status": "CANDIDATE", "side": "LONG", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "HEALTHY", "resolution_status": "TP2" if i % 3 else "STOPPED", "realized_r_multiple": "1.8" if i % 3 else "-1.0", "active_leg_boxes": "2", "is_extended_move": "0", "is_baseline_profile_match": "1", "quality_score": "0.9", "entry_distance_boxes": "1.5", "continuation_quality_score": "0.8", "extension_penalty": "0.1"})
        with path.open("w", encoding="utf-8", newline="") as h:
            w = csv.DictWriter(h, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)

    def test_scanner_outputs_and_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            ds = base / "labels.csv"
            self._write_fixture(ds)
            out1 = scan_rules(
                input_labeled_dataset_paths=[str(ds)], output_root=str(base / "o1"), split_mode="time",
                train_fraction=0.6, validation_fraction=0.2, oos_fraction=0.2,
                max_rules=120, random_seed=1337, force_side=None, min_validation_resolved=5, min_oos_resolved=5, max_complexity=5,
            )
            out2 = scan_rules(
                input_labeled_dataset_paths=[str(ds)], output_root=str(base / "o2"), split_mode="time",
                train_fraction=0.6, validation_fraction=0.2, oos_fraction=0.2,
                max_rules=120, random_seed=1337, force_side=None, min_validation_resolved=5, min_oos_resolved=5, max_complexity=5,
            )
            r1 = Path(out1["ranked_rules_csv"]).read_text(encoding="utf-8")
            r2 = Path(out2["ranked_rules_csv"]).read_text(encoding="utf-8")
            self.assertEqual(r1, r2)
            self.assertTrue(Path(out1["manifest"]).exists())
            self.assertIn("Research-only", Path(out1["ranked_rules_md"]).read_text(encoding="utf-8"))

    def test_rejects_overfit_and_respects_complexity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            ds = base / "labels.csv"
            self._write_fixture(ds)
            out = scan_rules(
                input_labeled_dataset_paths=[str(ds)], output_root=str(base / "o"), split_mode="time",
                train_fraction=0.6, validation_fraction=0.2, oos_fraction=0.2,
                max_rules=80, random_seed=7, force_side=None, min_validation_resolved=5, min_oos_resolved=5, max_complexity=2,
            )
            rejected = Path(out["rejected_rules_csv"]).read_text(encoding="utf-8")
            self.assertIn("oos_avg_r_non_positive", rejected)
            with Path(out["ranked_rules_csv"]).open("r", encoding="utf-8") as handle:
                ranked_rows = list(csv.DictReader(handle))
            self.assertTrue(ranked_rows)
            self.assertTrue(all(int(r["complexity"]) <= 2 for r in ranked_rows))


if __name__ == "__main__":
    unittest.main()
