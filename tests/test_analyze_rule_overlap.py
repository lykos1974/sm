from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from research_v2.optimizers.analyze_rule_overlap import analyze_rule_overlap


class AnalyzeRuleOverlapTests(unittest.TestCase):
    def _write_dataset(self, path: Path, *, include_strategy: bool = True, include_row_id: bool = True) -> None:
        fields = [
            "reference_ts", "symbol", "status", "side", "breakout_context", "pullback_quality", "trend_regime",
            "entry_distance_bucket", "continuation_execution_class", "is_extended_move", "active_leg_boxes", "quality_score",
            "resolution_status", "realized_r_multiple",
        ]
        if include_strategy:
            fields.append("strategy")
        if include_row_id:
            fields.append("row_id")
        rows = [
            {"reference_ts": "1", "symbol": "ETHUSDT", "status": "WATCH", "side": "LONG", "breakout_context": "LATE_EXTENSION", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "NEAR", "continuation_execution_class": "A", "is_extended_move": "0", "active_leg_boxes": "2", "quality_score": "0.9", "resolution_status": "TP2", "realized_r_multiple": "2", "strategy": "S1", "row_id": "r-1"},
            {"reference_ts": "2", "symbol": "ETHUSDT", "status": "WATCH", "side": "LONG", "breakout_context": "LATE_EXTENSION", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "NEAR", "continuation_execution_class": "A", "is_extended_move": "0", "active_leg_boxes": "2", "quality_score": "0.85", "resolution_status": "TP2", "realized_r_multiple": "1.5", "strategy": "S1", "row_id": "r-2"},
            {"reference_ts": "3", "symbol": "ETHUSDT", "status": "WATCH", "side": "LONG", "breakout_context": "LATE_EXTENSION", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "MID", "continuation_execution_class": "A", "is_extended_move": "0", "active_leg_boxes": "2", "quality_score": "0.7", "resolution_status": "STOPPED", "realized_r_multiple": "-1", "strategy": "S1", "row_id": "r-3"},
            {"reference_ts": "4", "symbol": "BTCUSDT", "status": "CANDIDATE", "side": "SHORT", "breakout_context": "POST_BREAKOUT_PULLBACK", "pullback_quality": "WEAK", "trend_regime": "DOWN", "entry_distance_bucket": "FAR", "continuation_execution_class": "B", "is_extended_move": "1", "active_leg_boxes": "1", "quality_score": "0.3", "resolution_status": "TP1_ONLY", "realized_r_multiple": "0.4", "strategy": "S1", "row_id": "r-4"},
            {"reference_ts": "5", "symbol": "ETHUSDT", "status": "WATCH", "side": "LONG", "breakout_context": "LATE_EXTENSION", "pullback_quality": "HEALTHY", "trend_regime": "UP", "entry_distance_bucket": "NEAR", "continuation_execution_class": "A", "is_extended_move": "0", "active_leg_boxes": "2", "quality_score": "0.95", "resolution_status": "TP2", "realized_r_multiple": "2.1", "strategy": "S1", "row_id": "r-5"},
        ]
        if not include_strategy:
            for row in rows:
                row.pop("strategy", None)
        if not include_row_id:
            for row in rows:
                row.pop("row_id", None)
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
            {"rule_id": "r2", "categorical_filters": {"status": {"mode": "include", "values": ["WATCH"]}, "side": {"mode": "include", "values": ["LONG"]}, "breakout_context": {"mode": "include", "values": ["LATE_EXTENSION"]}}},
            {"rule_id": "r3", "categorical_filters": {"status": {"mode": "include", "values": ["WATCH"]}}, "numeric_thresholds": {"quality_score": {"min": 0.8, "max": 1.0}}},
            {"rule_id": "r4", "categorical_filters": {"status": {"mode": "include", "values": ["CANDIDATE"]}, "side": {"mode": "include", "values": ["SHORT"]}}},
        ]
        with ranked.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["rule_id", "rule_json"])
            writer.writeheader()
            for rule in rules:
                writer.writerow({"rule_id": rule["rule_id"], "rule_json": json.dumps(rule)})
        for i, rule in enumerate(rules, start=1):
            (top_dir / f"{i:02d}_{rule['rule_id']}.json").write_text(json.dumps(rule), encoding="utf-8")
        return ranked, top_dir

    def test_overlap_outputs_and_jaccard_properties(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            dataset = base / "labels.csv"
            self._write_dataset(dataset)
            ranked, top_dir = self._write_ranked_and_rules(base)
            out = analyze_rule_overlap(
                ranked_rules_csv=str(ranked),
                top_rules_dir=str(top_dir),
                labeled_dataset_path=str(dataset),
                top_n=4,
                output_root=str(base / "out"),
            )
            self.assertEqual(out["identity_method"], "field")
            self.assertEqual(out["identity_field_or_composite"], "row_id")
            self.assertEqual(out["matched_row_identity_count"], 5)

            with Path(out["overlap_matrix_csv"]).open("r", encoding="utf-8") as handle:
                overlap_rows = list(csv.DictReader(handle))

            def j(left: str, right: str) -> float:
                row = next(r for r in overlap_rows if r["rule_id_left"] == left and r["rule_id_right"] == right)
                return float(row["jaccard"])

            self.assertAlmostEqual(j("r1", "r1"), 1.0)
            self.assertAlmostEqual(j("r4", "r4"), 1.0)
            self.assertAlmostEqual(j("r1", "r4"), 0.0)
            self.assertAlmostEqual(j("r1", "r3"), j("r3", "r1"))

            summary = Path(out["archetype_summary_md"]).read_text(encoding="utf-8")
            self.assertIn("Identity method:", summary)
            self.assertIn("Identity field/composite:", summary)
            self.assertIn("textual structural signatures", summary)

    def test_duplicate_reference_ts_fails_without_unique_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            dataset = base / "labels.csv"
            self._write_dataset(dataset, include_row_id=False, include_strategy=False)
            with dataset.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            rows[1]["reference_ts"] = rows[0]["reference_ts"]
            with dataset.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

            ranked, top_dir = self._write_ranked_and_rules(base)
            with self.assertRaisesRegex(ValueError, "No stable identity available"):
                analyze_rule_overlap(
                    ranked_rules_csv=str(ranked),
                    top_rules_dir=str(top_dir),
                    labeled_dataset_path=str(dataset),
                    top_n=3,
                    output_root=str(base / "out"),
                )

    def test_missing_identity_fields_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            dataset = base / "labels.csv"
            self._write_dataset(dataset, include_row_id=False, include_strategy=False)
            ranked, top_dir = self._write_ranked_and_rules(base)
            with self.assertRaisesRegex(ValueError, "No stable identity available"):
                analyze_rule_overlap(
                    ranked_rules_csv=str(ranked),
                    top_rules_dir=str(top_dir),
                    labeled_dataset_path=str(dataset),
                    top_n=3,
                    output_root=str(base / "out"),
                )


if __name__ == "__main__":
    unittest.main()
