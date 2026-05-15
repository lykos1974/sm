from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from research_v2.optimizers.manual_filter_evaluator import evaluate_rule


class ManualFilterEvaluatorTests(unittest.TestCase):
    def test_status_include_candidate_matches_only_candidate_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dataset_path = tmp_path / "labels.csv"
            rule_path = tmp_path / "rule.json"
            output_root = tmp_path / "out"

            with dataset_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "symbol",
                        "reference_ts",
                        "side",
                        "status",
                        "resolution_status",
                        "realized_r_multiple",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "symbol": "BTCUSDT",
                        "reference_ts": "1000",
                        "side": "LONG",
                        "status": "CANDIDATE",
                        "resolution_status": "TP2",
                        "realized_r_multiple": "2.0",
                    }
                )
                writer.writerow(
                    {
                        "symbol": "ETHUSDT",
                        "reference_ts": "2000",
                        "side": "LONG",
                        "status": "WATCH",
                        "resolution_status": "PENDING",
                        "realized_r_multiple": "0.0",
                    }
                )

            rule_path.write_text(
                json.dumps(
                    {
                        "rule_id": "candidate_only",
                        "categorical_filters": {
                            "status": {"mode": "include", "values": ["CANDIDATE"]}
                        },
                    }
                ),
                encoding="utf-8",
            )

            result = evaluate_rule(
                input_labeled_dataset_paths=[str(dataset_path)],
                rule_json_path=str(rule_path),
                output_root=str(output_root),
                split_mode="time",
                train_fraction=0.5,
                validation_fraction=0.0,
                oos_fraction=0.5,
                write_matched_rows=True,
            )

            all_metrics = next(row for row in result["metrics"] if row["split"] == "all")
            self.assertEqual(all_metrics["total_rows"], 2)
            self.assertEqual(all_metrics["matched_rows"], 1)
            self.assertEqual(all_metrics["candidate_rows_registered"], 1)
            self.assertEqual(all_metrics["resolved_rows"], 1)
            self.assertEqual(all_metrics["tp2_rate"], 1.0)

            self.assertTrue(Path(result["evaluated_rule_csv_path"]).exists())
            self.assertTrue(Path(result["summary_markdown_path"]).exists())
            self.assertTrue(Path(result["manifest_path"]).exists())
            self.assertTrue(Path(result["matched_rows_csv_path"]).exists())

    def test_missing_rule_column_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dataset_path = tmp_path / "labels.csv"
            rule_path = tmp_path / "rule.json"

            dataset_path.write_text(
                "reference_ts,status,resolution_status,realized_r_multiple\n"
                "1000,CANDIDATE,TP2,2.0\n",
                encoding="utf-8",
            )
            rule_path.write_text(
                json.dumps(
                    {
                        "categorical_filters": {
                            "breakout_context": {"mode": "include", "values": ["POST_BREAKOUT_PULLBACK"]}
                        }
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "Rule references column"):
                evaluate_rule(
                    input_labeled_dataset_paths=[str(dataset_path)],
                    rule_json_path=str(rule_path),
                    output_root=str(tmp_path / "out"),
                    split_mode="time",
                    train_fraction=0.6,
                    validation_fraction=0.2,
                    oos_fraction=0.2,
                    write_matched_rows=False,
                    dry_run=True,
                )

    def test_missing_realized_r_multiple_mentions_label_export(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dataset_path = tmp_path / "labels.csv"
            rule_path = tmp_path / "rule.json"

            dataset_path.write_text(
                "reference_ts,status,resolution_status\n1000,CANDIDATE,TP2\n",
                encoding="utf-8",
            )
            rule_path.write_text(
                json.dumps(
                    {
                        "categorical_filters": {
                            "status": {"mode": "include", "values": ["CANDIDATE"]}
                        }
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "labeled/analytics export is required"):
                evaluate_rule(
                    input_labeled_dataset_paths=[str(dataset_path)],
                    rule_json_path=str(rule_path),
                    output_root=str(tmp_path / "out"),
                    split_mode="time",
                    train_fraction=0.6,
                    validation_fraction=0.2,
                    oos_fraction=0.2,
                    write_matched_rows=False,
                    dry_run=True,
                )


if __name__ == "__main__":
    unittest.main()
