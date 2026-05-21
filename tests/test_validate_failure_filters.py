from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from research_v2.optimizers.validate_failure_filters import (
    _select_failure_clusters,
    validate_failure_filters,
)


class ValidateFailureFiltersTests(unittest.TestCase):
    def _write_recurring_rows(self, path: Path) -> None:
        fields = [
            "row_identity", "active_leg_boxes", "entry_distance_bucket", "recurring_match_count", "pullback_quality", "trend_regime", "side", "symbol", "resolution_status", "realized_r_multiple",
        ]
        rows = [
            {"row_identity": "1", "active_leg_boxes": "2", "entry_distance_bucket": "NEAR", "recurring_match_count": "2", "pullback_quality": "HEALTHY", "trend_regime": "UP", "side": "LONG", "symbol": "ETHUSDT", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0"},
            {"row_identity": "2", "active_leg_boxes": "2.0", "entry_distance_bucket": "near", "recurring_match_count": "2", "pullback_quality": "healthy", "trend_regime": "up", "side": "long", "symbol": "ethusdt", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0"},
            {"row_identity": "3", "active_leg_boxes": "2", "entry_distance_bucket": "NEAR", "recurring_match_count": "2", "pullback_quality": "HEALTHY", "trend_regime": "UP", "side": "LONG", "symbol": "ETHUSDT", "resolution_status": "TP2", "realized_r_multiple": "2.0"},
            {"row_identity": "4", "active_leg_boxes": "1", "entry_distance_bucket": "FAR", "recurring_match_count": "3", "pullback_quality": "WEAK", "trend_regime": "DOWN", "side": "SHORT", "symbol": "BTCUSDT", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0"},
            {"row_identity": "5", "active_leg_boxes": "1", "entry_distance_bucket": "FAR", "recurring_match_count": "3", "pullback_quality": "WEAK", "trend_regime": "DOWN", "side": "SHORT", "symbol": "BTCUSDT", "resolution_status": "TP2", "realized_r_multiple": "1.8"},
            {"row_identity": "6", "active_leg_boxes": "4", "entry_distance_bucket": "IMMEDIATE", "recurring_match_count": "6", "pullback_quality": "HEALTHY", "trend_regime": "BEARISH_REGIME", "side": "SHORT", "symbol": "BINANCE_FUT:ETHUSDT", "resolution_status": "STOPPED", "realized_r_multiple": "-1.0"},
            {"row_identity": "7", "active_leg_boxes": "4", "entry_distance_bucket": "IMMEDIATE", "recurring_match_count": "5", "pullback_quality": "HEALTHY", "trend_regime": "BEARISH_REGIME", "side": "SHORT", "symbol": "BINANCE_FUT:ETHUSDT", "resolution_status": "TP2", "realized_r_multiple": "1.2"},
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def _write_clusters(self, path: Path) -> None:
        fields = [
            "active_leg_boxes", "entry_distance_bucket", "recurring_count_bucket", "pullback_quality", "trend_regime", "side", "symbol", "count", "stopped_lift",
        ]
        rows = [
            {"active_leg_boxes": "2", "entry_distance_bucket": "NEAR", "recurring_count_bucket": "2", "pullback_quality": "HEALTHY", "trend_regime": "UP", "side": "LONG", "symbol": "ETHUSDT", "count": "3", "stopped_lift": "0.40"},
            {"active_leg_boxes": "4", "entry_distance_bucket": "IMMEDIATE", "recurring_count_bucket": "5+", "pullback_quality": "HEALTHY", "trend_regime": "BEARISH_REGIME", "side": "SHORT", "symbol": "BINANCE_FUT:ETHUSDT", "count": "15", "stopped_lift": "0.50"},
            {"active_leg_boxes": "1", "entry_distance_bucket": "FAR", "recurring_count_bucket": "3", "pullback_quality": "WEAK", "trend_regime": "DOWN", "side": "SHORT", "symbol": "BTCUSDT", "count": "2", "stopped_lift": "0.20"},
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def test_cluster_selection_and_min_size_enforcement(self) -> None:
        clusters = [
            {"count": "12", "stopped_lift": "0.3"},
            {"count": "11", "stopped_lift": "0.5"},
            {"count": "9", "stopped_lift": "0.8"},
        ]
        selected = _select_failure_clusters(clusters, top_n=2, min_cluster_size=10)
        self.assertEqual(len(selected), 2)
        self.assertEqual(selected[0]["stopped_lift"], "0.5")
        self.assertEqual(selected[1]["stopped_lift"], "0.3")

    def test_exclusion_and_metrics_with_5plus_bucket_and_normalization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            recurring = base / "recurring_rows.csv"
            clusters = base / "strongest_failure_clusters.csv"
            self._write_recurring_rows(recurring)
            self._write_clusters(clusters)

            out = validate_failure_filters(
                recurring_rows_csv=str(recurring),
                failure_clusters_csv=str(clusters),
                output_root=str(base / "out"),
                top_n_failure_clusters=2,
                min_cluster_size=2,
            )
            self.assertEqual(out["selected_failure_clusters"], 2)
            self.assertEqual(out["excluded_rows"], 5)
            self.assertEqual(out["retained_rows"], 2)

            with Path(out["excluded_rows_csv"]).open("r", encoding="utf-8") as handle:
                excluded = list(csv.DictReader(handle))
            self.assertEqual({r["row_identity"] for r in excluded}, {"1", "2", "3", "6", "7"})

            with Path(out["filter_effects_csv"]).open("r", encoding="utf-8") as handle:
                effects = {r["metric"]: r for r in csv.DictReader(handle)}
            self.assertAlmostEqual(float(effects["tp2_ratio"]["before"]), 3 / 7)
            self.assertAlmostEqual(float(effects["tp2_ratio"]["after"]), 1 / 2)
            self.assertEqual(int(float(effects["tp2_count_removed"]["after"])), 2)

    def test_warns_loudly_when_selected_clusters_exclude_zero_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            recurring = base / "recurring_rows.csv"
            self._write_recurring_rows(recurring)
            clusters = base / "strongest_failure_clusters.csv"
            with clusters.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["active_leg_boxes", "entry_distance_bucket", "recurring_count_bucket", "pullback_quality", "trend_regime", "side", "symbol", "count", "stopped_lift"],
                )
                writer.writeheader()
                writer.writerow(
                    {"active_leg_boxes": "9", "entry_distance_bucket": "ULTRA", "recurring_count_bucket": "5+", "pullback_quality": "RARE", "trend_regime": "SIDEWAYS", "side": "SHORT", "symbol": "XRPUSDT", "count": "20", "stopped_lift": "0.9"}
                )

            out = validate_failure_filters(
                recurring_rows_csv=str(recurring),
                failure_clusters_csv=str(clusters),
                output_root=str(base / "out"),
                top_n_failure_clusters=1,
                min_cluster_size=2,
            )
            self.assertEqual(out["selected_failure_clusters"], 1)
            self.assertEqual(out["excluded_rows"], 0)
            summary = Path(out["failure_filter_summary_md"]).read_text(encoding="utf-8")
            self.assertIn("CRITICAL matching warning", summary)


if __name__ == "__main__":
    unittest.main()
