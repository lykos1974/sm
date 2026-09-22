import sys
from pathlib import Path
from unittest import TestCase

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
PNF_MVP_ROOT = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_ROOT))

from strategy_trade_export import (  # noqa: E402
    build_summary_activated_only,
    compute_trade_metrics,
)


def resolved_row(setup_id, status, resolved_price, *, ambiguous_low=None, ambiguous_high=None):
    return {
        "setup_id": setup_id,
        "created_ts": 1,
        "updated_ts": 2,
        "reference_ts": 1,
        "resolved_ts": 2,
        "symbol": "BTCUSDT",
        "strategy": "pullback_retest",
        "side": "LONG",
        "status": "CANDIDATE",
        "activation_status": "ACTIVE",
        "resolution_status": status,
        "ideal_entry": 100.0,
        "activated_ts": 2,
        "activated_price": 100.0,
        "invalidation": 98.0,
        "tp1": 104.0,
        "tp2": 106.0,
        "resolved_price": resolved_price,
        "tp1_hit": int(status == "TP2"),
        "quality_score": 70.0,
        "active_leg_boxes": 2,
        "is_extended_move": 0,
        "raw_setup_json": "{}",
        "ambiguous_pessimistic_r": ambiguous_low,
        "ambiguous_optimistic_r": ambiguous_high,
    }


class AmbiguousMetricBoundsTests(TestCase):
    def test_ambiguous_has_no_arbitrary_zero_r(self):
        metrics = compute_trade_metrics(
            pd.DataFrame(
                [resolved_row("amb", "AMBIGUOUS", None, ambiguous_low=-1.0, ambiguous_high=2.0)]
            )
        )
        observed = metrics.iloc[0]
        self.assertTrue(pd.isna(observed["realized_r_multiple"]))
        self.assertTrue(pd.isna(observed["outcome_r_multiple_proxy"]))
        self.assertEqual(observed["ambiguous_pessimistic_r"], -1.0)
        self.assertEqual(observed["ambiguous_optimistic_r"], 2.0)

    def test_headline_denominator_includes_ambiguous_and_reports_bounds(self):
        metrics = compute_trade_metrics(
            pd.DataFrame(
                [
                    resolved_row("win", "TP2", 106.0),
                    resolved_row("loss", "STOPPED", 98.0),
                    resolved_row(
                        "amb",
                        "AMBIGUOUS",
                        None,
                        ambiguous_low=-1.0,
                        ambiguous_high=2.0,
                    ),
                ]
            )
        )
        summary = build_summary_activated_only(metrics)

        self.assertEqual(summary["ambiguous"], 1)
        self.assertEqual(summary["headline_outcome_rows"], 3)
        self.assertEqual(summary["ambiguous_bound_rows"], 1)
        self.assertAlmostEqual(summary["win_rate_pessimistic_bound"], 1 / 3)
        self.assertAlmostEqual(summary["win_rate_optimistic_bound"], 2 / 3)
        self.assertAlmostEqual(summary["total_realized_r_pessimistic_bound"], 1.0)
        self.assertAlmostEqual(summary["total_realized_r_optimistic_bound"], 4.0)
        self.assertAlmostEqual(summary["avg_realized_r_pessimistic_bound"], 1 / 3)
        self.assertAlmostEqual(summary["avg_realized_r_optimistic_bound"], 4 / 3)

    def test_empty_summary_exposes_bound_fields(self):
        summary = build_summary_activated_only(pd.DataFrame())
        for field in (
            "headline_outcome_rows",
            "ambiguous_bound_rows",
            "win_rate_pessimistic_bound",
            "win_rate_optimistic_bound",
            "avg_realized_r_pessimistic_bound",
            "avg_realized_r_optimistic_bound",
            "total_realized_r_pessimistic_bound",
            "total_realized_r_optimistic_bound",
        ):
            self.assertIn(field, summary)


if __name__ == "__main__":
    import unittest

    unittest.main()
