import sys
import sqlite3
import tempfile
from pathlib import Path
from unittest import TestCase

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
PNF_MVP_ROOT = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_ROOT))

from strategy_evaluator import accounting_breakdown, basic_stats, outcome_breakdown  # noqa: E402
from strategy_trade_export import (  # noqa: E402
    build_accounting_breakdown,
    build_accounting_summary,
    build_diagnostics_export,
    compute_trade_metrics,
    load_validation_rows,
)


def row(setup_id, status, side="LONG", **overrides):
    base = {
        "setup_id": setup_id,
        "created_ts": int(setup_id.strip("r") or 0),
        "updated_ts": 10,
        "reference_ts": 1,
        "resolved_ts": 10,
        "symbol": "BTCUSDT",
        "strategy": "pullback_retest",
        "side": side,
        "status": "CANDIDATE",
        "activation_status": "ACTIVE",
        "resolution_status": status,
        "ideal_entry": 100.0,
        "activated_price": 100.0,
        "invalidation": 98.0 if side == "LONG" else 102.0,
        "tp1": 103.0 if side == "LONG" else 97.0,
        "tp2": 105.0 if side == "LONG" else 95.0,
        "tp1_hit": 0,
        "tp1_price": None,
        "resolved_price": None,
        "ambiguous_pessimistic_r": None,
        "ambiguous_optimistic_r": None,
        "quality_score": 70.0,
        "active_leg_boxes": 2,
        "is_extended_move": 0,
        "raw_setup_json": "{}",
    }
    base.update(overrides)
    return base


class AccountingInvariantTests(TestCase):
    def _frame(self):
        rows = [
            row("r1", "TP2", tp1_hit=1, tp1_price=103.0, resolved_price=105.0),
            row(
                "r2",
                "TP1_PARTIAL_THEN_BE",
                tp1_hit=1,
                tp1_price=103.0,
                resolved_price=100.02,
            ),
            row("r3", "STOPPED", side="SHORT", resolved_price=102.0),
            row(
                "r4",
                "AMBIGUOUS",
                ambiguous_pessimistic_r=-0.5,
                ambiguous_optimistic_r=2.0,
            ),
            row("r5", "PENDING", resolved_ts=None, activated_price=None, activation_status="PENDING"),
            row("r6", "EXPIRED", resolved_price=None, activation_status="PENDING"),
        ]
        return compute_trade_metrics(pd.DataFrame(rows))

    def test_actual_rr_partial_be_and_no_tp2_proxy(self):
        metrics = self._frame().set_index("setup_id")
        self.assertAlmostEqual(metrics.loc["r1", "realized_r_multiple"], 2.0)
        self.assertAlmostEqual(metrics.loc["r2", "realized_r_multiple"], 0.755)
        self.assertAlmostEqual(metrics.loc["r3", "realized_r_multiple"], -1.0)
        self.assertNotIn("outcome_r_multiple_proxy", metrics.columns)

    def test_headline_counts_and_bound_totals_reconcile(self):
        metrics = self._frame()
        summary = build_accounting_summary(metrics)
        self.assertEqual(summary["registered_rows"], 6)
        self.assertEqual(summary["resolved_rows"], 3)
        self.assertEqual(summary["ambiguous_branched_rows"], 1)
        self.assertEqual(summary["pending_rows"], 1)
        self.assertEqual(summary["expired_rows"], 0)
        self.assertEqual(summary["missed_rows"], 1)
        self.assertEqual(summary["headline_denominator_rows"], 4)
        self.assertAlmostEqual(summary["total_r_pessimistic"], 1.255)
        self.assertAlmostEqual(summary["total_r_optimistic"], 3.755)
        self.assertAlmostEqual(summary["expectancy_r_pessimistic"], 1.255 / 4)
        self.assertAlmostEqual(summary["expectancy_r_optimistic"], 3.755 / 4)
        self.assertAlmostEqual(summary["win_rate_pessimistic"], 0.5)
        self.assertAlmostEqual(summary["win_rate_optimistic"], 0.75)
        self.assertAlmostEqual(summary["max_drawdown_r_pessimistic"], 1.5)
        self.assertEqual(
            summary["registered_rows"],
            summary["resolved_rows"]
            + summary["ambiguous_branched_rows"]
            + summary["pending_rows"]
            + summary["expired_rows"]
            + summary["missed_rows"],
        )

    def test_grouped_export_and_evaluator_use_identical_denominators_and_r(self):
        metrics = self._frame()
        exported = build_accounting_breakdown(metrics, "side").sort_values("side").reset_index(drop=True)
        evaluated = accounting_breakdown(metrics, "side").sort_values("side").reset_index(drop=True)
        pd.testing.assert_frame_equal(exported, evaluated, check_dtype=False)
        self.assertEqual(exported["registered_rows"].sum(), 6)
        self.assertEqual(exported["headline_denominator_rows"].sum(), 4)
        self.assertAlmostEqual(exported["total_r_pessimistic"].sum(), 1.255)
        self.assertAlmostEqual(exported["total_r_optimistic"].sum(), 3.755)

    def test_every_diagnostic_section_reconciles_to_the_headline(self):
        metrics = self._frame()
        headline = build_accounting_summary(metrics)
        diagnostics = build_diagnostics_export(metrics)
        for section, grouped in diagnostics.groupby("section"):
            with self.subTest(section=section):
                self.assertEqual(grouped["registered_rows"].sum(), headline["registered_rows"])
                self.assertEqual(
                    grouped["headline_denominator_rows"].sum(),
                    headline["headline_denominator_rows"],
                )
                self.assertAlmostEqual(
                    grouped["total_r_pessimistic"].sum(),
                    headline["total_r_pessimistic"],
                )
                self.assertAlmostEqual(
                    grouped["total_r_optimistic"].sum(),
                    headline["total_r_optimistic"],
                )

    def test_evaluator_headline_and_outcome_groups_use_same_accounting(self):
        metrics = self._frame()
        self.assertEqual(basic_stats(metrics), build_accounting_summary(metrics))
        evaluated = outcome_breakdown(metrics, "side").reset_index()
        exported = build_accounting_breakdown(metrics, "side")
        pd.testing.assert_frame_equal(evaluated, exported, check_dtype=False)

    def test_export_loader_keeps_pending_expired_missed_and_ambiguous_rows(self):
        source = pd.DataFrame(
            [
                row("r1", "TP2", tp1_hit=1, tp1_price=103.0, resolved_price=105.0),
                row("r2", "AMBIGUOUS", ambiguous_pessimistic_r=-1.0, ambiguous_optimistic_r=2.0),
                row("r3", "PENDING", resolved_ts=None, activation_status="PENDING"),
                row("r4", "EXPIRED", activation_status="PENDING"),
            ]
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.db"
            with sqlite3.connect(db_path) as conn:
                source.to_sql("strategy_setups", conn, index=False)
            loaded = load_validation_rows(str(db_path))
        self.assertEqual(set(loaded["resolution_status"]), {"TP2", "AMBIGUOUS", "PENDING", "EXPIRED"})
        self.assertEqual(len(loaded), 4)


if __name__ == "__main__":
    import unittest

    unittest.main()
