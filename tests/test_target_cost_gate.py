"""Offline checks for the one bounded research cost stress."""
import csv
import json
import unittest

from research_v2 import backtest_workspace as workspace
from research_v2 import target_cost_gate
from tests import test_backtest_workspace as workspace_tests


class CostGateTests(unittest.TestCase):
    def setUp(self):
        self.fixture = workspace_tests.WorkspaceTests(methodName="test_end_to_end_and_no_overwrite")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.job["schema"] = workspace.SWEEP_SCHEMA
        self.fixture.job["maximum_entry_ts"] = self.fixture.job["minimum_entry_ts"] + 16 * 60_000
        self.fixture.job["target_sweep"] = True
        self.fixture.write()
        (self.fixture.root / "results.job.json").write_bytes(self.fixture.path.read_bytes())
        workspace.run_job(self.fixture.path)
        self.root = self.fixture.root / "results"

    def test_all_targets_and_exact_notional_sensitive_cost(self):
        rows = target_cost_gate.analyze(self.root)
        self.assertEqual(len(rows), 27)
        base, cost = rows[:2]
        self.assertEqual(base["target_R"], "2.5")
        self.assertEqual(base["gross_R"], "2.5")
        self.assertEqual(cost["cost_bps_per_side"], "10")
        with (self.root / "causal_decisions.csv").open(newline="") as stream:
            decision = next(csv.DictReader(stream))
        from decimal import Decimal
        entry, stop = Decimal(decision["entry_price"]), Decimal(decision["stop_price"])
        expected = 2 * Decimal("10") / 10000 * entry / (entry - stop)
        self.assertLess(abs(Decimal(cost["cost_R"]) - expected), Decimal("0.00000001"))
        self.assertLess(abs(Decimal(cost["adjusted_R"]) - (Decimal("2.5") - expected)),
                        Decimal("0.00000001"))
        self.assertEqual(rows[2]["cost_bps_per_side"], "20")
        self.assertEqual(rows[2]["quarters_with_trades"], "1")
        self.assertEqual(sum((Decimal(value) for value in
                              json.loads(cost["quarterly_adjusted_R"]).values()), Decimal(0)),
                         Decimal(cost["adjusted_R"]))

    def test_changed_trade_evidence_fails_closed(self):
        trade = self.root / "portfolio/portfolio_reality_trade_sequence.csv"
        trade.write_text(trade.read_text().replace("OPP-000001", "OPP-999999"))
        with self.assertRaisesRegex(ValueError, "missing causal decision"):
            target_cost_gate.analyze(self.root)

    def test_changed_manifest_and_decision_hash_fail_closed(self):
        manifest = self.root / "portfolio/portfolio_reality_manifest.json"
        manifest.write_text(manifest.read_text() + " ")
        with self.assertRaisesRegex(ValueError, "manifest hash"):
            target_cost_gate.analyze(self.root)
        manifest.write_text(manifest.read_text()[:-1])
        decisions = self.root / "causal_decisions.csv"
        decisions.write_text(decisions.read_text() + " ")
        with self.assertRaisesRegex(ValueError, "decision ledger hash"):
            target_cost_gate.analyze(self.root)


if __name__ == "__main__":
    unittest.main()
