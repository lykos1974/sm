import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from research_v2.patterns.pole_miner_btc_cost_scenario import evaluate
from research_v2.patterns.pole_miner_btc_preflight import _sha


class ScenarioTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.decisions = self.root / "decisions.csv"
        self.trades = self.root / "trades.csv"
        self.preflight = self.root / "preflight.json"
        self.output = self.root / "scenario.json"
        with self.decisions.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(("decision_id", "pole_column_index", "reversal_column_index",
                        "confirmation_column_index", "decision_known_at_ms",
                        "entry_candle_open_ms", "entry_candle_close_ms", "direction",
                        "entry_price", "stop_price"))
            w.writerow(("DEC-000001", 1, 2, 3, 1700000000000, 1700000060000,
                        1700000119999, "LONG", "100", "99"))
        with self.trades.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(("trade_id", "opportunity_id", "symbol", "direction",
                        "entry_timestamp", "exit_timestamp", "result_R", "classification"))
            w.writerow(("TRADE-000001", "OPP-000001", "BTC", "LONG",
                        1700000120000, 1700000180000, "2.5", "TARGET_FIRST"))
        self.preflight.write_text(json.dumps({"schema": "pole-miner-btc-preflight-v1",
            "status": "GROSS_ONLY_NOT_MINER_INPUT", "decisions": 1,
            "linked_trades": 1, "gross_total_R": "2.5",
            "source_sha256": {"decisions": _sha(self.decisions), "trades": _sha(self.trades)}}))

    def test_exact_scenario_math_and_non_net_label(self):
        report = evaluate(self.decisions, self.trades, self.preflight,
                          self.output, bps_grid=(Decimal(0), Decimal(10)),
                          expected_counts=(1, 1),
                          expected_hashes=(_sha(self.decisions), _sha(self.trades)))
        self.assertEqual(report["scenarios"][0]["modeled_total_R"], "2.5")
        self.assertEqual(report["scenarios"][1]["modeled_total_R"], "2.3")
        self.assertEqual(report["break_even_bps_per_side"], "125")
        self.assertEqual(report["status"], "ILLUSTRATIVE_COST_ONLY")
        self.assertEqual(json.loads(self.output.read_text()), report)

    def test_tamper_and_duplicate_output_fail_closed(self):
        self.decisions.write_text(self.decisions.read_text() + "tamper")
        with self.assertRaisesRegex(ValueError, "hash"):
            evaluate(self.decisions, self.trades, self.preflight, self.output,
                     expected_counts=(1, 1),
                     expected_hashes=(self.preflight_hash("decisions"), self.preflight_hash("trades")))
        self.assertFalse(self.output.exists())

    def test_invalid_scenario_inputs_and_existing_output(self):
        with self.assertRaisesRegex(ValueError, "cost"):
            evaluate(self.decisions, self.trades, self.preflight, self.output,
                     bps_grid=(Decimal("-1"),), expected_counts=(1, 1),
                     expected_hashes=(_sha(self.decisions), _sha(self.trades)))
        self.output.write_bytes(b"existing")
        with self.assertRaises(FileExistsError):
            evaluate(self.decisions, self.trades, self.preflight, self.output,
                     expected_counts=(1, 1),
                     expected_hashes=(_sha(self.decisions), _sha(self.trades)))
        self.assertEqual(self.output.read_bytes(), b"existing")

    def preflight_hash(self, key):
        return json.loads(self.preflight.read_text())["source_sha256"][key]


if __name__ == "__main__":
    unittest.main()
