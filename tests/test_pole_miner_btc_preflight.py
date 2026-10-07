import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from research_v2.patterns.pole_miner_btc_preflight import inspect


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.decisions = self.root / "causal_decisions.csv"
        self.trades = self.root / "trades.csv"
        self.causal = self.root / "causal_manifest.json"
        self.portfolio = self.root / "portfolio_manifest.json"
        self.output = self.root / "preflight.json"
        with self.decisions.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(("decision_id", "pole_column_index", "reversal_column_index",
                        "confirmation_column_index", "decision_known_at_ms",
                        "entry_candle_open_ms", "entry_candle_close_ms", "direction",
                        "entry_price", "stop_price"))
            for n in range(1, 4):
                w.writerow((f"DEC-{n:06d}", n, n+1, n+2, 1700000000000+n*60000,
                            1700000060000+n*60000, 1700000119999+n*60000,
                            "LONG", "100", "99"))
        with self.trades.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(("trade_id", "opportunity_id", "symbol", "direction",
                        "entry_timestamp", "exit_timestamp", "result_R", "classification"))
            w.writerow(("TRADE-000001", "OPP-000002", "BTC", "LONG",
                        1700000240000, 1700000300000, "2.5", "TARGET_FIRST"))
        self.causal.write_text(json.dumps({"stage": "causal_long_pole_research_v1",
            "research_only": True, "symbol": "BTCUSDT", "decision_count": 3,
            "resolved_portfolio_trades": 1, "gross_total_R": 2.5,
            "causal_decisions_sha256": self.sha(self.decisions)}))
        self.portfolio.write_text(json.dumps({"resolved_portfolio_trades": 1,
            "summary_metrics": {"total_R": 2.5}}))

    @staticmethod
    def sha(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def run_check(self):
        return inspect(self.decisions, self.causal, self.trades, self.portfolio,
                       self.output, expected_hashes=tuple(self.sha(p) for p in
                       (self.decisions, self.causal, self.trades, self.portfolio)),
                       expected_counts=(3, 1))

    def test_valid_linkage_is_gross_only(self):
        report = self.run_check()
        self.assertEqual(report["linked_trades"], 1)
        self.assertEqual(report["status"], "GROSS_ONLY_NOT_MINER_INPUT")
        self.assertEqual(report["gross_total_R"], "2.5")
        self.assertEqual(json.loads(self.output.read_text()), report)

    def test_hash_mismatch_and_unlinked_trade_fail_without_report(self):
        with self.assertRaisesRegex(ValueError, "hash"):
            inspect(self.decisions, self.causal, self.trades, self.portfolio,
                    self.output, expected_hashes=("0"*64,)*4, expected_counts=(3, 1))
        self.assertFalse(self.output.exists())
        raw = self.trades.read_text().replace("OPP-000002", "OPP-000004")
        self.trades.write_text(raw)
        with self.assertRaisesRegex(ValueError, "identity"):
            self.run_check()

    def test_temporal_violation_and_duplicate_fail(self):
        raw = self.trades.read_text().replace("1700000240000", "1700000000000")
        self.trades.write_text(raw)
        with self.assertRaisesRegex(ValueError, "chronology"):
            self.run_check()
        self.trades.write_text(raw.replace("1700000000000", "1700000240000"))
        self.trades.write_text(self.trades.read_text() + self.trades.read_text().splitlines()[-1] + "\n")
        cm = json.loads(self.causal.read_text())
        cm["resolved_portfolio_trades"] = 2
        self.causal.write_text(json.dumps(cm))
        self.portfolio.write_text(json.dumps({"resolved_portfolio_trades": 2,
            "summary_metrics": {"total_R": 5}}))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            inspect(self.decisions, self.causal, self.trades, self.portfolio,
                    self.output, expected_hashes=tuple(self.sha(p) for p in
                    (self.decisions, self.causal, self.trades, self.portfolio)),
                    expected_counts=(3, 2))

    def test_manifest_mismatch_and_existing_output_fail(self):
        self.portfolio.write_text(json.dumps({"resolved_portfolio_trades": 1,
            "summary_metrics": {"total_R": 10}}))
        with self.assertRaisesRegex(ValueError, "gross"):
            self.run_check()
        self.portfolio.write_text(json.dumps({"resolved_portfolio_trades": 1,
            "summary_metrics": {"total_R": 2.5}}))
        self.output.write_bytes(b"existing")
        with self.assertRaises(FileExistsError):
            self.run_check()
        self.assertEqual(self.output.read_bytes(), b"existing")

    def test_conservative_same_candle_stop_is_legal_only_with_minus_one_r(self):
        self.trades.write_text(self.trades.read_text().replace(
            "1700000300000,2.5,TARGET_FIRST",
            "1700000240000,-1,SAME_CANDLE_FILL_STOP_CONSERVATIVE"))
        cm = json.loads(self.causal.read_text())
        cm["gross_total_R"] = -1
        self.causal.write_text(json.dumps(cm))
        self.portfolio.write_text(json.dumps({"resolved_portfolio_trades": 1,
            "summary_metrics": {"total_R": -1}}))
        self.assertEqual(self.run_check()["same_candle_conservative_stops"], 1)
        self.output.unlink()
        self.trades.write_text(self.trades.read_text().replace("SAME_CANDLE_FILL_STOP_CONSERVATIVE", "STOP_FIRST"))
        with self.assertRaisesRegex(ValueError, "chronology"):
            self.run_check()


if __name__ == "__main__":
    unittest.main()
