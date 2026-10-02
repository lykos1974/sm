"""Offline research job contract, using synthetic frozen CSV fixtures."""
import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_v2 import backtest_workspace as workspace
from tests.test_pole_causal_long_research import START, candles, columns


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cols = self.root / "columns.csv"
        self.cands = self.root / "candles.csv"
        with self.cols.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["symbol", "profile_name", "idx", "kind", "top", "bottom", "start_ts", "end_ts"])
            for col in columns()[:6]:
                writer.writerow(["BTC", "BTC_bs1_rev3", col.idx, col.kind,
                                 col.top, col.bottom, col.start_ts, col.end_ts])
        with self.cands.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["close_time", "open", "high", "low", "close"])
            for i in range(16):
                high = 106.5 if i == 7 else 108 if i == 8 else 101
                low = 101 if i in (7, 8) else 99
                opened = 102 if i in (7, 8) else 100
                writer.writerow([START + i * 60_000, opened, high, low, opened])
        self.job = {"schema": workspace.SCHEMA, "strategy_id": "causal_long_pole",
                    "columns_csv": str(self.cols), "columns_sha256": workspace._sha(self.cols),
                    "candles_csv": str(self.cands), "candles_sha256": workspace._sha(self.cands),
                    "minimum_entry_ts": START, "output_root": str(self.root / "results")}
        self.path = self.root / "job.json"

    def write(self):
        self.path.write_text(json.dumps(self.job), encoding="utf-8")

    def test_end_to_end_and_no_overwrite(self):
        self.write()
        report = workspace.run_job(self.path)
        self.assertEqual(report["result"]["resolved_portfolio_trades"], 1)
        self.assertEqual(report["result"]["gross_total_R"], 2.5)
        manifest = json.loads((self.root / "results/research_job_manifest.json").read_text())
        self.assertEqual(manifest["job_sha256"], workspace._sha(self.path))
        with self.assertRaises(FileExistsError):
            workspace.run_job(self.path)

    def test_gui_job_creation_and_cli_use_same_runner(self):
        job = workspace.create_job(self.cols, self.cands, START,
                                   self.root / "results", self.path)
        self.assertEqual(workspace.load_job(self.path), job)
        command = [sys.executable, "-B", "-m", "research_v2.backtest_workspace",
                   "--job", str(self.path)]
        completed = subprocess.run(command, cwd=Path(__file__).resolve().parent.parent,
                                   text=True, capture_output=True, check=True)
        self.assertEqual(json.loads(completed.stdout)["result"]["decision_count"], 1)
        self.assertTrue((self.root / "results/research_job_manifest.json").is_file())

    def test_unreviewed_strategy_and_modified_input_cannot_run(self):
        self.job["strategy_id"] = "bullish_triangle"
        self.write()
        with self.assertRaises(ValueError):
            workspace.run_job(self.path)
        self.job["strategy_id"] = "causal_long_pole"
        self.write()
        self.cands.write_bytes(self.cands.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            workspace.run_job(self.path)
        self.assertFalse((self.root / "results").exists())

    def test_duplicate_fields_relative_paths_and_bool_timestamp_fail_closed(self):
        self.path.write_text('{"schema":"research-backtest-job-v1","schema":"research-backtest-job-v1"}')
        with self.assertRaises(ValueError):
            workspace.load_job(self.path)
        self.job["columns_csv"] = "local.csv"
        self.write()
        with self.assertRaises(ValueError):
            workspace.load_job(self.path)
        self.job["columns_csv"] = str(self.cols)
        self.job["minimum_entry_ts"] = True
        self.write()
        with self.assertRaises(ValueError):
            workspace.load_job(self.path)

    def test_no_operational_import_before_frozen_checks(self):
        self.job["candles_sha256"] = "0" * 64
        self.write()
        with patch("builtins.__import__", wraps=__import__) as importer:
            with self.assertRaises(ValueError):
                workspace.run_job(self.path)
        self.assertFalse(any(call.args[0].startswith("pnf_mvp.app") for call in importer.call_args_list))
        self.assertFalse((self.root / "results").exists())


if __name__ == "__main__":
    unittest.main()
