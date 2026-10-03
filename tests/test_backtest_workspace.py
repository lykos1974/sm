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
from research_v2 import trade_chart
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

    def test_swapped_csv_fields_fail_before_job_or_output(self):
        with self.assertRaisesRegex(ValueError, "wrong columns CSV format"):
            workspace.create_job(self.cands, self.cols, START,
                                 self.root / "results", self.path)
        self.assertFalse(self.path.exists())
        self.assertFalse((self.root / "results").exists())
        self.job["columns_csv"] = str(self.cands)
        self.job["columns_sha256"] = workspace._sha(self.cands)
        self.job["candles_csv"] = str(self.cols)
        self.job["candles_sha256"] = workspace._sha(self.cols)
        self.write()
        with self.assertRaisesRegex(ValueError, "wrong columns CSV format"):
            workspace.run_job(self.path)
        self.assertFalse((self.root / "results").exists())

    def test_output_picker_proposes_new_child_below_existing_parent(self):
        proposed = workspace.suggest_output_directory(self.root)
        self.assertEqual(proposed.parent, self.root)
        self.assertFalse(proposed.exists())
        self.assertFalse(proposed.with_suffix(".job.json").exists())
        self.assertNotEqual(proposed, workspace.suggest_output_directory(self.root))

    def test_one_folder_reference_workflow_rejects_wrong_bytes_and_runs(self):
        dataset = self.root / "frozen" / "results"
        dataset.mkdir(parents=True)
        self.cols.rename(dataset / "columns.csv")
        self.cands.rename(dataset / "candles_1m.csv")
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            workspace.prepare_btc_2024_job(dataset)
        self.assertEqual(list(self.root.glob("*.job.json")), [])
        with patch.object(workspace, "BTC_2024_COLUMNS_SHA", workspace._sha(dataset / "columns.csv")), \
             patch.object(workspace, "BTC_2024_CANDLES_SHA", workspace._sha(dataset / "candles_1m.csv")), \
             patch.object(workspace, "BTC_2024_MINIMUM_ENTRY_TS", START):
            job_file = workspace.prepare_btc_2024_job(dataset)
        job = workspace.load_job(job_file)
        self.assertEqual(Path(job["columns_csv"]).name, "columns.csv")
        self.assertEqual(Path(job["candles_csv"]).name, "candles_1m.csv")
        self.assertEqual(Path(job["output_root"]).parent, self.root)
        self.assertEqual(workspace.run_job(job_file)["result"]["decision_count"], 1)

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

    def test_chart_links_real_trade_to_causal_decision_and_closed_candles(self):
        self.write()
        workspace.run_job(self.path)
        root = self.root / "results"
        trades = trade_chart.load_trades(root)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].opportunity_id, "OPP-000001")
        self.assertEqual(trades[0].entry - trades[0].stop, 3)
        self.assertEqual(trades[0].target - trades[0].entry, 7.5)
        times, candles = trade_chart.load_candles(self.cands)
        around, marker = trade_chart.chart_window(times, candles, trades[0], "entry", 2)
        self.assertEqual(around[marker].close_ms, trades[0].entry_ms)
        around, marker = trade_chart.chart_window(times, candles, trades[0], "exit", 2)
        self.assertEqual(around[marker].close_ms, trades[0].exit_ms)

    def test_chart_rejects_unfinished_or_changed_decision_evidence(self):
        job_file = self.root / "results.job.json"
        workspace.create_job(self.cols, self.cands, START, self.root / "results", job_file)
        workspace.run_job(job_file)
        root = self.root / "results"
        with patch.object(trade_chart, "BTC_2024_COLUMNS_SHA", workspace._sha(self.cols)), \
             patch.object(trade_chart, "BTC_2024_CANDLES_SHA", workspace._sha(self.cands)):
            self.assertEqual(trade_chart.completed_run_inputs(root), self.cands)
            ledger = root / "causal_decisions.csv"
            ledger.write_bytes(ledger.read_bytes() + b"\n")
            with self.assertRaisesRegex(ValueError, "provenance"):
                trade_chart.completed_run_inputs(root)

    def test_pnf_view_maps_entry_and_exit_to_frozen_columns(self):
        self.write()
        workspace.run_job(self.path)
        trade = trade_chart.load_trades(self.root / "results")[0]
        starts, pnf, box = trade_chart.load_pnf_columns(self.cols)
        self.assertEqual(box, 1)
        around, entry, exit = trade_chart.pnf_window(starts, pnf, trade, "entry", 2)
        self.assertEqual(around[entry].idx, 5)
        self.assertTrue(0 <= exit < len(around))
        self.assertEqual(around[exit].kind, "O")
        around, entry, exit = trade_chart.pnf_window(starts, pnf, trade, "exit", 1)
        self.assertEqual(around[exit].idx, 5)
        self.assertEqual(around[entry].idx, 5)

    def test_pnf_view_rejects_changed_profile_and_unmappable_event(self):
        starts, pnf, box = trade_chart.load_pnf_columns(self.cols)
        self.assertEqual(len(pnf), 6)
        trade = trade_chart.Trade("T", "O", START - 1, START, "STOP_FIRST", -1,
                                  100, 99, 102.5)
        with self.assertRaisesRegex(ValueError, "precedes"):
            trade_chart.pnf_window(starts, pnf, trade, "entry")
        data = self.cols.read_text()
        self.cols.write_text(data.replace("BTC_bs1_rev3", "BTC_bs1_rev4", 1))
        with self.assertRaisesRegex(ValueError, "profile"):
            trade_chart.load_pnf_columns(self.cols)

    def test_pnf_input_is_pinned_to_the_completed_job(self):
        job_file = self.root / "results.job.json"
        workspace.create_job(self.cols, self.cands, START, self.root / "results", job_file)
        workspace.run_job(job_file)
        with patch.object(trade_chart, "BTC_2024_COLUMNS_SHA", workspace._sha(self.cols)), \
             patch.object(trade_chart, "BTC_2024_CANDLES_SHA", workspace._sha(self.cands)):
            self.assertEqual(trade_chart.completed_run_sources(self.root / "results"),
                             (self.cols, self.cands))
            self.cols.write_bytes(self.cols.read_bytes() + b"\n")
            with self.assertRaisesRegex(ValueError, "frozen input"):
                trade_chart.completed_run_sources(self.root / "results")

    def test_trade_conditions_replay_only_recorded_decision_and_fill(self):
        self.write()
        workspace.run_job(self.path)
        root = self.root / "results"
        trade = trade_chart.load_trades(root)[0]
        _, pnf, box = trade_chart.load_pnf_columns(self.cols)
        times, candles = trade_chart.load_candles(self.cands)
        rows = trade_chart.build_trade_explanations(root, [trade], pnf, box,
                                                     times, candles)[trade.trade_id]
        self.assertEqual(rows[0], ("Στρατηγική", "LOW_POLE LONG", "γνωστό στο σήμα"))
        self.assertIn(("Μοτίβο PnF", "O → X → O", "γνωστό στο σήμα"), rows)
        self.assertIn(("Pole", "20 boxes", "> 5"), rows)
        self.assertTrue(any(name == "Εκτέλεση limit" and "1/3" in value
                            for name, value, _ in rows))
        self.assertTrue(any(name == "Έξοδος" and value == "TARGET_FIRST, +2.5 R"
                            for name, value, _ in rows))
        self.assertTrue(any(name == "Target έξοδος" and value ==
                            trade_chart._utc(trade.exit_ms) for name, value, _ in rows))
        self.assertTrue(any(name == "Κερί εξόδου H/L" for name, _, _ in rows))

    def test_trade_conditions_reject_mismatched_decision_or_fill_evidence(self):
        self.write()
        workspace.run_job(self.path)
        root = self.root / "results"
        trade = trade_chart.load_trades(root)[0]
        _, pnf, box = trade_chart.load_pnf_columns(self.cols)
        times, candles = trade_chart.load_candles(self.cands)
        ledger = root / "causal_decisions.csv"
        source = ledger.read_text()
        ledger.write_text(source.replace(",LONG,", ",SHORT,", 1))
        with self.assertRaises(ValueError):
            trade_chart.build_trade_explanations(root, [trade], pnf, box,
                                                 times, candles)
        ledger.write_text(source)
        wrong = trade_chart.Trade(trade.trade_id, trade.opportunity_id,
                                  trade.entry_ms - 60_000, trade.exit_ms,
                                  trade.classification, trade.result_r, trade.entry,
                                  trade.stop, trade.target)
        with self.assertRaisesRegex(ValueError, "fill"):
            trade_chart.build_trade_explanations(root, [wrong], pnf, box,
                                                 times, candles)

    def test_exit_timeline_shows_be_arm_then_later_exit(self):
        first = START
        rows = [trade_chart.Candle(first, 100, 101, 99, 100),
                trade_chart.Candle(first+60_000, 102, 106.5, 101, 104),
                trade_chart.Candle(first+120_000, 104, 106, 99.5, 100)]
        trade = trade_chart.Trade("T", "O", first, first+120_000,
                                  "BREAK_EVEN_EXIT", 0, 100, 97, 107.5)
        evidence = trade_chart.exit_timeline(trade, rows, 2.0)
        self.assertIn(("BE ενεργό", trade_chart._utc(first+60_000), "high ≥ 106"), evidence)
        self.assertIn(("BE έξοδος", trade_chart._utc(first+120_000), "low ≤ 100"), evidence)

    def test_exit_timeline_target_stop_fill_candle_and_tampering(self):
        first = START
        fill = trade_chart.Candle(first, 100, 101, 99, 100)
        target = trade_chart.Candle(first+60_000, 101, 108, 100.5, 107)
        win = trade_chart.Trade("T", "O", first, first+60_000,
                                "TARGET_FIRST", 2.5, 100, 97, 107.5)
        self.assertIn(("Target έξοδος", trade_chart._utc(first+60_000), "high ≥ 107.5"),
                      trade_chart.exit_timeline(win, [fill, target], 2.0))
        stop = trade_chart.Candle(first+60_000, 100, 101, 96, 97)
        loss = trade_chart.Trade("T", "O", first, first+60_000,
                                 "STOP_FIRST", -1, 100, 97, 107.5)
        self.assertIn(("Stop έξοδος", trade_chart._utc(first+60_000), "low ≤ 97"),
                      trade_chart.exit_timeline(loss, [fill, stop], 2.0))
        conservative = trade_chart.Trade("T", "O", first, first,
                                         "SAME_CANDLE_FILL_STOP_CONSERVATIVE", -1,
                                         100, 97, 107.5)
        self.assertTrue(any(name == "Fill + stop ίδιο κερί" for name, _, _ in
                            trade_chart.exit_timeline(conservative,
                                [trade_chart.Candle(first, 100, 101, 96, 100)], 2.0)))
        with self.assertRaisesRegex(ValueError, "outcome"):
            trade_chart.exit_timeline(win, [fill, stop], 2.0)
        with self.assertRaisesRegex(ValueError, "chronology"):
            trade_chart.exit_timeline(win, [fill, target,
                trade_chart.Candle(first+120_000, 107, 108, 106, 107)], 2.0)

    def test_exit_timeline_rejects_same_candle_be_ambiguity(self):
        first = START
        fill = trade_chart.Candle(first, 100, 101, 99, 100)
        ambiguous = trade_chart.Candle(first+60_000, 100, 106.5, 99, 105)
        claimed = trade_chart.Trade("T", "O", first, first+60_000,
                                    "BREAK_EVEN_EXIT", 0, 100, 97, 107.5)
        with self.assertRaisesRegex(ValueError, "ambiguity"):
            trade_chart.exit_timeline(claimed, [fill, ambiguous], 2.0)

    def test_exit_timeline_rejects_prior_target_before_claimed_be(self):
        first = START
        fill = trade_chart.Candle(first, 100, 101, 99, 100)
        target = trade_chart.Candle(first+60_000, 101, 108, 100.5, 107)
        later_be = trade_chart.Candle(first+120_000, 101, 102, 99, 100)
        claimed = trade_chart.Trade("T", "O", first, first+120_000,
                                    "BREAK_EVEN_EXIT", 0, 100, 97, 107.5)
        with self.assertRaisesRegex(ValueError, "outcome"):
            trade_chart.exit_timeline(claimed, [fill, target, later_be], 2.0)

    def test_trade_navigation_forward_back_and_boundaries(self):
        ids = ["TRADE-000001", "TRADE-000002", "TRADE-000003"]
        self.assertEqual(trade_chart.adjacent_trade_id(ids, ids[0], -1), ids[0])
        self.assertEqual(trade_chart.adjacent_trade_id(ids, ids[0], 1), ids[1])
        self.assertEqual(trade_chart.adjacent_trade_id(ids, ids[1], 1), ids[2])
        self.assertEqual(trade_chart.adjacent_trade_id(ids, ids[2], -1), ids[1])
        self.assertEqual(trade_chart.adjacent_trade_id(ids, ids[2], 1), ids[2])
        with self.assertRaises(ValueError):
            trade_chart.adjacent_trade_id(ids, "UNKNOWN", 1)
        with self.assertRaises(ValueError):
            trade_chart.adjacent_trade_id(ids, ids[1], 2)


if __name__ == "__main__":
    unittest.main()
