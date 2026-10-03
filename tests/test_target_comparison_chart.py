"""Offline checks for the two-metric chart; no Tk display is required."""
import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from research_v2 import backtest_workspace as workspace
from research_v2.target_comparison_chart import (
    TargetResult, bank_lines, draw_bank, draw_comparison, load_comparison, load_equity,
)
from tests.test_pole_causal_long_research import START, columns


class FakeCanvas:
    def __init__(self):
        self.bars = []
        self.labels = []
        self.lines = []

    def create_line(self, *args, **kwargs):
        self.lines.append((args, kwargs))

    def delete(self, *args, **kwargs):
        self.bars.clear()
        self.labels.clear()
        self.lines.clear()

    def create_oval(self, *args, **kwargs):
        pass

    def create_rectangle(self, *args, **kwargs):
        self.bars.append((args, kwargs))

    def create_text(self, *args, **kwargs):
        self.labels.append((args, kwargs))


class TargetComparisonTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        folder = Path(temp.name)
        cols, cands = folder / 'columns.csv', folder / 'candles.csv'
        with cols.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['symbol','profile_name','idx','kind','top','bottom','start_ts','end_ts'])
            for col in columns()[:6]:
                writer.writerow(['BTC','BTC_bs1_rev3',col.idx,col.kind,
                                 col.top,col.bottom,col.start_ts,col.end_ts])
        with cands.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['close_time','open','high','low','close'])
            for i in range(16):
                high = 106.5 if i == 7 else 108 if i == 8 else 101
                low = 101 if i in (7, 8) else 99
                opened = 102 if i in (7, 8) else 100
                writer.writerow([START+i*60_000, opened, high, low, opened])
        self.root = folder / 'results'
        job_file = self.root.with_suffix('.job.json')
        workspace.create_job(cols, cands, START, self.root, job_file,
                             START+16*60_000, True)
        workspace.run_job(job_file)

    def test_complete_pinned_run_loads_only_profit_and_drawdown(self):
        rows = load_comparison(self.root)
        self.assertEqual(len(rows), 9)
        self.assertEqual(rows[0], TargetResult(Decimal('2.5'), Decimal('2.5'), Decimal('0')))
        self.assertEqual(rows[-1].target, Decimal('10'))
        canvas = FakeCanvas()
        draw_comparison(canvas, rows)
        self.assertEqual(len(canvas.bars), 18)
        self.assertEqual(len([label for _, label in canvas.labels if str(label.get('text', '')).endswith('R')]), 9)
        self.assertLess(canvas.bars[0][0][1], 235)  # profit above zero
        self.assertGreaterEqual(canvas.bars[1][0][3], 235)  # drawdown below zero

    def test_tampered_report_and_manifest_fail_closed(self):
        report = self.root / 'target_sweep/comparison.csv'
        original = report.read_bytes()
        report.write_bytes(original + b'\n')
        with self.assertRaisesRegex(ValueError, 'provenance'):
            load_comparison(self.root)
        report.write_bytes(original)
        manifest = self.root / 'target_sweep/comparison_manifest.json'
        data = json.loads(manifest.read_text())
        data['targets_R'][-1] = 11
        manifest.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, 'provenance'):
            load_comparison(self.root)

    def test_negative_profit_draws_below_zero_and_rejects_invalid_numeric(self):
        canvas = FakeCanvas()
        draw_comparison(canvas, [TargetResult(Decimal('2.5'), Decimal('-5'), Decimal('20'))])
        self.assertEqual(canvas.bars[0][0][1], 235)
        self.assertGreater(canvas.bars[0][0][3], 235)
        report = self.root / 'target_sweep/comparison.csv'
        with report.open(newline='') as stream:
            rows = list(csv.DictReader(stream))
        rows[0]['max_drawdown_R'] = '-1'
        with report.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        manifest = self.root / 'target_sweep/comparison_manifest.json'
        data = json.loads(manifest.read_text())
        data['comparison_sha256'] = workspace._sha(report)
        manifest.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, 'target mismatch'):
            load_comparison(self.root)

    def test_bank_chart_single_and_all_targets_use_exit_sequence(self):
        summary = load_comparison(self.root)
        series = load_equity(self.root, summary)
        self.assertEqual(set(series), {item.target for item in summary})
        all_lines = bank_lines(series, None, Decimal('1000'), Decimal('10'))
        self.assertEqual(len(all_lines), 9)
        self.assertEqual(all_lines[Decimal('2.5')][-1][1], Decimal('1025'))
        chosen = bank_lines(series, Decimal('2.5'), Decimal('1000'), Decimal('10'))
        self.assertEqual(len(chosen), 1)
        canvas = FakeCanvas()
        draw_bank(canvas, all_lines, Decimal('1000'), {target: '#123456' for target in series})
        self.assertTrue(canvas.labels)
        with self.assertRaises(ValueError):
            bank_lines(series, None, Decimal('1000'), Decimal('1001'))

    def test_bank_chart_rejects_altered_ledger_and_wrong_manifest(self):
        summary = load_comparison(self.root)
        ledger = self.root / 'portfolio/portfolio_reality_equity_curve.csv'
        original = ledger.read_bytes()
        ledger.write_bytes(original.replace(b'2.5', b'9.5'))
        with self.assertRaisesRegex(ValueError, 'equity'):
            load_equity(self.root, summary)
        ledger.write_bytes(original)
        report = self.root / 'target_sweep/comparison.csv'
        original = report.read_bytes()
        report.write_bytes(original.replace(b'portfolio_manifest_sha256', b'other_manifest_sha256'))
        manifest = self.root / 'target_sweep/comparison_manifest.json'
        data = json.loads(manifest.read_text())
        data['comparison_sha256'] = workspace._sha(report)
        manifest.write_text(json.dumps(data))
        with self.assertRaises((ValueError, KeyError)):
            load_equity(self.root, load_comparison(self.root))


if __name__ == '__main__':
    unittest.main()
