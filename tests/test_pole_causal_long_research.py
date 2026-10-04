"""Causal chronology checks for research-only LONG pole signals."""
import tempfile
import csv
import json
import hashlib
import unittest
from pathlib import Path
from unittest.mock import patch

from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle
from research_v2.patterns.pole_core_motif_sl_c_candle_chronology import TimedColumn
from research_v2.patterns import pole_causal_long_research as causal


START = 1_704_067_200_000


def columns():
    geometry = [('X', 110, 100), ('O', 110, 100), ('X', 110, 100),
                ('O', 109, 90), ('X', 105, 91), ('O', 104, 95),
                ('X', 120, 100), ('O', 119, 105)]
    return [TimedColumn(i, kind, top, bottom, START + i * 60_000,
                        START + i * 60_000)
            for i, (kind, top, bottom) in enumerate(geometry)]


def candles():
    return [Candle(START + i * 60_000, 100, 101, 99, 100) for i in range(16)]


class CausalLongTests(unittest.TestCase):
    def test_stop_variants_freeze_decisions_and_preserve_three_box_baseline(self):
        from dataclasses import replace
        base = causal.causal_observations('BTC', columns()[:6], 1, candles())[0]
        rows = causal.stop_variants([base], (2, 3, 4, 6))
        self.assertEqual([rows[n][0].stop for n in (2, 3, 4, 6)],
                         [base.entry - n for n in (2, 3, 4, 6)])
        self.assertEqual(rows[3][0], base)
        for n in (2, 4, 6):
            self.assertEqual(replace(rows[n][0], stop=base.stop), base)
        with self.assertRaises(ValueError):
            causal.stop_variants([base], (0, 3))

    def test_stop_changes_exit_and_risk_without_changing_signal_or_fill(self):
        from dataclasses import replace
        from research_v2.patterns.pole_portfolio_reality_audit import _pending_limit_be_classify
        base = replace(causal.causal_observations('BTC', columns()[:6], 1, candles())[0],
                       entry=100, stop=97, observable_entry_ts=START)
        narrow, wide = (causal.stop_variants([base], (2, 6))[n][0] for n in (2, 6))
        sequence = [Candle(START, 101, 101, 99, 100),
                    Candle(START + 60_000, 100, 101, 97, 98)]
        self.assertEqual(_pending_limit_be_classify(narrow, sequence, 3)[:2],
                         ('STOP_FIRST', -1.0))
        self.assertEqual(_pending_limit_be_classify(wide, sequence, 3)[0],
                         'NOT_REACHED')

    def test_bounded_stop_sweep_replays_independently_and_pins_baseline(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            cols, cands = root / 'columns.csv', root / 'candles.csv'
            with cols.open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(('symbol', 'profile_name', 'idx', 'kind', 'top', 'bottom', 'start_ts', 'end_ts'))
                for col in columns()[:6]:
                    writer.writerow(('BTC', 'BTC_bs1_rev3', col.idx, col.kind,
                                     col.top, col.bottom, col.start_ts, col.end_ts))
            with cands.open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(('close_time', 'open', 'high', 'low', 'close'))
                for i in range(16):
                    writer.writerow((START + i*60_000, 100, 106 if i == 8 else 101,
                                     99, 100))
            sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            output = root / 'sweep'
            result = causal.run(cols, cands, output, START, sha(cols), sha(cands),
                                stop_sweep=True)
            self.assertEqual(result['stop_sweep_manifest'], 'stop_sweep/comparison_manifest.json')
            with (output / 'stop_sweep/comparison.csv').open(newline='') as stream:
                rows = {int(row['stop_boxes']): row for row in csv.DictReader(stream)}
            self.assertEqual(set(rows), {2, 3, 4, 6})
            with (output / 'portfolio/portfolio_reality_manifest.json').open() as stream:
                baseline = json.load(stream)
            self.assertEqual(float(rows[3]['gross_total_R']),
                             baseline['summary_metrics']['total_R'])
            self.assertEqual(rows[3]['portfolio_manifest_sha256'],
                             sha(output / 'portfolio/portfolio_reality_manifest.json'))
            for n in (2, 4, 6):
                self.assertTrue((output / f'stop_sweep/stop_{n}_boxes/portfolio_reality_manifest.json').is_file())
            with self.assertRaises(FileExistsError):
                causal.run(cols, cands, output, START, sha(cols), sha(cands), stop_sweep=True)

    def test_future_opposing_pole_cannot_create_or_change_earlier_signal(self):
        from pnf_mvp.patterns.poles import detect_pole_patterns
        cols = columns()
        early = detect_pole_patterns(cols[:6], box_size=1)[0]
        late = detect_pole_patterns(cols, box_size=1)[0]
        self.assertIsNone(early['opposing_pole_distance_columns'])
        self.assertEqual(late['opposing_pole_distance_columns'], 3)
        before = causal.causal_observations('BTC', cols[:6], 1, candles())
        after = causal.causal_observations('BTC', cols, 1, candles())
        self.assertEqual(len(before), 1)
        self.assertEqual(before[0], after[0])
        self.assertEqual(before[0].direction, 'LONG')
        self.assertEqual(before[0].observable_entry_ts, START + 6 * 60_000)

    def test_changing_future_columns_does_not_change_prior_decision(self):
        cols = columns()
        baseline = causal.causal_observations('BTC', cols, 1, candles())[0]
        changed = cols[:6] + [TimedColumn(6, 'X', 106, 100, START + 360_000,
                                          START + 360_000)]
        self.assertEqual(causal.causal_observations('BTC', changed, 1, candles())[0], baseline)

    def test_prefix_detector_equivalence_at_threshold_boundaries(self):
        from pnf_mvp.patterns.poles import detect_pole_patterns
        for bottom in range(89, 106):
            for top in range(92, 111, 3):
                cols = columns()[:6]
                cols[3] = TimedColumn(3, 'O', 109, bottom, START + 180_000, START + 180_000)
                cols[4] = TimedColumn(4, 'X', top, 91, START + 240_000, START + 240_000)
                if top < 91 or bottom > 109:
                    continue
                expected = any(row['pattern_name'] == 'LOW_POLE' and row['pole_column_index'] == 3
                               for row in detect_pole_patterns(cols[:5], box_size=1))
                actual = bool(causal.causal_observations('BTC', cols, 1, candles()))
                self.assertEqual(actual, expected, (bottom, top))

    def test_decision_requires_closed_reversal_and_later_entry_candle(self):
        cols = columns()
        invalid = cols.copy()
        invalid[4] = TimedColumn(4, 'X', 105, 91, START + 240_000, START + 360_001)
        with self.assertRaisesRegex(ValueError, 'chronology'):
            causal.causal_observations('BTC', invalid, 1, candles())
        self.assertEqual(causal.causal_observations('BTC', cols[:6], 1, candles()[:6]), [])

    def test_earlier_signal_ignores_future_candle_prices(self):
        base = causal.causal_observations('BTC', columns()[:6], 1, candles())[0]
        later = candles()
        later[-1] = Candle(later[-1].ts, 50, 60, 40, 50)
        self.assertEqual(causal.causal_observations('BTC', columns()[:6], 1, later)[0], base)

    def test_entry_cohort_boundaries_preserve_full_history_and_exits(self):
        first = START + 6 * 60_000
        self.assertEqual(len(causal.causal_observations('BTC', columns(), 1, candles(),
                                                        first, first + 1)), 1)
        self.assertEqual(causal.causal_observations('BTC', columns(), 1, candles(),
                                                     first + 1, first + 2), [])
        self.assertEqual(causal.causal_observations('BTC', columns(), 1, candles(),
                                                     START, first), [])
        with self.assertRaisesRegex(ValueError, 'period'):
            causal.causal_observations('BTC', columns(), 1, candles(), first, first)

    def test_selected_period_can_have_zero_decisions(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            cols = root / 'columns.csv'
            cands = root / 'candles.csv'
            with cols.open('w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['symbol','profile_name','idx','kind','top','bottom','start_ts','end_ts'])
                for c in columns()[:6]:
                    w.writerow(['BTC','BTC_bs1_rev3',c.idx,c.kind,c.top,c.bottom,c.start_ts,c.end_ts])
            with cands.open('w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['close_time','open','high','low','close'])
                for c in candles():
                    w.writerow([c.ts,c.open,c.high,c.low,c.close])
            digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
            result = causal.run(cols, cands, root / 'out', START,
                                digest(cols), digest(cands), START + 60_000)
            self.assertEqual(result['decision_count'], 0)
            self.assertEqual(result['resolved_portfolio_trades'], 0)

    def test_target_rerun_preserves_be_and_handles_ten_r_with_ambiguous_candle(self):
        from research_v2.patterns.pole_be_research_audit import _be_classify
        rep = causal.causal_observations('BTC', columns()[:6], 1, candles())[0]
        fill = rep.observable_entry_ts
        # Construct a post-fill replay with separate BE arming and target candles.
        from dataclasses import replace
        rep = replace(rep, entry=100, stop=97, observable_entry_ts=fill,
                      replay_includes_anchor=False)
        replay = [Candle(fill + 60_000, 101, 106, 101, 106),
                  Candle(fill + 120_000, 106, 130, 106, 130)]
        self.assertEqual(_be_classify(rep, replay, 2, 10)[:2], ('TARGET_FIRST', 10))
        self.assertEqual(_be_classify(rep, replay, 2, 2.5)[:2], ('TARGET_FIRST', 2.5))
        ambiguous = [Candle(fill + 60_000, 100, 130, 96, 101)]
        self.assertEqual(_be_classify(rep, ambiguous, 2, 10)[0], 'SAME_CANDLE_AMBIGUOUS')
        for bad in (True, float('nan'), float('inf'), 2, 10.1):
            with self.assertRaises(ValueError):
                _be_classify(rep, replay, 2, bad)

    def test_pending_limit_target_sweep_replays_fill_and_later_exit(self):
        from dataclasses import replace
        from research_v2.patterns.pole_portfolio_reality_audit import _pending_limit_be_classify
        rep = causal.causal_observations('BTC', columns()[:6], 1, candles())[0]
        rep = replace(rep, entry=100, stop=97, observable_entry_ts=START)
        sequence = [Candle(START, 102, 103, 99, 101),
                    Candle(START + 60_000, 101, 106, 101, 105),
                    Candle(START + 120_000, 106, 108, 101, 105),
                    Candle(START + 180_000, 105, 130, 105, 130)]
        small = _pending_limit_be_classify(rep, sequence, 3, 2.5)
        large = _pending_limit_be_classify(rep, sequence, 3, 10)
        self.assertEqual(small[:4], ('TARGET_FIRST', 2.5, START, START + 120_000))
        self.assertEqual(large[:4], ('TARGET_FIRST', 10, START, START + 180_000))
        self.assertEqual(_pending_limit_be_classify(rep,
            [Candle(START, 102, 130, 99, 101)], 3, 10)[0],
            'SAME_CANDLE_FILL_TARGET_AMBIGUOUS')

    def test_portfolio_injection_preserves_default_loader(self):
        from research_v2.patterns import pole_portfolio_reality_audit as portfolio
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            with patch.object(portfolio, '_load_observations', return_value=(['BTC'], [], {'BTC': []})) as original:
                portfolio.run({'BTC': path}, {'BTC': path}, {'BTC': path}, path / 'result')
                original.assert_called_once()

    def test_isolated_causal_runner_uses_existing_portfolio_simulator(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            columns_path, candles_path = root / 'columns.csv', root / 'candles.csv'
            with columns_path.open('w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['symbol','profile_name','idx','kind','top','bottom','start_ts','end_ts'])
                for c in columns()[:6]:
                    writer.writerow(['BTC','BTC_bs1_rev3', c.idx, c.kind, c.top, c.bottom,
                                     c.start_ts, c.end_ts])
            with candles_path.open('w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['close_time','open','high','low','close'])
                for i in range(16):
                    high = 106.5 if i == 7 else 108 if i == 8 else 101
                    low = 101 if i in (7, 8) else 99
                    opened = 102 if i in (7, 8) else 100
                    writer.writerow([START+i*60_000, opened, high, low, opened])
            digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
            result = causal.run(columns_path, candles_path, root / 'out', START,
                                digest(columns_path), digest(candles_path))
            self.assertEqual(result['decision_count'], 1)
            self.assertEqual(result['resolved_portfolio_trades'], 1)
            self.assertEqual(result['gross_total_R'], 2.5)
            with (root / 'out/causal_decisions.csv').open(newline='') as f:
                ledger = list(csv.DictReader(f))
            self.assertEqual(len(ledger), 1)
            self.assertLess(int(ledger[0]['decision_known_at_ms']),
                            int(ledger[0]['entry_candle_open_ms']))
            self.assertEqual(result['causal_decisions_sha256'],
                             digest(root / 'out/causal_decisions.csv'))
            with (root / 'out/portfolio/portfolio_reality_manifest.json').open() as f:
                self.assertEqual(json.load(f)['resolved_portfolio_trades'], 1)
            with self.assertRaises(FileExistsError):
                causal.run(columns_path, candles_path, root / 'out', START,
                           digest(columns_path), digest(candles_path))


if __name__ == '__main__':
    unittest.main()
