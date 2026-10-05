import tempfile
import unittest
from pathlib import Path

from research_v2.du_plessis_poles_annual import run, summarize
from research_v2.du_plessis_poles_forward_sim import simulate


class AnnualTests(unittest.TestCase):
    def test_accounting_normalized_bps_and_unresolved_separation(self):
        replay = {'trades': [
            {'status': 'SIMULATED_CLOSED', 'direction': 'LONG', 'signal_ts': 1704302219999,
             'entry_simulated_open_price': '100', 'exit_simulated_open_price': '102',
             'gross_price_delta': '2'},
            {'status': 'SIMULATED_CLOSED', 'direction': 'SHORT', 'signal_ts': 1712000000000,
             'entry_simulated_open_price': '200', 'exit_simulated_open_price': '202',
             'gross_price_delta': '-2'},
            {'status': 'SIMULATED_OPEN'}], 'skipped': [{}],
            'pending_entry_event_id': None, 'pending_exit_event_id': None}
        result = summarize(replay)
        self.assertEqual(result['completed_trades'], 2)
        self.assertEqual(result['open_at_end'], 1)
        self.assertEqual(result['annual']['gross_sum_bps_constant_notional_proxy'], '100')
        self.assertEqual(result['annual']['maximum_consecutive_losses'], 1)
        self.assertEqual(result['break_even_symmetric_bps_per_side_before_other_costs'], '25')
        self.assertEqual(result['quarters']['2024-Q1']['completed'], 1)
        self.assertEqual(result['quarters']['2024-Q2']['completed'], 1)
        replay['trades'][0]['gross_price_delta'] = '3'
        with self.assertRaisesRegex(ValueError, 'accounting'):
            summarize(replay)

    def test_unpinned_csv_rejected_before_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'candles.csv'
            path.write_text('close_time,open,close\n1,100,100\n')
            with self.assertRaisesRegex(ValueError, 'SHA-256'):
                run(path)

    def test_optimized_ledger_matches_bounded_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'candles.csv'
            path.write_text('close_time,open,close\n' + ''.join(
                f'{n*60000-1},{op},{cl}\n' for n, (op, cl) in enumerate(
                    [(7,7), (7,10), (10,7), (7,10), (10,7),
                     (7,19), (19,13), (12,19), (20,20)], 1)))
            result = simulate(path, box_size=1, reversal_boxes=3, max_candles=9)
            self.assertEqual(result['trades'][0]['event_id'],
                             'du_plessis_poles_v1:EARLY_ENTRY:4:5')
            self.assertEqual(result['trades'][0]['gross_price_delta'], '-8')

    def test_exact_year_boundaries_fail_closed(self):
        from unittest.mock import patch
        from research_v2.du_plessis_poles_annual import SOURCE_SHA256, SOURCE_MINUTES
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'candles.csv'
            path.write_text('synthetic fixture')
            bad = {'candles_processed': SOURCE_MINUTES, 'first_close_ts': 1704067259999,
                   'last_close_ts': 1735689599998}
            with patch('research_v2.du_plessis_poles_annual._sha256', return_value=SOURCE_SHA256), \
                 patch('research_v2.du_plessis_poles_annual.simulate', return_value=bad):
                with self.assertRaisesRegex(ValueError, 'incomplete pinned UTC year'):
                    run(path)
