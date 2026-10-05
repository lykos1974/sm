import tempfile
import unittest
from pathlib import Path

from research_v2.du_plessis_poles_forward_sim import simulate


class ForwardSimulationTests(unittest.TestCase):
    def candles(self, values):
        directory = tempfile.TemporaryDirectory()
        path = Path(directory.name) / 'research.csv'
        path.write_text('close_time,open,close\n' + ''.join(
            f'{n*60000-1},{open_},{close}\n'
            for n, (open_, close) in enumerate(values, 1)))
        self.addCleanup(directory.cleanup)
        return path

    def test_signal_then_next_open_fill_and_reversal_then_next_open_exit(self):
        path = self.candles([(7,7), (7,10), (10,7), (7,10), (10,7),
                             (7,19), (19,13), (12,19), (20,20)])
        result = simulate(path, box_size=1, reversal_boxes=3, max_candles=9)
        self.assertEqual(result['execution'], 'OFFLINE_SIMULATION_ONLY')
        self.assertEqual(len(result['trades']), 1)
        trade = result['trades'][0]
        self.assertEqual(trade['theoretical_trigger_level'], '13')
        self.assertEqual(trade['entry_simulated_open_price'], '12')
        self.assertEqual(trade['entry_simulated_open_ts'], 420000)
        self.assertEqual(trade['exit_simulated_open_price'], '20')
        self.assertEqual(trade['gross_price_delta'], '-8')
        self.assertEqual(trade['exit_reason'], 'NEXT_VALID_REVERSAL')
        self.assertEqual(trade['status'], 'SIMULATED_CLOSED')

    def test_no_next_open_does_not_infer_fill_and_spot_blocks_short(self):
        path = self.candles([(7,7), (7,10), (10,7), (7,10), (10,7), (7,19), (19,13)])
        result = simulate(path, box_size=1, reversal_boxes=3, max_candles=7)
        self.assertEqual(result['trades'], [])
        self.assertTrue(result['pending_entry_event_id'])
        spot = simulate(path, box_size=1, reversal_boxes=3, max_candles=7, venue='SPOT')
        self.assertEqual(spot['trades'], [])
        self.assertIsNone(spot['pending_entry_event_id'])
        self.assertEqual(spot['skipped'][0]['reason'], 'BLOCKED_SPOT_SHORT')

    def test_gap_fails_closed(self):
        path = self.candles([(7,7), (7,10)])
        text = path.read_text().replace('119999,7,10', '180000,7,10')
        path.write_text(text)
        with self.assertRaisesRegex(ValueError, 'noncontinuous'):
            simulate(path, box_size=1, reversal_boxes=3)
