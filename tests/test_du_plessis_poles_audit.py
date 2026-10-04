import tempfile
import unittest
from pathlib import Path

from research_v2.du_plessis_poles_audit import audit


class AuditTests(unittest.TestCase):
    def test_first_eligible_closed_candle_and_independent_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'candles.csv'
            path.write_text('close_time,close\n' + ''.join(
                f'{i},{price}\n' for i, price in enumerate((7, 10, 7, 10, 7, 19, 14, 13), 1)))
            result = audit(path, pole_index=4, retrace_index=5,
                           box_size=1, reversal_boxes=3)
            self.assertEqual(result['decision_ts'], 8)
            self.assertEqual(result['candles_processed'], 8)
            self.assertEqual(result['column_length_boxes'], 12)
            self.assertEqual(result['retracement_boxes'], 6)
            self.assertTrue(result['threshold_met'])
            self.assertFalse(result['structural_strict_gt_50'])
            self.assertEqual(result['execution'], 'OFF; no fill inferred')
            with self.assertRaisesRegex(ValueError, 'not found'):
                audit(path, pole_index=4, retrace_index=5,
                      box_size=1, reversal_boxes=3, max_candles=7)

    def test_missing_event_does_not_guess_or_access_db(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'candles.csv'
            path.write_text('close_time,close\n1,7\n2,10\n')
            with self.assertRaisesRegex(ValueError, 'not found'):
                audit(path, pole_index=28, retrace_index=29,
                      box_size=1, reversal_boxes=3)
