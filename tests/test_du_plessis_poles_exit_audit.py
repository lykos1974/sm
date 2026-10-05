import tempfile
import unittest
from pathlib import Path

from research_v2.du_plessis_poles_exit_audit import audit_exit


class ExitAuditTests(unittest.TestCase):
    def test_next_valid_reversal_and_following_open(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'candles.csv'
            path.write_text('close_time,open,close\n' + ''.join(
                f'{n*60000-1},{op},{cl}\n' for n, (op, cl) in enumerate(
                    [(7,7), (7,10), (10,7), (7,10), (10,7),
                     (7,19), (19,13), (12,19), (20,20)], 1)))
            event = 'du_plessis_poles_v1:EARLY_ENTRY:4:5'
            report = audit_exit(path, event_id=event, box_size=1, reversal_boxes=3)
            self.assertEqual(report['chronology'], 'PASS')
            self.assertEqual(report['reversal_column']['kind'], 'X')
            self.assertEqual(report['reversal_column']['index'], 6)
            self.assertEqual(report['exit_signal_close_ts'], 8*60000-1)
            self.assertEqual(report['following_open_ts'], 8*60000)
            self.assertEqual(report['following_open_price'], '20')
            with self.assertRaisesRegex(ValueError, 'completed simulated trade'):
                audit_exit(path, event_id=event, box_size=1, reversal_boxes=3,
                           max_candles=8)
