import csv
from decimal import Decimal as D
import hashlib
from pathlib import Path
import tempfile
import unittest

from research_v2.patterns import hourly_breakout_v1 as m


def data(hours=24, *, both=False):
    start = 1735689600000
    for i in range(hours * 60):
        hour, minute = divmod(i, 60)
        op = D(100)
        hi, lo, close = D(101), D(99), D(100)
        if hour == 21 and minute == 59:
            hi, close = D(103), D(102)
        if hour >= 22:
            op, hi, lo, close = D(102), D(103), D(101), D(102)
            if both and hour == 22 and minute == 0:
                hi, lo = D(120), D(80)
        yield start + i * 60000 + 59999, op, hi, lo, close


class HourlyBreakoutTests(unittest.TestCase):
    def test_closed_signal_next_minute_entry(self):
        result = m.simulate(data(hours=23, both=True), expected_minutes=1380)
        self.assertEqual(result['signals'], 1)
        trade = result['trades'][0]
        self.assertEqual(trade['entry_ts'], trade['signal_ts'] + 1)
        self.assertEqual(trade['reason'], 'STOP_FIRST')
        self.assertEqual(D(trade['gross_R']), -1)
        self.assertLess(D(trade['modeled_R']['2']), D(trade['gross_R']))

    def test_no_future_signal_before_hour_closes(self):
        result = m.simulate(data(hours=21))
        self.assertEqual(result['signals'], 0)
        self.assertEqual(result['trades'], [])

    def test_prefix_and_restart_determinism(self):
        first = m.simulate(data(hours=23, both=True))
        later = m.simulate(data(hours=24, both=True))
        self.assertEqual(first['trades'], later['trades'])
        self.assertEqual(first, m.simulate(data(hours=23, both=True)))

    def test_incomplete_and_misaligned_fail(self):
        with self.assertRaises(ValueError):
            m.simulate(data(hours=21), expected_minutes=22 * 60)
        with self.assertRaises(ValueError):
            m.simulate(list(data(hours=1))[1:])

    def test_hash_gate_and_one_new_report(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, output = root / 'candles.csv', root / 'report.json'
            with source.open('w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(('close_time', 'open', 'high', 'low', 'close'))
                writer.writerows(data(hours=23, both=True))
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, 'SHA mismatch'):
                m.run(source, '0' * 64, output, expected_minutes=1380)
            self.assertFalse(output.exists())
            result = m.run(source, digest, output, expected_minutes=1380)
            self.assertEqual(result['resolved_trades'], 1)
            with self.assertRaisesRegex(ValueError, 'New output'):
                m.run(source, digest, output, expected_minutes=1380)

    def test_no_order_or_database_capability(self):
        source = Path(m.__file__).read_text()
        for text in ('sqlite3', 'urlopen', 'place_order', 'Storage('):
            self.assertNotIn(text, source)


if __name__ == '__main__':
    unittest.main()
