import calendar
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from research_v2.patterns import binance_um_2025_archive_preflight as m


class ArchivePreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def month(self, count=2, *, bad=None):
        name = 'BTCUSDT-1m-2025-01.zip'
        start = 1735689600000
        with zipfile.ZipFile(self.root / name, 'w', compression=zipfile.ZIP_DEFLATED) as z:
            out = io.StringIO()
            for i in range(count):
                index = i if bad != 'duplicate' or i != 1 else 0
                t = start + index * 60000
                fields = [t, '100.0', '101', '99', '100', '1.2', t + 59999, '0', '1', '0', '0', '0']
                if bad == 'ohlc' and i == 0:
                    fields[2] = '98'
                if bad == 'timestamp' and i == 0:
                    fields[0] = str(t * 1000)
                out.write(','.join(map(str, fields)) + '\n')
            z.writestr(name[:-4] + '.csv', out.getvalue())
        digest = hashlib.sha256((self.root / name).read_bytes()).hexdigest()
        (self.root / (name + '.CHECKSUM')).write_text(digest + '  ' + name + '\n', encoding='ascii')

    def test_valid_month_and_report(self):
        self.month()
        with patch.object(m, '_expected_count', return_value=2):
            report = m.run(self.root, months=(1,))
        self.assertEqual(report['rows'], 2)
        self.assertFalse(report['complete_year'])
        with self.assertRaises(ValueError):
            m.run(self.root, months=(1,))

    def test_checksum_rejected_without_report(self):
        self.month()
        (self.root / 'BTCUSDT-1m-2025-01.zip.CHECKSUM').write_text('0' * 64 + '  BTCUSDT-1m-2025-01.zip\n')
        with self.assertRaisesRegex(ValueError, 'SHA-256'):
            m.inspect_month(self.root, 1)
        self.assertFalse((self.root / 'binance_um_btc_2025_preflight.json').exists())

    def test_missing_or_duplicate_rejected(self):
        for bad in (None, 'duplicate'):
            self.month(count=1 if bad is None else 2, bad=bad)
            with patch.object(m, '_expected_count', return_value=2):
                with self.assertRaises(ValueError):
                    m.inspect_month(self.root, 1)

    def test_invalid_candle_and_timestamp(self):
        for bad in ('ohlc', 'timestamp'):
            self.month(bad=bad)
            with patch.object(m, '_expected_count', return_value=2):
                with self.assertRaises(ValueError):
                    m.inspect_month(self.root, 1)

    def test_numeric_rejection(self):
        for item in ('NaN', '-1', 'Infinity', '1e9999999', '1' * 81):
            with self.assertRaises(ValueError):
                m._number(item)

    def test_no_database_or_trader_import(self):
        source = Path(m.__file__).read_text()
        self.assertNotIn('sqlite3', source)
        self.assertNotIn('live_mexc_forward_trader', source)
        self.assertNotIn('Storage(', source)


if __name__ == '__main__':
    unittest.main()
