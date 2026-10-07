import csv
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from research_v2.patterns import binance_um_2025_frozen_inputs as m
from research_v2.patterns.binance_um_2025_archive_preflight import SCHEMA


class FrozenInputTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.src = self.root / 'source'
        self.src.mkdir()
        self.dst = self.root / 'output'
        self.items = []
        for month in range(1, 13):
            name = f'BTCUSDT-1m-2025-{month:02}.zip'
            self.items.append({'month': f'2025-{month:02}', 'rows': 43800})
            with zipfile.ZipFile(self.src / name, 'w') as z:
                content = io.StringIO()
                writer = csv.writer(content)
                for i in range(2):
                    ts = 1735689600000 + i * 60000
                    writer.writerow((ts, '100', '101', '99', '100', '1', ts + 59999, '0', '1', '0', '0', '0'))
                z.writestr(name[:-4] + '.csv', content.getvalue())
        manifest = {'schema': SCHEMA, 'venue': 'BINANCE_UM', 'symbol': 'BTCUSDT',
                    'interval': '1m', 'year': 2025, 'complete_year': True,
                    'months': self.items, 'rows': 525600}
        self.manifest = self.src / 'binance_um_btc_2025_preflight.json'
        self.manifest.write_text(json.dumps(manifest))
        self.sha = hashlib.sha256(self.manifest.read_bytes()).hexdigest()

    def test_conversion_writes_separate_pinned_inputs(self):
        with patch.object(m, 'inspect_month', side_effect=lambda _source, month: self.items[month - 1]):
            report = m.build(self.src, self.dst, self.sha, months=(1,))
        self.assertEqual(report['candles'], 2)
        self.assertEqual(report['box_size'], 100)
        with (self.dst / 'candles_1m.csv').open(newline='') as f:
            candles = list(csv.DictReader(f))
        self.assertEqual([c['close_time'] for c in candles], ['1735689659999', '1735689719999'])
        with (self.dst / 'columns.csv').open(newline='') as f:
            columns = list(csv.DictReader(f))
        self.assertEqual(columns[0]['profile_name'], 'BTCUSDT_bs100_rev3')
        self.assertEqual(m._sha(self.dst / 'candles_1m.csv'), report['candles_sha256'])
        self.assertFalse(any(x.name.endswith('.db') for x in self.src.iterdir()))
        with self.assertRaises(ValueError):
            m.build(self.src, self.dst, self.sha, months=(1,))

    def test_manifest_hash_and_schema_fail_closed(self):
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            m.build(self.src, self.dst, '0' * 64)
        self.assertFalse(self.dst.exists())
        self.manifest.write_text(self.manifest.read_text().replace('BINANCE_UM', 'SPOT'))
        digest = m._sha(self.manifest)
        with self.assertRaisesRegex(ValueError, 'preflight'):
            m.build(self.src, self.dst, digest)
        self.assertFalse(self.dst.exists())

    def test_changed_archive_and_cleanup(self):
        def changed(_source, month):
            value = dict(self.items[month - 1])
            if month == 2:
                value['rows'] -= 1
            return value
        with patch.object(m, 'inspect_month', side_effect=changed):
            with self.assertRaisesRegex(ValueError, 'differ'):
                m.build(self.src, self.dst, self.sha)
        self.assertFalse(self.dst.exists())

    def test_no_runtime_or_network_capability(self):
        source = Path(m.__file__).read_text()
        for forbidden in ('urlopen', 'sqlite3', 'Storage(', 'place_order', 'live_mexc_forward_trader'):
            self.assertNotIn(forbidden, source)


if __name__ == '__main__':
    unittest.main()
