"""Offline, synthetic and strictly read-only reconciliation observer tests."""
import contextlib
import hashlib
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import urllib.request
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import mexc_readonly_reconciliation_observer as observer


DB_SCHEMA = '''CREATE TABLE live_trades (
    id INTEGER PRIMARY KEY, status TEXT, exchange_order_id TEXT, external_oid TEXT,
    native_symbol TEXT, side TEXT, requested_quantity TEXT, submitted_at_ms INTEGER,
    bot_generated INTEGER)'''
BINDING = (1, 'ORDER_SENT', '858376132774441472', 'pnf-syn1-1790323251000-S',
           'SUI_USDT', 'SHORT', '44.000', 1790323251000, 1)
FILL = {'exchange_order_id': BINDING[2], 'external_oid': BINDING[3], 'symbol': BINDING[4],
        'side': 3, 'requested_quantity': '44', 'cumulative_filled_quantity': '44.0',
        'average_fill_price': '1.015', 'exchange_fill_timestamp': 1790323252000,
        'status': 'FILLED'}


class Adapter:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get_order_status(self, order_id, symbol):
        self.calls.append((order_id, symbol))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class ObserverTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / 'operational.sqlite'
        self.report = Path(self.tmp.name) / 'observer.json'
        with sqlite3.connect(self.db) as conn:
            conn.execute(DB_SCHEMA)
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)', BINDING)

    def execute(self, adapter, *, report=None, max_records=5):
        out = io.StringIO()
        env = {'MEXC_FUTURES_API_KEY': 'synthetic-key',
               'MEXC_FUTURES_API_SECRET': 'synthetic-secret'}
        args = ['--database', str(self.db), '--output-report', str(report or self.report),
                '--max-records', str(max_records)]
        with patch.dict(os.environ, env, clear=True), \
                patch.object(urllib.request, 'urlopen', side_effect=AssertionError('network')), \
                patch.object(urllib.request, 'build_opener', side_effect=AssertionError('network')), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            code = observer.main(args, adapter=adapter, clock_ms=lambda: 1790323253000)
        return code, json.loads(out.getvalue())

    def test_full_fill_short_digest_replay_restart_and_read_only(self):
        first = Adapter([{'success': True, 'data': dict(FILL)}])
        before = self.db.read_bytes()
        code, result = self.execute(first)
        self.assertEqual((code, first.calls), (0, [(BINDING[2], BINDING[4])]))
        self.assertEqual(result['status'], 'PASS')
        entry = json.loads(self.report.read_bytes())['observations'][0]
        self.assertEqual(entry['classification'], 'FULL_FILL_MATCH')
        self.assertRegex(entry['digest'], r'^[0-9a-f]{64}$')
        self.assertEqual(self.db.read_bytes(), before)
        self.assertFalse(self.db.with_name(self.db.name + '-wal').exists())
        self.assertFalse(self.db.with_name(self.db.name + '-shm').exists())
        second_report = Path(self.tmp.name) / 'retry.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]),
                                      report=second_report)[0], 0)
        self.assertEqual(self.report.read_bytes(), second_report.read_bytes())
        self.assertEqual(self.db.read_bytes(), before)

    def test_current_schema_lacks_required_binding_and_makes_zero_exchange_calls(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute('DROP TABLE live_trades')
            conn.execute('''CREATE TABLE live_trades (
                id INTEGER PRIMARY KEY, symbol TEXT, side TEXT, status TEXT,
                exchange_order_id TEXT, raw_order_response TEXT)''')
            conn.execute('INSERT INTO live_trades VALUES(1,?,?,?,?,?)',
                         ('MEXC_FUT:SUIUSDT', 'SHORT', 'ORDER_SENT', BINDING[2],
                          json.dumps({'order_request': {'externalOid': BINDING[3]}})))
        adapter = Adapter([])
        before = self.db.read_bytes()
        code, result = self.execute(adapter)
        self.assertEqual(code, 0)
        self.assertEqual(adapter.calls, [])
        report = json.loads(self.report.read_text())
        self.assertFalse(report['schema_supported'])
        self.assertEqual(report['observations'][0]['classification'], 'INSUFFICIENT_BINDING')
        self.assertEqual(self.db.read_bytes(), before)
        self.assertNotIn(BINDING[2], json.dumps(result) + self.report.read_text())

    def test_classifications_and_strict_complete_binding(self):
        cases = (
            (dict(FILL, cumulative_filled_quantity='20', status='PARTIALLY_FILLED'), 'PARTIAL_FILL'),
            (dict(FILL, cumulative_filled_quantity='0', status='NEW'), 'ZERO_FILL'),
            (dict(FILL, status='CANCELLED'), 'CANCELLED_OR_REJECTED'),
            (dict(FILL, status='REJECTED'), 'CANCELLED_OR_REJECTED'),
            (dict(FILL, status='EXPIRED'), 'CANCELLED_OR_REJECTED'),
            (dict(FILL, exchange_order_id='2'), 'IDENTITY_MISMATCH'),
            (dict(FILL, external_oid='pnf-syn2-1790323251000-S'), 'IDENTITY_MISMATCH'),
            (dict(FILL, symbol='BTC_USDT'), 'IDENTITY_MISMATCH'),
            (dict(FILL, side=1), 'IDENTITY_MISMATCH'),
            (dict(FILL, requested_quantity='43'), 'QUANTITY_MISMATCH'),
            (dict(FILL, cumulative_filled_quantity='45'), 'QUANTITY_MISMATCH'),
            (dict(FILL, exchange_fill_timestamp=BINDING[7] - 1), 'STALE_OR_INVALID_TIMESTAMP'),
            (dict(FILL, exchange_fill_timestamp=1790323253001), 'STALE_OR_INVALID_TIMESTAMP'),
            (dict(FILL, status='UNKNOWN'), 'EVIDENCE_UNAVAILABLE'),
            (dict(FILL, average_fill_price='NaN'), 'CONFLICTING_EVIDENCE'),
            (None, 'EVIDENCE_UNAVAILABLE'),
            (TimeoutError('synthetic-secret'), 'EVIDENCE_UNAVAILABLE'),
        )
        for evidence, expected in cases:
            with self.subTest(expected=expected, evidence=evidence):
                report = Path(self.tmp.name) / f'case-{len(list(Path(self.tmp.name).glob("case-*.json")))}.json'
                response = {'success': True, 'data': evidence} if evidence is not None and not isinstance(evidence, Exception) else evidence
                code, result = self.execute(Adapter([response]), report=report)
                self.assertEqual(code, 0)
                self.assertEqual(json.loads(report.read_text())['observations'][0]['classification'], expected)
                self.assertNotIn('synthetic-secret', json.dumps(result) + report.read_text())

    def test_opening_sides_equivalent_decimal_and_timestamp_boundaries(self):
        self.assertEqual(observer._canonical(Decimal('123456789012345678901234567890.000')),
                         '123456789012345678901234567890')
        for side, exchange_side, suffix in (('LONG', 1, 'L'), ('SHORT', 3, 'S')):
            for timestamp in (BINDING[7], 1790323253000):
                with self.subTest(side=side, timestamp=timestamp):
                    oid = 'pnf-syn1-1790323251000-' + suffix
                    with sqlite3.connect(self.db) as conn:
                        conn.execute('UPDATE live_trades SET side=?,external_oid=?,requested_quantity=?',
                                     (side, oid, '44.000'))
                    response = dict(FILL, side=exchange_side, external_oid=oid,
                                    exchange_fill_timestamp=timestamp,
                                    vol=44, dealVol='44.000', dealAvgPrice='1.0150')
                    target = Path(self.tmp.name) / f'{side}-{timestamp}.json'
                    code, _ = self.execute(Adapter([{'success': True, 'data': response}]), report=target)
                    self.assertEqual(code, 0)
                    self.assertEqual(json.loads(target.read_text())['observations'][0]['classification'],
                                     'FULL_FILL_MATCH')

    def test_incomplete_or_manual_local_rows_never_call_exchange(self):
        alterations = (
            ('external_oid', None), ('external_oid', 'manual-order'),
            ('bot_generated', 0), ('native_symbol', None), ('side', 'CLOSE'),
            ('requested_quantity', 'NaN'), ('requested_quantity', b'44.0'),
            ('submitted_at_ms', None), ('exchange_order_id', None),
        )
        for index, (field, value) in enumerate(alterations):
            with self.subTest(field=field, value=value):
                with sqlite3.connect(self.db) as conn:
                    conn.execute(f'UPDATE live_trades SET {field}=?', (value,))
                adapter = Adapter([])
                report = Path(self.tmp.name) / f'incomplete-{index}.json'
                code, _ = self.execute(adapter, report=report)
                self.assertEqual((code, adapter.calls), (0, []))
                self.assertEqual(json.loads(report.read_text())['observations'][0]['classification'],
                                 'INSUFFICIENT_BINDING')
                with sqlite3.connect(self.db) as conn:
                    conn.execute('DELETE FROM live_trades')
                    conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)', BINDING)

    def test_missing_evidence_oid_and_alias_conflicts_fail_closed(self):
        for index, changes in enumerate(({'external_oid': None}, {'dealAvgPrice': '2'},
                                         {'orderId': '2'}, {'externalOid': 'pnf-syn2-1790323251000-S'},
                                         {'vol': True}, {'fill_time': 1790323251000})):
            with self.subTest(changes=changes):
                response = dict(FILL, **changes)
                if changes == {'external_oid': None}:
                    del response['external_oid']
                report = Path(self.tmp.name) / f'conflict-{index}.json'
                code, _ = self.execute(Adapter([{'success': True, 'data': response}]), report=report)
                self.assertEqual(code, 0)
                self.assertEqual(json.loads(report.read_text())['observations'][0]['classification'],
                                 'INSUFFICIENT_BINDING' if 'external_oid' not in response
                                 else 'CONFLICTING_EVIDENCE')

    def test_only_order_sent_bounded_and_stop_after_unavailable(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                         (2, 'FILLED', '2', 'pnf-syn1-1790323251000-S', 'SUI_USDT', 'SHORT', '44', BINDING[7], 1))
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                         (3, 'ORDER_SENT', '3', 'pnf-syn1-1790323251000-S', 'SUI_USDT', 'SHORT', '44', BINDING[7], 1))
        for maximum, responses, count in ((1, [None], 1),
                                          (2, [TimeoutError('rate limit'), dict(FILL)], 1)):
            with self.subTest(maximum=maximum):
                adapter = Adapter(responses)
                target = Path(self.tmp.name) / f'bound-{maximum}.json'
                code, _ = self.execute(adapter, report=target, max_records=maximum)
                self.assertEqual(code, 0)
                self.assertEqual(len(adapter.calls), count)
                self.assertEqual(len(json.loads(target.read_text())['observations']), count)
        for maximum in (0, 21, -1):
            report = Path(self.tmp.name) / f'bad-{maximum}.json'
            code, result = self.execute(Adapter([]), report=report, max_records=maximum)
            self.assertEqual((code, result['reason']), (1, 'INVALID_INPUT'))
            self.assertFalse(report.exists())

    def test_duplicate_local_order_binding_and_future_submission_fail_without_requests(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                         (2, 'ORDER_SENT', *BINDING[2:]))
        adapter = Adapter([])
        code, _ = self.execute(adapter)
        self.assertEqual((code, adapter.calls), (0, []))
        self.assertEqual([item['classification'] for item in json.loads(self.report.read_text())['observations']],
                         ['CONFLICTING_EVIDENCE', 'CONFLICTING_EVIDENCE'])
        with sqlite3.connect(self.db) as conn:
            conn.execute('DELETE FROM live_trades WHERE id=2')
            conn.execute('UPDATE live_trades SET submitted_at_ms=? WHERE id=1', (1790323253001,))
        report = Path(self.tmp.name) / 'future.json'
        self.assertEqual(self.execute(adapter, report=report)[0], 0)
        self.assertEqual(adapter.calls, [])
        self.assertEqual(json.loads(report.read_text())['observations'][0]['classification'],
                         'STALE_OR_INVALID_TIMESTAMP')

    def test_missing_table_is_classified_without_exchange_or_schema_creation(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute('DROP TABLE live_trades')
        before = self.db.read_bytes()
        adapter = Adapter([])
        code, _ = self.execute(adapter)
        self.assertEqual((code, adapter.calls), (0, []))
        self.assertEqual(json.loads(self.report.read_text())['observations'][0]['classification'],
                         'INSUFFICIENT_BINDING')
        self.assertEqual(self.db.read_bytes(), before)

    def test_read_only_authorizer_refuses_writes_and_mutating_pragmas(self):
        original_connect = sqlite3.connect
        checked = []

        class GuardedConnection:
            def __init__(self, path_uri, **kwargs):
                self.conn = original_connect(path_uri, **kwargs)
                self.uri = path_uri
                self.kwargs = kwargs

            def close(self):
                self.conn.close()

            def execute(self, *args):
                return self.conn.execute(*args)

            def set_authorizer(self, fn):
                self.conn.set_authorizer(fn)
                if fn is not None:
                    self.assert_closed_to_writes()

            def assert_closed_to_writes(self):
                if '?mode=ro&immutable=1' not in self.uri or self.kwargs.get('uri') is not True:
                    raise AssertionError('database was not opened in URI mode=ro')
                for sql in ('UPDATE live_trades SET status="FILLED"',
                            'DELETE FROM live_trades', 'CREATE TABLE observer_tmp(x)',
                            'PRAGMA user_version=3', 'PRAGMA query_only=OFF',
                            "ATTACH DATABASE ':memory:' AS unexpected"):
                    try:
                        self.conn.execute(sql)
                    except sqlite3.DatabaseError:
                        checked.append(sql)
                    else:
                        raise AssertionError('write allowed: ' + sql)

        with patch.object(observer.sqlite3, 'connect', side_effect=GuardedConnection):
            code, result = self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))
        self.assertEqual(code, 0, result)
        self.assertEqual(len(checked), 6)
        with original_connect(self.db) as conn:
            self.assertEqual(conn.execute('SELECT status FROM live_trades').fetchone()[0], 'ORDER_SENT')

    def test_existing_report_publication_error_and_sidecars_preserve_all_bytes(self):
        before = self.db.read_bytes()
        self.report.write_bytes(b'existing operator report\r\n')
        code, result = self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))
        self.assertEqual((code, result['reason']), (1, 'OBSERVATION_ERROR'))
        self.assertEqual(self.report.read_bytes(), b'existing operator report\r\n')
        self.assertEqual(self.db.read_bytes(), before)
        self.assertEqual(list(Path(self.tmp.name).glob('.mexc-shadow-*')), [])
        for suffix in ('-wal', '-shm'):
            path = self.db.with_name(self.db.name + suffix)
            path.write_bytes(b'pre-existing operator bytes' + suffix.encode())
        adapter = Adapter([])
        other = Path(self.tmp.name) / 'sidecars.json'
        code, result = self.execute(adapter, report=other)
        self.assertEqual((code, result['reason'], adapter.calls), (1, 'OBSERVATION_ERROR', []))
        self.assertFalse(other.exists())
        self.assertEqual(self.db.read_bytes(), before)
        for suffix in ('-wal', '-shm'):
            self.assertEqual(self.db.with_name(self.db.name + suffix).read_bytes(),
                             b'pre-existing operator bytes' + suffix.encode())

    def test_wal_header_without_sidecars_still_fails_before_sqlite_open(self):
        header = bytearray(self.db.read_bytes())
        header[18:20] = b'\x02\x02'
        self.db.write_bytes(header)
        before = self.db.read_bytes()
        adapter = Adapter([])
        with patch.object(observer.sqlite3, 'connect', side_effect=AssertionError('opened WAL')):
            code, result = self.execute(adapter)
        self.assertEqual((code, result['reason'], adapter.calls), (1, 'OBSERVATION_ERROR', []))
        self.assertFalse(self.report.exists())
        self.assertEqual(self.db.read_bytes(), before)
        self.assertFalse(self.db.with_name(self.db.name + '-shm').exists())

    def test_publication_failure_cleans_temp_and_never_mutates_database(self):
        before = self.db.read_bytes()
        with patch.object(observer, '_write_new_report', wraps=observer._write_new_report):
            with patch('mexc_readonly_shadow_check._publish_new_report',
                       side_effect=OSError('synthetic-secret')):
                code, result = self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))
        self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'OBSERVATION_ERROR'}))
        self.assertFalse(self.report.exists())
        self.assertEqual(list(Path(self.tmp.name).glob('.mexc-shadow-*')), [])
        self.assertEqual(self.db.read_bytes(), before)

    def test_database_access_denied_fails_without_report_or_exchange_request(self):
        before = self.db.read_bytes()
        adapter = Adapter([])
        with patch.object(observer.sqlite3, 'connect', side_effect=PermissionError('synthetic-secret')):
            code, result = self.execute(adapter)
        self.assertEqual((code, result, adapter.calls),
                         (1, {'status': 'FAIL', 'reason': 'OBSERVATION_ERROR'}, []))
        self.assertFalse(self.report.exists())
        self.assertEqual(self.db.read_bytes(), before)

    def test_credentials_are_not_loaded_when_local_binding_is_missing(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute('UPDATE live_trades SET bot_generated=0')
        with patch.object(observer, 'ReadOnlyMexcOrderStatusAdapter',
                          side_effect=AssertionError('adapter construction')) as construct:
            code, _ = self.execute(adapter=None)
        self.assertEqual(code, 0)
        construct.assert_not_called()

    def test_audited_adapter_omits_external_oid_so_live_match_remains_unavailable(self):
        class RecordedTransport:
            def __init__(self):
                self.calls = []

            def get(self, url, headers, timeout):
                self.calls.append(url)
                if len(self.calls) == 1:
                    data = {'orderId': BINDING[2], 'externalOid': BINDING[3],
                            'symbol': BINDING[4], 'side': 3, 'state': 3,
                            'vol': '44', 'dealVol': '44', 'dealAvgPriceStr': '1.015',
                            'updateTime': 1790323252000}
                else:
                    data = [{'orderId': BINDING[2], 'symbol': BINDING[4], 'side': 3,
                             'id': 'deal-1', 'vol': '44', 'price': '1.015',
                             'timestamp': 1790323252000}]
                return json.dumps({'success': True, 'code': 0, 'data': data}).encode()

        transport = RecordedTransport()
        with patch.dict(os.environ, {'MEXC_FUTURES_API_KEY': 'synthetic-key',
                                     'MEXC_FUTURES_API_SECRET': 'synthetic-secret'}, clear=True):
            audited = observer.ReadOnlyMexcOrderStatusAdapter(enabled=True, transport=transport)
        code, _ = self.execute(audited)
        self.assertEqual(code, 0)
        self.assertEqual(len(transport.calls), 2)
        self.assertTrue(all('/api/v1/private/order/' in url for url in transport.calls))
        self.assertEqual(json.loads(self.report.read_text())['observations'][0]['classification'],
                         'INSUFFICIENT_BINDING')

    def test_duplicate_exchange_json_is_rejected_by_audited_adapter(self):
        class DuplicateTransport:
            def __init__(self):
                self.calls = 0

            def get(self, url, headers, timeout):
                self.calls += 1
                return b'{"success":true,"code":0,"data":{"orderId":"1","orderId":"1"}}'

        transport = DuplicateTransport()
        with patch.dict(os.environ, {'MEXC_FUTURES_API_KEY': 'synthetic-key',
                                     'MEXC_FUTURES_API_SECRET': 'synthetic-secret'}, clear=True):
            audited = observer.ReadOnlyMexcOrderStatusAdapter(enabled=True, transport=transport)
        code, _ = self.execute(audited)
        self.assertEqual((code, transport.calls), (0, 1))
        self.assertEqual(json.loads(self.report.read_text())['observations'][0]['classification'],
                         'EVIDENCE_UNAVAILABLE')

    def test_no_mutating_trader_imports_and_sanitized_report(self):
        self.assertNotIn('live_mexc_forward_trader', observer.__dict__)
        self.assertFalse(hasattr(observer, 'reconcile_exchange_fills'))
        self.assertNotIn('live_mexc_forward_trader', sys.modules)
        code, output = self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))
        self.assertEqual(code, 0)
        report = self.report.read_text()
        for forbidden in (BINDING[2], BINDING[3], BINDING[4],
                          'synthetic-secret', 'synthetic-key', 'Signature', 'ApiKey', 'https://'):
            self.assertNotIn(forbidden, report + json.dumps(output))


if __name__ == '__main__':
    unittest.main()
