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

    def execute(self, adapter, *, report=None, max_records=5, previous=None):
        out = io.StringIO()
        env = {'MEXC_FUTURES_API_KEY': 'synthetic-key',
               'MEXC_FUTURES_API_SECRET': 'synthetic-secret'}
        args = ['--database', str(self.db), '--output-report', str(report or self.report),
                '--max-records', str(max_records)]
        if previous is not None:
            args.extend(('--previous-report', str(previous)))
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
            (dict(FILL, cumulative_filled_quantity='0', average_fill_price='0', status='NEW'), 'ZERO_FILL'),
            (dict(FILL, cumulative_filled_quantity='20', status='CANCELLED'), 'CANCELLED_OR_REJECTED'),
            (dict(FILL, cumulative_filled_quantity='20', status='REJECTED'), 'CANCELLED_OR_REJECTED'),
            (dict(FILL, cumulative_filled_quantity='20', status='EXPIRED'), 'CANCELLED_OR_REJECTED'),
            (dict(FILL, status='CANCELLED'), 'CONFLICTING_EVIDENCE'),
            (dict(FILL, exchange_order_id='2'), 'IDENTITY_MISMATCH'),
            (dict(FILL, external_oid='pnf-syn2-1790323251000-S'), 'IDENTITY_MISMATCH'),
            (dict(FILL, symbol='BTC_USDT'), 'IDENTITY_MISMATCH'),
            (dict(FILL, side=1), 'IDENTITY_MISMATCH'),
            (dict(FILL, requested_quantity='43'), 'QUANTITY_MISMATCH'),
            (dict(FILL, cumulative_filled_quantity='45'), 'QUANTITY_MISMATCH'),
            (dict(FILL, exchange_fill_timestamp=BINDING[7] - 1), 'STALE_OR_INVALID_TIMESTAMP'),
            (dict(FILL, exchange_fill_timestamp=1790323258001), 'STALE_OR_INVALID_TIMESTAMP'),
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
                         (3, 'ORDER_SENT', '3', 'pnf-syn2-1790323251000-S', 'SUI_USDT', 'SHORT', '44', BINDING[7], 1))
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
            conn.execute('UPDATE live_trades SET submitted_at_ms=? WHERE id=1', (1790323258001,))
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
                if '?mode=ro' not in self.uri or 'immutable=' in self.uri or self.kwargs.get('uri') is not True:
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

    def test_duplicate_beyond_observation_limit_is_ambiguous(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                         (2, 'ORDER_SENT', '999', 'pnf-syn2-1790323251000-S',
                          'SUI_USDT', 'SHORT', '44', BINDING[7], 1))
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                         (3, 'ORDER_SENT', *BINDING[2:]))
        adapter = Adapter([])
        code, _ = self.execute(adapter, max_records=1)
        self.assertEqual((code, adapter.calls), (0, []))
        self.assertEqual(json.loads(self.report.read_text())['observations'][0]['classification'],
                         'CONFLICTING_EVIDENCE')

    def test_unknown_status_never_infers_quantity_state(self):
        for index, quantity in enumerate(('0', '20', '44')):
            for status in ('UNKNOWN', None):
                evidence = dict(FILL, cumulative_filled_quantity=quantity)
                if status is None:
                    evidence.pop('status')
                else:
                    evidence['status'] = status
                target = Path(self.tmp.name) / f'unknown-{index}-{status}.json'
                code, _ = self.execute(Adapter([{'success': True, 'data': evidence}]), report=target)
                self.assertEqual(code, 0)
                self.assertEqual(json.loads(target.read_text())['observations'][0]['classification'],
                                 'EVIDENCE_UNAVAILABLE')

    def test_previous_report_progression_regression_and_tampering(self):
        first = dict(FILL, status='NEW', cumulative_filled_quantity='0', average_fill_price='0')
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': first}]))[0], 0)
        partial = dict(FILL, status='PARTIALLY_FILLED', cumulative_filled_quantity='20')
        second = Path(self.tmp.name) / 'partial.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': partial}]),
                                      report=second, previous=self.report)[0], 0)
        self.assertEqual(json.loads(second.read_text())['observations'][0]['classification'], 'PARTIAL_FILL')
        final = Path(self.tmp.name) / 'final.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]),
                                      report=final, previous=second)[0], 0)
        self.assertEqual(json.loads(final.read_text())['observations'][0]['classification'], 'FULL_FILL_MATCH')
        for name, payload, prior in (
                ('regression', first, second),
                ('frozen', dict(FILL, average_fill_price='2'), final),
                ('timestamp', dict(FILL, exchange_fill_timestamp=BINDING[7]), final)):
            target = Path(self.tmp.name) / (name + '.json')
            self.assertEqual(self.execute(Adapter([{'success': True, 'data': payload}]),
                                          report=target, previous=prior)[0], 0)
            self.assertEqual(json.loads(target.read_text())['observations'][0]['classification'],
                             'CONFLICTING_EVIDENCE')
        self.assertEqual(self.report.read_text(), self.report.read_text())
        tampered = Path(self.tmp.name) / 'tampered.json'
        tampered.write_text(second.read_text().replace('PARTIAL_FILL', 'FULL_FILL_MATCH'))
        adapter = Adapter([])
        target = Path(self.tmp.name) / 'invalid.json'
        self.assertEqual(self.execute(adapter, report=target, previous=tampered)[0], 1)
        self.assertEqual(adapter.calls, [])
        self.assertFalse(target.exists())

    def test_final_anchor_survives_indeterminate_chain(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        first = json.loads(self.report.read_text())['observations'][0]
        previous = self.report
        for index, response in enumerate(({'success': True, 'data': dict(FILL, status='UNKNOWN')},
                                          TimeoutError('secret'), None)):
            target = Path(self.tmp.name) / f'unknown-chain-{index}.json'
            self.assertEqual(self.execute(Adapter([response]), report=target, previous=previous)[0], 0)
            row = json.loads(target.read_text())['observations'][0]
            self.assertEqual(row['classification'], 'EVIDENCE_UNAVAILABLE')
            self.assertEqual(row['confirmed_evidence_anchor'], first['confirmed_evidence_anchor'])
            self.assertEqual(row['confirmed_anchor_digest'], first['confirmed_anchor_digest'])
            previous = target
        mutated = Path(self.tmp.name) / 'mutated.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL, average_fill_price='2')}]),
                                      report=mutated, previous=previous)[0], 0)
        row = json.loads(mutated.read_text())['observations'][0]
        self.assertEqual(row['classification'], 'CONFLICTING_EVIDENCE')
        self.assertEqual(row['confirmed_evidence_anchor'], first['confirmed_evidence_anchor'])

    def test_prior_anchor_cannot_be_cleared_even_with_recomputed_report_hash(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        for index, alteration in enumerate(('missing', 'cleared', 'downgraded', 'chain')):
            report = json.loads(self.report.read_text())
            item = report['observations'][0]
            if alteration == 'missing':
                del item['confirmed_evidence_anchor']
            elif alteration == 'cleared':
                item['confirmed_evidence_anchor'] = None
            elif alteration == 'downgraded':
                item['confirmed_evidence_anchor']['status'] = 'NEW'
            else:
                item['history_chain_digest'] = '0' * 64
            report['report_sha256'] = observer._report_hash({k: v for k, v in report.items()
                                                             if k != 'report_sha256'})
            prior = Path(self.tmp.name) / f'forged-{index}.json'
            prior.write_text(json.dumps(report))
            adapter = Adapter([])
            target = Path(self.tmp.name) / f'after-forged-{index}.json'
            code, output = self.execute(adapter, report=target, previous=prior)
            self.assertEqual((code, output['reason'], adapter.calls),
                             (1, 'PREVIOUS_REPORT_INVALID', []))
            self.assertFalse(target.exists())

    def test_partial_unknown_progression_and_terminal_cancellation(self):
        partial = dict(FILL, status='PARTIALLY_FILLED', cumulative_filled_quantity='20')
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': partial}]))[0], 0)
        unknown = Path(self.tmp.name) / 'partial-unknown.json'
        self.assertEqual(self.execute(Adapter([None]), report=unknown, previous=self.report)[0], 0)
        original_anchor = json.loads(self.report.read_text())['observations'][0]['confirmed_evidence_anchor']
        self.assertEqual(json.loads(unknown.read_text())['observations'][0]['confirmed_evidence_anchor'],
                         original_anchor)
        for index, (response, expected) in enumerate((
                (dict(FILL, status='PARTIALLY_FILLED', cumulative_filled_quantity='19'),
                 'CONFLICTING_EVIDENCE'),
                (dict(FILL, status='PARTIALLY_FILLED', cumulative_filled_quantity='21',
                      exchange_fill_timestamp=BINDING[7]), 'CONFLICTING_EVIDENCE'),
                (dict(FILL, status='PARTIALLY_FILLED', cumulative_filled_quantity='30'),
                 'PARTIAL_FILL'),
                (dict(FILL, cumulative_filled_quantity='20', status='CANCELLED'),
                 'CANCELLED_OR_REJECTED'),
                (dict(FILL, cumulative_filled_quantity='20', status='REJECTED'),
                 'CANCELLED_OR_REJECTED'),
                (dict(FILL), 'FULL_FILL_MATCH'))):
            target = Path(self.tmp.name) / f'partial-next-{index}.json'
            self.assertEqual(self.execute(Adapter([{'success': True, 'data': response}]),
                                          report=target, previous=unknown)[0], 0)
            self.assertEqual(json.loads(target.read_text())['observations'][0]['classification'],
                             expected)
            if expected == 'CANCELLED_OR_REJECTED':
                terminal = json.loads(target.read_text())['observations'][0]
                self.assertEqual(terminal['confirmed_evidence_anchor']['cumulative_quantity'], '20')
                later = Path(self.tmp.name) / f'terminal-unknown-{index}.json'
                self.assertEqual(self.execute(Adapter([None]), report=later, previous=target)[0], 0)
                self.assertEqual(json.loads(later.read_text())['observations'][0]['confirmed_evidence_anchor'],
                                 terminal['confirmed_evidence_anchor'])

    def test_anchor_decimal_equivalence_long_short_and_repeatable_chain(self):
        for side, native_side, suffix in (('LONG', 1, 'L'), ('SHORT', 3, 'S')):
            oid = 'pnf-syn1-1790323251000-' + suffix
            with sqlite3.connect(self.db) as conn:
                conn.execute('UPDATE live_trades SET side=?,external_oid=?', (side, oid))
            fill = dict(FILL, side=native_side, external_oid=oid)
            original = Path(self.tmp.name) / f'original-{side}.json'
            self.assertEqual(self.execute(Adapter([{'success': True, 'data': fill}]),
                                          report=original)[0], 0)
            equivalent = dict(fill, requested_quantity='44.000',
                              cumulative_filled_quantity='44', average_fill_price='1.0150')
            outcomes = []
            for index in (1, 2):
                target = Path(self.tmp.name) / f'equal-{side}-{index}.json'
                self.assertEqual(self.execute(Adapter([{'success': True, 'data': equivalent}]),
                                              report=target, previous=original)[0], 0)
                outcomes.append(json.loads(target.read_text())['observations'][0])
            self.assertEqual(outcomes[0]['classification'], 'FULL_FILL_MATCH')
            self.assertEqual(outcomes[0]['confirmed_evidence_anchor'], outcomes[1]['confirmed_evidence_anchor'])
            self.assertEqual(outcomes[0]['history_chain_digest'], outcomes[1]['history_chain_digest'])

    def test_final_anchor_rejects_identity_quantity_and_time_mutations(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        frozen = json.loads(self.report.read_text())['observations'][0]['confirmed_evidence_anchor']
        changes = ({'exchange_order_id': '9'},
                   {'external_oid': 'pnf-syn2-1790323251000-S'},
                   {'symbol': 'BTC_USDT'}, {'side': 1},
                   {'requested_quantity': '43'},
                   {'cumulative_filled_quantity': '43'},
                   {'average_fill_price': '2'},
                   {'exchange_fill_timestamp': FILL['exchange_fill_timestamp'] + 1})
        for index, mutation in enumerate(changes):
            target = Path(self.tmp.name) / f'final-mutation-{index}.json'
            code, _ = self.execute(Adapter([{'success': True, 'data': dict(FILL, **mutation)}]),
                                   report=target, previous=self.report)
            self.assertEqual(code, 0)
            row = json.loads(target.read_text())['observations'][0]
            self.assertEqual(row['classification'], 'CONFLICTING_EVIDENCE')
            self.assertEqual(row['confirmed_evidence_anchor'], frozen)

    def test_downgraded_and_broken_chain_rejected_before_request(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        first = self.report
        unknown = Path(self.tmp.name) / 'chain-second.json'
        self.assertEqual(self.execute(Adapter([None]), report=unknown, previous=first)[0], 0)
        for index, mutation in enumerate(('version', 'previous_chain', 'skipped_chain',
                                          'reordered_chain', 'previous_anchor',
                                          'observation', 'status')):
            data = json.loads(unknown.read_text())
            row = data['observations'][0]
            if mutation == 'version':
                data['version'] = 2
            elif mutation == 'previous_chain':
                row['previous_history_chain_digest'] = '0' * 64
            elif mutation == 'skipped_chain':
                row['history_chain_digest'] = json.loads(first.read_text())['observations'][0]['history_chain_digest']
            elif mutation == 'reordered_chain':
                row['previous_history_chain_digest'], row['previous_confirmed_anchor_digest'] = (
                    row['previous_confirmed_anchor_digest'], row['previous_history_chain_digest'])
            elif mutation == 'previous_anchor':
                row['previous_confirmed_anchor_digest'] = '0' * 64
            elif mutation == 'observation':
                row['digest'] = '0' * 64
            else:
                row['classification'] = 'FULL_FILL_MATCH'
            data['report_sha256'] = observer._report_hash({key: value for key, value in data.items()
                                                           if key != 'report_sha256'})
            prior = Path(self.tmp.name) / f'broken-chain-{index}.json'
            prior.write_text(json.dumps(data))
            target = Path(self.tmp.name) / f'broken-result-{index}.json'
            adapter = Adapter([])
            code, output = self.execute(adapter, report=target, previous=prior)
            self.assertEqual((code, output['reason'], adapter.calls),
                             (1, 'PREVIOUS_REPORT_INVALID', []))
            self.assertFalse(target.exists())

    def test_multiple_duplicate_groups_outside_batch(self):
        with sqlite3.connect(self.db) as conn:
            for row in ((2, '900', 'pnf-syn2-1790323251000-S'),
                        (3, BINDING[2], BINDING[3]),
                        (4, '900', 'pnf-syn3-1790323251000-S')):
                conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                             (row[0], 'ORDER_SENT', row[1], row[2], 'SUI_USDT',
                              'SHORT', '44', BINDING[7], 1))
        adapter = Adapter([])
        self.assertEqual(self.execute(adapter, max_records=2)[0], 0)
        self.assertEqual(adapter.calls, [])
        self.assertEqual([x['reason'] for x in json.loads(self.report.read_text())['observations']],
                         ['AMBIGUOUS_OWNERSHIP', 'AMBIGUOUS_OWNERSHIP'])

    def test_two_connections_keep_integrity_decision_in_read_snapshot(self):
        original = sqlite3.connect
        writer = original(self.db, timeout=0.01)
        self.addCleanup(writer.close)
        before = self.db.read_bytes()
        class Reader:
            def __init__(self, path_uri, **kwargs):
                self.conn = original(path_uri, **kwargs)
            def execute(self, sql, *args):
                result = self.conn.execute(sql, *args)
                if sql == 'PRAGMA data_version':
                    writer.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                                   (2, 'ORDER_SENT', *BINDING[2:]))
                    with self_test.assertRaises(sqlite3.OperationalError):
                        writer.commit()
                    writer.rollback()
                return result
            def set_authorizer(self, fn):
                self.conn.set_authorizer(fn)
            def close(self):
                self.conn.close()
        self_test = self
        with patch.object(observer.sqlite3, 'connect', side_effect=Reader):
            code, _ = self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(self.report.read_text())['observations'][0]['classification'],
                         'FULL_FILL_MATCH')
        self.assertEqual(self.db.read_bytes(), before)
        self.assertFalse(self.db.with_name(self.db.name + '-journal').exists())

    def test_begin_failure_is_snapshot_unavailable(self):
        original = sqlite3.connect
        class Reader:
            def __init__(self, path_uri, **kwargs):
                self.conn = original(path_uri, **kwargs)
            def execute(self, sql, *args):
                if sql == 'BEGIN':
                    raise sqlite3.OperationalError('sensitive exception')
                return self.conn.execute(sql, *args)
            def set_authorizer(self, fn):
                self.conn.set_authorizer(fn)
            def close(self):
                self.conn.close()
        adapter = Adapter([])
        with patch.object(observer.sqlite3, 'connect', side_effect=Reader):
            code, output = self.execute(adapter)
        self.assertEqual((code, output['reason'], adapter.calls),
                         (1, 'SNAPSHOT_UNAVAILABLE', []))
        self.assertFalse(self.report.exists())


if __name__ == '__main__':
    unittest.main()
