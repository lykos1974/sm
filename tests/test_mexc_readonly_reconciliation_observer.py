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

    def execute(self, adapter, *, report=None, max_records=5, previous=None, genesis=True):
        out = io.StringIO()
        env = {'MEXC_FUTURES_API_KEY': 'synthetic-key',
               'MEXC_FUTURES_API_SECRET': 'synthetic-secret'}
        args = ['--database', str(self.db), '--output-report', str(report or self.report),
                '--max-records', str(max_records)]
        if previous is not None:
            args.extend(('--previous-report', str(previous)))
        elif genesis:
            args.append('--genesis')
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
        self.assertEqual(code, 0)
        self.assertNotIn((BINDING[2], BINDING[4]), adapter.calls)
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
            self.assertEqual(row['history_chain_digest'], first['history_chain_digest'])
            previous = target
        mutated = Path(self.tmp.name) / 'mutated.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL, average_fill_price='2')}]),
                                      report=mutated, previous=previous)[0], 0)
        row = json.loads(mutated.read_text())['observations'][0]
        self.assertEqual(row['classification'], 'CONFLICTING_EVIDENCE')
        self.assertEqual(row['confirmed_evidence_anchor'], first['confirmed_evidence_anchor'])

    def test_two_bindings_early_stop_keeps_unobserved_final_anchor(self):
        second = (2, 'ORDER_SENT', '900', 'pnf-syn2-1790323251000-S',
                  'SUI_USDT', 'SHORT', '44', BINDING[7], 1)
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)', second)
        second_fill = dict(FILL, exchange_order_id='900', external_oid=second[3])
        first_adapter = Adapter([{'success': True, 'data': dict(FILL)},
                                 {'success': True, 'data': second_fill}])
        self.assertEqual(self.execute(first_adapter)[0], 0)
        first_report = json.loads(self.report.read_text())
        self.assertEqual(first_report['registry_count'], 2)
        middle = Path(self.tmp.name) / 'middle.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL, status='UNKNOWN')}]),
                                      report=middle, previous=self.report)[0], 0)
        middle_report = json.loads(middle.read_text())
        self.assertEqual(middle_report['registry_count'], 2)
        self.assertEqual(middle_report['binding_registry'][1]['confirmed_evidence_anchor'],
                         first_report['binding_registry'][1]['confirmed_evidence_anchor'])
        self.assertEqual(middle_report['binding_registry'][1]['history_chain_digest'],
                         first_report['binding_registry'][1]['history_chain_digest'])
        self.assertEqual(middle_report['binding_registry'][0]['history_chain_digest'],
                         first_report['binding_registry'][0]['history_chain_digest'])
        last = Path(self.tmp.name) / 'last.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)},
                                               {'success': True, 'data': dict(second_fill, average_fill_price='2')}]),
                                      report=last, previous=middle)[0], 0)
        self.assertEqual(json.loads(last.read_text())['observations'][1]['classification'],
                         'CONFLICTING_EVIDENCE')

    def test_genesis_is_explicit(self):
        adapter = Adapter([])
        code, result = self.execute(adapter, genesis=False)
        self.assertEqual((code, adapter.calls), (1, []))
        self.assertEqual(result['reason'], 'INVALID_INPUT')
        self.assertFalse(self.report.exists())

    def test_registry_survives_shrinking_batch_disappearance_and_reappearance(self):
        second = (2, 'ORDER_SENT', '900', 'pnf-syn2-1790323251000-S',
                  'SUI_USDT', 'SHORT', '44', BINDING[7], 1)
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)', second)
        fill2 = dict(FILL, exchange_order_id='900', external_oid=second[3])
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)},
                                               {'success': True, 'data': fill2}]))[0], 0)
        prior = self.report
        initial = {row['binding_digest']: row for row in json.loads(prior.read_text())['binding_registry']}
        for index, maximum in enumerate((1, 1)):
            current = Path(self.tmp.name) / f'small-{index}.json'
            adapter = Adapter([{'success': True, 'data': dict(FILL)}])
            self.assertEqual(self.execute(adapter, report=current, previous=prior,
                                          max_records=maximum)[0], 0)
            self.assertEqual(len(adapter.calls), 1)
            rows = json.loads(current.read_text())['binding_registry']
            second_entry = next(x for x in rows if x['exchange_order_identity_digest'] ==
                                observer._report_hash({'order_id': '900'}))
            old = initial[second_entry['binding_digest']]
            for field in ('confirmed_evidence_anchor', 'confirmed_anchor_digest',
                          'history_chain_digest', 'last_successful_observation'):
                self.assertEqual(second_entry[field], old[field])
            self.assertEqual(second_entry['current_observation_state'], 'UNOBSERVED')
            prior = current
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE live_trades SET status='FILLED' WHERE id=2")
        missing = Path(self.tmp.name) / 'missing-row.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]),
                                      report=missing, previous=prior)[0], 0)
        absent = next(x for x in json.loads(missing.read_text())['binding_registry']
                      if x['exchange_order_identity_digest'] == observer._report_hash({'order_id': '900'}))
        self.assertEqual(absent['current_observation_state'], 'NOT_PRESENT_IN_CURRENT_SNAPSHOT')
        self.assertEqual(absent['history_chain_digest'], old['history_chain_digest'])
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE live_trades SET status='ORDER_SENT' WHERE id=2")
        later = Path(self.tmp.name) / 'reappeared.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)},
                                               {'success': True, 'data': dict(fill2, average_fill_price='2')}]),
                                      report=later, previous=missing, max_records=2)[0], 0)
        self.assertEqual(json.loads(later.read_text())['observations'][1]['classification'],
                         'CONFLICTING_EVIDENCE')

    def test_local_identity_reuse_and_mutated_binding_never_genesis(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        with sqlite3.connect(self.db) as conn:
            conn.execute('UPDATE live_trades SET exchange_order_id=? WHERE id=1', ('900',))
        current = Path(self.tmp.name) / 'identity-reuse.json'
        adapter = Adapter([])
        self.assertEqual(self.execute(adapter, report=current, previous=self.report)[0], 0)
        self.assertEqual(adapter.calls, [])
        result = json.loads(current.read_text())
        self.assertEqual(result['observations'][0]['classification'], 'CONFLICTING_EVIDENCE')
        self.assertEqual(result['registry_count'], 2)
        self.assertEqual({row['lineage_origin'] for row in result['binding_registry']},
                         {'GENESIS', 'CONFLICT'})
        old = next(row for row in result['binding_registry'] if row['lineage_origin'] == 'GENESIS')
        self.assertEqual(old['confirmed_evidence_anchor']['status'], 'FILLED')

    def test_regression_a_final_fill_unknown_changed_price(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        original = json.loads(self.report.read_text())['binding_registry'][0]
        unknown_report = Path(self.tmp.name) / 'regression-a-unknown.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL, status='UNKNOWN')}]),
                                      report=unknown_report, previous=self.report)[0], 0)
        unknown = json.loads(unknown_report.read_text())
        self.assertEqual(unknown['observations'][0]['classification'], 'EVIDENCE_UNAVAILABLE')
        self.assertEqual(unknown['binding_registry'][0]['confirmed_evidence_anchor'],
                         original['confirmed_evidence_anchor'])
        changed_report = Path(self.tmp.name) / 'regression-a-changed.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL, average_fill_price='2')}]),
                                      report=changed_report, previous=unknown_report)[0], 0)
        changed = json.loads(changed_report.read_text())
        self.assertEqual(changed['observations'][0]['classification'], 'CONFLICTING_EVIDENCE')
        self.assertEqual(changed['binding_registry'][0]['confirmed_evidence_anchor'],
                         original['confirmed_evidence_anchor'])

    def test_regression_b_two_fills_unknown_skip_second_changed_price(self):
        second = (2, 'ORDER_SENT', '900', 'pnf-syn2-1790323251000-S',
                  'SUI_USDT', 'SHORT', '44', BINDING[7], 1)
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)', second)
        second_fill = dict(FILL, exchange_order_id=second[2], external_oid=second[3])
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)},
                                               {'success': True, 'data': second_fill}]))[0], 0)
        original = json.loads(self.report.read_text())
        old_second = next(x for x in original['binding_registry'] if x['exchange_order_identity_digest'] ==
                          observer._report_hash({'order_id': second[2]}))
        unknown_report = Path(self.tmp.name) / 'regression-b-unknown.json'
        adapter = Adapter([{'success': True, 'data': dict(FILL, status='UNKNOWN')}])
        self.assertEqual(self.execute(adapter, report=unknown_report, previous=self.report,
                                      max_records=2)[0], 0)
        self.assertEqual(adapter.calls, [(BINDING[2], BINDING[4])])
        interim = json.loads(unknown_report.read_text())
        retained = next(x for x in interim['binding_registry'] if x['binding_digest'] ==
                        old_second['binding_digest'])
        self.assertEqual(retained['confirmed_evidence_anchor'], old_second['confirmed_evidence_anchor'])
        self.assertEqual(retained['history_chain_digest'], old_second['history_chain_digest'])
        changed_report = Path(self.tmp.name) / 'regression-b-changed.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)},
                                               {'success': True, 'data': dict(second_fill,
                                                                              average_fill_price='2')}]),
                                      report=changed_report, previous=unknown_report,
                                      max_records=2)[0], 0)
        changed = json.loads(changed_report.read_text())
        self.assertEqual(changed['observations'][1]['classification'], 'CONFLICTING_EVIDENCE')

    def test_regression_c_mutated_local_quantity_conflict_persists_on_retry(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        original = json.loads(self.report.read_text())['binding_registry'][0]
        with sqlite3.connect(self.db) as conn:
            conn.execute('UPDATE live_trades SET requested_quantity=? WHERE id=?', ('45', BINDING[0]))
        first_conflict = Path(self.tmp.name) / 'regression-c-conflict.json'
        no_requests = Adapter([])
        self.assertEqual(self.execute(no_requests, report=first_conflict,
                                      previous=self.report)[0], 0)
        self.assertEqual(no_requests.calls, [])
        conflict = json.loads(first_conflict.read_text())
        self.assertEqual(conflict['observations'][0]['classification'], 'CONFLICTING_EVIDENCE')
        self.assertEqual(conflict['registry_count'], 2)
        prior_frozen = next(x for x in conflict['binding_registry'] if x['binding_digest'] ==
                            original['binding_digest'])
        self.assertEqual(prior_frozen['confirmed_evidence_anchor'], original['confirmed_evidence_anchor'])
        retry = Path(self.tmp.name) / 'regression-c-retry.json'
        mutated_fill = dict(FILL, requested_quantity='45', cumulative_filled_quantity='45',
                            average_fill_price='2')
        adapter = Adapter([{'success': True, 'data': mutated_fill}])
        self.assertEqual(self.execute(adapter, report=retry, previous=first_conflict)[0], 0)
        result = json.loads(retry.read_text())
        self.assertEqual((result['observations'][0]['classification'], adapter.calls),
                         ('CONFLICTING_EVIDENCE', []),
                         'persisted binding conflict must block all adapter calls')
        frozen = next(x for x in result['binding_registry'] if x['binding_digest'] ==
                      original['binding_digest'])
        self.assertEqual(frozen['confirmed_evidence_anchor'], original['confirmed_evidence_anchor'])

    def test_conflict_survives_retries_disappearance_restoration_and_limits(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        original = json.loads(self.report.read_text())['binding_registry'][0]
        with sqlite3.connect(self.db) as conn:
            conn.execute('UPDATE live_trades SET requested_quantity=? WHERE id=1', ('45',))
        first = Path(self.tmp.name) / 'permanent-conflict-0.json'
        self.assertEqual(self.execute(Adapter([]), report=first, previous=self.report)[0], 0)
        conflict_key = next(x['binding_digest'] for x in json.loads(first.read_text())['binding_registry']
                            if x['lineage_origin'] == 'CONFLICT')
        previous = first
        fill45 = dict(FILL, requested_quantity='45', cumulative_filled_quantity='45',
                      average_fill_price='2')
        for index, (quantity, present, maximum) in enumerate((
                ('45', True, 1), ('45', True, 2), ('45', False, 1),
                ('45', True, 2), ('44', True, 1), ('45', True, 2))):
            with sqlite3.connect(self.db) as conn:
                conn.execute('UPDATE live_trades SET requested_quantity=?,status=? WHERE id=1',
                             (quantity, 'ORDER_SENT' if present else 'FILLED'))
            current = Path(self.tmp.name) / f'permanent-conflict-{index + 1}.json'
            adapter = Adapter([{'success': True, 'data': fill45}])
            self.assertEqual(self.execute(adapter, report=current, previous=previous,
                                          max_records=maximum)[0], 0)
            self.assertEqual(adapter.calls, [], 'conflicted binding must never call adapter')
            report = json.loads(current.read_text())
            entries = {row['binding_digest']: row for row in report['binding_registry']}
            self.assertEqual(entries[conflict_key]['lineage_origin'], 'CONFLICT')
            self.assertEqual(entries[conflict_key]['current_observation_state'],
                             'CONFLICTING_EVIDENCE')
            self.assertEqual(entries[original['binding_digest']]['confirmed_evidence_anchor'],
                             original['confirmed_evidence_anchor'])
            if present:
                self.assertEqual(report['observations'][0]['classification'], 'CONFLICTING_EVIDENCE')
            previous = current

    def test_conflicted_binding_never_blocks_unrelated_valid_observation(self):
        second = (2, 'ORDER_SENT', '900', 'pnf-syn2-1790323251000-S',
                  'SUI_USDT', 'SHORT', '44', BINDING[7], 1)
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)', second)
        fill2 = dict(FILL, exchange_order_id=second[2], external_oid=second[3])
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)},
                                               {'success': True, 'data': fill2}]))[0], 0)
        with sqlite3.connect(self.db) as conn:
            conn.execute('UPDATE live_trades SET requested_quantity=? WHERE id=1', ('45',))
        first = Path(self.tmp.name) / 'unrelated-conflict.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': fill2}]),
                                      report=first, previous=self.report, max_records=1)[0], 0)
        previous = first
        for index, (response, maximum) in enumerate((
                (dict(fill2), 2), (dict(fill2, status='UNKNOWN'), 1),
                (TimeoutError('secret'), 2), (dict(fill2), 1))):
            current = Path(self.tmp.name) / f'unrelated-retry-{index}.json'
            adapter = Adapter([{'success': True, 'data': response}
                               if not isinstance(response, Exception) else response])
            self.assertEqual(self.execute(adapter, report=current, previous=previous,
                                          max_records=maximum)[0], 0)
            self.assertEqual(adapter.calls, [(second[2], second[4])])
            report = json.loads(current.read_text())
            self.assertEqual(report['observations'][0]['classification'], 'CONFLICTING_EVIDENCE')
            self.assertEqual(report['registry_count'], 3)
            previous = current

    def test_persisted_fill_evidence_conflict_blocks_matching_retry(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        conflict = Path(self.tmp.name) / 'frozen-fill-conflict.json'
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL,
                                            average_fill_price='2')}]),
                                      report=conflict, previous=self.report)[0], 0)
        self.assertEqual(json.loads(conflict.read_text())['observations'][0]['classification'],
                         'CONFLICTING_EVIDENCE')
        retry = Path(self.tmp.name) / 'frozen-fill-retry.json'
        adapter = Adapter([{'success': True, 'data': dict(FILL)}])
        self.assertEqual(self.execute(adapter, report=retry, previous=conflict)[0], 0)
        self.assertEqual(adapter.calls, [])
        self.assertEqual(json.loads(retry.read_text())['observations'][0]['classification'],
                         'CONFLICTING_EVIDENCE')

    def test_persisted_ambiguous_ownership_blocks_after_duplicate_disappears(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                         (2, 'ORDER_SENT', *BINDING[2:]))
        initial_adapter = Adapter([])
        self.assertEqual(self.execute(initial_adapter)[0], 0)
        self.assertEqual(initial_adapter.calls, [])
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE live_trades SET status='FILLED' WHERE id=2")
        retry = Path(self.tmp.name) / 'ambiguous-retry.json'
        adapter = Adapter([{'success': True, 'data': dict(FILL)}])
        self.assertEqual(self.execute(adapter, report=retry, previous=self.report)[0], 0)
        self.assertEqual(adapter.calls, [])
        self.assertEqual(json.loads(retry.read_text())['observations'][0]['classification'],
                         'CONFLICTING_EVIDENCE')

    def test_unobserved_duplicate_ownership_survives_unknown_and_restart(self):
        for stop_at_middle in (False, True):
            with self.subTest(stop_at_middle=stop_at_middle):
                with sqlite3.connect(self.db) as conn:
                    conn.execute('DELETE FROM live_trades WHERE id > 1')
                    if stop_at_middle:
                        conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                                     (2, 'ORDER_SENT', '9002', 'pnf-syn2-1790323251000-S',
                                      'SUI_USDT', 'SHORT', '44', BINDING[7], 1))
                        duplicate_ids = (3, 4)
                    else:
                        duplicate_ids = (2, 3)
                    for trade_id in duplicate_ids:
                        conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                                     (trade_id, 'ORDER_SENT', '9003',
                                      'pnf-syn9-1790323251000-S', 'SUI_USDT', 'SHORT',
                                      '44', BINDING[7], 1))
                unknown = dict(FILL, status='UNKNOWN')
                first = Path(self.tmp.name) / f'duplicates-first-{stop_at_middle}.json'
                initial = Adapter([{'success': True, 'data': dict(FILL)}] if stop_at_middle else
                                  [{'success': True, 'data': unknown}])
                if stop_at_middle:
                    initial.responses.append({'success': True, 'data': unknown})
                self.assertEqual(self.execute(initial, report=first,
                                              max_records=1 if not stop_at_middle else 2)[0], 0)
                data = json.loads(first.read_text())
                self.assertEqual(data['observations'][-1]['classification'],
                                 'EVIDENCE_UNAVAILABLE')
                implicated = [entry for entry in data['binding_registry']
                              if entry['present_in_snapshot'] and entry['binding_digest']
                              not in {item['binding_digest'] for item in data['observations']}]
                self.assertEqual(len(implicated), 2)
                self.assertTrue(all(entry['current_observation_state'] == 'CONFLICTING_EVIDENCE'
                                    for entry in implicated))
                with sqlite3.connect(self.db) as conn:
                    conn.execute('DELETE FROM live_trades WHERE id=?', (duplicate_ids[1],))
                retry = Path(self.tmp.name) / f'duplicates-retry-{stop_at_middle}.json'
                valid = Adapter([{'success': True, 'data': dict(FILL)},
                                 {'success': True, 'data': dict(FILL)}])
                self.assertEqual(self.execute(valid, report=retry, previous=first,
                                              max_records=2)[0], 0)
                retried = json.loads(retry.read_text())
                self.assertEqual(retried['observations'][-1]['classification'],
                                 'CONFLICTING_EVIDENCE')
                self.assertNotIn(('9003', 'SUI_USDT'), valid.calls)
                self.assertEqual(len(valid.calls), 1 if not stop_at_middle else 2)
                self.assertTrue(all(entry['current_observation_state'] == 'CONFLICTING_EVIDENCE'
                                    for entry in retried['binding_registry']
                                    if entry['binding_digest'] in
                                    {item['binding_digest'] for item in implicated}))
                with sqlite3.connect(self.db) as conn:
                    conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                                 (duplicate_ids[1], 'ORDER_SENT', '9003',
                                  'pnf-syn9-1790323251000-S', 'SUI_USDT', 'SHORT',
                                  '44', BINDING[7], 1))
                previous = retry
                for maximum in (1, 2):
                    restarted = Path(self.tmp.name) / (f'duplicates-restarted-'
                                 f'{stop_at_middle}-{maximum}.json')
                    adapter = Adapter([{'success': True, 'data': dict(FILL)}] * maximum)
                    self.assertEqual(self.execute(adapter, report=restarted,
                                                  previous=previous, max_records=maximum)[0], 0)
                    current = json.loads(restarted.read_text())
                    self.assertNotIn(('9003', 'SUI_USDT'), adapter.calls)
                    self.assertTrue(all(entry['current_observation_state'] == 'CONFLICTING_EVIDENCE'
                                        for entry in current['binding_registry']
                                        if entry['binding_digest'] in
                                        {item['binding_digest'] for item in implicated}))
                    previous = restarted

    def test_tampered_registry_fails_before_adapter_and_report_publication(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        for index, mutation in enumerate(('missing', 'duplicate', 'cleared', 'root',
                                          'chain', 'extra', 'old_schema')):
            forged = json.loads(self.report.read_text())
            entries = forged['binding_registry']
            if mutation == 'missing':
                entries.pop()
            elif mutation == 'duplicate':
                entries.append(dict(entries[0]))
            elif mutation == 'cleared':
                entries[0]['confirmed_evidence_anchor'] = None
            elif mutation == 'root':
                forged['registry_root_digest'] = '0' * 64
            elif mutation == 'chain':
                entries[0]['history_chain_digest'] = '0' * 64
            elif mutation == 'extra':
                entries[0]['unexpected'] = 'unsafe'
            else:
                forged['version'] -= 1
            forged['report_sha256'] = observer._report_hash({k: v for k, v in forged.items()
                                                             if k != 'report_sha256'})
            previous = Path(self.tmp.name) / f'invalid-registry-{index}.json'
            previous.write_text(json.dumps(forged))
            destination = Path(self.tmp.name) / f'after-invalid-registry-{index}.json'
            adapter = Adapter([])
            code, output = self.execute(adapter, report=destination, previous=previous)
            self.assertEqual((code, output['reason'], adapter.calls),
                             (1, 'PREVIOUS_REPORT_INVALID', []))
            self.assertFalse(destination.exists())

    def test_three_bindings_stop_at_each_position_and_resume(self):
        bindings = [BINDING]
        with sqlite3.connect(self.db) as conn:
            for trade_id in (2, 3):
                row = (trade_id, 'ORDER_SENT', str(900 + trade_id),
                       f'pnf-syn{trade_id}-1790323251000-S', 'SUI_USDT', 'SHORT',
                       '44', BINDING[7], 1)
                conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)', row)
                bindings.append(row)
        fills = [dict(FILL, exchange_order_id=row[2], external_oid=row[3]) for row in bindings]
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': x} for x in fills]))[0], 0)
        original = json.loads(self.report.read_text())
        for stop_at, failure in ((0, None), (1, TimeoutError('secret')),
                                 (2, RuntimeError('secret'))):
            responses = ([{'success': True, 'data': fills[i]} for i in range(stop_at)] +
                         [failure])
            middle = Path(self.tmp.name) / f'early-stop-{stop_at}.json'
            adapter = Adapter(responses)
            self.assertEqual(self.execute(adapter, report=middle, previous=self.report)[0], 0)
            middle_data = json.loads(middle.read_text())
            self.assertEqual(len(adapter.calls), stop_at + 1)
            self.assertEqual(middle_data['registry_count'], 3)
            old = {r['binding_digest']: r for r in original['binding_registry']}
            for entry in middle_data['binding_registry']:
                if entry['exchange_order_identity_digest'] != observer._report_hash(
                        {'order_id': bindings[stop_at][2]}) and entry['current_observation_state'] == 'UNOBSERVED':
                    self.assertEqual(entry['history_chain_digest'], old[entry['binding_digest']]['history_chain_digest'])
                    self.assertEqual(entry['confirmed_evidence_anchor'], old[entry['binding_digest']]['confirmed_evidence_anchor'])
            final = Path(self.tmp.name) / f'early-stop-final-{stop_at}.json'
            later = list(fills)
            later[stop_at] = dict(later[stop_at], average_fill_price='2')
            self.assertEqual(self.execute(Adapter([{'success': True, 'data': x} for x in later]),
                                          report=final, previous=middle)[0], 0)
            self.assertEqual(json.loads(final.read_text())['observations'][stop_at]['classification'],
                             'CONFLICTING_EVIDENCE')

    def test_reordered_registry_entries_rejected_before_exchange(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO live_trades VALUES(?,?,?,?,?,?,?,?,?)',
                         (2, 'ORDER_SENT', '900', 'pnf-syn2-1790323251000-S',
                          'SUI_USDT', 'SHORT', '44', BINDING[7], 1))
        second = dict(FILL, exchange_order_id='900', external_oid='pnf-syn2-1790323251000-S')
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)},
                                               {'success': True, 'data': second}]))[0], 0)
        data = json.loads(self.report.read_text())
        data['binding_registry'].reverse()
        data['registry_root_digest'] = observer._report_hash(data['binding_registry'])
        data['report_chain_digest'] = observer._report_chain(data)
        data['report_sha256'] = observer._report_hash({key: value for key, value in data.items()
                                                      if key != 'report_sha256'})
        tampered = Path(self.tmp.name) / 'reordered-registry.json'
        tampered.write_text(json.dumps(data))
        adapter = Adapter([])
        destination = Path(self.tmp.name) / 'after-reorder.json'
        code, result = self.execute(adapter, report=destination, previous=tampered)
        self.assertEqual((code, result['reason'], adapter.calls),
                         (1, 'PREVIOUS_REPORT_INVALID', []))
        self.assertFalse(destination.exists())

    def test_extra_or_missing_registry_binding_rejected_with_recomputed_hashes(self):
        self.assertEqual(self.execute(Adapter([{'success': True, 'data': dict(FILL)}]))[0], 0)
        for index, operation in enumerate(('extra', 'extra_absent', 'missing')):
            data = json.loads(self.report.read_text())
            if operation.startswith('extra'):
                invented = dict(data['binding_registry'][0])
                invented['binding_digest'] = 'f' * 64
                invented['confirmed_evidence_anchor'] = None
                invented['confirmed_anchor_digest'] = observer._EMPTY_ANCHOR_DIGEST
                invented['history_chain_digest'] = observer._GENESIS_CHAIN_DIGEST
                invented['last_successful_observation'] = None
                invented['lineage_origin'] = 'GENESIS'
                invented['conflicting_prior_binding_digest'] = None
                invented['current_observation_state'] = 'GENESIS'
                if operation == 'extra_absent':
                    invented['present_in_snapshot'] = False
                    invented['current_observation_state'] = 'NOT_PRESENT_IN_CURRENT_SNAPSHOT'
                data['binding_registry'].append(invented)
                data['binding_registry'].sort(key=lambda row: row['binding_digest'])
            else:
                data['binding_registry'].clear()
            data['registry_count'] = len(data['binding_registry'])
            data['registry_root_digest'] = observer._report_hash(data['binding_registry'])
            data['report_chain_digest'] = observer._report_chain(data)
            data['report_sha256'] = observer._report_hash({k: v for k, v in data.items()
                                                           if k != 'report_sha256'})
            prior = Path(self.tmp.name) / f'invented-registry-{index}.json'
            prior.write_text(json.dumps(data))
            destination = Path(self.tmp.name) / f'invented-out-{index}.json'
            adapter = Adapter([])
            code, output = self.execute(adapter, report=destination, previous=prior)
            self.assertEqual((code, output['reason'], adapter.calls),
                             (1, 'PREVIOUS_REPORT_INVALID', []))
            self.assertFalse(destination.exists())

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
                row['history_chain_digest'] = observer._GENESIS_CHAIN_DIGEST
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
        self.assertEqual([x['reason'] for x in json.loads(self.report.read_text())['observations'][:2]],
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
