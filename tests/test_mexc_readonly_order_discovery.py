"""Synthetic, offline tests of the standalone GET-only discovery tool."""
import contextlib
import hashlib
import hmac
import io
import json
import os
import sqlite3
import socket
import unittest
import urllib.request
import urllib.error
from unittest.mock import patch

import mexc_readonly_order_discovery as discovery

SYMBOL = 'BTC_USDT'
ORDER = {'orderId': '739211854408619008', 'symbol': SYMBOL, 'side': 1, 'state': 3,
         'vol': '3.000000000000000001', 'dealVol': '3.000000000000000001',
         'dealAvgPriceStr': '100.500000000000000001', 'updateTime': 1761912240000}


class Transport:
    def __init__(self, body):
        self.body, self.calls = body, []

    def get(self, url, headers, timeout):
        self.calls.append((url, headers, timeout))
        if isinstance(self.body, Exception):
            raise self.body
        return self.body if isinstance(self.body, bytes) else json.dumps(self.body).encode()


class DiscoveryTests(unittest.TestCase):
    def assert_diagnostic(self, result, reason, stage, *, status=None, envelope='UNAVAILABLE',
                          container='UNAVAILABLE', pagination='UNAVAILABLE', rows=None,
                          index=None, field=None):
        self.assertEqual(result, {'status': 'FAIL', 'reason': reason,
                                  'diagnostic': {'endpoint': 'HISTORY_ORDERS',
                                                 'http_status': status, 'stage': stage,
                                                 'envelope': envelope, 'container': container,
                                                 'pagination': pagination, 'row_count': rows,
                                                 'row_index': index, 'field': field}})

    def run_cli(self, body, args=('--symbol', SYMBOL), credentials=True):
        transport = Transport(body)
        output = io.StringIO()
        env = {'MEXC_FUTURES_API_KEY': 'synthetic-key',
               'MEXC_FUTURES_API_SECRET': 'synthetic-secret'} if credentials else {}
        with patch.dict(os.environ, env, clear=True), patch.object(sqlite3, 'connect', side_effect=AssertionError('DB')):
            with patch.object(urllib.request, 'build_opener', side_effect=AssertionError('network')):
                with patch.object(urllib.request, 'urlopen', side_effect=AssertionError('network')):
                    with contextlib.redirect_stdout(output):
                        code = discovery.main(list(args), transport=transport)
        return code, json.loads(output.getvalue()), transport.calls

    def test_completed_order_one_allowlisted_get_and_exact_decimal(self):
        body = {'success': True, 'code': 0, 'data': [ORDER]}
        code, result, calls = self.run_cli(body)
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], discovery.BASE_URL + discovery.HISTORY_PATH +
                         '?page_num=1&page_size=5&states=3&symbol=BTC_USDT')
        self.assertEqual(result, {'status': 'PASS', 'orders': [{
            'order_id': ORDER['orderId'], 'native_symbol': SYMBOL, 'side': 'LONG',
            'requested_quantity': ORDER['vol'], 'filled_quantity': ORDER['dealVol'],
            'average_fill_price': ORDER['dealAvgPriceStr'], 'fill_time': None,
            'status': 'FILLED'}]})
        self.assertNotIn('synthetic-key', str(result))
        self.assertNotIn('synthetic-secret', str(result))

    def test_exact_signature_and_transmitted_query_match(self):
        with patch.object(discovery.time, 'time', return_value=1761912240):
            code, _, calls = self.run_cli({'success': True, 'code': 0, 'data': [ORDER]})
        self.assertEqual(code, 0)
        url, headers, _ = calls[0]
        query = 'page_num=1&page_size=5&states=3&symbol=BTC_USDT'
        self.assertEqual(url.split('?', 1)[1], query)
        self.assertEqual(headers['Request-Time'], '1761912240000')
        signed_payload = 'synthetic-key1761912240000' + query
        expected = hmac.new(b'synthetic-secret', signed_payload.encode(), hashlib.sha256).hexdigest()
        self.assertEqual(headers['Signature'], expected)

    def test_canonical_parameters_are_order_independent_and_encoded(self):
        first = [('symbol', 'BTC USDT/ü'), ('states', 3), ('page_size', 5), ('page_num', 1)]
        second = list(reversed(first))
        expected = 'page_num=1&page_size=5&states=3&symbol=BTC%20USDT%2F%C3%BC'
        self.assertEqual(discovery._canonical_parameters(first), expected)
        self.assertEqual(discovery._canonical_parameters(second), expected)
        for pairs in ([('states', 3), ('states', 3)], [('', 'a')],
                      [('symbol', None)], [('states', True)], [('states', 3.0)],
                      [('states', ['3'])], [('symbol', '')],
                      [('Signature', 'bad')], [('ApiKey', 'bad')],
                      [('Request-Time', 'bad')], [('signature', 'bad')]):
            with self.subTest(pairs=pairs), self.assertRaises(ValueError):
                discovery._canonical_parameters(pairs)

    def test_invalid_inputs_and_missing_credentials_never_call_transport(self):
        for args, credentials in ((('--symbol', '../BTC_USDT'), True),
                                  (('--symbol', SYMBOL, '--limit', '101'), True),
                                  (('--symbol', SYMBOL), False)):
            code, result, calls = self.run_cli({}, args, credentials)
            self.assertEqual(code, 1)
            self.assertEqual(calls, [])
            self.assertEqual(result['status'], 'FAIL')

    def test_ambiguous_records_fail_closed(self):
        for broken in (dict(ORDER, dealVol='2'), dict(ORDER, dealAvgPriceStr='NaN'),
                       dict(ORDER, side=5), dict(ORDER, symbol='ETH_USDT'),
                       dict(ORDER, updateTime=None), dict(ORDER, orderId='bad')):
            with self.subTest(broken=broken):
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [broken]})
                self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))

    def test_all_four_exact_sides_and_closing_orders_are_validated_before_filtering(self):
        rows = [dict(ORDER, side=side, orderId=str(side)) for side in (1, 2, 3, 4)]
        for envelope in (rows, {'currentPage': 1, 'pageSize': 5, 'resultList': rows}):
            with self.subTest(container=type(envelope).__name__):
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': envelope})
                self.assertEqual(code, 0)
                self.assertEqual([(order['order_id'], order['side']) for order in result['orders']],
                                 [('1', 'LONG'), ('3', 'SHORT')])
        for side in (2, 4):
            code, result, _ = self.run_cli({'success': True, 'code': 0, 'data':
                                            [dict(ORDER, side=side)]})
            self.assertEqual((code, result), (0, {'status': 'PASS', 'orders': []}))
            for field in ('orderId', 'symbol', 'vol', 'dealVol', 'dealAvgPriceStr', 'updateTime'):
                with self.subTest(side=side, field=field):
                    row = dict(ORDER, side=side)
                    del row[field]
                    code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [row]})
                    self.assertEqual((code, result),
                                     (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))
        for side in (None, True, 2.0, '2', 0, 5, {}, []):
            with self.subTest(side=side):
                code, result, _ = self.run_cli({'success': True, 'code': 0,
                                                'data': [dict(ORDER, side=side)]})
                self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))
        row = dict(ORDER, side=4, dealAvgPrice='200')
        code, result, _ = self.run_cli({'success': True, 'code': 0,
                                        'data': [dict(ORDER), row]})
        self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))
        for broken in (dict(ORDER, side=4, order_side=3),
                       dict(ORDER, side=2, dealVol='2'),
                       dict(ORDER, side=4, symbol='ETH_USDT'),
                       dict(ORDER, side=2, fillTime=1, fill_time=2)):
            with self.subTest(broken=broken):
                code, result, _ = self.run_cli({'success': True, 'code': 0,
                                                'data': [dict(ORDER), broken]})
                self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))

    def test_conflicting_economic_aliases_fail_closed(self):
        aliases = {'order_id': '2', 'native_symbol': 'ETH_USDT', 'order_side': 3,
                   'requested_quantity': '2', 'filled_quantity': '2',
                   'dealAvgPrice': '200', 'status': 4, 'update_time': 1761912240001}
        for alias, value in aliases.items():
            with self.subTest(alias=alias):
                body = {'success': True, 'code': 0, 'data': [dict(ORDER, **{alias: value})]}
                code, result, _ = self.run_cli(body, args=('--symbol', SYMBOL, '--diagnostic'))
                self.assertEqual(code, 1)
                self.assert_diagnostic(result, 'RESPONSE_SCHEMA', 'RESPONSE', envelope='OBJECT',
                                       container='LIST', pagination='LIST', rows=1, index=0,
                                       field={'order_id': 'ORDER_ID', 'native_symbol': 'SYMBOL',
                                              'order_side': 'SIDE', 'requested_quantity': 'QUANTITY',
                                              'filled_quantity': 'QUANTITY', 'dealAvgPrice': 'PRICE',
                                              'status': 'STATE', 'update_time': 'TIMESTAMP'}[alias])

    def test_only_integer_state_three_is_accepted_for_every_row(self):
        for state in (None, 1, 2, 4, 5, 0, 6, True, 3.0, '3', 'bad', [], {}):
            with self.subTest(state=state):
                row = dict(ORDER, state=state)
                code, result, _ = self.run_cli(
                    {'success': True, 'code': 0, 'data': [row]},
                    args=('--symbol', SYMBOL, '--diagnostic'))
                self.assertEqual(code, 1)
                self.assert_diagnostic(result, 'RESPONSE_SCHEMA', 'RESPONSE', envelope='OBJECT',
                                       container='LIST', pagination='LIST', rows=1, index=0, field='STATE')
        for rows in ([{'state': 4}], [dict(ORDER), {'state': 4}],
                     [dict(ORDER), dict(ORDER, orderId='2', state=4)]):
            with self.subTest(rows=rows):
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': rows})
                self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))

    def test_state_three_requires_all_discovery_evidence_and_consistent_aliases(self):
        for field in ('orderId', 'symbol', 'side', 'vol', 'dealVol',
                      'dealAvgPriceStr', 'updateTime'):
            with self.subTest(field=field):
                row = dict(ORDER)
                del row[field]
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [row]})
                self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))
        for alias, value in (('status', '3'), ('status', 4), ('dealAvgPrice', '200'),
                             ('fillTime', 100), ('fill_time', 101)):
            with self.subTest(alias=alias):
                row = dict(ORDER, **{alias: value})
                if alias in ('fillTime', 'fill_time'):
                    row['fillTime'], row['fill_time'] = 100, 101
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [row]})
                self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))

    def test_equivalent_aliases_use_exact_decimal_and_reject_invalid(self):
        with self.assertRaises(ValueError):
            discovery._positive(100.0)
        with self.assertRaises(ValueError):
            discovery._positive(True)
        row = dict(ORDER, order_id=ORDER['orderId'], native_symbol=SYMBOL, order_side=1,
                   requested_quantity='3.000000000000000001', filled_quantity=ORDER['dealVol'],
                   dealAvgPrice='100.5000000000000000010', status=3,
                   update_time=ORDER['updateTime'])
        code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [row]})
        self.assertEqual(code, 0)
        self.assertEqual(result['orders'][0]['average_fill_price'], ORDER['dealAvgPriceStr'])
        for value in (100.5, True, 'NaN', 'Infinity', 'bad'):
            with self.subTest(value=value):
                row['dealAvgPrice'] = value
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [row]})
                self.assertEqual((code, result['reason']), (1, 'RESPONSE_SCHEMA'))
        for value in ('100', '100.0', 100):
            with self.subTest(equivalent=value):
                row = dict(ORDER, dealAvgPriceStr='100', dealAvgPrice=value)
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [row]})
                self.assertEqual(code, 0)
                self.assertEqual(result['orders'][0]['average_fill_price'], '100')

    def test_missing_required_evidence_and_conflicting_fill_time_aliases(self):
        for field in ('orderId', 'symbol', 'side', 'state', 'vol', 'dealVol',
                      'dealAvgPriceStr', 'updateTime'):
            with self.subTest(field=field):
                row = dict(ORDER)
                del row[field]
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [row]})
                self.assertEqual((code, result['reason']), (1, 'RESPONSE_SCHEMA'))
        row = dict(ORDER, fillTime=1761912240000, fill_time=1761912240001)
        code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [row]})
        self.assertEqual((code, result['reason']), (1, 'RESPONSE_SCHEMA'))

    def test_duplicate_json_keys_and_transport_failure_fail_closed(self):
        for body in (b'{"success":true,"success":true,"code":0,"data":[]}',
                     b'{"success":true,"code":0,"data":[{"orderId":"1","orderId":"1"}]}',
                     b'{broken'):
            code, result, _ = self.run_cli(body)
            self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))

    def test_bounded_diagnostic_classifies_envelope_container_and_late_row_without_values(self):
        cases = (
            (b'{"success":true,"success":true,"code":0,"data":[]}',
             'UNAVAILABLE', 'UNAVAILABLE', 'UNAVAILABLE', None, None, 'JSON'),
            (b'[]', 'OTHER', 'UNAVAILABLE', 'UNAVAILABLE', None, None, 'ENVELOPE'),
            ({'success': True, 'code': 0},
             'OBJECT', 'MISSING', 'INVALID', None, None, 'CONTAINER'),
            (b'{"success":true,"code":0,"data":[],"secret":"synthetic-secret"}',
             'OBJECT', 'LIST', 'LIST', 0, None, None),
            ({'success': True, 'code': 0, 'data': 'synthetic-secret'},
             'OBJECT', 'OTHER', 'INVALID', None, None, 'CONTAINER'),
            ({'success': True, 'code': 0, 'data': {'currentPage': 2, 'pageSize': 5,
                                                  'resultList': [ORDER]}},
             'OBJECT', 'OBJECT', 'INVALID', None, None, 'PAGINATION'),
            ({'success': True, 'code': 0, 'data': [ORDER, dict(ORDER, side=4, orderId='2',
                                                               dealAvgPriceStr='NaN')]},
             'OBJECT', 'LIST', 'LIST', 2, 1, 'PRICE'),
            ({'success': True, 'code': 0, 'data': [dict(ORDER, side=4, orderId='2',
                                                               dealAvgPriceStr='NaN')]},
             'OBJECT', 'LIST', 'LIST', 1, 0, 'PRICE'),
        )
        for body, envelope, container, pagination, rows, index, field in cases:
            with self.subTest(field=field, rows=rows):
                code, result, _ = self.run_cli(body, args=('--symbol', SYMBOL, '--diagnostic'))
                if field is None:
                    self.assertEqual(result, {'status': 'PASS', 'diagnostic': {
                        'endpoint': 'HISTORY_ORDERS', 'http_status': None, 'stage': 'VALID',
                        'envelope': envelope, 'container': container, 'pagination': pagination,
                        'row_count': rows, 'row_index': None, 'field': None}})
                    continue
                self.assertEqual(code, 1)
                self.assert_diagnostic(result, 'RESPONSE_SCHEMA', 'RESPONSE',
                                       envelope=envelope, container=container, pagination=pagination,
                                       rows=rows, index=index, field=field)
                rendered = json.dumps(result)
                for sensitive in ('synthetic-secret', 'synthetic-key', SYMBOL, ORDER['orderId'],
                                  'NaN', 'https://', 'ApiKey', 'Signature'):
                    self.assertNotIn(sensitive, rendered)

    def test_diagnostic_missing_credentials_and_invalid_input_have_no_evidence(self):
        for args, credentials, reason, stage in (
                (('--symbol', SYMBOL), False, 'MISSING_CREDENTIALS', 'CREDENTIALS'),
                (('--symbol', '../SECRET', '--diagnostic'), True, 'INVALID_INPUT', 'INPUT')):
            code, result, calls = self.run_cli({}, args=(*args, '--diagnostic') if credentials is False else args,
                                               credentials=credentials)
            self.assertEqual((code, calls), (1, []))
            self.assert_diagnostic(result, reason, stage, field=stage)

    def test_request_stage_failure_is_sanitized_and_does_not_call_transport(self):
        with patch.object(discovery, '_canonical_parameters', side_effect=ValueError('synthetic-secret')):
            code, result, calls = self.run_cli({}, args=('--symbol', SYMBOL, '--diagnostic'))
        self.assertEqual((code, calls), (1, []))
        self.assert_diagnostic(result, 'INTERNAL_ERROR', 'REQUEST', field='REQUEST')

    def test_deterministic_diagnostics_redact_exceptions_and_response(self):
        cases = (
            (urllib.error.HTTPError('https://secret.invalid/key', 302, 'synthetic-secret', {}, None), 'HTTP_STATUS', 302, 'TRANSPORT', 'UNAVAILABLE', 'UNAVAILABLE', 'UNAVAILABLE', 'TRANSPORT'),
            (urllib.error.HTTPError('https://secret.invalid/key', 401, 'synthetic-secret', {}, None), 'AUTH_REJECTED', 401, 'TRANSPORT', 'UNAVAILABLE', 'UNAVAILABLE', 'UNAVAILABLE', 'TRANSPORT'),
            (urllib.error.HTTPError('https://secret.invalid/key', 500, 'synthetic-secret', {}, None), 'HTTP_STATUS', 500, 'TRANSPORT', 'UNAVAILABLE', 'UNAVAILABLE', 'UNAVAILABLE', 'TRANSPORT'),
            ({'success': False, 'code': 602, 'msg': 'synthetic-secret'}, 'AUTH_REJECTED', None, 'API', 'OBJECT', 'UNAVAILABLE', 'UNAVAILABLE', 'API'),
            ({'success': False, 'code': 600, 'msg': 'synthetic-secret'}, 'API_REJECTED', None, 'API', 'OBJECT', 'UNAVAILABLE', 'UNAVAILABLE', 'API'),
            (b'{"success":true,"code":0,"data":{"resultList":[]}}', 'RESPONSE_SCHEMA', None, 'RESPONSE', 'OBJECT', 'OBJECT', 'INVALID', 'PAGINATION'),
            (socket.timeout('synthetic-secret'), 'NETWORK_ERROR', None, 'TRANSPORT', 'UNAVAILABLE', 'UNAVAILABLE', 'UNAVAILABLE', 'TRANSPORT'),
            (urllib.error.URLError('synthetic-secret'), 'NETWORK_ERROR', None, 'TRANSPORT', 'UNAVAILABLE', 'UNAVAILABLE', 'UNAVAILABLE', 'TRANSPORT'),
            (RuntimeError('synthetic-secret'), 'INTERNAL_ERROR', None, 'INTERNAL', 'UNAVAILABLE', 'UNAVAILABLE', 'UNAVAILABLE', 'INTERNAL'),
        )
        for body, reason, http_status, stage, envelope, container, pagination, field in cases:
            with self.subTest(reason=reason, body=type(body).__name__):
                code, result, calls = self.run_cli(body, args=('--symbol', SYMBOL, '--diagnostic'))
                self.assertEqual(code, 1)
                self.assertEqual(len(calls), 1)
                self.assert_diagnostic(result, reason, stage, status=http_status, envelope=envelope,
                                       container=container, pagination=pagination, field=field)
                rendered = json.dumps(result)
                for forbidden in ('synthetic-secret', 'synthetic-key', 'Signature', 'Request-Time',
                                  'https://', 'orderId', SYMBOL):
                    self.assertNotIn(forbidden, rendered)

    def test_diagnostic_is_opt_in_and_success_does_not_expose_headers(self):
        body = {'success': True, 'code': 0, 'data': [ORDER]}
        code, result, _ = self.run_cli(body, args=('--symbol', SYMBOL, '--diagnostic'))
        self.assertEqual(code, 0)
        self.assertEqual(result, {'status': 'PASS', 'diagnostic': {
            'endpoint': 'HISTORY_ORDERS', 'http_status': None, 'stage': 'VALID',
            'envelope': 'OBJECT', 'container': 'LIST', 'pagination': 'LIST',
            'row_count': 1, 'row_index': None, 'field': None}})
        code, result, _ = self.run_cli(urllib.error.URLError('synthetic-secret'))
        self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'NETWORK_ERROR'}))
        for data, container, pagination in (
                ([dict(ORDER, side=4)], 'LIST', 'LIST'),
                ({'currentPage': 1, 'pageSize': 5,
                  'resultList': [dict(ORDER, side=2)]}, 'OBJECT', 'VALID')):
            with self.subTest(container=container):
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': data},
                                                args=('--symbol', SYMBOL, '--diagnostic'))
                self.assertEqual(code, 0)
                self.assertEqual(result, {'status': 'PASS', 'diagnostic': {
                    'endpoint': 'HISTORY_ORDERS', 'http_status': None, 'stage': 'VALID',
                    'envelope': 'OBJECT', 'container': container, 'pagination': pagination,
                    'row_count': 1, 'row_index': None, 'field': None}})

    def test_non_2xx_response_object_is_http_status_without_body(self):
        class Response:
            status = 429

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self):
                raise AssertionError('must not read rejected response')

        class Opener:
            def open(self, request, timeout):
                self.assert_get(request)
                return Response()

            def assert_get(self, request):
                if request.get_method() != 'GET':
                    raise AssertionError('unexpected method')

        # Offline production transport: a synthetic non-2xx response never yields evidence.
        with patch.object(discovery.urllib.request, 'build_opener', return_value=Opener()):
            code, result, _ = self.run_cli_with_production_transport()
        self.assertEqual(code, 1)
        self.assert_diagnostic(result, 'HTTP_STATUS', 'TRANSPORT', status=429, field='TRANSPORT')

    def test_actual_success_http_status_is_available_on_schema_failure(self):
        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self):
                return b'{"success":true,"code":0,"data":[{"state":4}]}'

        class Opener:
            def open(self, request, timeout):
                if request.get_method() != 'GET':
                    raise AssertionError('non-GET request')
                return Response()

        with patch.object(discovery.urllib.request, 'build_opener', return_value=Opener()):
            code, result, _ = self.run_cli_with_production_transport()
        self.assertEqual(code, 1)
        self.assert_diagnostic(result, 'RESPONSE_SCHEMA', 'RESPONSE', status=200,
                               envelope='OBJECT', container='LIST', pagination='LIST',
                               rows=1, index=0, field='STATE')

    def run_cli_with_production_transport(self):
        output = io.StringIO()
        with patch.dict(os.environ, {'MEXC_FUTURES_API_KEY': 'synthetic-key',
                                     'MEXC_FUTURES_API_SECRET': 'synthetic-secret'}, clear=True):
            with patch.object(sqlite3, 'connect', side_effect=AssertionError('DB')):
                with patch.object(urllib.request, 'urlopen', side_effect=AssertionError('network')):
                    with contextlib.redirect_stdout(output):
                        code = discovery.main(['--symbol', SYMBOL, '--diagnostic'])
        return code, json.loads(output.getvalue()), None

    def test_reject_every_other_path_before_transport(self):
        transport = Transport(b'{}')
        for path in ('/api/v1/private/order/create', '/api/v1/private/order/cancel',
                     '/api/v1/private/order/list/history_orders/../cancel',
                     '/api/v1/private/order/list/history_orders?symbol=BTC_USDT&states=4',
                     'https://evil.example/api/v1/private/order/list/history_orders',
                     '/API/v1/private/order/list/history_orders'):
            with self.assertRaises(ValueError):
                discovery._checked_url(discovery.BASE_URL + path)
        self.assertEqual(transport.calls, [])

    def test_production_transport_get_only_and_redirect_denial(self):
        self.assertFalse(hasattr(discovery._GetOnlyTransport(), 'post'))
        self.assertFalse(hasattr(discovery._GetOnlyTransport(), 'cancel'))
        self.assertIsNone(discovery._NoRedirect().redirect_request(None, None, 302, '', {},
                                                                    'https://evil.example/'))
        url = (discovery.BASE_URL + discovery.HISTORY_PATH +
               '?page_num=1&page_size=5&states=3&symbol=BTC_USDT')
        for status in (301, 302, 303, 307, 308):
            class FakeOpener:
                def open(self, request, timeout):
                    self.assert_request(request)
                    raise urllib.error.HTTPError(url, status, 'redirect',
                                                 {'Location': 'https://evil.example/'}, None)

                def assert_request(self, request):
                    if request.get_method() != 'GET':
                        raise AssertionError('non-GET request')
            with self.subTest(status=status), patch.object(urllib.request, 'build_opener', return_value=FakeOpener()) as build:
                with self.assertRaises(urllib.error.HTTPError):
                    discovery._GetOnlyTransport().get(url, {}, 15)
                self.assertEqual(build.call_count, 1)
                self.assertIsInstance(build.call_args.args[0], discovery._NoRedirect)


if __name__ == '__main__':
    unittest.main()
