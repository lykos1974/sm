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
        body = {'success': True, 'code': 0, 'data': [ORDER, dict(ORDER, orderId='2', state=4)]}
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
                       dict(ORDER, side=2), dict(ORDER, symbol='ETH_USDT'),
                       dict(ORDER, updateTime=None), dict(ORDER, orderId='bad')):
            with self.subTest(broken=broken):
                code, result, _ = self.run_cli({'success': True, 'code': 0, 'data': [broken]})
                self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))

    def test_duplicate_json_keys_and_transport_failure_fail_closed(self):
        for body in (b'{"success":true,"success":true,"code":0,"data":[]}',
                     b'{"success":true,"code":0,"data":[{"orderId":"1","orderId":"1"}]}',
                     b'{broken'):
            code, result, _ = self.run_cli(body)
            self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'RESPONSE_SCHEMA'}))

    def test_deterministic_diagnostics_redact_exceptions_and_response(self):
        cases = (
            (urllib.error.HTTPError('https://secret.invalid/key', 302, 'synthetic-secret', {}, None), 'HTTP_STATUS', 302, 'TRANSPORT'),
            (urllib.error.HTTPError('https://secret.invalid/key', 401, 'synthetic-secret', {}, None), 'AUTH_REJECTED', 401, 'TRANSPORT'),
            (urllib.error.HTTPError('https://secret.invalid/key', 500, 'synthetic-secret', {}, None), 'HTTP_STATUS', 500, 'TRANSPORT'),
            ({'success': False, 'code': 602, 'msg': 'synthetic-secret'}, 'AUTH_REJECTED', None, 'API'),
            ({'success': False, 'code': 600, 'msg': 'synthetic-secret'}, 'API_REJECTED', None, 'API'),
            (b'{"success":true,"code":0,"data":{"resultList":[]}}', 'RESPONSE_SCHEMA', None, 'RESPONSE'),
            (socket.timeout('synthetic-secret'), 'NETWORK_ERROR', None, 'TRANSPORT'),
            (urllib.error.URLError('synthetic-secret'), 'NETWORK_ERROR', None, 'TRANSPORT'),
            (RuntimeError('synthetic-secret'), 'INTERNAL_ERROR', None, 'INTERNAL'),
        )
        for body, reason, http_status, schema in cases:
            with self.subTest(reason=reason, body=type(body).__name__):
                code, result, calls = self.run_cli(body, args=('--symbol', SYMBOL, '--diagnostic'))
                self.assertEqual(code, 1)
                self.assertEqual(len(calls), 1)
                self.assertEqual(result, {'status': 'FAIL', 'reason': reason,
                                          'diagnostic': {'endpoint': 'HISTORY_ORDERS',
                                                         'http_status': http_status, 'schema': schema}})
                rendered = json.dumps(result)
                for forbidden in ('synthetic-secret', 'synthetic-key', 'Signature', 'Request-Time',
                                  'https://', 'orderId', SYMBOL):
                    self.assertNotIn(forbidden, rendered)

    def test_diagnostic_is_opt_in_and_success_does_not_expose_headers(self):
        body = {'success': True, 'code': 0, 'data': [ORDER]}
        code, result, _ = self.run_cli(body, args=('--symbol', SYMBOL, '--diagnostic'))
        self.assertEqual(code, 0)
        self.assertEqual(result['diagnostic'], {'endpoint': 'HISTORY_ORDERS',
                                                'http_status': None, 'schema': 'VALID'})
        code, result, _ = self.run_cli(urllib.error.URLError('synthetic-secret'))
        self.assertEqual((code, result), (1, {'status': 'FAIL', 'reason': 'NETWORK_ERROR'}))

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
        self.assertEqual(result, {'status': 'FAIL', 'reason': 'HTTP_STATUS',
                                  'diagnostic': {'endpoint': 'HISTORY_ORDERS',
                                                 'http_status': 429, 'schema': 'TRANSPORT'}})

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
