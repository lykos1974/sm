"""Offline externalOid lookup regressions; no exchange or order actions."""
import hashlib
import hmac
import json
import os
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from mexc_readonly_order_status import (
    BASE_URL, ReadOnlyMexcExternalOidRecovery, _checked_path, _GetOnlyTransport,
)


SYMBOL = 'SUI_USDT'
OID = 'synthetic-oid_1'
PATH = '/api/v1/private/order/external/SUI_USDT/synthetic-oid_1'
ROW = {'externalOid': OID, 'orderId': '123456', 'symbol': SYMBOL, 'side': 1,
       'vol': '44', 'dealVol': '44.0', 'dealAvgPriceStr': '1.015',
       'dealAvgPrice': '1.0150', 'state': 3, 'updateTime': 1761912240000}


class Transport:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    def get(self, url, headers, timeout):
        self.calls.append((url, headers, timeout))
        if self.error:
            raise self.error
        return self.response


def envelope(row=ROW, *, success=True, code=0):
    return json.dumps({'success': success, 'code': code, 'data': row}).encode()


class RecoveryTests(unittest.TestCase):
    def adapter(self, response=None, error=None, enabled=True):
        transport = Transport(response if response is not None else envelope(), error)
        with patch.dict(os.environ, {'MEXC_FUTURES_API_KEY': 'synthetic-key',
                                     'MEXC_FUTURES_API_SECRET': 'synthetic-secret'}):
            adapter = ReadOnlyMexcExternalOidRecovery(enabled=enabled, transport=transport)
        return adapter, transport

    def test_full_fill_exact_binding_signature_and_idempotence(self):
        adapter, transport = self.adapter()
        with patch('mexc_readonly_order_status.time.time', return_value=1761912240):
            result = adapter.recover(SYMBOL, OID)
            self.assertEqual(adapter.recover(SYMBOL, OID), result)
        self.assertEqual(result, {'outcome': 'FULL_FILL', 'evidence': {
            'external_oid': OID, 'exchange_order_id': '123456', 'symbol': SYMBOL,
            'side': 'LONG', 'requested_quantity': '44',
            'cumulative_filled_quantity': '44', 'status': 'FILLED',
            'average_fill_price': '1.015', 'exchange_update_timestamp_ms': 1761912240000}})
        self.assertEqual(len(transport.calls), 2)
        for url, headers, timeout in transport.calls:
            self.assertEqual((url, timeout), (BASE_URL + PATH, 15))
            self.assertEqual(headers['ApiKey'], 'synthetic-key')
            self.assertEqual(headers['Request-Time'], '1761912240000')
            self.assertEqual(headers['Signature'], hmac.new(b'synthetic-secret',
                b'synthetic-key1761912240000', hashlib.sha256).hexdigest())
            self.assertNotIn('synthetic-secret', repr(result))

    def test_statuses_are_explicit_and_never_grant_resubmission(self):
        examples = ((ROW | {'state': 2, 'dealVol': '10', 'dealAvgPriceStr': '1.015',
                            'dealAvgPrice': '1.015'}, 'PARTIAL_FILL'),
                    (ROW | {'state': 1, 'dealVol': '0', 'dealAvgPriceStr': '0',
                            'dealAvgPrice': '0'}, 'ZERO_FILL'),
                    (ROW | {'state': 4, 'dealVol': '0', 'dealAvgPriceStr': '0',
                            'dealAvgPrice': '0'}, 'CANCELLED'),
                    (ROW | {'state': 5, 'dealVol': '0', 'dealAvgPriceStr': '0',
                            'dealAvgPrice': '0'}, 'REJECTED'),
                    (ROW | {'state': 99}, 'UNKNOWN'))
        for row, outcome in examples:
            with self.subTest(outcome=outcome):
                adapter, _ = self.adapter(envelope(row))
                result = adapter.recover(SYMBOL, OID)
                self.assertEqual(result['outcome'], outcome)
                self.assertNotIn('may_resubmit', result)
        for raw in (envelope(None, success=False, code=2040),):
            adapter, _ = self.adapter(raw)
            self.assertEqual(adapter.recover(SYMBOL, OID), {'outcome': 'NOT_FOUND', 'evidence': None})
        adapter, _ = self.adapter(envelope(None, success=True, code=0))
        self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        adapter, _ = self.adapter(envelope(ROW | {'side': 3, 'vol': '100000',
             'dealVol': '100000.0', 'dealAvgPriceStr': '1.500', 'dealAvgPrice': '1.5'}))
        self.assertEqual(adapter.recover(SYMBOL, OID)['evidence']['side'], 'SHORT')
        self.assertEqual(adapter.recover(SYMBOL, OID)['evidence']['requested_quantity'], '100000')
        for row in (ROW | {'state': 3, 'dealVol': '10', 'dealAvgPriceStr': '1.015',
                            'dealAvgPrice': '1.015'},
                    ROW | {'state': 2, 'dealVol': '44'},
                    ROW | {'state': 4, 'dealVol': '44'}):
            adapter, _ = self.adapter(envelope(row))
            self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')

    def test_malformed_and_conflicting_evidence_fails_closed(self):
        changes = ({'externalOid': 'wrong'}, {'symbol': 'BTC_USDT'}, {'orderId': ''},
                   {'side': 2}, {'side': True}, {'side': 1.0}, {'state': '3'},
                   {'vol': True}, {'vol': 2.5}, {'vol': 'NaN'},
                   {'dealVol': '45'}, {'dealVol': '0'},
                   {'dealAvgPrice': '2'}, {'updateTime': False}, {'updateTime': 0},
                   {'external_oid': 'other'}, {'order_id': '777'},
                   {'native_symbol': 'BTC_USDT'}, {'requested_quantity': '43'},
                   {'cumulative_filled_quantity': '43'}, {'status': 4},
                   {'average_fill_price': '2'}, {'update_time': 1761912240001})
        for change in changes:
            with self.subTest(change=change):
                adapter, _ = self.adapter(envelope(ROW | change))
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        for key in ROW.keys() - {'dealAvgPrice', 'dealAvgPriceStr'}:
            with self.subTest(missing=key):
                adapter, _ = self.adapter(envelope({k: v for k, v in ROW.items() if k != key}))
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        for raw in (b'{"success":true,"success":true,"code":0,"data":{}}',
                    b'{"success":true,"code":0,"data":{"side":1,"side":1}}',
                    b'{"success":true,"code":0,"data":{"nested":{"x":1,"x":1}}}',
                    b'not-json', b'{"success":true,"code":0,"data":{"vol":NaN}}'):
            adapter, _ = self.adapter(raw)
            self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')

    def test_invalid_inputs_missing_credentials_and_transport_errors(self):
        for symbol, oid in (('SUI/USDT', OID), (SYMBOL, '../cancel'),
                            (SYMBOL, 'a%2Fb'), (SYMBOL, 'x?y=1'),
                            (SYMBOL, 'X'*33), ('sui_usdt', OID), (SYMBOL, '')):
            adapter, transport = self.adapter()
            self.assertEqual(adapter.recover(symbol, oid)['outcome'], 'UNKNOWN')
            self.assertEqual(transport.calls, [])
        with patch.dict(os.environ, {'MEXC_FUTURES_API_KEY': '',
                                     'MEXC_FUTURES_API_SECRET': ''}):
            transport = Transport(envelope())
            adapter = ReadOnlyMexcExternalOidRecovery(enabled=True, transport=transport)
        self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'TRANSPORT_FAILURE')
        self.assertFalse(transport.calls)
        for error in (TimeoutError('synthetic-secret'), OSError('synthetic-secret')) + tuple(
                HTTPError('secret-url', code, 'synthetic-secret', {}, None)
                for code in (301, 302, 303, 307, 308)):
            adapter, _ = self.adapter(error=error)
            self.assertEqual(adapter.recover(SYMBOL, OID),
                             {'outcome': 'TRANSPORT_FAILURE', 'evidence': None})
        adapter, transport = self.adapter(enabled=False)
        self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'TRANSPORT_FAILURE')
        self.assertFalse(transport.calls)

    def test_only_allowlisted_get_no_redirect_or_order_action(self):
        self.assertEqual(_checked_path(PATH), PATH)
        for path in (PATH + '/', PATH + '?x=1', PATH.replace('external', 'cancel'),
                     '/api/v1/private/order/submit', '//api/v1/private/order/external/' +
                     SYMBOL + '/' + OID, PATH.replace('SUI_USDT', 'SUI%5FUSDT'),
                     PATH.replace('external', 'EXTERNAL'), 'https://evil.example' + PATH):
            with self.subTest(path=path), self.assertRaises(ValueError):
                _checked_path(path)
            with self.assertRaises(ValueError):
                _GetOnlyTransport().get(BASE_URL + path, {}, 1)
        class FakeOpener:
            def open(self, request, timeout):
                self.request = request
                raise HTTPError(request.full_url, 302, 'redirect', {'Location': 'https://evil.example'}, None)
        for code in (301, 302, 303, 307, 308):
            opener = FakeOpener()
            def denied(request, timeout):
                opener.request = request
                raise HTTPError(request.full_url, code, 'redirect',
                                {'Location': 'https://evil.example'}, None)
            opener.open = denied
            with patch('mexc_readonly_order_status.urllib.request.build_opener',
                       return_value=opener) as build:
                with self.assertRaises(HTTPError):
                    _GetOnlyTransport().get(BASE_URL + PATH, {'ApiKey': 'synthetic-key'}, 1)
                self.assertEqual(opener.request.get_method(), 'GET')
                self.assertEqual(build.call_count, 1)
                self.assertEqual(type(build.call_args.args[0]).__name__, '_NoRedirect')


if __name__ == '__main__':
    unittest.main()
