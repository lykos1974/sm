"""Offline externalOid lookup regressions; no exchange or order actions."""
import hashlib
import hmac
import json
import os
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from mexc_readonly_order_status import (
    BASE_URL, ReadOnlyMexcExternalOidRecovery, _checked_external_path, _ExternalGetOnlyTransport,
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

    def test_price_alias_matrix_and_all_economic_alias_conflicts(self):
        for value in ('2', None, False, 1.5, 'NaN', 'Infinity', '-1', '0', '', 'x'):
            with self.subTest(value=value):
                adapter, _ = self.adapter(envelope(ROW | {'dealAvgPrice': value}))
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        for name in ('dealAvgPriceStr', 'dealAvgPrice'):
            for value in (None, True, 1.5, 'NaN', '-1', '0'):
                with self.subTest(name=name, value=value):
                    adapter, _ = self.adapter(envelope(ROW | {name: value}))
                    self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        adapter, _ = self.adapter(envelope({k: v for k, v in ROW.items()
                                            if k not in ('dealAvgPriceStr', 'dealAvgPrice')}))
        self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        for alias, value in (('external_oid', 'other'), ('order_id', '2'),
                             ('native_symbol', 'ETH_USDT'), ('requested_quantity', '45'),
                             ('cumulative_filled_quantity', '43'), ('status', 4),
                             ('update_time', 1761912240001), ('average_fill_price', '2')):
            with self.subTest(alias=alias):
                adapter, _ = self.adapter(envelope(ROW | {alias: value}))
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        equivalent = ROW | {'external_oid': OID, 'order_id': 123456,
                            'native_symbol': SYMBOL, 'requested_quantity': '44.000',
                            'cumulative_filled_quantity': 44, 'status': 3,
                            'update_time': 1761912240000, 'average_fill_price': '1.01500'}
        adapter, _ = self.adapter(envelope(equivalent))
        self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'FULL_FILL')

    def test_recovery_bounded_complete_alias_matrix(self):
        aliases = {'order_id': ('order_id', 999), 'external_oid': ('external_oid', 'other'),
                   'symbol': ('native_symbol', 'ETH_USDT'), 'side': ('order_side', 3),
                   'requested': ('requested_quantity', '45'),
                   'filled': ('cumulative_filled_quantity', '43'),
                   'price': ('average_fill_price', '2'), 'status': ('status', 4),
                   'time': ('update_time', 1)}
        for name, (alias, value) in aliases.items():
            with self.subTest(field=name):
                adapter, _ = self.adapter(envelope(ROW | {alias: value}))
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        adapter, _ = self.adapter(envelope(ROW | dict(aliases.values())))
        self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        equivalent = ROW | dict(order_id=123456, external_oid=OID, native_symbol=SYMBOL,
                                order_side=1, requested_quantity='44.000',
                                cumulative_filled_quantity=44, average_fill_price='1.01500',
                                status=3, update_time=1761912240000)
        adapter, _ = self.adapter(envelope(equivalent))
        self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'FULL_FILL')
        domains = ((('order_id', True), ('order_id', 1.5), ('order_id', 'x'),
                    ('external_oid', None), ('external_oid', 'x/y'),
                    ('native_symbol', None), ('native_symbol', 'sui_usdt'),
                    ('order_side', True), ('order_side', 1.0), ('order_side', '1'),
                    ('requested_quantity', True), ('requested_quantity', 1.5),
                    ('requested_quantity', '-1'), ('requested_quantity', '1e101'),
                    ('cumulative_filled_quantity', True), ('cumulative_filled_quantity', 1.5),
                    ('cumulative_filled_quantity', '-1'), ('average_fill_price', True),
                    ('average_fill_price', 1.5), ('average_fill_price', 'NaN'),
                    ('status', True), ('status', 3.0), ('status', '3'),
                    ('update_time', True), ('update_time', '1761912240000'),
                    ('update_time', 9_000_000_000_000_001)))
        for alias, value in domains:
            with self.subTest(alias=alias, value=value):
                adapter, _ = self.adapter(envelope(ROW | {alias: value}))
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        for alias, value in (('status', 3.0), ('update_time', 1761912240000.0)):
            with self.subTest(equivalent_float=alias):
                adapter, _ = self.adapter(envelope(ROW | {alias: value}))
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')

    def test_numeric_tokens_and_unrelated_fee_are_exact_and_bounded(self):
        for token in (b'1.0150', b'1.01500', b'1015e-3'):
            with self.subTest(price_token=token):
                raw = envelope().replace(b'"1.0150"', token)
                adapter, _ = self.adapter(raw)
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'FULL_FILL')
        for field, value in (('requested_quantity', 44.0),
                             ('cumulative_filled_quantity', 44.0),
                             ('average_fill_price', 1.015)):
            with self.subTest(numeric_alias=field):
                adapter, _ = self.adapter(envelope(ROW | {field: value}))
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'FULL_FILL')
        raw = envelope().replace(b'"orderId"', b'"takerFee":0.0001,"orderId"')
        adapter, _ = self.adapter(raw)
        self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'FULL_FILL')
        for token in (b'44.0', b'44e0'):
            with self.subTest(requested_token=token):
                raw = envelope().replace(b'"vol": "44"', b'"vol": ' + token)
                adapter, _ = self.adapter(raw)
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'FULL_FILL')
        for token in (b'1.0160', b'1016e-3', b'1e101', b'1e999999',
                      b'1.' + b'0' * 130, b'NaN', b'Infinity', b'-Infinity'):
            with self.subTest(invalid_token=token):
                adapter, _ = self.adapter(envelope().replace(b'"1.0150"', token))
                self.assertEqual(adapter.recover(SYMBOL, OID)['outcome'], 'UNKNOWN')
        for token in (b'1e101', b'1e999999', b'1.' + b'0' * 130,
                      b'9' * 130, b'NaN', b'Infinity', b'-Infinity'):
            with self.subTest(invalid_unrelated_fee=token):
                raw = envelope().replace(b'"orderId"', b'"takerFee":' + token + b',"orderId"')
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
        self.assertEqual(_checked_external_path(PATH), PATH)
        adapter, injected = self.adapter()
        self.assertFalse(hasattr(adapter, 'get_order_status'))
        self.assertFalse(hasattr(adapter, '_get'))
        self.assertFalse(hasattr(adapter, '_raw_get'))
        for forbidden in ('/api/v1/private/order/get/123456',
                          '/api/v1/private/order/deal_details/123456'):
            with self.assertRaises(ValueError):
                _checked_external_path(forbidden)
            with self.assertRaises(ValueError):
                _ExternalGetOnlyTransport().get(BASE_URL + forbidden, {}, 1)
        self.assertEqual(injected.calls, [])
        for path in (PATH + '/', PATH + '?x=1', PATH.replace('external', 'cancel'),
                     '/api/v1/private/order/submit', '//api/v1/private/order/external/' +
                     SYMBOL + '/' + OID, PATH.replace('SUI_USDT', 'SUI%5FUSDT'),
                     PATH.replace('external', 'EXTERNAL'), 'https://evil.example' + PATH):
            with self.subTest(path=path), self.assertRaises(ValueError):
                _checked_external_path(path)
            with self.assertRaises(ValueError):
                _ExternalGetOnlyTransport().get(BASE_URL + path, {}, 1)
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
                    _ExternalGetOnlyTransport().get(BASE_URL + PATH, {'ApiKey': 'synthetic-key'}, 1)
                self.assertEqual(opener.request.get_method(), 'GET')
                self.assertEqual(build.call_count, 1)
                self.assertEqual(type(build.call_args.args[0]).__name__, '_NoRedirect')


if __name__ == '__main__':
    unittest.main()
