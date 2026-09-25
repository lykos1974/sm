"""Synthetic order/trade fixtures; never open a socket."""
import copy
import json
import os
import sqlite3
import unittest
import urllib.request
from decimal import Decimal
from unittest.mock import patch

from mexc_readonly_order_status import ReadOnlyMexcOrderStatusAdapter, _GetOnlyTransport, _NoRedirect
import live_mexc_forward_trader as trader

ORDER_ID = '739106551624717312'
SYMBOL = 'BTC_USDT'
ORDER = {'success': True, 'code': 0, 'data': {
    'orderId': ORDER_ID, 'symbol': SYMBOL, 'side': 1, 'vol': '3', 'dealVol': '3',
    'dealAvgPriceStr': '100.500000000000000001', 'state': 3,
    'updateTime': 190000, 'price': '99',
}}
TRADES = {'success': True, 'code': 0, 'data': [
    {'id': '1', 'orderId': ORDER_ID, 'symbol': SYMBOL, 'side': 1, 'vol': '1', 'price': '100.500000000000000000', 'timestamp': 150000},
    {'id': '2', 'orderId': ORDER_ID, 'symbol': SYMBOL, 'side': 1, 'vol': '2', 'price': '100.5000000000000000015', 'timestamp': 160000},
]}


class Transport:
    def __init__(self, order=ORDER, trades=TRADES):
        self.order, self.trades = order, trades
        self.calls = []

    def get(self, url, headers, timeout):
        self.calls.append((url, headers, timeout))
        response = self.trades if '/deal_details/' in url else self.order
        if isinstance(response, Exception):
            raise response
        return json.dumps(response).encode() if not isinstance(response, bytes) else response


class AdapterTests(unittest.TestCase):
    def adapter(self, transport, enabled=True):
        with patch.dict(os.environ, {'MEXC_FUTURES_API_KEY': 'synthetic-key', 'MEXC_FUTURES_API_SECRET': 'synthetic-secret'}):
            return ReadOnlyMexcOrderStatusAdapter(transport=transport, enabled=enabled)

    def test_full_fill_exact_mapping_and_idempotence(self):
        transport = Transport()
        adapter = self.adapter(transport)
        first = adapter.get_order_status(ORDER_ID, SYMBOL)
        self.assertEqual(first, adapter.get_order_status(ORDER_ID, SYMBOL))
        self.assertEqual(first['data'], {
            'exchange_order_id': ORDER_ID, 'symbol': SYMBOL, 'side': 1,
            'requested_quantity': '3', 'cumulative_filled_quantity': '3',
            'average_fill_price': '100.500000000000000001',
            'exchange_fill_timestamp': 160000, 'status': 'FILLED',
        })
        self.assertEqual(Decimal(first['data']['average_fill_price']), Decimal('100.500000000000000001'))
        self.assertEqual(len(transport.calls), 4)
        self.assertTrue(all('/api/v1/private/order/get/' in u or '/api/v1/private/order/deal_details/' in u for u, _, _ in transport.calls))
        self.assertTrue(all('synthetic-secret' not in str(call) for call in transport.calls))
        self.assertFalse(hasattr(adapter, 'post'))
        self.assertFalse(hasattr(adapter, 'submit_order'))
        self.assertFalse(hasattr(adapter, 'cancel_order'))

    def test_disabled_never_loads_credentials_or_calls_transport(self):
        transport = Transport()
        with patch.dict(os.environ, {}, clear=True):
            adapter = ReadOnlyMexcOrderStatusAdapter(transport=transport)
            self.assertIsNone(adapter.get_order_status(ORDER_ID, SYMBOL))
        self.assertEqual(transport.calls, [])

    def test_invalid_order_or_trade_fails_closed(self):
        alterations = [
            ('order', 'state', 1), ('order', 'state', 2), ('order', 'state', 4),
            ('order', 'state', 5), ('order', 'state', 999),
            ('order', 'orderId', 'other'), ('order', 'symbol', 'ETH_USDT'),
            ('order', 'side', 3), ('order', 'vol', '4'), ('order', 'dealVol', '1'),
            ('order', 'dealVol', '0'), ('order', 'dealAvgPriceStr', 'NaN'),
            ('order', 'dealAvgPriceStr', None), ('order', 'dealAvgPriceStr', '0'),
            ('trade', 'orderId', 'other'), ('trade', 'symbol', 'ETH_USDT'),
            ('trade', 'side', 3), ('trade', 'vol', '0'), ('trade', 'timestamp', None),
            ('trade', 'timestamp', 0), ('trade', 'price', 'Infinity'),
        ]
        for which, key, value in alterations:
            with self.subTest(which=which, key=key, value=value):
                order, trades = copy.deepcopy(ORDER), copy.deepcopy(TRADES)
                (order['data'] if which == 'order' else trades['data'][0])[key] = value
                self.assertIsNone(self.adapter(Transport(order, trades)).get_order_status(ORDER_ID, SYMBOL))

    def test_missing_fields_malformed_auth_timeout_http_invalid_json_fail_closed(self):
        cases = [(None, TRADES), ({'success': False, 'code': 401}, TRADES),
                 (b'{broken', TRADES), (TimeoutError(), TRADES), (OSError('HTTP 401 synthetic-secret'), TRADES),
                 (ORDER, {'success': False}), (ORDER, b'{broken'), (ORDER, TimeoutError())]
        for order, trades in cases:
            with self.subTest(order=order, trades=trades):
                self.assertIsNone(self.adapter(Transport(order, trades)).get_order_status(ORDER_ID, SYMBOL))

    def test_inconsistent_trades_never_substitute_update_time(self):
        for mutate in ('missing', 'quantity', 'average', 'duplicate', 'future'):
            with self.subTest(mutate=mutate):
                trades = copy.deepcopy(TRADES)
                if mutate == 'missing': trades['data'] = []
                if mutate == 'quantity': trades['data'][0]['vol'] = '0.5'
                if mutate == 'average': trades['data'][0]['price'] = '101'
                if mutate == 'duplicate': trades['data'].append(copy.deepcopy(trades['data'][0]))
                if mutate == 'future': trades['data'][0]['timestamp'] = 200000
                self.assertIsNone(self.adapter(Transport(ORDER, trades)).get_order_status(ORDER_ID, SYMBOL))

    def test_normalized_evidence_integrates_only_through_explicit_gate(self):
        conn = sqlite3.connect(':memory:')
        trader.init_live_tables(conn)
        conn.execute("""INSERT INTO live_trades(created_at,symbol,pattern,side,entry_time,entry_price,stop_price,tp1_price,tp2_price,notional_usdt,exchange_order_id,status,raw_order_response)
            VALUES('now','MEXC_FUT:BTCUSDT','triangle','LONG',1,99,90,105,120,1,?,'ORDER_SENT',?)""",
            (ORDER_ID, json.dumps({'order_request': {'symbol': SYMBOL, 'side': 1, 'vol': '3'}})))
        conn.commit()
        transport = Transport()
        adapter = self.adapter(transport)
        self.assertEqual(trader.reconcile_exchange_fills(conn, adapter), 0)
        self.assertEqual(transport.calls, [])
        self.assertEqual(trader.reconcile_exchange_fills(conn, adapter, enabled=True), 1)
        self.assertEqual(conn.execute('SELECT status, execution_entry_price, confirmed_fill_ts FROM live_trades').fetchone(),
                         ('FILLED', '100.500000000000000001', 160000))
        before = conn.total_changes
        self.assertEqual(trader.reconcile_exchange_fills(conn, adapter, enabled=True), 0)
        self.assertEqual(conn.total_changes, before)
        conn.close()

    def test_exact_endpoint_allowlist_rejects_all_other_paths_before_transport(self):
        transport = Transport()
        adapter = self.adapter(transport)
        bad = (
            '/api/v1/private/order/create', '/api/v1/private/order/modify',
            '/api/v1/private/order/cancel', '/api/v1/private/order/cancel_all',
            '/api/v1/private/order/get/OTHER', '/api/v1/private/order/get/' + ORDER_ID + '/..',
            '/api/v1/private/order/get/%37' + ORDER_ID[1:],
            '/api/v1/private/order/get/' + ORDER_ID + '?x=1',
            '/API/v1/private/order/get/' + ORDER_ID,
            '/api//v1/private/order/get/' + ORDER_ID,
            'https://audit.invalid/collect', '//audit.invalid/collect',
        )
        for path in bad:
            with self.subTest(path=path), self.assertRaises(ValueError):
                adapter._get(path)
        self.assertEqual(transport.calls, [])

    def test_redirects_never_forward_sensitive_headers(self):
        request = urllib.request.Request('https://api.mexc.com/api/v1/private/order/get/' + ORDER_ID,
                                         headers={'ApiKey': 'synthetic-key', 'Signature': 'synthetic-signature',
                                                  'Request-Time': '123'}, method='GET')
        for code in (301, 302, 303, 307, 308):
            with self.subTest(code=code):
                opened = []
                class FakeOpener:
                    def open(self, req, timeout):
                        opened.append(req.full_url)
                        handler = _NoRedirect()
                        if handler.redirect_request(req, None, code, 'redirect', {},
                                                    'https://audit.invalid/collect') is None:
                            raise urllib.error.HTTPError(req.full_url, code, 'redirect', {}, None)
                        raise AssertionError('redirect followed')
                with patch('urllib.request.build_opener', return_value=FakeOpener()) as build:
                    with self.assertRaises(urllib.error.HTTPError) as error:
                        _GetOnlyTransport().get(request.full_url, dict(request.header_items()), 15)
                self.assertTrue(any(isinstance(arg, _NoRedirect) for arg in build.call_args.args))
                self.assertEqual(opened, [request.full_url])
                self.assertNotIn('synthetic-key', str(error.exception))
                self.assertNotIn('synthetic-signature', str(error.exception))

    def test_duplicate_json_keys_at_every_depth_fail_closed(self):
        original = json.dumps(ORDER)
        cases = [
            original.replace('"success": true', '"success": false, "success": true', 1),
            original.replace('"state": 3', '"state": 4, "state": 3', 1),
            original.replace('"orderId":', '"orderId": "other", "orderId":', 1),
            original.replace('"dealVol": "3"', '"dealVol": "3", "dealVol": "3"', 1),
            original.replace('"dealAvgPriceStr":', '"dealAvgPriceStr": "1", "dealAvgPriceStr":', 1),
            original.replace('"price": "99"', '"price": "99", "unknown": {"x": 1, "x": 1}', 1),
        ]
        trades = json.dumps(TRADES).replace('"timestamp": 150000',
                                          '"timestamp": 1, "timestamp": 150000', 1)
        for raw in cases:
            with self.subTest(raw=raw):
                self.assertIsNone(self.adapter(Transport(raw.encode(), TRADES)).get_order_status(ORDER_ID, SYMBOL))
        self.assertIsNone(self.adapter(Transport(ORDER, trades.encode())).get_order_status(ORDER_ID, SYMBOL))


if __name__ == '__main__': unittest.main()
