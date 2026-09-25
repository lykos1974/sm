#!/usr/bin/env python3
"""Explicit, standalone read-only discovery of historical MEXC Futures orders.

The history endpoint gives order updateTime, not a proven execution timestamp;
fill_time is therefore null. The separate shadow check verifies actual deals.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import time
import urllib.request
from decimal import Decimal, InvalidOperation
from typing import Any

BASE_URL = 'https://api.mexc.com'
HISTORY_PATH = '/api/v1/private/order/list/history_orders'
_ALLOWED_URL = re.compile(
    r'https://api\.mexc\.com/api/v1/private/order/list/history_orders'
    r'\?symbol=[A-Z0-9]+_[A-Z0-9]+&states=3&page_num=1&page_size=(?:[1-9]|1[0-9]|20)\Z'
)


def _checked_url(url: str) -> str:
    if not isinstance(url, str) or _ALLOWED_URL.fullmatch(url) is None:
        raise ValueError('history endpoint not allowed')
    return url


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        return None


class _GetOnlyTransport:
    def get(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        request = urllib.request.Request(_checked_url(url), headers=headers, method='GET')
        with urllib.request.build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
            if not 200 <= response.status < 300:
                raise ValueError('history HTTP failure')
            return response.read()


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError('invalid arguments')


def _json(raw: bytes) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate JSON field')
            result[key] = value
        return result

    if not isinstance(raw, bytes):
        raise ValueError('invalid response')
    parsed = json.loads(raw.decode('utf-8'), object_pairs_hook=unique,
                        parse_float=Decimal,
                        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('non-finite')))
    if (not isinstance(parsed, dict) or parsed.get('success') is not True
            or type(parsed.get('code')) is not int or parsed['code'] != 0):
        raise ValueError('invalid response')
    return parsed


def _positive(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError('invalid quantity or price')
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError('invalid quantity or price') from exc
    if not result.is_finite() or result <= 0:
        raise ValueError('invalid quantity or price')
    return result


def _orders(raw: bytes, symbol: str, limit: int) -> list[dict[str, Any]]:
    data = _json(raw)['data']
    if isinstance(data, dict):
        if (type(data.get('currentPage')) is not int or data['currentPage'] != 1
                or type(data.get('pageSize')) is not int or data['pageSize'] != limit):
            raise ValueError('invalid page')
        data = data['resultList']
    if not isinstance(data, list) or len(data) > limit:
        raise ValueError('invalid result set')
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in data:
        if not isinstance(row, dict) or type(row.get('state')) is not int:
            raise ValueError('ambiguous order')
        if row['state'] in (1, 2, 4, 5):
            continue
        if row['state'] != 3 or row.get('symbol') != symbol or type(row.get('side')) is not int:
            raise ValueError('ambiguous order')
        if row['side'] not in (1, 3):
            raise ValueError('ambiguous side')
        order_id = str(row['orderId'])
        if (re.fullmatch(r'[0-9]{1,30}', order_id) is None or order_id in seen
                or type(row.get('updateTime')) is not int or row['updateTime'] <= 0):
            raise ValueError('ambiguous identity or time')
        seen.add(order_id)
        requested = _positive(row['vol'])
        filled = _positive(row['dealVol'])
        average = _positive(row.get('dealAvgPriceStr', row.get('dealAvgPrice')))
        if requested != filled:
            raise ValueError('not fully filled')
        result.append({'order_id': order_id, 'native_symbol': symbol,
                       'side': 'LONG' if row['side'] == 1 else 'SHORT',
                       'requested_quantity': str(requested), 'filled_quantity': str(filled),
                       'average_fill_price': str(average), 'fill_time': None,
                       'status': 'FILLED'})
    return result


def _emit(status: str, **fields: Any) -> None:
    print(json.dumps({'status': status, **fields}, separators=(',', ':'), sort_keys=True))


def main(argv: list[str] | None = None, *, transport: Any = None) -> int:
    parser = _Parser(description='Find existing filled MEXC Futures order IDs (read only)')
    parser.add_argument('--symbol', required=True, help='Native symbol, e.g. BTC_USDT')
    parser.add_argument('--limit', type=int, default=5)
    try:
        args = parser.parse_args(argv)
        if re.fullmatch(r'[A-Z0-9]+_[A-Z0-9]+', args.symbol) is None or not 1 <= args.limit <= 20:
            raise ValueError('invalid input')
    except (ValueError, TypeError):
        _emit('FAIL', reason='INVALID_INPUT')
        return 1
    key = os.environ.get('MEXC_FUTURES_API_KEY')
    secret = os.environ.get('MEXC_FUTURES_API_SECRET')
    if not key or not secret:
        _emit('FAIL', reason='MISSING_CREDENTIALS')
        return 1
    url = _checked_url(BASE_URL + HISTORY_PATH +
                       f'?symbol={args.symbol}&states=3&page_num=1&page_size={args.limit}')
    try:
        timestamp = str(int(time.time() * 1000))
        signature = hmac.new(secret.encode(), f'{key}{timestamp}'.encode(), hashlib.sha256).hexdigest()
        headers = {'ApiKey': key, 'Request-Time': timestamp, 'Signature': signature}
        raw = (transport if transport is not None else _GetOnlyTransport()).get(url, headers, 15)
        orders = _orders(raw, args.symbol, args.limit)
    except Exception:
        _emit('FAIL', reason='EVIDENCE_UNAVAILABLE')
        return 1
    _emit('PASS', orders=orders)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
