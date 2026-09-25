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
import urllib.error
from urllib.parse import quote
from decimal import Decimal, InvalidOperation
from typing import Any

BASE_URL = 'https://api.mexc.com'
HISTORY_PATH = '/api/v1/private/order/list/history_orders'
_ALLOWED_URL = re.compile(
    r'https://api\.mexc\.com/api/v1/private/order/list/history_orders'
    r'\?page_num=1&page_size=(?:[1-9]|1[0-9]|20)&states=3&symbol=[A-Z0-9]+_[A-Z0-9]+\Z'
)
_AUTH_API_CODES = frozenset((401, 402, 406, 511, 602, 701, 703))


class _ApiRejected(Exception):
    def __init__(self, *, authentication: bool):
        self.authentication = authentication


class _HttpStatus(Exception):
    def __init__(self, status: int | None):
        self.status = status


def _canonical_parameters(pairs: list[tuple[str, str | int]]) -> str:
    """One sorted, URL-encoded representation for both signing and transport."""
    values: dict[str, str] = {}
    for key, value in pairs:
        if (not isinstance(key, str) or key not in ('page_num', 'page_size', 'states', 'symbol')
                or key in values
                or type(value) not in (str, int) or value == ''):
            raise ValueError('invalid request parameters')
        values[key] = str(value)
    if not values:
        raise ValueError('empty request parameters')
    return '&'.join(f'{quote(key, safe="-._~")}={quote(values[key], safe="-._~")}'
                    for key in sorted(values))


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
                raise _HttpStatus(response.status)
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
    if (not isinstance(parsed, dict) or type(parsed.get('success')) is not bool
            or type(parsed.get('code')) is not int):
        raise ValueError('invalid response')
    if not parsed['success'] or parsed['code'] != 0:
        raise _ApiRejected(authentication=parsed['code'] in _AUTH_API_CODES)
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


def _aliases(row: dict[str, Any], names: tuple[str, ...], normalize: Any,
             *, required: bool = True) -> Any:
    present = [name for name in names if name in row]
    if not present:
        if required:
            raise ValueError('missing evidence')
        return None
    values = [normalize(row[name]) for name in present]
    if any(value != values[0] for value in values[1:]):
        raise ValueError('conflicting evidence aliases')
    return values[0]


def _positive_integer(value: Any) -> int:
    if type(value) is int:
        result = value
    elif isinstance(value, str) and re.fullmatch(r'[0-9]+', value):
        result = int(value)
    else:
        raise ValueError('invalid integer evidence')
    if result <= 0:
        raise ValueError('invalid integer evidence')
    return result


def _order_id(value: Any) -> str:
    if (type(value) not in (str, int)
            or re.fullmatch(r'[0-9]{1,30}', str(value)) is None):
        raise ValueError('invalid order ID')
    return str(value)


def _symbol(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r'[A-Z0-9]+_[A-Z0-9]+', value) is None:
        raise ValueError('invalid symbol')
    return value


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
        if not isinstance(row, dict):
            raise ValueError('ambiguous order')
        state = _aliases(row, ('state', 'status'), _positive_integer)
        if state in (1, 2, 4, 5):
            continue
        native_symbol = _aliases(row, ('symbol', 'native_symbol'), _symbol)
        side = _aliases(row, ('side', 'order_side'), _positive_integer)
        if state != 3 or native_symbol != symbol:
            raise ValueError('ambiguous order')
        if side not in (1, 3):
            raise ValueError('ambiguous side')
        order_id = _aliases(row, ('orderId', 'order_id'), _order_id)
        _aliases(row, ('updateTime', 'update_time'), _positive_integer)
        _aliases(row, ('fillTime', 'fill_time'), _positive_integer, required=False)
        if order_id in seen:
            raise ValueError('ambiguous identity or time')
        seen.add(order_id)
        requested = _aliases(row, ('vol', 'requested_quantity'), _positive)
        filled = _aliases(row, ('dealVol', 'filled_quantity'), _positive)
        average = _aliases(row, ('dealAvgPriceStr', 'dealAvgPrice'), _positive)
        if requested != filled:
            raise ValueError('not fully filled')
        result.append({'order_id': order_id, 'native_symbol': symbol,
                       'side': 'LONG' if side == 1 else 'SHORT',
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
    parser.add_argument('--diagnostic', action='store_true')
    stage = 'INTERNAL'
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
    try:
        parameters = _canonical_parameters([
            ('symbol', args.symbol), ('states', 3), ('page_num', 1), ('page_size', args.limit),
        ])
        url = _checked_url(BASE_URL + HISTORY_PATH + '?' + parameters)
        timestamp = str(int(time.time() * 1000))
        signature = hmac.new(secret.encode(),
                             (key + timestamp + parameters).encode(), hashlib.sha256).hexdigest()
        headers = {'ApiKey': key, 'Request-Time': timestamp, 'Signature': signature}
        stage = 'TRANSPORT'
        raw = (transport if transport is not None else _GetOnlyTransport()).get(url, headers, 15)
        stage = 'RESPONSE'
        orders = _orders(raw, args.symbol, args.limit)
    except Exception as exc:
        status = None
        if isinstance(exc, urllib.error.HTTPError):
            status = exc.code if type(exc.code) is int and 100 <= exc.code <= 599 else None
            reason = 'AUTH_REJECTED' if status in (401, 403) else 'HTTP_STATUS'
        elif isinstance(exc, _HttpStatus):
            status = exc.status if type(exc.status) is int and 100 <= exc.status <= 599 else None
            reason = 'AUTH_REJECTED' if status in (401, 403) else 'HTTP_STATUS'
        elif isinstance(exc, _ApiRejected):
            reason = 'AUTH_REJECTED' if exc.authentication else 'API_REJECTED'
            stage = 'API'
        elif stage == 'RESPONSE' and isinstance(exc, (ValueError, KeyError, TypeError, UnicodeError)):
            reason = 'RESPONSE_SCHEMA'
        elif stage == 'TRANSPORT' and isinstance(exc, (OSError, urllib.error.URLError)):
            reason = 'NETWORK_ERROR'
        else:
            reason = 'INTERNAL_ERROR'
            stage = 'INTERNAL'
        metadata = {'diagnostic': {'endpoint': 'HISTORY_ORDERS', 'http_status': status,
                                   'schema': stage}} if args.diagnostic else {}
        _emit('FAIL', reason=reason, **metadata)
        return 1
    metadata = {'diagnostic': {'endpoint': 'HISTORY_ORDERS', 'http_status': None,
                               'schema': 'VALID'}} if args.diagnostic else {}
    _emit('PASS', orders=orders, **metadata)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
