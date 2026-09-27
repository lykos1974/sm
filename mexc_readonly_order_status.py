"""Opt-in, read-only MEXC Futures order and deal evidence adapter.

The legacy order-status adapter and isolated externalOid recovery have separate
GET path allowlists. Both are inert unless explicitly enabled.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
import urllib.request
from decimal import Decimal, Inexact, InvalidOperation, Rounded, localcontext
from typing import Any
from urllib.parse import quote

BASE_URL = "https://api.mexc.com"
ORDER_PATH = "/api/v1/private/order/get/"
DEALS_PATH = "/api/v1/private/order/deal_details/"
_ALLOWED_PATH = re.compile(r"/api/v1/private/order/(?:get|deal_details)/[0-9]{1,30}\Z")
_EXTERNAL_PATH = re.compile(
    r"/api/v1/private/order/external/[A-Z0-9]+_[A-Z0-9]+/[A-Za-z0-9_-]{1,32}\Z"
)


def _checked_path(path: str) -> str:
    # Full ASCII match rejects traversal, escapes, queries, alternate case,
    # duplicate slashes and absolute or scheme-relative URLs without decoding.
    if not isinstance(path, str) or _ALLOWED_PATH.fullmatch(path) is None:
        raise ValueError("order status endpoint not allowed")
    return path


def _checked_external_path(path: str) -> str:
    if not isinstance(path, str) or _EXTERNAL_PATH.fullmatch(path) is None:
        raise ValueError('external order endpoint not allowed')
    return path


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        return None


def _read_get(url: str, headers: dict[str, str], timeout: float,
              check_path: Any) -> bytes:
    if not isinstance(url, str) or not url.startswith(BASE_URL + '/'):
        raise ValueError('order status origin not allowed')
    check_path(url[len(BASE_URL):])
    request = urllib.request.Request(url, headers=headers, method='GET')
    # A 3xx is an HTTP error. Authentication headers never reach Location.
    with urllib.request.build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
        if not 200 <= response.status < 300:
            raise ValueError('order status HTTP failure')
        return response.read()


class _GetOnlyTransport:
    def get(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        return _read_get(url, headers, timeout, _checked_path)


class _ExternalGetOnlyTransport:
    def get(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        return _read_get(url, headers, timeout, _checked_external_path)


def _signed_get(transport: Any, path: str, check_path: Any,
                key: str, secret: str) -> bytes:
    check_path(path)  # Refuse unsafe paths before signing or calling transport.
    timestamp = str(int(time.time() * 1000))  # Authentication, never execution time.
    signature = hmac.new(secret.encode(), f'{key}{timestamp}'.encode(), hashlib.sha256).hexdigest()
    headers = {'ApiKey': key, 'Request-Time': timestamp, 'Signature': signature}
    return transport.get(BASE_URL + path, headers, 15)


def _consistent_alias(row: dict[str, Any], names: tuple[str, ...], normalize: Any) -> Any:
    present = [name for name in names if name in row]
    if not present:
        raise ValueError('missing evidence')
    values = [normalize(row[name]) for name in present]
    if any(value != values[0] for value in values[1:]):
        raise ValueError('conflicting evidence aliases')
    return values[0]


def _decimal(value: Any, *, zero: bool = False) -> Decimal:
    if (type(value) not in (str, int, Decimal) or
            isinstance(value, str) and (len(value) > 128 or '\x00' in value)):
        raise ValueError('invalid decimal')
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError('invalid decimal') from exc
    if (not number.is_finite() or number < 0 or (not zero and number == 0) or
            len(number.as_tuple().digits) > 100 or abs(number.as_tuple().exponent) > 100):
        raise ValueError('invalid decimal')
    return number


def _canonical(value: Decimal) -> str:
    rendered = format(value, 'f')
    return rendered.rstrip('0').rstrip('.') if '.' in rendered else rendered


def _order_id(value: Any) -> str:
    if type(value) not in (str, int) or re.fullmatch(r'[0-9]{1,30}', str(value)) is None:
        raise ValueError('invalid order ID')
    return str(value)


def _external_oid(value: Any) -> str:
    if type(value) is not str or re.fullmatch(r'[A-Za-z0-9_-]{1,32}', value) is None:
        raise ValueError('invalid external OID')
    return value


def _symbol(value: Any) -> str:
    if (type(value) is not str or len(value) > 50 or
            re.fullmatch(r'[A-Z0-9]+_[A-Z0-9]+', value) is None):
        raise ValueError('invalid symbol')
    return value


def _side(value: Any) -> int:
    if type(value) is not int or value not in (1, 3):
        raise ValueError('invalid opening side')
    return value


def _state(value: Any) -> int:
    if type(value) is not int:
        raise ValueError('invalid state')
    return value


def _timestamp(value: Any) -> int:
    if type(value) is not int or not 0 < value <= 9_000_000_000_000_000:
        raise ValueError('invalid timestamp')
    return value


# Every supported spelling of each economic field is enumerated here. An
# unknown key is never promoted to evidence, and any supplied alias is checked.
_ORDER_ALIASES = {
    'order_id': ('orderId', 'order_id', 'exchange_order_id'),
    'external_oid': ('externalOid', 'external_oid'),
    'symbol': ('symbol', 'native_symbol'),
    'side': ('side', 'order_side'),
    'requested': ('vol', 'requested_quantity'),
    'filled': ('dealVol', 'filled_quantity', 'cumulative_filled_quantity'),
    'average': ('dealAvgPriceStr', 'dealAvgPrice', 'average_fill_price'),
    'state': ('state', 'status'),
    'update_time': ('updateTime', 'update_time', 'exchange_update_timestamp_ms'),
}
_DEAL_ALIASES = {
    'deal_id': ('id', 'deal_id'),
    'order_id': _ORDER_ALIASES['order_id'],
    'external_oid': _ORDER_ALIASES['external_oid'],
    'symbol': _ORDER_ALIASES['symbol'],
    'side': _ORDER_ALIASES['side'],
    'requested': ('requested_quantity',),
    'filled': ('vol', 'filled_quantity', 'trade_volume'),
    'cumulative': ('dealVol', 'cumulative_filled_quantity'),
    'price': ('price', 'trade_price', 'tradePrice'),
    'average': _ORDER_ALIASES['average'],
    'state': _ORDER_ALIASES['state'],
    'update_time': _ORDER_ALIASES['update_time'],
    'fill_time': ('timestamp', 'tradeTime', 'trade_time', 'exchange_fill_timestamp'),
}


def _normalize_evidence(row: Any, *, deal: bool = False,
                        require_external: bool = False) -> dict[str, Any]:
    if type(row) is not dict:
        raise ValueError('invalid evidence row')
    aliases = _DEAL_ALIASES if deal else _ORDER_ALIASES
    normalizers = {
        'deal_id': _order_id, 'order_id': _order_id, 'external_oid': _external_oid,
        'symbol': _symbol, 'side': _side, 'requested': _decimal,
        'filled': lambda value: _decimal(value, zero=not deal),
        'cumulative': lambda value: _decimal(value, zero=True),
        'price': _decimal, 'average': lambda value: _decimal(value, zero=True),
        'state': _state, 'update_time': _timestamp, 'fill_time': _timestamp,
    }
    required = ({'deal_id', 'order_id', 'symbol', 'side', 'filled', 'price', 'fill_time'}
                if deal else {'order_id', 'symbol', 'side', 'requested', 'filled',
                              'average', 'state', 'update_time'})
    if require_external:
        required.add('external_oid')
    result: dict[str, Any] = {}
    for field, names in aliases.items():
        if field in required or any(name in row for name in names):
            result[field] = _consistent_alias(row, names, normalizers[field])
    return result


def _parse_json(raw: bytes) -> dict[str, Any]:
    if not isinstance(raw, bytes):
        raise ValueError("invalid response")
    def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result

    value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_pairs,
                       parse_float=lambda _: (_ for _ in ()).throw(ValueError('float evidence')),
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")))
    if not isinstance(value, dict) or type(value.get('success')) is not bool or type(value.get('code')) is not int:
        raise ValueError('invalid envelope')
    return value


def _json(raw: bytes) -> dict[str, Any]:
    value = _parse_json(raw)
    if value['success'] is not True or value['code'] != 0:
        raise ValueError("unsuccessful response")
    return value


class ReadOnlyMexcOrderStatusAdapter:
    """get_order_status matches reconcile_exchange_fills' injectable adapter API."""

    def __init__(self, *, enabled: bool = False, transport: Any = None) -> None:
        self._enabled = enabled
        self._transport = transport if transport is not None else _GetOnlyTransport()
        # Read only from runtime environment, never settings, CLI, or fixtures.
        self._key = os.environ.get("MEXC_FUTURES_API_KEY") if enabled else None
        self._secret = os.environ.get("MEXC_FUTURES_API_SECRET") if enabled else None

    def _raw_get(self, path: str) -> bytes:
        return _signed_get(self._transport, path, _checked_path, self._key, self._secret)

    def _get(self, path: str) -> dict[str, Any]:
        return _json(self._raw_get(path))

    def get_order_status(self, order_id: str, symbol: str) -> dict[str, Any] | None:
        if not self._enabled:
            return None
        try:
            if (not self._key or not self._secret or not isinstance(order_id, str)
                    or re.fullmatch(r"[0-9]{1,30}", order_id) is None
                    or not isinstance(symbol, str) or re.fullmatch(r"[A-Z0-9]+_[A-Z0-9]+", symbol) is None):
                return None
            order = _normalize_evidence(self._get(ORDER_PATH + order_id)['data'])
            if (order['order_id'] != order_id or order['symbol'] != symbol or
                    order['state'] != 3):
                return None
            requested, filled, price, update_time = (order[key] for key in
                ('requested', 'filled', 'average', 'update_time'))
            if requested != filled or price <= 0:
                return None
            deals = self._get(DEALS_PATH + order_id)["data"]
            if not isinstance(deals, list) or not deals:
                return None
            seen: set[str] = set()
            quantity = Decimal(0)
            notional = Decimal(0)
            latest = 0
            with localcontext() as context:
                context.prec = 100
                context.traps[Inexact] = True
                context.traps[Rounded] = True
                for deal in deals:
                    item = _normalize_evidence(deal, deal=True)
                    if (item['order_id'] != order_id or item['symbol'] != symbol or
                            item['side'] != order['side'] or
                            ('external_oid' in item and item['external_oid'] !=
                             order.get('external_oid')) or
                            ('requested' in item and item['requested'] != requested) or
                            ('cumulative' in item and item['cumulative'] != filled) or
                            ('average' in item and item['average'] != price) or
                            ('state' in item and item['state'] != order['state']) or
                            ('update_time' in item and item['update_time'] != update_time)):
                        return None
                    deal_id, timestamp = item['deal_id'], item['fill_time']
                    if deal_id in seen or timestamp > update_time:
                        return None
                    seen.add(deal_id)
                    volume, trade_price = item['filled'], item['price']
                    quantity += volume
                    notional += volume * trade_price
                    latest = max(latest, timestamp)
                if quantity != filled or notional != price * quantity:
                    return None
            return {"success": True, "data": {
                "exchange_order_id": order_id, "symbol": symbol, "side": order['side'],
                "requested_quantity": str(requested), "cumulative_filled_quantity": str(filled),
                "average_fill_price": str(price), "exchange_fill_timestamp": latest,
                "status": "FILLED",
            }}
        except Exception:
            # Includes auth, HTTP, timeout, invalid JSON and malformed evidence.
            # Never emit response bodies, headers, credentials or signatures.
            return None


class ReadOnlyMexcExternalOidRecovery:
    """Opt-in, single GET lookup. Outcomes never authorize another submission.

    updateTime is an exchange order-update timestamp, not a proven fill time.
    The separate deal-details adapter remains necessary to prove execution time.
    """

    def __init__(self, *, enabled: bool = False, transport: Any = None) -> None:
        self._enabled = enabled
        self._transport = transport if transport is not None else _ExternalGetOnlyTransport()
        self._key = os.environ.get('MEXC_FUTURES_API_KEY') if enabled else None
        self._secret = os.environ.get('MEXC_FUTURES_API_SECRET') if enabled else None

    def recover(self, symbol: str, external_oid: str) -> dict[str, Any]:
        unavailable = {'outcome': 'TRANSPORT_FAILURE', 'evidence': None}
        unknown = {'outcome': 'UNKNOWN', 'evidence': None}
        if (not self._enabled or not self._key or not self._secret):
            return unavailable
        try:
            _symbol(symbol)
            _external_oid(external_oid)
        except ValueError:
            return unknown
        path = ('/api/v1/private/order/external/' + quote(symbol, safe='') + '/' +
                quote(external_oid, safe=''))
        try:
            raw = _signed_get(self._transport, path, _checked_external_path,
                              self._key, self._secret)
        except Exception:
            return unavailable
        try:
            envelope = _parse_json(raw)
            if envelope['success'] is False or envelope['code'] != 0:
                if envelope['success'] is False and envelope['code'] == 2040:
                    return {'outcome': 'NOT_FOUND', 'evidence': None}
                return unknown
            item = _normalize_evidence(envelope['data'], require_external=True)
            oid, order_id, native, side = (item[key] for key in
                ('external_oid', 'order_id', 'symbol', 'side'))
            requested, filled, price, state, timestamp = (item[key] for key in
                ('requested', 'filled', 'average', 'state', 'update_time'))
            if (oid != external_oid or native != symbol or filled > requested or
                    (filled > 0) != (price > 0)):
                return unknown
            if state == 3 and filled == requested:
                outcome, status = 'FULL_FILL', 'FILLED'
            elif state in (1, 2) and filled < requested:
                outcome, status = ('PARTIAL_FILL', 'PARTIAL') if filled > 0 else ('ZERO_FILL', 'ZERO')
            elif state in (4, 5) and filled < requested:
                outcome, status = ('CANCELLED', 'CANCELLED') if state == 4 else ('REJECTED', 'REJECTED')
            else:
                return unknown
            return {'outcome': outcome, 'evidence': {
                'external_oid': oid, 'exchange_order_id': order_id, 'symbol': native,
                'side': 'LONG' if side == 1 else 'SHORT',
                'requested_quantity': _canonical(requested),
                'cumulative_filled_quantity': _canonical(filled),
                'status': status,
                'average_fill_price': _canonical(price),
                'exchange_update_timestamp_ms': timestamp,
            }}
        except Exception:
            return unknown
