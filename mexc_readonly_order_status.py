"""Opt-in, read-only MEXC Futures order and deal evidence adapter.

Only three fixed GET endpoint families are reachable. Construction is inert
unless enabled; no runtime caller enables or instantiates these adapters.
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
    if (not isinstance(path, str) or
            not (_ALLOWED_PATH.fullmatch(path) or _EXTERNAL_PATH.fullmatch(path))):
        raise ValueError("order status endpoint not allowed")
    return path


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        return None


class _GetOnlyTransport:
    def get(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        if not isinstance(url, str) or not url.startswith(BASE_URL + "/"):
            raise ValueError("order status origin not allowed")
        _checked_path(url[len(BASE_URL):])
        request = urllib.request.Request(url, headers=headers, method="GET")
        # A 3xx is an HTTP error. Authentication headers never reach Location.
        with urllib.request.build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
            if not 200 <= response.status < 300:
                raise ValueError("order status HTTP failure")
            return response.read()


def _positive_decimal(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("invalid decimal")
    try:
        decimal = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid decimal") from exc
    if not decimal.is_finite() or decimal <= 0:
        raise ValueError("invalid decimal")
    return decimal


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

    value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_pairs, parse_float=Decimal,
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
        _checked_path(path)
        timestamp = str(int(time.time() * 1000))  # Authentication time, never fill time.
        signature = hmac.new(self._secret.encode(),
                             f"{self._key}{timestamp}".encode(), hashlib.sha256).hexdigest()
        headers = {"ApiKey": self._key, "Request-Time": timestamp, "Signature": signature}
        return self._transport.get(BASE_URL + path, headers, 15)

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
            order = self._get(ORDER_PATH + order_id)["data"]
            if (not isinstance(order, dict) or str(order["orderId"]) != order_id
                    or order["symbol"] != symbol or type(order["side"]) is not int
                    or order["side"] not in (1, 3) or type(order["state"]) is not int
                    or order["state"] != 3):
                return None
            requested = _positive_decimal(order["vol"])
            filled = _positive_decimal(order["dealVol"])
            price = _positive_decimal(order.get("dealAvgPriceStr", order.get("dealAvgPrice")))
            update_time = order["updateTime"]
            if type(update_time) is not int or update_time <= 0:
                return None
            if requested != filled:
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
                    if (not isinstance(deal, dict) or str(deal["orderId"]) != order_id
                            or deal["symbol"] != symbol or type(deal["side"]) is not int
                            or deal["side"] != order["side"]):
                        return None
                    deal_id = str(deal["id"])
                    timestamp = deal["timestamp"]
                    if (not deal_id or deal_id in seen or type(timestamp) is not int
                            or timestamp <= 0 or timestamp > update_time):
                        return None
                    seen.add(deal_id)
                    volume = _positive_decimal(deal["vol"])
                    trade_price = _positive_decimal(deal["price"])
                    quantity += volume
                    notional += volume * trade_price
                    latest = max(latest, timestamp)
                if quantity != filled or notional != price * quantity:
                    return None
            return {"success": True, "data": {
                "exchange_order_id": order_id, "symbol": symbol, "side": order["side"],
                "requested_quantity": str(requested), "cumulative_filled_quantity": str(filled),
                "average_fill_price": str(price), "exchange_fill_timestamp": latest,
                "status": "FILLED",
            }}
        except Exception:
            # Includes auth, HTTP, timeout, invalid JSON and malformed evidence.
            # Never emit response bodies, headers, credentials or signatures.
            return None


class ReadOnlyMexcExternalOidRecovery(ReadOnlyMexcOrderStatusAdapter):
    """Opt-in, single GET lookup. Outcomes never authorize another submission.

    updateTime is an exchange order-update timestamp, not a proven fill time.
    The separate deal-details adapter remains necessary to prove execution time.
    """

    @staticmethod
    def _field(row: dict[str, Any], names: tuple[str, ...], normalize: Any) -> Any:
        present = [name for name in names if name in row]
        if not present:
            raise ValueError('missing evidence')
        values = [normalize(row[name]) for name in present]
        if any(value != values[0] for value in values[1:]):
            raise ValueError('conflicting evidence')
        return values[0]

    @staticmethod
    def _decimal(value: Any, *, zero: bool = False) -> Decimal:
        if (isinstance(value, bool) or not isinstance(value, (str, int, Decimal)) or
                isinstance(value, str) and (len(value) > 128 or '\x00' in value)):
            raise ValueError('invalid decimal')
        number = Decimal(str(value))
        if (not number.is_finite() or number < 0 or (not zero and number == 0) or
                len(number.as_tuple().digits) > 100 or abs(number.as_tuple().exponent) > 100):
            raise ValueError('invalid decimal')
        return number

    @staticmethod
    def _canonical(value: Decimal) -> str:
        rendered = format(value, 'f')
        return rendered.rstrip('0').rstrip('.') if '.' in rendered else rendered

    @staticmethod
    def _oid(value: Any) -> str:
        if type(value) is not str or re.fullmatch(r'[A-Za-z0-9_-]{1,32}', value) is None:
            raise ValueError('invalid OID')
        return value

    @staticmethod
    def _symbol(value: Any) -> str:
        if type(value) is not str or len(value) > 50 or re.fullmatch(r'[A-Z0-9]+_[A-Z0-9]+', value) is None:
            raise ValueError('invalid symbol')
        return value

    @staticmethod
    def _side(value: Any) -> int:
        if type(value) is not int or value not in (1, 3):
            raise ValueError('invalid opening side')
        return value

    @staticmethod
    def _state(value: Any) -> int:
        if type(value) is not int:
            raise ValueError('invalid state')
        return value

    @staticmethod
    def _timestamp(value: Any) -> int:
        if type(value) is not int or not 0 < value <= 9_000_000_000_000_000:
            raise ValueError('invalid timestamp')
        return value

    @staticmethod
    def _id(value: Any) -> str:
        if type(value) not in (str, int) or re.fullmatch(r'[0-9]{1,30}', str(value)) is None:
            raise ValueError('invalid order ID')
        return str(value)

    def recover(self, symbol: str, external_oid: str) -> dict[str, Any]:
        unavailable = {'outcome': 'TRANSPORT_FAILURE', 'evidence': None}
        unknown = {'outcome': 'UNKNOWN', 'evidence': None}
        if (not self._enabled or not self._key or not self._secret):
            return unavailable
        try:
            self._symbol(symbol)
            self._oid(external_oid)
        except ValueError:
            return unknown
        path = ('/api/v1/private/order/external/' + quote(symbol, safe='') + '/' +
                quote(external_oid, safe=''))
        try:
            raw = self._raw_get(path)
        except Exception:
            return unavailable
        try:
            envelope = _parse_json(raw)
            if envelope['success'] is False or envelope['code'] != 0:
                if envelope['success'] is False and envelope['code'] == 2040:
                    return {'outcome': 'NOT_FOUND', 'evidence': None}
                return unknown
            row = envelope['data']
            if type(row) is not dict:
                return unknown
            oid = self._field(row, ('externalOid', 'external_oid'), self._oid)
            order_id = self._field(row, ('orderId', 'order_id'), self._id)
            native = self._field(row, ('symbol', 'native_symbol'), self._symbol)
            side = self._field(row, ('side',), self._side)
            requested = self._field(row, ('vol', 'requested_quantity'), self._decimal)
            filled = self._field(row, ('dealVol', 'cumulative_filled_quantity'),
                                 lambda value: self._decimal(value, zero=True))
            price = self._field(row, ('dealAvgPriceStr', 'dealAvgPrice', 'average_fill_price'),
                                lambda value: self._decimal(value, zero=True))
            state = self._field(row, ('state', 'status'), self._state)
            timestamp = self._field(row, ('updateTime', 'update_time'), self._timestamp)
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
                'requested_quantity': self._canonical(requested),
                'cumulative_filled_quantity': self._canonical(filled),
                'status': status,
                'average_fill_price': self._canonical(price),
                'exchange_update_timestamp_ms': timestamp,
            }}
        except Exception:
            return unknown
