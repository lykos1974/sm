#!/usr/bin/env python3
"""Explicit, standalone MEXC Futures historical order shadow check.

This command has no database or trading imports. Only an operator invocation
with runtime environment credentials can issue the adapter's two GET requests.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from mexc_readonly_order_status import ReadOnlyMexcOrderStatusAdapter


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError("invalid arguments")


def _parser() -> _Parser:
    parser = _Parser(description="Read-only check of one existing MEXC Futures order")
    parser.add_argument('--order-id', required=True)
    parser.add_argument('--symbol', required=True, help='Native futures symbol, e.g. BTC_USDT')
    parser.add_argument('--side', required=True, choices=('LONG', 'SHORT'))
    parser.add_argument('--requested-quantity', required=True)
    parser.add_argument('--output-report')
    return parser


def _emit(status: str, *, evidence: dict[str, Any] | None = None,
          reason: str | None = None) -> str:
    result = {'status': status, 'evidence': evidence} if status == 'PASS' else {'status': status, 'reason': reason}
    return json.dumps(result, sort_keys=True, separators=(',', ':')) + '\n'


def _write_new_report(path: str, payload: str) -> None:
    """Write a complete report via same-directory temporary file and exclusive link."""
    destination = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.mexc-shadow-', suffix='.tmp', dir=str(destination.parent))
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload.encode('utf-8'))
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, destination)  # Atomic creation; existing operator files are never replaced.
    finally:
        os.unlink(temporary)


def main(argv: list[str] | None = None, *, transport: Any = None) -> int:
    try:
        args = _parser().parse_args(argv)
        if (re.fullmatch(r'[0-9]{1,30}', args.order_id) is None
                or re.fullmatch(r'[A-Z0-9]+_[A-Z0-9]+', args.symbol) is None):
            raise ValueError('invalid identity')
        requested = Decimal(args.requested_quantity)
        if not requested.is_finite() or requested <= 0:
            raise ValueError('invalid quantity')
    except (ValueError, InvalidOperation, TypeError):
        print(_emit('FAIL', reason='INVALID_INPUT'), end='')
        return 1

    if not os.environ.get('MEXC_FUTURES_API_KEY') or not os.environ.get('MEXC_FUTURES_API_SECRET'):
        print(_emit('FAIL', reason='MISSING_CREDENTIALS'), end='')
        return 1

    adapter = ReadOnlyMexcOrderStatusAdapter(enabled=True, transport=transport)
    evidence = adapter.get_order_status(args.order_id, args.symbol)
    if not isinstance(evidence, dict) or evidence.get('success') is not True:
        print(_emit('FAIL', reason='EVIDENCE_UNAVAILABLE'), end='')
        return 1
    data = evidence.get('data')
    expected_side = 1 if args.side == 'LONG' else 3
    try:
        if (not isinstance(data, dict) or data['exchange_order_id'] != args.order_id
                or data['symbol'] != args.symbol or type(data['side']) is not int
                or data['side'] != expected_side or data['status'] != 'FILLED'
                or Decimal(data['requested_quantity']) != requested
                or Decimal(data['cumulative_filled_quantity']) != requested
                or not Decimal(data['average_fill_price']).is_finite()
                or Decimal(data['average_fill_price']) <= 0
                or type(data['exchange_fill_timestamp']) is not int
                or data['exchange_fill_timestamp'] <= 0):
            raise ValueError('evidence mismatch')
    except (KeyError, ValueError, InvalidOperation, TypeError):
        print(_emit('FAIL', reason='EVIDENCE_MISMATCH'), end='')
        return 1

    # Construct a fresh allowlisted object; never serialize headers or raw responses.
    sanitized = {key: data[key] for key in (
        'exchange_order_id', 'symbol', 'side', 'requested_quantity',
        'cumulative_filled_quantity', 'average_fill_price',
        'exchange_fill_timestamp', 'status',
    )}
    payload = _emit('PASS', evidence=sanitized)
    if args.output_report:
        try:
            _write_new_report(args.output_report, payload)
        except OSError:
            print(_emit('FAIL', reason='REPORT_ERROR'), end='')
            return 1
    print(payload, end='')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
