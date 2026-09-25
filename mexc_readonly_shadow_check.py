#!/usr/bin/env python3
"""Explicit, standalone MEXC Futures historical order shadow check.

This command has no database or trading imports. Only an operator invocation
with runtime environment credentials can issue the adapter's two GET requests.
"""
from __future__ import annotations

import argparse
import ctypes
import json
import os
import re
import sys
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


def _emit_published_report_best_effort(payload: str) -> None:
    """A committed report stays authoritative if the console is unavailable."""
    try:
        print(payload, end='')
        sys.stdout.flush()
    except Exception:
        pass


def _cleanup_temporary(temporary: str) -> bool:
    """Try both ordinary removal APIs after the file handle has closed."""
    for remove in (os.unlink, os.remove, os.unlink):
        try:
            remove(temporary)
            return True
        except FileNotFoundError:
            return True
        except OSError:
            pass
    return False  # Universal OS deletion denial cannot be repaired in process.


def _write_new_report(path: str, payload: str) -> None:
    """Publish once via a no-replace rename, with no cleanup after publication."""
    destination = Path(path)
    if os.path.lexists(destination) or _is_reparse_point(destination):
        raise FileExistsError('report target exists')
    fd, temporary = tempfile.mkstemp(prefix='.mexc-shadow-', suffix='.tmp', dir=str(destination.parent))
    try:
        try:
            stream_context = os.fdopen(fd, 'wb')
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            raise
        with stream_context as stream:
            stream.write(payload.encode('utf-8'))
            stream.flush()
            os.fsync(stream.fileno())
        _publish_new_report(temporary, destination)
    except BaseException:
        _cleanup_temporary(temporary)
        raise


def _is_reparse_point(path: Path) -> bool:
    if os.name != 'nt':
        return path.is_symlink()
    attributes = ctypes.windll.kernel32.GetFileAttributesW(str(path))
    return attributes != 0xFFFFFFFF and bool(attributes & 0x400)


def _publish_new_report(temporary: str, destination: Path) -> None:
    """Atomic same-directory move that refuses to replace an existing path."""
    if os.name == 'nt':
        # Windows rename fails if the destination exists, including symlinks.
        os.rename(temporary, destination)
        return
    # Linux test/development path; plain os.rename would replace an existing file.
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as exc:
        raise OSError('exclusive report publication unavailable') from exc
    renameat2.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    renameat2.restype = ctypes.c_int
    result = renameat2(-100, os.fsencode(temporary), -100, os.fsencode(destination), 1)
    if result != 0:
        raise OSError(ctypes.get_errno(), 'exclusive report publication failed')


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
        _emit_published_report_best_effort(payload)
        return 0
    try:
        print(payload, end='')
        sys.stdout.flush()
    except OSError:
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
