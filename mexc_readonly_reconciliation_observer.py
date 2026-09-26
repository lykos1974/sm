#!/usr/bin/env python3
"""Separate read-only observation of MEXC ORDER_SENT trades; never reconcile state."""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import sqlite3
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote

from mexc_readonly_order_status import ReadOnlyMexcOrderStatusAdapter
from mexc_readonly_shadow_check import _write_new_report

_REQUIRED = frozenset(('id', 'status', 'exchange_order_id', 'external_oid',
                       'native_symbol', 'side', 'requested_quantity', 'submitted_at_ms',
                       'bot_generated'))
_OID = re.compile(r'pnf-[A-Za-z0-9_]{1,4}-[0-9]+-[LS]\Z')
_ORDER_ID = re.compile(r'[0-9]{1,30}\Z')
_SYMBOL = re.compile(r'[A-Z0-9]+_[A-Z0-9]+\Z')
_SKEW_MS = 5000
_REPORT_VERSION = 2
class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError('invalid arguments')


def _positive(value: Any, *, zero: bool = False) -> Decimal:
    if isinstance(value, bool) or type(value) not in (str, int, Decimal):
        raise ValueError('invalid decimal')
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError('invalid decimal') from exc
    if (not number.is_finite() or number < 0 or (number == 0 and not zero)
            or len(number.as_tuple().digits) > 100
            or abs(number.as_tuple().exponent) > 100):
        raise ValueError('invalid decimal')
    return number


def _canonical(value: Decimal) -> str:
    plain = format(value, 'f')
    return plain.rstrip('0').rstrip('.') if '.' in plain else plain


def _digest(binding: dict[str, Any], evidence: dict[str, Any]) -> str:
    canonical = json.dumps({'binding': binding, 'evidence': evidence},
                           sort_keys=True, separators=(',', ':'), ensure_ascii=True)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _authorizer(action: int, arg1: str | None, arg2: str | None,
                database: str | None, source: str | None) -> int:
    if action == sqlite3.SQLITE_PRAGMA:
        return sqlite3.SQLITE_OK if arg1 in ('table_info', 'schema_version', 'data_version') and (arg1 == 'table_info' or arg2 is None) else sqlite3.SQLITE_DENY
    if action == sqlite3.SQLITE_TRANSACTION:
        return sqlite3.SQLITE_OK if arg1 in ('BEGIN', 'ROLLBACK') else sqlite3.SQLITE_DENY
    allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ}
    return sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY


def _rows(path: Path, maximum: int, now: int) -> tuple[bool, list[dict[str, Any]], set[int], dict[str, Any]]:
    # An active WAL could require a shared-memory write or represent newer
    # records than the main file. Refuse this snapshot without touching sidecars.
    if not path.is_file() or any(path.with_name(path.name + suffix).exists()
                                 for suffix in ('-wal', '-shm', '-journal')):
        raise OSError('database snapshot unavailable')
    with path.open('rb') as stream:
        header = stream.read(100)
    if (header[:16] != b'SQLite format 3\x00' or len(header) < 100
            or header[18] != 1 or header[19] != 1):
        # WAL-mode files require a different snapshot protocol; never create or
        # touch their shared-memory and WAL sidecars during observation.
        raise OSError('unsupported database journal mode')
    uri = 'file:' + quote(str(path.resolve()), safe='/') + '?mode=ro'
    with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=1)) as conn:
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute('PRAGMA query_only=ON')
        conn.set_authorizer(_authorizer)
        try:
            try:
                conn.execute('BEGIN')
                schema_version = conn.execute('PRAGMA schema_version').fetchone()[0]
                data_version = conn.execute('PRAGMA data_version').fetchone()[0]
            except sqlite3.Error as exc:
                raise _SnapshotUnavailable() from exc
            provenance = {'snapshot_started_at_ms': now, 'schema_version': schema_version,
                          'data_version': data_version,
                          'database_identity_digest': hashlib.sha256(header).hexdigest()}
            columns = {row[1]: row for row in conn.execute('PRAGMA table_info(live_trades)')}
            if not _REQUIRED.issubset(columns) or columns['id'][5] != 1:
                if 'id' not in columns or 'status' not in columns:
                    return False, [{'id': None}], set(), provenance
                ids = conn.execute("SELECT id FROM live_trades WHERE status = ? ORDER BY id LIMIT ?",
                                   ('ORDER_SENT', maximum)).fetchall()
                return False, [{'id': row[0]} for row in ids], set(), provenance
            names = ('id', 'exchange_order_id', 'external_oid', 'native_symbol',
                     'side', 'requested_quantity', 'submitted_at_ms', 'bot_generated')
            query = ('SELECT ' + ','.join(names) + ' FROM live_trades '
                     'WHERE status = ? ORDER BY id')
            records = []
            owners: dict[tuple[str, str], int] = {}
            ambiguous: set[int] = set()
            for raw in conn.execute(query, ('ORDER_SENT',)):
                row = dict(zip(names, raw))
                if len(records) < maximum:
                    records.append(row)
                try:
                    binding = _binding(row, now)
                except ValueError:
                    continue
                for kind, key in (('order', binding['exchange_order_id']),
                                  ('oid', binding['external_oid'])):
                    identity = (kind, key)
                    previous = owners.setdefault(identity, binding['trade_id'])
                    if previous != binding['trade_id']:
                        ambiguous.update((previous, binding['trade_id']))
            return True, records, ambiguous, provenance
        finally:
            with contextlib.suppress(sqlite3.Error):
                conn.execute('ROLLBACK')
            conn.set_authorizer(None)


class _SnapshotUnavailable(Exception):
    pass


def _binding(row: dict[str, Any], now: int) -> dict[str, Any]:
    if (type(row['id']) is not int or row['id'] <= 0
            or type(row['exchange_order_id']) is not str
            or _ORDER_ID.fullmatch(row['exchange_order_id']) is None
            or type(row['external_oid']) is not str or len(row['external_oid']) > 32
            or _OID.fullmatch(row['external_oid']) is None
            or row['bot_generated'] != 1 or type(row['bot_generated']) is not int
            or type(row['native_symbol']) is not str
            or _SYMBOL.fullmatch(row['native_symbol']) is None
            or row['side'] not in ('LONG', 'SHORT') or type(row['side']) is not str
            or type(row['submitted_at_ms']) is not int or row['submitted_at_ms'] <= 0):
        raise ValueError('insufficient local binding')
    return {'trade_id': row['id'], 'exchange_order_id': row['exchange_order_id'],
            'external_oid': row['external_oid'], 'symbol': row['native_symbol'],
            'side': 1 if row['side'] == 'LONG' else 3,
            'quantity': _canonical(_positive(row['requested_quantity'])),
            'submitted_at_ms': row['submitted_at_ms']}


def _observe(binding: dict[str, Any], response: Any, now: int) -> tuple[str, str | None, dict[str, Any]]:
    unavailable = ('EVIDENCE_UNAVAILABLE', None, {})
    if not isinstance(response, dict) or response.get('success') is not True:
        return unavailable
    data = response.get('data')
    if not isinstance(data, dict):
        return unavailable
    if data.get('status') not in ('FILLED', 'PARTIALLY_FILLED', 'NEW', 'CANCELLED', 'REJECTED', 'EXPIRED') or type(data.get('status')) is not str:
        timestamp = data.get('exchange_fill_timestamp')
        return 'EVIDENCE_UNAVAILABLE', None, ({'exchange_timestamp_ms': timestamp}
                if type(timestamp) is int and timestamp > 0 else {})
    if 'external_oid' not in data:
        # The current audited adapter omits this field. No match is possible.
        return 'INSUFFICIENT_BINDING', None, {}
    try:
        order_id, oid, symbol, side, state = (data[name] for name in (
            'exchange_order_id', 'external_oid', 'symbol', 'side', 'status'))
        if (type(order_id) is not str or _ORDER_ID.fullmatch(order_id) is None
                or type(oid) is not str or _OID.fullmatch(oid) is None
                or type(symbol) is not str or _SYMBOL.fullmatch(symbol) is None
                or type(side) is not int or side not in (1, 3) or type(state) is not str):
            return 'CONFLICTING_EVIDENCE', None, {}
        requested = _positive(data['requested_quantity'])
        cumulative = _positive(data['cumulative_filled_quantity'], zero=True)
        price = _positive(data['average_fill_price'], zero=True)
        timestamp = data['exchange_fill_timestamp']
        if type(timestamp) is not int:
            return 'STALE_OR_INVALID_TIMESTAMP', None, {}
        for key, aliases, number in (
                ('requested_quantity', ('vol',), requested),
                ('cumulative_filled_quantity', ('dealVol',), cumulative),
                ('average_fill_price', ('dealAvgPrice', 'dealAvgPriceStr'), price)):
            for alias in aliases:
                if alias in data and _positive(data[alias], zero=True) != number:
                    return 'CONFLICTING_EVIDENCE', None, {}
        for key, alias in (('exchange_order_id', 'orderId'), ('external_oid', 'externalOid'),
                           ('symbol', 'native_symbol'), ('side', 'order_side'),
                           ('status', 'state'), ('exchange_fill_timestamp', 'fill_time')):
            if alias in data and (type(data[alias]) is not type(data[key]) or data[alias] != data[key]):
                return 'CONFLICTING_EVIDENCE', None, {}
        normalized = {'exchange_order_id': order_id, 'external_oid': oid, 'symbol': symbol,
                      'side': side, 'requested_quantity': _canonical(requested),
                      'cumulative_filled_quantity': _canonical(cumulative),
                      'average_fill_price': _canonical(price),
                      'exchange_fill_timestamp': timestamp, 'status': state}
        digest = _digest(binding, normalized)
        details = {'evidence_status': state, 'cumulative_quantity': _canonical(cumulative),
                   'average_fill_price': _canonical(price), 'exchange_timestamp_ms': timestamp}
        if (order_id != binding['exchange_order_id'] or oid != binding['external_oid']
                or symbol != binding['symbol'] or side != binding['side']):
            return 'IDENTITY_MISMATCH', digest, details
        if requested != Decimal(binding['quantity']) or cumulative > requested:
            return 'QUANTITY_MISMATCH', digest, details
        if timestamp < binding['submitted_at_ms'] or timestamp > now + _SKEW_MS or timestamp <= 0:
            return 'STALE_OR_INVALID_TIMESTAMP', digest, details
        if state in ('CANCELLED', 'REJECTED', 'EXPIRED'):
            return 'CANCELLED_OR_REJECTED', digest, details
        if state == 'NEW':
            return ('ZERO_FILL' if cumulative == 0 and price == 0 else 'CONFLICTING_EVIDENCE'), digest, details
        if state == 'PARTIALLY_FILLED':
            return ('PARTIAL_FILL' if 0 < cumulative < requested and price > 0 else 'CONFLICTING_EVIDENCE'), digest, details
        return ('FULL_FILL_MATCH' if cumulative == requested and price > 0 else 'CONFLICTING_EVIDENCE'), digest, details
    except (KeyError, ValueError, InvalidOperation, TypeError):
        return 'CONFLICTING_EVIDENCE', None, {}


def _report_hash(report: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(report, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=True).encode()).hexdigest()


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate key')
        result[key] = value
    return result


def _prior(path: str) -> dict[str, dict[str, Any]]:
    if Path(path).is_symlink() or Path(path).stat().st_size > 1024 * 1024:
        raise ValueError('invalid previous report')
    report = json.loads(Path(path).read_text(encoding='utf-8'), object_pairs_hook=_unique_pairs)
    if (type(report) is not dict or set(report) != {'version', 'status', 'schema_supported',
            'snapshot', 'observations', 'report_sha256'} or report.get('version') != _REPORT_VERSION
            or report.get('status') != 'PASS' or type(report.get('report_sha256')) is not str):
        raise ValueError('invalid previous report')
    checksum = report.pop('report_sha256')
    snapshot = report.get('snapshot')
    if (checksum != _report_hash(report) or type(report.get('observations')) is not list
            or type(report.get('schema_supported')) is not bool or type(snapshot) is not dict
            or set(snapshot) != {'snapshot_started_at_ms', 'schema_version', 'data_version',
                                 'database_identity_digest'}
            or any(type(snapshot[key]) is not int or snapshot[key] < 0 for key in
                   ('snapshot_started_at_ms', 'schema_version', 'data_version'))
            or type(snapshot['database_identity_digest']) is not str
            or re.fullmatch(r'[0-9a-f]{64}', snapshot['database_identity_digest']) is None):
        raise ValueError('invalid previous report')
    entries = {}
    for item in report['observations']:
        if type(item) is not dict or (item.get('binding_digest') is not None and
                (type(item['binding_digest']) is not str or
                 re.fullmatch(r'[0-9a-f]{64}', item['binding_digest']) is None)):
            raise ValueError('invalid previous report')
        key = item['binding_digest']
        if (type(item.get('observed_at_ms')) is not int or item['observed_at_ms'] <= 0
                or (key is not None and key in entries) or item.get('classification') not in (
                'FULL_FILL_MATCH', 'PARTIAL_FILL', 'ZERO_FILL', 'CANCELLED_OR_REJECTED',
                'IDENTITY_MISMATCH', 'QUANTITY_MISMATCH', 'CONFLICTING_EVIDENCE',
                'INSUFFICIENT_BINDING', 'EVIDENCE_UNAVAILABLE', 'STALE_OR_INVALID_TIMESTAMP')):
            raise ValueError('invalid previous report')
        if item.get('evidence_status') is not None:
            _positive(item.get('cumulative_quantity'), zero=True)
            _positive(item.get('average_fill_price'), zero=True)
            if type(item.get('exchange_timestamp_ms')) is not int or item['exchange_timestamp_ms'] <= 0:
                raise ValueError('invalid previous report')
        if key is not None:
            entries[key] = item
    return entries


def _contradicts(previous: dict[str, Any], current: dict[str, Any]) -> bool:
    old_status, new_status = previous.get('evidence_status'), current.get('evidence_status')
    old_time, new_time = previous.get('exchange_timestamp_ms'), current.get('exchange_timestamp_ms')
    if type(old_time) is int and type(new_time) is int and new_time < old_time:
        return True
    if old_status is None or new_status is None:
        return False
    if (_positive(current['cumulative_quantity'], zero=True) <
            _positive(previous['cumulative_quantity'], zero=True)):
        return True
    if old_status == 'FILLED':
        return (new_status != 'FILLED' or any(previous.get(key) != current.get(key) for key in
                ('cumulative_quantity', 'average_fill_price', 'exchange_timestamp_ms')))
    return old_status in ('CANCELLED', 'REJECTED', 'EXPIRED') and old_status != new_status


def main(argv: list[str] | None = None, *, adapter: Any = None,
         clock_ms: Any = None) -> int:
    parser = _Parser(description='Observe bound MEXC orders without changing trade state')
    parser.add_argument('--database', required=True)
    parser.add_argument('--output-report', required=True)
    parser.add_argument('--max-records', type=int, default=5)
    parser.add_argument('--previous-report')
    try:
        args = parser.parse_args(argv)
        if (not 1 <= args.max_records <= 20 or not args.database or not args.output_report):
            raise ValueError('invalid arguments')
        now = (clock_ms or (lambda: int(time.time() * 1000)))()
        if type(now) is not int or now <= 0:
            raise ValueError('invalid observation time')
    except (ValueError, TypeError):
        print('{"reason":"INVALID_INPUT","status":"FAIL"}')
        return 1
    try:
        previous = _prior(args.previous_report) if args.previous_report else {}
    except (ValueError, OSError, UnicodeError, json.JSONDecodeError):
        print('{"reason":"PREVIOUS_REPORT_INVALID","status":"FAIL"}')
        return 1
    try:
        supported, records, ambiguous, snapshot = _rows(Path(args.database), args.max_records, now)
        observations = []
        active_adapter = adapter
        for record in records:
            category, digest, details = 'INSUFFICIENT_BINDING', None, {}
            entry = {'classification': category, 'digest': digest, 'observed_at_ms': now,
                     'exchange_timestamp_ms': None, 'binding_digest': None}
            if supported:
                try:
                    binding = _binding(record, now)
                except ValueError:
                    pass
                else:
                    entry['binding_digest'] = _digest(binding, {})
                    if record['id'] in ambiguous:
                        category = 'CONFLICTING_EVIDENCE'
                        entry['reason'] = 'AMBIGUOUS_OWNERSHIP'
                    elif binding['submitted_at_ms'] > now + _SKEW_MS:
                        category = 'STALE_OR_INVALID_TIMESTAMP'
                    elif active_adapter is None and (not os.environ.get('MEXC_FUTURES_API_KEY')
                                                     or not os.environ.get('MEXC_FUTURES_API_SECRET')):
                        category = 'EVIDENCE_UNAVAILABLE'
                        entry['classification'] = category
                        observations.append(entry)
                        break
                    else:
                        if active_adapter is None:
                            active_adapter = ReadOnlyMexcOrderStatusAdapter(enabled=True)
                        try:
                            response = active_adapter.get_order_status(binding['exchange_order_id'],
                                                                       binding['symbol'])
                        except Exception:
                            response = None
                        category, digest, details = _observe(binding, response, now)
                        entry.update(details)
                        entry.update(classification=category, digest=digest)
                        if entry['binding_digest'] in previous and _contradicts(previous[entry['binding_digest']], entry):
                            entry['classification'] = 'CONFLICTING_EVIDENCE'
                            entry['reason'] = 'NON_MONOTONIC_EVIDENCE'
                        observations.append(entry)
                        if category == 'EVIDENCE_UNAVAILABLE':
                            break  # Includes rate limiting: never retry within one run.
                        continue
            entry.update(classification=category, digest=digest)
            observations.append(entry)
        report = {'version': _REPORT_VERSION, 'status': 'PASS', 'schema_supported': supported,
                  'snapshot': snapshot, 'observations': observations}
        report['report_sha256'] = _report_hash(report)
        payload = json.dumps(report, sort_keys=True, separators=(',', ':')) + '\n'
        _write_new_report(args.output_report, payload)
    except _SnapshotUnavailable:
        print('{"reason":"SNAPSHOT_UNAVAILABLE","status":"FAIL"}')
        return 1
    except (OSError, sqlite3.Error):
        print('{"reason":"OBSERVATION_ERROR","status":"FAIL"}')
        return 1
    except Exception:
        print('{"reason":"EVIDENCE_UNAVAILABLE","status":"FAIL"}')
        return 1
    # The report is authoritative after publication; console output is best effort.
    try:
        print('{"status":"PASS"}')
    except Exception:
        pass
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
