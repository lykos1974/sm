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
_REPORT_VERSION = 4
_REGISTRY_VERSION = 1
_ANCHOR_VERSION = 1
_EMPTY_ANCHOR_DIGEST = hashlib.sha256(b'null').hexdigest()
_GENESIS_CHAIN_DIGEST = hashlib.sha256(b'mexc-observer-history-v1').hexdigest()
_DETERMINATE = frozenset(('FULL_FILL_MATCH', 'PARTIAL_FILL', 'ZERO_FILL',
                          'CANCELLED_OR_REJECTED'))
_HEX_DIGEST = re.compile(r'[0-9a-f]{64}\Z')
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
            return ('CANCELLED_OR_REJECTED' if cumulative < requested and
                    (price > 0 if cumulative > 0 else price == 0) else
                    'CONFLICTING_EVIDENCE'), digest, details
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


def _is_digest(value: Any) -> bool:
    return type(value) is str and _HEX_DIGEST.fullmatch(value) is not None


def _anchor_digest(anchor: dict[str, Any] | None) -> str:
    return _report_hash(anchor) if anchor is not None else _EMPTY_ANCHOR_DIGEST


def _chain_digest(entry: dict[str, Any]) -> str:
    return _report_hash({key: entry[key] for key in (
        'previous_history_chain_digest', 'previous_confirmed_anchor_digest',
        'digest', 'classification', 'confirmed_anchor_digest')})


def _validate_anchor(anchor: Any, binding_digest: str | None) -> None:
    if (type(anchor) is not dict or set(anchor) != {
            'version', 'binding_digest', 'evidence_digest', 'status',
            'requested_quantity', 'cumulative_quantity', 'average_fill_price',
            'exchange_timestamp_ms'} or type(anchor['version']) is not int or
            anchor['version'] != _ANCHOR_VERSION or not _is_digest(anchor['binding_digest']) or
            anchor['binding_digest'] != binding_digest or not _is_digest(anchor['evidence_digest']) or
            anchor['status'] not in ('NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED',
                                     'REJECTED', 'EXPIRED') or type(anchor['status']) is not str or
            type(anchor['exchange_timestamp_ms']) is not int or
            anchor['exchange_timestamp_ms'] <= 0):
        raise ValueError('invalid confirmed anchor')
    requested = _positive(anchor['requested_quantity'])
    cumulative = _positive(anchor['cumulative_quantity'], zero=True)
    price = _positive(anchor['average_fill_price'], zero=True)
    if (any(anchor[key] != _canonical(number) for key, number in (
            ('requested_quantity', requested), ('cumulative_quantity', cumulative),
            ('average_fill_price', price))) or cumulative > requested):
        raise ValueError('invalid confirmed anchor')
    status = anchor['status']
    valid = ((status == 'NEW' and cumulative == 0 and price == 0) or
             (status == 'PARTIALLY_FILLED' and 0 < cumulative < requested and price > 0) or
             (status == 'FILLED' and cumulative == requested and price > 0) or
             (status in ('CANCELLED', 'REJECTED', 'EXPIRED') and cumulative < requested and
              (price > 0 if cumulative > 0 else price == 0)))
    if not valid:
        raise ValueError('invalid confirmed anchor')


def _anchor_for(entry: dict[str, Any], binding: dict[str, Any]) -> dict[str, Any]:
    anchor = {'version': _ANCHOR_VERSION, 'binding_digest': entry['binding_digest'],
              'evidence_digest': entry['digest'], 'status': entry['evidence_status'],
              'requested_quantity': binding['quantity'],
              'cumulative_quantity': entry['cumulative_quantity'],
              'average_fill_price': entry['average_fill_price'],
              'exchange_timestamp_ms': entry['exchange_timestamp_ms']}
    _validate_anchor(anchor, entry['binding_digest'])
    return anchor


def _contradicts(previous: dict[str, Any], current: dict[str, Any]) -> bool:
    old_status, new_status = previous['status'], current['status']
    if (previous['binding_digest'] != current['binding_digest'] or
            previous['requested_quantity'] != current['requested_quantity']):
        return True
    if old_status == 'FILLED' or old_status in ('CANCELLED', 'REJECTED', 'EXPIRED'):
        return previous != current
    transitions = {'NEW': frozenset(('NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED',
                                     'REJECTED', 'EXPIRED')),
                   'PARTIALLY_FILLED': frozenset(('PARTIALLY_FILLED', 'FILLED',
                                                'CANCELLED', 'REJECTED', 'EXPIRED'))}
    if new_status not in transitions[old_status]:
        return True
    old_qty = _positive(previous['cumulative_quantity'], zero=True)
    new_qty = _positive(current['cumulative_quantity'], zero=True)
    if (new_qty < old_qty or current['exchange_timestamp_ms'] < previous['exchange_timestamp_ms']
            or (new_qty == old_qty and current['average_fill_price'] != previous['average_fill_price'])):
        return True
    return False


def _seal_observation(entry: dict[str, Any], previous: dict[str, Any] | None,
                      binding: dict[str, Any] | None) -> None:
    prior_anchor = previous['confirmed_evidence_anchor'] if previous else None
    entry['previous_confirmed_evidence_anchor'] = prior_anchor
    entry['previous_history_chain_digest'] = (previous['history_chain_digest'] if previous
                                               else _GENESIS_CHAIN_DIGEST)
    entry['previous_confirmed_anchor_digest'] = (previous['confirmed_anchor_digest'] if previous
                                                  else _EMPTY_ANCHOR_DIGEST)
    anchor = prior_anchor
    if prior_anchor is not None and entry['classification'] in ('IDENTITY_MISMATCH',
                                                                'QUANTITY_MISMATCH'):
        entry['classification'] = 'CONFLICTING_EVIDENCE'
        entry['reason'] = 'NON_MONOTONIC_EVIDENCE'
    if entry['classification'] in _DETERMINATE:
        if binding is None:
            raise ValueError('determinate evidence requires complete binding')
        candidate = _anchor_for(entry, binding)
        if prior_anchor is not None and _contradicts(prior_anchor, candidate):
            entry['classification'] = 'CONFLICTING_EVIDENCE'
            entry['reason'] = 'NON_MONOTONIC_EVIDENCE'
        else:
            anchor = candidate
    entry['confirmed_evidence_anchor'] = anchor
    entry['confirmed_anchor_digest'] = _anchor_digest(anchor)
    entry['history_chain_digest'] = _chain_digest(entry)


def _identity(binding: dict[str, Any]) -> dict[str, str]:
    return {'binding_digest': _digest(binding, {}),
            'local_identity_digest': _report_hash({'trade_id': binding['trade_id']}),
            'exchange_order_identity_digest': _report_hash({'order_id': binding['exchange_order_id']}),
            'external_oid_identity_digest': _report_hash({'external_oid': binding['external_oid']})}


def _new_registry_entry(binding: dict[str, Any], conflict: str | None = None) -> dict[str, Any]:
    entry = {'version': _REGISTRY_VERSION, **_identity(binding),
             'lineage_origin': 'CONFLICT' if conflict else 'GENESIS',
             'conflicting_prior_binding_digest': conflict,
             'confirmed_evidence_anchor': None,
             'confirmed_anchor_digest': _EMPTY_ANCHOR_DIGEST,
             'history_chain_digest': _GENESIS_CHAIN_DIGEST,
             'last_successful_observation': None,
             'present_in_snapshot': True,
             'current_observation_state': 'CONFLICTING_EVIDENCE' if conflict else 'GENESIS'}
    return entry


def _persisted_conflict(entry: dict[str, Any]) -> bool:
    return (entry['lineage_origin'] == 'CONFLICT' or
            entry['current_observation_state'] == 'CONFLICTING_EVIDENCE')


def _validate_registry(entries: Any, count: Any, root: Any) -> dict[str, dict[str, Any]]:
    if type(entries) is not list or type(count) is not int or count != len(entries) or not _is_digest(root):
        raise ValueError('invalid binding registry')
    by_binding = {}
    expected_keys = {'version', 'binding_digest', 'local_identity_digest',
                     'exchange_order_identity_digest', 'external_oid_identity_digest',
                     'lineage_origin', 'conflicting_prior_binding_digest',
                     'confirmed_evidence_anchor', 'confirmed_anchor_digest',
                     'history_chain_digest', 'last_successful_observation',
                     'current_observation_state', 'present_in_snapshot'}
    for entry in entries:
        if (type(entry) is not dict or set(entry) != expected_keys or
                type(entry['version']) is not int or entry['version'] != _REGISTRY_VERSION or
                any(not _is_digest(entry[key]) for key in (
                    'binding_digest', 'local_identity_digest', 'exchange_order_identity_digest',
                    'external_oid_identity_digest', 'confirmed_anchor_digest',
                    'history_chain_digest')) or
                type(entry['present_in_snapshot']) is not bool or
                entry['lineage_origin'] not in ('GENESIS', 'CONFLICT') or
                entry['current_observation_state'] not in (
                    'GENESIS', 'OBSERVED', 'UNOBSERVED', 'NOT_PRESENT_IN_CURRENT_SNAPSHOT',
                    'CONFLICTING_EVIDENCE') or
                (entry['conflicting_prior_binding_digest'] is not None and
                 not _is_digest(entry['conflicting_prior_binding_digest'])) or
                (entry['lineage_origin'] == 'CONFLICT') !=
                (entry['conflicting_prior_binding_digest'] is not None) or
                (entry['current_observation_state'] == 'NOT_PRESENT_IN_CURRENT_SNAPSHOT' and
                 entry['present_in_snapshot'])):
            raise ValueError('invalid binding registry')
        key = entry['binding_digest']
        if key in by_binding or (by_binding and key <= next(reversed(by_binding))):
            raise ValueError('duplicate or unordered binding')
        anchor = entry['confirmed_evidence_anchor']
        if anchor is not None:
            _validate_anchor(anchor, key)
        if entry['confirmed_anchor_digest'] != _anchor_digest(anchor):
            raise ValueError('invalid registry anchor digest')
        last = entry['last_successful_observation']
        if anchor is None:
            if last is not None:
                raise ValueError('invalid registry metadata')
        elif (type(last) is not dict or set(last) != {'observed_at_ms', 'observation_digest',
                                                      'classification', 'exchange_timestamp_ms'} or
              type(last['observed_at_ms']) is not int or last['observed_at_ms'] <= 0 or
              last['observation_digest'] != anchor['evidence_digest'] or
              last['classification'] != _anchor_classification(anchor['status']) or
              last['exchange_timestamp_ms'] != anchor['exchange_timestamp_ms']):
            raise ValueError('invalid registry metadata')
        if entry['history_chain_digest'] == _GENESIS_CHAIN_DIGEST and anchor is not None:
            raise ValueError('invalid genesis anchor')
        by_binding[key] = entry
    if root != _report_hash(entries):
        raise ValueError('invalid registry root')
    for entry in entries:
        conflict = entry['conflicting_prior_binding_digest']
        if conflict is not None and (conflict not in by_binding or conflict == entry['binding_digest']):
            raise ValueError('orphaned registry conflict')
    return by_binding


def _report_chain(report: dict[str, Any]) -> str:
    return _report_hash({key: report[key] for key in (
        'previous_report_digest', 'previous_registry_root_digest',
        'database_binding_set_digest', 'registry_root_digest')} | {
        'observations': [(item['binding_digest'], item['digest'], item['classification'])
                         for item in report['observations']]})


def _registry_for_snapshot(prior: dict[str, dict[str, Any]],
                           records: list[dict[str, Any]], now: int) -> tuple[
                               dict[str, dict[str, Any]], dict[int, dict[str, Any]],
                               dict[int, str], str]:
    registry = {key: {**value, 'present_in_snapshot': False,
                      'current_observation_state': ('CONFLICTING_EVIDENCE'
                                                    if _persisted_conflict(value)
                                                    else 'NOT_PRESENT_IN_CURRENT_SNAPSHOT')}
                for key, value in prior.items()}
    prior_identity: dict[tuple[str, str], str] = {}
    for key, value in prior.items():
        for name in ('local_identity_digest', 'exchange_order_identity_digest',
                     'external_oid_identity_digest'):
            prior_identity.setdefault((name, value[name]), key)
    bindings: dict[int, dict[str, Any]] = {}
    conflicts: dict[int, str] = {}
    binding_set: list[str] = []
    for row in records:
        try:
            binding = _binding(row, now)
        except ValueError:
            if type(row.get('id')) is int:
                previous = prior_identity.get(('local_identity_digest',
                                               _report_hash({'trade_id': row['id']})))
                if previous:
                    conflicts[row['id']] = previous
                    registry[previous]['current_observation_state'] = 'CONFLICTING_EVIDENCE'
            continue
        key = _digest(binding, {})
        binding_set.append(key)
        bindings[binding['trade_id']] = binding
        identity = _identity(binding)
        if key in registry:
            if any(registry[key][name] != identity[name] for name in identity):
                raise ValueError('inconsistent binding identity')
            if _persisted_conflict(registry[key]):
                conflicts[binding['trade_id']] = key
                registry[key]['current_observation_state'] = 'CONFLICTING_EVIDENCE'
            else:
                registry[key]['current_observation_state'] = 'UNOBSERVED'
            registry[key]['present_in_snapshot'] = True
            continue
        conflict = next((prior_identity[(name, identity[name])] for name in (
            'local_identity_digest', 'exchange_order_identity_digest',
            'external_oid_identity_digest') if (name, identity[name]) in prior_identity), None)
        registry[key] = _new_registry_entry(binding, conflict)
        if conflict:
            conflicts[binding['trade_id']] = conflict
            registry[conflict]['current_observation_state'] = 'CONFLICTING_EVIDENCE'
    return registry, bindings, conflicts, _report_hash(sorted(binding_set))


def _adapter_ineligibility(record: dict[str, Any], binding: dict[str, Any] | None,
                           registry: dict[str, dict[str, Any]], conflicts: dict[int, str],
                           ambiguous: set[int], now: int, request_count: int,
                           maximum: int) -> tuple[str, str | None] | None:
    trade_id = record.get('id')
    if type(trade_id) is int and trade_id in conflicts:
        return 'CONFLICTING_EVIDENCE', 'LOCAL_BINDING_MUTATION'
    if binding is None:
        return 'INSUFFICIENT_BINDING', None
    state = registry.get(_digest(binding, {}))
    if state is None or _persisted_conflict(state):
        return 'CONFLICTING_EVIDENCE', 'PERSISTED_BINDING_CONFLICT'
    if trade_id in ambiguous:
        return 'CONFLICTING_EVIDENCE', 'AMBIGUOUS_OWNERSHIP'
    if binding['submitted_at_ms'] > now + _SKEW_MS:
        return 'STALE_OR_INVALID_TIMESTAMP', None
    if request_count >= maximum:
        return 'OBSERVATION_LIMIT', None
    return None


def _update_registry(registry: dict[str, dict[str, Any]], entry: dict[str, Any],
                     *, attempted: bool) -> None:
    key = entry['binding_digest']
    if key is None or key not in registry:
        return
    item = registry[key]
    if attempted:
        item['confirmed_evidence_anchor'] = entry['confirmed_evidence_anchor']
        item['confirmed_anchor_digest'] = entry['confirmed_anchor_digest']
        item['history_chain_digest'] = entry['history_chain_digest']
        if entry['classification'] in _DETERMINATE:
            item['last_successful_observation'] = {
                'observed_at_ms': entry['observed_at_ms'],
                'observation_digest': entry['digest'],
                'classification': entry['classification'],
                'exchange_timestamp_ms': entry['exchange_timestamp_ms']}
        item['current_observation_state'] = ('CONFLICTING_EVIDENCE' if
            entry['classification'] == 'CONFLICTING_EVIDENCE' else 'OBSERVED')
    elif entry['classification'] == 'CONFLICTING_EVIDENCE':
        item['current_observation_state'] = 'CONFLICTING_EVIDENCE'


def _prior(path: str) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if Path(path).is_symlink() or Path(path).stat().st_size > 1024 * 1024:
        raise ValueError('invalid previous report')
    report = json.loads(Path(path).read_text(encoding='utf-8'), object_pairs_hook=_unique_pairs)
    if (type(report) is not dict or set(report) != {'version', 'confirmed_evidence_anchor_version',
            'status', 'schema_supported', 'snapshot', 'observations', 'report_sha256',
            'binding_registry', 'registry_count', 'registry_root_digest',
            'previous_report_digest', 'previous_registry_root_digest',
            'database_binding_set_digest', 'report_chain_digest'}
            or type(report.get('confirmed_evidence_anchor_version')) is not int or
            report['confirmed_evidence_anchor_version'] != _ANCHOR_VERSION or
            report.get('version') != _REPORT_VERSION
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
    registry = _validate_registry(report['binding_registry'], report['registry_count'],
                                  report['registry_root_digest'])
    if (any(not _is_digest(report[key]) for key in (
            'previous_report_digest', 'previous_registry_root_digest',
            'database_binding_set_digest', 'report_chain_digest')) or
            report['database_binding_set_digest'] != _report_hash(sorted(
                key for key, item in registry.items() if item['present_in_snapshot'])) or
            report['report_chain_digest'] != _report_chain(report)):
        raise ValueError('invalid report chain')
    if (report['previous_report_digest'] == _GENESIS_CHAIN_DIGEST and
            (report['previous_registry_root_digest'] != _report_hash([]) or
             any(not item['present_in_snapshot'] or item['lineage_origin'] != 'GENESIS'
                 for item in registry.values()))):
        raise ValueError('invalid genesis report chain')
    entries = {}
    for item in report['observations']:
        required = {'classification', 'digest', 'observed_at_ms', 'exchange_timestamp_ms',
                    'binding_digest', 'confirmed_evidence_anchor', 'confirmed_anchor_digest',
                    'history_chain_digest', 'previous_history_chain_digest',
                    'previous_confirmed_anchor_digest', 'previous_confirmed_evidence_anchor'}
        optional = {'reason', 'evidence_status', 'cumulative_quantity', 'average_fill_price',
                    'observation_performed', 'history_advanced'}
        if (type(item) is not dict or not required.issubset(item) or
                set(item) - required - optional or
                (item['binding_digest'] is not None and not _is_digest(item['binding_digest'])) or
                (item['digest'] is not None and not _is_digest(item['digest'])) or
                any(not _is_digest(item[key]) for key in (
                    'confirmed_anchor_digest', 'history_chain_digest',
                    'previous_history_chain_digest', 'previous_confirmed_anchor_digest'))):
            raise ValueError('invalid previous report')
        key = item['binding_digest']
        if (type(item.get('observed_at_ms')) is not int or item['observed_at_ms'] <= 0
                or (key is not None and key in entries) or item.get('classification') not in (
                'FULL_FILL_MATCH', 'PARTIAL_FILL', 'ZERO_FILL', 'CANCELLED_OR_REJECTED',
                'IDENTITY_MISMATCH', 'QUANTITY_MISMATCH', 'CONFLICTING_EVIDENCE',
                'INSUFFICIENT_BINDING', 'EVIDENCE_UNAVAILABLE', 'STALE_OR_INVALID_TIMESTAMP')):
            raise ValueError('invalid previous report')
        old_anchor, anchor = (item['previous_confirmed_evidence_anchor'],
                              item['confirmed_evidence_anchor'])
        if old_anchor is not None:
            _validate_anchor(old_anchor, key)
        if anchor is not None:
            _validate_anchor(anchor, key)
        skipped = item.get('history_advanced') is False
        if (('history_advanced' in item and not skipped) or
                ('observation_performed' in item and type(item['observation_performed']) is not bool) or
                (skipped and (item['classification'] != 'EVIDENCE_UNAVAILABLE' or
                             item['digest'] is not None or
                             item['history_chain_digest'] != item['previous_history_chain_digest']))):
            raise ValueError('invalid skipped observation')
        if (item['previous_confirmed_anchor_digest'] != _anchor_digest(old_anchor)
                or item['confirmed_anchor_digest'] != _anchor_digest(anchor)
                or (not skipped and item['history_chain_digest'] != _chain_digest(item))
                or (old_anchor is None and item['previous_confirmed_anchor_digest'] !=
                    _EMPTY_ANCHOR_DIGEST)
                or (anchor is None and item['confirmed_anchor_digest'] != _EMPTY_ANCHOR_DIGEST)):
            raise ValueError('invalid previous report')
        if item['classification'] in _DETERMINATE:
            if (anchor is None or item['digest'] != anchor['evidence_digest'] or
                    item.get('evidence_status') != anchor['status'] or
                    item.get('cumulative_quantity') != anchor['cumulative_quantity'] or
                    item.get('average_fill_price') != anchor['average_fill_price'] or
                    item.get('exchange_timestamp_ms') != anchor['exchange_timestamp_ms'] or
                    _anchor_classification(anchor['status']) != item['classification'] or
                    (old_anchor is not None and _contradicts(old_anchor, anchor))):
                raise ValueError('invalid previous report')
        elif anchor != old_anchor:
            raise ValueError('invalid previous report')
        if item['previous_history_chain_digest'] == _GENESIS_CHAIN_DIGEST and old_anchor is not None:
            raise ValueError('invalid previous report')
        if key is not None:
            entries[key] = item
            if (key not in registry or
                    registry[key]['history_chain_digest'] != item['history_chain_digest'] or
                    registry[key]['confirmed_anchor_digest'] != item['confirmed_anchor_digest'] or
                    registry[key]['confirmed_evidence_anchor'] != item['confirmed_evidence_anchor']):
                raise ValueError('observation missing registry binding')
    report['report_sha256'] = checksum
    return report, registry


def _anchor_classification(status: str) -> str:
    return {'NEW': 'ZERO_FILL', 'PARTIALLY_FILLED': 'PARTIAL_FILL',
            'FILLED': 'FULL_FILL_MATCH', 'CANCELLED': 'CANCELLED_OR_REJECTED',
            'REJECTED': 'CANCELLED_OR_REJECTED', 'EXPIRED': 'CANCELLED_OR_REJECTED'}[status]


def main(argv: list[str] | None = None, *, adapter: Any = None,
         clock_ms: Any = None) -> int:
    parser = _Parser(description='Observe bound MEXC orders without changing trade state')
    parser.add_argument('--database', required=True)
    parser.add_argument('--output-report', required=True)
    parser.add_argument('--max-records', type=int, default=5)
    parser.add_argument('--previous-report')
    parser.add_argument('--genesis', action='store_true')
    try:
        args = parser.parse_args(argv)
        if (not 1 <= args.max_records <= 20 or not args.database or not args.output_report
                or args.genesis == bool(args.previous_report)):
            raise ValueError('invalid arguments')
        now = (clock_ms or (lambda: int(time.time() * 1000)))()
        if type(now) is not int or now <= 0:
            raise ValueError('invalid observation time')
    except (ValueError, TypeError):
        print('{"reason":"INVALID_INPUT","status":"FAIL"}')
        return 1
    try:
        previous_report, previous_registry = (_prior(args.previous_report) if args.previous_report
                                              else (None, {}))
    except (ValueError, OSError, UnicodeError, json.JSONDecodeError):
        print('{"reason":"PREVIOUS_REPORT_INVALID","status":"FAIL"}')
        return 1
    try:
        supported, records, ambiguous, snapshot = _rows(Path(args.database), args.max_records, now)
        registry, bindings, conflicts, binding_set_digest = _registry_for_snapshot(
            previous_registry, records if supported else [], now)
        observations = []
        active_adapter = adapter
        request_count = 0
        for record in records:
            category, digest, details = 'INSUFFICIENT_BINDING', None, {}
            entry = {'classification': category, 'digest': digest, 'observed_at_ms': now,
                     'exchange_timestamp_ms': None, 'binding_digest': None}
            binding = bindings.get(record.get('id')) if supported else None
            attempted = False
            stop = False
            if binding is not None:
                entry['binding_digest'] = _digest(binding, {})
            elif type(record.get('id')) is int and record['id'] in conflicts:
                entry['binding_digest'] = conflicts[record['id']]
            ineligible = _adapter_ineligibility(record, binding, registry, conflicts,
                                                 ambiguous, now, request_count,
                                                 args.max_records)
            if ineligible is not None:
                category, reason = ineligible
                if category == 'OBSERVATION_LIMIT':
                    continue
                if reason is not None:
                    entry['reason'] = reason
            elif active_adapter is None and (not os.environ.get('MEXC_FUTURES_API_KEY')
                                             or not os.environ.get('MEXC_FUTURES_API_SECRET')):
                category = 'EVIDENCE_UNAVAILABLE'
                stop = True
            else:
                if active_adapter is None:
                    active_adapter = ReadOnlyMexcOrderStatusAdapter(enabled=True)
                # The same gate guards every adapter call, including injected adapters.
                if _adapter_ineligibility(record, binding, registry, conflicts,
                                          ambiguous, now, request_count,
                                          args.max_records) is not None:
                    raise ValueError('adapter eligibility changed')
                request_count += 1
                attempted = True
                try:
                    response = active_adapter.get_order_status(binding['exchange_order_id'],
                                                               binding['symbol'])
                except Exception:
                    response = None
                category, digest, details = _observe(binding, response, now)
                entry.update(details)
                stop = category == 'EVIDENCE_UNAVAILABLE'
            entry.update(classification=category, digest=digest)
            previous_entry = registry.get(entry['binding_digest'])
            if category != 'EVIDENCE_UNAVAILABLE':
                _seal_observation(entry, previous_entry, binding)
            else:
                # Indeterminate evidence is bound by the report chain; confirmed
                # per-binding history stays frozen until determinate evidence.
                entry['previous_confirmed_evidence_anchor'] = previous_entry['confirmed_evidence_anchor'] if previous_entry else None
                entry['previous_confirmed_anchor_digest'] = previous_entry['confirmed_anchor_digest'] if previous_entry else _EMPTY_ANCHOR_DIGEST
                entry['previous_history_chain_digest'] = previous_entry['history_chain_digest'] if previous_entry else _GENESIS_CHAIN_DIGEST
                entry['confirmed_evidence_anchor'] = entry['previous_confirmed_evidence_anchor']
                entry['confirmed_anchor_digest'] = entry['previous_confirmed_anchor_digest']
                entry['history_chain_digest'] = entry['previous_history_chain_digest']
                entry['observation_performed'] = attempted
                entry['history_advanced'] = False
            _update_registry(registry, entry, attempted=attempted or category != 'EVIDENCE_UNAVAILABLE')
            observations.append(entry)
            if stop:
                break
        registry_entries = [registry[key] for key in sorted(registry)]
        registry_root = _report_hash(registry_entries)
        report = {'version': _REPORT_VERSION,
                  'confirmed_evidence_anchor_version': _ANCHOR_VERSION,
                  'status': 'PASS', 'schema_supported': supported,
                  'snapshot': snapshot, 'observations': observations,
                  'binding_registry': registry_entries, 'registry_count': len(registry_entries),
                  'registry_root_digest': registry_root,
                  'database_binding_set_digest': binding_set_digest,
                  'previous_report_digest': (previous_report['report_sha256'] if previous_report
                                             else _GENESIS_CHAIN_DIGEST),
                  'previous_registry_root_digest': (previous_report['registry_root_digest']
                                                    if previous_report else _report_hash([]))}
        report['report_chain_digest'] = _report_chain(report)
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
