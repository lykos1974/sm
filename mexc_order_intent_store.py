"""Isolated durable intent bookkeeping. No exchange or trader integration."""
from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
from contextlib import closing, contextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator


class IntentError(ValueError):
    """An intent cannot safely be created, claimed, or advanced."""


_SCHEMA_VERSION = 1
_IDENTITY = ('intent_id', 'local_trade_id', 'account_id', 'venue', 'external_oid',
             'native_symbol', 'opening_side', 'requested_quantity')
_IMMUTABLE = _IDENTITY + ('order_type', 'order_price', 'strategy_decision_id',
                          'created_at_ms', 'intent_digest')
_COLUMNS = _IMMUTABLE + ('state', 'revision', 'attempted_at_ms', 'exchange_order_id',
                          'bound_at_ms', 'binding_digest', 'filled_quantity',
                          'average_fill_price', 'fill_timestamp_ms', 'fill_digest')
_SQL = '''
CREATE TABLE bot_order_intents (
 intent_id TEXT PRIMARY KEY NOT NULL,
 local_trade_id INTEGER NOT NULL CHECK(local_trade_id > 0),
 account_id TEXT NOT NULL CHECK(length(account_id) BETWEEN 1 AND 128),
 venue TEXT NOT NULL CHECK(venue = 'MEXC_FUTURES'),
 external_oid TEXT NOT NULL CHECK(length(external_oid) BETWEEN 1 AND 32),
 native_symbol TEXT NOT NULL CHECK(length(native_symbol) BETWEEN 3 AND 50),
 opening_side TEXT NOT NULL CHECK(opening_side IN ('LONG','SHORT')),
 requested_quantity TEXT NOT NULL CHECK(length(requested_quantity) BETWEEN 1 AND 128),
 order_type TEXT NOT NULL CHECK(order_type IN ('MARKET','LIMIT')),
 order_price TEXT,
 strategy_decision_id TEXT NOT NULL CHECK(length(strategy_decision_id) BETWEEN 1 AND 128),
 created_at_ms INTEGER NOT NULL CHECK(created_at_ms > 0),
 intent_digest TEXT NOT NULL CHECK(length(intent_digest) = 64),
 state TEXT NOT NULL CHECK(state IN ('PREPARED','SUBMISSION_UNCERTAIN','BOUND',
                                    'FILL_CONFIRMED','REJECTED','CONFLICT')),
 revision INTEGER NOT NULL CHECK(revision >= 0),
 attempted_at_ms INTEGER,
 exchange_order_id TEXT,
 bound_at_ms INTEGER,
 binding_digest TEXT,
 filled_quantity TEXT,
 average_fill_price TEXT,
 fill_timestamp_ms INTEGER,
 fill_digest TEXT,
 CHECK ((order_type = 'MARKET' AND order_price IS NULL) OR
        (order_type = 'LIMIT' AND order_price IS NOT NULL)),
 CHECK ((state = 'PREPARED' AND revision = 0 AND attempted_at_ms IS NULL) OR
        (state = 'CONFLICT' AND revision > 0 AND attempted_at_ms IS NULL) OR
        (state NOT IN ('PREPARED','CONFLICT') AND revision > 0
         AND attempted_at_ms IS NOT NULL AND attempted_at_ms >= created_at_ms) OR
        (state = 'CONFLICT' AND revision > 0 AND attempted_at_ms IS NOT NULL
         AND attempted_at_ms >= created_at_ms)),
 CHECK ((state IN ('PREPARED','SUBMISSION_UNCERTAIN') AND
         exchange_order_id IS NULL AND bound_at_ms IS NULL AND binding_digest IS NULL) OR
        (state = 'CONFLICT') OR
        (state IN ('BOUND','FILL_CONFIRMED','REJECTED') AND
         exchange_order_id IS NOT NULL AND bound_at_ms IS NOT NULL
         AND binding_digest IS NOT NULL AND
         (attempted_at_ms IS NULL OR bound_at_ms >= attempted_at_ms))),
 CHECK ((state != 'FILL_CONFIRMED' AND filled_quantity IS NULL AND
         average_fill_price IS NULL AND fill_timestamp_ms IS NULL AND fill_digest IS NULL) OR
        (state = 'FILL_CONFIRMED' AND filled_quantity IS NOT NULL AND
         average_fill_price IS NOT NULL AND fill_timestamp_ms IS NOT NULL AND
         fill_timestamp_ms >= bound_at_ms AND fill_digest IS NOT NULL)),
 UNIQUE(account_id, venue, external_oid),
 UNIQUE(account_id, venue, exchange_order_id),
 UNIQUE(account_id, venue, strategy_decision_id),
 UNIQUE(account_id, venue, local_trade_id)
);
CREATE TRIGGER bot_intent_immutable BEFORE UPDATE ON bot_order_intents
BEGIN
 SELECT RAISE(ABORT, 'immutable intent') WHERE
''' + ' OR\n'.join(' NEW.' + key + ' IS NOT OLD.' + key for key in _IMMUTABLE) + ''';
 SELECT RAISE(ABORT, 'invalid intent transition') WHERE
 NEW.revision != OLD.revision + 1 OR NOT (
  (OLD.state = 'PREPARED' AND NEW.state IN ('SUBMISSION_UNCERTAIN','CONFLICT')) OR
  (OLD.state = 'SUBMISSION_UNCERTAIN' AND NEW.state IN ('BOUND','REJECTED','CONFLICT')) OR
  (OLD.state = 'BOUND' AND NEW.state IN ('FILL_CONFIRMED','CONFLICT')));
 SELECT RAISE(ABORT, 'attempt timestamp changed') WHERE
  OLD.attempted_at_ms IS NOT NULL AND NEW.attempted_at_ms IS NOT OLD.attempted_at_ms;
 SELECT RAISE(ABORT, 'binding evidence changed') WHERE
  (OLD.exchange_order_id IS NOT NULL AND NEW.exchange_order_id IS NOT OLD.exchange_order_id)
  OR (OLD.bound_at_ms IS NOT NULL AND NEW.bound_at_ms IS NOT OLD.bound_at_ms)
  OR (OLD.binding_digest IS NOT NULL AND NEW.binding_digest IS NOT OLD.binding_digest);
 SELECT RAISE(ABORT, 'fill evidence changed') WHERE
  (OLD.fill_digest IS NOT NULL AND NEW.fill_digest IS NOT OLD.fill_digest)
  OR (OLD.fill_timestamp_ms IS NOT NULL AND NEW.fill_timestamp_ms IS NOT OLD.fill_timestamp_ms)
  OR (OLD.average_fill_price IS NOT NULL AND NEW.average_fill_price IS NOT OLD.average_fill_price)
  OR (OLD.filled_quantity IS NOT NULL AND NEW.filled_quantity IS NOT OLD.filled_quantity);
END;
CREATE TRIGGER bot_intent_no_delete BEFORE DELETE ON bot_order_intents
BEGIN SELECT RAISE(ABORT, 'intent deletion forbidden'); END;
'''


def _text(value: Any, pattern: str, name: str) -> str:
    if type(value) is not str or re.fullmatch(pattern, value, flags=re.ASCII) is None:
        raise IntentError('invalid ' + name)
    return value


def _integer(value: Any, name: str) -> int:
    if type(value) is not int or value <= 0 or value > 9_000_000_000_000_000:
        raise IntentError('invalid ' + name)
    return value


def _decimal(value: Any, name: str) -> str:
    if type(value) not in (str, int, Decimal):
        raise IntentError('invalid ' + name)
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise IntentError('invalid ' + name) from exc
    if (not number.is_finite() or number <= 0 or len(number.as_tuple().digits) > 100
            or abs(number.as_tuple().exponent) > 100):
        raise IntentError('invalid ' + name)
    value = format(number, 'f')
    return value.rstrip('0').rstrip('.') if '.' in value else value


def _digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=True).encode('ascii')).hexdigest()


def _identity(values: dict[str, Any]) -> dict[str, Any]:
    return {key: (_decimal(values[key], key) if key == 'requested_quantity' else
                  _integer(values[key], key) if key == 'local_trade_id' else
                  _text(values[key], r'[A-Z0-9]+_[A-Z0-9]+', key) if key == 'native_symbol' else
                  _text(values[key], r'LONG|SHORT', key) if key == 'opening_side' else
                  _text(values[key], r'MEXC_FUTURES', key) if key == 'venue' else
                  _text(values[key], r'[A-Za-z0-9_-]{1,128}', key) if key != 'external_oid' else
                  _text(values[key], r'[A-Za-z0-9_-]{1,32}', key))
            for key in _IDENTITY}


def _binding_evidence(values: Any, intent: dict[str, Any], status: str) -> dict[str, Any]:
    expected = set(_IDENTITY) | {'exchange_order_id', 'exchange_timestamp_ms', 'status'}
    if status == 'FILLED':
        expected |= {'cumulative_filled_quantity', 'average_fill_price',
                     'exchange_fill_timestamp_ms'}
    if type(values) is not dict or set(values) != expected or values.get('status') != status:
        raise IntentError('incomplete exchange evidence')
    identity = _identity(values)
    if any(identity[key] != intent[key] for key in _IDENTITY):
        raise IntentError('mismatched exchange identity')
    order_id = _text(values['exchange_order_id'], r'[0-9]{1,30}', 'exchange order ID')
    timestamp = _integer(values['exchange_timestamp_ms'], 'exchange timestamp')
    if timestamp < intent['attempted_at_ms']:
        raise IntentError('stale exchange timestamp')
    result = identity | {'exchange_order_id': order_id, 'exchange_timestamp_ms': timestamp,
                         'status': status}
    if status == 'FILLED':
        if timestamp != intent['bound_at_ms']:
            raise IntentError('conflicting bound timestamp')
        quantity = _decimal(values['cumulative_filled_quantity'], 'filled quantity')
        if quantity != intent['requested_quantity']:
            raise IntentError('incomplete fill')
        result.update(cumulative_filled_quantity=quantity,
                      average_fill_price=_decimal(values['average_fill_price'], 'fill price'),
                      exchange_fill_timestamp_ms=_integer(
                          values['exchange_fill_timestamp_ms'], 'fill timestamp'))
        if result['exchange_fill_timestamp_ms'] < intent['bound_at_ms']:
            raise IntentError('stale fill timestamp')
    return result


class IntentStore:
    def __init__(self, database: str | Path):
        self.database = Path(database)

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        if self.database.is_symlink() or not self.database.is_file():
            raise IntentError('intent database not initialized')
        uri = self.database.resolve().as_uri() + '?mode=' + ('rw' if write else 'ro')
        try:
            with closing(sqlite3.connect(uri, uri=True, timeout=2, isolation_level=None)) as db:
                db.row_factory = sqlite3.Row
                db.execute('PRAGMA foreign_keys=ON')
                if not write:
                    db.execute('PRAGMA query_only=ON')
                if db.execute('PRAGMA user_version').fetchone()[0] != _SCHEMA_VERSION or not db.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bot_order_intents'").fetchone():
                    raise IntentError('unsupported intent schema')
                columns = {row[1] for row in db.execute('PRAGMA table_info(bot_order_intents)')}
                triggers = {row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='bot_order_intents'")}
                if columns != set(_COLUMNS) or not {'bot_intent_immutable', 'bot_intent_no_delete'} <= triggers:
                    raise IntentError('unsupported intent schema')
                if write:
                    db.execute('BEGIN IMMEDIATE')
                yield db
                if write:
                    db.execute('COMMIT')
        except sqlite3.Error as exc:
            raise IntentError('intent database unavailable or constraint failed') from exc

    def initialize(self) -> None:
        if self.database.is_symlink():
            raise IntentError('unsafe intent database path')
        if self.database.exists():
            with self._connection():
                return
        try:
            with sqlite3.connect(self.database, timeout=2) as db:
                db.executescript(_SQL)
                db.execute('PRAGMA user_version=1')
        except sqlite3.Error as exc:
            raise IntentError('intent database initialization failed') from exc

    @staticmethod
    def _get(db: sqlite3.Connection, intent_id: str) -> dict[str, Any]:
        row = db.execute('SELECT * FROM bot_order_intents WHERE intent_id=?', (intent_id,)).fetchone()
        if row is None:
            raise IntentError('unknown intent')
        result = dict(row)
        immutable = {key: result[key] for key in _IMMUTABLE if key != 'intent_digest'}
        if (_digest(immutable) != result['intent_digest'] or
                _identity(result) != {key: result[key] for key in _IDENTITY} or
                (result['order_price'] is not None and
                 _decimal(result['order_price'], 'order price') != result['order_price'])):
            raise IntentError('corrupt intent')
        return result

    def get(self, intent_id: str) -> dict[str, Any]:
        with self._connection() as db:
            return self._get(db, _text(intent_id, r'[A-Za-z0-9_-]{1,128}', 'intent ID'))

    def prepare(self, *, intent_id: str, local_trade_id: int, account_id: str,
                venue: str, native_symbol: str, opening_side: str,
                requested_quantity: str | int | Decimal, order_type: str,
                order_price: str | int | Decimal | None, strategy_decision_id: str,
                created_at_ms: int, external_oid: str | None = None) -> dict[str, Any]:
        values = _identity(dict(intent_id=intent_id, local_trade_id=local_trade_id,
                                account_id=account_id, venue=venue,
                                external_oid=external_oid if external_oid is not None else
                                secrets.token_hex(16), native_symbol=native_symbol,
                                opening_side=opening_side, requested_quantity=requested_quantity))
        kind = _text(order_type, r'MARKET|LIMIT', 'order type')
        if (kind == 'LIMIT') != (order_price is not None):
            raise IntentError('invalid order price')
        values.update(order_type=kind,
                      order_price=_decimal(order_price, 'order price') if order_price is not None else None,
                      strategy_decision_id=_text(strategy_decision_id,
                                                 r'[A-Za-z0-9_-]{1,128}', 'decision ID'),
                      created_at_ms=_integer(created_at_ms, 'created timestamp'))
        values['intent_digest'] = _digest(values)
        with self._connection(write=True) as db:
            existing = db.execute('SELECT 1 FROM bot_order_intents WHERE intent_id=?',
                                  (intent_id,)).fetchone()
            if existing is not None:
                record = self._get(db, intent_id)
                if any(record[key] != values[key] for key in _IMMUTABLE):
                    raise IntentError('conflicting intent retry')
                return record
            keys = tuple(values) + ('state', 'revision')
            db.execute('INSERT INTO bot_order_intents (' + ','.join(keys) + ') VALUES (' +
                       ','.join('?' for _ in keys) + ')', tuple(values.values()) + ('PREPARED', 0))
            return self._get(db, intent_id)

    def _advance(self, intent_id: str, expected_revision: int, target: str,
                 evidence: dict[str, Any] | None = None,
                 attempted_at_ms: int | None = None) -> dict[str, Any]:
        if type(expected_revision) is not int or expected_revision < 0:
            raise IntentError('invalid revision')
        with self._connection(write=True) as db:
            record = self._get(db, intent_id)
            if record['revision'] != expected_revision:
                raise IntentError('stale revision')
            updates: dict[str, Any] = {}
            if target == 'SUBMISSION_UNCERTAIN':
                timestamp = _integer(attempted_at_ms, 'attempt timestamp')
                if record['state'] == target and record['attempted_at_ms'] == timestamp:
                    return record
                if record['state'] != 'PREPARED' or timestamp < record['created_at_ms']:
                    raise IntentError('invalid claim')
                updates['attempted_at_ms'] = timestamp
            elif target in ('BOUND', 'REJECTED', 'FILL_CONFIRMED'):
                if attempted_at_ms is not None or evidence is None:
                    raise IntentError('invalid transition evidence')
                status = {'BOUND': 'ACCEPTED', 'REJECTED': 'REJECTED',
                          'FILL_CONFIRMED': 'FILLED'}[target]
                normalized = _binding_evidence(evidence, record, status)
                digest = _digest(normalized)
                frozen = {'BOUND': 'binding_digest', 'REJECTED': 'binding_digest',
                          'FILL_CONFIRMED': 'fill_digest'}[target]
                if record['state'] == target:
                    if record[frozen] == digest:
                        return record
                    raise IntentError('contradictory exchange evidence')
                if ((target in ('BOUND', 'REJECTED') and record['state'] != 'SUBMISSION_UNCERTAIN')
                        or (target == 'FILL_CONFIRMED' and record['state'] != 'BOUND')):
                    raise IntentError('invalid state transition')
                if target == 'FILL_CONFIRMED':
                    if (record['exchange_order_id'] != normalized['exchange_order_id'] or
                            normalized['exchange_fill_timestamp_ms'] < record['bound_at_ms']):
                        raise IntentError('conflicting bound identity')
                    updates.update(filled_quantity=normalized['cumulative_filled_quantity'],
                                   average_fill_price=normalized['average_fill_price'],
                                   fill_timestamp_ms=normalized['exchange_fill_timestamp_ms'],
                                   fill_digest=digest)
                else:
                    updates.update(exchange_order_id=normalized['exchange_order_id'],
                                   bound_at_ms=normalized['exchange_timestamp_ms'],
                                   binding_digest=digest)
            elif target == 'CONFLICT':
                if record['state'] == 'CONFLICT':
                    return record
                if record['state'] not in ('PREPARED', 'SUBMISSION_UNCERTAIN', 'BOUND'):
                    raise IntentError('invalid conflict transition')
            else:
                raise IntentError('invalid target state')
            updates.update(state=target, revision=expected_revision + 1)
            assignment = ','.join(f'{key}=?' for key in updates)
            cursor = db.execute('UPDATE bot_order_intents SET ' + assignment +
                                ' WHERE intent_id=? AND revision=?',
                                tuple(updates.values()) + (intent_id, expected_revision))
            if cursor.rowcount != 1:
                raise IntentError('competing intent writer')
            return self._get(db, intent_id)

    def claim(self, intent_id: str, *, expected_revision: int,
              attempted_at_ms: int) -> dict[str, Any]:
        return self._advance(intent_id, expected_revision, 'SUBMISSION_UNCERTAIN',
                             attempted_at_ms=attempted_at_ms)

    def bind(self, intent_id: str, *, expected_revision: int,
             evidence: dict[str, Any]) -> dict[str, Any]:
        return self._advance(intent_id, expected_revision, 'BOUND', evidence)

    def confirm_fill(self, intent_id: str, *, expected_revision: int,
                     evidence: dict[str, Any]) -> dict[str, Any]:
        return self._advance(intent_id, expected_revision, 'FILL_CONFIRMED', evidence)

    def reject(self, intent_id: str, *, expected_revision: int,
               evidence: dict[str, Any]) -> dict[str, Any]:
        return self._advance(intent_id, expected_revision, 'REJECTED', evidence)

    def mark_conflict(self, intent_id: str, *, expected_revision: int) -> dict[str, Any]:
        return self._advance(intent_id, expected_revision, 'CONFLICT')
