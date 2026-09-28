"""Isolated durable intent bookkeeping. No exchange or trader integration."""
from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
from functools import lru_cache
from contextlib import closing, contextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator


class IntentError(ValueError):
    """An intent cannot safely be created, claimed, or advanced."""


_SCHEMA_VERSION = 3
_IDENTITY = ('intent_id', 'local_trade_id', 'account_id', 'venue', 'external_oid',
             'native_symbol', 'opening_side', 'requested_quantity')
_IMMUTABLE = _IDENTITY + ('order_type', 'order_price', 'strategy_decision_id',
                          'created_at_ms', 'intent_digest')
_COLUMNS = _IMMUTABLE + ('state', 'revision', 'attempted_at_ms', 'exchange_order_id',
                          'bound_at_ms', 'binding_digest', 'filled_quantity',
                          'average_fill_price', 'fill_timestamp_ms', 'fill_digest')


def _decimal_sql(field: str) -> str:
    """Canonical positive ASCII decimal, compared as text (never SQLite REAL)."""
    return (f"typeof({field})='text' AND length({field}) BETWEEN 1 AND 128 "
            f"AND length(replace({field},'.','')) <= 127 "
            f"AND {field} NOT GLOB '*[^0-9.]*' AND {field} GLOB '*[1-9]*' "
            f"AND (substr({field},1,1) BETWEEN '1' AND '9' OR "
            f"(substr({field},1,2)='0.' AND length({field}) > 2)) "
            f"AND (instr({field},'.')=0 OR "
            f"(length({field})-length(replace({field},'.',''))=1 "
            f"AND substr({field},-1,1) BETWEEN '1' AND '9'))")


_SQL = '''
CREATE TABLE bot_order_intents (
 intent_id TEXT PRIMARY KEY NOT NULL CHECK(length(intent_id) BETWEEN 1 AND 128 AND
   intent_id NOT GLOB '*[^A-Za-z0-9_-]*'),
 local_trade_id INTEGER NOT NULL CHECK(local_trade_id > 0),
 account_id TEXT NOT NULL CHECK(length(account_id) BETWEEN 1 AND 128 AND
   account_id NOT GLOB '*[^A-Za-z0-9_-]*'),
 venue TEXT NOT NULL CHECK(venue = 'MEXC_FUTURES'),
 external_oid TEXT NOT NULL CHECK(length(external_oid) BETWEEN 1 AND 32 AND
   external_oid NOT GLOB '*[^A-Za-z0-9_-]*'),
 native_symbol TEXT NOT NULL CHECK(length(native_symbol) BETWEEN 3 AND 50 AND
   native_symbol NOT GLOB '*[^A-Z0-9_]*' AND
   instr(native_symbol,'_') BETWEEN 2 AND length(native_symbol)-1 AND
   length(native_symbol)-length(replace(native_symbol,'_',''))=1),
 opening_side TEXT NOT NULL CHECK(opening_side IN ('LONG','SHORT')),
 requested_quantity TEXT NOT NULL CHECK(''' + _decimal_sql('requested_quantity') + '''),
 order_type TEXT NOT NULL CHECK(order_type IN ('MARKET','LIMIT')),
 order_price TEXT CHECK(order_price IS NULL OR (''' + _decimal_sql('order_price') + ''')),
 strategy_decision_id TEXT NOT NULL CHECK(length(strategy_decision_id) BETWEEN 1 AND 128 AND
   strategy_decision_id NOT GLOB '*[^A-Za-z0-9_-]*'),
 created_at_ms INTEGER NOT NULL CHECK(created_at_ms > 0),
 intent_digest TEXT NOT NULL CHECK(length(intent_digest) = 64 AND
   intent_digest NOT GLOB '*[^0-9a-f]*'),
 state TEXT NOT NULL CHECK(state IN ('PREPARED','SUBMISSION_UNCERTAIN','BOUND',
                                    'FILL_CONFIRMED','REJECTED','CONFLICT')),
 revision INTEGER NOT NULL CHECK(revision >= 0),
 attempted_at_ms INTEGER,
 exchange_order_id TEXT CHECK(exchange_order_id IS NULL OR
   (typeof(exchange_order_id)='text' AND length(exchange_order_id) BETWEEN 1 AND 30
    AND exchange_order_id NOT GLOB '*[^0-9]*')),
 bound_at_ms INTEGER,
 binding_digest TEXT CHECK(binding_digest IS NULL OR
   (length(binding_digest)=64 AND binding_digest NOT GLOB '*[^0-9a-f]*')),
 filled_quantity TEXT CHECK(filled_quantity IS NULL OR (''' + _decimal_sql('filled_quantity') + ''')),
 average_fill_price TEXT CHECK(average_fill_price IS NULL OR (''' + _decimal_sql('average_fill_price') + ''')),
 fill_timestamp_ms INTEGER,
 fill_digest TEXT CHECK(fill_digest IS NULL OR
   (length(fill_digest)=64 AND fill_digest NOT GLOB '*[^0-9a-f]*')),
 CHECK ((order_type = 'MARKET' AND order_price IS NULL) OR
        (order_type = 'LIMIT' AND order_price IS NOT NULL)),
 CHECK ((state='PREPARED' AND revision=0 AND attempted_at_ms IS NULL) OR
        (state='SUBMISSION_UNCERTAIN' AND revision=1) OR
        (state IN ('BOUND','REJECTED') AND revision=2) OR
        (state='FILL_CONFIRMED' AND revision=3) OR
        (state='CONFLICT' AND revision BETWEEN 1 AND 3)),
 CHECK (attempted_at_ms IS NULL OR
        (typeof(attempted_at_ms)='integer' AND attempted_at_ms >= created_at_ms)),
 CHECK ((state IN ('PREPARED','CONFLICT') AND revision <= 1 AND
         attempted_at_ms IS NULL) OR
        (state NOT IN ('PREPARED') AND
         NOT (state='CONFLICT' AND revision=1) AND attempted_at_ms IS NOT NULL)),
 CHECK ((state IN ('PREPARED','SUBMISSION_UNCERTAIN') OR
         (state='CONFLICT' AND revision <= 2)) AND
         exchange_order_id IS NULL AND bound_at_ms IS NULL AND binding_digest IS NULL
        OR state IN ('BOUND','FILL_CONFIRMED','REJECTED') OR
         (state='CONFLICT' AND revision=3)),
 CHECK ((state IN ('BOUND','FILL_CONFIRMED','REJECTED') OR
         (state='CONFLICT' AND revision=3)) AND
         exchange_order_id IS NOT NULL AND bound_at_ms IS NOT NULL AND
         typeof(bound_at_ms)='integer' AND bound_at_ms >= attempted_at_ms AND
         binding_digest IS NOT NULL
        OR state IN ('PREPARED','SUBMISSION_UNCERTAIN') OR
         (state='CONFLICT' AND revision <= 2)),
 CHECK ((state='FILL_CONFIRMED' AND filled_quantity IS NOT NULL AND
         filled_quantity = requested_quantity COLLATE BINARY AND
         average_fill_price IS NOT NULL AND fill_timestamp_ms IS NOT NULL AND
         typeof(fill_timestamp_ms)='integer' AND fill_timestamp_ms >= bound_at_ms AND
         fill_digest IS NOT NULL) OR
        (state!='FILL_CONFIRMED' AND filled_quantity IS NULL AND
         average_fill_price IS NULL AND fill_timestamp_ms IS NULL AND fill_digest IS NULL)),
 UNIQUE(account_id, venue, external_oid),
 UNIQUE(account_id, venue, exchange_order_id),
 UNIQUE(account_id, venue, strategy_decision_id),
 UNIQUE(account_id, venue, local_trade_id)
 ) WITHOUT ROWID;
CREATE TRIGGER bot_intent_no_reinsert BEFORE INSERT ON bot_order_intents
BEGIN
 SELECT RAISE(ABORT, 'invalid intent insert') WHERE
   bot_intent_validate('INSERT',
''' + ','.join('NULL' for _ in _COLUMNS) + ',\n' + ','.join('NEW.' + key for key in _COLUMNS) + ''') != 1;
END;
CREATE TRIGGER bot_intent_immutable BEFORE UPDATE ON bot_order_intents
BEGIN
 SELECT RAISE(ABORT, 'invalid intent update') WHERE
   bot_intent_validate('UPDATE',
''' + ','.join('OLD.' + key for key in _COLUMNS) + ',\n' + ','.join('NEW.' + key for key in _COLUMNS) + ''') != 1;
END;
CREATE TRIGGER bot_intent_no_delete BEFORE DELETE ON bot_order_intents
BEGIN SELECT RAISE(ABORT, 'intent deletion forbidden'); END;
'''


def _text(value: Any, pattern: str, name: str) -> str:
    if (type(value) is not str or len(value) > 128 or '\x00' in value or
            re.fullmatch(pattern, value, flags=re.ASCII) is None):
        raise IntentError('invalid ' + name)
    return value


def _integer(value: Any, name: str) -> int:
    if type(value) is not int or value <= 0 or value > 9_000_000_000_000_000:
        raise IntentError('invalid ' + name)
    return value


def _decimal(value: Any, name: str) -> str:
    if type(value) not in (str, int, Decimal):
        raise IntentError('invalid ' + name)
    if type(value) is str and (len(value) > 128 or '\x00' in value):
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


_SCHEMA_QUERY = 'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'


def _schema_contract(db: sqlite3.Connection) -> tuple[Any, ...]:
    objects = tuple(tuple(row) for row in db.execute(_SCHEMA_QUERY))
    columns = tuple(tuple(row) for row in db.execute('PRAGMA table_info(bot_order_intents)'))
    indexes = tuple(tuple(row) for row in db.execute('PRAGMA index_list(bot_order_intents)'))
    index_detail = tuple((row[1], tuple(tuple(part) for part in db.execute(
        f'PRAGMA index_xinfo("{row[1]}")'))) for row in indexes)
    return objects, columns, indexes, index_detail


@lru_cache(maxsize=1)
def _expected_schema() -> tuple[Any, ...]:
    with closing(sqlite3.connect(':memory:')) as reference:
        IntentStore._register_validator(reference)
        reference.executescript(_SQL)
        return _schema_contract(reference)


def _identity(values: dict[str, Any]) -> dict[str, Any]:
    return {key: (_decimal(values[key], key) if key == 'requested_quantity' else
                  _integer(values[key], key) if key == 'local_trade_id' else
                  _text(values[key], r'[A-Z0-9]{1,49}_[A-Z0-9]{1,49}', key) if key == 'native_symbol' else
                  _text(values[key], r'LONG|SHORT', key) if key == 'opening_side' else
                  _text(values[key], r'MEXC_FUTURES', key) if key == 'venue' else
                  _text(values[key], r'[A-Za-z0-9_-]{1,128}', key) if key != 'external_oid' else
                  _text(values[key], r'[A-Za-z0-9_-]{1,32}', key))
            for key in _IDENTITY}


def _validate_row(result: dict[str, Any]) -> dict[str, Any]:
    if set(result) != set(_COLUMNS):
        raise IntentError('incomplete intent row')
    if type(result['native_symbol']) is not str or len(result['native_symbol']) > 50:
        raise IntentError('invalid native symbol')
    immutable = {key: result[key] for key in _IMMUTABLE if key != 'intent_digest'}
    if (_digest(immutable) != result['intent_digest'] or
            _identity(result) != {key: result[key] for key in _IDENTITY} or
            _text(result['order_type'], r'MARKET|LIMIT', 'order type') != result['order_type'] or
            _text(result['strategy_decision_id'], r'[A-Za-z0-9_-]{1,128}',
                  'decision ID') != result['strategy_decision_id'] or
            _integer(result['created_at_ms'], 'created timestamp') != result['created_at_ms'] or
            (result['order_price'] is None) != (result['order_type'] == 'MARKET') or
            (result['order_price'] is not None and
             _decimal(result['order_price'], 'order price') != result['order_price'])):
        raise IntentError('corrupt intent')
    state, revision, attempted = (result[key] for key in ('state', 'revision', 'attempted_at_ms'))
    if (type(revision) is not int or state not in (
            'PREPARED', 'SUBMISSION_UNCERTAIN', 'BOUND', 'FILL_CONFIRMED',
            'REJECTED', 'CONFLICT') or
            (state == 'PREPARED' and (revision != 0 or attempted is not None)) or
            (state == 'SUBMISSION_UNCERTAIN' and revision != 1) or
            (state in ('BOUND', 'REJECTED') and revision != 2) or
            (state == 'FILL_CONFIRMED' and revision != 3) or
            (state == 'CONFLICT' and revision not in (1, 2, 3)) or
            (attempted is not None and (_integer(attempted, 'attempt timestamp') <
                                         result['created_at_ms'])) or
            (state == 'CONFLICT' and revision == 1 and attempted is not None) or
            (attempted is None and not (state == 'PREPARED' or
                                        (state == 'CONFLICT' and revision == 1)))):
        raise IntentError('corrupt intent state')
    has_binding = state in ('BOUND', 'FILL_CONFIRMED', 'REJECTED') or (
        state == 'CONFLICT' and revision == 3)
    bound_fields = ('exchange_order_id', 'bound_at_ms', 'binding_digest')
    if has_binding:
        if (any(result[key] is None for key in bound_fields) or
                _integer(result['bound_at_ms'], 'bound timestamp') < attempted or
                _text(result['exchange_order_id'], r'[0-9]{1,30}', 'exchange order ID') !=
                result['exchange_order_id']):
            raise IntentError('corrupt binding')
        expected = _identity(result) | dict(exchange_order_id=result['exchange_order_id'],
            exchange_timestamp_ms=result['bound_at_ms'],
            status='REJECTED' if state == 'REJECTED' else 'ACCEPTED')
        if _digest(expected) != result['binding_digest']:
            raise IntentError('corrupt binding evidence')
    elif any(result[key] is not None for key in bound_fields):
        raise IntentError('unexpected binding')
    fill_fields = ('filled_quantity', 'average_fill_price', 'fill_timestamp_ms', 'fill_digest')
    if state == 'FILL_CONFIRMED':
        if (any(result[key] is None for key in fill_fields) or
                _decimal(result['filled_quantity'], 'filled quantity') !=
                result['requested_quantity'] or
                result['filled_quantity'] != result['requested_quantity'] or
                _decimal(result['average_fill_price'], 'fill price') !=
                result['average_fill_price'] or
                _integer(result['fill_timestamp_ms'], 'fill timestamp') <
                result['bound_at_ms'] or
                _digest(_identity(result) | dict(
                    exchange_order_id=result['exchange_order_id'],
                    exchange_timestamp_ms=result['bound_at_ms'], status='FILLED',
                    cumulative_filled_quantity=result['filled_quantity'],
                    average_fill_price=result['average_fill_price'],
                    exchange_fill_timestamp_ms=result['fill_timestamp_ms'])) !=
                result['fill_digest']):
            raise IntentError('corrupt fill evidence')
    elif any(result[key] is not None for key in fill_fields):
        raise IntentError('unexpected fill')
    return result


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

    @staticmethod
    def _register_validator(db: sqlite3.Connection) -> None:
        def validate(operation: Any, *fields: Any) -> int:
            try:
                width = len(_COLUMNS)
                if len(fields) != 2 * width:
                    return 0
                old = dict(zip(_COLUMNS, fields[:width]))
                new = dict(zip(_COLUMNS, fields[width:]))
                if operation == 'INSERT':
                    if any(value is not None for value in old.values()):
                        return 0
                    if new['state'] != 'PREPARED':
                        return 0
                elif operation == 'UPDATE':
                    _validate_row(old)
                    if any(old[key] != new[key] for key in _IMMUTABLE):
                        return 0
                    if (type(old['revision']) is not int or
                            type(new['revision']) is not int or
                            new['revision'] != old['revision'] + 1 or
                            new['state'] not in {
                                'PREPARED': ('SUBMISSION_UNCERTAIN', 'CONFLICT'),
                                'SUBMISSION_UNCERTAIN': ('BOUND', 'REJECTED', 'CONFLICT'),
                                'BOUND': ('FILL_CONFIRMED', 'CONFLICT'),
                            }.get(old['state'], ())):
                        return 0
                    for key in ('attempted_at_ms', 'exchange_order_id', 'bound_at_ms',
                                'binding_digest', 'filled_quantity', 'average_fill_price',
                                'fill_timestamp_ms', 'fill_digest'):
                        if old[key] is not None and old[key] != new[key]:
                            return 0
                else:
                    return 0
                _validate_row(new)
                owner = db.execute('''SELECT intent_id FROM bot_order_intents WHERE
                    intent_id=? OR (account_id=? AND venue=? AND (
                    external_oid=? OR strategy_decision_id=? OR local_trade_id=? OR
                    (? IS NOT NULL AND exchange_order_id=?))) LIMIT 2''',
                    (new['intent_id'], new['account_id'], new['venue'],
                     new['external_oid'], new['strategy_decision_id'], new['local_trade_id'],
                     new['exchange_order_id'], new['exchange_order_id'])).fetchall()
                if any(operation == 'INSERT' or row[0] != old['intent_id'] for row in owner):
                    return 0
                return 1
            except (IntentError, sqlite3.Error, ValueError, TypeError, OverflowError):
                return 0
        db.create_function('bot_intent_validate', 1 + 2 * len(_COLUMNS), validate)

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        if self.database.is_symlink() or not self.database.is_file():
            raise IntentError('intent database not initialized')
        uri = self.database.resolve().as_uri() + '?mode=' + ('rw' if write else 'ro')
        try:
            with closing(sqlite3.connect(uri, uri=True, timeout=2, isolation_level=None)) as db:
                self._register_validator(db)
                db.row_factory = sqlite3.Row
                db.execute('PRAGMA foreign_keys=ON')
                if not write:
                    db.execute('PRAGMA query_only=ON')
                if (db.execute('PRAGMA user_version').fetchone()[0] != _SCHEMA_VERSION or
                        _schema_contract(db) != _expected_schema()):
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
                self._register_validator(db)
                db.executescript(_SQL)
                db.execute(f'PRAGMA user_version={_SCHEMA_VERSION}')
        except sqlite3.Error as exc:
            raise IntentError('intent database initialization failed') from exc

    @staticmethod
    def _get(db: sqlite3.Connection, intent_id: str) -> dict[str, Any]:
        row = db.execute('SELECT * FROM bot_order_intents WHERE intent_id=?', (intent_id,)).fetchone()
        if row is None:
            raise IntentError('unknown intent')
        result = dict(row)
        return _validate_row(result)

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
                                'pending-generated-oid', native_symbol=native_symbol,
                                opening_side=opening_side, requested_quantity=requested_quantity))
        kind = _text(order_type, r'MARKET|LIMIT', 'order type')
        if (kind == 'LIMIT') != (order_price is not None):
            raise IntentError('invalid order price')
        values.update(order_type=kind,
                      order_price=_decimal(order_price, 'order price') if order_price is not None else None,
                      strategy_decision_id=_text(strategy_decision_id,
                                                 r'[A-Za-z0-9_-]{1,128}', 'decision ID'),
                      created_at_ms=_integer(created_at_ms, 'created timestamp'))
        with self._connection(write=True) as db:
            existing = db.execute('SELECT 1 FROM bot_order_intents WHERE intent_id=?',
                                  (intent_id,)).fetchone()
            if existing is not None:
                record = self._get(db, intent_id)
                if any(record[key] != values[key] for key in _IMMUTABLE
                       if key != 'intent_digest' and (key != 'external_oid' or external_oid is not None)):
                    raise IntentError('conflicting intent retry')
                return record
            if external_oid is None:
                values['external_oid'] = secrets.token_hex(16)
            values['intent_digest'] = _digest(values)
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
            if target == 'SUBMISSION_UNCERTAIN' and record['state'] == target:
                timestamp = _integer(attempted_at_ms, 'attempt timestamp')
                if (record['attempted_at_ms'] == timestamp and
                        expected_revision in (record['revision'], record['revision'] - 1)):
                    return record | {'claim_outcome': 'ALREADY_CLAIMED'}
                raise IntentError('conflicting claim retry')
            if record['revision'] != expected_revision:
                raise IntentError('stale revision')
            updates: dict[str, Any] = {}
            if target == 'SUBMISSION_UNCERTAIN':
                timestamp = _integer(attempted_at_ms, 'attempt timestamp')
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
            result = self._get(db, intent_id)
            return (result | {'claim_outcome': 'CLAIM_ACQUIRED'}
                    if target == 'SUBMISSION_UNCERTAIN' else result)

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
