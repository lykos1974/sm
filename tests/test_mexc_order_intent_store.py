"""Synthetic, offline durability tests for the separate order-intent database."""
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import mexc_order_intent_store as intent_module
from mexc_order_intent_store import IntentStore, IntentError


class IntentStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'intents.sqlite'
        self.store = IntentStore(self.path)
        self.store.initialize()
        self.fields = dict(intent_id='intent-1', local_trade_id=17, account_id='account-a',
                           venue='MEXC_FUTURES', external_oid='synthetic-oid-1',
                           native_symbol='SUI_USDT', opening_side='LONG',
                           requested_quantity='44.000', order_type='LIMIT',
                           order_price='1.0150', strategy_decision_id='decision-1',
                           created_at_ms=1000)

    def prepare(self, **changes):
        return self.store.prepare(**(self.fields | changes))

    def bound(self):
        self.prepare()
        self.store.claim('intent-1', expected_revision=0, attempted_at_ms=1001)
        return dict(intent_id='intent-1', local_trade_id=17, account_id='account-a',
                    venue='MEXC_FUTURES', external_oid='synthetic-oid-1',
                    native_symbol='SUI_USDT', opening_side='LONG',
                    requested_quantity='44', exchange_order_id='123456',
                    exchange_timestamp_ms=1002, status='ACCEPTED')

    def test_decimal_canonicalization_and_exact_retry(self):
        first = self.prepare()
        self.assertEqual((first['requested_quantity'], first['order_price'], first['revision']),
                         ('44', '1.015', 0))
        self.assertEqual(self.prepare(requested_quantity=Decimal('44.00'),
                                      order_price=Decimal('1.015')), first)
        with self.assertRaises(IntentError):
            self.prepare(requested_quantity='45')
        for value in (True, 1.5, 'NaN', 'Infinity', '-1', '0', '1e500'):
            with self.subTest(value=value), self.assertRaises(IntentError):
                self.prepare(intent_id='other', strategy_decision_id='other',
                             external_oid='other', requested_quantity=value)

    def test_duplicate_per_account_and_schema_guards(self):
        self.prepare()
        for change in (dict(intent_id='other', strategy_decision_id='other'),
                       dict(intent_id='other', external_oid='other'),
                       dict(intent_id='other', external_oid='other',
                            strategy_decision_id='other')):
            with self.subTest(change=change):
                if len(change) == 3:
                    self.assertEqual(self.prepare(account_id='account-b', **change)['account_id'],
                                     'account-b')
                else:
                    with self.assertRaises(IntentError):
                        self.prepare(**change)
        with sqlite3.connect(self.path) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('UPDATE bot_order_intents SET requested_quantity=? WHERE intent_id=?',
                           ('45', 'intent-1'))
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('UPDATE bot_order_intents SET state=? WHERE intent_id=?',
                           ('PREPARED_AGAIN', 'intent-1'))

    def test_claim_is_durable_and_competing_workers_cannot_resubmit(self):
        self.prepare()
        second = IntentStore(self.path)
        claimed = self.store.claim('intent-1', expected_revision=0, attempted_at_ms=1001)
        self.assertEqual((claimed['state'], claimed['revision'], claimed['attempted_at_ms']),
                         ('SUBMISSION_UNCERTAIN', 1, 1001))
        self.assertEqual(claimed['claim_outcome'], 'CLAIM_ACQUIRED')
        self.assertEqual(second.get('intent-1'),
                         {k: v for k, v in claimed.items() if k != 'claim_outcome'})
        for revision in (0, 1):
            self.assertEqual(second.claim('intent-1', expected_revision=revision,
                                          attempted_at_ms=1001)['claim_outcome'],
                             'ALREADY_CLAIMED')
        for revision, timestamp in ((0, 1002), (1, 1002)):
            with self.assertRaises(IntentError):
                second.claim('intent-1', expected_revision=revision,
                             attempted_at_ms=timestamp)
        self.assertEqual(IntentStore(self.path).get('intent-1'),
                         {k: v for k, v in claimed.items() if k != 'claim_outcome'})

    def test_bind_and_separate_confirm_frozen_after_restart(self):
        evidence = self.bound()
        bound = self.store.bind('intent-1', expected_revision=1, evidence=evidence)
        self.assertEqual((bound['state'], bound['revision']), ('BOUND', 2))
        self.assertEqual(IntentStore(self.path).bind('intent-1', expected_revision=2,
                                                    evidence=dict(evidence)), bound)
        with self.assertRaises(IntentError):
            self.store.bind('intent-1', expected_revision=2,
                            evidence=evidence | {'exchange_order_id': 'other'})
        fill = evidence | dict(status='FILLED', cumulative_filled_quantity='44.0',
                               average_fill_price='1.0250', exchange_fill_timestamp_ms=1003)
        final = IntentStore(self.path).confirm_fill('intent-1', expected_revision=2,
                                                     evidence=fill)
        self.assertEqual((final['state'], final['average_fill_price'], final['revision']),
                         ('FILL_CONFIRMED', '1.025', 3))
        self.assertEqual(self.store.confirm_fill('intent-1', expected_revision=3,
                                                 evidence=dict(fill)), final)
        for change in (dict(average_fill_price='2'), dict(exchange_fill_timestamp_ms=1004),
                       dict(cumulative_filled_quantity='43'),
                       dict(exchange_timestamp_ms=1004)):
            with self.assertRaises(IntentError):
                self.store.confirm_fill('intent-1', expected_revision=3,
                                        evidence=fill | change)
        self.assertEqual(IntentStore(self.path).get('intent-1'), final)

    def test_mismatched_missing_legacy_and_invalid_transitions(self):
        evidence = self.bound()
        for change in (dict(external_oid='manual'), dict(account_id='other'),
                       dict(opening_side='SHORT'), dict(requested_quantity='45'),
                       dict(status='UNKNOWN'), dict(exchange_timestamp_ms=999),
                       dict(exchange_order_id=None)):
            with self.subTest(change=change), self.assertRaises(IntentError):
                self.store.bind('intent-1', expected_revision=1, evidence=evidence | change)
        with self.assertRaises(IntentError):
            self.store.bind('intent-1', expected_revision=1,
                            evidence={k: v for k, v in evidence.items() if k != 'external_oid'})
        with self.assertRaises(IntentError):
            self.store.confirm_fill('intent-1', expected_revision=1, evidence=evidence)
        self.assertEqual(self.store.get('intent-1')['state'], 'SUBMISSION_UNCERTAIN')
        with sqlite3.connect(self.path) as db:
            db.execute('CREATE TABLE legacy_orders (id INTEGER)')
        with self.assertRaises(IntentError):
            IntentStore(self.path.with_name('missing.sqlite')).get('intent-1')

    def test_rejected_and_conflict_are_terminal_and_atomic(self):
        evidence = self.bound()
        rejected = self.store.reject('intent-1', expected_revision=1,
                                     evidence=evidence | {'status': 'REJECTED'})
        self.assertEqual(rejected['state'], 'REJECTED')
        with self.assertRaises(IntentError):
            self.store.bind('intent-1', expected_revision=2, evidence=evidence)
        with self.assertRaises(IntentError):
            self.store.mark_conflict('intent-1', expected_revision=2)
        self.prepare(intent_id='intent-2', local_trade_id=18,
                     strategy_decision_id='decision-2',
                     external_oid='synthetic-oid-2')
        conflicted = self.store.mark_conflict('intent-2', expected_revision=0)
        self.assertEqual(self.store.mark_conflict('intent-2', expected_revision=1), conflicted)
        with self.assertRaises(IntentError):
            self.store.claim('intent-2', expected_revision=1, attempted_at_ms=1001)

    def test_exchange_order_id_unique_per_account_and_no_partial_mutation(self):
        evidence = self.bound()
        self.store.bind('intent-1', expected_revision=1, evidence=evidence)
        other = self.prepare(intent_id='intent-2', local_trade_id=18,
                             external_oid='synthetic-oid-2', strategy_decision_id='decision-2')
        self.store.claim('intent-2', expected_revision=0, attempted_at_ms=1001)
        with self.assertRaises(IntentError):
            self.store.bind('intent-2', expected_revision=1,
                            evidence=evidence | dict(intent_id='intent-2', local_trade_id=18,
                                                    external_oid='synthetic-oid-2'))
        self.assertEqual(self.store.get('intent-2')['state'], 'SUBMISSION_UNCERTAIN')
        self.assertEqual(other['revision'], 0)

    def test_competing_workers_only_one_claim_and_restarts_at_every_commit(self):
        initial = self.prepare()
        self.assertEqual(IntentStore(self.path).get('intent-1'), initial)
        def claim(_):
            try:
                return IntentStore(self.path).claim('intent-1', expected_revision=0,
                                                    attempted_at_ms=1001)['claim_outcome']
            except IntentError:
                return 'BLOCKED'
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(claim, (0, 1)))
        self.assertEqual(sorted(outcomes), ['ALREADY_CLAIMED', 'CLAIM_ACQUIRED'])
        claimed = IntentStore(self.path).get('intent-1')
        self.assertEqual((claimed['revision'], claimed['attempted_at_ms']), (1, 1001))
        evidence = dict(intent_id='intent-1', local_trade_id=17, account_id='account-a',
                        venue='MEXC_FUTURES', external_oid='synthetic-oid-1',
                        native_symbol='SUI_USDT', opening_side='LONG',
                        requested_quantity='44', exchange_order_id='123456',
                        exchange_timestamp_ms=1002, status='ACCEPTED')
        bound = self.store.bind('intent-1', expected_revision=1, evidence=evidence)
        self.assertEqual(IntentStore(self.path).get('intent-1'), bound)
        filled = self.store.confirm_fill('intent-1', expected_revision=2,
                                         evidence=evidence | dict(status='FILLED',
                                             cumulative_filled_quantity='44',
                                             average_fill_price='1.025',
                                             exchange_fill_timestamp_ms=1003))
        self.assertEqual(IntentStore(self.path).get('intent-1'), filled)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM bot_order_intents WHERE intent_id=?', ('intent-1',))
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('UPDATE bot_order_intents SET average_fill_price=? WHERE intent_id=?',
                           ('2', 'intent-1'))

    def test_absent_legacy_database_never_migrates_or_creates_rows(self):
        missing = self.path.with_name('missing.sqlite')
        with self.assertRaises(IntentError):
            IntentStore(missing).prepare(**self.fields)
        self.assertFalse(missing.exists())
        legacy = self.path.with_name('legacy.sqlite')
        with sqlite3.connect(legacy) as db:
            db.execute('CREATE TABLE old_orders (id INTEGER PRIMARY KEY)')
        original = legacy.read_bytes()
        with self.assertRaises(IntentError):
            IntentStore(legacy).initialize()
        self.assertEqual(legacy.read_bytes(), original)
        self.prepare()
        with sqlite3.connect(self.path) as db:
            db.execute('DROP TRIGGER bot_intent_no_reinsert')
        older_schema = self.path.read_bytes()
        with self.assertRaises(IntentError):
            IntentStore(self.path).initialize()
        with self.assertRaises(IntentError):
            IntentStore(self.path).claim('intent-1', expected_revision=0,
                                         attempted_at_ms=1001)
        self.assertEqual(self.path.read_bytes(), older_schema)

    def test_raw_replace_cannot_restore_prepared_or_allow_second_claim(self):
        self.prepare()
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            saved = dict(db.execute('SELECT * FROM bot_order_intents').fetchone())
        self.store.claim('intent-1', expected_revision=0, attempted_at_ms=1001)
        before = self.path.read_bytes()
        names = ','.join(saved)
        slots = ','.join('?' for _ in saved)
        with sqlite3.connect(self.path) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute(f'INSERT OR REPLACE INTO bot_order_intents ({names}) '
                           f'VALUES ({slots})', tuple(saved.values()))
            self.assertEqual(db.execute('SELECT state, revision FROM bot_order_intents').fetchone(),
                             ('SUBMISSION_UNCERTAIN', 1))
        self.assertEqual(self.path.read_bytes(), before)
        with self.assertRaises(IntentError):
            IntentStore(self.path).claim('intent-1', expected_revision=0,
                                         attempted_at_ms=1002)

    def test_raw_insert_guards_all_states_and_ownership_keys(self):
        for state in ('PREPARED', 'SUBMISSION_UNCERTAIN', 'BOUND',
                      'FILL_CONFIRMED', 'REJECTED', 'CONFLICT'):
            with self.subTest(state=state):
                path = Path(self.tmp.name) / f'{state}.sqlite'
                store = IntentStore(path)
                store.initialize()
                store.prepare(**self.fields)
                with sqlite3.connect(path) as db:
                    db.row_factory = sqlite3.Row
                    saved = dict(db.execute('SELECT * FROM bot_order_intents').fetchone())
                if state != 'PREPARED':
                    store.claim('intent-1', expected_revision=0, attempted_at_ms=1001)
                identity = dict(intent_id='intent-1', local_trade_id=17,
                                account_id='account-a', venue='MEXC_FUTURES',
                                external_oid='synthetic-oid-1', native_symbol='SUI_USDT',
                                opening_side='LONG', requested_quantity='44',
                                exchange_order_id='123456', exchange_timestamp_ms=1002,
                                status='ACCEPTED')
                if state in ('BOUND', 'FILL_CONFIRMED'):
                    store.bind('intent-1', expected_revision=1, evidence=identity)
                if state == 'FILL_CONFIRMED':
                    store.confirm_fill('intent-1', expected_revision=2,
                                       evidence=identity | dict(status='FILLED',
                                           cumulative_filled_quantity='44',
                                           average_fill_price='1.025',
                                           exchange_fill_timestamp_ms=1003))
                if state == 'REJECTED':
                    store.reject('intent-1', expected_revision=1,
                                 evidence=identity | {'status': 'REJECTED'})
                if state == 'CONFLICT':
                    store.mark_conflict('intent-1', expected_revision=1)
                baseline = store.get('intent-1')
                before = path.read_bytes()
                with sqlite3.connect(path) as db:
                    with self.assertRaises(sqlite3.IntegrityError):
                        db.execute('DELETE FROM bot_order_intents WHERE intent_id=?',
                                   ('intent-1',))
                self.assertEqual(path.read_bytes(), before)
                names = ','.join(saved)
                slots = ','.join('?' for _ in saved)
                variants = [dict(saved),
                            dict(saved, intent_id='another', local_trade_id=18,
                                 strategy_decision_id='another'),
                            dict(saved, intent_id='another', local_trade_id=18,
                                 external_oid='another'),
                            dict(saved, intent_id='another', local_trade_id=18,
                                 external_oid='another', strategy_decision_id='another')]
                if baseline['exchange_order_id'] is not None:
                    variants[-1]['exchange_order_id'] = baseline['exchange_order_id']
                else:
                    variants.pop()
                for verb in ('INSERT OR REPLACE', 'REPLACE', 'INSERT'):
                    for index, values in enumerate(variants):
                        sql = f'{verb} INTO bot_order_intents ({names}) VALUES ({slots})'
                        if verb == 'INSERT':
                            key = ('intent_id', 'account_id,venue,external_oid',
                                   'account_id,venue,strategy_decision_id',
                                   'account_id,venue,exchange_order_id')[index]
                            sql += (f' ON CONFLICT({key}) DO UPDATE SET '
                                    'state=excluded.state,revision=excluded.revision')
                        with sqlite3.connect(path) as db:
                            with self.assertRaises(sqlite3.IntegrityError):
                                db.execute(sql, tuple(values.values()))
                        self.assertEqual(store.get('intent-1'), baseline)
                        self.assertEqual(path.read_bytes(), before)
                if state != 'PREPARED':
                    with self.assertRaises(IntentError):
                        IntentStore(path).claim('intent-1', expected_revision=0,
                                                attempted_at_ms=1004)

    def test_update_or_replace_cannot_delete_bound_owner(self):
        binding = self.bound()
        self.store.bind('intent-1', expected_revision=1, evidence=binding)
        self.prepare(intent_id='intent-2', local_trade_id=18,
                     strategy_decision_id='decision-2', external_oid='synthetic-oid-2')
        self.store.claim('intent-2', expected_revision=0, attempted_at_ms=1001)
        before = self.path.read_bytes()
        with sqlite3.connect(self.path) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('''UPDATE OR REPLACE bot_order_intents SET state='BOUND',
                    revision=2, exchange_order_id='123456', bound_at_ms=1002,
                    binding_digest=? WHERE intent_id='intent-2' ''', ('a' * 64,))
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.store.get('intent-1')['state'], 'BOUND')
        self.assertEqual(self.store.get('intent-2')['state'], 'SUBMISSION_UNCERTAIN')

    def test_rowid_replace_cannot_delete_existing_owner(self):
        self.prepare()
        self.prepare(intent_id='intent-2', local_trade_id=18,
                     external_oid='synthetic-oid-2', strategy_decision_id='decision-2')
        with sqlite3.connect(self.path) as db:
            owner_rowid = db.execute("SELECT rowid FROM bot_order_intents WHERE intent_id='intent-1'").fetchone()[0]
            competitor_rowid = db.execute("SELECT rowid FROM bot_order_intents WHERE intent_id='intent-2'").fetchone()[0]
            columns = [row[1] for row in db.execute('PRAGMA table_info(bot_order_intents)')]
            competitor = dict(zip(columns, db.execute('SELECT ' + ','.join(columns) +
                                    ' FROM bot_order_intents WHERE intent_id=?', ('intent-2',)).fetchone()))
            third = competitor | dict(intent_id='intent-3', local_trade_id=19,
                                      external_oid='synthetic-oid-3',
                                      strategy_decision_id='decision-3')
        before = self.path.read_bytes()
        with sqlite3.connect(self.path) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE OR REPLACE bot_order_intents SET rowid=? WHERE intent_id='intent-2'",
                           (owner_rowid,))
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('INSERT OR REPLACE INTO bot_order_intents (rowid,' +
                           ','.join(columns) + ') VALUES (' +
                           ','.join('?' for _ in range(len(columns)+1)) + ')',
                           (owner_rowid, *third.values()))
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.store.get('intent-1')['state'], 'PREPARED')
        self.assertEqual(self.store.get('intent-2')['state'], 'PREPARED')
        self.assertNotEqual(owner_rowid, competitor_rowid)

    def test_update_ownership_attack_matrix_all_states_and_keys(self):
        states = ('PREPARED', 'SUBMISSION_UNCERTAIN', 'BOUND',
                  'FILL_CONFIRMED', 'REJECTED', 'CONFLICT')
        for state in states:
            with self.subTest(state=state):
                path = Path(self.tmp.name) / ('update-' + state + '.sqlite')
                store = IntentStore(path)
                store.initialize()
                store.prepare(**self.fields)
                store.prepare(**(self.fields | dict(intent_id='intent-2', local_trade_id=18,
                                                    external_oid='synthetic-oid-2',
                                                    strategy_decision_id='decision-2')))
                identity = dict(intent_id='intent-1', local_trade_id=17,
                                account_id='account-a', venue='MEXC_FUTURES',
                                external_oid='synthetic-oid-1', native_symbol='SUI_USDT',
                                opening_side='LONG', requested_quantity='44',
                                exchange_order_id='123456', exchange_timestamp_ms=1002,
                                status='ACCEPTED')
                if state != 'PREPARED':
                    store.claim('intent-1', expected_revision=0, attempted_at_ms=1001)
                if state in ('BOUND', 'FILL_CONFIRMED'):
                    store.bind('intent-1', expected_revision=1, evidence=identity)
                if state == 'FILL_CONFIRMED':
                    store.confirm_fill('intent-1', expected_revision=2,
                                       evidence=identity | dict(status='FILLED',
                                         cumulative_filled_quantity='44',
                                         average_fill_price='1.025',
                                         exchange_fill_timestamp_ms=1003))
                if state == 'REJECTED':
                    store.reject('intent-1', expected_revision=1,
                                 evidence=identity | {'status': 'REJECTED'})
                if state == 'CONFLICT':
                    store.mark_conflict('intent-1', expected_revision=1)
                for target in ('intent_id', 'external_oid', 'strategy_decision_id',
                               'local_trade_id', 'exchange_order_id'):
                    if target == 'exchange_order_id' and state not in (
                            'BOUND', 'FILL_CONFIRMED', 'REJECTED'):
                        continue
                    for verb in ('UPDATE', 'UPDATE OR REPLACE'):
                        with self.subTest(target=target, verb=verb):
                            before = path.read_bytes()
                            with sqlite3.connect(path) as db:
                                rows_before = db.execute(
                                    'SELECT rowid,* FROM bot_order_intents ORDER BY intent_id').fetchall()
                            values = tuple(dict(intent_id='intent-1',
                                external_oid='synthetic-oid-1',
                                strategy_decision_id='decision-1', local_trade_id=17,
                                exchange_order_id='123456')[target] for _ in (0,))
                            assignment = f'{target}=?'
                            if target == 'exchange_order_id':
                                assignment += ",state='BOUND',revision=2,bound_at_ms=1002,binding_digest='" + 'a'*64 + "',attempted_at_ms=1001"
                            with sqlite3.connect(path) as db:
                                with self.assertRaises(sqlite3.IntegrityError):
                                    db.execute(f'{verb} bot_order_intents SET {assignment} '
                                               "WHERE intent_id='intent-2'", values)
                            self.assertEqual(path.read_bytes(), before)
                            with sqlite3.connect(path) as db:
                                self.assertEqual(db.execute(
                                    'SELECT rowid,* FROM bot_order_intents ORDER BY intent_id').fetchall(),
                                    rows_before)
                            self.assertEqual(store.get('intent-1')['state'], state)
                            self.assertEqual(store.get('intent-2')['state'], 'PREPARED')
                if state != 'PREPARED':
                    with self.assertRaises(IntentError):
                        store.claim('intent-1', expected_revision=0, attempted_at_ms=1004)

    def test_invalid_raw_insert_and_direct_state_jump(self):
        self.prepare()
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            saved = dict(db.execute('SELECT * FROM bot_order_intents').fetchone())
        before = self.path.read_bytes()
        for changes in (dict(state='BOUND', revision=2, attempted_at_ms=1001,
                             exchange_order_id='123456', bound_at_ms=1002,
                             binding_digest='a'*64),
                        dict(requested_quantity='44.0'),
                        dict(order_price='NaN'),
                        dict(state='FILL_CONFIRMED', revision=3, attempted_at_ms=1001,
                             exchange_order_id='123456', bound_at_ms=1002,
                             binding_digest='a'*64, filled_quantity='43',
                             average_fill_price='1.025', fill_timestamp_ms=1003,
                             fill_digest='a'*64)):
            values = saved | {'intent_id': 'new-' + str(len(changes)),
                              'external_oid': 'new-' + str(len(changes)),
                              'strategy_decision_id': 'new-' + str(len(changes)),
                              'local_trade_id': 99} | changes
            with self.subTest(changes=changes), sqlite3.connect(self.path) as db:
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute('INSERT INTO bot_order_intents (' + ','.join(values) +
                               ') VALUES (' + ','.join('?' for _ in values) + ')',
                               tuple(values.values()))
            self.assertEqual(self.path.read_bytes(), before)
        with sqlite3.connect(self.path) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE bot_order_intents SET state='BOUND',revision=2,"
                           "attempted_at_ms=1001,exchange_order_id='123456',"
                           "bound_at_ms=1002,binding_digest=? WHERE intent_id='intent-1'",
                           ('a'*64,))
        self.assertEqual(self.path.read_bytes(), before)

    def test_invalid_full_fill_evidence_fails_without_sql_mutation(self):
        evidence = self.bound()
        self.store.bind('intent-1', expected_revision=1, evidence=evidence)
        fill = evidence | dict(status='FILLED', cumulative_filled_quantity='44',
                               average_fill_price='1.025', exchange_fill_timestamp_ms=1003)
        before = self.path.read_bytes()
        for change in (dict(cumulative_filled_quantity='43'),
                       dict(cumulative_filled_quantity='0'),
                       dict(cumulative_filled_quantity='-1'),
                       dict(cumulative_filled_quantity='NaN'),
                       dict(average_fill_price='0'),
                       dict(average_fill_price='Infinity'),
                       dict(average_fill_price=1.025),
                       dict(exchange_fill_timestamp_ms=1001),
                       dict(exchange_order_id='999999'),
                       dict(status='UNKNOWN')):
            with self.subTest(change=change), self.assertRaises(IntentError):
                self.store.confirm_fill('intent-1', expected_revision=2,
                                        evidence=fill | change)
            self.assertEqual(self.path.read_bytes(), before)
        for quantity, price in (('43', '1.025'), ('0', '1.025'),
                                ('-1', '1.025'), ('44', 'NaN'),
                                ('44', '0'), ('44', '1.0')):
            with self.subTest(quantity=quantity, price=price), sqlite3.connect(self.path) as db:
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute("UPDATE bot_order_intents SET state='FILL_CONFIRMED',"
                               "revision=3,filled_quantity=?,average_fill_price=?,"
                               "fill_timestamp_ms=1003,fill_digest=? WHERE intent_id='intent-1'",
                               (quantity, price, 'a'*64))
            self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.store.get('intent-1')['state'], 'BOUND')

    def test_generated_oid_exact_retry_and_changed_input(self):
        values = {k: v for k, v in self.fields.items() if k != 'external_oid'}
        with patch.object(intent_module.secrets, 'token_hex', return_value='a'*32) as generate:
            first = self.store.prepare(**values)
            self.assertEqual(first['external_oid'], 'a'*32)
            retry = IntentStore(self.path).prepare(**values | dict(requested_quantity='44.00'))
            self.assertEqual(retry, first)
            generate.assert_called_once()
        with self.assertRaises(IntentError):
            IntentStore(self.path).prepare(**values | dict(order_price='2'))

    def test_read_time_rejects_corrupt_identity_binding_and_fill(self):
        for phase, field, value in (('PREPARED', 'intent_digest', '0'*64),
                                    ('BOUND', 'binding_digest', '0'*64),
                                    ('FILL_CONFIRMED', 'average_fill_price', '2')):
            with self.subTest(phase=phase):
                path = Path(self.tmp.name) / ('corrupt-' + phase + '.sqlite')
                store = IntentStore(path)
                store.initialize()
                store.prepare(**self.fields)
                evidence = dict(intent_id='intent-1', local_trade_id=17,
                                account_id='account-a', venue='MEXC_FUTURES',
                                external_oid='synthetic-oid-1', native_symbol='SUI_USDT',
                                opening_side='LONG', requested_quantity='44',
                                exchange_order_id='123456', exchange_timestamp_ms=1002,
                                status='ACCEPTED')
                if phase != 'PREPARED':
                    store.claim('intent-1', expected_revision=0, attempted_at_ms=1001)
                    store.bind('intent-1', expected_revision=1, evidence=evidence)
                if phase == 'FILL_CONFIRMED':
                    store.confirm_fill('intent-1', expected_revision=2,
                        evidence=evidence | dict(status='FILLED',
                            cumulative_filled_quantity='44', average_fill_price='1.025',
                            exchange_fill_timestamp_ms=1003))
                # Simulate preexisting on-disk corruption, then restore the exact
                # schema to isolate read-time validation from schema validation.
                with sqlite3.connect(path) as db:
                    trigger = db.execute("SELECT sql FROM sqlite_master WHERE name='bot_intent_immutable'").fetchone()[0]
                    db.execute('DROP TRIGGER bot_intent_immutable')
                    db.execute(f'UPDATE bot_order_intents SET {field}=?', (value,))
                    db.execute(trigger)
                frozen = path.read_bytes()
                with self.assertRaises(IntentError):
                    IntentStore(path).get('intent-1')
                with self.assertRaises(IntentError):
                    IntentStore(path).claim('intent-1', expected_revision=0,
                                             attempted_at_ms=1001)
                self.assertEqual(path.read_bytes(), frozen)

    def test_schema_contract_rejects_same_name_changed_trigger(self):
        self.prepare()
        with sqlite3.connect(self.path) as db:
            db.execute('DROP TRIGGER bot_intent_immutable')
            db.execute('CREATE TRIGGER bot_intent_immutable BEFORE UPDATE ON '
                       'bot_order_intents BEGIN SELECT 1; END')
        before = self.path.read_bytes()
        with self.assertRaises(IntentError):
            IntentStore(self.path).get('intent-1')
        with self.assertRaises(IntentError):
            IntentStore(self.path).claim('intent-1', expected_revision=0,
                                         attempted_at_ms=1001)
        self.assertEqual(self.path.read_bytes(), before)

    def test_claim_failure_before_or_during_commit_rolls_back(self):
        self.prepare()
        before = self.path.read_bytes()
        original_get = IntentStore._get
        calls = 0
        def fail_after_update(db, intent_id):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise IntentError('injected before commit')
            return original_get(db, intent_id)
        with patch.object(IntentStore, '_get', staticmethod(fail_after_update)):
            with self.assertRaises(IntentError):
                self.store.claim('intent-1', expected_revision=0, attempted_at_ms=1001)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(IntentStore(self.path).get('intent-1')['state'], 'PREPARED')
        original_connect = sqlite3.connect
        class FailCommit(sqlite3.Connection):
            def execute(self, sql, *args):
                if sql == 'COMMIT':
                    raise sqlite3.OperationalError('synthetic commit failure')
                return super().execute(sql, *args)
        def connect(*args, **kwargs):
            return original_connect(*args, **(kwargs | {'factory': FailCommit}))
        with patch.object(intent_module.sqlite3, 'connect', side_effect=connect):
            with self.assertRaises(IntentError):
                self.store.claim('intent-1', expected_revision=0, attempted_at_ms=1001)
        self.assertEqual(self.path.read_bytes(), before)
        acquired = IntentStore(self.path).claim('intent-1', expected_revision=0,
                                                attempted_at_ms=1001)
        self.assertEqual(acquired['claim_outcome'], 'CLAIM_ACQUIRED')
        self.assertEqual(IntentStore(self.path).claim('intent-1', expected_revision=0,
            attempted_at_ms=1001)['claim_outcome'], 'ALREADY_CLAIMED')

    def test_every_commit_boundary_rolls_back_and_retries_after_restart(self):
        original_connect = sqlite3.connect
        original_get = IntentStore._get
        class FailCommit(sqlite3.Connection):
            def execute(self, sql, *args):
                if sql == 'COMMIT':
                    raise sqlite3.OperationalError('synthetic commit failure')
                return super().execute(sql, *args)
        def connect(*args, **kwargs):
            return original_connect(*args, **(kwargs | {'factory': FailCommit}))
        for stage in ('PREPARE', 'CLAIM', 'BIND', 'FILL', 'REJECT', 'CONFLICT'):
            with self.subTest(stage=stage):
                path = Path(self.tmp.name) / ('failure-' + stage + '.sqlite')
                store = IntentStore(path)
                store.initialize()
                identity = dict(intent_id='intent-1', local_trade_id=17,
                                account_id='account-a', venue='MEXC_FUTURES',
                                external_oid='synthetic-oid-1', native_symbol='SUI_USDT',
                                opening_side='LONG', requested_quantity='44',
                                exchange_order_id='123456', exchange_timestamp_ms=1002,
                                status='ACCEPTED')
                if stage != 'PREPARE':
                    store.prepare(**self.fields)
                if stage in ('BIND', 'FILL', 'REJECT'):
                    store.claim('intent-1', expected_revision=0, attempted_at_ms=1001)
                if stage == 'FILL':
                    store.bind('intent-1', expected_revision=1, evidence=identity)
                def operation():
                    if stage == 'PREPARE':
                        return store.prepare(**self.fields)
                    if stage == 'CLAIM':
                        return store.claim('intent-1', expected_revision=0,
                                           attempted_at_ms=1001)
                    if stage == 'BIND':
                        return store.bind('intent-1', expected_revision=1,
                                          evidence=identity)
                    if stage == 'FILL':
                        return store.confirm_fill('intent-1', expected_revision=2,
                            evidence=identity | dict(status='FILLED',
                                cumulative_filled_quantity='44', average_fill_price='1.025',
                                exchange_fill_timestamp_ms=1003))
                    if stage == 'REJECT':
                        return store.reject('intent-1', expected_revision=1,
                            evidence=identity | {'status': 'REJECTED'})
                    return store.mark_conflict('intent-1', expected_revision=0)
                before = path.read_bytes()
                calls = 0
                def fail_before_commit(db, intent_id):
                    nonlocal calls
                    calls += 1
                    if calls == (1 if stage == 'PREPARE' else 2):
                        raise IntentError('synthetic precommit failure')
                    return original_get(db, intent_id)
                with patch.object(IntentStore, '_get', staticmethod(fail_before_commit)):
                    with self.assertRaises(IntentError):
                        operation()
                self.assertEqual(path.read_bytes(), before)
                with patch.object(intent_module.sqlite3, 'connect', side_effect=connect):
                    with self.assertRaises(IntentError):
                        operation()
                self.assertEqual(path.read_bytes(), before)
                result = operation()
                self.assertEqual(result['state'], {'PREPARE':'PREPARED',
                    'CLAIM':'SUBMISSION_UNCERTAIN', 'BIND':'BOUND',
                    'FILL':'FILL_CONFIRMED', 'REJECT':'REJECTED',
                    'CONFLICT':'CONFLICT'}[stage])
                self.assertEqual(IntentStore(path).get('intent-1')['state'], result['state'])


if __name__ == '__main__':
    unittest.main()
