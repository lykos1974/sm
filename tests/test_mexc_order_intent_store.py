"""Synthetic, offline durability tests for the separate order-intent database."""
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

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
        self.assertEqual(second.get('intent-1'), claimed)
        self.assertEqual(second.claim('intent-1', expected_revision=1,
                                      attempted_at_ms=1001), claimed)
        for revision, timestamp in ((0, 1001), (1, 1002)):
            with self.assertRaises(IntentError):
                second.claim('intent-1', expected_revision=revision,
                             attempted_at_ms=timestamp)
        self.assertEqual(IntentStore(self.path).get('intent-1'), claimed)

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
                                                    attempted_at_ms=1001)['state']
            except IntentError:
                return 'BLOCKED'
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(claim, (0, 1)))
        self.assertEqual(sorted(outcomes), ['BLOCKED', 'SUBMISSION_UNCERTAIN'])
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


if __name__ == '__main__':
    unittest.main()
