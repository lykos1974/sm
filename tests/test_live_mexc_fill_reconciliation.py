import json
from decimal import Decimal
import sqlite3
import tempfile
from pathlib import Path

import unittest

import live_mexc_forward_trader as trader


class Adapter:
    def __init__(self, response):
        self.response = response
        self.calls = 0

    def get_order_status(self, order_id, symbol):
        self.calls += 1
        return self.response


def setup(conn, side='LONG'):
    trader.init_live_tables(conn)
    order = {'symbol': 'BTC_USDT', 'side': 1 if side == 'LONG' else 3, 'vol': '2'}
    conn.execute("""INSERT INTO live_trades(created_at,symbol,pattern,side,entry_time,entry_price,stop_price,tp1_price,tp2_price,notional_usdt,exchange_order_id,status,raw_order_response)
        VALUES('now','MEXC_FUT:BTCUSDT','triangle',?,1,100,90,105,120,1,'oid','ORDER_SENT',?)""", (side, json.dumps({'order_request': order})))
    conn.commit()


def evidence(side='LONG'):
    return {'success': True, 'data': {'exchange_order_id': 'oid', 'symbol': 'BTC_USDT', 'side': 1 if side == 'LONG' else 3,
            'requested_quantity': '2', 'cumulative_filled_quantity': '2', 'average_fill_price': '100.5',
            'exchange_fill_timestamp': 150000, 'status': 'FILLED'}}


class FillReconciliationTests(unittest.TestCase):
    def test_full_fill_atomic_retry_restart_and_conflict(self):
        for side in ('LONG', 'SHORT'):
            with self.subTest(side=side), tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / 'db.sqlite'
                conn = sqlite3.connect(path)
                setup(conn, side)
                adapter = Adapter(evidence(side))
                self.assertEqual(trader.reconcile_exchange_fills(conn, adapter), 0)
                self.assertEqual(adapter.calls, 0)
                self.assertEqual(trader.reconcile_exchange_fills(conn, adapter, enabled=True), 1)
                original = conn.execute('SELECT status,confirmed_fill_ts,raw_fill_evidence,entry_price,signal_entry_price,execution_entry_price FROM live_trades').fetchone()
                self.assertEqual(original[:2], ('FILLED', 150000))
                self.assertEqual(json.loads(original[2]), evidence(side))
                self.assertEqual(original[3:], (100.5, '100.0', '100.5'))
                conn.close()
                conn = sqlite3.connect(path)
                adapter.response = evidence(side)
                adapter.response['data']['average_fill_price'] = '999'
                before = conn.total_changes
                self.assertEqual(trader.reconcile_exchange_fills(conn, adapter, enabled=True), 0)
                self.assertEqual(conn.total_changes, before)
                self.assertEqual(conn.execute('SELECT status,confirmed_fill_ts,raw_fill_evidence,entry_price,signal_entry_price,execution_entry_price FROM live_trades').fetchone(), original)
                conn.close()

    def test_exact_fill_price_drives_exits_and_r_both_sides(self):
        for side, stop, target, high, low, expected in (
            ('LONG', 90, 120, 121, 100, Decimal('19.5') / Decimal('10.5')),
            ('SHORT', 110, 80, 109, 79, Decimal('20.5') / Decimal('9.5')),
        ):
            with self.subTest(side=side):
                conn = sqlite3.connect(':memory:')
                setup(conn, side)
                conn.execute('UPDATE live_trades SET stop_price=?, tp2_price=?', (stop, target))
                conn.execute('CREATE TABLE candles(symbol TEXT, interval TEXT, close_time INTEGER, high REAL, low REAL)')
                conn.executemany("INSERT INTO candles VALUES('MEXC_FUT:BTCUSDT','1m',?,?,?)", (
                    (120000, 130, 70), (180000, 130, 70), (240000, high, low),
                ))
                conn.commit()
                self.assertEqual(trader.reconcile_exchange_fills(conn, Adapter(evidence(side)), enabled=True), 1)
                trader.update_open_trade_exits(conn, object(), live_enabled=False)
                status, ts, result = conn.execute('SELECT status,exit_time,realized_r FROM live_trades').fetchone()
                self.assertEqual((status, ts), ('POSITION_CLOSED', 240000))
                self.assertEqual(result, float(expected))
                conn.close()

    def test_update_failure_rolls_back_every_fill_field(self):
        conn = sqlite3.connect(':memory:')
        setup(conn)
        conn.execute('CREATE TRIGGER fail_fill BEFORE UPDATE OF raw_fill_evidence ON live_trades BEGIN SELECT RAISE(ABORT, "injected"); END')
        conn.commit()
        before = conn.execute('SELECT * FROM live_trades').fetchone()
        with self.assertRaises(sqlite3.DatabaseError):
            trader.reconcile_exchange_fills(conn, Adapter(evidence()), enabled=True)
        self.assertEqual(conn.execute('SELECT * FROM live_trades').fetchone(), before)
        conn.close()

    def test_invalid_evidence_is_write_free(self):
        changes = [
            {'status': 'PARTIALLY_FILLED'}, {'status': 'CANCELLED'}, {'status': 'REJECTED'},
            {'status': 'EXPIRED'}, {'status': 'UNKNOWN'}, {'requested_quantity': '3'},
            {'cumulative_filled_quantity': '1'}, {'cumulative_filled_quantity': '0'},
            {'exchange_order_id': 'other'}, {'symbol': 'ETH_USDT'}, {'side': 3},
            {'average_fill_price': 'NaN'}, {'exchange_fill_timestamp': None},
            {'exchange_fill_timestamp': '150000'}, {'requested_quantity': None},
        ]
        for change in changes:
            with self.subTest(change=change):
                conn = sqlite3.connect(':memory:')
                setup(conn)
                response = evidence()
                response['data'].update(change)
                before = conn.total_changes
                self.assertEqual(trader.reconcile_exchange_fills(conn, Adapter(response), enabled=True), 0)
                self.assertEqual(conn.total_changes, before)
                self.assertEqual(conn.execute('SELECT status,confirmed_fill_ts,raw_fill_evidence,realized_r FROM live_trades').fetchone(), ('ORDER_SENT', None, None, None))
                conn.close()

    def test_bad_response_is_write_free(self):
        for response in (None, {}, {'success': False}, {'success': True, 'data': []}, TimeoutError(), ValueError('API error')):
            with self.subTest(response=response):
                conn = sqlite3.connect(':memory:')
                setup(conn)
                before = conn.total_changes
                self.assertEqual(trader.reconcile_exchange_fills(conn, Adapter(response), enabled=True), 0)
                self.assertEqual(conn.total_changes, before)
                conn.close()


if __name__ == '__main__':
    unittest.main()
