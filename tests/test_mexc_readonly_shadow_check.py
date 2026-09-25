"""Offline shadow CLI checks with synthetic exchange responses only."""
import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

from tests.test_mexc_readonly_order_status import ORDER, ORDER_ID, SYMBOL, TRADES, Transport
import mexc_readonly_shadow_check as cli

ARGS = ['--order-id', ORDER_ID, '--symbol', SYMBOL, '--side', 'LONG', '--requested-quantity', '3']


class ShadowCheckTests(unittest.TestCase):
    def run_cli(self, args=ARGS, order=ORDER, trades=TRADES, credentials=True):
        output = io.StringIO()
        transport = Transport(order, trades)
        env = {'MEXC_FUTURES_API_KEY': 'synthetic-key',
               'MEXC_FUTURES_API_SECRET': 'synthetic-secret'} if credentials else {}
        with patch.dict(os.environ, env, clear=True), patch.object(sqlite3, 'connect', side_effect=AssertionError('DB access')):
            with patch.object(urllib.request, 'urlopen', side_effect=AssertionError('network access')):
                with patch.object(urllib.request, 'build_opener', side_effect=AssertionError('network access')):
                    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
                        code = cli.main(args, transport=transport)
        return code, output.getvalue(), transport.calls

    def test_pass_exact_two_gets_sanitized_deterministic(self):
        first = self.run_cli()
        second = self.run_cli()
        self.assertEqual((first[0], first[1]), (0, second[1]))
        self.assertEqual(len(first[2]), 2)
        self.assertEqual([call[0].split('api.mexc.com')[1] for call in first[2]],
                         ['/api/v1/private/order/get/' + ORDER_ID,
                          '/api/v1/private/order/deal_details/' + ORDER_ID])
        result = json.loads(first[1])
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(result['evidence']['average_fill_price'], '100.500000000000000001')
        self.assertEqual(result['evidence']['exchange_fill_timestamp'], 160000)
        self.assertEqual(first[1].count('\n'), 1)
        for secret in ('synthetic-key', 'synthetic-secret', 'Signature', 'Request-Time', 'https://'):
            self.assertNotIn(secret, first[1])

    def test_report_created_atomically_with_sanitized_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'report.json'
            code, output, calls = self.run_cli(ARGS + ['--output-report', str(path)])
            self.assertEqual(code, 0)
            self.assertEqual(len(calls), 2)
            self.assertEqual(path.read_text(encoding='utf-8'), output)
            self.assertEqual(list(Path(folder).iterdir()), [path])
            self.assertEqual(self.run_cli(ARGS + ['--output-report', str(path)])[0], 1)
            self.assertEqual(path.read_text(encoding='utf-8'), output)

    def test_report_creation_error_leaves_no_file_or_temp(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'report.json'
            with patch.object(cli.os, 'link', side_effect=OSError('synthetic-secret')):
                code, output, calls = self.run_cli(ARGS + ['--output-report', str(path)])
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(output), {'status': 'FAIL', 'reason': 'REPORT_ERROR'})
            self.assertEqual(len(calls), 2)
            self.assertEqual(list(Path(folder).iterdir()), [])
            self.assertNotIn('synthetic-secret', output)

    def test_fail_closed_without_report_or_secrets(self):
        for args, order, trades, credentials, expected in (
            (ARGS, ORDER, TRADES, False, 'MISSING_CREDENTIALS'),
            (ARGS[:-1] + ['2'], ORDER, TRADES, True, 'EVIDENCE_MISMATCH'),
            (ARGS[:-3] + ['SHORT', '--requested-quantity', '3'], ORDER, TRADES, True, 'EVIDENCE_MISMATCH'),
            (ARGS, {'success': True, 'code': 0, 'data': dict(ORDER['data'], state=4)}, TRADES, True, 'EVIDENCE_UNAVAILABLE'),
            (ARGS, TimeoutError('synthetic-secret'), TRADES, True, 'EVIDENCE_UNAVAILABLE'),
            (ARGS, b'{broken', TRADES, True, 'EVIDENCE_UNAVAILABLE'),
            (['--order-id', '../cancel', '--symbol', SYMBOL, '--side', 'LONG', '--requested-quantity', '3'], ORDER, TRADES, True, 'INVALID_INPUT'),
        ):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / 'failed.json'
                code, output, calls = self.run_cli(args + ['--output-report', str(path)], order, trades, credentials)
                self.assertEqual(code, 1)
                self.assertEqual(json.loads(output), {'status': 'FAIL', 'reason': expected})
                self.assertFalse(path.exists())
                self.assertNotIn('synthetic-secret', output)
                self.assertLessEqual(len(calls), 2)

    def test_help_and_unknown_credentials_arguments(self):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(SystemExit) as help_exit:
                cli.main(['--help'], transport=Transport())
        self.assertEqual(help_exit.exception.code, 0)
        self.assertIn('--order-id', out.getvalue())
        self.assertNotIn('--api-key', out.getvalue())
        with contextlib.redirect_stdout(io.StringIO()) as invalid:
            code = cli.main(ARGS + ['--api-key', 'synthetic-key'], transport=Transport())
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(invalid.getvalue()), {'status': 'FAIL', 'reason': 'INVALID_INPUT'})


if __name__ == '__main__': unittest.main()
