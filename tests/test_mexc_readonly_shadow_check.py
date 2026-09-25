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
            with patch.object(cli, '_publish_new_report', side_effect=OSError('synthetic-secret')):
                code, output, calls = self.run_cli(ARGS + ['--output-report', str(path)])
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(output), {'status': 'FAIL', 'reason': 'REPORT_ERROR'})
            self.assertEqual(len(calls), 2)
            self.assertEqual(list(Path(folder).iterdir()), [])
            self.assertNotIn('synthetic-secret', output)

    def test_cleanup_failure_cannot_turn_published_pass_into_fail(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'report.json'
            with patch.object(cli.os, 'unlink', side_effect=OSError('cleanup failed')):
                code, output, _ = self.run_cli(ARGS + ['--output-report', str(target)])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output)['status'], 'PASS')
            self.assertEqual(list(Path(folder).iterdir()), [target])

    def test_cleanup_failure_before_publication_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'report.json'
            with patch.object(cli.os, 'fsync', side_effect=OSError('fsync failed')):
                with patch.object(cli.os, 'unlink', side_effect=OSError('cleanup failed')):
                    code, output, _ = self.run_cli(ARGS + ['--output-report', str(target)])
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(output), {'status': 'FAIL', 'reason': 'REPORT_ERROR'})
            self.assertEqual(list(Path(folder).iterdir()), [])

    def test_broken_pipe_after_publication_keeps_committed_pass(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'report.json'
            with patch.object(cli, 'print', side_effect=BrokenPipeError('synthetic-secret'), create=True):
                code, output, calls = self.run_cli(ARGS + ['--output-report', str(target)])
            self.assertEqual(code, 0)
            self.assertEqual(output, '')
            self.assertEqual(len(calls), 2)
            self.assertEqual(json.loads(target.read_text())['status'], 'PASS')
            self.assertEqual(list(Path(folder).iterdir()), [target])

    def test_closed_console_after_publication_keeps_committed_pass(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'report.json'
            closed_output = io.StringIO()
            closed_output.close()
            with patch.object(cli, 'print', side_effect=lambda *args, **kwargs: closed_output.write(args[0]), create=True):
                code, output, calls = self.run_cli(ARGS + ['--output-report', str(target)])
            self.assertEqual((code, output, len(calls)), (0, '', 2))
            self.assertEqual(json.loads(target.read_text())['status'], 'PASS')
            self.assertEqual(list(Path(folder).iterdir()), [target])

    def test_other_console_errors_after_publication_keep_committed_pass(self):
        for error in (OSError('closed stream'), UnicodeError('encoding failure')):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as folder:
                target = Path(folder) / 'report.json'
                with patch.object(cli, 'print', side_effect=error, create=True):
                    code, output, _ = self.run_cli(ARGS + ['--output-report', str(target)])
                self.assertEqual((code, output), (0, ''))
                self.assertEqual(json.loads(target.read_text())['status'], 'PASS')

    def test_no_report_does_not_mask_console_failure(self):
        with patch.object(cli, 'print', side_effect=ValueError('closed stream'), create=True):
            with self.assertRaises(ValueError):
                self.run_cli()

    def test_process_control_exceptions_are_not_suppressed(self):
        for exception in (KeyboardInterrupt, SystemExit, GeneratorExit):
            with self.subTest(exception=exception.__name__), tempfile.TemporaryDirectory() as folder:
                target = Path(folder) / 'report.json'
                with patch.object(cli, 'print', side_effect=exception(), create=True):
                    with self.assertRaises(exception):
                        self.run_cli(ARGS + ['--output-report', str(target)])
                self.assertEqual(json.loads(target.read_text())['status'], 'PASS')

    def test_universal_deletion_denial_fails_closed_without_publication(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'report.json'
            with patch.object(cli.os, 'fsync', side_effect=OSError('synthetic-secret')):
                with patch.object(cli.os, 'unlink', side_effect=PermissionError('synthetic-secret')) as unlink:
                    with patch.object(cli.os, 'remove', side_effect=PermissionError('synthetic-secret')) as remove:
                        code, output, _ = self.run_cli(ARGS + ['--output-report', str(target)])
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(output), {'status': 'FAIL', 'reason': 'REPORT_ERROR'})
            self.assertNotIn('synthetic-secret', output)
            self.assertFalse(target.exists())
            self.assertGreaterEqual(unlink.call_count, 1)
            self.assertGreaterEqual(remove.call_count, 1)
            # Universal OS deletion denial makes removal impossible; clear the fixture afterward.
            for artifact in Path(folder).iterdir():
                self.assertTrue(artifact.name.startswith('.mexc-shadow-'))
                artifact.unlink()

    def test_target_created_during_publication_is_not_replaced(self):
        original_publish = cli._publish_new_report
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'report.json'
            def competing_publish(temporary, destination):
                self.assertEqual(Path(temporary).parent, target.parent)
                target.write_bytes(b'competing operator bytes')
                original_publish(temporary, destination)
            with patch.object(cli, '_publish_new_report', side_effect=competing_publish):
                code, output, _ = self.run_cli(ARGS + ['--output-report', str(target)])
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(output), {'status': 'FAIL', 'reason': 'REPORT_ERROR'})
            self.assertEqual(target.read_bytes(), b'competing operator bytes')
            self.assertEqual(list(Path(folder).iterdir()), [target])

    def test_existing_report_and_symlink_remain_untouched(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'report.json'
            path.write_bytes(b'operator bytes')
            code, output, _ = self.run_cli(ARGS + ['--output-report', str(path)])
            self.assertEqual((code, json.loads(output)['reason']), (1, 'REPORT_ERROR'))
            self.assertEqual(path.read_bytes(), b'operator bytes')
            self.assertEqual(list(Path(folder).iterdir()), [path])
            link = Path(folder) / 'alias.json'
            link.symlink_to(path)
            code, output, _ = self.run_cli(ARGS + ['--output-report', str(link)])
            self.assertEqual((code, json.loads(output)['reason']), (1, 'REPORT_ERROR'))
            self.assertEqual(path.read_bytes(), b'operator bytes')
            self.assertTrue(link.is_symlink())

    def test_create_write_flush_fsync_close_publish_failures_leave_no_artifacts(self):
        real_mkstemp, real_fdopen = cli.tempfile.mkstemp, cli.os.fdopen

        class BrokenStream:
            def __init__(self, stream, broken):
                self.stream, self.broken = stream, broken
            def __enter__(self): return self
            def __exit__(self, typ, value, trace):
                if self.broken == 'close':
                    self.stream.close()
                    raise OSError('synthetic-secret')
                return self.stream.__exit__(typ, value, trace)
            def write(self, value):
                if self.broken == 'write': raise OSError('synthetic-secret')
                return self.stream.write(value)
            def flush(self):
                if self.broken == 'flush': raise OSError('synthetic-secret')
                return self.stream.flush()
            def fileno(self): return self.stream.fileno()

        for boundary in ('create', 'write', 'flush', 'fsync', 'close', 'publication'):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as folder:
                target = Path(folder) / 'report.json'
                def broken_create(*args, **kwargs):
                    if boundary == 'create': raise OSError('synthetic-secret')
                    return real_mkstemp(*args, **kwargs)
                def broken_fdopen(fd, mode):
                    return BrokenStream(real_fdopen(fd, mode), boundary)
                with patch.object(cli.tempfile, 'mkstemp', side_effect=broken_create):
                    with patch.object(cli.os, 'fdopen', side_effect=broken_fdopen):
                        with patch.object(cli.os, 'fsync', side_effect=OSError('synthetic-secret') if boundary == 'fsync' else None):
                            with patch.object(cli, '_publish_new_report', side_effect=OSError('synthetic-secret') if boundary == 'publication' else None):
                                code, output, _ = self.run_cli(ARGS + ['--output-report', str(target)])
                self.assertEqual(code, 1)
                self.assertEqual(json.loads(output), {'status': 'FAIL', 'reason': 'REPORT_ERROR'})
                self.assertEqual(list(Path(folder).iterdir()), [])

    def test_fdopen_failure_closes_descriptor_and_removes_temp(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'report.json'
            with patch.object(cli.os, 'fdopen', side_effect=OSError('synthetic-secret')):
                code, output, _ = self.run_cli(ARGS + ['--output-report', str(target)])
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(output), {'status': 'FAIL', 'reason': 'REPORT_ERROR'})
            self.assertEqual(list(Path(folder).iterdir()), [])

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
