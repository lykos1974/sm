import json
import hashlib
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "pnf_mvp" / "validation_tick_provenance_preflight.py"
SNAPSHOT = (
    ROOT
    / "pnf_mvp"
    / "data"
    / "tick_provenance"
    / "strategy_validation_tick_provenance.json"
)


class ValidationTickProvenancePreflightCliTests(TestCase):
    def run_cli(self, *args, cwd=None):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_help_prints_usage(self):
        result = self.run_cli("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("usage:", result.stdout)
        self.assertIn("--settings", result.stdout)
        self.assertIn("--snapshot", result.stdout)

    def test_default_cli_passes_from_an_unrelated_working_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.run_cli(cwd=temp_dir)
            payload = json.loads(result.stdout)
            self.assertEqual(result.returncode, 0)
            self.assertEqual(
                payload,
                {
                    "snapshot_sha256": "8ad27ceb2d89d2e9ab57b954240189982fdbaa5ceb02f99d474f540ea2dfa562",
                    "status": "PASS",
                    "verified_symbol_count": 12,
                },
            )
            self.assertEqual(list(Path(temp_dir).iterdir()), [])

    def test_cli_mismatch_fails_nonzero_with_one_json_result(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            changed = Path(temp_dir) / "changed.json"
            changed.write_bytes(SNAPSHOT.read_bytes().replace(b'"0.01"', b'"0.02"', 1))
            result = self.run_cli("--snapshot", str(changed), cwd=temp_dir)
            lines = result.stdout.splitlines()
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(len(lines), 1)
            payload = json.loads(lines[0])
            self.assertEqual(payload["status"], "FAIL")
            self.assertEqual(
                payload["snapshot_sha256"],
                hashlib.sha256(changed.read_bytes()).hexdigest(),
            )
            self.assertEqual(payload["verified_symbol_count"], 0)
            self.assertIn("SHA-256 mismatch", payload["error"])


if __name__ == "__main__":
    import unittest

    unittest.main()
