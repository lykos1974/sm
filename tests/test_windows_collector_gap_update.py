"""Offline package pins and reversible isolated collector installation."""

import hashlib
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "market_collector"
SCRIPT = PACKAGE / "WINDOWS_COLLECTOR_GAP_UPDATE.ps1"
PS = shutil.which("pwsh") or shutil.which("powershell.exe")


class CollectorPackageManifestTests(unittest.TestCase):
    def test_exact_two_lf_pins(self):
        content = SCRIPT.read_text(encoding="utf-8")
        pins = dict(re.findall(r"^\s*'([^']+\.py)' = '([0-9a-f]{64})'$", content, re.M))
        self.assertEqual(set(pins), {"collector.py", "storage.py"})
        for name, expected in pins.items():
            raw = (PACKAGE / name).read_bytes()
            self.assertEqual(hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest(), expected)

    def test_scope_and_no_process_start(self):
        content = SCRIPT.read_text(encoding="utf-8")
        for forbidden in ("Start-Process", "Start-Service", "Invoke-WebRequest", "settings.json", "market_data.db", "sqlite3", "mexc.com"):
            self.assertNotIn(forbidden, content)
        for marker in ("Assert-Package $source", "Hash-File (Join-Path $target $name)", "manifest.json", "-ConfirmServicesStopped"):
            self.assertIn(marker, content)


@unittest.skipUnless(PS, "PowerShell unavailable; Windows mode tests require PowerShell")
class CollectorPackageModeTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.source = Path(temp.name) / "source"
        self.target = Path(temp.name) / "target"
        self.source.mkdir()
        self.target.mkdir()
        for name in ("collector.py", "storage.py"):
            canonical = (PACKAGE / name).read_bytes().replace(b"\r\n", b"\n")
            (self.source / name).write_bytes(canonical.replace(b"\n", b"\r\n"))
            (self.target / name).write_bytes(("old " + name).encode())
        self.operator = {
            "settings.json": b'{"operator":true}',
            "market_data.db": b"db bytes",
            "market_data.db-wal": b"wal bytes",
            "market_data.db-shm": b"shm bytes",
            "operator.log": b"log bytes",
            "app.py": b"app bytes",
        }
        for name, data in self.operator.items():
            (self.target / name).write_bytes(data)

    def run_mode(self, mode, *args, success=True):
        p = subprocess.run(
            [PS, "-NoProfile", "-NonInteractive", "-File", str(SCRIPT),
             "-Mode", mode, "-SourceRoot", str(self.source), "-TargetRoot", str(self.target), *args],
            capture_output=True, text=True, timeout=45,
        )
        self.assertEqual(p.returncode == 0, success, p.stdout + p.stderr)
        return p.stdout

    def check_operator_bytes(self):
        for name, data in self.operator.items():
            self.assertEqual((self.target / name).read_bytes(), data)

    def test_validate_apply_rollback_preserve_operator_bytes(self):
        self.run_mode("Validate")
        originals = {n: (self.target / n).read_bytes() for n in ("collector.py", "storage.py")}
        output = self.run_mode("Apply", "-ConfirmServicesStopped")
        backup = output.strip().split("BACKUP=", 1)[1]
        for name in originals:
            self.assertEqual((self.target / name).read_bytes(), (self.source / name).read_bytes().replace(b"\r\n", b"\n"))
        self.check_operator_bytes()
        self.run_mode("Rollback", "-BackupPath", backup, "-ConfirmServicesStopped")
        for name, data in originals.items():
            self.assertEqual((self.target / name).read_bytes(), data)
        self.check_operator_bytes()

    def test_stale_package_rejected_before_apply(self):
        (self.source / "collector.py").write_bytes(b"stale")
        self.run_mode("Validate", success=False)
        self.run_mode("Apply", "-ConfirmServicesStopped", success=False)
        self.assertEqual((self.target / "collector.py").read_bytes(), b"old collector.py")
        self.check_operator_bytes()

    def test_apply_requires_explicit_idle_confirmation(self):
        self.run_mode("Apply", success=False)
        self.assertEqual((self.target / "collector.py").read_bytes(), b"old collector.py")

    def test_rollback_without_source_after_interrupted_two_file_apply(self):
        output = self.run_mode("Apply", "-ConfirmServicesStopped")
        backup = output.strip().split("BACKUP=", 1)[1]
        # Atomic per-file replacement can be interrupted between the two files.
        (self.target / "storage.py").write_bytes(b"old storage.py")
        shutil.rmtree(self.source)
        self.run_mode("Rollback", "-BackupPath", backup, "-ConfirmServicesStopped")
        self.assertEqual((self.target / "collector.py").read_bytes(), b"old collector.py")
        self.assertEqual((self.target / "storage.py").read_bytes(), b"old storage.py")
        self.check_operator_bytes()

    def test_rollback_refuses_unrelated_target_edit(self):
        output = self.run_mode("Apply", "-ConfirmServicesStopped")
        backup = output.strip().split("BACKUP=", 1)[1]
        (self.target / "collector.py").write_bytes(b"operator edit")
        self.run_mode("Rollback", "-BackupPath", backup, "-ConfirmServicesStopped", success=False)
        self.assertEqual((self.target / "collector.py").read_bytes(), b"operator edit")
        self.check_operator_bytes()


if __name__ == "__main__":
    unittest.main()
