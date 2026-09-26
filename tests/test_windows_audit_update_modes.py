"""Small isolated updater exercises; execute on Windows PowerShell or pwsh."""

import hashlib
import json
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "WINDOWS_AUDIT_UPDATE.ps1"
POWERSHELL = shutil.which("pwsh") or shutil.which("powershell.exe")
PINNED = re.findall(
    r'^\s*"([^"]+\.(?:py|json))"\s*=\s*"[0-9a-f]{64}"$',
    SCRIPT.read_text(encoding="utf-8"),
    re.MULTILINE,
)


@unittest.skipUnless(POWERSHELL, "PowerShell is required for updater mode exercises")
class WindowsAuditUpdateModeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name)
        self.source, self.target = base / "source", base / "target"
        self.source.mkdir()
        self.target.mkdir()
        for name in PINNED:
            destination = self.source / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes((ROOT / name).read_bytes())
            target = self.target / name
            target.parent.mkdir(parents=True, exist_ok=True)
            if name == PINNED[0]:
                target.write_bytes(b"original exact bytes\r\n")
        settings = (ROOT / "pnf_mvp/settings.json").read_bytes()
        assert json.loads(settings)["strategy_validation_enabled"] is False
        assert json.loads(settings)["operational_alerts_enabled"] is False
        for folder in (self.source, self.target):
            dest = folder / "pnf_mvp/settings.json"
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(settings)
        self.settings_hash = hashlib.sha256(settings).hexdigest()

    def run_mode(self, mode, *args, success=True):
        process = subprocess.run(
            [POWERSHELL, "-NoProfile", "-NonInteractive", "-File", str(SCRIPT),
             "-Mode", mode, "-SourceRoot", str(self.source), "-TargetRoot", str(self.target),
             *args], capture_output=True, text=True, timeout=30,
        )
        if success:
            self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        else:
            self.assertNotEqual(process.returncode, 0)
        return process

    def test_validate_accepts_crlf_and_rejects_tampered_package(self):
        path = self.source / "pnf_mvp/app.py"
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
        self.run_mode("Validate")
        path.write_bytes(path.read_bytes() + b"# altered\n")
        self.run_mode("Validate", success=False)

    def test_mexc_trader_pin_rejects_tampering(self):
        path = self.source / "live_mexc_forward_trader.py"
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
        self.run_mode("Validate")
        path.write_bytes(path.read_bytes() + b"# altered\n")
        self.run_mode("Validate", success=False)

    def test_mexc_readonly_adapter_pin_rejects_tampering(self):
        path = self.source / "mexc_readonly_order_status.py"
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
        self.run_mode("Validate")
        path.write_bytes(path.read_bytes() + b"# altered\n")
        self.run_mode("Validate", success=False)

    def test_shadow_check_pin_rejects_stale_and_altered_bytes(self):
        path = self.source / "mexc_readonly_shadow_check.py"
        original = path.read_bytes()
        path.write_bytes(original.replace(b"\n", b"\r\n"))
        self.run_mode("Validate")
        path.write_bytes(original.replace(b"except Exception:", b"except OSError:"))
        self.run_mode("Validate", success=False)
        path.write_bytes(original + b"# altered\n")
        self.run_mode("Validate", success=False)

    def test_discovery_pin_rejects_stale_and_altered_bytes(self):
        path = self.source / "mexc_readonly_order_discovery.py"
        original = path.read_bytes()
        path.write_bytes(original.replace(b"\n", b"\r\n"))
        self.run_mode("Validate")
        path.write_bytes(original.replace(b"if side in (2, 4):", b"if side not in (1, 3):\n            raise ValueError()"))
        self.run_mode("Validate", success=False)
        path.write_bytes(original + b"# altered\n")
        self.run_mode("Validate", success=False)

    def test_apply_rejects_tampered_discovery_before_backup(self):
        path = self.source / "mexc_readonly_order_discovery.py"
        path.write_bytes(path.read_bytes() + b"# altered\n")
        operator = self.target / "operator.log"
        operator.write_bytes(b"operator bytes\r\n")
        self.run_mode("Apply", "-ConfirmServicesStopped", success=False)
        self.assertFalse((self.target / "_audit_update_backups").exists())
        self.assertEqual(operator.read_bytes(), b"operator bytes\r\n")
        self.assertFalse((self.target / "mexc_readonly_order_discovery.py").exists())

    def test_discovery_apply_and_rollback_preserve_operator_files(self):
        operator = self.target / "operator.log"
        operator.write_bytes(b"operator bytes\r\n")
        database = self.target / "pnf_mvp" / "operator.sqlite3"
        database.write_bytes(b"database bytes\x00")
        self.run_mode("Apply", "-ConfirmServicesStopped")
        installed = self.target / "mexc_readonly_order_discovery.py"
        self.assertEqual(installed.read_bytes(), (ROOT / "mexc_readonly_order_discovery.py").read_bytes())
        backup = next((self.target / "_audit_update_backups").glob("*/manifest.json"))
        self.run_mode("Rollback", "-BackupPath", str(backup.parent), "-ConfirmServicesStopped")
        self.assertFalse(installed.exists())
        self.assertEqual(operator.read_bytes(), b"operator bytes\r\n")
        self.assertEqual(database.read_bytes(), b"database bytes\x00")
        self.assertEqual(hashlib.sha256((self.target / "pnf_mvp/settings.json").read_bytes()).hexdigest(), self.settings_hash)

    def test_discovery_upgrade_restores_previous_bytes_and_preserves_operator_data(self):
        previous = b"existing discovery bytes\r\n"
        installed = self.target / "mexc_readonly_order_discovery.py"
        installed.write_bytes(previous)
        operator = self.target / "operator-created.txt"
        operator.write_bytes(b"operator\x00contents")
        credentials = self.target / "private.credentials"
        credentials.write_bytes(b"opaque local data\r\n")
        database = self.target / "pnf_mvp" / "operator.sqlite3"
        database.write_bytes(b"database\x00contents")
        log = self.target / "operator.log"
        log.write_bytes(b"existing log\r\n")
        self.run_mode("Validate")
        self.run_mode("Apply", "-ConfirmServicesStopped")
        self.assertEqual(installed.read_bytes(), (ROOT / "mexc_readonly_order_discovery.py").read_bytes())
        backup = next((self.target / "_audit_update_backups").glob("*/manifest.json"))
        self.assertEqual((backup.parent / "code/mexc_readonly_order_discovery.py").read_bytes(), previous)
        manifest = json.loads(backup.read_text(encoding="utf-8-sig"))
        self.assertEqual(manifest["package_source_commit"], "f35da9ce2a222fdebdc4f14b14a1dc54e7bc08ef")
        entry = next(item for item in manifest["files"]
                     if item["relative_path"] == "mexc_readonly_order_discovery.py")
        self.assertEqual(entry["expected_sha256"], "bbdd80f88f85ec3f395e4059c8f3e586cec1a57ae32037868b2ba79d2d85a084")
        self.assertRegex(backup.parent.name, r"^\d{8}_\d{6}_\d{7}_[0-9a-f]{8}$")
        self.assertEqual(log.read_bytes(), b"existing log\r\n")
        self.assertEqual(operator.read_bytes(), b"operator\x00contents")
        self.assertEqual(credentials.read_bytes(), b"opaque local data\r\n")
        self.assertEqual(database.read_bytes(), b"database\x00contents")
        self.run_mode("Rollback", "-BackupPath", str(backup.parent), "-ConfirmServicesStopped")
        self.assertEqual(installed.read_bytes(), previous)
        self.assertEqual(log.read_bytes(), b"existing log\r\n")
        self.assertEqual(operator.read_bytes(), b"operator\x00contents")
        self.assertEqual(credentials.read_bytes(), b"opaque local data\r\n")
        self.assertEqual(database.read_bytes(), b"database\x00contents")
        self.assertEqual(hashlib.sha256((self.target / "pnf_mvp/settings.json").read_bytes()).hexdigest(), self.settings_hash)

    def test_apply_rejects_tampered_shadow_without_touching_operator_files(self):
        path = self.source / "mexc_readonly_shadow_check.py"
        path.write_bytes(path.read_bytes() + b"# altered\n")
        operator = self.target / "operator.log"
        operator.write_bytes(b"operator bytes\r\n")
        self.run_mode("Apply", "-ConfirmServicesStopped", success=False)
        self.assertFalse((self.target / "_audit_update_backups").exists())
        self.assertEqual(operator.read_bytes(), b"operator bytes\r\n")
        self.assertFalse((self.target / "mexc_readonly_shadow_check.py").exists())

    def test_shadow_apply_and_rollback_preserve_operator_files(self):
        operator = self.target / "operator.log"
        operator.write_bytes(b"operator bytes\r\n")
        database = self.target / "pnf_mvp" / "operator.sqlite3"
        database.write_bytes(b"database bytes\x00")
        self.run_mode("Apply", "-ConfirmServicesStopped")
        installed = self.target / "mexc_readonly_shadow_check.py"
        self.assertEqual(installed.read_bytes(), (ROOT / "mexc_readonly_shadow_check.py").read_bytes())
        backup = next((self.target / "_audit_update_backups").glob("*/manifest.json"))
        self.run_mode("Rollback", "-BackupPath", str(backup.parent), "-ConfirmServicesStopped")
        self.assertFalse(installed.exists())
        self.assertEqual(operator.read_bytes(), b"operator bytes\r\n")
        self.assertEqual(database.read_bytes(), b"database bytes\x00")
        self.assertEqual(hashlib.sha256((self.target / "pnf_mvp/settings.json").read_bytes()).hexdigest(), self.settings_hash)

    def test_apply_and_rollback_restore_exact_bytes_and_settings(self):
        snapshot = self.source / "pnf_mvp/data/tick_provenance/strategy_validation_tick_provenance.json"
        snapshot.write_bytes(snapshot.read_bytes().replace(b"\n", b"\r\n"))
        original = (self.target / PINNED[0]).read_bytes()
        self.run_mode("Apply", "-ConfirmServicesStopped")
        installed = self.target / "pnf_mvp/data/tick_provenance/strategy_validation_tick_provenance.json"
        self.assertEqual(hashlib.sha256(installed.read_bytes()).hexdigest(),
                         "8ad27ceb2d89d2e9ab57b954240189982fdbaa5ceb02f99d474f540ea2dfa562")
        backup = next((self.target / "_audit_update_backups").glob("*/manifest.json"))
        self.run_mode("Rollback", "-BackupPath", str(backup.parent), "-ConfirmServicesStopped")
        self.assertEqual((self.target / PINNED[0]).read_bytes(), original)
        self.assertFalse(installed.exists())
        self.assertEqual(hashlib.sha256((self.target / "pnf_mvp/settings.json").read_bytes()).hexdigest(), self.settings_hash)

    def test_apply_rejects_enabled_settings_before_creating_backups(self):
        path = self.target / "pnf_mvp/settings.json"
        obj = json.loads(path.read_text(encoding="utf-8"))
        obj["strategy_validation_enabled"] = True
        path.write_text(json.dumps(obj), encoding="utf-8")
        self.run_mode("Apply", "-ConfirmServicesStopped", success=False)
        self.assertFalse((self.target / "_audit_update_backups").exists())


if __name__ == "__main__":
    unittest.main()
