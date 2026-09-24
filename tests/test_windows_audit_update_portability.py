import hashlib
import re
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "WINDOWS_AUDIT_UPDATE.ps1"
def canonical_text_sha256(payload):
    text = payload.decode("utf-8", errors="strict")
    canonical = text.replace("\r\n", "\n")
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def expected_files(script):
    return dict(
        re.findall(
            r'^\s*"([^"]+\.(?:py|json))"\s*=\s*"([0-9a-f]{64})"$', script, re.MULTILINE
        )
    )


def package_source_commit(script):
    match = re.search(r'^\$PackageSourceCommit = "([0-9a-f]{40})"$', script, re.MULTILINE)
    if not match:
        raise AssertionError("missing hash-pinned PackageSourceCommit")
    return match.group(1)


class WindowsAuditUpdatePortabilityTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script = SCRIPT_PATH.read_text(encoding="utf-8")

    def test_source_root_default_is_resolved_after_parameter_binding(self):
        parameter_block = self.script[: self.script.index(")\n\n$ErrorActionPreference")]
        self.assertIn("[string]$SourceRoot,", parameter_block)
        self.assertNotIn("[string]$SourceRoot = $PSScriptRoot", parameter_block)

        fallback = self.script.index(
            'if ([string]::IsNullOrWhiteSpace($SourceRoot)) {\n    $SourceRoot = $PSScriptRoot'
        )
        resolution = self.script.index("$SourceRoot = Resolve-FullPath $SourceRoot")
        self.assertLess(fallback, resolution)
        self.assertIn("$MyInvocation.MyCommand.Path", self.script[fallback:resolution])

    def test_lf_and_crlf_have_same_canonical_hash_but_different_raw_hash(self):
        payload_lf = b"first line\nsecond line\n"
        payload_crlf = payload_lf.replace(b"\n", b"\r\n")
        payload_bare_cr = payload_lf.replace(b"\n", b"\r")
        self.assertNotEqual(
            hashlib.sha256(payload_lf).hexdigest(),
            hashlib.sha256(payload_crlf).hexdigest(),
        )
        self.assertEqual(
            canonical_text_sha256(payload_lf), canonical_text_sha256(payload_crlf)
        )
        self.assertNotEqual(
            canonical_text_sha256(payload_lf), canonical_text_sha256(payload_bare_cr)
        )

    def test_all_pinned_files_match_in_lf_and_crlf_forms(self):
        pinned = expected_files(self.script)
        source_commit = package_source_commit(self.script)
        self.assertEqual(source_commit, "b656eb23454bdbe951cebddf6c90acc036b9ae27")
        self.assertEqual(len(pinned), 11)
        self.assertEqual(pinned["live_mexc_forward_trader.py"], "37282d982b49a576ac0a6d08c94d03edfb7253935135063c15146ea73bb654e2")
        self.assertIn("pnf_mvp/app.py", pinned)
        self.assertIn("pnf_mvp/validation_tick_provenance_preflight.py", pinned)
        snapshot = "pnf_mvp/data/tick_provenance/strategy_validation_tick_provenance.json"
        self.assertEqual(pinned[snapshot], "8ad27ceb2d89d2e9ab57b954240189982fdbaa5ceb02f99d474f540ea2dfa562")
        for relative_path, expected in pinned.items():
            with self.subTest(relative_path=relative_path):
                payload = (ROOT / relative_path).read_bytes()
                lf_payload = payload.replace(b"\r\n", b"\n")
                crlf_payload = lf_payload.replace(b"\n", b"\r\n")
                self.assertEqual(canonical_text_sha256(lf_payload), expected)
                self.assertEqual(canonical_text_sha256(crlf_payload), expected)

    def test_final_runtime_hashes_pass_and_stale_runtime_hashes_fail(self):
        pinned = expected_files(self.script)
        changed_runtime = (
            "pnf_mvp/app.py",
            "pnf_mvp/strategy_validation.py",
            "pnf_mvp/strategy_trade_export.py",
            "pnf_mvp/strategy_evaluator.py",
        )
        for relative_path in changed_runtime:
            with self.subTest(relative_path=relative_path):
                final_payload = (ROOT / relative_path).read_bytes()
                stale_payload = final_payload + b"# stale\n"
                self.assertEqual(canonical_text_sha256(final_payload), pinned[relative_path])
                self.assertNotEqual(canonical_text_sha256(stale_payload), pinned[relative_path])

    def test_package_install_and_post_install_checks_use_canonical_hash(self):
        calls = re.findall(r"Get-CanonicalTextSha256 \$[A-Za-z]+", self.script)
        self.assertEqual(
            calls,
            [
                "Get-CanonicalTextSha256 $path",
                "Get-CanonicalTextSha256 $temporary",
                "Get-CanonicalTextSha256 $target",
            ],
        )
        self.assertIn("before_sha256 = $beforeHash", self.script)
        self.assertIn("sha256 = Get-Sha256 $database", self.script)

    def test_backup_rollback_and_settings_guards_remain_present(self):
        required_fragments = (
            "strategy_validation_enabled -ne $false",
            "operational_alerts_enabled -ne $false",
            "Restore-Code $loadedManifest $TargetRoot",
            "Restore-Databases $manifest",
            'Join-Path $TargetRoot "_audit_update_backups"',
            "-ConfirmServicesStopped",
            "Assert-Backups $manifest $BackupPath $TargetRoot",
            "Rollback byte verification failed",
        )
        for fragment in required_fragments:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, self.script)

    def test_settings_are_preflight_only_and_never_packaged_or_overwritten(self):
        pinned = expected_files(self.script)
        self.assertNotIn("pnf_mvp/settings.json", pinned)
        self.assertNotIn("settings_backup", self.script)
        self.assertNotIn('Destination (Join-Path $Target "pnf_mvp\\settings.json")', self.script)
        self.assertIn("$targetSettings = Assert-SettingsOff $targetSettingsPath", self.script)
        self.assertIn("$settingsHash = Get-Sha256 $targetSettingsPath", self.script)
        self.assertIn("Operator settings changed during Apply", self.script)
        self.assertIn("Operator settings changed during Rollback", self.script)

    def test_snapshot_installed_with_exact_raw_sha_after_lf_normalization(self):
        self.assertIn("Write-CanonicalLfFile $temporary", self.script)
        self.assertIn("(Get-Sha256 $temporary) -ne $entry.Value", self.script)
        self.assertIn("(Get-CanonicalTextSha256 $target) -ne $entry.Value", self.script)

    def test_no_process_start_or_settings_rewrite(self):
        self.assertNotIn("Invoke-Python", self.script)
        self.assertNotIn("Start-Process", self.script)
        self.assertNotIn("Start-Service", self.script)
        self.assertNotIn("Set-Content -LiteralPath $targetSettingsPath", self.script)


if __name__ == "__main__":
    import unittest

    unittest.main()
