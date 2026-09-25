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
        self.assertEqual(source_commit, "367a328f929e367f602a57a0d0615fd5e074c67b")
        self.assertEqual(len(pinned), 14)
        self.assertEqual(pinned["mexc_readonly_order_discovery.py"], "dc730c6ae959452fa5c56e2d729ebe8caafe3c85b0af7aeda7956326ee496b4d")
        self.assertEqual(pinned["mexc_readonly_shadow_check.py"], "1de65e669d9fb1aa3ff99e23cd2cfef32363f708d26964bf86e431abaed99583")
        self.assertEqual(pinned["mexc_readonly_order_status.py"], "29e86f65cf40c4047d680b5c8a06701f23663612a187f613b0dec75514c4a1e6")
        self.assertEqual(pinned["live_mexc_forward_trader.py"], "844b60cfed374fc665fc91aad9524403536e13fc2229ba44e57a5391a3f71f48")
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

    def test_previous_package_hashes_are_preserved(self):
        previous = {
            "live_mexc_forward_trader.py": "844b60cfed374fc665fc91aad9524403536e13fc2229ba44e57a5391a3f71f48",
            "mexc_readonly_order_status.py": "29e86f65cf40c4047d680b5c8a06701f23663612a187f613b0dec75514c4a1e6",
            "mexc_readonly_shadow_check.py": "1de65e669d9fb1aa3ff99e23cd2cfef32363f708d26964bf86e431abaed99583",
            "pnf_mvp/app.py": "e6e4686922e38a0c5588c3b44a22be93e751384232740b9fb435cc7cd5b7a6c9",
            "pnf_mvp/pnf_engine.py": "b561484175655da7b2327f5fea24f930ff5eeced7fb0d2344dc5e258894d4e9e",
            "pnf_mvp/storage.py": "7456e19757c557607f5985461f5b34baa628a2069560ded3ac72f276553d7f6e",
            "pnf_mvp/strategy_historical_backfill.py": "14c58e6983b74c7c1fee9e7f6817438fb48a54964552f566b930ea70319963fd",
            "pnf_mvp/strategy_setup_observer.py": "60ae21e82266b9c371261dbcb83634d301447224b8bf66ab82b6a04ccdab5289",
            "pnf_mvp/strategy_validation.py": "089b847ac55feacb8537b48ab3df90d2eed7768ea51874ddf1d866c4c290f3d2",
            "pnf_mvp/strategy_trade_export.py": "6b039c473c31453d4afeb185d9e7c15050387f2a2eca5d61fd7d6a4501499383",
            "pnf_mvp/strategy_evaluator.py": "37bf22f9bf42fbd8546b8aaddfe1b91ae3780371550736d63c75d894b331c467",
            "pnf_mvp/validation_tick_provenance_preflight.py": "d16c7ab4a755dd60b1a8dac60d30510fb1ccc1d4d5ec62e1b93e37d1589b0e7c",
            "pnf_mvp/data/tick_provenance/strategy_validation_tick_provenance.json": "8ad27ceb2d89d2e9ab57b954240189982fdbaa5ceb02f99d474f540ea2dfa562",
        }
        pinned = expected_files(self.script)
        self.assertEqual({key: pinned[key] for key in previous}, previous)
        self.assertEqual(set(pinned) - set(previous), {"mexc_readonly_order_discovery.py"})

    def test_discovery_source_is_canonical_and_stale_or_modified_bytes_are_rejected(self):
        expected = expected_files(self.script)["mexc_readonly_order_discovery.py"]
        payload = (ROOT / "mexc_readonly_order_discovery.py").read_bytes()
        self.assertEqual(canonical_text_sha256(payload), expected)
        self.assertEqual(canonical_text_sha256(payload.replace(b"\n", b"\r\n")), expected)
        self.assertNotEqual(canonical_text_sha256(payload + b"# altered\n"), expected)
        self.assertIn(b"key + timestamp + parameters", payload)
        stale = payload.replace(b"key + timestamp + parameters", b"key + timestamp")
        self.assertNotEqual(canonical_text_sha256(stale), expected)

    def test_shadow_source_is_canonical_and_stale_or_altered_bytes_are_rejected(self):
        expected = expected_files(self.script)["mexc_readonly_shadow_check.py"]
        payload = (ROOT / "mexc_readonly_shadow_check.py").read_bytes()
        self.assertEqual(canonical_text_sha256(payload), expected)
        self.assertEqual(canonical_text_sha256(payload.replace(b"\n", b"\r\n")), expected)
        self.assertNotEqual(canonical_text_sha256(payload + b"# altered\n"), expected)
        self.assertNotEqual(canonical_text_sha256(payload.replace(b"except Exception:", b"except OSError:")), expected)

    def test_adapter_source_is_canonical_and_tampering_is_rejected(self):
        expected = expected_files(self.script)["mexc_readonly_order_status.py"]
        payload = (ROOT / "mexc_readonly_order_status.py").read_bytes()
        self.assertEqual(canonical_text_sha256(payload), expected)
        self.assertEqual(canonical_text_sha256(payload.replace(b"\n", b"\r\n")), expected)
        self.assertNotEqual(canonical_text_sha256(payload + b"# altered\n"), expected)

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
        self.assertIn('if ($RestoreDatabases) {\n        Restore-Databases $manifest', self.script)


if __name__ == "__main__":
    import unittest

    unittest.main()
