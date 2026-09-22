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
            r'^\s*"([^"]+\.py)"\s*=\s*"([0-9a-f]{64})"$', script, re.MULTILINE
        )
    )


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
        self.assertEqual(len(pinned), 6)
        for relative_path, expected in pinned.items():
            with self.subTest(relative_path=relative_path):
                payload = (ROOT / relative_path).read_bytes()
                lf_payload = payload.replace(b"\r\n", b"\n")
                crlf_payload = lf_payload.replace(b"\n", b"\r\n")
                self.assertEqual(canonical_text_sha256(lf_payload), expected)
                self.assertEqual(canonical_text_sha256(crlf_payload), expected)

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
        )
        for fragment in required_fragments:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, self.script)


if __name__ == "__main__":
    import unittest

    unittest.main()
