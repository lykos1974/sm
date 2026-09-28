import hashlib
import subprocess
import tempfile
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]


class TickProvenanceSnapshotPortabilityTests(TestCase):
    def test_snapshot_bytes_and_sha_survive_windows_autocrlf(self):
        relative = Path("pnf_mvp/data/tick_provenance/snapshot.json")
        payload = b'{\n  "schema": "test-v1",\n  "tick_size": "0.1"\n}\n'
        expected_sha = hashlib.sha256(payload).hexdigest()

        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            (repo / ".gitattributes").write_bytes((ROOT / ".gitattributes").read_bytes())
            target = repo / relative
            target.parent.mkdir(parents=True)
            target.write_bytes(payload)
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(["git", "config", "core.autocrlf", "true"], cwd=repo, check=True)
            subprocess.run(["git", "add", ".gitattributes", str(relative)], cwd=repo, check=True)

            indexed = subprocess.check_output(["git", "show", f":{relative.as_posix()}"], cwd=repo)
            self.assertEqual(indexed, payload)
            self.assertEqual(hashlib.sha256(indexed).hexdigest(), expected_sha)

            target.unlink()
            subprocess.run(["git", "checkout-index", "--force", str(relative)], cwd=repo, check=True)
            checked_out = target.read_bytes()
            self.assertEqual(checked_out, payload)
            self.assertEqual(hashlib.sha256(checked_out).hexdigest(), expected_sha)


if __name__ == "__main__":
    import unittest

    unittest.main()
