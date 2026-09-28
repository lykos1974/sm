import hashlib
import json
import sys
import tempfile
from pathlib import Path
from unittest import TestCase, mock


ROOT = Path(__file__).resolve().parents[1]
PNF_ROOT = ROOT / "pnf_mvp"
if str(PNF_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_ROOT))

import validation_tick_provenance_preflight as preflight  # noqa: E402


class ValidationTickProvenancePreflightTests(TestCase):
    def test_committed_snapshot_is_exact_complete_and_read_only(self):
        settings_before = preflight.DEFAULT_SETTINGS.read_bytes()
        snapshot_before = preflight.DEFAULT_SNAPSHOT.read_bytes()
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "validation.sqlite3"
            result = preflight.load_validation_tick_provenance()

            self.assertEqual(result["sha256"], preflight.ACCEPTED_SNAPSHOT_SHA256)
            self.assertEqual(len(result["symbol_ticks"]), 12)
            self.assertEqual(set(result["symbol_ticks"]), set(result["symbol_identities"]))
            self.assertFalse(db_path.exists())
        settings = json.loads(settings_before)
        self.assertIs(settings["strategy_validation_enabled"], False)
        self.assertIs(settings["operational_alerts_enabled"], False)
        self.assertEqual(preflight.DEFAULT_SETTINGS.read_bytes(), settings_before)
        self.assertEqual(preflight.DEFAULT_SNAPSHOT.read_bytes(), snapshot_before)

    def test_rejects_hash_schema_identity_and_symbol_set_changes(self):
        accepted = json.loads(preflight.DEFAULT_SNAPSHOT.read_text(encoding="utf-8"))
        cases = []

        changed_tick = json.loads(json.dumps(accepted))
        changed_tick["symbols"][0]["tick_size"] = "0.02"
        cases.append((changed_tick, preflight.ACCEPTED_SNAPSHOT_SHA256))

        malformed_schema = json.loads(json.dumps(accepted))
        malformed_schema["schema"] = "unknown"
        cases.append((malformed_schema, None))

        duplicate = json.loads(json.dumps(accepted))
        duplicate["symbols"][1] = dict(duplicate["symbols"][0])
        cases.append((duplicate, None))

        unknown = json.loads(json.dumps(accepted))
        unknown["symbols"][0]["source_symbol"] = "UNKNOWN:FAKE"
        cases.append((unknown, None))

        missing = json.loads(json.dumps(accepted))
        missing["symbols"].pop()
        cases.append((missing, None))

        extra = json.loads(json.dumps(accepted))
        extra["symbols"].append(dict(extra["symbols"][0], source_symbol="EXTRAUSDT"))
        cases.append((extra, None))

        malformed = json.loads(json.dumps(accepted))
        malformed["symbols"][0]["tick_size"] = "NaN"
        cases.append((malformed, None))

        with tempfile.TemporaryDirectory() as temp_dir:
            for index, (payload, pinned_hash) in enumerate(cases):
                path = Path(temp_dir) / f"case-{index}.json"
                raw = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
                path.write_bytes(raw)
                digest = pinned_hash or hashlib.sha256(raw).hexdigest()
                with self.subTest(index=index), mock.patch.object(
                    preflight, "ACCEPTED_SNAPSHOT_SHA256", digest
                ), self.assertRaises(preflight.PreflightError):
                    preflight.load_validation_tick_provenance(snapshot_path=path)


if __name__ == "__main__":
    import unittest

    unittest.main()
