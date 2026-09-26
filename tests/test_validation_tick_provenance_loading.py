import json
import sys
import tempfile
from pathlib import Path
from unittest import TestCase, mock


ROOT = Path(__file__).resolve().parents[1]
PNF_ROOT = ROOT / "pnf_mvp"
if str(PNF_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_ROOT))

import app as app_module  # noqa: E402
import validation_tick_provenance_preflight as preflight  # noqa: E402


def enabled_settings(snapshot=preflight.DEFAULT_SNAPSHOT):
    repository_settings = json.loads(preflight.DEFAULT_SETTINGS.read_text(encoding="utf-8"))
    return {
        "strategy_validation_enabled": True,
        "strategy_validation_db_path": "must-not-open.sqlite3",
        "strategy_validation_tick_provenance_snapshot": str(snapshot),
        "symbols": repository_settings["symbols"],
        "strategy_validation_execution": {
            "activation_model": app_module.HISTORICAL_ACTIVATION_MODEL,
        },
    }


class ValidationTickProvenanceLoadingTests(TestCase):
    def test_off_path_never_requires_or_reads_snapshot(self):
        settings = {
            "strategy_validation_enabled": False,
            "strategy_validation_tick_provenance_snapshot": "missing.json",
            "strategy_validation_execution": None,
        }
        with mock.patch.object(
            app_module,
            "load_validation_tick_provenance",
            side_effect=AssertionError("preflight called while validation is OFF"),
        ), mock.patch.object(
            app_module,
            "StrategyValidationStore",
            side_effect=AssertionError("DB store opened while validation is OFF"),
        ):
            self.assertIsNone(app_module._build_validation_store(settings))

    def test_pass_constructs_store_only_from_verified_twelve_symbol_maps(self):
        sentinel = object()
        with mock.patch.object(
            app_module, "StrategyValidationStore", return_value=sentinel
        ) as store:
            result = app_module._build_validation_store(enabled_settings())

        self.assertIs(result, sentinel)
        self.assertEqual(store.call_count, 1)
        kwargs = store.call_args.kwargs
        self.assertEqual(len(kwargs["symbol_tick_provenance"]), 12)
        self.assertEqual(
            set(kwargs["symbol_tick_provenance"]),
            set(kwargs["symbol_identity_allowlist"]),
        )

    def test_invalid_snapshots_fail_before_store_or_db_access(self):
        accepted = json.loads(preflight.DEFAULT_SNAPSHOT.read_text(encoding="utf-8"))
        cases = {"missing": None}

        changed = json.loads(json.dumps(accepted))
        changed["symbols"][0]["tick_size"] = "0.02"
        cases["changed"] = changed

        malformed = json.loads(json.dumps(accepted))
        malformed["symbols"][0].pop("provider")
        cases["malformed"] = malformed

        extra = json.loads(json.dumps(accepted))
        extra["symbols"].append(dict(extra["symbols"][0], source_symbol="EXTRAUSDT"))
        cases["extra"] = extra

        duplicate = json.loads(json.dumps(accepted))
        duplicate["symbols"][1] = dict(duplicate["symbols"][0])
        cases["duplicate"] = duplicate

        with tempfile.TemporaryDirectory() as temp_dir:
            for name, payload in cases.items():
                snapshot = Path(temp_dir) / f"{name}.json"
                if payload is not None:
                    snapshot.write_text(
                        json.dumps(payload, sort_keys=True, indent=2) + "\n",
                        encoding="utf-8",
                    )
                with self.subTest(name=name), mock.patch.object(
                    app_module,
                    "StrategyValidationStore",
                    side_effect=AssertionError("store opened before preflight PASS"),
                ), self.assertRaises(preflight.PreflightError):
                    app_module._build_validation_store(enabled_settings(snapshot))


if __name__ == "__main__":
    import unittest

    unittest.main()
