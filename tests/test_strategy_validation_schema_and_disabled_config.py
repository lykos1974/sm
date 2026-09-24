import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest import TestCase, mock


REPO_ROOT = Path(__file__).resolve().parents[1]
PNF_MVP_ROOT = REPO_ROOT / "pnf_mvp"
if str(PNF_MVP_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_MVP_ROOT))

import app as app_module  # noqa: E402
from strategy_validation import StrategyValidationStore  # noqa: E402


PREVIOUS_SCHEMA = """
CREATE TABLE strategy_setups (
    setup_id TEXT PRIMARY KEY,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    strategy TEXT NOT NULL,
    side TEXT NOT NULL,
    status TEXT NOT NULL,
    reference_ts INTEGER NOT NULL,
    bars_observed INTEGER NOT NULL DEFAULT 0,
    activation_status TEXT NOT NULL DEFAULT 'PENDING',
    activation_tick_size REAL,
    activation_tick_source TEXT,
    resolution_status TEXT NOT NULL
)
"""


LEGACY_WITHOUT_ACTIVATION = """
CREATE TABLE strategy_setups (
    setup_id TEXT PRIMARY KEY,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    strategy TEXT NOT NULL,
    side TEXT NOT NULL,
    status TEXT NOT NULL,
    reference_ts INTEGER NOT NULL,
    bars_observed INTEGER NOT NULL DEFAULT 0,
    resolution_status TEXT NOT NULL
)
"""


def create_legacy_db(path, schema):
    with sqlite3.connect(path) as conn:
        conn.execute(schema)
        conn.execute(
            """
            INSERT INTO strategy_setups (
                setup_id, created_ts, updated_ts, symbol, strategy, side,
                status, reference_ts, bars_observed, resolution_status
            ) VALUES ('legacy-1', 1, 1, 'BTCUSDT', 'pullback_retest',
                      'LONG', 'CANDIDATE', 1, 0, 'PENDING')
            """
        )


def columns(path):
    with sqlite3.connect(path) as conn:
        return {row[1] for row in conn.execute("PRAGMA table_info(strategy_setups)")}


def integrity(path):
    with sqlite3.connect(path) as conn:
        return conn.execute("PRAGMA integrity_check").fetchone()[0]


class SchemaMigrationOrderingTests(TestCase):
    def _open_and_close(self, db_path):
        store = StrategyValidationStore(str(db_path), symbol_tick_provenance={})
        store.flush()
        store._conn.close()

    def test_empty_database_migrates_and_repeated_startup_is_integral(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "empty.db"
            self._open_and_close(db_path)
            self._open_and_close(db_path)
            migrated = columns(db_path)
            for required in (
                "last_evaluated_candle_ts",
                "validation_state_version",
                "branch_active",
                "activation_tick_provider",
                "activation_tick_venue",
                "activation_tick_instrument_type",
                "activation_tick_native_symbol",
                "activation_tick_source_symbol",
                "activation_tick_provenance_timestamp",
                "activation_tick_provenance_version",
            ):
                self.assertIn(required, migrated)
            with sqlite3.connect(db_path) as conn:
                tables = {
                    row[0] for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            self.assertIn("strategy_setup_branches", tables)
            self.assertEqual(integrity(db_path), "ok")

    def test_immediately_previous_schema_preserves_rows_and_adds_watermark(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "previous.db"
            create_legacy_db(db_path, PREVIOUS_SCHEMA)
            self._open_and_close(db_path)
            self._open_and_close(db_path)
            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM strategy_setups WHERE setup_id='legacy-1'"
                ).fetchone()[0]
            self.assertEqual(count, 1)
            self.assertIn("last_evaluated_candle_ts", columns(db_path))
            self.assertEqual(integrity(db_path), "ok")

    def test_legacy_table_missing_activation_status_migrates_before_index(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "legacy.db"
            create_legacy_db(db_path, LEGACY_WITHOUT_ACTIVATION)
            self._open_and_close(db_path)
            self._open_and_close(db_path)
            migrated = columns(db_path)
            self.assertIn("activation_status", migrated)
            self.assertIn("last_evaluated_candle_ts", migrated)
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT setup_id, activation_status FROM strategy_setups"
                ).fetchone()
                indexes = {
                    item[1] for item in conn.execute("PRAGMA index_list(strategy_setups)")
                }
            self.assertEqual(row, ("legacy-1", "PENDING"))
            self.assertIn("idx_strategy_setups_activation", indexes)
            self.assertEqual(integrity(db_path), "ok")


class DisabledValidationConfigurationTests(TestCase):
    def test_off_configuration_is_not_parsed_or_dereferenced(self):
        malformed_values = (
            "ABSENT",
            None,
            "malformed",
            42,
            ["not", "a", "mapping"],
            {"symbol_ticks": None},
        )
        for value in malformed_values:
            with self.subTest(value=value):
                settings = {
                    "strategy_validation_enabled": False,
                    "strategy_validation_db_path": "must-not-open.db",
                    "symbols": ["BTCUSDT"],
                }
                if value != "ABSENT":
                    settings["strategy_validation_execution"] = value
                with mock.patch.object(
                    app_module,
                    "StrategyValidationStore",
                    side_effect=AssertionError("validation store parsed while OFF"),
                ):
                    self.assertIsNone(app_module._build_validation_store(settings))

    def test_repository_settings_keep_validation_and_alerts_off(self):
        settings = json.loads(
            (PNF_MVP_ROOT / "settings.json").read_text(encoding="utf-8")
        )
        self.assertIs(settings["strategy_validation_enabled"], False)
        self.assertIs(settings["operational_alerts_enabled"], False)


if __name__ == "__main__":
    import unittest

    unittest.main()
