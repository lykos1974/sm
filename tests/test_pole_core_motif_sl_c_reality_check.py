from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_sl_candidates import EXPECTED_SYMBOLS


def _write_labels(path: Path) -> None:
    fields = ["pattern_name", "pole_column_index", "reversal_column_index", "opposing_pole_distance_columns", "enhanced_by_opposing_pole", "fav_path", "adv_path"]
    rows = [
        {"pattern_name": "LOW_POLE", "pole_column_index": "0", "reversal_column_index": "1", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False", "fav_path": "3,6", "adv_path": "0,0"},
        {"pattern_name": "HIGH_POLE", "pole_column_index": "3", "reversal_column_index": "4", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False", "fav_path": "3,6", "adv_path": "0,0"},
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_columns(path: Path) -> None:
    fields = ["profile_name", "idx", "kind", "top", "bottom"]
    rows = [
        {"profile_name": "TEST_bs1_rev3", "idx": "0", "kind": "O", "top": "13", "bottom": "9"},
        {"profile_name": "TEST_bs1_rev3", "idx": "1", "kind": "X", "top": "15", "bottom": "10"},
        {"profile_name": "TEST_bs1_rev3", "idx": "2", "kind": "O", "top": "14", "bottom": "12"},
        {"profile_name": "TEST_bs1_rev3", "idx": "3", "kind": "X", "top": "20", "bottom": "16"},
        {"profile_name": "TEST_bs1_rev3", "idx": "4", "kind": "O", "top": "19", "bottom": "14"},
        {"profile_name": "TEST_bs1_rev3", "idx": "5", "kind": "X", "top": "17", "bottom": "15"},
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_generates_reality_outputs_and_breaks_optimistic_1r_result(tmp_path: Path) -> None:
    args: list[str] = []
    for symbol in EXPECTED_SYMBOLS:
        labels, columns = tmp_path / f"{symbol}_labels.csv", tmp_path / f"{symbol}_columns.csv"
        _write_labels(labels)
        _write_columns(columns)
        args.extend(["--symbol-input", f"{symbol}={labels}", "--columns-input", f"{symbol}={columns}"])
    output = tmp_path / "output"
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_sl_c_reality_check", *args, "--output-root", str(output)], check=True)
    assert {path.name for path in output.iterdir()} == {
        "sl_c_reality_summary.md", "sl_c_geometry.csv", "sl_c_symbol_breakdown.csv", "sl_c_stress_test.csv", "sl_c_flags.csv",
    }
    geometry = list(csv.DictReader((output / "sl_c_geometry.csv").open()))
    assert {row["stop_distance_boxes"] for row in geometry} == {"3.0"}
    assert all(row["pre_confirmation_stop_risk"] == "UNSAFE" for row in geometry)
    pooled = {row["r_target"]: row for row in csv.DictReader((output / "sl_c_stress_test.csv").open()) if row["symbol"] == "ALL"}
    assert pooled["1.0"]["original_hit_rate"] == "1.0"
    assert pooled["1.0"]["stress_hit_rate"] == "0.0"
    assert pooled["1.0"]["stress_stop_rate"] == "1.0"
    summary = (output / "sl_c_reality_summary.md").read_text()
    assert "LOOKAHEAD_FOUND = YES" in summary
    assert "SL-C likely excursion artifact: YES" in summary
    assert "No strategy promotion" in summary
