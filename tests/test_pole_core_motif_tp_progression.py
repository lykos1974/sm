from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_r_targets import EXPECTED_SYMBOLS


def _write_labels(path: Path) -> None:
    fields = [
        "pattern_name", "reversal_column_index", "opposing_pole_distance_columns", "enhanced_by_opposing_pole",
        "retrace_ratio", "retrace_boxes", "max_favorable_boxes", "max_adverse_boxes", "fav_path", "adv_path",
    ]
    rows = [
        {"pattern_name": "LOW_POLE", "reversal_column_index": "1", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False", "retrace_ratio": "1.2", "retrace_boxes": "3", "max_favorable_boxes": "9", "max_adverse_boxes": "0", "fav_path": "3,6,9", "adv_path": "0,0,0"},
        {"pattern_name": "HIGH_POLE", "reversal_column_index": "2", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False", "retrace_ratio": "0.8", "retrace_boxes": "3", "max_favorable_boxes": "3", "max_adverse_boxes": "3", "fav_path": "0,3", "adv_path": "3,3"},
        {"pattern_name": "LOW_POLE", "reversal_column_index": "1", "opposing_pole_distance_columns": "2", "enhanced_by_opposing_pole": "False", "retrace_ratio": "2.0", "retrace_boxes": "3", "max_favorable_boxes": "12", "max_adverse_boxes": "0", "fav_path": "12", "adv_path": "0"},
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_columns(path: Path) -> None:
    fields = ["profile_name", "idx", "kind", "top", "bottom"]
    rows = [
        {"profile_name": "TEST_bs1_rev3", "idx": "1", "kind": "X", "top": "12", "bottom": "10"},
        {"profile_name": "TEST_bs1_rev3", "idx": "2", "kind": "O", "top": "12", "bottom": "10"},
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_generates_required_conditional_progression_outputs(tmp_path: Path) -> None:
    symbol_args = []
    columns_args = []
    for symbol in EXPECTED_SYMBOLS:
        labels = tmp_path / f"{symbol}_labels.csv"
        columns = tmp_path / f"{symbol}_columns.csv"
        _write_labels(labels)
        _write_columns(columns)
        symbol_args.extend(["--symbol-input", f"{symbol}={labels}"])
        columns_args.extend(["--columns-input", f"{symbol}={columns}"])
    output = tmp_path / "output"
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_tp_progression", *symbol_args, *columns_args, "--output-root", str(output)], check=True)

    assert {path.name for path in output.iterdir()} == {
        "tp_progression_summary.md", "tp_progression_matrix.csv", "tp_progression_by_symbol.csv", "tp_progression_directional.csv",
    }
    matrix = list(csv.DictReader((output / "tp_progression_matrix.csv").open()))
    pooled = {(row["from_r"], row["to_r"]): row for row in matrix if row["direction"] == "BOTH"}
    assert pooled[("1.0", "2.5")]["eligible_rows"] == "7"
    assert pooled[("1.0", "2.5")]["reached_rows"] == "7"
    assert pooled[("1.5", "2.5")]["conditional_probability"] == "1.0"

    directional = list(csv.DictReader((output / "tp_progression_directional.csv").open()))
    milestone = {(row["direction"], row["milestone_r"]): row for row in directional}
    assert milestone[("BOTH", "1.0")]["sample_size"] == "14"
    assert milestone[("BOTH", "1.0")]["reached_rows"] == "7"
    assert milestone[("LONG", "3.0")]["reach_probability"] == "1.0"

    by_symbol = list(csv.DictReader((output / "tp_progression_by_symbol.csv").open()))
    assert len(by_symbol) == len(EXPECTED_SYMBOLS) * 3 * 9
    summary = (output / "tp_progression_summary.md").read_text()
    assert "best TP1 level: **1.5R**" in summary
    assert "whether BE-after-1R is supported: **NOT ESTABLISHED**" in summary
    assert "whether runner-to-2.5R is supported: **SUPPORTED**" in summary
    assert "No strategy logic, execution logic, TP/SL logic, database, or existing pattern definition is changed." in summary
