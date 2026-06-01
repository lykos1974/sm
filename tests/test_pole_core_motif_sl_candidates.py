from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_sl_candidates import EXPECTED_SYMBOLS


def _write_labels(path: Path) -> None:
    fields = [
        "pattern_name", "pole_column_index", "reversal_column_index", "opposing_pole_partner_index",
        "opposing_pole_distance_columns", "enhanced_by_opposing_pole", "retrace_ratio", "fav_path", "adv_path",
    ]
    rows = [
        {"pattern_name": "LOW_POLE", "pole_column_index": "3", "reversal_column_index": "4", "opposing_pole_partner_index": "0", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False", "retrace_ratio": "1.2", "fav_path": "4,8,12", "adv_path": "0,0,0"},
        {"pattern_name": "HIGH_POLE", "pole_column_index": "6", "reversal_column_index": "7", "opposing_pole_partner_index": "9", "opposing_pole_distance_columns": "3", "enhanced_by_opposing_pole": "False", "retrace_ratio": "0.8", "fav_path": "0,3,3", "adv_path": "3,3,6"},
        {"pattern_name": "LOW_POLE", "pole_column_index": "3", "reversal_column_index": "4", "opposing_pole_partner_index": "0", "opposing_pole_distance_columns": "2", "enhanced_by_opposing_pole": "False", "retrace_ratio": "2", "fav_path": "12", "adv_path": "0"},
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_columns(path: Path) -> None:
    fields = ["profile_name", "idx", "kind", "top", "bottom"]
    rows = [
        {"profile_name": "TEST_bs1_rev3", "idx": "0", "kind": "O", "top": "13", "bottom": "9"},
        {"profile_name": "TEST_bs1_rev3", "idx": "3", "kind": "O", "top": "14", "bottom": "10"},
        {"profile_name": "TEST_bs1_rev3", "idx": "4", "kind": "X", "top": "14", "bottom": "11"},
        {"profile_name": "TEST_bs1_rev3", "idx": "6", "kind": "X", "top": "20", "bottom": "16"},
        {"profile_name": "TEST_bs1_rev3", "idx": "7", "kind": "O", "top": "19", "bottom": "16"},
        {"profile_name": "TEST_bs1_rev3", "idx": "9", "kind": "O", "top": "18", "bottom": "14"},
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _run(tmp_path: Path, *, with_columns: bool = True) -> Path:
    symbol_args: list[str] = []
    columns_args: list[str] = []
    for symbol in EXPECTED_SYMBOLS:
        labels = tmp_path / f"{symbol}_labels.csv"
        _write_labels(labels)
        symbol_args.extend(["--symbol-input", f"{symbol}={labels}"])
        if with_columns:
            columns = tmp_path / f"{symbol}_columns.csv"
            _write_columns(columns)
            columns_args.extend(["--columns-input", f"{symbol}={columns}"])
    output = tmp_path / "output"
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_sl_candidates", *symbol_args, *columns_args, "--output-root", str(output)], check=True)
    return output


def test_generates_required_outputs_and_refuses_future_sl_b_geometry(tmp_path: Path) -> None:
    output = _run(tmp_path)
    assert {path.name for path in output.iterdir()} == {
        "sl_candidate_summary.md", "sl_candidate_curve.csv", "sl_candidate_symbol_breakdown.csv",
        "sl_candidate_directional.csv", "sl_candidate_flags.csv",
    }
    curve = list(csv.DictReader((output / "sl_candidate_curve.csv").open()))
    pooled = {(row["sl_candidate"], row["r_target"]): row for row in curve if row["scope"] == "core_motif" and row["direction"] == "BOTH"}
    assert pooled[("SL-A", "1.0")]["valid_reconstructed_rows"] == "14"
    assert pooled[("SL-B", "1.0")]["valid_reconstructed_rows"] == "7"
    assert pooled[("SL-C", "1.0")]["valid_reconstructed_rows"] == "14"
    assert pooled[("SL-A", "1.0")]["average_r_distance_boxes"] == "5.0"
    flags = list(csv.DictReader((output / "sl_candidate_flags.csv").open()))
    assert any(row["sl_candidate"] == "SL-B" and "refusing lookahead geometry" in row["details"] for row in flags)
    summary = (output / "sl_candidate_summary.md").read_text()
    assert "Research-only SL hypothesis audit" in summary
    assert "does not promote any stop to production" in summary
    assert "NOT_COMPUTED_FOR_STRUCTURAL_SL_AUDIT" in summary


def test_missing_columns_excludes_structural_candidates_but_keeps_fixed_benchmark(tmp_path: Path) -> None:
    output = _run(tmp_path, with_columns=False)
    curve = list(csv.DictReader((output / "sl_candidate_curve.csv").open()))
    pooled = {(row["sl_candidate"], row["r_target"]): row for row in curve if row["scope"] == "core_motif" and row["direction"] == "BOTH"}
    assert pooled[("SL-A", "1.0")]["valid_reconstructed_rows"] == "0"
    assert pooled[("SL-B", "1.0")]["valid_reconstructed_rows"] == "0"
    assert pooled[("SL-C", "1.0")]["valid_reconstructed_rows"] == "14"
    flags = list(csv.DictReader((output / "sl_candidate_flags.csv").open()))
    assert any(row["check_name"] == "columns_geometry" and row["result"] == "WARN" for row in flags)
