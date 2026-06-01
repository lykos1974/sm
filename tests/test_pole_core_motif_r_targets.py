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


def test_generates_required_r_excursion_outputs_with_reconstructed_geometry(tmp_path: Path) -> None:
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
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_r_targets", *symbol_args, *columns_args, "--output-root", str(output)], check=True)

    assert {path.name for path in output.iterdir()} == {
        "pole_core_motif_r_targets_summary.md", "pole_core_motif_r_target_curve.csv", "pole_core_motif_r_directional.csv",
        "pole_core_motif_r_symbol_breakdown.csv", "pole_core_motif_r_flags.csv",
    }
    curve = list(csv.DictReader((output / "pole_core_motif_r_target_curve.csv").open()))
    core = {(row["direction"], row["r_target"]): row for row in curve if row["scope"] == "core_motif"}
    assert core[("BOTH", "1.0")]["sample_size"] == "14"
    assert core[("BOTH", "1.0")]["hit_rate"] == "0.5"
    assert core[("BOTH", "1.0")]["stopped_before_target_rate"] == "0.5"
    assert core[("LONG", "3.0")]["hit_rate"] == "1.0"

    flags = list(csv.DictReader((output / "pole_core_motif_r_flags.csv").open()))
    assert any(row["check_name"] == "reconstructed_pullback_sl" and row["result"] == "OK" for row in flags)
    summary = (output / "pole_core_motif_r_targets_summary.md").read_text()
    assert "**R excursion study**, not a full execution backtest or trade simulation" in summary
    assert "- recommended TP2: **3R**" in summary
    assert "`NOT_COMPUTED_FOR_R_EXCURSION_STUDY`" in summary


def test_missing_columns_are_explicit_excursion_only_fallback(tmp_path: Path) -> None:
    symbol_args = []
    for symbol in EXPECTED_SYMBOLS:
        labels = tmp_path / f"{symbol}_labels.csv"
        _write_labels(labels)
        symbol_args.extend(["--symbol-input", f"{symbol}={labels}"])
    output = tmp_path / "output"
    subprocess.run([sys.executable, "-m", "research_v2.patterns.pole_core_motif_r_targets", *symbol_args, "--output-root", str(output)], check=True)
    flags = list(csv.DictReader((output / "pole_core_motif_r_flags.csv").open()))
    assert any(row["check_name"] == "columns_geometry" and row["result"] == "WARN" for row in flags)
    assert any(row["check_name"] == "excursion_only_fallback" and "retrace_boxes" in row["details"] for row in flags)
