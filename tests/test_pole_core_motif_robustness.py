from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

from research_v2.patterns.pole_core_motif_robustness import EXPECTED_SYMBOLS


def _write_symbol(path: Path, *, supportive: bool = True) -> None:
    fields = [
        "pattern_name",
        "outcome_class",
        "opposing_pole_distance_columns",
        "enhanced_by_opposing_pole",
        "pole_boxes",
        "max_favorable_boxes",
        "max_adverse_boxes",
    ]
    rows = []
    for pattern_name in ("LOW_POLE", "HIGH_POLE"):
        continuation = "BULLISH_CONTINUATION" if pattern_name == "LOW_POLE" else "BEARISH_CONTINUATION"
        for pole_boxes in (8, 12, 20):
            rows.extend(
                [
                    {
                        "pattern_name": pattern_name,
                        "outcome_class": continuation if supportive else "FAILED_REVERSAL",
                        "opposing_pole_distance_columns": "3",
                        "enhanced_by_opposing_pole": "False",
                        "pole_boxes": str(pole_boxes),
                        "max_favorable_boxes": "5" if supportive else "1",
                        "max_adverse_boxes": "1" if supportive else "5",
                    },
                    {
                        "pattern_name": pattern_name,
                        "outcome_class": "FAILED_REVERSAL" if supportive else continuation,
                        "opposing_pole_distance_columns": "2",
                        "enhanced_by_opposing_pole": "False",
                        "pole_boxes": str(pole_boxes),
                        "max_favorable_boxes": "1" if supportive else "5",
                        "max_adverse_boxes": "5" if supportive else "1",
                    },
                ]
            )
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_audit_outputs_rankings_directions_sizes_and_ignores_retrace_ratio(tmp_path: Path) -> None:
    inputs = []
    for symbol in EXPECTED_SYMBOLS:
        path = tmp_path / f"{symbol}.csv"
        _write_symbol(path, supportive=symbol != "HYPE")
        inputs.extend(["--symbol-input", f"{symbol}={path}"])
    output = tmp_path / "output"
    subprocess.run(
        [sys.executable, "-m", "research_v2.patterns.pole_core_motif_robustness", *inputs, "--output-root", str(output), "--min-sample", "2"],
        check=True,
    )

    assert {path.name for path in output.iterdir()} == {
        "robustness_summary.md",
        "robustness_symbol_rankings.csv",
        "robustness_directional.csv",
        "robustness_size_buckets.csv",
    }
    rankings = list(csv.DictReader((output / "robustness_symbol_rankings.csv").open()))
    assert rankings[0]["symbol"] == "BTC"
    assert rankings[-1]["symbol"] == "HYPE"
    assert float(rankings[0]["edge_lift"]) > 0
    assert float(rankings[-1]["edge_lift"]) < 0

    directional = list(csv.DictReader((output / "robustness_directional.csv").open()))
    pooled = {(row["symbol"], row["direction"]): row for row in directional}
    assert pooled[("ALL", "LONG")]["support_status"] == "SUPPORTS"
    assert pooled[("ALL", "SHORT")]["support_status"] == "SUPPORTS"

    sizes = list(csv.DictReader((output / "robustness_size_buckets.csv").open()))
    pooled_sizes = {row["pole_size_bucket"]: row for row in sizes if row["symbol"] == "ALL"}
    assert set(pooled_sizes) == {"small", "medium", "large"}
    assert all(row["support_status"] == "SUPPORTS" for row in pooled_sizes.values())

    summary = (output / "robustness_summary.md").read_text()
    assert "`retrace_ratio` is ignored completely" in summary
    assert "- robust structural law: **NO**" in summary
    assert "- symbols contradicting it: **HYPE**" in summary
    assert "`candidate_rows_registered`: `NOT_COMPUTED" in summary
