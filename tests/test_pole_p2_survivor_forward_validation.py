from __future__ import annotations

import ast
import csv
import json
import subprocess
import sys
from pathlib import Path

import pytest

import research_v2.patterns.pole_p2_survivor_forward_validation as forward_validation
from research_v2.patterns.pole_p2_survivor_forward_validation import (
    FROZEN_RULE_DEFINITION,
    OUTPUT_NAMES,
    TARGET_CANDIDATE_ID,
    _frozen_rule,
)


def _write_forward_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    labels = tmp_path / "labels.csv"
    labels.write_text(
        "pattern_name,pole_column_index,reversal_column_index,retrace_ratio,market_regime,breakout_context,opposing_pole_distance_columns,enhanced_by_opposing_pole\n"
        "LOW_POLE,10,11,0.8,TREND,POST_BREAKOUT,999,true\n"
        "LOW_POLE,20,21,0.8,TREND,POST_BREAKOUT,999,true\n"
        "LOW_POLE,30,31,0.8,TREND,POST_BREAKOUT,999,true\n"
        "LOW_POLE,40,41,0.8,TREND,POST_BREAKOUT,999,true\n"
    )
    rows = ["idx,kind,top,bottom,start_ts,end_ts,profile_name"]
    for idx in range(0, 45):
        if idx in {10, 20, 30, 40}:
            start = {10: 1704067200, 20: 1711929600, 30: 1719792000, 40: 1727740800}[idx]
            rows.append(f"{idx},O,100,95,{start},{start + 10},TEST_bs1_rev3")
        elif idx in {11, 21, 31, 41}:
            start = {11: 1704067211, 21: 1711929611, 31: 1719792011, 41: 1727740811}[idx]
            rows.append(f"{idx},X,100,96,{start},{start + 10},TEST_bs1_rev3")
        elif idx in {12, 22, 32, 42}:
            start = {12: 1704067222, 22: 1711929622, 32: 1719792022, 42: 1727740822}[idx]
            rows.append(f"{idx},O,100,95,{start},{start + 10},TEST_bs1_rev3")
        else:
            # Keep a recent history of similarly-sized columns so relative_pole_size buckets to NEAR_RECENT_AVG_0_75X_1_25X.
            kind = "X" if idx % 2 else "O"
            start = 1700000000 + idx * 100
            rows.append(f"{idx},{kind},100,95,{start},{start + 10},TEST_bs1_rev3")
    columns = tmp_path / "columns.csv"
    columns.write_text("\n".join(rows) + "\n")
    candles = tmp_path / "candles.csv"
    candles.write_text(
        "close_time,open,high,low,close\n"
        "1704067233,102,120,103,116\n"
        "1711929633,102,120,103,116\n"
        "1719792033,102,120,103,116\n"
        "1727740833,100,101,94,95\n"
    )
    return labels, columns, candles


def test_frozen_rule_is_exact_cand_000053_definition() -> None:
    rule = _frozen_rule()

    assert rule.candidate_id == TARGET_CANDIDATE_ID
    assert rule.rule_definition == FROZEN_RULE_DEFINITION
    assert rule.predicates == (
        ("direction", frozenset({"LONG"})),
        ("relative_pole_size", frozenset({"NEAR_RECENT_AVG_0_75X_1_25X"})),
        ("reversal_boxes", frozenset({"NORMAL_REVERSAL_4_6_BOXES"})),
    )


def test_cli_emits_shadow_forward_outputs(tmp_path: Path) -> None:
    labels, columns, candles = _write_forward_fixture(tmp_path)
    output = tmp_path / "forward"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.patterns.pole_p2_survivor_forward_validation",
            "--symbol-input",
            f"ENA={labels}",
            "--columns-input",
            f"ENA={columns}",
            "--candles-input",
            f"ENA={candles}",
            "--output-root",
            str(output),
            "--allow-partial-universe",
        ],
        check=True,
    )

    assert set(OUTPUT_NAMES) == {path.name for path in output.iterdir()}

    windows = list(csv.DictReader((output / "p2_survivor_forward_validation_windows.csv").open()))
    assert [row["forward_quarter"] for row in windows] == ["2024-Q3", "2024-Q4"]
    assert windows[0]["train_quarters"] == "2024-Q1;2024-Q2"
    assert {"trades", "wins", "losses", "break_even_exits", "win_rate", "expectancy", "total_R"} <= set(windows[0])

    metrics = {row["metric"]: row["value"] for row in csv.DictReader((output / "p2_survivor_forward_validation_metrics.csv").open())}
    assert metrics["total_trades"] == "2"
    assert metrics["positive_window_count"] == "1"
    assert metrics["negative_window_count"] == "1"
    assert "expectancy_dispersion" in metrics
    assert "max_drawdown_R" in metrics
    assert {row["forward_quarter"] for row in windows} == {"2024-Q3", "2024-Q4"}

    summary = (output / "p2_survivor_forward_validation_summary.md").read_text()
    assert "CAND-000053 shadow forward validation" in summary
    assert "No optimization" in summary
    assert "candidate_fixed_from_beginning_to_end" in summary

    manifest = json.loads((output / "p2_survivor_forward_validation_manifest.json").read_text())
    assert manifest["research_only"] is True
    assert manifest["scope"] == "shadow_forward_validation_only"
    assert manifest["candidate_id"] == TARGET_CANDIDATE_ID
    assert manifest["candidate_modifications"] is False
    assert manifest["new_filters"] is False
    assert manifest["production_modifications"] is False
    assert manifest["live_trader_modifications"] is False
    assert manifest["detector_modifications"] is False
    assert manifest["strategy_modifications"] is False
    assert manifest["strategy_promotion"] is False
    assert manifest["genetic_algorithm"] is False
    assert manifest["optimization"] is False
    assert manifest["parameter_search"] is False
    assert manifest["chronological_evaluation_only"] is True
    assert manifest["initial_train_quarters"] == 2
    assert manifest["artifact_write_completed"] is True
    assert manifest["artifact_publish_mode"] == "staged_directory_replace"
    assert manifest["complete_artifact_set"] == list(OUTPUT_NAMES)


def test_artifact_writers_use_utf8_for_windows_unicode_text(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original_open = Path.open

    def windows_cp1252_default_open(
        self: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> object:
        if "b" not in mode and any(flag in mode for flag in ("w", "a", "x")) and encoding is None:
            encoding = "cp1252"
        return original_open(self, mode, buffering, encoding, errors, newline)

    monkeypatch.setattr(Path, "open", windows_cp1252_default_open)

    summary = tmp_path / "summary.md"
    forward_validation._write_summary(
        summary,
        [
            {
                "window_id": "WF-001",
                "train_start_quarter": "2024-Q1",
                "train_end_quarter": "2024-Q2",
                "forward_quarter": "2024-Q3",
                "trades": 1,
                "wins": 1,
                "losses": 0,
                "break_even_exits": 0,
                "win_rate": 1.0,
                "expectancy": 1.0,
                "total_R": 1.0,
            }
        ],
        {},
        "FORWARD_EDGE_SURVIVES",
        ["2024-Q1", "2024-Q2", "2024-Q3"],
    )
    assert "→" in summary.read_bytes().decode("utf-8")

    csv_path = tmp_path / "unicode.csv"
    forward_validation._write_csv(csv_path, ["label"], [{"label": "train → forward"}])
    assert "train → forward" in csv_path.read_bytes().decode("utf-8")


def test_failed_staged_write_leaves_no_summary_only_partial_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    labels, columns, candles = _write_forward_fixture(tmp_path)
    output = tmp_path / "forward_failure"

    def fail_summary(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated summary write failure")

    monkeypatch.setattr(forward_validation, "_write_summary", fail_summary)

    with pytest.raises(RuntimeError, match="simulated summary write failure"):
        forward_validation.run(
            {"ENA": labels},
            {"ENA": columns},
            {"ENA": candles},
            output,
            require_full_universe=False,
        )

    assert not (output / "p2_survivor_forward_validation_summary.md").exists()
    assert not output.exists()
    assert not list(tmp_path.glob(".forward_failure.tmp-*"))


def test_no_production_changes_and_no_live_trader_imports() -> None:
    tree = ast.parse(Path("research_v2/patterns/pole_p2_survivor_forward_validation.py").read_text())
    imported_modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    assert all("live_" not in module and "forward_trader" not in module for module in imported_modules)
    assert all("pnf_mvp.strategy" not in module for module in imported_modules)
