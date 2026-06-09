import csv
import subprocess
import sys
from pathlib import Path

from research_v2.structure_validation.structural_reaction_ratio_audit import (
    OUTPUT_NAMES,
    build_reaction_rows,
    load_confirmed_swings,
    run_audit,
)


def _write_swings(path: Path, rows: list[dict[str, object]]) -> None:
    fields = [
        "symbol",
        "swing_id",
        "direction",
        "swing_boxes",
        "start_ts",
        "end_ts",
        "status",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_no_unfinished_swing_is_included(tmp_path: Path) -> None:
    swings_csv = tmp_path / "swings.csv"
    _write_swings(
        swings_csv,
        [
            {
                "symbol": "ETH",
                "swing_id": "s1",
                "direction": "UP",
                "swing_boxes": 20,
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T00:20:00Z",
                "status": "CONFIRMED",
            },
            {
                "symbol": "ETH",
                "swing_id": "s2",
                "direction": "DOWN",
                "swing_boxes": 5,
                "start_ts": "2024-01-01T00:20:00Z",
                "end_ts": "2024-01-01T00:25:00Z",
                "status": "CONFIRMED",
            },
            {
                "symbol": "ETH",
                "swing_id": "s3",
                "direction": "UP",
                "swing_boxes": 8,
                "start_ts": "2024-01-01T00:25:00Z",
                "end_ts": "2024-01-01T00:33:00Z",
                "status": "UNFINISHED",
            },
        ],
    )

    swings = load_confirmed_swings(swings_csv)
    rows = build_reaction_rows(swings)

    assert [swing.swing_id for swing in swings] == ["s1", "s2"]
    assert len(rows) == 1
    assert rows[0]["reaction_swing_id"] == "s2"
    assert "s3" not in {rows[0]["prior_swing_id"], rows[0]["reaction_swing_id"]}


def test_ratio_calculation_is_exact_for_five_over_twenty(tmp_path: Path) -> None:
    swings_csv = tmp_path / "swings.csv"
    out = tmp_path / "out"
    _write_swings(
        swings_csv,
        [
            {
                "symbol": "BTC",
                "swing_id": "prior",
                "direction": "UP",
                "swing_boxes": 20,
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T00:20:00Z",
                "status": "CONFIRMED",
            },
            {
                "symbol": "BTC",
                "swing_id": "reaction",
                "direction": "DOWN",
                "swing_boxes": 5,
                "start_ts": "2024-01-01T00:20:00Z",
                "end_ts": "2024-01-01T00:25:00Z",
                "status": "CONFIRMED",
            },
        ],
    )

    run_audit(swings_csv, out)
    rows = _read_csv(out / "structural_reaction_ratios.csv")

    assert len(rows) == 1
    assert rows[0]["prior_swing_boxes"] == "20"
    assert rows[0]["reaction_boxes"] == "5"
    assert float(rows[0]["reaction_ratio"]) == 0.25
    assert rows[0]["reaction_ratio"] == "0.25"


def test_distribution_bucket_counts_equal_total_observations(tmp_path: Path) -> None:
    swings_csv = tmp_path / "swings.csv"
    out = tmp_path / "out"
    _write_swings(
        swings_csv,
        [
            {
                "symbol": "SOL",
                "swing_id": "s1",
                "direction": "UP",
                "swing_boxes": 10,
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T00:10:00Z",
                "status": "CONFIRMED",
            },
            {
                "symbol": "SOL",
                "swing_id": "s2",
                "direction": "DOWN",
                "swing_boxes": 1,
                "start_ts": "2024-01-01T00:10:00Z",
                "end_ts": "2024-01-01T00:11:00Z",
                "status": "CONFIRMED",
            },
            {
                "symbol": "SOL",
                "swing_id": "s3",
                "direction": "UP",
                "swing_boxes": 2,
                "start_ts": "2024-01-01T00:11:00Z",
                "end_ts": "2024-01-01T00:13:00Z",
                "status": "CONFIRMED",
            },
            {
                "symbol": "SOL",
                "swing_id": "s4",
                "direction": "DOWN",
                "swing_boxes": 4,
                "start_ts": "2024-01-01T00:13:00Z",
                "end_ts": "2024-01-01T00:17:00Z",
                "status": "CONFIRMED",
            },
        ],
    )

    run_audit(swings_csv, out)
    total_observations = len(_read_csv(out / "structural_reaction_ratios.csv"))
    distribution_total = sum(
        int(row["count"]) for row in _read_csv(out / "reaction_ratio_distribution.csv")
    )

    assert total_observations == 3
    assert distribution_total == total_observations


def test_all_output_artifacts_exist_via_cli(tmp_path: Path) -> None:
    swings_csv = tmp_path / "swings.csv"
    out = tmp_path / "out"
    _write_swings(
        swings_csv,
        [
            {
                "symbol": "ETH",
                "swing_id": "s1",
                "direction": "UP",
                "swing_boxes": 20,
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T00:20:00Z",
                "status": "CONFIRMED",
            },
            {
                "symbol": "ETH",
                "swing_id": "s2",
                "direction": "DOWN",
                "swing_boxes": 5,
                "start_ts": "2024-01-01T00:20:00Z",
                "end_ts": "2024-01-01T00:25:00Z",
                "status": "CONFIRMED",
            },
            {
                "symbol": "BTC",
                "swing_id": "b1",
                "direction": "DOWN",
                "swing_boxes": 12,
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T00:12:00Z",
                "status": "CONFIRMED",
            },
            {
                "symbol": "BTC",
                "swing_id": "b2",
                "direction": "UP",
                "swing_boxes": 9,
                "start_ts": "2024-01-01T00:12:00Z",
                "end_ts": "2024-01-01T00:21:00Z",
                "status": "CONFIRMED",
            },
        ],
    )

    subprocess.run(
        [
            sys.executable,
            "-m",
            "research_v2.structure_validation.structural_reaction_ratio_audit",
            "--swings-input",
            str(swings_csv),
            "--output-root",
            str(out),
        ],
        check=True,
    )

    assert {path.name for path in out.iterdir()} == set(OUTPUT_NAMES)
    summary = _read_csv(out / "reaction_ratio_summary.csv")
    by_symbol = _read_csv(out / "reaction_ratio_by_symbol.csv")

    assert summary[0]["count"] == "2"
    assert {row["symbol"] for row in by_symbol} == {"BTC", "ETH"}
