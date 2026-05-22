import csv
from pathlib import Path

from research_v2.optimizers.analyze_failure_reversal_followthrough import analyze_failure_reversal_followthrough


def _write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    fields = [
        "row_id", "symbol", "reference_ts", "side", "status", "strategy", "resolution_status", "breakout_context",
        "pullback_quality", "active_leg_boxes", "quality_score", "trend_regime", "continuation_execution_class", "late_extension", "realized_r_multiple",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_future_symbol_opposite_only_matching_and_counts(tmp_path: Path) -> None:
    dataset = tmp_path / "labeled.csv"
    rows = [
        {"row_id": "r1", "symbol": "ETH", "reference_ts": "2024-01-01T00:00:00", "side": "LONG", "status": "X", "strategy": "S", "resolution_status": "STOPPED", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "-1"},
        {"row_id": "r2", "symbol": "ETH", "reference_ts": "2024-01-01T00:01:00", "side": "LONG", "status": "X", "strategy": "S", "resolution_status": "TP2", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "71", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "1.2"},
        {"row_id": "r3", "symbol": "BTC", "reference_ts": "2024-01-01T00:02:00", "side": "SHORT", "status": "X", "strategy": "S", "resolution_status": "TP2", "breakout_context": "B", "pullback_quality": "HEALTHY", "active_leg_boxes": "3", "quality_score": "65", "trend_regime": "DOWN", "continuation_execution_class": "C2", "late_extension": "0", "realized_r_multiple": "1.5"},
        {"row_id": "r4", "symbol": "ETH", "reference_ts": "2024-01-01T00:03:00", "side": "SHORT", "status": "X", "strategy": "S", "resolution_status": "TP2", "breakout_context": "B", "pullback_quality": "HEALTHY", "active_leg_boxes": "3", "quality_score": "66", "trend_regime": "DOWN", "continuation_execution_class": "C2", "late_extension": "0", "realized_r_multiple": "2.0"},
        {"row_id": "r5", "symbol": "ETH", "reference_ts": "2024-01-01T00:04:00", "side": "SHORT", "status": "X", "strategy": "S", "resolution_status": "STOPPED", "breakout_context": "B", "pullback_quality": "HEALTHY", "active_leg_boxes": "2", "quality_score": "55", "trend_regime": "DOWN", "continuation_execution_class": "C1", "late_extension": "0", "realized_r_multiple": "-1.0"},
        {"row_id": "r6", "symbol": "ETH", "reference_ts": "2024-01-01T00:05:00", "side": "LONG", "status": "X", "strategy": "S", "resolution_status": "TP2", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "92", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "1.7"},
    ]
    _write_rows(dataset, rows)
    out = tmp_path / "out"
    analyze_failure_reversal_followthrough(labeled_dataset_path=str(dataset), output_root=str(out), forward_window_bars=3)

    seeds = list(csv.DictReader((out / "reversal_seed_rows.csv").open()))
    assert len(seeds) == 2
    assert seeds[0]["seed_row_identity"] == "row_id:r1"
    assert seeds[0]["reversal_found"] == "1"
    assert seeds[0]["reversal_side"] == "SHORT"
    assert seeds[0]["forward_distance_bars"] == "2"
    assert seeds[1]["seed_row_identity"] == "row_id:r5"
    assert seeds[1]["reversal_found"] == "1"
    assert seeds[1]["reversal_side"] == "LONG"


def test_first_opposite_non_tp2_later_opposite_tp2_still_counts(tmp_path: Path) -> None:
    dataset = tmp_path / "labeled.csv"
    rows = [
        {"row_id": "r1", "symbol": "SOL", "reference_ts": "2024-01-01T00:00:00", "side": "LONG", "status": "X", "strategy": "S", "resolution_status": "STOPPED", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "-1"},
        {"row_id": "r2", "symbol": "SOL", "reference_ts": "2024-01-01T00:01:00", "side": "SHORT", "status": "X", "strategy": "S", "resolution_status": "STOPPED", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "-1"},
        {"row_id": "r3", "symbol": "SOL", "reference_ts": "2024-01-01T00:02:00", "side": "SHORT", "status": "X", "strategy": "S", "resolution_status": "TP2", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "2"},
    ]
    _write_rows(dataset, rows)
    out = tmp_path / "out"
    analyze_failure_reversal_followthrough(labeled_dataset_path=str(dataset), output_root=str(out), forward_window_bars=3)
    seeds = list(csv.DictReader((out / "reversal_seed_rows.csv").open()))
    assert len(seeds) == 2
    assert seeds[0]["seed_row_identity"] == "row_id:r1"
    assert seeds[0]["reversal_found"] == "1"
    assert seeds[0]["reversal_side"] == "SHORT"
    assert seeds[0]["reversal_resolution_status"] == "TP2"
    assert seeds[0]["forward_distance_bars"] == "2"


def test_split_and_warning_detection(tmp_path: Path) -> None:
    dataset = tmp_path / "labeled.csv"
    rows = []
    for i in range(10):
        rows.append({"row_id": f"r{i}", "symbol": "ETH" if i < 8 else "BTC", "reference_ts": f"2024-01-01T00:{i:02d}:00", "side": "LONG" if i % 2 == 0 else "SHORT", "status": "X", "strategy": "S", "resolution_status": "STOPPED", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "-1"})
    rows += [
        {"row_id": "rx1", "symbol": "ETH", "reference_ts": "2024-01-01T00:00:30", "side": "SHORT", "status": "X", "strategy": "S", "resolution_status": "TP2", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "2"},
        {"row_id": "rx2", "symbol": "ETH", "reference_ts": "2024-01-01T00:01:30", "side": "LONG", "status": "X", "strategy": "S", "resolution_status": "TP2", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "2"},
    ]
    _write_rows(dataset, rows)
    out = tmp_path / "out"
    analyze_failure_reversal_followthrough(labeled_dataset_path=str(dataset), output_root=str(out), forward_window_bars=1, min_sample_size=20)

    split_rows = list(csv.DictReader((out / "reversal_split_metrics.csv").open()))
    assert [r["split"] for r in split_rows] == ["train", "validation", "oos"]

    warnings = {r["warning_code"] for r in csv.DictReader((out / "reversal_warnings.csv").open())}
    assert "OOS_COLLAPSE" in warnings
    assert "SYMBOL_CONCENTRATION" in warnings
    assert "SMALL_SAMPLE" in warnings


def test_split_uses_chronological_seed_reference_ts(tmp_path: Path) -> None:
    dataset = tmp_path / "labeled.csv"
    rows = [
        {"row_id": "a1", "symbol": "ETH", "reference_ts": "2024-01-01T00:00:00", "side": "LONG", "status": "X", "strategy": "S", "resolution_status": "STOPPED", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "-1"},
        {"row_id": "a2", "symbol": "ETH", "reference_ts": "2024-01-01T00:01:00", "side": "SHORT", "status": "X", "strategy": "S", "resolution_status": "TP2", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "2"},
        {"row_id": "b1", "symbol": "SOL", "reference_ts": "2024-01-01T00:10:00", "side": "LONG", "status": "X", "strategy": "S", "resolution_status": "STOPPED", "breakout_context": "A", "pullback_quality": "DEEP", "active_leg_boxes": "2", "quality_score": "75", "trend_regime": "UP", "continuation_execution_class": "C1", "late_extension": "1", "realized_r_multiple": "-1"},
    ]
    _write_rows(dataset, rows)
    out = tmp_path / "out"
    analyze_failure_reversal_followthrough(labeled_dataset_path=str(dataset), output_root=str(out), forward_window_bars=2, train_fraction=0.5, validation_fraction=0.0, oos_fraction=0.5)
    split_rows = list(csv.DictReader((out / "reversal_split_metrics.csv").open()))
    train = next(r for r in split_rows if r["split"] == "train")
    oos = next(r for r in split_rows if r["split"] == "oos")
    assert train["rows"] == "1"
    assert train["reversal_success_ratio"] == "1.0"
    assert oos["rows"] == "1"
    assert oos["reversal_success_ratio"] == "0.0"
