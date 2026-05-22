import csv
from pathlib import Path

from research_v2.optimizers.validate_execution_realism_surviving_hypothesis import validate_execution_realism_surviving_hypothesis


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_first_future_same_symbol_short_watch_and_cost(tmp_path: Path) -> None:
    rows = [
        {"row_id":"s1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"LATE_EXTENSION","realized_r_multiple":"-1"},
        {"row_id":"s2","symbol":"BTC","reference_ts":"2024-01-01T00:00:30Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"2.0"},
        {"row_id":"s3","symbol":"ETH","reference_ts":"2024-01-01T00:01:00Z","side":"SHORT","status":"WATCH","resolution_status":"STOPPED","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"-1.0"},
        {"row_id":"s4","symbol":"ETH","reference_ts":"2024-01-01T00:02:00Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"2.0"},
        {"row_id":"s5","symbol":"ETH","reference_ts":"2024-01-01T00:03:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"LATE_EXTENSION","realized_r_multiple":"-1"},
        {"row_id":"s6","symbol":"ETH","reference_ts":"2024-01-01T00:03:30Z","side":"SHORT","status":"WATCH","resolution_status":"EXPIRED","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"0.0"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    validate_execution_realism_surviving_hypothesis(
        labeled_dataset_path=str(data),
        output_root=str(out),
        forward_structural_window=5,
        cost_r_deduction=0.1,
        min_oos_sample=1,
    )

    with (out / "execution_realism_trades.csv").open("r", encoding="utf-8") as handle:
        trades = list(csv.DictReader(handle))

    assert len(trades) == 2
    assert trades[0]["selected_row_identity"] == "row_id:s3"  # first short watch only, no hindsight
    assert trades[0]["realized_r_after_cost"] == "-1.100000"
    assert trades[1]["selected_row_identity"] == "row_id:s6"


def test_overlap_and_split_metrics_and_warnings(tmp_path: Path) -> None:
    rows = []
    for i in range(8):
        rows.append({"row_id":f"L{i}","symbol":"ETH","reference_ts":f"2024-01-01T00:{i:02d}:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"LATE_EXTENSION","realized_r_multiple":"-1"})
        rows.append({"row_id":f"W{i}","symbol":"ETH","reference_ts":f"2024-01-01T00:{i:02d}:30Z","side":"SHORT","status":"WATCH","resolution_status":"STOPPED" if i > 2 else "TP2","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"-0.4" if i > 2 else "1.5"})
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)
    validate_execution_realism_surviving_hypothesis(labeled_dataset_path=str(data), output_root=str(out), forward_structural_window=1, cost_r_deduction=0.2, min_oos_sample=10)

    with (out / "execution_realism_split_metrics.csv").open("r", encoding="utf-8") as handle:
        splits = list(csv.DictReader(handle))
    split_names = {r["split"] for r in splits}
    assert {"train", "validation", "oos", "symbol:ETH"}.issubset(split_names)

    with (out / "execution_realism_warnings.csv").open("r", encoding="utf-8") as handle:
        warnings = {r["warning_code"] for r in csv.DictReader(handle)}
    assert "SMALL_OOS_SAMPLE" in warnings
    assert "ETH_CONCENTRATION" in warnings


def test_unresolved_selected_watch_blocks_future_symbol_signals(tmp_path: Path) -> None:
    rows = [
        {"row_id":"u1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"LATE_EXTENSION","realized_r_multiple":"-1"},
        {"row_id":"u2","symbol":"ETH","reference_ts":"2024-01-01T00:00:30Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"0.2"},
        {"row_id":"u3","symbol":"ETH","reference_ts":"2024-01-01T00:01:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"LATE_EXTENSION","realized_r_multiple":"-1"},
        {"row_id":"u4","symbol":"ETH","reference_ts":"2024-01-01T00:01:30Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"2.0"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    validate_execution_realism_surviving_hypothesis(
        labeled_dataset_path=str(data),
        output_root=str(out),
        forward_structural_window=5,
    )

    with (out / "execution_realism_trades.csv").open("r", encoding="utf-8") as handle:
        trades = list(csv.DictReader(handle))
    assert len(trades) == 1
    assert trades[0]["selected_row_identity"] == "row_id:u2"

    summary_text = (out / "execution_realism_summary.md").read_text(encoding="utf-8")
    assert "- overlap skips: 1" in summary_text


def test_default_seed_filter_requires_deep_and_late_extension(tmp_path: Path) -> None:
    rows = [
        {"row_id":"d1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"LATE_EXTENSION","realized_r_multiple":"-1"},
        {"row_id":"d2","symbol":"ETH","reference_ts":"2024-01-01T00:00:30Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"1.5"},
        {"row_id":"d3","symbol":"ETH","reference_ts":"2024-01-01T00:01:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"POST_BREAKOUT_PULLBACK","realized_r_multiple":"-1"},
        {"row_id":"d4","symbol":"ETH","reference_ts":"2024-01-01T00:01:30Z","side":"SHORT","status":"WATCH","resolution_status":"STOPPED","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"-1.0"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    validate_execution_realism_surviving_hypothesis(labeled_dataset_path=str(data), output_root=str(out), forward_structural_window=5)

    with (out / "execution_realism_trades.csv").open("r", encoding="utf-8") as handle:
        trades = list(csv.DictReader(handle))
    assert len(trades) == 1
    assert trades[0]["seed_row_identity"] == "row_id:d1"


def test_allow_any_breakout_context_includes_deep_seeds_from_other_contexts(tmp_path: Path) -> None:
    rows = [
        {"row_id":"a1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"POST_BREAKOUT_PULLBACK","realized_r_multiple":"-1"},
        {"row_id":"a2","symbol":"ETH","reference_ts":"2024-01-01T00:00:30Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"2.0"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    validate_execution_realism_surviving_hypothesis(
        labeled_dataset_path=str(data),
        output_root=str(out),
        forward_structural_window=5,
        allow_any_breakout_context=True,
    )

    with (out / "execution_realism_trades.csv").open("r", encoding="utf-8") as handle:
        trades = list(csv.DictReader(handle))
    assert len(trades) == 1
    assert trades[0]["seed_row_identity"] == "row_id:a1"


def test_no_hindsight_selection_unchanged_with_relaxed_breakout_context(tmp_path: Path) -> None:
    rows = [
        {"row_id":"n1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"POST_BREAKOUT_PULLBACK","realized_r_multiple":"-1"},
        {"row_id":"n2","symbol":"ETH","reference_ts":"2024-01-01T00:00:30Z","side":"SHORT","status":"WATCH","resolution_status":"STOPPED","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"-1"},
        {"row_id":"n3","symbol":"ETH","reference_ts":"2024-01-01T00:01:00Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","pullback_quality":"X","breakout_context":"Y","realized_r_multiple":"2"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    validate_execution_realism_surviving_hypothesis(
        labeled_dataset_path=str(data),
        output_root=str(out),
        forward_structural_window=5,
        allow_any_breakout_context=True,
    )

    with (out / "execution_realism_trades.csv").open("r", encoding="utf-8") as handle:
        trades = list(csv.DictReader(handle))
    assert len(trades) == 1
    assert trades[0]["selected_row_identity"] == "row_id:n2"
