import csv
from pathlib import Path

from research_v2.optimizers.analyze_watch_promotion_after_failed_continuation import analyze_watch_promotion_after_failed_continuation


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_exclude_same_timestamp_opposite_for_watch_promotion(tmp_path: Path) -> None:
    rows = [
        {"row_id":"p1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","pullback_quality":"DEEP","breakout_context":"CTX","realized_r_multiple":"-1"},
        {"row_id":"p2","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"X","breakout_context":"CTX","realized_r_multiple":"0"},
        {"row_id":"p3","symbol":"ETH","reference_ts":"2024-01-01T00:01:00Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","pullback_quality":"X","breakout_context":"CTX","realized_r_multiple":"0"},
    ]
    data = tmp_path / "labeled.csv"
    out_default = tmp_path / "out_default"
    out_excluded = tmp_path / "out_excluded"
    _write_csv(data, rows)

    analyze_watch_promotion_after_failed_continuation(labeled_dataset_path=str(data), output_root=str(out_default), forward_window_structural=5)
    analyze_watch_promotion_after_failed_continuation(labeled_dataset_path=str(data), output_root=str(out_excluded), forward_window_structural=5, exclude_same_timestamp_opposite=True)

    default_seed = list(csv.DictReader((out_default / "watch_promotion_seed_rows.csv").open("r", encoding="utf-8")))[0]
    excluded_seed = list(csv.DictReader((out_excluded / "watch_promotion_seed_rows.csv").open("r", encoding="utf-8")))[0]
    assert default_seed["structural_distance_to_watch"] == "1"
    assert excluded_seed["structural_distance_to_watch"] == "2"
