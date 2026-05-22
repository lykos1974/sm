import csv
from pathlib import Path

from research_v2.optimizers.analyze_structural_reversal_progression import analyze_structural_reversal_progression


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_progression_rules_and_distances(tmp_path: Path) -> None:
    rows = [
        {"row_id":"r1","symbol":"ETH","reference_ts":"2024-01-01T00:00:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","breakout_context":"POST_BREAKOUT_PULLBACK","pullback_quality":"HEALTHY","active_leg_boxes":"2","continuation_execution_class":"A","trend_regime":"UP","strategy":"S"},
        {"row_id":"r2","symbol":"ETH","reference_ts":"2024-01-01T00:01:00Z","side":"SHORT","status":"WATCH","resolution_status":"OPEN","breakout_context":"POST_BREAKOUT_PULLBACK","pullback_quality":"HEALTHY","active_leg_boxes":"2","continuation_execution_class":"A","trend_regime":"DOWN","strategy":"S"},
        {"row_id":"r3","symbol":"ETH","reference_ts":"2024-01-01T00:02:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"OPEN","breakout_context":"POST_BREAKOUT_PULLBACK","pullback_quality":"HEALTHY","active_leg_boxes":"2","continuation_execution_class":"A","trend_regime":"UP","strategy":"S"},
        {"row_id":"r4","symbol":"ETH","reference_ts":"2024-01-01T00:03:00Z","side":"SHORT","status":"CANDIDATE","resolution_status":"OPEN","breakout_context":"POST_BREAKOUT_PULLBACK","pullback_quality":"HEALTHY","active_leg_boxes":"2","continuation_execution_class":"A","trend_regime":"DOWN","strategy":"S"},
        {"row_id":"r5","symbol":"ETH","reference_ts":"2024-01-01T00:04:00Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","breakout_context":"POST_BREAKOUT_PULLBACK","pullback_quality":"HEALTHY","active_leg_boxes":"2","continuation_execution_class":"A","trend_regime":"DOWN","strategy":"S"},
        {"row_id":"r6","symbol":"BTC","reference_ts":"2024-01-01T00:01:30Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","breakout_context":"POST_BREAKOUT_PULLBACK","pullback_quality":"HEALTHY","active_leg_boxes":"2","continuation_execution_class":"A","trend_regime":"DOWN","strategy":"S"},
        {"row_id":"r7","symbol":"ETH","reference_ts":"2024-01-01T00:05:00Z","side":"SHORT","status":"CANDIDATE","resolution_status":"STOPPED","breakout_context":"LATE_EXTENSION","pullback_quality":"WEAK","active_leg_boxes":"1","continuation_execution_class":"B","trend_regime":"DOWN","strategy":"S"},
        {"row_id":"r8","symbol":"ETH","reference_ts":"2024-01-01T00:06:00Z","side":"LONG","status":"WATCH","resolution_status":"OPEN","breakout_context":"LATE_EXTENSION","pullback_quality":"WEAK","active_leg_boxes":"1","continuation_execution_class":"B","trend_regime":"UP","strategy":"S"},
    ]
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)

    analyze_structural_reversal_progression(
        labeled_dataset_path=str(data),
        output_root=str(out),
        forward_structural_window=10,
        min_sample_size=2,
    )

    with (out / "structural_progression_rows.csv").open("r", encoding="utf-8") as handle:
        seeds = list(csv.DictReader(handle))

    assert len(seeds) == 2
    long_seed = next(r for r in seeds if r["seed_side"] == "LONG")
    assert long_seed["structural_distance_to_watch"] == "1"
    assert long_seed["structural_distance_to_candidate"] == "3"
    assert long_seed["structural_distance_to_tp2"] == "4"
    short_seed = next(r for r in seeds if r["seed_side"] == "SHORT")
    assert short_seed["structural_distance_to_watch"] == "1"
    assert short_seed["structural_distance_to_candidate"] == ""
    assert short_seed["opposite_tp2_found"] == "0"


def test_split_and_warnings(tmp_path: Path) -> None:
    rows = []
    for i in range(10):
        rows.append({"row_id":f"l{i}","symbol":"ETH","reference_ts":f"2024-01-01T00:{i:02d}:00Z","side":"LONG","status":"CANDIDATE","resolution_status":"STOPPED","breakout_context":"X","pullback_quality":"Y","active_leg_boxes":"2","continuation_execution_class":"A","trend_regime":"UP","strategy":"S"})
        # train gets tp2, later rows miss to force oos collapse
        if i < 6:
            rows.append({"row_id":f"o{i}","symbol":"ETH","reference_ts":f"2024-01-01T00:{i:02d}:30Z","side":"SHORT","status":"WATCH","resolution_status":"TP2","breakout_context":"X","pullback_quality":"Y","active_leg_boxes":"2","continuation_execution_class":"A","trend_regime":"DOWN","strategy":"S"})
    data = tmp_path / "labeled.csv"
    out = tmp_path / "out"
    _write_csv(data, rows)
    analyze_structural_reversal_progression(labeled_dataset_path=str(data), output_root=str(out), forward_structural_window=1, min_sample_size=50)

    with (out / "structural_progression_warnings.csv").open("r", encoding="utf-8") as handle:
        warnings = {r["warning_code"] for r in csv.DictReader(handle)}
    assert "OOS_COLLAPSE" in warnings
    assert "SMALL_SAMPLE" in warnings
    assert "LONG_SHORT_ASYMMETRY" in warnings
