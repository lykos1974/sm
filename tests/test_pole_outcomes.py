from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research_v2.patterns.pole_outcomes import CsvColumn, box_move, label_pole_outcomes


def _base_pole(pattern_name: str) -> dict:
    return {
        "pattern_name": pattern_name,
        "status": "EARLY_50_RETRACE",
        "pole_column_index": "1",
        "reversal_column_index": "2",
        "pole_boxes": "8",
        "retrace_ratio": "0.75",
        "enhanced_by_opposing_pole": "False",
        "opposing_pole_nearby": "False",
        "breakout_excess_boxes": "3",
    }


def test_high_pole_continuation_after_bounce():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "X", 109, 106),  # bounce (X) should not contribute continuation
        CsvColumn(4, "O", 107, 101),  # continuation on O from rev bottom(105) -> 4 boxes
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "BEARISH_CONTINUATION"


def test_high_pole_invalidation_after_small_continuation():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "O", 107, 103),  # small continuation: 2 boxes
        CsvColumn(4, "X", 112, 110),  # invalidation on X from rev top(108) -> 4 boxes
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "FAILED_REVERSAL"


def test_low_pole_continuation_after_pullback():
    cols = [
        CsvColumn(1, "O", 100, 93),
        CsvColumn(2, "X", 98, 95),
        CsvColumn(3, "O", 97, 94),  # pullback (O) should not contribute continuation
        CsvColumn(4, "X", 102, 99),  # continuation on X from rev top(98) -> 4 boxes
    ]
    out = label_pole_outcomes([_base_pole("LOW_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "BULLISH_CONTINUATION"


def test_low_pole_invalidation_after_small_continuation():
    cols = [
        CsvColumn(1, "O", 100, 93),
        CsvColumn(2, "X", 98, 95),
        CsvColumn(3, "X", 100, 97),  # small continuation: 2 boxes
        CsvColumn(4, "O", 96, 90),  # invalidation on O from rev bottom(95) -> 5 boxes
    ]
    out = label_pole_outcomes([_base_pole("LOW_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "FAILED_REVERSAL"


def test_sideways_congestion():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "X", 109, 106),
        CsvColumn(4, "O", 107, 104),
        CsvColumn(5, "X", 108, 106),
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=5, invalidation_threshold_boxes=5)
    assert out[0]["outcome_class"] == "SIDEWAYS"


def test_future_horizon_exhaustion():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "X", 109, 106),
        CsvColumn(4, "O", 107, 104),
        CsvColumn(5, "X", 109, 106),
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=2, continuation_threshold_boxes=5, invalidation_threshold_boxes=5)
    assert out[0]["future_columns_observed"] == 2
    assert out[0]["outcome_class"] == "SIDEWAYS"


def test_same_column_equal_threshold_prefers_invalidation():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "X", 111, 104),  # adv == 3, fav == 0
        CsvColumn(4, "O", 108, 102),  # fav == 3, adv == 0
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "FAILED_REVERSAL"


def test_insufficient_future_data():
    cols = [CsvColumn(1, "X", 110, 103), CsvColumn(2, "O", 108, 105)]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "INSUFFICIENT_DATA"


def test_box_normalization_correctness():
    assert box_move(1.0, 0.5) == 2
    assert box_move(0.24, 0.5) == 0


def test_path_series_exported() -> None:
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "O", 107, 103),
        CsvColumn(4, "X", 112, 110),
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["fav_path"] == "2,0"
    assert out[0]["adv_path"] == "0,4"
