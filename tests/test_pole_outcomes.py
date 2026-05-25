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


def test_high_pole_continuation():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "X", 106, 103),
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "BEARISH_CONTINUATION"


def test_high_pole_failure():
    cols = [CsvColumn(1, "X", 110, 103), CsvColumn(2, "O", 108, 105), CsvColumn(3, "X", 112, 109)]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "FAILED_REVERSAL"


def test_low_pole_continuation():
    cols = [CsvColumn(1, "O", 100, 93), CsvColumn(2, "X", 98, 95), CsvColumn(3, "O", 101, 98)]
    out = label_pole_outcomes([_base_pole("LOW_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "BULLISH_CONTINUATION"


def test_low_pole_failure():
    cols = [CsvColumn(1, "O", 100, 93), CsvColumn(2, "X", 98, 95), CsvColumn(3, "O", 95, 92)]
    out = label_pole_outcomes([_base_pole("LOW_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "FAILED_REVERSAL"


def test_insufficient_future_data():
    cols = [CsvColumn(1, "X", 110, 103), CsvColumn(2, "O", 108, 105)]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "INSUFFICIENT_DATA"


def test_box_normalization_correctness():
    assert box_move(1.0, 0.5) == 2
    assert box_move(0.24, 0.5) == 0


def test_threshold_ordering_continuation_first():
    cols = [CsvColumn(1, "X", 110, 103), CsvColumn(2, "O", 108, 105), CsvColumn(3, "X", 107, 102), CsvColumn(4, "X", 112, 109)]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "BEARISH_CONTINUATION"


def test_threshold_ordering_invalidation_first_then_continuation():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "X", 112, 102),
        CsvColumn(4, "X", 108, 101),
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "FAILED_REVERSAL"


def test_threshold_ordering_neither_threshold_reached_is_sideways():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "X", 108, 106),
        CsvColumn(4, "O", 109, 106),
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=5, invalidation_threshold_boxes=5)
    assert out[0]["outcome_class"] == "SIDEWAYS"


def test_threshold_ordering_equal_threshold_same_column_prefers_invalidation():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "X", 112, 102),
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=20, continuation_threshold_boxes=3, invalidation_threshold_boxes=3)
    assert out[0]["outcome_class"] == "FAILED_REVERSAL"


def test_threshold_ordering_future_horizon_exhaustion_is_sideways():
    cols = [
        CsvColumn(1, "X", 110, 103),
        CsvColumn(2, "O", 108, 105),
        CsvColumn(3, "X", 108, 107),
        CsvColumn(4, "O", 108, 106),
        CsvColumn(5, "X", 106, 105),
    ]
    out = label_pole_outcomes([_base_pole("HIGH_POLE")], cols, box_size=1, future_columns=2, continuation_threshold_boxes=5, invalidation_threshold_boxes=5)
    assert out[0]["future_columns_observed"] == 2
    assert out[0]["outcome_class"] == "SIDEWAYS"
