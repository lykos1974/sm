from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
PNF_DIR = ROOT / "pnf_mvp"
if str(PNF_DIR) not in sys.path:
    sys.path.insert(0, str(PNF_DIR))

from pnf_engine import PnFColumn, PnFEngine, PnFProfile
from patterns.poles import detect_pole_patterns


def test_detects_high_pole_on_over_50_percent_o_retrace():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 106, 100, 4, 5),
        PnFColumn(3, "O", 105, 102, 6, 7),
    ]
    out = detect_pole_patterns(cols, box_size=1)
    assert len(out) == 1
    assert out[0]["pattern_name"] == "HIGH_POLE"
    assert out[0]["status"] == "EARLY_50_RETRACE"


def test_detects_low_pole_on_over_50_percent_x_retrace():
    cols = [
        PnFColumn(0, "O", 100, 96, 0, 1),
        PnFColumn(1, "X", 99, 97, 2, 3),
        PnFColumn(2, "O", 95, 89, 4, 5),
        PnFColumn(3, "X", 93, 90, 6, 7),
    ]
    out = detect_pole_patterns(cols, box_size=1)
    assert len(out) == 1
    assert out[0]["pattern_name"] == "LOW_POLE"


def test_rejects_pole_height_less_or_equal_5_boxes():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 106, 102, 4, 5),
        PnFColumn(3, "O", 105, 102, 6, 7),
    ]
    assert detect_pole_patterns(cols, box_size=1) == []


def test_rejects_breakout_excess_under_3_boxes():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 102, 96, 4, 5),
        PnFColumn(3, "O", 101, 98, 6, 7),
    ]
    assert detect_pole_patterns(cols, box_size=1) == []


def test_rejects_non_adjacent_reversal_column():
    cols = [
        PnFColumn(0, "X", 100, 96, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 106, 100, 4, 5),
        PnFColumn(4, "O", 105, 102, 6, 7),
    ]
    assert detect_pole_patterns(cols, box_size=1) == []


def test_does_not_change_existing_double_top_bottom_behavior():
    profile = PnFProfile(name="t", box_size=1, reversal_boxes=3)
    engine = PnFEngine(profile)
    engine.columns = [
        PnFColumn(0, "X", 100, 98, 0, 1),
        PnFColumn(1, "O", 99, 97, 2, 3),
        PnFColumn(2, "X", 101, 98, 4, 5),
    ]
    assert engine.latest_signal_name() == "BUY"
