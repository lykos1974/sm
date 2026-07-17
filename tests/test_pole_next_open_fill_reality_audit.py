from research_v2.patterns.pole_core_motif_entry_timing_audit import Candle
from research_v2.patterns.pole_next_open_fill_reality_audit import evaluate_pending_entry


def candle(ts, open_, high, low, close):
    return Candle(ts, open_, high, low, close)


def test_order_is_cancelled_when_entry_is_not_touched_before_expiry():
    result = evaluate_pending_entry(
        candles=[
            candle(60, 105, 110, 104, 109),
            candle(120, 109, 112, 106, 111),
            candle(180, 111, 115, 108, 114),
        ],
        placement_ts=0,
        direction="LONG",
        entry=100,
        stop=97,
        target=107.5,
        expiry_candles=3,
    )

    assert result.status == "CANCELLED_NOT_FILLED"
    assert result.fill_ts is None
    assert result.result_r is None
    assert result.candles_waited == 3


def test_fill_on_second_candle_then_target_counts_as_trade():
    result = evaluate_pending_entry(
        candles=[
            candle(60, 104, 106, 102, 105),
            candle(120, 101, 103, 99, 102),
            candle(180, 102, 108, 101, 107),
        ],
        placement_ts=0,
        direction="LONG",
        entry=100,
        stop=97,
        target=107.5,
        expiry_candles=3,
    )

    assert result.status == "FILLED_TARGET_FIRST"
    assert result.fill_ts == 120
    assert result.exit_ts == 180
    assert result.candles_waited == 2
    assert result.result_r == 2.5


def test_candle_at_or_before_placement_is_never_used_for_fill():
    result = evaluate_pending_entry(
        candles=[
            candle(60, 100, 101, 99, 100),
            candle(120, 105, 106, 104, 105),
        ],
        placement_ts=60,
        direction="LONG",
        entry=100,
        stop=97,
        target=107.5,
        expiry_candles=1,
    )

    assert result.status == "CANCELLED_NOT_FILLED"


def test_fill_and_stop_in_same_candle_is_stop_first_when_target_not_hit():
    result = evaluate_pending_entry(
        candles=[candle(60, 101, 102, 96, 97)],
        placement_ts=0,
        direction="LONG",
        entry=100,
        stop=97,
        target=107.5,
        expiry_candles=1,
    )

    assert result.status == "FILLED_STOP_FIRST"
    assert result.result_r == -1.0


def test_short_fill_and_target():
    result = evaluate_pending_entry(
        candles=[
            candle(60, 95, 98, 94, 96),
            candle(120, 99, 101, 98, 100),
            candle(180, 99, 100, 91, 92),
        ],
        placement_ts=0,
        direction="SHORT",
        entry=100,
        stop=103,
        target=92.5,
        expiry_candles=2,
    )

    assert result.status == "FILLED_TARGET_FIRST"
    assert result.fill_ts == 120
    assert result.result_r == 2.5
