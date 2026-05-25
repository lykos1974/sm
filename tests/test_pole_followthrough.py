from research_v2.patterns.pole_followthrough import (
    REGIME_FAST_MEAN_REVERSION,
    REGIME_FAILED_REVERSAL,
    REGIME_SIDEWAYS_COMPRESSION,
    REGIME_TREND_CONTINUATION,
    REGIME_VOLATILE_CHOP,
    _classify_regime,
    _compute_followthrough,
    _first_present,
)


def test_regime_strong_continuation_trajectory() -> None:
    regime = _classify_regime("HIGH_POLE", [2, 3, 4, 5], [0, 0, 1, 1], future_obs=4)
    assert regime == REGIME_TREND_CONTINUATION


def test_regime_strong_adverse_trajectory() -> None:
    regime = _classify_regime("HIGH_POLE", [0, 1, 1, 2], [2, 3, 4, 5], future_obs=4)
    assert regime in {REGIME_FAILED_REVERSAL, REGIME_FAST_MEAN_REVERSION}


def test_regime_sideways_compression() -> None:
    regime = _classify_regime("LOW_POLE", [0, 1, 1, 1], [0, 1, 0, 1], future_obs=4)
    assert regime == REGIME_SIDEWAYS_COMPRESSION


def test_regime_volatile_chop() -> None:
    regime = _classify_regime("LOW_POLE", [3, 0, 3, 0], [0, 3, 0, 3], future_obs=4)
    assert regime == REGIME_VOLATILE_CHOP


def test_regime_fast_mean_reversion() -> None:
    regime = _classify_regime("HIGH_POLE", [0, 0, 1, 1], [2, 3, 4, 4], future_obs=4)
    assert regime == REGIME_FAST_MEAN_REVERSION


def test_volatility_compression_is_not_degenerate_for_flat_then_expand() -> None:
    row = {"fav_path": "0,0,0,2,0,3", "adv_path": "0,0,0,0,2,0"}
    metrics = _compute_followthrough(row, future_columns=6)
    assert 1.0 < metrics.volatility_compression_after_signal <= 3.0


def test_metadata_fallbacks_skip_nan() -> None:
    row = {"symbol": "NaN", "asset": "BINANCE_FUT:BTCUSDT", "timestamp": "", "reference_ts": "1700000000"}
    assert _first_present(row, ("symbol", "asset")) == "BINANCE_FUT:BTCUSDT"
    assert _first_present(row, ("timestamp", "reference_ts")) == "1700000000"
