import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PNF_MVP = ROOT / "pnf_mvp"
if str(PNF_MVP) not in sys.path:
    sys.path.insert(0, str(PNF_MVP))

import app as app_module
from app import App


class RaisingStatus:
    def set(self, _text):
        raise RuntimeError("status exploded")


class CleanupDummy:
    _finish_refresh_state = App._finish_refresh_state
    _safe_status_set = App._safe_status_set
    _safe_log = App._safe_log
    _safe_console_log = App._safe_console_log
    _schedule_refresh_completion = App._schedule_refresh_completion

    def __init__(self):
        self._refresh_running = True
        self.status_var = RaisingStatus()
        self.console_messages = []

    def _log(self, _message):
        raise RuntimeError("log exploded")

    def _safe_console_log(self, message):
        self.console_messages.append(message)


def test_finish_refresh_state_resets_before_status_or_log_failures():
    dummy = CleanupDummy()

    dummy._finish_refresh_state("DB synced", "REFRESH_END status=success")

    assert dummy._refresh_running is False
    assert any("status_set_failed" in message for message in dummy.console_messages)
    assert any("REFRESH_END status=success" in message for message in dummy.console_messages)
    assert any("REFRESH_STATE_RESET running=False" in message for message in dummy.console_messages)


def test_schedule_refresh_completion_failure_resets_refresh_running():
    dummy = CleanupDummy()

    def after(_delay, _callback):
        raise RuntimeError("tk destroyed")

    dummy.after = after

    scheduled = dummy._schedule_refresh_completion(lambda: None, "refresh_apply_schedule_failed")

    assert scheduled is False
    assert dummy._refresh_running is False
    assert any("refresh_apply_schedule_failed" in message for message in dummy.console_messages)
    assert any("REFRESH_STATE_RESET running=False" in message for message in dummy.console_messages)


class CandleFilterDummy:
    _epoch_now_ms = App._epoch_now_ms
    _latest_candle_is_open = App._latest_candle_is_open
    _closed_candles_for_refresh = App._closed_candles_for_refresh


def test_closed_candles_for_refresh_filters_each_open_or_future_candle():
    dummy = CandleFilterDummy()
    now_ms = 1_000_000
    candles = [
        {"close_time": now_ms - 60_000, "close": 1},
        {"close_time": now_ms - 4_000, "close": 2},
        {"close_time": now_ms + 60_000, "close": 3},
    ]

    closed, dropped = dummy._closed_candles_for_refresh(candles, now_ms)

    assert closed == [candles[0]]
    assert dropped is True


def test_closed_candles_for_refresh_keeps_all_eligible_closed_candles():
    dummy = CandleFilterDummy()
    now_ms = 1_000_000
    candles = [
        {"close_time": now_ms - 60_000, "close": 1},
        {"close_time": now_ms - 5_000, "close": 2},
    ]

    closed, dropped = dummy._closed_candles_for_refresh(candles, now_ms)

    assert closed == candles
    assert dropped is False


def test_latest_candle_is_open_uses_epoch_time_not_naive_utc_timestamp(monkeypatch):
    dummy = CandleFilterDummy()
    epoch_now_ms = 1_782_429_158_213
    naive_utcnow_ms = epoch_now_ms - (3 * 60 * 60 * 1000)

    monkeypatch.setattr(app_module.time, "time", lambda: epoch_now_ms / 1000)

    assert dummy._epoch_now_ms() == epoch_now_ms
    assert dummy._latest_candle_is_open(naive_utcnow_ms + 60_000) is False
    assert dummy._latest_candle_is_open(epoch_now_ms - 4_000) is True


def test_closed_candles_for_refresh_keeps_db_candle_closed_by_epoch_time(monkeypatch):
    dummy = CandleFilterDummy()
    epoch_now_ms = 1_782_429_158_213
    monkeypatch.setattr(app_module.time, "time", lambda: epoch_now_ms / 1000)
    candle = {"close_time": epoch_now_ms - 5_000, "close": 1}

    closed, dropped = dummy._closed_candles_for_refresh([candle])

    assert closed == [candle]
    assert dropped is False


def test_scanner_clock_logic_has_no_hardcoded_timezone_offset():
    source = Path(ROOT / "pnf_mvp" / "app.py").read_text()

    assert "datetime.utcnow().timestamp() * 1000" not in source
    assert "+ 3" not in source
    assert "+3" not in source
    assert "10800000" not in source
    assert "10_800_000" not in source
