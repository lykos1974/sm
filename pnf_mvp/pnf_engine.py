from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List, Optional

from patterns_basic import (
    detect_double_top_breakout_signal,
    detect_double_bottom_breakdown_signal,
    has_double_top_breakout,
    has_double_bottom_breakdown,
    previous_same_type_column,
)


@dataclass
class PnFProfile:
    name: str
    box_size: float
    reversal_boxes: int


@dataclass
class PnFColumn:
    idx: int
    kind: str  # X or O
    top: float
    bottom: float
    start_ts: int
    end_ts: int

    def levels(self, box_size: float):
        values = []
        if self.kind == "X":
            p = self.bottom
            while p <= self.top + 1e-9:
                values.append(round(p, 10))
                p += box_size
        else:
            p = self.top
            while p >= self.bottom - 1e-9:
                values.append(round(p, 10))
                p -= box_size
        return values

    def box_count(self, box_size: float) -> int:
        return int(round((self.top - self.bottom) / box_size)) + 1


class PnFEngine:
    def __init__(self, profile: PnFProfile):
        self.profile = profile
        self.columns: List[PnFColumn] = []
        self.last_price: Optional[float] = None
        self.signals = []
        self._emitted_signal_keys = set()

    def state_dict(self):
        return {
            "last_price": self.last_price,
            "columns": [asdict(c) for c in self.columns[-500:]],
            "signals": self.signals[-100:],
        }

    def _current_column(self) -> Optional[PnFColumn]:
        if not self.columns:
            return None
        return self.columns[-1]

    def _previous_column(self) -> Optional[PnFColumn]:
        if len(self.columns) < 2:
            return None
        return self.columns[-2]

    def _previous_same_type_column(self, kind: str) -> Optional[PnFColumn]:
        return previous_same_type_column(self.columns, kind)

    def _append_new_o_column_from_x_reversal(self, ts: int, price: float):
        col = self.columns[-1]
        box = self.profile.box_size

        new_bottom = col.top - box
        while price <= new_bottom - box:
            new_bottom -= box

        self.columns.append(
            PnFColumn(
                len(self.columns),
                "O",
                round(col.top - box, 10),
                round(new_bottom, 10),
                ts,
                ts,
            )
        )

    def _append_new_x_column_from_o_reversal(self, ts: int, price: float):
        col = self.columns[-1]
        box = self.profile.box_size

        new_top = col.bottom + box
        while price >= new_top + box:
            new_top += box

        self.columns.append(
            PnFColumn(
                len(self.columns),
                "X",
                round(new_top, 10),
                round(col.bottom + box, 10),
                ts,
                ts,
            )
        )

    def _detect_signals_on_current_column(self, signal_ts: int):
        out = []

        sig = detect_double_top_breakout_signal(
            self.columns,
            self._emitted_signal_keys,
            signal_ts,
        )
        if sig is not None:
            out.append(sig)

        sig = detect_double_bottom_breakdown_signal(
            self.columns,
            self._emitted_signal_keys,
            signal_ts,
        )
        if sig is not None:
            out.append(sig)

        if out:
            self.signals.extend(out)

        return out

    def _has_double_top_breakout(self) -> bool:
        return has_double_top_breakout(self.columns)

    def _has_double_bottom_breakdown(self) -> bool:
        return has_double_bottom_breakdown(self.columns)

    def _is_bullish_trend(self) -> bool:
        if len(self.columns) < 3:
            return False

        last_col = self.columns[-1]
        prev_same = self._previous_same_type_column("X")

        if last_col.kind != "X" or prev_same is None:
            return False

        return last_col.top > prev_same.top and not self._has_double_top_breakout()

    def _is_bearish_trend(self) -> bool:
        if len(self.columns) < 3:
            return False

        last_col = self.columns[-1]
        prev_same = self._previous_same_type_column("O")

        if last_col.kind != "O" or prev_same is None:
            return False

        return last_col.bottom < prev_same.bottom and not self._has_double_bottom_breakdown()

    def _is_range(self) -> bool:
        if len(self.columns) < 4:
            return False

        if self._has_double_top_breakout() or self._has_double_bottom_breakdown():
            return False

        if self._is_bullish_trend() or self._is_bearish_trend():
            return False

        return True

    def latest_signal_name(self):
        if self._has_double_top_breakout():
            return "BUY"
        if self._has_double_bottom_breakdown():
            return "SELL"
        return None

    def market_state(self):
        if len(self.columns) < 2:
            return "EARLY"
        if self._has_double_top_breakout():
            return "BULLISH_BREAKOUT"
        if self._has_double_bottom_breakdown():
            return "BEARISH_BREAKDOWN"
        if self._is_bullish_trend():
            return "BULLISH_TREND"
        if self._is_bearish_trend():
            return "BEARISH_TREND"
        if self._is_range():
            return "RANGE"
        return "NEUTRAL"

    def score(self):
        if not self.columns:
            return 0

        state = self.market_state()
        signal = self.latest_signal_name()
        last_col = self._current_column()

        if state == "EARLY":
            return 35

        score = 50

        if state in ("BULLISH_BREAKOUT", "BEARISH_BREAKDOWN"):
            score += 25
        elif state in ("BULLISH_TREND", "BEARISH_TREND"):
            score += 15
        elif state == "RANGE":
            score -= 5

        if last_col is not None:
            boxes = last_col.box_count(self.profile.box_size)
            if boxes >= self.profile.reversal_boxes + 1:
                score += 5
            if boxes >= self.profile.reversal_boxes + 3:
                score += 5

        if signal in ("BUY", "SELL"):
            score += 20

        return max(0, min(score, 100))

    def _column_debug_dict(self, col: Optional[PnFColumn]):
        if col is None:
            return None

        return {
            "idx": col.idx,
            "kind": col.kind,
            "top": col.top,
            "bottom": col.bottom,
            "start_ts": col.start_ts,
            "end_ts": col.end_ts,
            "boxes": col.box_count(self.profile.box_size),
        }

    def debug_snapshot(self):
        last_col = self._current_column()
        prev_col = self._previous_column()
        prev_x = self._previous_same_type_column("X")
        prev_o = self._previous_same_type_column("O")

        double_top_check = None
        if last_col is not None and last_col.kind == "X" and prev_x is not None:
            double_top_check = {
                "current_top": last_col.top,
                "previous_x_idx": prev_x.idx,
                "previous_x_top": prev_x.top,
                "condition": last_col.top > prev_x.top,
            }

        double_bottom_check = None
        if last_col is not None and last_col.kind == "O" and prev_o is not None:
            double_bottom_check = {
                "current_bottom": last_col.bottom,
                "previous_o_idx": prev_o.idx,
                "previous_o_bottom": prev_o.bottom,
                "condition": last_col.bottom < prev_o.bottom,
            }

        return {
            "profile": {
                "name": self.profile.name,
                "box_size": self.profile.box_size,
                "reversal_boxes": self.profile.reversal_boxes,
            },
            "last_price": self.last_price,
            "column_count": len(self.columns),
            "last_column": self._column_debug_dict(last_col),
            "previous_column": self._column_debug_dict(prev_col),
            "previous_x": self._column_debug_dict(prev_x),
            "previous_o": self._column_debug_dict(prev_o),
            "double_top_check": double_top_check,
            "double_bottom_check": double_bottom_check,
            "is_bullish_trend": self._is_bullish_trend(),
            "is_bearish_trend": self._is_bearish_trend(),
            "latest_signal": self.latest_signal_name(),
            "market_state": self.market_state(),
            "score": self.score(),
            "recent_signals": self.signals[-10:],
        }

    def update_from_price(self, ts: int, price: float):
        box = self.profile.box_size
        rev = self.profile.reversal_boxes * box
        self.last_price = price
        new_signals = []

        if not self.columns:
            anchor = round(price / box) * box
            self.columns.append(PnFColumn(0, "X", anchor, anchor, ts, ts))
            return {"new_signal": False, "new_signals": []}

        col = self.columns[-1]

        if len(self.columns) == 1 and col.top == col.bottom:
            if price >= col.top + box:
                col.kind = "X"
                while price >= col.top + box:
                    col.top += box
                col.end_ts = ts
                pass  # defer signal detection until the end of the candle-close update

            elif price <= col.bottom - box:
                col.kind = "O"
                while price <= col.bottom - box:
                    col.bottom -= box
                col.end_ts = ts
                pass  # defer signal detection until the end of the candle-close update

            return {"new_signal": bool(new_signals), "new_signals": new_signals}

        if col.kind == "X":
            if price >= col.top + box:
                while price >= col.top + box:
                    col.top += box
                col.end_ts = ts
                pass  # defer signal detection until the end of the candle-close update

            elif price <= col.top - rev:
                self._append_new_o_column_from_x_reversal(ts, price)
                pass  # defer signal detection until the end of the candle-close update

        else:
            if price <= col.bottom - box:
                while price <= col.bottom - box:
                    col.bottom -= box
                col.end_ts = ts
                pass  # defer signal detection until the end of the candle-close update

            elif price >= col.bottom + rev:
                self._append_new_x_column_from_o_reversal(ts, price)
                pass  # defer signal detection until the end of the candle-close update

        # FINAL CLOSE-BASED SIGNAL CHECK
        new_signals.extend(self._detect_signals_on_current_column(ts))
        return {"new_signal": bool(new_signals), "new_signals": new_signals}
