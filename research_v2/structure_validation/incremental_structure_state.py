"""Phase 4 prototype: incremental structure state scaffolding.

This module is intentionally shadow-mode only. The snapshot currently delegates to
`build_structure_state(...)` so behavior remains identical while we track which
fields are already computed/cached incrementally.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from structure_engine import (
    REGIME_BEARISH,
    REGIME_BULLISH,
    REGIME_EARLY,
    REGIME_RANGE,
    SLOPE_BEARISH_PUSH,
    SLOPE_BEARISH_PULLBACK,
    SLOPE_BULLISH_PUSH,
    SLOPE_BULLISH_REBOUND,
    SLOPE_FLAT,
    SWING_DOWN,
    SWING_UP,
    TREND_BEARISH,
    TREND_BULLISH,
    StructureConfig,
    build_structure_state,
)


@dataclass
class IncrementalStructureState:
    """Prototype incremental structure state cache for a symbol/profile pair."""

    symbol: str
    profile: Any
    config: Any | None = None
    latest_signal_name: str | None = None
    market_state: str | None = None
    last_price: float | None = None
    _last_columns_count: int = 0
    _cached_fields: dict[str, Any] = field(default_factory=dict)

    # Phase 5 incrementally computes a small, stable subset of snapshot fields.
    _delegated_snapshot_fields: tuple[str, ...] = (
        "trend_state",
        "breakout_context",
        "notes",
    )

    @staticmethod
    def _detect_swing_direction_from_cached_values(
        *,
        last_two_meaningful_x_highs: list[float] | None,
        last_two_meaningful_o_lows: list[float] | None,
        last_completed_kind: str | None,
        columns_count: int,
    ) -> str:
        """Mirror legacy `_detect_swing_direction(...)` semantics exactly."""
        x_highs = list(last_two_meaningful_x_highs or [])
        o_lows = list(last_two_meaningful_o_lows or [])

        up = len(x_highs) >= 2 and x_highs[-1] > x_highs[-2]
        down = len(o_lows) >= 2 and o_lows[-1] < o_lows[-2]

        if up and not down:
            return "UP"
        if down and not up:
            return "DOWN"

        if columns_count >= 2:
            if last_completed_kind == "X":
                return "UP"
            if last_completed_kind == "O":
                return "DOWN"

        return "NEUTRAL"

    @staticmethod
    def _detect_immediate_slope_from_cached_values(
        current_column_kind: str | None,
        trend_regime: str | None,
        trend_state: str | None,
    ) -> str:
        """Mirror legacy `_detect_immediate_slope(...)` semantics exactly."""
        if current_column_kind == "X":
            if trend_state == TREND_BULLISH or trend_regime == REGIME_BULLISH:
                return SLOPE_BULLISH_PUSH
            if trend_state == TREND_BEARISH or trend_regime == REGIME_BEARISH:
                return SLOPE_BULLISH_REBOUND
            return SLOPE_BULLISH_REBOUND

        if current_column_kind == "O":
            if trend_state == TREND_BULLISH or trend_regime == REGIME_BULLISH:
                return SLOPE_BEARISH_PULLBACK
            if trend_state == TREND_BEARISH or trend_regime == REGIME_BEARISH:
                return SLOPE_BEARISH_PUSH
            return SLOPE_BEARISH_PULLBACK

        return SLOPE_FLAT

    @staticmethod
    def _recent_direction_bias_from_completed_kinds(completed_column_kinds: list[str] | None, window: int) -> int:
        """Mirror legacy `_recent_direction_bias(...)` semantics exactly."""
        recent = list(completed_column_kinds or [])[-window:]
        score = 0
        for kind in recent:
            if kind == "X":
                score += 1
            elif kind == "O":
                score -= 1
        return score

    @classmethod
    def _detect_trend_regime_from_cached_values(
        cls,
        *,
        columns_count: int,
        market_state: str | None,
        swing_direction: str | None,
        config: StructureConfig,
        last_two_meaningful_x_highs: list[float] | None,
        last_two_meaningful_o_lows: list[float] | None,
        last_meaningful_x_high: float | None,
        last_meaningful_o_low: float | None,
        current_column_top: float | None,
        current_column_bottom: float | None,
        completed_column_kinds: list[str] | None,
    ) -> str:
        """Mirror legacy `_detect_trend_regime(...)` semantics exactly."""
        if columns_count < config.early_min_columns:
            return REGIME_EARLY

        x_highs = list(last_two_meaningful_x_highs or [])
        o_lows = list(last_two_meaningful_o_lows or [])

        bullish_structure = len(x_highs) >= 2 and x_highs[-1] > x_highs[-2]
        bearish_structure = len(o_lows) >= 2 and o_lows[-1] < o_lows[-2]

        ms = (market_state or "").upper()
        bias = cls._recent_direction_bias_from_completed_kinds(completed_column_kinds, config.recent_columns_for_bias)

        if "BULLISH" in ms:
            return REGIME_BULLISH
        if "BEARISH" in ms:
            return REGIME_BEARISH

        if bullish_structure and swing_direction == SWING_UP:
            return REGIME_BULLISH
        if bearish_structure and swing_direction == SWING_DOWN:
            return REGIME_BEARISH

        if bias >= config.regime_bias_threshold and swing_direction == SWING_UP:
            return REGIME_BULLISH
        if bias <= -config.regime_bias_threshold and swing_direction == SWING_DOWN:
            return REGIME_BEARISH

        if last_meaningful_x_high is not None and last_meaningful_o_low is not None:
            if current_column_top >= last_meaningful_x_high and current_column_bottom > last_meaningful_o_low:
                return REGIME_BULLISH
            if current_column_bottom <= last_meaningful_o_low and current_column_top < last_meaningful_x_high:
                return REGIME_BEARISH

        return REGIME_RANGE

    def update_from_engine(
        self,
        engine: Any,
        latest_signal_name: str | None,
        market_state: str,
        last_price: float | None,
    ) -> None:
        """Update local cache from the latest engine state.

        In Phase 5, selected current-column fields are computed incrementally.
        """
        columns = list(getattr(engine, "columns", []) or [])
        box_size = float(getattr(self.profile, "box_size", 0.0) or 0.0)
        cfg = self.config if self.config is not None else StructureConfig()
        extension_threshold = int(getattr(cfg, "extension_boxes_threshold", StructureConfig().extension_boxes_threshold))

        self.latest_signal_name = latest_signal_name
        self.market_state = market_state
        self.last_price = last_price
        previous_columns_count = self._last_columns_count
        self._last_columns_count = len(columns)

        completed_columns = columns[:-1] if columns else []
        previous_completed_columns_count = max(previous_columns_count - 1, 0)
        completed_columns_count = len(completed_columns)
        cached_last_meaningful_x_high = self._cached_fields.get("last_meaningful_x_high")
        cached_last_meaningful_o_low = self._cached_fields.get("last_meaningful_o_low")
        cached_last_two_meaningful_x_highs = list(self._cached_fields.get("last_two_meaningful_x_highs") or [])
        cached_last_two_meaningful_o_lows = list(self._cached_fields.get("last_two_meaningful_o_lows") or [])
        cached_last_completed_kind = self._cached_fields.get("last_completed_kind")
        cached_completed_column_kinds = list(self._cached_fields.get("completed_column_kinds") or [])

        if not completed_columns:
            cached_last_meaningful_x_high = None
            cached_last_meaningful_o_low = None
            cached_last_two_meaningful_x_highs = []
            cached_last_two_meaningful_o_lows = []
            cached_last_completed_kind = None
            cached_completed_column_kinds = []
        else:
            needs_bootstrap_scan = (
                len(columns) < previous_columns_count
                or "last_meaningful_x_high" not in self._cached_fields
                or "last_meaningful_o_low" not in self._cached_fields
                or "last_two_meaningful_x_highs" not in self._cached_fields
                or "last_two_meaningful_o_lows" not in self._cached_fields
                or "last_completed_kind" not in self._cached_fields
            )
            if needs_bootstrap_scan:
                cached_last_meaningful_x_high = None
                cached_last_meaningful_o_low = None
                cached_last_two_meaningful_x_highs = []
                cached_last_two_meaningful_o_lows = []
                cached_completed_column_kinds = []
                for column in completed_columns:
                    column_kind = getattr(column, "kind", "")
                    cached_completed_column_kinds.append(column_kind)
                    if column_kind == "X":
                        x_high = float(getattr(column, "top", 0.0))
                        cached_last_meaningful_x_high = x_high
                        cached_last_two_meaningful_x_highs.append(x_high)
                        cached_last_two_meaningful_x_highs = cached_last_two_meaningful_x_highs[-2:]
                    elif column_kind == "O":
                        o_low = float(getattr(column, "bottom", 0.0))
                        cached_last_meaningful_o_low = o_low
                        cached_last_two_meaningful_o_lows.append(o_low)
                        cached_last_two_meaningful_o_lows = cached_last_two_meaningful_o_lows[-2:]
                cached_last_completed_kind = getattr(completed_columns[-1], "kind", "")
            else:
                if completed_columns_count > previous_completed_columns_count:
                    last_completed = completed_columns[-1]
                    last_completed_kind = getattr(last_completed, "kind", "")
                    cached_last_completed_kind = last_completed_kind
                    cached_completed_column_kinds.append(last_completed_kind)
                    if last_completed_kind == "X":
                        x_high = float(getattr(last_completed, "top", 0.0))
                        cached_last_meaningful_x_high = x_high
                        cached_last_two_meaningful_x_highs.append(x_high)
                        cached_last_two_meaningful_x_highs = cached_last_two_meaningful_x_highs[-2:]
                    elif last_completed_kind == "O":
                        o_low = float(getattr(last_completed, "bottom", 0.0))
                        cached_last_meaningful_o_low = o_low
                        cached_last_two_meaningful_o_lows.append(o_low)
                        cached_last_two_meaningful_o_lows = cached_last_two_meaningful_o_lows[-2:]
                else:
                    cached_last_completed_kind = getattr(completed_columns[-1], "kind", "")

        current = columns[-1] if columns else None
        if current is None:
            current_column_kind = None
            current_column_top = None
            current_column_bottom = None
            active_leg_boxes = 0
            is_extended_move = False
            current_column_span_boxes = None
        else:
            current_column_kind = getattr(current, "kind", "")
            current_column_top = float(getattr(current, "top", 0.0))
            current_column_bottom = float(getattr(current, "bottom", 0.0))
            span = abs(current_column_top - current_column_bottom)
            active_leg_boxes = int(round(span / box_size)) if box_size > 0 else 0
            is_extended_move = active_leg_boxes >= extension_threshold
            current_column_span_boxes = (span / box_size) if box_size > 0 else None

        prev_x_span_boxes = None
        if box_size > 0:
            for column in reversed(completed_columns):
                if getattr(column, "kind", "") == "X":
                    prev_x_span_boxes = abs(
                        float(getattr(column, "top", 0.0))
                        - float(getattr(column, "bottom", 0.0))
                    ) / box_size
                    break

        self._cached_fields = {
            "symbol": self.symbol,
            "latest_signal_name": latest_signal_name,
            "market_state": market_state,
            "last_price": last_price,
            "columns_count": len(columns),
            "current_column_kind": current_column_kind,
            "current_column_top": current_column_top,
            "current_column_bottom": current_column_bottom,
            "active_leg_boxes": active_leg_boxes,
            "is_extended_move": is_extended_move,
            "last_meaningful_x_high": cached_last_meaningful_x_high,
            "last_meaningful_o_low": cached_last_meaningful_o_low,
            "last_two_meaningful_x_highs": cached_last_two_meaningful_x_highs,
            "last_two_meaningful_o_lows": cached_last_two_meaningful_o_lows,
            "last_completed_kind": cached_last_completed_kind,
            "completed_column_kinds": cached_completed_column_kinds,
            "prev_x_span_boxes": prev_x_span_boxes,
            "current_column_span_boxes": current_column_span_boxes,
        }

    def snapshot(self, engine: Any) -> dict[str, Any]:
        """Return structure snapshot.

        Phase 5 behavior: delegate full snapshot, then replace selected fields
        with incrementally-computed values.
        """
        delegated_state = build_structure_state(
            symbol=self.symbol,
            profile=self.profile,
            columns=getattr(engine, "columns", []),
            latest_signal_name=self.latest_signal_name,
            market_state=str(self.market_state or ""),
            last_price=self.last_price,
            config=self.config,
        )

        swing_direction = self._detect_swing_direction_from_cached_values(
            last_two_meaningful_x_highs=self._cached_fields.get("last_two_meaningful_x_highs"),
            last_two_meaningful_o_lows=self._cached_fields.get("last_two_meaningful_o_lows"),
            last_completed_kind=self._cached_fields.get("last_completed_kind"),
            columns_count=int(self._cached_fields.get("columns_count", len(getattr(engine, "columns", []) or []))),
        )
        delegated_state["swing_direction"] = swing_direction
        self._cached_fields["swing_direction"] = swing_direction

        cfg = self.config if self.config is not None else StructureConfig()
        trend_regime = self._detect_trend_regime_from_cached_values(
            columns_count=int(self._cached_fields.get("columns_count", len(getattr(engine, "columns", []) or []))),
            market_state=self.market_state,
            swing_direction=swing_direction,
            config=cfg,
            last_two_meaningful_x_highs=self._cached_fields.get("last_two_meaningful_x_highs"),
            last_two_meaningful_o_lows=self._cached_fields.get("last_two_meaningful_o_lows"),
            last_meaningful_x_high=self._cached_fields.get("last_meaningful_x_high"),
            last_meaningful_o_low=self._cached_fields.get("last_meaningful_o_low"),
            current_column_top=self._cached_fields.get("current_column_top"),
            current_column_bottom=self._cached_fields.get("current_column_bottom"),
            completed_column_kinds=self._cached_fields.get("completed_column_kinds"),
        )
        delegated_state["trend_regime"] = trend_regime
        self._cached_fields["trend_regime"] = trend_regime

        immediate_slope = self._detect_immediate_slope_from_cached_values(
            current_column_kind=self._cached_fields.get(
                "current_column_kind",
                delegated_state.get("current_column_kind"),
            ),
            trend_regime=trend_regime,
            trend_state=delegated_state.get("trend_state"),
        )
        delegated_state["immediate_slope"] = immediate_slope
        self._cached_fields["immediate_slope"] = immediate_slope
        return delegated_state

    def implementation_status(self) -> dict[str, Any]:
        """Expose which parts are cached incrementally vs delegated."""
        cached_fields = set(self._cached_fields.keys())
        return {
            "cached_fields": sorted(cached_fields),
            "delegated_fields": list(self._delegated_snapshot_fields),
            "snapshot_strategy": "delegated_to_build_structure_state",
            "columns_observed": self._last_columns_count,
        }
