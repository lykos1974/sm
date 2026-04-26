"""Phase 4 prototype: incremental structure state scaffolding.

This module is intentionally shadow-mode only. The snapshot currently delegates to
`build_structure_state(...)` so behavior remains identical while we track which
fields are already computed/cached incrementally.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from structure_engine import build_structure_state


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

    # Phase 4 intentionally delegates all output fields to legacy builder.
    _delegated_snapshot_fields: tuple[str, ...] = (
        "symbol",
        "trend_state",
        "trend_regime",
        "immediate_slope",
        "swing_direction",
        "support_level",
        "resistance_level",
        "breakout_context",
        "is_extended_move",
        "active_leg_boxes",
        "impulse_boxes",
        "pullback_boxes",
        "impulse_to_pullback_ratio",
        "last_meaningful_x_high",
        "last_meaningful_o_low",
        "current_column_kind",
        "current_column_top",
        "current_column_bottom",
        "latest_signal_name",
        "market_state",
        "last_price",
        "notes",
    )

    def update_from_engine(
        self,
        engine: Any,
        latest_signal_name: str | None,
        market_state: str,
        last_price: float | None,
    ) -> None:
        """Update local cache from the latest engine state.

        In Phase 4 this cache is informational only (shadow mode).
        """
        columns = list(getattr(engine, "columns", []) or [])

        self.latest_signal_name = latest_signal_name
        self.market_state = market_state
        self.last_price = last_price
        self._last_columns_count = len(columns)

        current = columns[-1] if columns else None
        self._cached_fields = {
            "symbol": self.symbol,
            "latest_signal_name": latest_signal_name,
            "market_state": market_state,
            "last_price": last_price,
            "columns_count": len(columns),
            "current_column_kind": getattr(current, "kind", None),
            "current_column_top": float(getattr(current, "top", 0.0)) if current is not None else None,
            "current_column_bottom": float(getattr(current, "bottom", 0.0)) if current is not None else None,
        }

    def snapshot(self, engine: Any) -> dict[str, Any]:
        """Return structure snapshot.

        Phase 4 behavior: fully delegated to `build_structure_state(...)`.
        """
        return build_structure_state(
            symbol=self.symbol,
            profile=self.profile,
            columns=getattr(engine, "columns", []),
            latest_signal_name=self.latest_signal_name,
            market_state=str(self.market_state or ""),
            last_price=self.last_price,
            config=self.config,
        )

    def implementation_status(self) -> dict[str, Any]:
        """Expose which parts are cached incrementally vs delegated."""
        return {
            "cached_fields": sorted(self._cached_fields.keys()),
            "delegated_fields": list(self._delegated_snapshot_fields),
            "snapshot_strategy": "delegated_to_build_structure_state",
            "columns_observed": self._last_columns_count,
        }
