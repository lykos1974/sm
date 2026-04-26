"""Phase 4 prototype: incremental structure state scaffolding.

This module is intentionally shadow-mode only. The snapshot currently delegates to
`build_structure_state(...)` so behavior remains identical while we track which
fields are already computed/cached incrementally.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from structure_engine import StructureConfig, build_structure_state


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
        "trend_regime",
        "immediate_slope",
        "swing_direction",
        "support_level",
        "resistance_level",
        "breakout_context",
        "impulse_boxes",
        "pullback_boxes",
        "impulse_to_pullback_ratio",
        "last_meaningful_x_high",
        "last_meaningful_o_low",
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

        In Phase 5, selected current-column fields are computed incrementally.
        """
        columns = list(getattr(engine, "columns", []) or [])
        box_size = float(getattr(self.profile, "box_size", 0.0) or 0.0)
        cfg = self.config if self.config is not None else StructureConfig()
        extension_threshold = int(getattr(cfg, "extension_boxes_threshold", StructureConfig().extension_boxes_threshold))

        self.latest_signal_name = latest_signal_name
        self.market_state = market_state
        self.last_price = last_price
        self._last_columns_count = len(columns)

        current = columns[-1] if columns else None
        if current is None:
            current_column_kind = None
            current_column_top = None
            current_column_bottom = None
            active_leg_boxes = 0
            is_extended_move = False
        else:
            current_column_kind = getattr(current, "kind", "")
            current_column_top = float(getattr(current, "top", 0.0))
            current_column_bottom = float(getattr(current, "bottom", 0.0))
            span = abs(current_column_top - current_column_bottom)
            active_leg_boxes = int(round(span / box_size)) if box_size > 0 else 0
            is_extended_move = active_leg_boxes >= extension_threshold

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
        delegated_state["symbol"] = self._cached_fields.get(
            "symbol",
            delegated_state.get("symbol"),
        )
        delegated_state["latest_signal_name"] = self._cached_fields.get(
            "latest_signal_name",
            delegated_state.get("latest_signal_name"),
        )
        delegated_state["market_state"] = self._cached_fields.get(
            "market_state",
            delegated_state.get("market_state"),
        )
        delegated_state["last_price"] = self._cached_fields.get(
            "last_price",
            delegated_state.get("last_price"),
        )
        delegated_state["current_column_kind"] = self._cached_fields.get(
            "current_column_kind",
            delegated_state.get("current_column_kind"),
        )
        delegated_state["current_column_top"] = self._cached_fields.get(
            "current_column_top",
            delegated_state.get("current_column_top"),
        )
        delegated_state["current_column_bottom"] = self._cached_fields.get(
            "current_column_bottom",
            delegated_state.get("current_column_bottom"),
        )
        delegated_state["active_leg_boxes"] = self._cached_fields.get(
            "active_leg_boxes",
            delegated_state.get("active_leg_boxes"),
        )
        delegated_state["is_extended_move"] = self._cached_fields.get(
            "is_extended_move",
            delegated_state.get("is_extended_move"),
        )
        return delegated_state

    def implementation_status(self) -> dict[str, Any]:
        """Expose which parts are cached incrementally vs delegated."""
        return {
            "cached_fields": sorted(self._cached_fields.keys()),
            "delegated_fields": list(self._delegated_snapshot_fields),
            "snapshot_strategy": "delegated_to_build_structure_state",
            "columns_observed": self._last_columns_count,
        }
