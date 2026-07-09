from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Protocol, runtime_checkable

from .evidence import Evidence
from .evidence_source import EvidenceSource
from .exceptions import RequiredFieldError
from .ids import deterministic_id


@runtime_checkable
class LiquidationProvider(Protocol):
    """Provider interface for exchange-agnostic liquidation observations.

    Concrete adapters may call Binance, MEXC, cached files, databases, or any
    other market-data backend. This protocol intentionally accepts only a
    generic context mapping and returns generic observation mappings so the
    EvidenceSource remains independent from any exchange API shape.
    """

    def fetch_liquidations(self, context: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        """Return liquidation observations for the supplied context."""
        ...


@dataclass(frozen=True)
class LiquidationEvidenceSource(EvidenceSource):
    """Convert provider-supplied liquidation observations into Evidence.

    The source does not perform network I/O itself. Market access is delegated
    to an injected provider implementing :class:`LiquidationProvider`, keeping
    exchange adapters pluggable and testable.
    """

    _source_id: str
    _provider: LiquidationProvider
    _large_size: Decimal
    _source_quality: str
    _reproducibility: str

    def __init__(
        self,
        source_id: str,
        provider: LiquidationProvider,
        *,
        large_size: float | Decimal | str = "100000",
        source_quality: str = "market_data_provider",
        reproducibility: str = "provider_snapshot",
    ) -> None:
        if not isinstance(source_id, str) or not source_id:
            raise RequiredFieldError("source_id is required")
        if not isinstance(provider, LiquidationProvider):
            raise RequiredFieldError("provider must implement fetch_liquidations(context)")
        if not isinstance(source_quality, str) or not source_quality:
            raise RequiredFieldError("source_quality is required")
        if not isinstance(reproducibility, str) or not reproducibility:
            raise RequiredFieldError("reproducibility is required")

        normalized_large_size = self._to_decimal(large_size, "large_size")
        if normalized_large_size <= 0:
            raise RequiredFieldError("large_size must be positive")

        object.__setattr__(self, "_source_id", source_id)
        object.__setattr__(self, "_provider", provider)
        object.__setattr__(self, "_large_size", normalized_large_size)
        object.__setattr__(self, "_source_quality", source_quality)
        object.__setattr__(self, "_reproducibility", reproducibility)

    @property
    def source_id(self) -> str:
        """Stable identifier for this liquidation evidence producer."""
        return self._source_id

    def produce_evidence(self, context: Mapping[str, Any]) -> Iterable[Evidence]:
        """Fetch liquidation observations from the provider and emit Evidence."""
        return tuple(
            self._observation_to_evidence(observation)
            for observation in self._provider.fetch_liquidations(context)
        )

    def _observation_to_evidence(self, observation: Mapping[str, Any]) -> Evidence:
        if not isinstance(observation, Mapping):
            raise RequiredFieldError("liquidation observation must be a mapping")

        observation_id = observation.get("observation_id", observation.get("id"))
        if not isinstance(observation_id, str) or not observation_id:
            raise RequiredFieldError("liquidation observation id is required")

        side = self._extract_side(observation)
        size = self._extract_decimal(observation, ("size", "quantity", "qty"), "size")
        price = self._extract_decimal(observation, ("price",), "price")
        timestamp = self._extract_text(observation, "timestamp")
        symbol = self._extract_text(observation, "symbol")
        exchange = self._extract_text(observation, "exchange")
        confidence = min(1.0, float(abs(size) / self._large_size))
        evidence_payload = {
            "source_id": self.source_id,
            "observation_id": observation_id,
            "timestamp": timestamp,
            "side": side,
            "size": str(size),
            "price": str(price),
            "symbol": symbol,
            "exchange": exchange,
        }

        return Evidence(
            deterministic_id("ev_liquidation", evidence_payload),
            (observation_id,),
            confidence,
            str(observation.get("source_quality", self._source_quality)),
            str(observation.get("reproducibility", self._reproducibility)),
        )

    def _extract_side(self, observation: Mapping[str, Any]) -> str:
        raw_side = self._extract_text(observation, "side").lower()
        if raw_side not in {"long", "short"}:
            raise RequiredFieldError("side must be long or short")
        return raw_side

    def _extract_decimal(self, observation: Mapping[str, Any], fields: tuple[str, ...], label: str) -> Decimal:
        for field in fields:
            if field in observation:
                return self._to_decimal(observation[field], field)
        raise RequiredFieldError(f"{label} is required")

    @staticmethod
    def _extract_text(observation: Mapping[str, Any], field: str) -> str:
        value = observation.get(field)
        if not isinstance(value, str) or not value:
            raise RequiredFieldError(f"{field} is required")
        return value

    @staticmethod
    def _to_decimal(value: Any, field: str) -> Decimal:
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise RequiredFieldError(f"{field} must be numeric") from None
