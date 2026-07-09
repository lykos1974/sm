from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Protocol, runtime_checkable

from .evidence import Evidence
from .evidence_source import EvidenceSource
from .exceptions import RequiredFieldError
from .ids import deterministic_id


@runtime_checkable
class OpenInterestProvider(Protocol):
    """Provider interface for exchange-agnostic open-interest observations.

    Concrete adapters may call Binance, MEXC, cached files, databases, or any
    other market-data backend. This protocol intentionally accepts only a
    generic context mapping and returns generic observation mappings so the
    EvidenceSource remains independent from any exchange API shape.
    """

    def fetch_open_interest(self, context: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        """Return open-interest observations for the supplied context."""
        ...


@dataclass(frozen=True)
class OpenInterestEvidenceSource(EvidenceSource):
    """Convert provider-supplied open-interest observations into Evidence.

    The source does not perform network I/O itself. Market access is delegated
    to an injected provider implementing :class:`OpenInterestProvider`, keeping
    exchange adapters pluggable and testable.
    """

    _source_id: str
    _provider: OpenInterestProvider
    _extreme_change: Decimal
    _source_quality: str
    _reproducibility: str

    def __init__(
        self,
        source_id: str,
        provider: OpenInterestProvider,
        *,
        extreme_change: float | Decimal | str = "0.10",
        source_quality: str = "market_data_provider",
        reproducibility: str = "provider_snapshot",
    ) -> None:
        if not isinstance(source_id, str) or not source_id:
            raise RequiredFieldError("source_id is required")
        if not isinstance(provider, OpenInterestProvider):
            raise RequiredFieldError("provider must implement fetch_open_interest(context)")
        if not isinstance(source_quality, str) or not source_quality:
            raise RequiredFieldError("source_quality is required")
        if not isinstance(reproducibility, str) or not reproducibility:
            raise RequiredFieldError("reproducibility is required")

        normalized_extreme_change = self._to_decimal(extreme_change, "extreme_change")
        if normalized_extreme_change <= 0:
            raise RequiredFieldError("extreme_change must be positive")

        object.__setattr__(self, "_source_id", source_id)
        object.__setattr__(self, "_provider", provider)
        object.__setattr__(self, "_extreme_change", normalized_extreme_change)
        object.__setattr__(self, "_source_quality", source_quality)
        object.__setattr__(self, "_reproducibility", reproducibility)

    @property
    def source_id(self) -> str:
        """Stable identifier for this open-interest evidence producer."""
        return self._source_id

    def produce_evidence(self, context: Mapping[str, Any]) -> Iterable[Evidence]:
        """Fetch open-interest observations from the provider and emit Evidence."""
        return tuple(
            self._observation_to_evidence(observation)
            for observation in self._provider.fetch_open_interest(context)
        )

    def _observation_to_evidence(self, observation: Mapping[str, Any]) -> Evidence:
        if not isinstance(observation, Mapping):
            raise RequiredFieldError("open-interest observation must be a mapping")

        observation_id = observation.get("observation_id", observation.get("id"))
        if not isinstance(observation_id, str) or not observation_id:
            raise RequiredFieldError("open-interest observation id is required")

        change = self._extract_change(observation)
        confidence = min(1.0, float(abs(change) / self._extreme_change))
        evidence_payload = {
            "source_id": self.source_id,
            "observation_id": observation_id,
            "open_interest_change": str(change),
            "polarity": "positive" if change > 0 else "negative" if change < 0 else "neutral",
        }

        return Evidence(
            deterministic_id("ev_open_interest", evidence_payload),
            (observation_id,),
            confidence,
            str(observation.get("source_quality", self._source_quality)),
            str(observation.get("reproducibility", self._reproducibility)),
        )

    def _extract_change(self, observation: Mapping[str, Any]) -> Decimal:
        if "open_interest_change" in observation:
            return self._to_decimal(observation["open_interest_change"], "open_interest_change")
        if "change" in observation:
            return self._to_decimal(observation["change"], "change")
        if "open_interest_delta" in observation:
            return self._to_decimal(observation["open_interest_delta"], "open_interest_delta")
        raise RequiredFieldError("open_interest_change is required")

    @staticmethod
    def _to_decimal(value: Any, field: str) -> Decimal:
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise RequiredFieldError(f"{field} must be numeric") from None
