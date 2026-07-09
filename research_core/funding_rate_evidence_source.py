from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Protocol, runtime_checkable

from .evidence import Evidence
from .evidence_source import EvidenceSource
from .exceptions import RequiredFieldError
from .ids import deterministic_id


@runtime_checkable
class FundingRateProvider(Protocol):
    """Provider interface for exchange-agnostic funding-rate observations.

    Concrete adapters may call Binance, MEXC, cached files, databases, or any
    other market-data backend. This protocol intentionally accepts only a
    generic context mapping and returns generic observation mappings so the
    EvidenceSource remains independent from any exchange API shape.
    """

    def fetch_funding_rates(self, context: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        """Return funding-rate observations for the supplied context."""
        ...


@dataclass(frozen=True)
class FundingRateEvidenceSource(EvidenceSource):
    """Convert provider-supplied funding-rate observations into Evidence.

    The source does not perform network I/O itself. Market access is delegated
    to an injected provider implementing :class:`FundingRateProvider`, keeping
    exchange adapters pluggable and testable.
    """

    _source_id: str
    _provider: FundingRateProvider
    _extreme_rate: Decimal
    _source_quality: str
    _reproducibility: str

    def __init__(
        self,
        source_id: str,
        provider: FundingRateProvider,
        *,
        extreme_rate: float | Decimal | str = "0.01",
        source_quality: str = "market_data_provider",
        reproducibility: str = "provider_snapshot",
    ) -> None:
        if not isinstance(source_id, str) or not source_id:
            raise RequiredFieldError("source_id is required")
        if not isinstance(provider, FundingRateProvider):
            raise RequiredFieldError("provider must implement fetch_funding_rates(context)")
        if not isinstance(source_quality, str) or not source_quality:
            raise RequiredFieldError("source_quality is required")
        if not isinstance(reproducibility, str) or not reproducibility:
            raise RequiredFieldError("reproducibility is required")

        normalized_extreme_rate = self._to_decimal(extreme_rate, "extreme_rate")
        if normalized_extreme_rate <= 0:
            raise RequiredFieldError("extreme_rate must be positive")

        object.__setattr__(self, "_source_id", source_id)
        object.__setattr__(self, "_provider", provider)
        object.__setattr__(self, "_extreme_rate", normalized_extreme_rate)
        object.__setattr__(self, "_source_quality", source_quality)
        object.__setattr__(self, "_reproducibility", reproducibility)

    @property
    def source_id(self) -> str:
        """Stable identifier for this funding-rate evidence producer."""
        return self._source_id

    def produce_evidence(self, context: Mapping[str, Any]) -> Iterable[Evidence]:
        """Fetch funding observations from the provider and emit Evidence."""
        return tuple(
            self._observation_to_evidence(observation)
            for observation in self._provider.fetch_funding_rates(context)
        )

    def _observation_to_evidence(self, observation: Mapping[str, Any]) -> Evidence:
        if not isinstance(observation, Mapping):
            raise RequiredFieldError("funding-rate observation must be a mapping")

        observation_id = observation.get("observation_id", observation.get("id"))
        if not isinstance(observation_id, str) or not observation_id:
            raise RequiredFieldError("funding-rate observation id is required")

        rate = self._extract_rate(observation)
        confidence = min(1.0, float(abs(rate) / self._extreme_rate))
        evidence_payload = {
            "source_id": self.source_id,
            "observation_id": observation_id,
            "funding_rate": str(rate),
            "polarity": "positive" if rate > 0 else "negative" if rate < 0 else "neutral",
        }

        return Evidence(
            deterministic_id("ev_funding_rate", evidence_payload),
            (observation_id,),
            confidence,
            str(observation.get("source_quality", self._source_quality)),
            str(observation.get("reproducibility", self._reproducibility)),
        )

    def _extract_rate(self, observation: Mapping[str, Any]) -> Decimal:
        if "funding_rate" in observation:
            return self._to_decimal(observation["funding_rate"], "funding_rate")
        if "rate" in observation:
            return self._to_decimal(observation["rate"], "rate")
        raise RequiredFieldError("funding_rate is required")

    @staticmethod
    def _to_decimal(value: Any, field: str) -> Decimal:
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise RequiredFieldError(f"{field} must be numeric") from None
