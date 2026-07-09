from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from .evidence import Evidence
from .evidence_source import EvidenceSource
from .exceptions import RequiredFieldError


@dataclass(frozen=True)
class FilteringEvidenceSource(EvidenceSource):
    """EvidenceSource decorator that filters evidence from a wrapped source.

    The wrapper forwards runtime context to the wrapped source, evaluates each
    emitted Evidence object with the supplied predicate, and returns only the
    matching Evidence objects in their original relative order. Evidence objects
    are yielded unchanged; no market data access or trading decisions occur here.
    """

    _source_id: str
    _source: EvidenceSource
    _predicate: Callable[[Evidence], bool]

    def __init__(
        self,
        source_id: str,
        source: EvidenceSource,
        predicate: Callable[[Evidence], bool],
    ) -> None:
        if not isinstance(source_id, str) or not source_id:
            raise RequiredFieldError("source_id is required")
        if not callable(predicate):
            raise RequiredFieldError("predicate is required")

        object.__setattr__(self, "_source_id", source_id)
        object.__setattr__(self, "_source", source)
        object.__setattr__(self, "_predicate", predicate)

    @property
    def source_id(self) -> str:
        """Stable identifier for this filtering evidence producer."""
        return self._source_id

    def produce_evidence(self, context: Mapping[str, Any]) -> Iterable[Evidence]:
        """Return wrapped Evidence objects that satisfy the predicate."""
        return tuple(
            evidence
            for evidence in self._source.produce_evidence(context)
            if self._predicate(evidence)
        )
