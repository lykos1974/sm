from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from .evidence import Evidence
from .evidence_source import EvidenceSource
from .exceptions import RequiredFieldError


@dataclass(frozen=True)
class FilteringEvidenceSource(EvidenceSource):
    """EvidenceSource wrapper that keeps evidence matching a predicate.

    The wrapped source receives the caller's context unchanged. Returned Evidence
    objects are yielded in their original order and are not copied, deduplicated,
    mutated, or replaced.
    """

    _source_id: str
    source: EvidenceSource
    predicate: Callable[[Evidence], bool]

    def __init__(
        self,
        source_id: str,
        source: EvidenceSource,
        predicate: Callable[[Evidence], bool],
    ) -> None:
        if not isinstance(source_id, str) or not source_id:
            raise RequiredFieldError("source_id is required")
        if not isinstance(source, EvidenceSource):
            raise RequiredFieldError("source is required")
        if not callable(predicate):
            raise RequiredFieldError("predicate is required")

        object.__setattr__(self, "_source_id", source_id)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "predicate", predicate)

    @property
    def source_id(self) -> str:
        """Stable identifier for this filtering evidence producer."""
        return self._source_id

    def produce_evidence(self, context: Mapping[str, Any]) -> Iterable[Evidence]:
        """Return wrapped-source evidence for which the predicate is true."""
        return tuple(
            evidence
            for evidence in self.source.produce_evidence(context)
            if self.predicate(evidence)
        )
