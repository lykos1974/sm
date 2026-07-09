from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .evidence import Evidence
from .evidence_source import EvidenceSource
from .exceptions import RequiredFieldError


@dataclass(frozen=True)
class StaticEvidenceSource(EvidenceSource):
    """Deterministic EvidenceSource backed by predefined evidence.

    The source snapshots the provided iterable at construction time and returns
    the same Evidence objects, in the same order, for every call. Runtime
    context is accepted to satisfy the EvidenceSource protocol but is not used.
    """

    _source_id: str
    _evidence: tuple[Evidence, ...]

    def __init__(self, source_id: str, evidence: Iterable[Evidence]) -> None:
        if not isinstance(source_id, str) or not source_id:
            raise RequiredFieldError("source_id is required")

        object.__setattr__(self, "_source_id", source_id)
        object.__setattr__(self, "_evidence", tuple(evidence))

    @property
    def source_id(self) -> str:
        """Stable identifier for this static evidence producer."""
        return self._source_id

    def produce_evidence(self, context: Mapping[str, Any]) -> Iterable[Evidence]:
        """Return the predefined Evidence objects unchanged."""
        return self._evidence
