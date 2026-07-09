from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from .exceptions import RequiredFieldError


def _require_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise RequiredFieldError(f"{field} is required")


def _freeze(value: Any) -> Any:
    if isinstance(value, MappingProxyType):
        return value
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _thaw(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _hashable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple((k, _hashable(v)) for k, v in value.items())
    if isinstance(value, tuple):
        return tuple(_hashable(item) for item in value)
    return value


def _require_tuple(value: tuple[str, ...], field: str) -> None:
    if not isinstance(value, tuple) or not value or any(not isinstance(item, str) or not item for item in value):
        raise RequiredFieldError(f"{field} is required")

@dataclass(frozen=True)
class Knowledge:
    id: str
    current_state: str
    evidence_lineage: tuple[str, ...]
    confidence_history: tuple[float, ...]

    def __post_init__(self) -> None:
        _require_text(self.id, "id")
        _require_text(self.current_state, "current_state")
        object.__setattr__(self, "evidence_lineage", tuple(self.evidence_lineage))
        object.__setattr__(self, "confidence_history", tuple(self.confidence_history))
        _require_tuple(self.evidence_lineage, "evidence_lineage")
        if not self.confidence_history or any(not isinstance(item, (int, float)) for item in self.confidence_history):
            raise RequiredFieldError("confidence_history is required")

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "current_state": self.current_state, "evidence_lineage": list(self.evidence_lineage), "confidence_history": list(self.confidence_history)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Knowledge":
        return cls(**dict(data))
