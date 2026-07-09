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

from .enums import DecisionType


@dataclass(frozen=True)
class Decision:
    id: str
    decision_type: DecisionType
    rationale: str
    supporting_validation_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_text(self.id, "id")
        object.__setattr__(self, "decision_type", DecisionType(self.decision_type))
        _require_text(self.rationale, "rationale")
        object.__setattr__(self, "supporting_validation_ids", tuple(self.supporting_validation_ids))
        _require_tuple(self.supporting_validation_ids, "supporting_validation_ids")

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "decision_type": self.decision_type.value, "rationale": self.rationale, "supporting_validation_ids": list(self.supporting_validation_ids)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Decision":
        return cls(**dict(data))
