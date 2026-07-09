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
class Observation:
    id: str
    timestamp: str
    source: str
    symbol: str
    timeframe: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        for field in ("id", "timestamp", "source", "symbol", "timeframe"):
            _require_text(getattr(self, field), field)
        object.__setattr__(self, "payload", _freeze(self.payload))

    def __hash__(self) -> int:
        return hash((self.id, self.timestamp, self.source, self.symbol, self.timeframe, _hashable(self.payload)))

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "timestamp": self.timestamp, "source": self.source, "symbol": self.symbol, "timeframe": self.timeframe, "payload": _thaw(self.payload)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Observation":
        return cls(**dict(data))
