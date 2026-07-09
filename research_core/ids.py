from __future__ import annotations

import hashlib
import json
from typing import Any


def deterministic_id(prefix: str, payload: Any) -> str:
    """Return a deterministic identifier for a JSON-serializable payload."""
    if not prefix or not isinstance(prefix, str):
        raise ValueError("prefix is required")
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"
