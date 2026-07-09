from __future__ import annotations

from enum import Enum


class ValidationOutcome(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    INCONCLUSIVE = "inconclusive"


class DecisionType(str, Enum):
    ACCEPT = "accept"
    REJECT = "reject"
    DISCARD = "discard"
    DEFER = "defer"
