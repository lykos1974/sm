from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

from .matcher import is_same_item, match_score


@dataclass
class Offer:
    supermarket: str
    category: str
    description: str
    price: float
    unit: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PriceStore:
    def __init__(self) -> None:
        self.offers: list[Offer] = []

    def replace(self, offers: list[Offer]) -> None:
        self.offers = offers

    def grouped_categories(self) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for offer in self.offers:
            grouped.setdefault(offer.category, []).append(offer.to_dict())
        return grouped

    def search(self, query: str) -> list[dict[str, Any]]:
        scored = []
        for offer in self.offers:
            score = match_score(query, offer.description)
            if score >= 0.4:
                item = offer.to_dict()
                item["score"] = round(score, 3)
                scored.append(item)
        return sorted(scored, key=lambda item: item["score"], reverse=True)

    def best_deal_for_item(self, item: str) -> dict[str, Any] | None:
        candidates = [offer for offer in self.offers if is_same_item(item, offer.description)]
        if not candidates:
            return None
        best = min(candidates, key=lambda x: x.price)
        return {
            "requested_item": item,
            "best_offer": best.to_dict(),
            "alternatives": [c.to_dict() for c in sorted(candidates, key=lambda x: x.price)],
        }
