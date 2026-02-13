from __future__ import annotations

import json
from pathlib import Path

from .store import Offer

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _read_market_file(file_name: str, supermarket: str) -> list[Offer]:
    path = DATA_DIR / file_name
    rows = json.loads(path.read_text(encoding="utf-8"))
    return [
        Offer(
            supermarket=supermarket,
            category=row["category"],
            description=row["description"],
            price=float(row["price"]),
            unit=row.get("unit", "τεμ"),
        )
        for row in rows
    ]


def collect_all_prices() -> list[Offer]:
    offers: list[Offer] = []
    offers.extend(_read_market_file("sklavenitis.json", "Σκλαβενίτης"))
    offers.extend(_read_market_file("ab_vasilopoulos.json", "ΑΒ Βασιλόπουλος"))
    offers.extend(_read_market_file("masoutis.json", "Μασούτης"))
    return offers
