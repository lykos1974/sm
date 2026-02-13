import unittest

from app.collectors import collect_all_prices
from app.store import PriceStore


class StoreTests(unittest.TestCase):
    def test_best_deals(self) -> None:
        store = PriceStore()
        store.replace(collect_all_prices())
        result = store.best_deal_for_item("γαλα")
        self.assertIsNotNone(result)
        self.assertIn("best_offer", result)
        self.assertGreater(result["best_offer"]["price"], 0)


if __name__ == "__main__":
    unittest.main()
