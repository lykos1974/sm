import unittest

from app.matcher import is_same_item, match_score


class MatcherTests(unittest.TestCase):
    def test_matches_greek_and_latin_variants(self) -> None:
        self.assertTrue(is_same_item("Ντομάτες", "Tomata ελληνική χύμα"))
        self.assertTrue(is_same_item("γάλα 1 λίτρο", "Fresh milk full 1lt"))

    def test_score_reasonable_for_related_items(self) -> None:
        self.assertGreater(match_score("ρύζι", "Rice Carolina 500 g"), 0.68)


if __name__ == "__main__":
    unittest.main()
