from __future__ import annotations

import unittest

from db.models import BusinessAutoHireSessionRow


class BusinessManagerRerollCostTests(unittest.TestCase):
    def test_restaurant_is_fixed_baseline(self) -> None:
        prices = {"restaurant": 12_345, "nightclub": 120_000}
        self.assertEqual(
            BusinessAutoHireSessionRow.manager_reroll_cost_for_business("restaurant", prices),
            1_000,
        )

    def test_scales_from_business_price_ratio(self) -> None:
        prices = {"restaurant": 10_000, "nightclub": 100_000}
        self.assertEqual(
            BusinessAutoHireSessionRow.manager_reroll_cost_for_business("nightclub", prices),
            10_000,
        )

    def test_rounds_half_up_to_stable_integer(self) -> None:
        prices = {"restaurant": 3, "food_truck": 2}
        # 1000 * (2 / 3) = 666.666... -> 667
        self.assertEqual(
            BusinessAutoHireSessionRow.manager_reroll_cost_for_business("food_truck", prices),
            667,
        )

    def test_caps_at_hard_limit(self) -> None:
        prices = {"restaurant": 1, "mega_corp": 500_000_000}
        self.assertEqual(
            BusinessAutoHireSessionRow.manager_reroll_cost_for_business("mega_corp", prices),
            10_000_000,
        )

    def test_requires_restaurant_price(self) -> None:
        with self.assertRaises(ValueError):
            BusinessAutoHireSessionRow.manager_reroll_cost_for_business(
                "nightclub",
                {"nightclub": 10_000},
            )


if __name__ == "__main__":
    unittest.main()
