from __future__ import annotations

import unittest

from db.models import BusinessAutoHireSessionRow


class BusinessAutoHireSessionRowTests(unittest.TestCase):
    def test_rerolls_unlimited_when_negative(self) -> None:
        row = BusinessAutoHireSessionRow(remaining_rerolls=-1)
        self.assertTrue(row.rerolls_unlimited)
        self.assertTrue(row.can_reroll())

    def test_rerolls_not_available_when_zero(self) -> None:
        row = BusinessAutoHireSessionRow(remaining_rerolls=0)
        self.assertFalse(row.rerolls_unlimited)
        self.assertFalse(row.can_reroll())

    def test_rarity_filter_normalization_supports_legendary_plus_and_typo_alias(self) -> None:
        normalized = BusinessAutoHireSessionRow.normalize_rarity_filter_tokens(["legendary+", "legendsry", "epic", "junk"])
        self.assertEqual(normalized, {"epic", "legendary", "mythical"})


if __name__ == "__main__":
    unittest.main()
