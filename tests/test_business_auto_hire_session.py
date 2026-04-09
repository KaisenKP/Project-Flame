from __future__ import annotations

import unittest

from db.models import BusinessAutoHireSessionRow


class BusinessAutoHireSessionRowTests(unittest.TestCase):
    def test_rerolls_unlimited_when_negative(self) -> None:
        row = BusinessAutoHireSessionRow(staff_kind="manager", remaining_rerolls=-1)
        self.assertTrue(row.rerolls_unlimited)
        self.assertTrue(row.vip_auto_reroll_enabled)
        self.assertTrue(row.can_reroll())

    def test_rerolls_not_available_when_zero(self) -> None:
        row = BusinessAutoHireSessionRow(staff_kind="manager", remaining_rerolls=0)
        self.assertFalse(row.rerolls_unlimited)
        self.assertFalse(row.vip_auto_reroll_enabled)
        self.assertFalse(row.can_reroll())

    def test_rarity_filter_normalization_supports_legendary_plus_and_typo_alias(self) -> None:
        normalized = BusinessAutoHireSessionRow.normalize_rarity_filter_tokens(["legendary+", "legendsry", "epic", "junk"])
        self.assertEqual(normalized, {"epic", "legendary", "mythical"})

    def test_negative_rerolls_not_unlimited_for_non_manager_sessions(self) -> None:
        row = BusinessAutoHireSessionRow(staff_kind="worker", remaining_rerolls=-1)
        self.assertFalse(row.rerolls_unlimited)
        self.assertFalse(row.vip_auto_reroll_enabled)
        self.assertFalse(row.can_reroll())

    def test_starting_rerolls_for_vip_auto_session_is_manager_only(self) -> None:
        self.assertEqual(
            BusinessAutoHireSessionRow.starting_rerolls_for_session(staff_kind="manager", vip_auto=True),
            -1,
        )
        self.assertEqual(
            BusinessAutoHireSessionRow.starting_rerolls_for_session(staff_kind="worker", vip_auto=True),
            0,
        )


if __name__ == "__main__":
    unittest.main()
