from __future__ import annotations

import unittest

from cogs.Business.core import _manager_slots_for_level, _worker_slots_for_business_key_and_level


class BusinessWorkerSlotScalingTests(unittest.TestCase):
    def test_worker_slots_scale_with_prestige_only(self) -> None:
        self.assertEqual(_worker_slots_for_business_key_and_level("nightclub", 0, prestige=0), 3)
        self.assertEqual(_worker_slots_for_business_key_and_level("nightclub", 10, prestige=0), 3)
        self.assertEqual(_worker_slots_for_business_key_and_level("nightclub", 10, prestige=2), 5)

    def test_manager_slots_scale_with_prestige_only(self) -> None:
        self.assertEqual(_manager_slots_for_level(0, prestige=0), 3)
        self.assertEqual(_manager_slots_for_level(65, prestige=0), 3)
        self.assertEqual(_manager_slots_for_level(65, prestige=4), 7)

    def test_legacy_floors_are_preserved(self) -> None:
        worker_slots = _worker_slots_for_business_key_and_level("nightclub", 1, prestige=0, legacy_floor=40)
        manager_slots = _manager_slots_for_level(1, prestige=0, legacy_floor=12)
        self.assertEqual(worker_slots, 40)
        self.assertEqual(manager_slots, 12)


if __name__ == "__main__":
    unittest.main()
