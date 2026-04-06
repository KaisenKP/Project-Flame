from __future__ import annotations

import unittest

from cogs.Business.core import _manager_slots_for_level, _worker_slots_for_business_key_and_level


class BusinessWorkerSlotScalingTests(unittest.TestCase):
    def test_worker_slots_scale_with_level_and_prestige(self) -> None:
        self.assertEqual(_worker_slots_for_business_key_and_level("nightclub", 0, prestige=0), 3)
        self.assertEqual(_worker_slots_for_business_key_and_level("nightclub", 10, prestige=0), 8)
        self.assertEqual(_worker_slots_for_business_key_and_level("nightclub", 10, prestige=2), 10)

    def test_worker_slots_outpace_manager_slots_at_high_level(self) -> None:
        level = 65
        worker_slots = _worker_slots_for_business_key_and_level("nightclub", level, prestige=0)
        manager_slots = _manager_slots_for_level(level)
        self.assertGreater(worker_slots, manager_slots)
        self.assertEqual(manager_slots, 14)
        self.assertEqual(worker_slots, 35)

    def test_legacy_floor_is_preserved(self) -> None:
        slots = _worker_slots_for_business_key_and_level("nightclub", 1, prestige=0, legacy_floor=40)
        self.assertEqual(slots, 40)


if __name__ == "__main__":
    unittest.main()
