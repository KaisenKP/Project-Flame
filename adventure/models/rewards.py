from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AdventureRewards:
    silver: int = 0
    xp: int = 0
    adventure_xp: int = 0
    stamina_penalty: int = 0
    lootboxes: dict[str, int] = field(default_factory=dict)
    items: dict[str, int] = field(default_factory=dict)

    def merge(self, other: "AdventureRewards") -> None:
        self.silver += int(other.silver)
        self.xp += int(other.xp)
        self.adventure_xp += int(other.adventure_xp)
        self.stamina_penalty += int(other.stamina_penalty)
        for rarity, amt in other.lootboxes.items():
            self.lootboxes[rarity] = self.lootboxes.get(rarity, 0) + int(amt)
        for item_key, amt in other.items.items():
            self.items[item_key] = self.items.get(item_key, 0) + int(amt)
