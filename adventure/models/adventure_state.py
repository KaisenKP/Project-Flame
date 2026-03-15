from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

ADVENTURE_STAMINA_COST = 10
PARTY_MAX_SIZE = 4
PARTY_LOBBY_SECONDS = 30

PARTY_REWARD_BONUS_BP = {1: 0, 2: 1500, 3: 3000, 4: 5000}
PARTY_XP_BONUS_BP = {1: 0, 2: 1000, 3: 2000, 4: 3500}
PARTY_LOOTBOX_BONUS_BP = {1: 0, 2: 500, 3: 1000, 4: 1500}


class AdventureClass(str, Enum):
    DRAGON_SLAYER = "dragon_slayer"
    SHADOW_ASSASSIN = "shadow_assassin"
    ARCHMAGE = "archmage"
    STORM_KNIGHT = "storm_knight"
    TREASURE_HUNTER = "treasure_hunter"
    BEAST_TAMER = "beast_tamer"
    BERSERKER = "berserker"


CLASS_LABELS: dict[str, str] = {
    AdventureClass.DRAGON_SLAYER.value: "Dragon Slayer",
    AdventureClass.SHADOW_ASSASSIN.value: "Shadow Assassin",
    AdventureClass.ARCHMAGE.value: "Archmage",
    AdventureClass.STORM_KNIGHT.value: "Storm Knight",
    AdventureClass.TREASURE_HUNTER.value: "Treasure Hunter",
    AdventureClass.BEAST_TAMER.value: "Beast Tamer",
    AdventureClass.BERSERKER.value: "Berserker",
}


class AdventureMode(str, Enum):
    SOLO = "solo"
    PARTY = "party"


class StageTag(str, Enum):
    COMBAT = "combat"
    TREASURE = "treasure"
    MYSTIC = "mystic"
    TRAP = "trap"
    SOCIAL = "social"
    PUZZLE = "puzzle"
    BOSS = "boss"


@dataclass(frozen=True)
class StageTemplate:
    key: str
    title: str
    beats: list[str]
    choices: list[str]
    tag: StageTag
    party_only: bool = False
    min_adv_level: int = 1
