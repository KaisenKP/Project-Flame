# services/items_models.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class ItemRarity(str, Enum):
    COMMON = "common"
    UNCOMMON = "uncommon"
    RARE = "rare"
    MYTHICAL = "mythical"


class EffectStacking(str, Enum):
    ADD = "add"
    REPLACE = "replace"
    REFRESH = "refresh"
    EXTEND = "extend"
    DENY = "deny"


@dataclass(frozen=True)
class EffectDef:
    effect_key: str
    group_key: str
    payload: Dict[str, int]
    duration_seconds: Optional[int] = None
    charges: Optional[int] = None
    stacking: EffectStacking = EffectStacking.ADD


@dataclass(frozen=True)
class ItemDef:
    key: str
    name: str
    rarity: ItemRarity
    price: int
    daily_limit: int
    tradable: bool
    effect: EffectDef
