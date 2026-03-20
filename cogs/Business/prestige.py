from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

MAX_BUSINESS_PRESTIGE = 100
BASE_VISIBLE_LEVEL = 1
LEVELS_PER_PRESTIGE = 10
BULK_UNLOCK_X5_PRESTIGE = 3
BULK_UNLOCK_X10_PRESTIGE = 10


@dataclass(frozen=True, slots=True)
class PrestigeConfig:
    base_cost: int
    growth_rate: str


@dataclass(frozen=True, slots=True)
class BulkUpgradeOption:
    amount: int
    unlocked: bool


def clamp_prestige(prestige: int) -> int:
    return max(0, min(int(prestige), MAX_BUSINESS_PRESTIGE))


def visible_level_for(stored_level: int) -> int:
    return max(int(stored_level), 0) + BASE_VISIBLE_LEVEL


def total_visible_level_for(*, stored_level: int, prestige: int) -> int:
    return (clamp_prestige(prestige) * LEVELS_PER_PRESTIGE) + visible_level_for(stored_level)


def max_visible_level_for_prestige(prestige: int) -> int:
    return (clamp_prestige(prestige) + 1) * LEVELS_PER_PRESTIGE


def max_stored_level_for_prestige(prestige: int) -> int:
    return max_visible_level_for_prestige(prestige) - BASE_VISIBLE_LEVEL


def at_level_cap(*, stored_level: int, prestige: int) -> bool:
    return max(int(stored_level), 0) >= max_stored_level_for_prestige(prestige)


def prestige_multiplier(prestige: int) -> Decimal:
    p = clamp_prestige(prestige)
    return Decimal("1.5") ** Decimal(p)


def prestige_multiplier_display(prestige: int) -> str:
    multiplier = prestige_multiplier(prestige)
    normalized = multiplier.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def prestige_cost(*, config: PrestigeConfig, current_prestige: int) -> int:
    p = clamp_prestige(current_prestige)
    cost = Decimal(int(config.base_cost)) * (Decimal(str(config.growth_rate)) ** Decimal(p))
    return max(int(cost.to_integral_value(rounding=ROUND_HALF_UP)), 1)


def bulk_option_for(prestige: int, amount: int) -> BulkUpgradeOption:
    p = clamp_prestige(prestige)
    if amount <= 1:
        return BulkUpgradeOption(amount=1, unlocked=True)
    if amount == 5:
        return BulkUpgradeOption(amount=5, unlocked=p >= BULK_UNLOCK_X5_PRESTIGE)
    if amount == 10:
        return BulkUpgradeOption(amount=10, unlocked=p >= BULK_UNLOCK_X10_PRESTIGE)
    return BulkUpgradeOption(amount=int(amount), unlocked=False)
