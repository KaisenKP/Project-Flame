from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

MAX_BUSINESS_PRESTIGE = 100
BASE_VISIBLE_LEVEL = 1
LEVELS_PER_PRESTIGE = 10
BULK_UNLOCK_X5_PRESTIGE = 3
BULK_UNLOCK_X10_PRESTIGE = 10


@dataclass(frozen=True, slots=True)
class PrestigeConfig:
    base_cost: int
    growth_rate: str
    revenue_per_hour: int = 0
    revenue_hours_multiplier: int = 0


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
    # Prestige was previously compounding at +50% every tier, which pushed
    # starter businesses into runaway late-game income. Keep prestige valuable,
    # but flatten it to a predictable +6.5% per tier so P10 restaurants land in
    # the ~20k-30k/hr range before staffing bonuses.
    return Decimal("1.0") + (Decimal("0.065") * Decimal(p))


def prestige_multiplier_display(prestige: int) -> str:
    multiplier = prestige_multiplier(prestige)
    normalized = multiplier.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def prestige_cost(*, config: PrestigeConfig, current_prestige: int) -> int:
    p = clamp_prestige(current_prestige)
    base_cost = max(int(config.base_cost), 0)
    flat_step = 25_000
    cost = base_cost + (flat_step * p)
    return max(cost, 1)


def bulk_option_for(prestige: int, amount: int) -> BulkUpgradeOption:
    p = clamp_prestige(prestige)
    if amount <= 1:
        return BulkUpgradeOption(amount=1, unlocked=True)
    if amount == 5:
        return BulkUpgradeOption(amount=5, unlocked=p >= BULK_UNLOCK_X5_PRESTIGE)
    if amount == 10:
        return BulkUpgradeOption(amount=10, unlocked=p >= BULK_UNLOCK_X10_PRESTIGE)
    return BulkUpgradeOption(amount=int(amount), unlocked=False)
