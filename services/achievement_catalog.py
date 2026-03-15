from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class AchievementTier(StrEnum):
    COMMON = "common"
    RARE = "rare"
    EPIC = "epic"
    LEGENDARY = "legendary"
    MYTHIC = "mythic"


@dataclass(frozen=True, slots=True)
class AchievementDefinition:
    achievement_key: str
    name: str
    description: str
    category: str
    tier: AchievementTier
    icon: str
    flavor_text: str
    unlock_condition: str
    is_hidden: bool
    sort_order: int


ACHIEVEMENT_CATALOG: dict[str, AchievementDefinition] = {
    "first_job": AchievementDefinition(
        achievement_key="first_job",
        name="Clocked In",
        description="Complete your first job.",
        category="jobs",
        tier=AchievementTier.COMMON,
        icon="⚒️",
        flavor_text="first shift complete, payroll unlocked.",
        unlock_condition="jobs_completed >= 1",
        is_hidden=False,
        sort_order=10,
    ),
    "first_business": AchievementDefinition(
        achievement_key="first_business",
        name="Founder Era",
        description="Purchase your first business.",
        category="business",
        tier=AchievementTier.RARE,
        icon="🏢",
        flavor_text="ceo arc has officially begun.",
        unlock_condition="businesses_owned >= 1",
        is_hidden=False,
        sort_order=20,
    ),
    "millionaire": AchievementDefinition(
        achievement_key="millionaire",
        name="Silver Millionaire",
        description="Reach 1,000,000 silver.",
        category="economy",
        tier=AchievementTier.EPIC,
        icon="💸",
        flavor_text="wallet looking kinda disrespectful.",
        unlock_condition="wallet_silver >= 1_000_000",
        is_hidden=False,
        sort_order=30,
    ),
    "grind_master": AchievementDefinition(
        achievement_key="grind_master",
        name="Grind Master",
        description="Complete 100 jobs.",
        category="jobs",
        tier=AchievementTier.EPIC,
        icon="🔥",
        flavor_text="built different. sustained output.",
        unlock_condition="jobs_completed >= 100",
        is_hidden=False,
        sort_order=40,
    ),
    "xp_addict": AchievementDefinition(
        achievement_key="xp_addict",
        name="XP Addict",
        description="Reach level 50.",
        category="xp",
        tier=AchievementTier.LEGENDARY,
        icon="🧠",
        flavor_text="touching grass is optional now.",
        unlock_condition="level >= 50",
        is_hidden=False,
        sort_order=50,
    ),
    "wealth_lord": AchievementDefinition(
        achievement_key="wealth_lord",
        name="Wealth Lord",
        description="Reach 10,000,000 net worth.",
        category="economy",
        tier=AchievementTier.MYTHIC,
        icon="👑",
        flavor_text="economy final boss behavior.",
        unlock_condition="net_worth >= 10_000_000",
        is_hidden=False,
        sort_order=60,
    ),
}


def sorted_achievements() -> list[AchievementDefinition]:
    return sorted(ACHIEVEMENT_CATALOG.values(), key=lambda a: (a.sort_order, a.name.lower()))
