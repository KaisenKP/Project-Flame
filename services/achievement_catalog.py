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


ACHIEVEMENT_CATALOG: dict[str, AchievementDefinition] = {}


def _add(
    *,
    achievement_key: str,
    name: str,
    description: str,
    category: str,
    tier: AchievementTier,
    icon: str,
    flavor_text: str,
    unlock_condition: str,
    sort_order: int,
    is_hidden: bool = False,
) -> None:
    ACHIEVEMENT_CATALOG[achievement_key] = AchievementDefinition(
        achievement_key=achievement_key,
        name=name,
        description=description,
        category=category,
        tier=tier,
        icon=icon,
        flavor_text=flavor_text,
        unlock_condition=unlock_condition,
        is_hidden=is_hidden,
        sort_order=sort_order,
    )


def _build_catalog() -> None:
    order = 10

    _add(
        achievement_key="first_job",
        name="Clocked In",
        description="Complete your first job.",
        category="jobs",
        tier=AchievementTier.COMMON,
        icon="⚒️",
        flavor_text="first shift complete, payroll unlocked.",
        unlock_condition="jobs_completed >= 1",
        sort_order=order,
    )
    order += 10

    _add(
        achievement_key="first_business",
        name="Founder Era",
        description="Purchase your first business.",
        category="business",
        tier=AchievementTier.RARE,
        icon="🏢",
        flavor_text="ceo arc has officially begun.",
        unlock_condition="businesses_owned >= 1",
        sort_order=order,
    )
    order += 10

    _add(
        achievement_key="millionaire",
        name="Silver Millionaire",
        description="Reach 1,000,000 silver.",
        category="economy",
        tier=AchievementTier.EPIC,
        icon="💸",
        flavor_text="wallet looking kinda disrespectful.",
        unlock_condition="wallet_silver >= 1_000_000",
        sort_order=order,
    )
    order += 10

    _add(
        achievement_key="grind_master",
        name="Grind Master",
        description="Complete 100 jobs.",
        category="jobs",
        tier=AchievementTier.EPIC,
        icon="🔥",
        flavor_text="built different. sustained output.",
        unlock_condition="jobs_completed >= 100",
        sort_order=order,
    )
    order += 10

    _add(
        achievement_key="xp_addict",
        name="XP Addict",
        description="Reach level 50.",
        category="xp",
        tier=AchievementTier.LEGENDARY,
        icon="🧠",
        flavor_text="touching grass is optional now.",
        unlock_condition="level >= 50",
        sort_order=order,
    )
    order += 10

    _add(
        achievement_key="wealth_lord",
        name="Wealth Lord",
        description="Reach 10,000,000 net worth.",
        category="economy",
        tier=AchievementTier.MYTHIC,
        icon="👑",
        flavor_text="economy final boss behavior.",
        unlock_condition="net_worth >= 10_000_000",
        sort_order=order,
    )
    order += 10

    _add(
        achievement_key="first_selfie",
        name="Main Feed Debut",
        description="Post your first selfie in <#1460859587275001866>.",
        category="social",
        tier=AchievementTier.COMMON,
        icon="🤳",
        flavor_text="camera roll approved. comments pending.",
        unlock_condition="selfies_posted >= 1",
        sort_order=order,
    )
    order += 10

    _add(
        achievement_key="chatroom_100",
        name="Chronic Chatter",
        description="Type 100 messages in <#1460856536795578443>.",
        category="social",
        tier=AchievementTier.RARE,
        icon="💬",
        flavor_text="keyboard keys asking for overtime pay.",
        unlock_condition="chatroom_messages >= 100",
        sort_order=order,
    )
    order += 10

    message_goals = [1, 10, 25, 50, 100, 250, 500, 750, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 75_000, 100_000]
    for idx, goal in enumerate(message_goals, start=1):
        tier = (
            AchievementTier.COMMON
            if goal <= 100
            else AchievementTier.RARE
            if goal <= 1_000
            else AchievementTier.EPIC
            if goal <= 10_000
            else AchievementTier.LEGENDARY
        )
        if goal >= 50_000:
            tier = AchievementTier.MYTHIC
        _add(
            achievement_key=f"messages_{goal}",
            name=f"Chat Streak {idx}",
            description=f"Send {goal:,} total messages.",
            category="social",
            tier=tier,
            icon="🗨️",
            flavor_text=f"message grind level {idx}. timeline never sleeping.",
            unlock_condition=f"messages_sent >= {goal}",
            sort_order=order,
        )
        order += 10

    job_goals = [5, 10, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1_000, 1_500, 2_000]
    for idx, goal in enumerate(job_goals, start=1):
        tier = (
            AchievementTier.COMMON
            if goal <= 25
            else AchievementTier.RARE
            if goal <= 100
            else AchievementTier.EPIC
            if goal <= 300
            else AchievementTier.LEGENDARY
        )
        if goal >= 1_000:
            tier = AchievementTier.MYTHIC
        _add(
            achievement_key=f"jobs_{goal}",
            name=f"Work Arc {idx}",
            description=f"Complete {goal:,} jobs.",
            category="jobs",
            tier=tier,
            icon="🛠️",
            flavor_text="clock in. lock in. stack wins.",
            unlock_condition=f"jobs_completed >= {goal}",
            sort_order=order,
        )
        order += 10

    business_goals = [1, 2, 3, 5, 8, 10, 15, 20, 30, 40]
    for idx, goal in enumerate(business_goals, start=1):
        tier = (
            AchievementTier.RARE
            if goal <= 3
            else AchievementTier.EPIC
            if goal <= 10
            else AchievementTier.LEGENDARY
        )
        if goal >= 20:
            tier = AchievementTier.MYTHIC
        _add(
            achievement_key=f"businesses_{goal}",
            name=f"Ownership Arc {idx}",
            description=f"Own {goal} businesses.",
            category="business",
            tier=tier,
            icon="🏬",
            flavor_text="portfolio looking loud.",
            unlock_condition=f"businesses_owned >= {goal}",
            sort_order=order,
        )
        order += 10

    level_goals = [5, 10, 15, 20, 25, 30, 40, 50, 60, 75, 100, 125, 150]
    for idx, goal in enumerate(level_goals, start=1):
        tier = (
            AchievementTier.COMMON
            if goal <= 15
            else AchievementTier.RARE
            if goal <= 30
            else AchievementTier.EPIC
            if goal <= 60
            else AchievementTier.LEGENDARY
        )
        if goal >= 100:
            tier = AchievementTier.MYTHIC
        _add(
            achievement_key=f"level_{goal}",
            name=f"XP Evolution {idx}",
            description=f"Reach level {goal}.",
            category="xp",
            tier=tier,
            icon="✨",
            flavor_text="level up animation on loop.",
            unlock_condition=f"level >= {goal}",
            sort_order=order,
        )
        order += 10

    wallet_goals = [10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000, 100_000_000]
    for idx, goal in enumerate(wallet_goals, start=1):
        tier = (
            AchievementTier.COMMON
            if goal <= 100_000
            else AchievementTier.RARE
            if goal <= 1_000_000
            else AchievementTier.EPIC
            if goal <= 5_000_000
            else AchievementTier.LEGENDARY
        )
        if goal >= 25_000_000:
            tier = AchievementTier.MYTHIC
        _add(
            achievement_key=f"wallet_{goal}",
            name=f"Bag Status {idx}",
            description=f"Hold {goal:,} silver in wallet.",
            category="economy",
            tier=tier,
            icon="💰",
            flavor_text="finance arc moving respectfully.",
            unlock_condition=f"wallet_silver >= {goal}",
            sort_order=order,
        )
        order += 10

    net_worth_goals = [100_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000, 100_000_000, 250_000_000, 500_000_000]
    for idx, goal in enumerate(net_worth_goals, start=1):
        tier = (
            AchievementTier.RARE
            if goal <= 1_000_000
            else AchievementTier.EPIC
            if goal <= 10_000_000
            else AchievementTier.LEGENDARY
            if goal <= 50_000_000
            else AchievementTier.MYTHIC
        )
        _add(
            achievement_key=f"networth_{goal}",
            name=f"Empire Value {idx}",
            description=f"Reach {goal:,} total net worth.",
            category="economy",
            tier=tier,
            icon="🏦",
            flavor_text="assets plus aura equals huge numbers.",
            unlock_condition=f"net_worth >= {goal}",
            sort_order=order,
        )
        order += 10

    chatroom_goals = [10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000]
    for idx, goal in enumerate(chatroom_goals, start=1):
        tier = (
            AchievementTier.COMMON
            if goal <= 50
            else AchievementTier.RARE
            if goal <= 250
            else AchievementTier.EPIC
            if goal <= 1_000
            else AchievementTier.LEGENDARY
        )
        if goal >= 2_500:
            tier = AchievementTier.MYTHIC
        _add(
            achievement_key=f"chatroom_{goal}",
            name=f"Chatroom Camper {idx}",
            description=f"Type {goal:,} messages in <#1460856536795578443>.",
            category="social",
            tier=tier,
            icon="📱",
            flavor_text="chatroom residency almost permanent.",
            unlock_condition=f"chatroom_messages >= {goal}",
            sort_order=order,
        )
        order += 10

    selfie_goals = [1, 3, 5, 10, 25, 50, 100]
    for idx, goal in enumerate(selfie_goals, start=1):
        tier = (
            AchievementTier.COMMON
            if goal <= 3
            else AchievementTier.RARE
            if goal <= 10
            else AchievementTier.EPIC
            if goal <= 25
            else AchievementTier.LEGENDARY
        )
        if goal >= 50:
            tier = AchievementTier.MYTHIC
        _add(
            achievement_key=f"selfies_{goal}",
            name=f"Selfie Saga {idx}",
            description=f"Post {goal} selfies in <#1460859587275001866>.",
            category="social",
            tier=tier,
            icon="📸",
            flavor_text="angles locked. lighting immaculate.",
            unlock_condition=f"selfies_posted >= {goal}",
            sort_order=order,
        )
        order += 10


_build_catalog()


def sorted_achievements() -> list[AchievementDefinition]:
    return sorted(ACHIEVEMENT_CATALOG.values(), key=lambda a: (a.sort_order, a.name.lower()))
