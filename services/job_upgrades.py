from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import JobRow, UserJobUpgradeRow, WalletRow
from services.jobs_core import JobCategory, JobDef, fmt_int

COST_GROWTH = 1.5
INCOME_GROWTH_PER_LEVEL = 0.25

BASE_UPGRADE_COST_BY_CATEGORY: Dict[JobCategory, int] = {
    JobCategory.EASY: 25,
    JobCategory.STABLE: 125,
    JobCategory.HARD: 500,
}

JOB_UPGRADE_LABELS: Dict[str, str] = {
    "miner": "Pickaxe",
    "fisherman": "Fishing Rod",
    "lumberjack": "Axe",
    "messenger": "Delivery Bag",
    "cook": "Kitchen Set",
    "farmer": "Tractor",
    "blacksmith": "Forge",
    "president": "Campaign Office",
    "streamer": "Streaming Rig",
    "pirate": "Ship",
    "robber": "Heist Kit",
    "swordsman": "Blade",
    "influencer": "Brand Studio",
    "bounty_hunter": "Bounty Gear",
    "onlychat_model": "Content Studio",
}


@dataclass(frozen=True)
class JobUpgradeSnapshot:
    level: int
    income_multiplier: float
    income_bonus_pct: int
    current_cost: int
    next_cost: int
    label: str


def _pow_growth(base: int, level: int) -> int:
    return max(int(round(float(base) * (COST_GROWTH ** max(int(level), 0)))), 1)


def upgrade_label(job_key: str, fallback_job_name: str) -> str:
    return JOB_UPGRADE_LABELS.get((job_key or "").strip().lower(), f"{fallback_job_name} Gear")


def current_upgrade_cost(job_def: JobDef, level: int) -> int:
    base = BASE_UPGRADE_COST_BY_CATEGORY.get(job_def.category, 25)
    return _pow_growth(base, level)


def income_multiplier_for_level(level: int) -> float:
    lvl = max(int(level), 0)
    return 1.0 + (INCOME_GROWTH_PER_LEVEL * lvl)


def apply_income_upgrade(base_income: int, upgrade_level: int) -> int:
    value = max(int(base_income), 0)
    multi = income_multiplier_for_level(upgrade_level)
    return max(int(round(value * multi)), 0)


async def get_or_create_upgrade_row(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_id: int,
) -> UserJobUpgradeRow:
    q = await session.execute(
        select(UserJobUpgradeRow).where(
            UserJobUpgradeRow.guild_id == guild_id,
            UserJobUpgradeRow.user_id == user_id,
            UserJobUpgradeRow.job_id == job_id,
        )
    )
    row = q.scalar_one_or_none()
    if row is not None:
        return row

    row = UserJobUpgradeRow(
        guild_id=guild_id,
        user_id=user_id,
        job_id=job_id,
        upgrade_level=0,
        silver_spent=0,
    )
    session.add(row)
    await session.flush()
    return row


async def get_upgrade_level(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_id: int,
) -> int:
    row = await get_or_create_upgrade_row(session, guild_id=guild_id, user_id=user_id, job_id=job_id)
    return max(int(getattr(row, "upgrade_level", 0) or 0), 0)


async def build_upgrade_snapshot(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_row: JobRow,
    job_def: JobDef,
) -> JobUpgradeSnapshot:
    row = await get_or_create_upgrade_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        job_id=int(job_row.id),
    )
    level = max(int(row.upgrade_level or 0), 0)
    multiplier = income_multiplier_for_level(level)
    next_cost = current_upgrade_cost(job_def, level)
    current_cost = 0 if level <= 0 else current_upgrade_cost(job_def, level - 1)
    return JobUpgradeSnapshot(
        level=level,
        income_multiplier=multiplier,
        income_bonus_pct=max(int(round((multiplier - 1.0) * 100)), 0),
        current_cost=current_cost,
        next_cost=next_cost,
        label=upgrade_label(job_def.key, job_def.name),
    )


async def upgrade_once(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_row: JobRow,
    job_def: JobDef,
) -> tuple[bool, str, JobUpgradeSnapshot]:
    row = await get_or_create_upgrade_row(session, guild_id=guild_id, user_id=user_id, job_id=int(job_row.id))
    level = max(int(row.upgrade_level or 0), 0)
    cost = current_upgrade_cost(job_def, level)

    wq = await session.execute(
        select(WalletRow)
        .where(
            WalletRow.guild_id == guild_id,
            WalletRow.user_id == user_id,
        )
        .with_for_update()
    )
    wallet = wq.scalar_one_or_none()
    if wallet is None:
        wallet = WalletRow(guild_id=guild_id, user_id=user_id, silver=0, diamonds=0)
        session.add(wallet)
        await session.flush()

    current_silver = int(getattr(wallet, "silver", 0) or 0)
    if current_silver < cost:
        snap = await build_upgrade_snapshot(session, guild_id=guild_id, user_id=user_id, job_row=job_row, job_def=job_def)
        msg = f"Not enough silver. You need **{fmt_int(cost)}** silver for the next {snap.label} upgrade."
        return False, msg, snap

    wallet.silver = current_silver - cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent = int(getattr(wallet, "silver_spent", 0) or 0) + cost

    row.upgrade_level = level + 1
    row.silver_spent = int(getattr(row, "silver_spent", 0) or 0) + cost

    snap = await build_upgrade_snapshot(session, guild_id=guild_id, user_id=user_id, job_row=job_row, job_def=job_def)
    msg = (
        f"{snap.label} upgraded to **Lv {snap.level}** for **{fmt_int(cost)}** silver. "
        f"Income bonus is now **+{snap.income_bonus_pct}%**."
    )
    return True, msg, snap


async def play_upgrade_animation(message, *, label: str) -> None:
    frames = [
        f"⚙️ Upgrading **{label}** `[`░░░░░`]`",
        f"⚙️ Upgrading **{label}** `[`█░░░░`]`",
        f"⚙️ Upgrading **{label}** `[`███░░`]`",
        f"⚙️ Upgrading **{label}** `[`█████`]` ✅",
    ]
    for idx, frame in enumerate(frames):
        await message.edit(content=frame)
        if idx < len(frames) - 1:
            await asyncio.sleep(0.35)
