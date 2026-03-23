from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import BankRobberyCooldownRow, BankRobberyProfileRow


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def get_or_create_profile(session: AsyncSession, *, guild_id: int, user_id: int) -> BankRobberyProfileRow:
    row = await session.scalar(select(BankRobberyProfileRow).where(BankRobberyProfileRow.guild_id == guild_id, BankRobberyProfileRow.user_id == user_id))
    if row is None:
        row = BankRobberyProfileRow(guild_id=guild_id, user_id=user_id)
        session.add(row)
        await session.flush()
    return row


async def get_cooldowns(session: AsyncSession, *, guild_id: int, user_id: int) -> list[BankRobberyCooldownRow]:
    result = await session.execute(
        select(BankRobberyCooldownRow)
        .where(BankRobberyCooldownRow.guild_id == guild_id, BankRobberyCooldownRow.user_id == user_id)
        .order_by(BankRobberyCooldownRow.ends_at.asc())
    )
    return list(result.scalars())
