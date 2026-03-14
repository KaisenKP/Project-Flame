from __future__ import annotations

from typing import Dict, Tuple

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import UserRow


_CACHE_KEY = "pulse_user_cache"


async def get_or_create_user(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
) -> UserRow:
    """
    Safe user bootstrap.
    - Prevents duplicate pending inserts inside the same session.
    - Handles race conditions across concurrent requests.
    """
    cache: Dict[Tuple[int, int], UserRow] = session.info.setdefault(_CACHE_KEY, {})
    key = (guild_id, user_id)
    cached = cache.get(key)
    if cached is not None:
        return cached

    existing = await session.scalar(
        select(UserRow).where(
            UserRow.guild_id == guild_id,
            UserRow.user_id == user_id,
        )
    )
    if existing is not None:
        cache[key] = existing
        return existing

    row = UserRow(guild_id=guild_id, user_id=user_id)
    session.add(row)

    try:
        async with session.begin_nested():
            await session.flush()
    except IntegrityError:
        existing2 = await session.scalar(
            select(UserRow).where(
                UserRow.guild_id == guild_id,
                UserRow.user_id == user_id,
            )
        )
        if existing2 is None:
            raise
        cache[key] = existing2
        return existing2

    cache[key] = row
    return row


async def ensure_user_rows(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
) -> UserRow:
    """
    Backwards-compatible alias used by older code/cogs.
    """
    return await get_or_create_user(session, guild_id=guild_id, user_id=user_id)
