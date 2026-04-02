from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    BankRobberyCooldownRow,
    BankRobberyHistoryRow,
    BankRobberyLobbyRow,
    BankRobberyParticipantRow,
    BankRobberyProfileRow,
    HeistUserStateRow,
)


async def get_or_create_user_state(session: AsyncSession, *, guild_id: int, user_id: int) -> HeistUserStateRow:
    row = await session.scalar(select(HeistUserStateRow).where(HeistUserStateRow.guild_id == guild_id, HeistUserStateRow.user_id == user_id))
    if row is None:
        row = HeistUserStateRow(guild_id=guild_id, user_id=user_id)
        session.add(row)
        await session.flush()
    return row


async def get_or_create_profile(session: AsyncSession, *, guild_id: int, user_id: int) -> BankRobberyProfileRow:
    row = await session.scalar(select(BankRobberyProfileRow).where(BankRobberyProfileRow.guild_id == guild_id, BankRobberyProfileRow.user_id == user_id))
    if row is None:
        row = BankRobberyProfileRow(guild_id=guild_id, user_id=user_id)
        session.add(row)
        await session.flush()
    return row


async def get_user_lobby(session: AsyncSession, *, guild_id: int, user_id: int) -> BankRobberyLobbyRow | None:
    p = await session.scalar(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.guild_id == guild_id, BankRobberyParticipantRow.user_id == user_id))
    if p is None:
        return None
    return await session.get(BankRobberyLobbyRow, p.lobby_id)


async def get_lobby_members(session: AsyncSession, *, lobby_id: int) -> list[BankRobberyParticipantRow]:
    rows = await session.execute(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.lobby_id == lobby_id).order_by(BankRobberyParticipantRow.joined_at.asc()))
    return list(rows.scalars())


async def list_open_lobbies(session: AsyncSession, *, guild_id: int) -> list[BankRobberyLobbyRow]:
    rows = await session.execute(select(BankRobberyLobbyRow).where(BankRobberyLobbyRow.guild_id == guild_id, BankRobberyLobbyRow.status == "open").order_by(BankRobberyLobbyRow.created_at.desc()).limit(15))
    return list(rows.scalars())


async def get_active_cooldowns(session: AsyncSession, *, guild_id: int, user_id: int, now) -> list[BankRobberyCooldownRow]:
    rows = await session.execute(select(BankRobberyCooldownRow).where(BankRobberyCooldownRow.guild_id == guild_id, BankRobberyCooldownRow.user_id == user_id, BankRobberyCooldownRow.ends_at > now).order_by(BankRobberyCooldownRow.ends_at.asc()))
    return list(rows.scalars())


async def clear_lobby_members(session: AsyncSession, *, lobby_id: int) -> None:
    await session.execute(delete(BankRobberyParticipantRow).where(BankRobberyParticipantRow.lobby_id == lobby_id))


async def create_history(session: AsyncSession, **kwargs) -> BankRobberyHistoryRow:
    row = BankRobberyHistoryRow(**kwargs)
    session.add(row)
    await session.flush()
    return row
