from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import CrownsWalletRow, LootboxInventoryRow, WalletRow


async def get_or_create_wallet(session: AsyncSession, *, guild_id: int, user_id: int) -> WalletRow:
    row = await session.scalar(select(WalletRow).where(WalletRow.guild_id == guild_id, WalletRow.user_id == user_id))
    if row is None:
        row = WalletRow(guild_id=guild_id, user_id=user_id, silver=0, diamonds=0)
        session.add(row)
        await session.flush()
    return row


async def get_or_create_crowns_wallet(session: AsyncSession, *, guild_id: int, user_id: int) -> CrownsWalletRow:
    row = await session.scalar(select(CrownsWalletRow).where(CrownsWalletRow.guild_id == guild_id, CrownsWalletRow.user_id == user_id))
    if row is None:
        row = CrownsWalletRow(guild_id=guild_id, user_id=user_id, crowns=0)
        session.add(row)
        await session.flush()
    return row


async def grant_lootbox(session: AsyncSession, *, guild_id: int, user_id: int, rarity: str, amount: int) -> None:
    row = await session.scalar(select(LootboxInventoryRow).where(LootboxInventoryRow.guild_id == guild_id, LootboxInventoryRow.user_id == user_id, LootboxInventoryRow.rarity == rarity))
    if row is None:
        row = LootboxInventoryRow(guild_id=guild_id, user_id=user_id, rarity=rarity, amount=max(0, int(amount)))
        session.add(row)
        await session.flush()
        return
    row.amount = int(row.amount) + max(0, int(amount))
