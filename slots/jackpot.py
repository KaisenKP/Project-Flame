from __future__ import annotations

from sqlalchemy import select

from db.models import SlotJackpotRow


class JackpotService:
    def __init__(self, sessionmaker):
        self.sessionmaker = sessionmaker

    async def get_pool(self, guild_id: int) -> int:
        async with self.sessionmaker() as session:
            row = await session.scalar(select(SlotJackpotRow).where(SlotJackpotRow.guild_id == int(guild_id)))
            return int(row.pool_silver) if row else 0

    async def add_loss(self, guild_id: int, amount: int) -> int:
        if amount <= 0:
            return await self.get_pool(guild_id)
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(select(SlotJackpotRow).where(SlotJackpotRow.guild_id == int(guild_id)))
                if row is None:
                    row = SlotJackpotRow(guild_id=int(guild_id), pool_silver=0)
                    session.add(row)
                    await session.flush()
                row.pool_silver = int(row.pool_silver) + int(amount)
                return int(row.pool_silver)

    async def claim(self, guild_id: int) -> int:
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(select(SlotJackpotRow).where(SlotJackpotRow.guild_id == int(guild_id)))
                if row is None:
                    row = SlotJackpotRow(guild_id=int(guild_id), pool_silver=0)
                    session.add(row)
                    await session.flush()
                    return 0
                payout = int(row.pool_silver)
                row.pool_silver = 0
                return payout
