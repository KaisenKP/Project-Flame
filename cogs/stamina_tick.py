# cogs/stamina_tick.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from discord.ext import commands, tasks
from sqlalchemy import select

from db.models import StaminaRow
from services.db import sessions
from services.stamina import StaminaService


log = logging.getLogger("cogs.stamina_tick")
UTC = timezone.utc


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


class StaminaTickCog(commands.Cog):
    """
    Proactive stamina regen tick.

    What it does:
      - Every minute, finds stamina rows that likely need regen applied
      - Calls StaminaService.get_snapshot() to apply regen + persist
      - Keeps DB 'current_stamina' fresh without requiring user commands

    Safe for single-server and low load.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.stamina = StaminaService()
        self.stamina_tick.start()

    def cog_unload(self):
        try:
            self.stamina_tick.cancel()
        except Exception:
            pass

    @tasks.loop(minutes=1)
    async def stamina_tick(self):
        cutoff = utcnow() - timedelta(minutes=1)

        async with self.sessionmaker() as session:
            async with session.begin():
                rows = (
                    await session.execute(
                        select(StaminaRow).where(
                            (StaminaRow.current_stamina < StaminaRow.max_stamina)
                            | (StaminaRow.last_regen_at < cutoff)
                        )
                    )
                ).scalars().all()

                if not rows:
                    return

                for r in rows:
                    try:
                        await self.stamina.get_snapshot(
                            session,
                            guild_id=int(r.guild_id),
                            user_id=int(r.user_id),
                            is_vip=bool(r.is_vip),
                        )
                    except Exception:
                        log.exception(
                            "stamina_tick failed for guild_id=%s user_id=%s",
                            r.guild_id,
                            r.user_id,
                        )

    @stamina_tick.before_loop
    async def before_stamina_tick(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(StaminaTickCog(bot))
