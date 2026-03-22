from __future__ import annotations

import logging

import discord
from discord.ext import commands, tasks

from services.work_xp import is_weekend, weekend_id

LOG = logging.getLogger(__name__)
CHANNEL_ID = 1460859446480867339
CHECK_INTERVAL_MINUTES = 5


class WeekendEventsCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.double_xp_announced: bool = False
        self.last_weekend_id: str | None = None
        self.weekend_announcement_loop.start()

    def cog_unload(self) -> None:
        self.weekend_announcement_loop.cancel()

    @tasks.loop(minutes=CHECK_INTERVAL_MINUTES)
    async def weekend_announcement_loop(self) -> None:
        current_weekend_id = weekend_id()
        if not is_weekend():
            self.double_xp_announced = False
            return

        if self.last_weekend_id != current_weekend_id:
            self.last_weekend_id = current_weekend_id
            self.double_xp_announced = False

        if self.double_xp_announced:
            return

        channel = self.bot.get_channel(CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(CHANNEL_ID)
            except Exception:
                LOG.exception("Failed to fetch weekend XP announcement channel", extra={"channel_id": CHANNEL_ID})
                return

        if not isinstance(channel, discord.abc.Messageable):
            LOG.warning(
                "Weekend XP announcement channel is not messageable",
                extra={"channel_id": CHANNEL_ID, "channel_type": type(channel).__name__},
            )
            return

        embed = discord.Embed(
            title="🔥 DOUBLE XP WEEKEND ACTIVE",
            description=(
                "Earn 2x XP from all /work commands this weekend.\n\n"
                "Now is the best time to grind your jobs, level up faster, and push toward prestige.\n\n"
                "This bonus lasts until the end of Sunday."
            ),
            color=discord.Color.orange(),
        )
        await channel.send(embed=embed)
        self.double_xp_announced = True

    @weekend_announcement_loop.before_loop
    async def before_weekend_announcement_loop(self) -> None:
        await self.bot.wait_until_ready()
