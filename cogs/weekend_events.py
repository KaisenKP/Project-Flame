from __future__ import annotations

from discord.ext import commands

from tasks.weekend_events import WeekendEventsCog


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(WeekendEventsCog(bot))
