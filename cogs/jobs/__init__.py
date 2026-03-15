from __future__ import annotations

from discord.ext import commands


async def setup(bot: commands.Bot) -> None:
    from .cog import JobsCog
    from .work import WorkCog

    await bot.add_cog(JobsCog(bot))
    await bot.add_cog(WorkCog(bot))
