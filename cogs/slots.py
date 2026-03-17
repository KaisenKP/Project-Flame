from __future__ import annotations

from discord.ext import commands

from slots.cog import SlotsCog

__all__ = ["SlotsCog", "setup"]


async def setup(bot: commands.Bot):
    await bot.add_cog(SlotsCog(bot))
