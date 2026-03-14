# business/__init__.py
from __future__ import annotations

from discord.ext import commands


async def setup(bot: commands.Bot) -> None:
    """
    Import-safe entry point for the Business package.
    Discord.py will call this when loading the extension.

    Keeps imports local so importing `business` never triggers heavy side effects,
    DB touches, or Discord UI registration until the extension is actually loaded.
    """
    from .cog import BusinessCog  # local import by design

    await bot.add_cog(BusinessCog(bot))