from __future__ import annotations

import discord
from discord.ext import commands

from services.message_counter import MessageCounterService


class ActivityListenerCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.msg_counter = MessageCounterService()
        self.msg_counter.start()

    def cog_unload(self) -> None:
        # discord.py calls this on unload
        try:
            # fire and forget, safe
            self.bot.loop.create_task(self.msg_counter.stop())
        except Exception:
            pass

    @commands.Cog.listener("on_message")
    async def on_message_activity(self, message: discord.Message) -> None:
        if message.guild is None:
            return
        if message.author.bot:
            return

        # Optional: ignore empty content, keeps spam like pure embeds down
        if not message.content:
            return

        await self.msg_counter.track_message(
            guild_id=message.guild.id,
            user_id=message.author.id,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(ActivityListenerCog(bot))
