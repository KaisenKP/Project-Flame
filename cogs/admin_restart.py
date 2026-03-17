from __future__ import annotations

import asyncio
import os
import sys

import discord
from discord import app_commands
from discord.ext import commands


class AdminRestart(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._restart_lock = asyncio.Lock()

    @app_commands.command(name="restart", description="Admin: restart the bot process.")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def restart(self, interaction: discord.Interaction) -> None:
        async with self._restart_lock:
            await interaction.response.send_message("Restarting bot now...", ephemeral=True)
            await self.bot.close()
            os.execv(sys.executable, [sys.executable, *sys.argv])

    @restart.error
    async def restart_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.errors.MissingPermissions):
            if interaction.response.is_done():
                await interaction.followup.send("You must be an administrator to use this command.", ephemeral=True)
            else:
                await interaction.response.send_message(
                    "You must be an administrator to use this command.",
                    ephemeral=True,
                )
            return
        raise error


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AdminRestart(bot))
