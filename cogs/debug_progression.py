from __future__ import annotations

import inspect
import discord
from discord.ext import commands
from discord import app_commands

import services.jobs_core as jc


class DebugProgressionCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="debug_progression", description="Shows where progression functions live.")
    async def debug_progression(self, interaction: discord.Interaction):
        try:
            mod1 = inspect.getmodule(jc.progression_award_job_xp)
            mod2 = inspect.getmodule(jc.progression_snapshot)

            msg = (
                f"progression_award_job_xp module:\n{mod1}\n\n"
                f"progression_snapshot module:\n{mod2}"
            )
        except Exception as e:
            msg = f"Error: {type(e).__name__}: {e}"

        await interaction.response.send_message(f"```{msg}```", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(DebugProgressionCog(bot))