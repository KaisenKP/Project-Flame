import discord
from discord import app_commands
from discord.ext import commands


class Ping(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="ping", description="Check if CatBot is alive")
    async def ping(self, interaction: discord.Interaction) -> None:
        latency_ms = round(self.bot.latency * 1000)
        await interaction.response.send_message(f"Pulse is alive. {latency_ms}ms", ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Ping(bot))
