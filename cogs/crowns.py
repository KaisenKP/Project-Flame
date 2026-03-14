from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select, text

from db.models import CrownsWalletRow
from services.db import sessions


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


class CrownsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    async def _ensure_table(self) -> None:
        sql = """
        CREATE TABLE IF NOT EXISTS crowns_wallets (
            id INT NOT NULL AUTO_INCREMENT,
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            crowns INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_crowns_wallets_guild_user (guild_id, user_id),
            KEY ix_crowns_wallets_guild_id (guild_id),
            KEY ix_crowns_wallets_user_id (user_id)
        );
        """
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql))

    async def _get_crowns(self, guild_id: int, user_id: int) -> int:
        await self._ensure_table()
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(
                    select(CrownsWalletRow).where(
                        CrownsWalletRow.guild_id == int(guild_id),
                        CrownsWalletRow.user_id == int(user_id),
                    )
                )
                if row is None:
                    return 0
                return int(row.crowns)

    crowns = app_commands.Group(name="crowns", description="Crowns currency.")

    @crowns.command(name="balance", description="Check your crowns balance.")
    async def crowns_balance(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        gid = interaction.guild.id
        uid = interaction.user.id
        amt = await self._get_crowns(gid, uid)

        embed = discord.Embed(
            title="👑 Crowns",
            description=f"You have **{_fmt_int(amt)}** crowns.",
            color=discord.Color.gold(),
        )
        embed.set_footer(text="Crowns are earned by winning monthly challenges.")
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CrownsCog(bot))
