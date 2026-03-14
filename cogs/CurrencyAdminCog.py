from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.app_commands import checks
from discord.ext import commands
from sqlalchemy import delete, select, update

from db.models import WalletRow
from services.db import sessions
from services.users import ensure_user_rows


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


class CurrencyAdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    @app_commands.command(name="currency_reset", description="Admin: reset Silver (and optionally Diamonds).")
    @app_commands.describe(
        user="Target user (leave empty to reset everyone)",
        silver="Reset Silver to this amount (default 0)",
        diamonds="Reset Diamonds to this amount (default 0)",
        include_diamonds="Also reset Diamonds (default True)",
    )
    @checks.has_permissions(manage_guild=True)
    async def currency_reset(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        silver: int = 0,
        diamonds: int = 0,
        include_diamonds: bool = True,
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        silver = max(int(silver), 0)
        diamonds = max(int(diamonds), 0)

        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                if user is None:
                    # Reset everyone
                    stmt = update(WalletRow).where(WalletRow.guild_id == guild_id).values(silver=silver)
                    if include_diamonds:
                        stmt = stmt.values(diamonds=diamonds)
                    res = await session.execute(stmt)

                    # res.rowcount is driver-dependent; safe to compute separately if needed
                    count = await session.scalar(
                        select(WalletRow.guild_id).where(WalletRow.guild_id == guild_id).count()  # type: ignore[attr-defined]
                    )
                else:
                    uid = int(user.id)
                    await ensure_user_rows(session, guild_id=guild_id, user_id=uid)

                    wallet = await session.scalar(
                        select(WalletRow).where(
                            WalletRow.guild_id == guild_id,
                            WalletRow.user_id == uid,
                        )
                    )
                    if wallet is None:
                        wallet = WalletRow(guild_id=guild_id, user_id=uid, silver=0, diamonds=0)
                        session.add(wallet)
                        await session.flush()

                    wallet.silver = silver
                    if include_diamonds:
                        wallet.diamonds = diamonds

        if user is None:
            msg = (
                f"✅ Reset currency for **everyone** in this server.\n"
                f"Silver → **{_fmt_int(silver)}**"
                + (f"\nDiamonds → **{_fmt_int(diamonds)}**" if include_diamonds else "")
            )
        else:
            msg = (
                f"✅ Reset currency for {user.mention}.\n"
                f"Silver → **{_fmt_int(silver)}**"
                + (f"\nDiamonds → **{_fmt_int(diamonds)}**" if include_diamonds else "")
            )

        await interaction.followup.send(msg, ephemeral=True)

    @app_commands.command(name="currency_wipe", description="Admin: delete all wallet rows for this server.")
    @app_commands.describe(confirm="Type WIPE to confirm (required)")
    @checks.has_permissions(manage_guild=True)
    async def currency_wipe(self, interaction: discord.Interaction, confirm: str):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        if (confirm or "").strip().upper() != "WIPE":
            await interaction.response.send_message("Refused. To wipe, run: `/currency_wipe confirm:WIPE`", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                res = await session.execute(delete(WalletRow).where(WalletRow.guild_id == guild_id))
                deleted = getattr(res, "rowcount", None)

        extra = f"Deleted rows: **{deleted}**" if isinstance(deleted, int) else "Wipe complete."
        await interaction.followup.send(f"🧹 Currency wipe done. {extra}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CurrencyAdminCog(bot))
