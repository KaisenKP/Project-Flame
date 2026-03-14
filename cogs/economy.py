# cogs/economy.py

from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import WalletRow
from services.db import sessions
from services.users import ensure_user_rows


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


async def _get_or_create_wallet(session, *, guild_id: int, user_id: int) -> WalletRow:
    wallet = await session.scalar(
        select(WalletRow).where(
            WalletRow.guild_id == int(guild_id),
            WalletRow.user_id == int(user_id),
        )
    )
    if wallet is None:
        wallet = WalletRow(guild_id=int(guild_id), user_id=int(user_id), silver=0, diamonds=0)
        session.add(wallet)
        await session.flush()
    return wallet


class EconomyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    @app_commands.command(name="pay", description="Pay another user Silver.")
    @app_commands.describe(user="Who you want to pay", amount="Amount of Silver", note="Optional note")
    async def pay_cmd(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        amount: int,
        note: Optional[str] = None,
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        if user.bot:
            await interaction.response.send_message("You can’t pay bots.", ephemeral=True)
            return

        payer_id = int(interaction.user.id)
        payee_id = int(user.id)

        if payer_id == payee_id:
            await interaction.response.send_message("You can’t pay yourself.", ephemeral=True)
            return

        amt = int(amount)
        if amt <= 0:
            await interaction.response.send_message("Amount must be more than 0.", ephemeral=True)
            return

        if amt > 2_000_000_000:
            await interaction.response.send_message("That amount is too large.", ephemeral=True)
            return

        try:
            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)
        except Exception:
            return

        guild_id = int(interaction.guild.id)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=payer_id)
                await ensure_user_rows(session, guild_id=guild_id, user_id=payee_id)

                payer_wallet = await _get_or_create_wallet(session, guild_id=guild_id, user_id=payer_id)
                payee_wallet = await _get_or_create_wallet(session, guild_id=guild_id, user_id=payee_id)

                if int(payer_wallet.silver) < amt:
                    await interaction.followup.send(
                        f"You don’t have enough Silver. You have **{_fmt_int(payer_wallet.silver)}**.",
                        ephemeral=True,
                    )
                    return

                payer_wallet.silver -= amt
                payee_wallet.silver += amt

                if hasattr(payer_wallet, "silver_spent"):
                    payer_wallet.silver_spent += amt
                if hasattr(payee_wallet, "silver_earned"):
                    payee_wallet.silver_earned += amt

        note_txt = ""
        if note:
            note_clean = str(note).strip()
            if note_clean:
                note_txt = f"\n📝 {note_clean[:200]}"

        embed = discord.Embed(
            title="Payment Sent",
            description=(
                f"✅ {interaction.user.mention} paid {user.mention} **{_fmt_int(amt)} Silver**."
                f"{note_txt}"
            ),
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="balance", description="Check your Silver balance.")
    async def balance_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        user_id = int(interaction.user.id)

        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
                wallet = await _get_or_create_wallet(session, guild_id=guild_id, user_id=user_id)

        embed = discord.Embed(
            title="Your Balance",
            description=f"💰 Silver: **{_fmt_int(wallet.silver)}**",
            color=discord.Color.blurple(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(EconomyCog(bot))
