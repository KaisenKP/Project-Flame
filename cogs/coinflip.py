# cogs/coinflip.py
from __future__ import annotations

import random
import time
from typing import Dict, Tuple, Optional

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import WalletRow
from services.db import sessions
from services.users import ensure_user_rows
from services.vip import is_vip_member


_COOLDOWNS: Dict[Tuple[int, int], float] = {}

MAX_BET = 2_500
MAX_DEBT = 50_000  # wallet can go down to -50,000


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


class CoinflipCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    @app_commands.command(name="coinflip", description="Flip a coin and gamble Silver.")
    @app_commands.describe(amount="Silver to bet", side="heads or tails")
    @app_commands.choices(
        side=[
            app_commands.Choice(name="heads", value="heads"),
            app_commands.Choice(name="tails", value="tails"),
        ]
    )
    async def coinflip(
        self,
        interaction: discord.Interaction,
        amount: int,
        side: app_commands.Choice[str],
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        user_id = int(interaction.user.id)
        now = time.time()

        amt = int(amount)
        if amt <= 0:
            await interaction.response.send_message("Bet must be at least **1 Silver**.", ephemeral=True)
            return

        if amt > MAX_BET:
            await interaction.response.send_message(
                f"Max bet is **{_fmt_int(MAX_BET)} Silver**.",
                ephemeral=True,
            )
            return

        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]

        cd_seconds = 6 if vip else 10
        cd_key = (guild_id, user_id)
        ready_at = float(_COOLDOWNS.get(cd_key, 0.0))
        if ready_at > now:
            left = int(max(ready_at - now, 0))
            await interaction.response.send_message(
                f"Cooldown. Try again in **{_fmt_int(left)}s**.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)

        # TRUE 50/50 COINFLIP
        user_pick = side.value
        landed = random.choice(["heads", "tails"])
        won = (user_pick == landed)

        warning: Optional[str] = None
        after = 0
        denied = False

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                wallet = await session.scalar(
                    select(WalletRow).where(
                        WalletRow.guild_id == guild_id,
                        WalletRow.user_id == user_id,
                    )
                )
                if wallet is None:
                    wallet = WalletRow(guild_id=guild_id, user_id=user_id, silver=0, diamonds=0)
                    session.add(wallet)
                    await session.flush()

                before = int(wallet.silver)

                if won:
                    wallet.silver = before + amt
                    if hasattr(wallet, "silver_earned"):
                        wallet.silver_earned = int(wallet.silver_earned) + amt
                else:
                    if (before - amt) < -MAX_DEBT:
                        denied = True
                    else:
                        wallet.silver = before - amt
                        if hasattr(wallet, "silver_spent"):
                            wallet.silver_spent = int(wallet.silver_spent) + amt

                after = int(wallet.silver)

        if denied:
            await interaction.followup.send(
                f"🚫 You can’t bet that.\n"
                f"Max debt is **-{_fmt_int(MAX_DEBT)} Silver**.\n"
                f"Your balance is **{_fmt_int(after)} Silver**.",
                ephemeral=True,
            )
            return

        _COOLDOWNS[cd_key] = float(now) + float(cd_seconds)

        if after < 0:
            warning = f"⚠️ You’re in **debt**. Current balance: **{_fmt_int(after)} Silver**."

        delta_txt = f"+{_fmt_int(amt)}" if won else f"-{_fmt_int(amt)}"
        outcome_txt = "✅ **WIN**" if won else "❌ **LOSE**"

        lines = [
            f"You picked **{user_pick}**",
            f"Landed **{landed}**",
            "",
            f"{outcome_txt} • **{delta_txt} Silver**",
            f"💰 Wallet: **{_fmt_int(after)} Silver**",
        ]
        if warning:
            lines.append("")
            lines.append(warning)

        embed = discord.Embed(
            title="🪙 Coinflip",
            description="\n".join(lines),
            color=discord.Color.green() if won else discord.Color.red(),
        )
        embed.set_footer(text="50/50")

        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(CoinflipCog(bot))
