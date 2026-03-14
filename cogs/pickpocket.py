# cogs/pickpocket.py
from __future__ import annotations

import random
import time
from typing import Dict, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import WalletRow
from services.db import sessions
from services.users import ensure_user_rows
from services.vip import is_vip_member


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _cooldown_text(seconds_left: int) -> str:
    s = max(int(seconds_left), 0)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{(s + 59) // 60}m"
    return f"{(s + 3599) // 3600}h"


_COOLDOWNS: Dict[Tuple[int, int], float] = {}


class PickpocketCog(commands.Cog):
    COOLDOWN_REGULAR = 3 * 60 * 60  # 3 hours
    COOLDOWN_VIP = 3 * 60  # 3 minutes

    FAIL_CHANCE_BP = 7800  # 78% fail chance

    FAIL_LINES_NO_MONEY = [
        "You reach into their pockets and pull out… lint. Absolute cinema.",
        "You check every pocket. They’re financially extinct.",
        "You find a single crumb and a dream. No coins though.",
        "You loot them and discover: broke. Same.",
        "Their wallet is on life support. It flatlined.",
        "You open the purse. A sad violin plays. Empty.",
        "You pat them down and find negative money vibes.",
        "You steal their air. That’s all they had.",
        "You find a receipt from 2017 and zero silver.",
        "You whisper 'gimme the loot' and the universe says 'nah'.",
    ]

    FAIL_LINES_GENERAL = [
        "You fumble the bag and trip over your own confidence.",
        "You go for the swipe and hit… absolutely nothing.",
        "They look at you. You look away. Cringe detected.",
        "Your stealth stat is fake news.",
        "You get caught and act like you were just fixing their pocket. Sure.",
        "You miss the pocket and steal… their respect for you.",
        "They dodge like they have ultra instinct.",
        "You try to pickpocket but your hands lag IRL.",
        "You reach in and pull out shame. Just shame.",
        "You nearly had it… then you remembered you don’t got it like that.",
    ]

    SUCCESS_LINES = [
        "Clean lift. You vanish like a rumor.",
        "Yoink. Easy money.",
        "You snag the coins and walk away like you own the server.",
        "A professional moment. No witnesses. Allegedly.",
        "You pocket the silver and pretend you were never here.",
        "Swipe complete. Respectfully disrespectful.",
    ]

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    def _roll_bp(self, chance_bp: int) -> bool:
        bp = max(int(chance_bp), 0)
        if bp <= 0:
            return False
        if bp >= 10000:
            return True
        return random.randint(1, 10000) <= bp

    @app_commands.command(name="pickpocket", description="Try to steal some silver from someone. Risky.")
    @app_commands.describe(target="The person you want to attempt to pickpocket")
    async def pickpocket_cmd(self, interaction: discord.Interaction, target: discord.Member):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        thief = interaction.user
        if not isinstance(thief, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        if target.bot:
            await interaction.response.send_message("Pickpocketing bots is crazy work.", ephemeral=True)
            return

        if target.id == thief.id:
            await interaction.response.send_message("You can’t pickpocket yourself. Nice try though.", ephemeral=True)
            return

        guild_id = interaction.guild.id
        thief_id = thief.id
        target_id = target.id

        vip = is_vip_member(thief)  # type: ignore[arg-type]
        cooldown = self.COOLDOWN_VIP if vip else self.COOLDOWN_REGULAR

        now = time.time()
        cd_key = (guild_id, thief_id)
        ready_at = _COOLDOWNS.get(cd_key, 0.0)
        if ready_at > now:
            left = int(max(ready_at - now, 0))
            await interaction.response.send_message(
                f"Cooldown active. Try again in **{_cooldown_text(left)}**.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=thief_id)
                await ensure_user_rows(session, guild_id=guild_id, user_id=target_id)

                thief_wallet = await session.scalar(
                    select(WalletRow).where(WalletRow.guild_id == guild_id, WalletRow.user_id == thief_id)
                )
                if thief_wallet is None:
                    thief_wallet = WalletRow(guild_id=guild_id, user_id=thief_id, silver=0, diamonds=0)
                    session.add(thief_wallet)
                    await session.flush()

                target_wallet = await session.scalar(
                    select(WalletRow).where(WalletRow.guild_id == guild_id, WalletRow.user_id == target_id)
                )
                if target_wallet is None:
                    target_wallet = WalletRow(guild_id=guild_id, user_id=target_id, silver=0, diamonds=0)
                    session.add(target_wallet)
                    await session.flush()

                target_silver = max(int(getattr(target_wallet, "silver", 0)), 0)

                # Auto-fail if target has no money
                if target_silver <= 0:
                    _COOLDOWNS[cd_key] = now + float(cooldown)

                    line = random.choice(self.FAIL_LINES_NO_MONEY)
                    embed = discord.Embed(
                        title="Pickpocket Result",
                        description=f"{thief.mention} tried to pickpocket {target.mention}.\n\n**FAILED** ❌\n{line}",
                        color=discord.Color.red(),
                    )
                    await interaction.followup.send(embed=embed)
                    return

                # Roll fail/success
                failed = self._roll_bp(self.FAIL_CHANCE_BP)

                if failed:
                    _COOLDOWNS[cd_key] = now + float(cooldown)

                    line = random.choice(self.FAIL_LINES_GENERAL)
                    embed = discord.Embed(
                        title="Pickpocket Result",
                        description=f"{thief.mention} tried to pickpocket {target.mention}.\n\n**FAILED** ❌\n{line}",
                        color=discord.Color.red(),
                    )
                    await interaction.followup.send(embed=embed)
                    return

                # Success: steal 5% to 15%, clamped to [10..500] and not more than target has
                pct = random.randint(5, 15)
                raw = int((target_silver * pct) // 100)

                steal_amount = max(raw, 10)
                steal_amount = min(steal_amount, 500)
                steal_amount = min(steal_amount, target_silver)
                steal_amount = max(int(steal_amount), 0)

                if steal_amount <= 0:
                    _COOLDOWNS[cd_key] = now + float(cooldown)

                    line = random.choice(self.FAIL_LINES_NO_MONEY)
                    embed = discord.Embed(
                        title="Pickpocket Result",
                        description=f"{thief.mention} tried to pickpocket {target.mention}.\n\n**FAILED** ❌\n{line}",
                        color=discord.Color.red(),
                    )
                    await interaction.followup.send(embed=embed)
                    return

                target_wallet.silver -= int(steal_amount)
                thief_wallet.silver += int(steal_amount)

                if hasattr(thief_wallet, "silver_earned"):
                    thief_wallet.silver_earned += int(max(steal_amount, 0))
                if hasattr(target_wallet, "silver_spent"):
                    target_wallet.silver_spent += int(max(steal_amount, 0))

                _COOLDOWNS[cd_key] = now + float(cooldown)

                line = random.choice(self.SUCCESS_LINES)
                embed = discord.Embed(
                    title="Pickpocket Result",
                    description=(
                        f"{thief.mention} tried to pickpocket {target.mention}.\n\n"
                        f"**SUCCESS** ✅\n"
                        f"Stolen: **{_fmt_int(steal_amount)} Silver**\n"
                        f"{line}"
                    ),
                    color=discord.Color.green(),
                )
                await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(PickpocketCog(bot))
