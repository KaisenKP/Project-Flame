from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import func, select

from db.models import ActivityDailyRow
from services.db import sessions
from services.users import ensure_user_rows


def _utc_today_date():
    return datetime.now(timezone.utc).date()


def _fmt_duration(seconds: int) -> str:
    s = max(int(seconds), 0)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:,}h {m:02d}m {sec:02d}s"


class VcTimeCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    @app_commands.command(
        name="vctime",
        description="Show voice chat time (today / last 7 days / all time).",
    )
    @app_commands.describe(
        user="View someone else's VC time (optional).",
    )
    async def vctime_cmd(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
    ):
        if interaction.guild is None:
            await interaction.response.send_message(
                "This only works in a server.",
                ephemeral=True,
            )
            return

        target: discord.Member = user or interaction.user  # type: ignore[assignment]
        guild_id = interaction.guild.id
        user_id = target.id

        await interaction.response.defer(thinking=True)

        today = _utc_today_date()
        week_start = today - timedelta(days=6)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                today_seconds = await session.scalar(
                    select(func.coalesce(func.sum(ActivityDailyRow.vc_seconds), 0)).where(
                        ActivityDailyRow.guild_id == guild_id,
                        ActivityDailyRow.user_id == user_id,
                        ActivityDailyRow.day == today,
                    )
                )

                week_seconds = await session.scalar(
                    select(func.coalesce(func.sum(ActivityDailyRow.vc_seconds), 0)).where(
                        ActivityDailyRow.guild_id == guild_id,
                        ActivityDailyRow.user_id == user_id,
                        ActivityDailyRow.day >= week_start,
                        ActivityDailyRow.day <= today,
                    )
                )

                all_time_seconds = await session.scalar(
                    select(func.coalesce(func.sum(ActivityDailyRow.vc_seconds), 0)).where(
                        ActivityDailyRow.guild_id == guild_id,
                        ActivityDailyRow.user_id == user_id,
                    )
                )

        today_seconds = int(today_seconds or 0)
        week_seconds = int(week_seconds or 0)
        all_time_seconds = int(all_time_seconds or 0)

        # Current VC session estimate (live, not stored)
        current_session = "Not in VC"
        vs = target.voice
        if vs is not None and vs.channel is not None:
            # If your VC XP ticker requires "not muted/deaf + not alone", we can mirror that here.
            # For a pure "time in VC right now" display, just show the channel and elapsed unknown.
            current_session = f"In **{vs.channel.name}**"

            muted = bool(vs.self_mute or vs.mute)
            deaf = bool(vs.self_deaf or vs.deaf)
            flags = []
            if muted:
                flags.append("muted")
            if deaf:
                flags.append("deafened")
            if flags:
                current_session += f" ({', '.join(flags)})"

        embed = discord.Embed(
            color=discord.Color.blurple(),
        )
        embed.set_author(
            name=f"{target.display_name}'s VC Time",
            icon_url=target.display_avatar.url,
        )

        embed.add_field(
            name="Current",
            value=current_session,
            inline=False,
        )

        embed.add_field(
            name="Today",
            value=f"**{_fmt_duration(today_seconds)}**",
            inline=True,
        )

        embed.add_field(
            name="Last 7 Days",
            value=f"**{_fmt_duration(week_seconds)}**",
            inline=True,
        )

        embed.add_field(
            name="All Time",
            value=f"**{_fmt_duration(all_time_seconds)}**",
            inline=True,
        )

        embed.set_footer(text=f"User ID: {target.id}")

        await interaction.followup.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(VcTimeCog(bot))