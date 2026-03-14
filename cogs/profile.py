from __future__ import annotations

import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import (
    GamblingStatsRow,
    JobProgressRow,
    JobRow,
    UserJobSlotRow,
    WalletRow,
    XpRow,
)
from services.db import sessions
from services.stamina import StaminaService
from services.users import ensure_user_rows
from services.vip import is_vip_member

log = logging.getLogger("cogs.profile")


def _xp_bar(current: int, needed: int, width: int = 18) -> str:
    needed = max(int(needed), 1)
    current = max(0, int(current))
    ratio = min(current / needed, 1.0)

    filled = int(round(ratio * width))
    empty = max(width - filled, 0)

    return "▰" * filled + "▱" * empty


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _silver_str(silver: int) -> str:
    s = int(silver)
    if s < 0:
        return f"-{_fmt_int(abs(s))} Silver"
    return f"{_fmt_int(s)} Silver"


class ProfileCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessions = sessions()
        self.stamina = StaminaService()

    @app_commands.command(
        name="profile",
        description="Show your calling card profile.",
    )
    @app_commands.describe(
        user="View someone else's profile (optional).",
    )
    async def profile_cmd(
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

        vip = is_vip_member(target)

        await interaction.response.defer(thinking=True)

        # NEW: read equipped job from your jobs cog (memory-based equip)
        equipped_job_key: Optional[str] = None
        equipped_job_name: Optional[str] = None

        try:
            # This matches your jobs.py module-level state.
            # If you restart the bot, the equipped job won't persist (by design of your current jobs system).
            from cogs import jobs as jobs_module  # type: ignore

            equipped_job_key = jobs_module._EQUIPPED.get((guild_id, user_id))  # type: ignore[attr-defined]
            if equipped_job_key:
                jd = jobs_module.JOB_DEFS.get(equipped_job_key)  # type: ignore[attr-defined]
                equipped_job_name = jd.name if jd else None
        except Exception:
            equipped_job_key = None
            equipped_job_name = None

        async with self.sessions() as session:
            async with session.begin():
                await ensure_user_rows(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

                xp_row = await self._get_xp(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

                wallet_row = await self._get_wallet(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

                stamina_snap = await self.stamina.get_snapshot(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    is_vip=vip,
                )

                # Keep the old DB-based slot system too (in case you re-enable it later)
                slots = await self._get_job_slots(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

                jobs_by_id = await self._get_jobs_lookup(
                    session,
                    slots,
                )

                job_prog = await self._get_job_progress(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

                gambling = await self._get_gambling(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

        # ────────── EMBED ──────────

        from services.xp import get_xp_progress, render_xp_bar

        embed = discord.Embed(
            color=discord.Color.gold() if vip else discord.Color.blurple(),
        )

        embed.set_author(
            name=f"{target.display_name}'s Chatbox Card" + (" ⭐ VIP" if vip else ""),
            icon_url=target.display_avatar.url,
        )

        embed.set_thumbnail(url=target.display_avatar.url)

        # XP (computed from xp_total using deterministic curve)
        if xp_row is None:
            xp_total = 0
        else:
            xp_total = int(xp_row.xp_total)

        prog = get_xp_progress(xp_total)
        bar = render_xp_bar(prog.xp_into_level, prog.xp_to_next, width=18)

        xp_line = (
            f"`{bar}`  **{prog.pct}%**  "
            f"`XP: {_fmt_int(prog.xp_into_level)}/{_fmt_int(prog.xp_to_next)}`"
        )

        embed.description = (
            f"**LEVEL {_fmt_int(prog.level)}**\n"
            f"{xp_line}\n"
            f"Total XP: **{_fmt_int(prog.xp_total)}**"
        )

        # Wallet
        if wallet_row is None:
            silver = 0
            diamonds = 0
        else:
            silver = int(wallet_row.silver)
            diamonds = int(wallet_row.diamonds)

        embed.add_field(
            name="Wallet",
            value=(
                f"Silver: **{_fmt_int(silver)}**\n"
                f"Diamonds: **{_fmt_int(diamonds)}**"
            ),
            inline=True,
        )

        stamina_value = f"**{_fmt_int(stamina_snap.current)}/{_fmt_int(stamina_snap.max)}**"
        if vip:
            stamina_value += "\n⭐ VIP"

        embed.add_field(
            name="Stamina",
            value=stamina_value,
            inline=True,
        )

        embed.add_field(
            name="\u200b",
            value="\u200b",
            inline=False,
        )

        # FIX: show equipped job from the jobs cog, fallback to legacy DB slots
        embed.add_field(
            name="Jobs Equipped",
            value=self._format_equipped_jobs(
                slots,
                jobs_by_id,
                equipped_job_key=equipped_job_key,
                equipped_job_name=equipped_job_name,
            ),
            inline=False,
        )

        # If you ever want job progress back, re-add it here. For now, keep it clean.

        embed.set_footer(text=f"User ID: {target.id}")

        await interaction.followup.send(embed=embed)

    # ────────── DB HELPERS ──────────

    async def _get_xp(
        self,
        session,
        *,
        guild_id: int,
        user_id: int,
    ) -> Optional[XpRow]:
        res = await session.execute(
            select(XpRow).where(
                XpRow.guild_id == guild_id,
                XpRow.user_id == user_id,
            )
        )
        return res.scalar_one_or_none()

    async def _get_wallet(
        self,
        session,
        *,
        guild_id: int,
        user_id: int,
    ) -> Optional[WalletRow]:
        res = await session.execute(
            select(WalletRow).where(
                WalletRow.guild_id == guild_id,
                WalletRow.user_id == user_id,
            )
        )
        return res.scalar_one_or_none()

    async def _get_job_slots(
        self,
        session,
        *,
        guild_id: int,
        user_id: int,
    ) -> list[UserJobSlotRow]:
        res = await session.execute(
            select(UserJobSlotRow)
            .where(
                UserJobSlotRow.guild_id == guild_id,
                UserJobSlotRow.user_id == user_id,
            )
            .order_by(UserJobSlotRow.slot_index.asc())
        )
        return list(res.scalars().all())

    async def _get_jobs_lookup(
        self,
        session,
        slots: list[UserJobSlotRow],
    ) -> dict[int, JobRow]:
        job_ids = [s.job_id for s in slots]
        if not job_ids:
            return {}

        res = await session.execute(
            select(JobRow).where(JobRow.id.in_(job_ids))
        )
        jobs = list(res.scalars().all())
        return {j.id: j for j in jobs}

    async def _get_job_progress(
        self,
        session,
        *,
        guild_id: int,
        user_id: int,
    ) -> list[JobProgressRow]:
        res = await session.execute(
            select(JobProgressRow)
            .where(
                JobProgressRow.guild_id == guild_id,
                JobProgressRow.user_id == user_id,
            )
            .order_by(JobProgressRow.job_level.desc())
        )
        return list(res.scalars().all())

    async def _get_gambling(
        self,
        session,
        *,
        guild_id: int,
        user_id: int,
    ) -> Optional[GamblingStatsRow]:
        res = await session.execute(
            select(GamblingStatsRow).where(
                GamblingStatsRow.guild_id == guild_id,
                GamblingStatsRow.user_id == user_id,
            )
        )
        return res.scalar_one_or_none()

    # ────────── FORMATTERS ──────────

    def _format_equipped_jobs(
        self,
        slots: list[UserJobSlotRow],
        jobs_by_id: dict[int, JobRow],
        *,
        equipped_job_key: Optional[str],
        equipped_job_name: Optional[str],
    ) -> str:
        # Prefer the new jobs system (memory equip)
        if equipped_job_key:
            name = equipped_job_name or equipped_job_key
            return f"Equipped: **{name}** (`{equipped_job_key}`)"

        # Fallback: old slot system
        if not slots:
            return "No jobs equipped yet."

        lines: list[str] = []
        for s in slots:
            job = jobs_by_id.get(s.job_id)
            name = job.name if job else f"Job #{s.job_id}"
            lines.append(f"Slot {s.slot_index + 1}: **{name}**")

        return "\n".join(lines)


async def setup(bot: commands.Bot):
    await bot.add_cog(ProfileCog(bot))
