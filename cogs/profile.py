from __future__ import annotations

import io
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import JobRow, ProfileSettingsRow, UserJobSlotRow, WalletRow, XpRow
from services.db import sessions
from services.profile_backgrounds import ensure_profile_background_rows, resolve_background_key
from services.profile_card import JobDisplay, ProfileCardPayload, ProfileCardRenderer
from services.stamina import StaminaService
from services.users import ensure_user_rows
from services.vip import is_vip_member
from services.xp import get_xp_progress

log = logging.getLogger("cogs.profile")


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


class ProfileCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessions = sessions()
        self.stamina = StaminaService()
        self.renderer = ProfileCardRenderer()

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

        equipped_job_key: Optional[str] = None
        equipped_job_name: Optional[str] = None

        try:
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

                profile_settings = await ensure_profile_background_rows(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

                slots = await self._get_job_slots(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

                jobs_by_id = await self._get_jobs_lookup(
                    session,
                    slots,
                )

        xp_total = int(xp_row.xp_total) if xp_row else 0
        prog = get_xp_progress(xp_total)

        silver = int(wallet_row.silver) if wallet_row else 0
        diamonds = int(wallet_row.diamonds) if wallet_row else 0

        jobs = self._build_job_labels(
            slots,
            jobs_by_id,
            equipped_job_key=equipped_job_key,
            equipped_job_name=equipped_job_name,
        )

        avatar_asset = target.display_avatar.replace(size=256)
        avatar_bytes = await avatar_asset.read()

        bg_key = resolve_background_key(profile_settings.selected_background_key if profile_settings else None)

        payload = ProfileCardPayload(
            username=target.display_name,
            user_id=target.id,
            vip=vip,
            level=prog.level,
            xp_into_level=prog.xp_into_level,
            xp_to_next=prog.xp_to_next,
            xp_total=prog.xp_total,
            silver=silver,
            diamonds=diamonds,
            stamina_current=stamina_snap.current,
            stamina_max=stamina_snap.max,
            jobs=tuple(jobs),
            background_key=bg_key,
            avatar_bytes=avatar_bytes,
        )
        png = self.renderer.render(payload)

        file = discord.File(fp=io.BytesIO(png), filename="profile-card.png")
        embed = discord.Embed(
            title=f"{target.display_name}'s Calling Card",
            color=discord.Color.gold() if vip else discord.Color.blurple(),
            description=(
                f"Level **{_fmt_int(prog.level)}** • XP **{_fmt_int(prog.xp_into_level)}/{_fmt_int(prog.xp_to_next)}**\n"
                f"Background: **{bg_key}**"
            ),
        )
        embed.set_image(url="attachment://profile-card.png")
        embed.set_footer(text=f"User ID: {target.id}")

        await interaction.followup.send(embed=embed, file=file)

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

    async def _get_profile_settings(
        self,
        session,
        *,
        guild_id: int,
        user_id: int,
    ) -> Optional[ProfileSettingsRow]:
        res = await session.execute(
            select(ProfileSettingsRow).where(
                ProfileSettingsRow.guild_id == guild_id,
                ProfileSettingsRow.user_id == user_id,
            )
        )
        return res.scalar_one_or_none()

    def _build_job_labels(
        self,
        slots: list[UserJobSlotRow],
        jobs_by_id: dict[int, JobRow],
        *,
        equipped_job_key: Optional[str],
        equipped_job_name: Optional[str],
    ) -> list[JobDisplay]:
        if equipped_job_key:
            name = equipped_job_name or equipped_job_key
            return [JobDisplay(slot=1, label=name)]

        if not slots:
            return []

        labels: list[JobDisplay] = []
        for s in slots:
            job = jobs_by_id.get(s.job_id)
            name = job.name if job else f"Job #{s.job_id}"
            labels.append(JobDisplay(slot=s.slot_index + 1, label=name))
        return labels


async def setup(bot: commands.Bot):
    await bot.add_cog(ProfileCog(bot))
