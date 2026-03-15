from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.app_commands import checks
from discord.ext import commands

from services.db import sessions
from services.jobs_core import (
    JOB_SWITCH_COST,
    JOB_UNLOCK_LEVEL,
    ensure_job_row,
    fmt_int,
    get_equipped_key,
    get_level,
    get_or_create_job_row,
    job_row_image_set,
)
from services.jobs_embeds import make_panel_embed
from services.jobs_views import EquipConfirmView, JobsPanelView
from services.users import ensure_user_rows
from services.vip import is_vip_member

from .jobs import JOB_MODULES, get_job_def


class JobsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    @app_commands.command(name="work_image_admin", description="Admin: set an image URL used in /work embeds for a job.")
    @app_commands.describe(job="Job key (miner, fisherman, etc.)", image_url="Direct image URL from your image library")
    @checks.has_permissions(manage_guild=True)
    async def work_image_admin(self, interaction: discord.Interaction, job: str, image_url: Optional[str] = None):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        key = (job or "").strip().lower()
        d = get_job_def(key)
        if d is None:
            await interaction.response.send_message(f"Unknown job key `{key}`.", ephemeral=True)
            return

        url = (image_url or "").strip() if image_url else None
        if url is not None and not (url.startswith("http://") or url.startswith("https://")):
            await interaction.response.send_message("That does not look like a valid URL.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                row = await get_or_create_job_row(session, job_key=key)
                ok = job_row_image_set(row, url)
                if not ok:
                    await interaction.followup.send(
                        "I could not find a compatible image column on JobRow to persist this. "
                        "If your JobRow has a column like `work_image_url`, add it and we’re good.",
                        ephemeral=True,
                    )
                    return

        if url:
            await interaction.followup.send(f"✅ Set /work image for **{d.name}**.", ephemeral=True)
        else:
            await interaction.followup.send(f"✅ Cleared /work image for **{d.name}**.", ephemeral=True)

    @app_commands.command(name="job", description="Open jobs panel or equip a job.")
    @app_commands.describe(job="Job key to equip (miner, fisherman, robber, etc.)")
    async def job_cmd(self, interaction: discord.Interaction, job: Optional[str] = None):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        guild_id = interaction.guild.id
        user_id = interaction.user.id
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]

        if job is not None and str(job).strip():
            key = str(job).strip().lower()
            d = get_job_def(key)
            if d is None:
                await interaction.response.send_message(
                    f"Unknown job key `{key}`. Use `/job` to open the panel.",
                    ephemeral=True,
                )
                return

            if d.vip_only and not vip:
                await interaction.response.send_message("That job is VIP-locked.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            async with self.sessionmaker() as session:
                async with session.begin():
                    await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                    level = await get_level(session, guild_id=guild_id, user_id=user_id)
                    need = JOB_UNLOCK_LEVEL[d.category]
                    if (not vip) and level < need:
                        await interaction.followup.send(
                            f"🔒 **{d.name}** unlocks at **Level {need}**.",
                            ephemeral=True,
                        )
                        return

                    row = await ensure_job_row(session, key=key, name=d.name)
                    if not bool(getattr(row, "enabled", True)):
                        await interaction.followup.send(f"Job `{key}` is disabled in DB.", ephemeral=True)
                        return

                    old = await get_equipped_key(session, guild_id=guild_id, user_id=user_id)
                    first_free = old is None
                    cost = JOB_SWITCH_COST[d.category]

            msg = "Equip this job for free?" if old is None else f"Switch jobs for **{fmt_int(cost)} Silver**?"
            view = EquipConfirmView(
                sessionmaker=self.sessionmaker,
                guild_id=guild_id,
                user_id=user_id,
                vip=vip,
                new_key=key,
                old_key=old,
                cost=cost,
                first_free=first_free,
            )
            await interaction.followup.send(msg, view=view, ephemeral=True)
            return

        async with self.sessionmaker() as session:
            async with session.begin():
                equipped = await get_equipped_key(session, guild_id=guild_id, user_id=user_id)

        embed = make_panel_embed(user=interaction.user, vip=vip, page="standard", equipped=equipped)
        view = JobsPanelView(sessionmaker=self.sessionmaker, vip=vip, guild_id=guild_id, user_id=user_id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="job_admin", description="Enable or disable a job (admin only).")
    @app_commands.describe(job="Job key (miner, fisherman, robber, etc.)", enabled="Enable or disable the job")
    @checks.has_permissions(manage_guild=True)
    async def job_admin(self, interaction: discord.Interaction, job: str, enabled: bool):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        key = job.strip().lower()
        d = get_job_def(key)
        if d is None:
            await interaction.response.send_message(f"Unknown job key `{key}`.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                row = await ensure_job_row(session, key=key, name=d.name)
                row.enabled = bool(enabled)

        state = "enabled ✅" if enabled else "disabled ❌"
        embed = discord.Embed(
            title="Job Updated",
            description=f"**{d.name}** (`{key}`) is now **{state}**.",
            color=discord.Color.green() if enabled else discord.Color.red(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @job_admin.autocomplete("job")
    async def job_admin_autocomplete(self, interaction: discord.Interaction, current: str):
        cur = (current or "").lower()
        choices = [
            app_commands.Choice(name=f"{d.name} ({d.key})", value=d.key)
            for d in (mod.definition() for mod in JOB_MODULES.values())
            if cur in d.key or cur in d.name.lower()
        ]
        return choices[:25]
