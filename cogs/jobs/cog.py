from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.app_commands import checks
from discord.ext import commands

from services.db import sessions
from services.job_hub import ensure_job_hub_slots, get_slot_snapshot
from services.jobs_core import ensure_job_row, get_or_create_job_row, job_row_image_set
from services.jobs_embeds import make_job_hub_embed
from services.jobs_views import JobHubView
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
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await get_or_create_job_row(session, job_key=key)
                job_row_image_set(row, url)
        await interaction.followup.send(f"✅ Updated /work image for **{d.name}**.", ephemeral=True)

    async def _job_key_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]
        needle = (current or "").strip().lower()
        choices: list[app_commands.Choice[str]] = []
        for mod in JOB_MODULES.values():
            d = mod.definition()
            if d.vip_only and not vip:
                continue
            hay = f"{d.key} {d.name}".lower()
            if needle and needle not in hay:
                continue
            choices.append(app_commands.Choice(name=f"{d.name} ({d.key})", value=d.key))
        return choices[:25]

    @app_commands.command(name="job", description="Open the Job Hub or seed your first slots.")
    @app_commands.describe(job_1="Seed Slot 1", job_2="Seed Slot 2", job_3="Seed Slot 3")
    @app_commands.autocomplete(job_1=_job_key_autocomplete, job_2=_job_key_autocomplete, job_3=_job_key_autocomplete)
    async def job_cmd(self, interaction: discord.Interaction, job_1: Optional[str] = None, job_2: Optional[str] = None, job_3: Optional[str] = None):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        guild_id = interaction.guild.id
        user_id = interaction.user.id
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
                await ensure_job_hub_slots(session, guild_id=guild_id, user_id=user_id, vip=vip)

        view = JobHubView(sessionmaker=self.sessionmaker, guild_id=guild_id, user_id=user_id, vip=vip)
        if any((job_1, job_2, job_3)):
            async with self.sessionmaker() as session:
                async with session.begin():
                    await ensure_job_hub_slots(session, guild_id=guild_id, user_id=user_id, vip=vip)
                    for idx, job_key in enumerate((job_1, job_2, job_3)):
                        if not job_key:
                            continue
                        job_def = get_job_def(job_key.strip().lower())
                        if job_def is None:
                            await interaction.response.send_message(f"Unknown job key `{job_key}`.", ephemeral=True)
                            return
                        from services.job_hub import assign_job_to_slot
                        await assign_job_to_slot(session, guild_id=guild_id, user_id=user_id, vip=vip, slot_index=idx, job_key=job_key.strip().lower())

        async with self.sessionmaker() as session:
            async with session.begin():
                snap = await get_slot_snapshot(session, guild_id=guild_id, user_id=user_id, vip=vip, slot_index=0)
        embed = make_job_hub_embed(user=interaction.user, vip=vip, slot_snap=snap, section="overview")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="job_admin", description="Enable or disable a job (admin only).")
    @app_commands.describe(job="Job key", enabled="Enable or disable the job")
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
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await ensure_job_row(session, key=key, name=d.name)
                row.enabled = bool(enabled)
        await interaction.response.send_message(f"✅ **{d.name}** is now {'enabled' if enabled else 'disabled'}.", ephemeral=True)

    @app_commands.command(name="job_upgrade", description="Open the Job Hub on the tools section.")
    async def job_upgrade_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]
        view = JobHubView(sessionmaker=self.sessionmaker, guild_id=interaction.guild.id, user_id=interaction.user.id, vip=vip, section="tools")
        async with self.sessionmaker() as session:
            async with session.begin():
                snap = await get_slot_snapshot(session, guild_id=interaction.guild.id, user_id=interaction.user.id, vip=vip, slot_index=0)
        embed = make_job_hub_embed(user=interaction.user, vip=vip, slot_snap=snap, section="tools")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @job_admin.autocomplete("job")
    async def job_admin_autocomplete(self, interaction: discord.Interaction, current: str):
        cur = (current or "").lower()
        return [
            app_commands.Choice(name=f"{d.name} ({d.key})", value=d.key)
            for d in (mod.definition() for mod in JOB_MODULES.values())
            if cur in d.key or cur in d.name.lower()
        ][:25]


async def setup(bot: commands.Bot):
    await bot.add_cog(JobsCog(bot))
