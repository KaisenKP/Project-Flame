from __future__ import annotations

import re
from typing import Optional

import discord
from discord import app_commands
from discord.app_commands import checks
from sqlalchemy import select
from discord.ext import commands

from db.models import XpRow
from services.db import sessions
from services.job_hub import ensure_job_hub_slots, get_slot_snapshot
from services.jobs_core import ensure_job_row, get_or_create_job_row, job_row_image_set
from services.jobs_embeds import make_job_hub_embed
from services.jobs_views import JobHubView
from services.users import ensure_user_rows
from services.vip import is_vip_member
from services.xp import get_xp_progress

from .jobs import JOB_MODULES, get_job_def

_WORK_RESULT_TITLE_SUFFIX = " Work Result"
_USER_XP_RE = re.compile(r"User XP:\s*\*\*\+([\d,]+)\*\*")


class JobsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    @staticmethod
    def _message_matches_work_result(message: discord.Message, *, user_id: int) -> bool:
        metadata = getattr(message, "interaction_metadata", None)
        if metadata is None or getattr(metadata, "user", None) is None:
            return False
        if int(metadata.user.id) != int(user_id):
            return False
        if not message.embeds:
            return False
        embed = message.embeds[0]
        title = (embed.title or "").strip()
        return title.endswith(_WORK_RESULT_TITLE_SUFFIX)

    @staticmethod
    def _extract_work_user_xp(message: discord.Message) -> int:
        if not message.embeds:
            return 0
        embed = message.embeds[0]
        for field in embed.fields:
            if (field.name or "").strip().lower() != "gains":
                continue
            match = _USER_XP_RE.search(field.value or "")
            if match is None:
                return 0
            return int(match.group(1).replace(",", ""))
        return 0

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

    @app_commands.command(name="fixjobxp", description="Admin: scan a channel's /work results and restore missing user XP.")
    @app_commands.describe(
        user="User whose /work XP should be checked",
        channel="Channel to scan for /work result messages",
        limit="How many recent messages to scan (50-5000)",
    )
    @checks.has_permissions(manage_guild=True)
    async def fixjobxp(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        channel: discord.TextChannel,
        limit: app_commands.Range[int, 50, 5000] = 1000,
    ):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        me = interaction.guild.me
        if me is None:
            await interaction.response.send_message("I couldn't verify my server permissions.", ephemeral=True)
            return
        perms = channel.permissions_for(me)
        if not perms.read_message_history or not perms.view_channel:
            await interaction.response.send_message(
                f"I need **View Channel** and **Read Message History** in {channel.mention} to scan /work messages.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        scanned_messages = 0
        matched_work_messages = 0
        recovered_xp_total = 0

        async for message in channel.history(limit=int(limit), oldest_first=True):
            scanned_messages += 1
            if not self._message_matches_work_result(message, user_id=user.id):
                continue
            recovered_xp_total += self._extract_work_user_xp(message)
            matched_work_messages += 1

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=interaction.guild.id, user_id=user.id)
                xp_row = await session.scalar(
                    select(XpRow).where(
                        XpRow.guild_id == interaction.guild.id,
                        XpRow.user_id == user.id,
                    )
                )
                if xp_row is None:
                    xp_row = XpRow(guild_id=interaction.guild.id, user_id=user.id, xp_total=0, level_cached=1)
                    session.add(xp_row)
                    await session.flush()

                current_xp = int(xp_row.xp_total)
                xp_missing = max(int(recovered_xp_total) - current_xp, 0)

                if xp_missing > 0:
                    new_total = current_xp + xp_missing
                    prog = get_xp_progress(new_total)
                    xp_row.xp_total = int(prog.xp_total)
                    xp_row.level_cached = int(prog.level)

                final_xp = int(xp_row.xp_total)
                final_level = int(xp_row.level_cached)

        lines = [
            f"Scanned **{scanned_messages:,}** messages in {channel.mention}.",
            f"Found **{matched_work_messages:,}** `/work` result messages for {user.mention}.",
            f"Recovered `/work` XP total from that channel: **{recovered_xp_total:,}**.",
        ]
        if xp_missing > 0:
            lines.append(f"✅ Restored **{xp_missing:,}** missing XP.")
        else:
            lines.append("ℹ️ No missing XP was found from the scanned `/work` messages.")
        lines.append(f"Current stored XP: **{final_xp:,}** • Level **{final_level:,}**.")

        await interaction.followup.send("\n".join(lines), ephemeral=True)

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
