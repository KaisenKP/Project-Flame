from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import discord
from discord import app_commands
from discord.app_commands import checks
from discord.ext import commands
from sqlalchemy import select

from db.models import UserRow
from services.db import sessions
from services.job_hub import ensure_job_hub_slots, get_or_create_progress, get_slot_snapshot, set_slot_progress
from services.job_progression import state_from_total_xp, total_xp_from_state
from services.jobs_core import ensure_job_row, get_or_create_job_row, job_row_image_set
from services.jobs_views import open_job_hub
from services.users import ensure_user_rows
from services.vip import is_vip_member
from services.xp import xp_req_for_next

from .jobs import JOB_MODULES, get_job_def

_WORK_RESULT_TITLE_SUFFIX = " Work Result"
_LOG = logging.getLogger(__name__)
_PROGRESS_PATTERNS = (
    re.compile(r"prestige\s*[:#-]?\s*(?P<prestige>\d+)\D{0,12}level\s*[:#-]?\s*(?P<level>\d+)", re.IGNORECASE | re.DOTALL),
    re.compile(r"\bp\s*[:#-]?\s*(?P<prestige>\d+)\D{0,8}l\s*[:#-]?\s*(?P<level>\d+)\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"\*\*p(?P<prestige>\d+)\*\*.*?level\s*\*\*(?P<level>\d+)\*\*", re.IGNORECASE | re.DOTALL),
)


@dataclass(frozen=True)
class _RecoveredWorkState:
    prestige: int
    level: int


@dataclass(frozen=True)
class _ParsedWorkMessage:
    job_name: str | None
    state: _RecoveredWorkState | None


@lru_cache(maxsize=4096)
def _extract_progress_state(text: str) -> _RecoveredWorkState | None:
    normalized = " ".join((text or "").replace("|", " ").split())
    if not normalized:
        return None
    for pattern in _PROGRESS_PATTERNS:
        match = pattern.search(normalized)
        if match is None:
            continue
        return _RecoveredWorkState(
            prestige=max(int(match.group("prestige")), 0),
            level=max(int(match.group("level")), 1),
        )
    return None


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
    def _extract_work_job_name(message: discord.Message) -> str | None:
        if not message.embeds:
            return None
        title = (message.embeds[0].title or "").strip()
        if not title.endswith(_WORK_RESULT_TITLE_SUFFIX):
            return None
        return title[: -len(_WORK_RESULT_TITLE_SUFFIX)].strip() or None

    def _parse_work_message(self, message: discord.Message, *, cache: dict[int, _ParsedWorkMessage | None]) -> _ParsedWorkMessage | None:
        cached = cache.get(int(message.id))
        if cached is not None or int(message.id) in cache:
            return cached
        if not message.embeds:
            cache[int(message.id)] = None
            return None

        job_name = self._extract_work_job_name(message)
        if not job_name:
            cache[int(message.id)] = None
            return None

        embed = message.embeds[0]
        text_blocks: list[str] = [embed.description or ""]
        text_blocks.extend(field.value or "" for field in embed.fields)
        text_blocks.append(embed.footer.text if embed.footer else "")

        state: _RecoveredWorkState | None = None
        for block in text_blocks:
            state = _extract_progress_state(block)
            if state is not None:
                break

        if state is None:
            _LOG.debug("fixjobxp: unable to parse progression from work message", extra={"message_id": int(message.id), "author_id": int(message.author.id)})

        parsed = _ParsedWorkMessage(job_name=job_name, state=state)
        cache[int(message.id)] = parsed
        return parsed

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

        await open_job_hub(
            interaction=interaction,
            sessionmaker=self.sessionmaker,
            guild_id=guild_id,
            user_id=user_id,
            vip=vip,
            section="overview",
        )

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

    @app_commands.command(name="job_progress_admin", description="Admin: set a user's XP and prestige for one assigned job slot.")
    @app_commands.describe(
        user="Target user",
        slot="The assigned slot to update",
        xp="Stored XP into the slot's current level",
        prestige="Prestige to set for that slot",
    )
    @checks.has_permissions(manage_guild=True)
    async def job_progress_admin(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        slot: app_commands.Range[int, 1, 3],
        xp: app_commands.Range[int, 0],
        prestige: app_commands.Range[int, 0, 100],
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        user_id = int(user.id)
        slot_index = int(slot) - 1
        vip = is_vip_member(user)

        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                rows = await ensure_job_hub_slots(session, guild_id=guild_id, user_id=user_id, vip=vip)
                target_slot = rows[slot_index]
                job_key = (target_slot.job_key or "").strip().lower()
                if not job_key:
                    await interaction.followup.send(
                        f"{user.mention} does not have a job assigned in slot **{slot}**.",
                        ephemeral=True,
                    )
                    return

                await set_slot_progress(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    slot_index=slot_index,
                    job_key=job_key,
                    xp=int(xp),
                    prestige=int(prestige),
                )
                snap = await get_slot_snapshot(session, guild_id=guild_id, user_id=user_id, vip=vip, slot_index=slot_index)

        d = get_job_def(job_key)
        job_name = d.name if d is not None else job_key.replace("_", " ").title()
        progress_snap = snap.progress
        if progress_snap is None:
            await interaction.followup.send("Updated the slot, but I couldn't load the progress snapshot afterwards.", ephemeral=True)
            return
        await interaction.followup.send(
            "\n".join(
                (
                    f"✅ Updated {user.mention}'s **{job_name}** progress in slot **{slot}**.",
                    f"• Level: **{progress_snap.level}/{progress_snap.level_cap}**",
                    f"• Prestige: **{progress_snap.prestige}**",
                    f"• XP: **{progress_snap.xp:,}/{progress_snap.xp_needed:,}**",
                    f"• Total XP: **{progress_snap.total_xp:,}**",
                )
            ),
            ephemeral=True,
        )

    @staticmethod
    def _xp_total_for_level_floor(level: int) -> int:
        total = 0
        for current_level in range(1, max(int(level), 1)):
            total += int(xp_req_for_next(current_level))
        return total

    @app_commands.command(name="levelfix", description="Admin: rollback mistaken regular XP levels and clamp absurd cached XP outliers.")
    @checks.has_permissions(manage_guild=True)
    async def levelfix(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        await interaction.response.defer(ephemeral=True, thinking=True)

        from services.xp_award import get_or_create_xp_row

        async with self.sessionmaker() as session:
            async with session.begin():
                existing_user_ids = list(await session.scalars(
                    select(UserRow.user_id)
                    .where(UserRow.guild_id == guild_id)
                    .order_by(UserRow.user_id.asc())
                ))

                user_level_rows_touched = 0
                absurd_rows_clamped = 0
                for raw_user_id in existing_user_ids:
                    user_id = int(raw_user_id)
                    xp_row = await get_or_create_xp_row(session, guild_id=guild_id, user_id=user_id)
                    boosted_level = max(int(xp_row.level_cached or 1), 1)
                    restored_level = max(boosted_level // 50, 1)
                    if restored_level > 130:
                        restored_level = 100
                        absurd_rows_clamped += 1
                    xp_row.level_cached = restored_level
                    xp_row.xp_total = self._xp_total_for_level_floor(restored_level)
                    user_level_rows_touched += 1

        await interaction.followup.send(
            "\n".join((
                "✅ Restored the mistaken existing-user regular XP boost.",
                f"• Existing users scanned: **{len(existing_user_ids):,}**",
                f"• Global XP rows rolled back by restoring levels from the prior **50x** boost: **{user_level_rows_touched:,}**",
                f"• Users with absurd cached XP (post-rollback level > 130) clamped to **level 100**: **{absurd_rows_clamped:,}**",
                "• Job Hub XP, levels, and prestige were left unchanged.",
            )),
            ephemeral=True,
        )

    @app_commands.command(name="job_upgrade", description="Open the Job Hub on the tools section.")
    async def job_upgrade_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]
        await open_job_hub(
            interaction=interaction,
            sessionmaker=self.sessionmaker,
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
            vip=vip,
            section="tools",
        )

    @app_commands.command(name="fixjobxp", description="Admin: scan /work results and repair job progression for a user.")
    @app_commands.describe(
        user="User whose job /work history should be repaired",
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
        parse_failures = 0
        parsed_message_cache: dict[int, _ParsedWorkMessage | None] = {}
        recovered_job_state_by_name: dict[str, _RecoveredWorkState] = {}

        async for message in channel.history(limit=int(limit), oldest_first=False):
            scanned_messages += 1
            if not self._message_matches_work_result(message, user_id=user.id):
                continue
            matched_work_messages += 1

            parsed_message = self._parse_work_message(message, cache=parsed_message_cache)
            if parsed_message is None or not parsed_message.job_name:
                continue

            key = parsed_message.job_name.casefold()
            parsed_state = parsed_message.state
            if parsed_state is None:
                parse_failures += 1
                continue

            current_state = recovered_job_state_by_name.get(key)
            if current_state is None or (parsed_state.prestige, parsed_state.level) > (current_state.prestige, current_state.level):
                recovered_job_state_by_name[key] = parsed_state

        async with self.sessionmaker() as session:
            async with session.begin():
                vip = is_vip_member(user)
                slots = await ensure_job_hub_slots(session, guild_id=interaction.guild.id, user_id=user.id, vip=vip)
                restored_jobs: list[str] = []
                skipped_jobs: list[str] = []

                for slot in slots:
                    job_key = (slot.job_key or "").strip().lower()
                    if not job_key:
                        continue
                    job_def = get_job_def(job_key)
                    if job_def is None:
                        skipped_jobs.append(f"Slot {int(slot.slot_index) + 1}: unknown job `{job_key}`")
                        continue

                    recovered_state = recovered_job_state_by_name.get(job_def.name.casefold())
                    if recovered_state is None:
                        skipped_jobs.append(f"Slot {int(slot.slot_index) + 1}: {job_def.name} (no prestige/level history found)")
                        continue

                    progress = await get_or_create_progress(
                        session,
                        guild_id=interaction.guild.id,
                        user_id=user.id,
                        slot_index=int(slot.slot_index),
                        job_key=job_key,
                    )
                    current_state = (max(int(progress.prestige), 0), max(int(progress.level), 1))
                    target_state = (int(recovered_state.prestige), int(recovered_state.level))
                    target_total_xp = int(total_xp_from_state(
                        tier=tier_for_category(job_def.category),
                        job_key=job_key,
                        prestige=target_state[0],
                        level=target_state[1],
                        xp_into=0,
                    ))

                    current_total_xp = max(int(progress.total_xp), 0)
                    if current_state > target_state:
                        skipped_jobs.append(
                            f"Slot {int(slot.slot_index) + 1}: {job_def.name} (stored P{current_state[0]} Lv {current_state[1]} is already above recovered P{target_state[0]} Lv {target_state[1]})"
                        )
                        continue
                    if current_state == target_state and current_total_xp >= target_total_xp:
                        skipped_jobs.append(
                            f"Slot {int(slot.slot_index) + 1}: {job_def.name} (stored P{current_state[0]} Lv {current_state[1]} already matches or exceeds the recovered floor)"
                        )
                        continue

                    repaired_state = state_from_total_xp(
                        tier=tier_for_category(job_def.category),
                        job_key=job_key,
                        total_xp=max(current_total_xp, target_total_xp) if current_state == target_state else target_total_xp,
                    )
                    new_total_xp = max(current_total_xp, target_total_xp) if current_state == target_state else target_total_xp
                    progress.prestige = int(repaired_state.prestige)
                    progress.level = int(repaired_state.level)
                    progress.xp = int(repaired_state.xp_into)
                    progress.total_xp = int(new_total_xp)
                    _LOG.info(
                        "fixjobxp corrected job progression",
                        extra={
                            "guild_id": int(interaction.guild.id),
                            "user_id": int(user.id),
                            "slot_index": int(slot.slot_index),
                            "job_key": job_key,
                            "old_prestige": current_state[0],
                            "old_level": current_state[1],
                            "old_total_xp": int(current_total_xp),
                            "new_prestige": int(repaired_state.prestige),
                            "new_level": int(repaired_state.level),
                            "new_total_xp": int(new_total_xp),
                        },
                    )
                    restored_jobs.append(
                        f"Slot {int(slot.slot_index) + 1}: {job_def.name} — P{int(progress.prestige)} Lv {int(progress.level)} ({int(progress.total_xp):,} total XP minimum reconstructed from /work history)"
                    )

        lines = [
            f"Scanned **{scanned_messages:,}** messages in {channel.mention}.",
            f"Found **{matched_work_messages:,}** `/work` result messages for {user.mention}.",
            "Checked each assigned job against the highest prestige/level visible in historical `/work` embeds and only repaired job progression fields.",
        ]

        if parse_failures:
            lines.append(f"⚠️ Skipped **{parse_failures:,}** malformed `/work` embed(s) that did not contain readable job progression.")

        if restored_jobs:
            lines.append(f"✅ Repaired **{len(restored_jobs)}** assigned job(s) from `/work` history.")
            lines.extend(restored_jobs)
        else:
            lines.append("ℹ️ No assigned jobs needed repair from the scanned `/work` history.")

        if skipped_jobs:
            lines.append("Skipped:")
            lines.extend(skipped_jobs[:10])

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
