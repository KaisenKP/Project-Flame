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
    get_equipped_keys,
    get_level,
    get_or_create_job_row,
    job_row_image_set,
)
from services.job_upgrades import build_upgrade_snapshot, play_upgrade_animation, upgrade_once
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

    async def _job_key_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> list[app_commands.Choice[str]]:
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]
        needle = (current or "").strip().lower()

        choices: list[app_commands.Choice[str]] = []
        for d in JOB_MODULES:
            if d.vip_only and not vip:
                continue
            hay = f"{d.key} {d.name}".lower()
            if needle and needle not in hay:
                continue
            choices.append(app_commands.Choice(name=f"{d.name} ({d.key})", value=d.key))
            if len(choices) >= 25:
                break
        return choices

    @app_commands.command(name="job", description="Open jobs panel or equip up to 3 jobs.")
    @app_commands.describe(
        job_1="First job key to equip",
        job_2="Second job key to equip",
        job_3="Third job key to equip",
    )
    @app_commands.autocomplete(job_1=_job_key_autocomplete, job_2=_job_key_autocomplete, job_3=_job_key_autocomplete)
    async def job_cmd(
        self,
        interaction: discord.Interaction,
        job_1: Optional[str] = None,
        job_2: Optional[str] = None,
        job_3: Optional[str] = None,
    ):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        guild_id = interaction.guild.id
        user_id = interaction.user.id
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]

        requested = []
        for raw in (job_1, job_2, job_3):
            key = (raw or "").strip().lower()
            if key and key not in requested:
                requested.append(key)

        if requested:
            if len(requested) > 3:
                requested = requested[:3]

            if not requested:
                await interaction.response.send_message("Provide at least one valid job key.", ephemeral=True)
                return

            for key in requested:
                d = get_job_def(key)
                if d is None:
                    await interaction.response.send_message(
                        f"Unknown job key `{key}`. Use `/job` and select from autocomplete or panel.",
                        ephemeral=True,
                    )
                    return
                if d.vip_only and not vip:
                    await interaction.response.send_message(f"**{d.name}** is VIP-locked.", ephemeral=True)
                    return

            await interaction.response.defer(ephemeral=True)

            async with self.sessionmaker() as session:
                async with session.begin():
                    await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
                    level = await get_level(session, guild_id=guild_id, user_id=user_id)

                    for key in requested:
                        d = get_job_def(key)
                        if d is None:
                            await interaction.followup.send(f"Unknown job key `{key}`.", ephemeral=True)
                            return

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

                    old_keys = await get_equipped_keys(session, guild_id=guild_id, user_id=user_id)

                    old_set = set(old_keys)
                    first_free = len(old_keys) == 0
                    cost = 0
                    for key in requested:
                        if key in old_set:
                            continue
                        d = get_job_def(key)
                        if d is None:
                            continue
                        add = JOB_SWITCH_COST[d.category]
                        if vip:
                            add = add // 2
                        if first_free:
                            add = 0
                            first_free = False
                        cost += add

            names = [get_job_def(k).name for k in requested if get_job_def(k) is not None]
            listing = "\n".join(f"{idx+1}. **{name}**" for idx, name in enumerate(names))
            msg = ("Set this loadout for free?\n" + listing) if cost <= 0 else ("Set this loadout?\n" + listing + f"\n\nCost: **{fmt_int(cost)} Silver**")
            view = EquipConfirmView(
                sessionmaker=self.sessionmaker,
                guild_id=guild_id,
                user_id=user_id,
                vip=vip,
                new_keys=requested,
                old_keys=old_keys,
                cost=cost,
            )
            await interaction.followup.send(msg, view=view, ephemeral=True)
            return

        async with self.sessionmaker() as session:
            async with session.begin():
                equipped = await get_equipped_key(session, guild_id=guild_id, user_id=user_id)
                equipped_keys = await get_equipped_keys(session, guild_id=guild_id, user_id=user_id)

        embed = make_panel_embed(user=interaction.user, vip=vip, page="standard", equipped=equipped, equipped_keys=equipped_keys)
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



    @app_commands.command(name="job_upgrade", description="Upgrade your currently equipped job tool for more income.")
    async def job_upgrade_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        guild_id = interaction.guild.id
        user_id = interaction.user.id

        await interaction.response.defer(ephemeral=True)

        status_msg = await interaction.followup.send("⚙️ Preparing upgrade...", ephemeral=True, wait=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                key = await get_equipped_key(session, guild_id=guild_id, user_id=user_id)
                if not key:
                    await status_msg.edit(content="You don’t have a job equipped. Use **/job** first.")
                    return

                d = get_job_def(key)
                if d is None:
                    await status_msg.edit(content="Your equipped job no longer exists. Re-equip with **/job**.")
                    return

                job_row = await get_or_create_job_row(session, job_key=key, name=d.name)
                ok, result_text, snap = await upgrade_once(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    job_row=job_row,
                    job_def=d,
                )

                if not ok:
                    await status_msg.edit(content=f"❌ {result_text}")
                    return

                await play_upgrade_animation(status_msg, label=snap.label)

                await status_msg.edit(
                    content=(
                        "✅ "
                        f"{result_text}\n"
                        f"Next upgrade cost: **{fmt_int(snap.next_cost)}** silver."
                    )
                )

    @app_commands.command(name="job_upgrade_info", description="View upgrade progress and next cost for a job.")
    @app_commands.describe(job="Optional job key. Defaults to your equipped job.")
    async def job_upgrade_info_cmd(self, interaction: discord.Interaction, job: Optional[str] = None):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        guild_id = interaction.guild.id
        user_id = interaction.user.id

        async with self.sessionmaker() as session:
            async with session.begin():
                key = (job or "").strip().lower()
                if not key:
                    eq = await get_equipped_key(session, guild_id=guild_id, user_id=user_id)
                    if not eq:
                        await interaction.response.send_message("No equipped job. Use **/job** first.", ephemeral=True)
                        return
                    key = eq

                d = get_job_def(key)
                if d is None:
                    await interaction.response.send_message(f"Unknown job key `{key}`.", ephemeral=True)
                    return

                job_row = await get_or_create_job_row(session, job_key=key, name=d.name)
                snap = await build_upgrade_snapshot(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    job_row=job_row,
                    job_def=d,
                )

        embed = discord.Embed(
            title=f"{d.name} Upgrade",
            description=(
                f"Tool: **{snap.label}**\n"
                f"Current Level: **{snap.level}**\n"
                f"Income Bonus: **+{snap.income_bonus_pct}%**\n"
                f"Next Cost: **{fmt_int(snap.next_cost)} silver**\n\n"
                "Each level increases income by a flat **25%**, and each new upgrade cost grows by **50%**."
            ),
            color=discord.Color.gold(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
    @job_admin.autocomplete("job")
    async def job_admin_autocomplete(self, interaction: discord.Interaction, current: str):
        cur = (current or "").lower()
        choices = [
            app_commands.Choice(name=f"{d.name} ({d.key})", value=d.key)
            for d in (mod.definition() for mod in JOB_MODULES.values())
            if cur in d.key or cur in d.name.lower()
        ]
        return choices[:25]
