from __future__ import annotations

from typing import Optional, Tuple

import discord
from discord import app_commands
from discord.app_commands import checks
from discord.ext import commands
from sqlalchemy import select

from services.db import sessions
from services.jobs_core import JOB_DEFS, JobRow, JobCategory

# IMPORTANT:
# This import MUST match the filename where you pasted that big module.
# If your file is services/job_progression.py, keep this as-is.
# If you named it something else, change ONLY this line.
from services.job_progression import JobTier, migrate_job_xp_multiplier


def _tier_for_category(cat: JobCategory) -> JobTier:
    if cat == JobCategory.EASY:
        return JobTier.EASY
    if cat == JobCategory.STABLE:
        return JobTier.STABLE
    if cat == JobCategory.HARD:
        return JobTier.HARD
    return JobTier.STABLE


class _LiveJobMetaLookup:
    async def get_meta(self, session, *, job_id: int) -> Tuple[str, JobTier]:
        job_row = await session.scalar(select(JobRow).where(JobRow.id == int(job_id)))
        if job_row is None:
            raise RuntimeError(f"JobRow not found for job_id={job_id}")

        key = (getattr(job_row, "key", None) or "").strip().lower()
        if not key:
            raise RuntimeError(f"JobRow.key missing/blank for job_id={job_id}")

        d = JOB_DEFS.get(key)
        if d is None:
            raise RuntimeError(f"Job key `{key}` not found in JOB_DEFS (job_id={job_id})")

        tier = _tier_for_category(d.category)
        return key, tier


class JobXPMigrationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    @app_commands.command(
        name="job_xp_migrate",
        description="Admin: retroactively multiply job progress (default x10). Dry-run by default.",
    )
    @app_commands.describe(
        apply="If true, writes to DB. If false, dry-run only.",
        factor="Multiplier (10 = x10).",
        all_guilds="If true, runs across ALL guilds in DB. If false, only this server.",
    )
    @checks.has_permissions(manage_guild=True)
    async def job_xp_migrate(
        self,
        interaction: discord.Interaction,
        apply: bool = False,
        factor: float = 10.0,
        all_guilds: bool = False,
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        if factor <= 0:
            await interaction.response.send_message("factor must be > 0", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        lookup = _LiveJobMetaLookup()
        guild_id: Optional[int] = None if all_guilds else int(interaction.guild.id)
        dry_run = not bool(apply)

        async with self.sessionmaker() as session:
            try:
                async with session.begin():
                    touched = await migrate_job_xp_multiplier(
                        session,
                        factor=float(factor),
                        lookup=lookup,
                        guild_id=guild_id,
                        dry_run=dry_run,
                    )
            except Exception as e:
                await interaction.followup.send(
                    f"❌ Migration failed:\n```{type(e).__name__}: {e}```",
                    ephemeral=True,
                )
                return

        mode = "APPLIED ✅" if apply else "DRY RUN 🧪"
        scope = "ALL guilds" if all_guilds else f"guild {interaction.guild.id}"

        await interaction.followup.send(
            f"{mode}\n"
            f"- factor: x{factor}\n"
            f"- scope: {scope}\n"
            f"- rows touched: {touched}\n"
            f"\nIf this looks right, run again with `apply:true`.",
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(JobXPMigrationCog(bot))