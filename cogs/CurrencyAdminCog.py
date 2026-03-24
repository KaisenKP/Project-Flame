from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.app_commands import checks
from discord.ext import commands
from sqlalchemy import delete, func, select, update

from db.models import ActivityDailyRow, UserAchievementCounterRow, WalletRow, XpRow
from services.db import sessions
from services.users import ensure_user_rows
from services.xp import get_xp_progress


RECON_WORK_XP_BASE = 8
RECON_WORK_XP_MULTIPLIER = 1.75


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


class CurrencyAdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    @app_commands.command(name="currency_reset", description="Admin: reset Silver (and optionally Diamonds).")
    @app_commands.describe(
        user="Target user (leave empty to reset everyone)",
        silver="Reset Silver to this amount (default 0)",
        diamonds="Reset Diamonds to this amount (default 0)",
        include_diamonds="Also reset Diamonds (default True)",
    )
    @checks.has_permissions(manage_guild=True)
    async def currency_reset(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        silver: int = 0,
        diamonds: int = 0,
        include_diamonds: bool = True,
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        silver = max(int(silver), 0)
        diamonds = max(int(diamonds), 0)

        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                if user is None:
                    # Reset everyone
                    stmt = update(WalletRow).where(WalletRow.guild_id == guild_id).values(silver=silver)
                    if include_diamonds:
                        stmt = stmt.values(diamonds=diamonds)
                    res = await session.execute(stmt)

                    # res.rowcount is driver-dependent; safe to compute separately if needed
                    count = await session.scalar(
                        select(WalletRow.guild_id).where(WalletRow.guild_id == guild_id).count()  # type: ignore[attr-defined]
                    )
                else:
                    uid = int(user.id)
                    await ensure_user_rows(session, guild_id=guild_id, user_id=uid)

                    wallet = await session.scalar(
                        select(WalletRow).where(
                            WalletRow.guild_id == guild_id,
                            WalletRow.user_id == uid,
                        )
                    )
                    if wallet is None:
                        wallet = WalletRow(guild_id=guild_id, user_id=uid, silver=0, diamonds=0)
                        session.add(wallet)
                        await session.flush()

                    wallet.silver = silver
                    if include_diamonds:
                        wallet.diamonds = diamonds

        if user is None:
            msg = (
                f"✅ Reset currency for **everyone** in this server.\n"
                f"Silver → **{_fmt_int(silver)}**"
                + (f"\nDiamonds → **{_fmt_int(diamonds)}**" if include_diamonds else "")
            )
        else:
            msg = (
                f"✅ Reset currency for {user.mention}.\n"
                f"Silver → **{_fmt_int(silver)}**"
                + (f"\nDiamonds → **{_fmt_int(diamonds)}**" if include_diamonds else "")
            )

        await interaction.followup.send(msg, ephemeral=True)

    @app_commands.command(name="currency_wipe", description="Admin: delete all wallet rows for this server.")
    @app_commands.describe(confirm="Type WIPE to confirm (required)")
    @checks.has_permissions(manage_guild=True)
    async def currency_wipe(self, interaction: discord.Interaction, confirm: str):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        if (confirm or "").strip().upper() != "WIPE":
            await interaction.response.send_message("Refused. To wipe, run: `/currency_wipe confirm:WIPE`", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                res = await session.execute(delete(WalletRow).where(WalletRow.guild_id == guild_id))
                deleted = getattr(res, "rowcount", None)

        extra = f"Deleted rows: **{deleted}**" if isinstance(deleted, int) else "Wipe complete."
        await interaction.followup.send(f"🧹 Currency wipe done. {extra}", ephemeral=True)

    @app_commands.command(
        name="rebuild_levels_from_activity",
        description="Admin: rebuild XP/levels from messages, VC time, and /work count.",
    )
    @app_commands.describe(
        user="Optional target user. Leave empty to rebuild everyone in this server.",
        apply_changes="If false, shows a preview only (dry-run).",
    )
    @checks.has_permissions(manage_guild=True)
    async def rebuild_levels_from_activity(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        apply_changes: bool = False,
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        work_xp_per_use = int(round(RECON_WORK_XP_BASE * RECON_WORK_XP_MULTIPLIER))

        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                msg_stmt = (
                    select(
                        ActivityDailyRow.user_id,
                        func.coalesce(func.sum(ActivityDailyRow.message_count), 0).label("message_total"),
                        func.coalesce(func.sum(ActivityDailyRow.vc_seconds), 0).label("vc_seconds_total"),
                    )
                    .where(ActivityDailyRow.guild_id == guild_id)
                    .group_by(ActivityDailyRow.user_id)
                )
                if user is not None:
                    msg_stmt = msg_stmt.where(ActivityDailyRow.user_id == int(user.id))
                msg_rows = await session.execute(msg_stmt)
                activity_rows = {
                    int(uid): (int(msg_total or 0), int(vc_seconds or 0))
                    for uid, msg_total, vc_seconds in msg_rows.all()
                }

                work_stmt = (
                    select(UserAchievementCounterRow.user_id, UserAchievementCounterRow.counter_value)
                    .where(UserAchievementCounterRow.guild_id == guild_id)
                    .where(UserAchievementCounterRow.counter_key == "jobs_completed")
                )
                if user is not None:
                    work_stmt = work_stmt.where(UserAchievementCounterRow.user_id == int(user.id))
                work_rows = await session.execute(work_stmt)
                work_counts = {int(uid): int(count or 0) for uid, count in work_rows.all()}

                impacted_user_ids = set(activity_rows.keys()) | set(work_counts.keys())
                if user is not None:
                    impacted_user_ids.add(int(user.id))

                if not impacted_user_ids:
                    await interaction.followup.send(
                        "No stored activity found for this target, so nothing to rebuild.",
                        ephemeral=True,
                    )
                    return

                xp_stmt = select(XpRow).where(XpRow.guild_id == guild_id)
                if user is not None:
                    xp_stmt = xp_stmt.where(XpRow.user_id == int(user.id))
                xp_rows = await session.execute(xp_stmt)
                xp_by_user = {int(row.user_id): row for row in xp_rows.scalars().all()}

                changed = 0
                preview_lines: list[str] = []

                for uid in sorted(impacted_user_ids):
                    messages_total, vc_seconds_total = activity_rows.get(uid, (0, 0))
                    work_count = int(work_counts.get(uid, 0))

                    msg_xp = int(messages_total) * 12
                    vc_xp = (int(vc_seconds_total) // 60) * 60
                    work_xp = int(work_count) * int(work_xp_per_use)
                    reconstructed_xp = max(int(msg_xp + vc_xp + work_xp), 0)
                    reconstructed_level = int(get_xp_progress(reconstructed_xp).level)

                    row = xp_by_user.get(uid)
                    old_xp = int(getattr(row, "xp_total", 0) or 0)
                    old_level = int(getattr(row, "level_cached", 1) or 1)

                    if apply_changes:
                        if row is None:
                            row = XpRow(guild_id=guild_id, user_id=uid, xp_total=0, level_cached=1)
                            session.add(row)
                            await session.flush()
                        row.xp_total = reconstructed_xp
                        row.level_cached = reconstructed_level

                    if old_xp != reconstructed_xp or old_level != reconstructed_level:
                        changed += 1

                    if len(preview_lines) < 12:
                        preview_lines.append(
                            "• "
                            f"`{uid}`: L{old_level} ({_fmt_int(old_xp)} XP) → "
                            f"L{reconstructed_level} ({_fmt_int(reconstructed_xp)} XP) "
                            f"[msg={_fmt_int(messages_total)}, vc_min={_fmt_int(vc_seconds_total // 60)}, work={_fmt_int(work_count)}]"
                        )

        mode = "APPLIED" if apply_changes else "DRY-RUN"
        summary = [
            f"**Level rebuild ({mode}) complete**",
            f"Users analyzed: **{_fmt_int(len(impacted_user_ids))}**",
            f"Users with changed XP/level: **{_fmt_int(changed)}**",
            "",
            "Reconstruction formula used:",
            "- `message_xp = total_messages * 12`",
            "- `vc_xp = floor(total_vc_seconds / 60) * 60`",
            f"- `work_xp = jobs_completed * {work_xp_per_use}` (base `{RECON_WORK_XP_BASE}` × multiplier `{RECON_WORK_XP_MULTIPLIER}`)",
            "",
            "Sample results:",
            *preview_lines,
        ]
        await interaction.followup.send("\n".join(summary), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CurrencyAdminCog(bot))
