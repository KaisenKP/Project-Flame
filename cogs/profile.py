from __future__ import annotations

import io
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import func, select

from db.models import JobRow, ProfileBackgroundRow, ProfileSettingsRow, UserAchievementRow, UserJobSlotRow, WalletRow, XpRow
from services.achievement_catalog import sorted_achievements
from services.achievements import build_achievement_context, parse_unlock_condition
from services.db import sessions
from services.profile_backgrounds import (
    ALL_BACKGROUNDS,
    DEFAULT_BACKGROUNDS,
    ensure_profile_background_rows,
    resolve_background_key,
)
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


class ProfileEditView(discord.ui.View):
    def __init__(
        self,
        cog: "ProfileCog",
        *,
        guild_id: int,
        user_id: int,
        current_key: str,
        options: list[discord.SelectOption],
        timeout: float = 120.0,
    ):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.user_id = user_id
        self.current_key = current_key
        self.pending_key = current_key
        self.message: Optional[discord.Message] = None

        if options:
            self.bg_select = discord.ui.Select(
                placeholder="Choose a profile background",
                min_values=1,
                max_values=1,
                options=options,
                custom_id="profile:bg_select",
            )
            self.bg_select.callback = self._on_select  # type: ignore[assignment]
            self.add_item(self.bg_select)
        else:
            self.bg_select = discord.ui.Select(
                placeholder="No unlocked backgrounds available",
                min_values=1,
                max_values=1,
                options=[discord.SelectOption(label="No backgrounds unlocked", value="none")],
                disabled=True,
                custom_id="profile:bg_select_disabled",
            )
            self.add_item(self.bg_select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "Only the command user can use this menu.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    async def _on_select(self, interaction: discord.Interaction) -> None:
        selected = self.bg_select.values[0]
        if selected not in ALL_BACKGROUNDS:
            await interaction.response.send_message(
                "That background is invalid.",
                ephemeral=True,
            )
            return

        self.pending_key = selected
        preview_name = ALL_BACKGROUNDS[selected].name
        embed = interaction.message.embeds[0] if interaction.message and interaction.message.embeds else None
        if embed is None:
            await interaction.response.defer()
            return

        updated = self.cog._build_profile_edit_embed(
            current_key=self.current_key,
            preview_key=self.pending_key,
        )
        updated.add_field(
            name="Pending Selection",
            value=f"{preview_name} (`{selected}`)\nPress **Apply Background** to confirm.",
            inline=False,
        )
        await interaction.response.edit_message(embed=updated, view=self)

    @discord.ui.button(label="Apply Background", style=discord.ButtonStyle.success, custom_id="profile:apply")
    async def apply(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.pending_key not in ALL_BACKGROUNDS:
            await interaction.response.send_message("Invalid background selection.", ephemeral=True)
            return

        ok, reason = await self.cog._set_selected_background(
            guild_id=self.guild_id,
            user_id=self.user_id,
            background_key=self.pending_key,
        )
        if not ok:
            await interaction.response.send_message(reason or "Could not apply that background.", ephemeral=True)
            return

        self.current_key = self.pending_key
        updated = self.cog._build_profile_edit_embed(
            current_key=self.current_key,
            preview_key=self.pending_key,
        )
        updated.add_field(
            name="Updated",
            value=f"Equipped **{ALL_BACKGROUNDS[self.current_key].name}**.",
            inline=False,
        )
        await interaction.response.edit_message(embed=updated, view=self)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.secondary, custom_id="profile:close")
    async def close(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()


class ProfileCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessions = sessions()
        self.stamina = StaminaService()
        self.renderer = ProfileCardRenderer()

    @commands.hybrid_group(
        name="profile",
        description="Show your calling card profile.",
        fallback="view",
    )
    @app_commands.describe(user="View someone else's profile (optional).")
    async def profile_group(self, ctx: commands.Context, user: Optional[discord.Member] = None):
        if ctx.guild is None:
            await ctx.reply("This only works in a server.", ephemeral=True)
            return

        target: discord.Member = user or ctx.author  # type: ignore[assignment]

        try:
            await ctx.defer(thinking=True)
        except Exception:
            pass

        png = await self._render_profile_card(
            guild_id=ctx.guild.id,
            target=target,
        )
        if png is None:
            await ctx.reply("Failed to render profile card.", ephemeral=True)
            return

        file = discord.File(fp=io.BytesIO(png), filename="profile-card.png")
        await ctx.send(file=file)

    @profile_group.command(name="edit", description="Customize your profile card background.")
    async def profile_edit(self, ctx: commands.Context):
        if ctx.guild is None:
            await ctx.reply("This only works in a server.", ephemeral=True)
            return

        try:
            await ctx.defer(ephemeral=True, thinking=True)
        except Exception:
            pass

        guild_id = ctx.guild.id
        user_id = ctx.author.id

        async with self.sessions() as session:
            async with session.begin():
                settings = await ensure_profile_background_rows(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )
                unlocked_rows = await self._get_unlocked_background_rows(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

        unlocked_keys = sorted(
            {r.background_key for r in unlocked_rows if r.background_key in ALL_BACKGROUNDS},
            key=lambda k: (ALL_BACKGROUNDS[k].source, ALL_BACKGROUNDS[k].name.lower()),
        )

        current_key = resolve_background_key(settings.selected_background_key if settings else None)
        if current_key not in unlocked_keys and current_key in ALL_BACKGROUNDS:
            unlocked_keys.insert(0, current_key)

        options: list[discord.SelectOption] = []
        for key in unlocked_keys:
            bg = ALL_BACKGROUNDS[key]
            options.append(
                discord.SelectOption(
                    label=bg.name,
                    value=bg.key,
                    description=f"Category: {bg.source}",
                    default=(key == current_key),
                )
            )

        embed = self._build_profile_edit_embed(
            current_key=current_key,
            preview_key=current_key,
        )

        view = ProfileEditView(
            self,
            guild_id=guild_id,
            user_id=user_id,
            current_key=current_key,
            options=options,
        )
        sent = await ctx.send(embed=embed, view=view, ephemeral=True)
        if isinstance(sent, discord.Message):
            view.message = sent


    @profile_group.command(name="achievements", description="View your achievements dashboard and unlock paths.")
    @app_commands.describe(user="View another member's achievements dashboard (optional).")
    async def profile_achievements(self, ctx: commands.Context, user: Optional[discord.Member] = None):
        if ctx.guild is None:
            await ctx.reply("This only works in a server.", ephemeral=True)
            return

        target: discord.Member = user or ctx.author  # type: ignore[assignment]

        try:
            await ctx.defer(thinking=True)
        except Exception:
            pass

        async with self.sessions() as session:
            async with session.begin():
                unlocked_count = int(
                    await session.scalar(
                        select(func.count(UserAchievementRow.id)).where(
                            UserAchievementRow.guild_id == ctx.guild.id,
                            UserAchievementRow.user_id == target.id,
                        )
                    )
                    or 0
                )
                ach_ctx = await build_achievement_context(
                    session,
                    guild_id=ctx.guild.id,
                    user_id=target.id,
                )

        defs = sorted_achievements()
        total = len(defs)

        pending_lines: list[str] = []
        for definition in defs:
            parsed = parse_unlock_condition(definition.unlock_condition)
            if parsed is None:
                continue
            stat_key, threshold = parsed
            current = int(getattr(ach_ctx, stat_key, 0))
            if current >= threshold:
                continue
            pending_lines.append(
                f"{definition.icon} **{definition.name}**\n"
                f"Goal: {definition.description}\n"
                f"Progress: `{_fmt_int(current)}/{_fmt_int(threshold)}`"
            )
            if len(pending_lines) >= 8:
                break

        completion_pct = (unlocked_count / total * 100.0) if total else 0.0
        embed = discord.Embed(
            title=f"🏆 {target.display_name}'s Achievement Dashboard",
            description=(
                "Track your unlocks, chase your next milestones, and farm that leaderboard aura.\n"
                "No gatekeeping, just clean goals and chaos."
            ),
            color=discord.Color.gold(),
        )
        embed.add_field(
            name="Completion",
            value=f"**{_fmt_int(unlocked_count)} / {_fmt_int(total)}** unlocked ({completion_pct:.1f}%)",
            inline=False,
        )
        embed.add_field(
            name="Social Questboard",
            value=(
                "• Post your first selfie in <#1460859587275001866>\n"
                "• Type your first 100 messages in <#1460856536795578443>\n"
                "• Keep chatting to stack message and chatroom milestones"
            ),
            inline=False,
        )
        embed.add_field(
            name="Your Next Unlocks",
            value="\n\n".join(pending_lines) if pending_lines else "You are caught up. This is elite behavior.",
            inline=False,
        )
        embed.set_footer(text="Tip: use /work, /business, and chat regularly to progress multiple tracks at once.")
        await ctx.send(embed=embed)

    async def _render_profile_card(self, *, guild_id: int, target: discord.Member) -> Optional[bytes]:
        user_id = target.id
        vip = is_vip_member(target)

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

        try:
            return self.renderer.render(payload)
        except Exception:
            log.exception("Failed rendering profile card", extra={"user_id": user_id, "guild_id": guild_id, "bg_key": bg_key})
            return None

    async def _set_selected_background(
        self,
        *,
        guild_id: int,
        user_id: int,
        background_key: str,
    ) -> tuple[bool, Optional[str]]:
        if background_key not in ALL_BACKGROUNDS:
            return False, "That background does not exist."

        async with self.sessions() as session:
            async with session.begin():
                settings = await ensure_profile_background_rows(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )

                owned = await session.scalar(
                    select(ProfileBackgroundRow).where(
                        ProfileBackgroundRow.guild_id == guild_id,
                        ProfileBackgroundRow.user_id == user_id,
                        ProfileBackgroundRow.background_key == background_key,
                    )
                )
                if owned is None:
                    return False, "You do not own that background."

                settings.selected_background_key = background_key

        return True, None

    def _build_profile_edit_embed(self, *, current_key: str, preview_key: str) -> discord.Embed:
        current_bg = ALL_BACKGROUNDS.get(current_key, ALL_BACKGROUNDS[DEFAULT_BACKGROUNDS[0].key])
        preview_bg = ALL_BACKGROUNDS.get(preview_key, current_bg)

        embed = discord.Embed(
            title="Profile Customization",
            description="Use the options below to customize your profile card appearance.",
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Current Background",
            value=f"{current_bg.name} (`{current_bg.key}`)",
            inline=False,
        )
        embed.add_field(
            name="Available Backgrounds",
            value="Choose from your unlocked backgrounds using the menu below.",
            inline=False,
        )
        embed.add_field(
            name="How to Unlock Backgrounds",
            value="• events\n• store purchases\n• achievements\n• seasonal rewards",
            inline=False,
        )
        embed.add_field(
            name="Preview Instructions",
            value=(
                "Selecting a background updates the pending preview. "
                "Press **Apply Background** to confirm the change."
            ),
            inline=False,
        )
        embed.add_field(
            name="Preview",
            value=f"Pending: **{preview_bg.name}** (`{preview_bg.key}`)",
            inline=False,
        )
        embed.set_footer(text="Profile appearance changes only affect the visual card.")
        return embed

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

    async def _get_unlocked_background_rows(
        self,
        session,
        *,
        guild_id: int,
        user_id: int,
    ) -> list[ProfileBackgroundRow]:
        res = await session.execute(
            select(ProfileBackgroundRow).where(
                ProfileBackgroundRow.guild_id == guild_id,
                ProfileBackgroundRow.user_id == user_id,
            )
        )
        return list(res.scalars().all())

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
