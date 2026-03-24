from __future__ import annotations

import logging
from typing import Optional

import discord
from discord.ui import Button, Select

from services.job_hub import (
    MAX_JOB_HUB_SLOTS,
    assign_job_to_slot,
    buy_or_upgrade_tool,
    ensure_job_hub_slots,
    get_wallet,
    get_slot_snapshot,
    prestige_slot,
    set_active_slot,
    set_selected_tool,
    slot_label,
    tool_defs_for,
)
from services.jobs_core import JOB_DEFS, JOB_SWITCH_COST, fmt_int, get_level, unlock_level_for
from services.jobs_embeds import make_job_hub_embed

log = logging.getLogger(__name__)


class JobPicker(Select):
    def __init__(self, *, vip: bool, slot_index: int):
        self.slot_index = slot_index
        options = []
        for job in sorted(JOB_DEFS.values(), key=lambda item: item.name.lower()):
            if job.vip_only and not vip:
                continue
            options.append(discord.SelectOption(label=job.name, value=job.key, description=f"Unlock Lv {unlock_level_for(job.key, job.category)}"))
        super().__init__(
            placeholder=f"Assign a job to {slot_label(slot_index)}",
            min_values=1,
            max_values=1,
            options=options[:25],
            custom_id=f"jobhub:pick:{slot_index}",
        )

    async def callback(self, interaction: discord.Interaction):
        view: JobHubView = self.view  # type: ignore[assignment]
        await view.handle_job_assignment(interaction, self.slot_index, self.values[0])


class ToolPicker(Select):
    def __init__(self, *, slot_index: int, job_key: str):
        self.slot_index = slot_index
        self.job_key = job_key
        options = [discord.SelectOption(label=tool.name, value=tool.key, description=tool.description[:100] or tool.name) for tool in tool_defs_for(job_key)]
        super().__init__(
            placeholder="Select an active tool",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"jobhub:tool:{slot_index}:{job_key}",
        )

    async def callback(self, interaction: discord.Interaction):
        view: JobHubView = self.view  # type: ignore[assignment]
        async with view.sessionmaker() as session:
            async with session.begin():
                await set_selected_tool(session, guild_id=view.guild_id, user_id=view.user_id, vip=view.vip, slot_index=self.slot_index, tool_key=self.values[0])
        await view.refresh(interaction, notice="✅ Active tool updated.")




async def open_job_hub(*, interaction: discord.Interaction, sessionmaker, guild_id: int, user_id: int, vip: bool, selected_slot: int = 0, section: str = "overview", notice: Optional[str] = None) -> None:
    view = JobHubView(sessionmaker=sessionmaker, guild_id=guild_id, user_id=user_id, vip=vip, selected_slot=selected_slot, section=section)
    async with sessionmaker() as session:
        async with session.begin():
            slot_snap = await get_slot_snapshot(session, guild_id=guild_id, user_id=user_id, vip=vip, slot_index=selected_slot)
    embed = make_job_hub_embed(user=interaction.user, vip=vip, slot_snap=slot_snap, section=section)
    ephemeral = interaction.guild is not None
    if interaction.response.is_done():
        view.message = await interaction.followup.send(embed=embed, view=view, ephemeral=ephemeral, content=notice, wait=True)
    else:
        await interaction.response.send_message(embed=embed, view=view, ephemeral=ephemeral, content=notice)
        await view.bind_to_interaction(interaction)

class JobHubView(discord.ui.View):
    def __init__(self, *, sessionmaker, guild_id: int, user_id: int, vip: bool, selected_slot: int = 0, section: str = "overview", timeout: float = 900.0):
        super().__init__(timeout=timeout)
        self.sessionmaker = sessionmaker
        self.guild_id = guild_id
        self.user_id = user_id
        self.vip = vip
        self.selected_slot = selected_slot
        self.section = section
        self.message: discord.Message | None = None
        self._build_static_buttons()

    def _build_static_buttons(self) -> None:
        for idx in range(MAX_JOB_HUB_SLOTS):
            btn = Button(
                label=f"Slot {idx+1}",
                style=discord.ButtonStyle.primary if idx == self.selected_slot else discord.ButtonStyle.secondary,
                row=0,
                custom_id=f"jobhub:slot:{idx}",
            )
            btn.callback = self._make_slot_callback(idx)
            self.add_item(btn)

        for label, section, row in (("Overview", "overview", 1), ("Switch Job", "switch", 1), ("Tools & Upgrades", "tools", 1), ("Perks", "perks", 2), ("Prestige", "prestige", 2)):
            btn = Button(
                label=label,
                style=discord.ButtonStyle.success if self.section == section else discord.ButtonStyle.secondary,
                row=row,
                custom_id=f"jobhub:section:{section}",
            )
            btn.callback = self._make_section_callback(section)
            self.add_item(btn)

        work_btn = Button(label="Work", style=discord.ButtonStyle.success, emoji="💼", row=3, custom_id="jobhub:work")
        work_btn.callback = self.work_now
        self.add_item(work_btn)

        upgrade_btn = Button(label="Upgrade Selected Tool", style=discord.ButtonStyle.primary, row=3, custom_id="jobhub:upgrade")
        upgrade_btn.callback = self.upgrade_tool
        self.add_item(upgrade_btn)

        prestige_btn = Button(label="Prestige Slot", style=discord.ButtonStyle.danger, row=3, custom_id="jobhub:prestige")
        prestige_btn.callback = self.prestige_btn
        self.add_item(prestige_btn)

    def _dynamic_refresh(self, slot_snap) -> None:
        for item in list(self.children):
            if isinstance(item, Select):
                self.remove_item(item)
        if self.section == "switch":
            self.add_item(JobPicker(vip=self.vip, slot_index=self.selected_slot))
        elif self.section == "tools" and slot_snap.job_key:
            self.add_item(ToolPicker(slot_index=self.selected_slot, job_key=slot_snap.job_key))

    def _make_slot_callback(self, slot_index: int):
        async def callback(interaction: discord.Interaction):
            async with self.sessionmaker() as session:
                async with session.begin():
                    try:
                        await set_active_slot(session, guild_id=self.guild_id, user_id=self.user_id, vip=self.vip, slot_index=slot_index)
                    except ValueError:
                        await interaction.response.send_message("That slot is locked right now.", ephemeral=True)
                        return
            self.selected_slot = slot_index
            await self.refresh(interaction)
        return callback

    def _make_section_callback(self, section: str):
        async def callback(interaction: discord.Interaction):
            self.section = section
            await self.refresh(interaction)
        return callback

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This Job Hub belongs to another user.", ephemeral=True)
            return False
        return True

    async def bind_to_interaction(self, interaction: discord.Interaction) -> None:
        try:
            self.message = await interaction.original_response()
        except Exception:
            log.debug("Job Hub could not bind original response message", exc_info=True)

    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True

        if self.message is None:
            return

        try:
            await self.message.edit(
                content="This Job Hub expired. Run `/job` to open a fresh panel.",
                view=self,
            )
        except (discord.NotFound, discord.HTTPException):
            log.debug(
                "Job Hub timeout cleanup could not edit message_id=%s user_id=%s",
                getattr(self.message, "id", None),
                self.user_id,
                exc_info=True,
            )

    async def _safe_edit(
        self,
        interaction: discord.Interaction,
        *,
        embed: discord.Embed,
        content: Optional[str],
    ) -> None:
        response_open = not interaction.response.is_done()

        if response_open:
            try:
                await interaction.response.edit_message(embed=embed, view=self, content=content)
                self.message = interaction.message or self.message
                return
            except discord.NotFound:
                log.info(
                    "Primary Job Hub response edit returned not found; attempting recovery for user_id=%s message_id=%s",
                    self.user_id,
                    getattr(interaction.message, "id", None),
                )
            except discord.HTTPException:
                log.warning(
                    "Primary Job Hub response edit failed for user_id=%s message_id=%s; attempting recovery",
                    self.user_id,
                    getattr(interaction.message, "id", None),
                    exc_info=True,
                )

        edit_attempts = []
        if self.message is not None:
            edit_attempts.append(("bound_message", self.message.edit))
        edit_attempts.append(("original_response", interaction.edit_original_response))
        if interaction.message is not None:
            edit_attempts.append(("component_message", interaction.message.edit))
            edit_attempts.append(("followup_message", lambda **kwargs: interaction.followup.edit_message(interaction.message.id, **kwargs)))

        for label, editor in edit_attempts:
            try:
                await editor(embed=embed, view=self, content=content)
                if interaction.message is not None:
                    self.message = interaction.message
                return
            except discord.NotFound:
                log.info(
                    "Job Hub %s edit returned not found for user_id=%s message_id=%s",
                    label,
                    self.user_id,
                    getattr(interaction.message, "id", None),
                )
            except discord.HTTPException:
                log.warning(
                    "Job Hub %s edit failed for user_id=%s message_id=%s",
                    label,
                    self.user_id,
                    getattr(interaction.message, "id", None),
                    exc_info=True,
                )

        stale_message = "That Job Hub is stale. Please run `/job` to open a fresh panel."

        if response_open:
            try:
                await interaction.response.send_message(stale_message, ephemeral=True)
                return
            except discord.NotFound:
                log.info(
                    "Job Hub stale response send returned not found for user_id=%s message_id=%s",
                    self.user_id,
                    getattr(interaction.message, "id", None),
                )
            except discord.HTTPException:
                log.warning(
                    "Job Hub stale response send failed for user_id=%s message_id=%s",
                    self.user_id,
                    getattr(interaction.message, "id", None),
                    exc_info=True,
                )

        try:
            await interaction.followup.send(stale_message, ephemeral=True)
        except discord.NotFound:
            log.info(
                "Job Hub stale followup send returned not found for user_id=%s message_id=%s",
                self.user_id,
                getattr(interaction.message, "id", None),
            )
        except discord.HTTPException:
            log.warning(
                "Job Hub stale followup send failed for user_id=%s message_id=%s",
                self.user_id,
                getattr(interaction.message, "id", None),
                exc_info=True,
            )

    async def refresh(self, interaction: discord.Interaction, notice: Optional[str] = None) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                slot_snap = await get_slot_snapshot(session, guild_id=self.guild_id, user_id=self.user_id, vip=self.vip, slot_index=self.selected_slot)
        self.clear_items()
        self._build_static_buttons()
        self._dynamic_refresh(slot_snap)
        embed = make_job_hub_embed(user=interaction.user, vip=self.vip, slot_snap=slot_snap, section=self.section)
        await self._safe_edit(interaction, embed=embed, content=notice)

    async def _update_after_deferred_interaction(
        self,
        interaction: discord.Interaction,
        *,
        slot_snap,
        notice: Optional[str] = None,
    ) -> None:
        self.clear_items()
        self._build_static_buttons()
        self._dynamic_refresh(slot_snap)
        embed = make_job_hub_embed(user=interaction.user, vip=self.vip, slot_snap=slot_snap, section=self.section)
        try:
            self.message = await interaction.original_response()
        except Exception:
            self.message = interaction.message or self.message
        await interaction.edit_original_response(embed=embed, view=self, content=notice)

    async def handle_job_assignment(self, interaction: discord.Interaction, slot_index: int, job_key: str) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                level = await get_level(session, guild_id=self.guild_id, user_id=self.user_id)
                job = JOB_DEFS[job_key]
                if job.vip_only and not self.vip:
                    await interaction.response.send_message("That job is VIP-only.", ephemeral=True)
                    return
                unlock_level = unlock_level_for(job.key, job.category)
                if level < unlock_level and not self.vip:
                    await interaction.response.send_message(f"🔒 {job.name} unlocks at Level {unlock_level}.", ephemeral=True)
                    return
                snap = await get_slot_snapshot(session, guild_id=self.guild_id, user_id=self.user_id, vip=self.vip, slot_index=slot_index)
                existing = snap.job_key
                cost = 0 if existing is None else JOB_SWITCH_COST[job.category]
                if existing != job_key and cost > 0 and not self.vip:
                    wallet = await get_wallet(session, guild_id=self.guild_id, user_id=self.user_id)
                    if int(wallet.silver) < cost:
                        await interaction.response.send_message(f"Need **{fmt_int(cost)}** Silver to switch this slot.", ephemeral=True)
                        return
                    wallet.silver -= cost
                    wallet.silver_spent += cost
                await assign_job_to_slot(session, guild_id=self.guild_id, user_id=self.user_id, vip=self.vip, slot_index=slot_index, job_key=job_key)
        self.selected_slot = slot_index
        self.section = "overview"
        await self.refresh(interaction, notice=f"✅ {slot_label(slot_index)} set to **{JOB_DEFS[job_key].name}**.")

    async def work_now(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("This button only works in the original server.", ephemeral=True)
            return

        cog = interaction.client.get_cog("WorkCog") if interaction.client else None
        cmd = getattr(cog, "work_cmd", None) if cog is not None else None
        if cog is None or cmd is None or not hasattr(cmd, "callback"):
            await interaction.response.send_message("Work command is currently unavailable.", ephemeral=True)
            return

        await cmd.callback(cog, interaction)

    async def upgrade_tool(self, interaction: discord.Interaction):
        async with self.sessionmaker() as session:
            async with session.begin():
                snap = await get_slot_snapshot(session, guild_id=self.guild_id, user_id=self.user_id, vip=self.vip, slot_index=self.selected_slot)
                if not snap.job_key or not snap.selected_tool_key:
                    await interaction.response.send_message("Assign a job and select a tool first.", ephemeral=True)
                    return
                ok, message = await buy_or_upgrade_tool(session, guild_id=self.guild_id, user_id=self.user_id, slot_index=self.selected_slot, job_key=snap.job_key, tool_key=snap.selected_tool_key)
        if not ok:
            await interaction.response.send_message(message, ephemeral=True)
            return
        await self.refresh(interaction, notice=f"✅ {message}")

    async def prestige_btn(self, interaction: discord.Interaction):
        log.debug(
            "Prestige button clicked: guild_id=%s user_id=%s actor_id=%s slot_index=%s message_id=%s",
            self.guild_id,
            self.user_id,
            interaction.user.id,
            self.selected_slot,
            getattr(interaction.message, "id", None),
        )
        await interaction.response.defer()
        try:
            async with self.sessionmaker() as session:
                snap = await get_slot_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.user_id,
                    vip=self.vip,
                    slot_index=self.selected_slot,
                )
                if not snap.job_key:
                    await interaction.followup.send("Assign a job first.", ephemeral=True)
                    return

                ok, message, updated_snap = await prestige_slot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.user_id,
                    slot_index=self.selected_slot,
                    job_key=snap.job_key,
                    vip=self.vip,
                )
                if not ok:
                    await session.rollback()
                    await interaction.followup.send(message, ephemeral=True)
                    return

                log.debug(
                    "Prestige commit starting: guild_id=%s user_id=%s slot_index=%s job_key=%s",
                    self.guild_id,
                    self.user_id,
                    self.selected_slot,
                    snap.job_key,
                )
                await session.commit()
                log.debug(
                    "Prestige DB commit complete: guild_id=%s user_id=%s slot_index=%s job_key=%s",
                    self.guild_id,
                    self.user_id,
                    self.selected_slot,
                    snap.job_key,
                )

                refreshed_snap = updated_snap
                if refreshed_snap is None:
                    refreshed_snap = await get_slot_snapshot(
                        session,
                        guild_id=self.guild_id,
                        user_id=self.user_id,
                        vip=self.vip,
                        slot_index=self.selected_slot,
                    )

            await self._update_after_deferred_interaction(
                interaction,
                slot_snap=refreshed_snap,
                notice=f"✅ {message}",
            )
        except Exception:
            log.exception(
                "Prestige button failed: guild_id=%s user_id=%s actor_id=%s slot_index=%s",
                self.guild_id,
                self.user_id,
                interaction.user.id,
                self.selected_slot,
            )
            await interaction.followup.send(
                "Something went wrong while applying prestige. Please try again.",
                ephemeral=True,
            )
