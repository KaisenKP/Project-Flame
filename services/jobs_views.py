# services/jobs_views.py
from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from sqlalchemy import select

from db.models import WalletRow
from services.users import ensure_user_rows
from services.jobs_core import (
    JOB_DEFS,
    JOB_SWITCH_COST,
    JOB_UNLOCK_LEVEL,
    JobCategory,
    ensure_job_row,
    fmt_int,
    get_equipped_key,
    get_level,
    set_equipped_key,
)
from services.jobs_embeds import make_job_info_embed, make_panel_embed, make_rules_embed


def _discounted_switch_cost(*, vip: bool, base_cost: int, first_free: bool) -> int:
    if first_free:
        return 0
    if vip:
        return max(int(base_cost) // 2, 0)
    return max(int(base_cost), 0)


class EquipConfirmView(discord.ui.View):
    def __init__(
        self,
        *,
        sessionmaker,
        guild_id: int,
        user_id: int,
        vip: bool,
        new_key: str,
        old_key: Optional[str],
        cost: int,
        first_free: bool,
        timeout: float = 45.0,
    ):
        super().__init__(timeout=timeout)
        self.sessionmaker = sessionmaker
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.vip = bool(vip)
        self.new_key = (new_key or "").strip().lower()
        self.old_key = (old_key or "").strip().lower() if old_key else None
        self.base_cost = int(cost)
        self.first_free = bool(first_free)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user is None or interaction.user.id != self.user_id:
            await interaction.response.send_message("This confirmation isn’t for you.", ephemeral=True)
            return False
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("Wrong server.", ephemeral=True)
            return False
        return True

    async def _finalize(self, interaction: discord.Interaction, *, msg: str) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        try:
            await interaction.response.edit_message(content=msg, view=self)
        except Exception:
            try:
                await interaction.followup.send(msg, ephemeral=True)
            except Exception:
                pass

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        d = JOB_DEFS.get(self.new_key)
        if d is None:
            await self._finalize(interaction, msg="That job no longer exists. Use `/job` again.")
            return

        if d.vip_only and not self.vip:
            await self._finalize(interaction, msg="That job is VIP-locked.")
            return

        # Recompute cost server-side (vip discount lives here)
        base_cost = JOB_SWITCH_COST[d.category]
        final_cost = _discounted_switch_cost(vip=self.vip, base_cost=base_cost, first_free=self.first_free)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.user_id)

                # Re-check unlock (VIP bypasses unlock)
                lvl = await get_level(session, guild_id=self.guild_id, user_id=self.user_id)
                need = JOB_UNLOCK_LEVEL[d.category]
                if (not self.vip) and lvl < need:
                    await self._finalize(interaction, msg=f"🔒 **{d.name}** unlocks at **Level {need}**.")
                    return

                row = await ensure_job_row(session, key=d.key, name=d.name)
                if not bool(getattr(row, "enabled", True)):
                    await self._finalize(interaction, msg=f"Job `{d.key}` is disabled.")
                    return

                current = await get_equipped_key(session, guild_id=self.guild_id, user_id=self.user_id)
                if current == self.new_key:
                    await self._finalize(interaction, msg=f"✅ You already have **{d.name}** equipped.")
                    return

                if final_cost > 0:
                    wallet = await session.scalar(
                        select(WalletRow).where(
                            WalletRow.guild_id == self.guild_id,
                            WalletRow.user_id == self.user_id,
                        )
                    )
                    if wallet is None:
                        wallet = WalletRow(guild_id=self.guild_id, user_id=self.user_id, silver=0, diamonds=0)
                        session.add(wallet)
                        await session.flush()

                    if int(getattr(wallet, "silver", 0)) < final_cost:
                        await self._finalize(
                            interaction,
                            msg=f"Not enough Silver. Need **{fmt_int(final_cost)}**.",
                        )
                        return

                    wallet.silver -= int(final_cost)

                await set_equipped_key(session, guild_id=self.guild_id, user_id=self.user_id, job_key=self.new_key)

        if final_cost <= 0:
            await self._finalize(interaction, msg=f"✅ Equipped **{d.name}** (free).")
        else:
            await self._finalize(interaction, msg=f"✅ Equipped **{d.name}** for **{fmt_int(final_cost)}** Silver.")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.gray)
    async def cancel_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._finalize(interaction, msg="Cancelled.")


class _JobSelect(discord.ui.Select):
    def __init__(self, *, vip: bool):
        self.vip = bool(vip)

        choices: list[discord.SelectOption] = []
        for key, d in sorted(JOB_DEFS.items(), key=lambda kv: kv[1].name.lower()):
            if d.vip_only and not self.vip:
                continue
            desc = f"{d.category.value} • unlock {JOB_UNLOCK_LEVEL[d.category]}"
            if d.vip_only:
                desc = "VIP job"
            choices.append(discord.SelectOption(label=d.name, value=key, description=desc))

        # Discord limit
        choices = choices[:25]

        super().__init__(
            placeholder="Select a job to preview…",
            min_values=1,
            max_values=1,
            options=choices,
        )

    async def callback(self, interaction: discord.Interaction):
        view: JobsPanelView = self.view  # type: ignore[assignment]
        key = (self.values[0] or "").strip().lower()
        view.selected_key = key

        equipped = None
        async with view.sessionmaker() as session:
            async with session.begin():
                equipped = await get_equipped_key(session, guild_id=view.guild_id, user_id=view.user_id)

        embed = make_job_info_embed(vip=view.vip, job_key=key, equipped=equipped)
        await interaction.response.edit_message(embed=embed, view=view)


class JobsPanelView(discord.ui.View):
    def __init__(
        self,
        *,
        sessionmaker,
        vip: bool,
        guild_id: int,
        user_id: int,
        timeout: float = 120.0,
    ):
        super().__init__(timeout=timeout)
        self.sessionmaker = sessionmaker
        self.vip = bool(vip)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)

        self.selected_key: Optional[str] = None

        self.add_item(_JobSelect(vip=self.vip))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user is None or interaction.user.id != self.user_id:
            await interaction.response.send_message("This panel isn’t for you.", ephemeral=True)
            return False
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("Wrong server.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Equip Selected", style=discord.ButtonStyle.green)
    async def equip_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self.selected_key:
            await interaction.response.send_message("Pick a job in the dropdown first.", ephemeral=True)
            return

        key = (self.selected_key or "").strip().lower()
        d = JOB_DEFS.get(key)
        if d is None:
            await interaction.response.send_message("That job no longer exists. Use `/job` again.", ephemeral=True)
            return

        if d.vip_only and not self.vip:
            await interaction.response.send_message("That job is VIP-locked.", ephemeral=True)
            return

        async with self.sessionmaker() as session:
            async with session.begin():
                old = await get_equipped_key(session, guild_id=self.guild_id, user_id=self.user_id)

        first_free = old is None
        base_cost = JOB_SWITCH_COST[d.category]
        final_cost = _discounted_switch_cost(vip=self.vip, base_cost=base_cost, first_free=first_free)

        msg = "Equip this job for free?" if final_cost <= 0 else f"Switch jobs for **{fmt_int(final_cost)} Silver**?"
        view = EquipConfirmView(
            sessionmaker=self.sessionmaker,
            guild_id=self.guild_id,
            user_id=self.user_id,
            vip=self.vip,
            new_key=key,
            old_key=old,
            cost=base_cost,
            first_free=first_free,
        )
        await interaction.response.send_message(msg, view=view, ephemeral=True)

    @discord.ui.button(label="Panel", style=discord.ButtonStyle.blurple)
    async def panel_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        async with self.sessionmaker() as session:
            async with session.begin():
                equipped = await get_equipped_key(session, guild_id=self.guild_id, user_id=self.user_id)

        embed = make_panel_embed(user=interaction.user, vip=self.vip, page="standard", equipped=equipped)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Rules", style=discord.ButtonStyle.gray)
    async def rules_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        embed = make_rules_embed(vip=self.vip)
        await interaction.response.edit_message(embed=embed, view=self)
