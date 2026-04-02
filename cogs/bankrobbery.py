from __future__ import annotations

from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

from db.models import BankRobberyHistoryRow, BankRobberyLobbyRow
from services.bankrobbery.catalog import BankApproach, FinalePhase, get_template
from services.bankrobbery.finale import calculate_outcome, run_entry, run_escape, run_loot_round, run_vault, use_override
from services.bankrobbery.lobby import (
    auto_configure_crew,
    create_lobby,
    finalize_lobby,
    get_active_lobby_for_user,
    join_lobby,
    leave_lobby,
    list_participants,
    set_ready,
    start_finale,
)
from services.bankrobbery.prep import complete_prep, prep_summary
from services.bankrobbery.progression import get_cooldowns, get_or_create_profile
from services.bankrobbery.rewards import get_or_create_crowns_wallet, get_or_create_wallet, grant_lootbox
from services.bankrobbery.ui import (
    HeistHubView,
    build_finale_embed,
    build_hub_embed,
    build_lobby_embed,
    build_prep_embed,
    build_results_embed,
    validate_view_component_rows,
)
from services.db import sessions
from services.users import ensure_user_rows


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class BankRobberyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    async def _resolve_lobby(self, session, guild_id: int, user_id: int) -> BankRobberyLobbyRow:
        lobby = await get_active_lobby_for_user(session, guild_id=guild_id, user_id=user_id)
        if lobby is None:
            raise ValueError("You are not in an active heist crew.")
        return lobby

    async def _apply_outcome(self, session, *, lobby: BankRobberyLobbyRow):
        if (lobby.state_json or {}).get("results_applied"):
            return await calculate_outcome(session, lobby)
        outcome = await calculate_outcome(session, lobby)
        members = await list_participants(session, lobby_id=lobby.id)
        for member in members:
            await ensure_user_rows(session, guild_id=lobby.guild_id, user_id=member.user_id)
            wallet = await get_or_create_wallet(session, guild_id=lobby.guild_id, user_id=member.user_id)
            payout = int(outcome.splits.get(int(member.user_id), 0))
            if payout > 0:
                wallet.silver += payout
                wallet.silver_earned += payout
            profile = await get_or_create_profile(session, guild_id=lobby.guild_id, user_id=member.user_id)
            profile.heist_rep += int(outcome.rep_gain)
            profile.personal_heat += int(outcome.heat_gain)
            profile.lifetime_bankrobbery_earnings += payout
            profile.highest_single_take = max(int(profile.highest_single_take), payout)
            if outcome.outcome.value == "clean_success":
                profile.clean_successes += 1
            elif outcome.outcome.value == "messy_success":
                profile.messy_successes += 1
            elif outcome.outcome.value == "partial_success":
                profile.partial_successes += 1
            elif outcome.outcome.value == "failed_escape":
                profile.failed_escapes += 1
            else:
                profile.full_failures += 1
            xp_map = outcome.role_xp.get(int(member.user_id), {})
            profile.leader_xp += int(xp_map.get("leader", 0))
            profile.hacker_xp += int(xp_map.get("hacker", 0))
            profile.driver_xp += int(xp_map.get("driver", 0))
            profile.enforcer_xp += int(xp_map.get("enforcer", 0))
            if int(outcome.bonus_rewards.get("diamonds", 0)) > 0:
                wallet.diamonds += int(outcome.bonus_rewards["diamonds"])
            if int(outcome.bonus_rewards.get("crowns", 0)) > 0:
                crowns = await get_or_create_crowns_wallet(session, guild_id=lobby.guild_id, user_id=member.user_id)
                crowns.crowns += int(outcome.bonus_rewards["crowns"])
            if int(outcome.bonus_rewards.get("lootboxes_rare", 0)) > 0:
                await grant_lootbox(session, guild_id=lobby.guild_id, user_id=member.user_id, rarity="rare", amount=int(outcome.bonus_rewards["lootboxes_rare"]))
            if int(outcome.bonus_rewards.get("lootboxes_epic", 0)) > 0:
                await grant_lootbox(session, guild_id=lobby.guild_id, user_id=member.user_id, rarity="epic", amount=int(outcome.bonus_rewards["lootboxes_epic"]))
        history = BankRobberyHistoryRow(
            lobby_id=lobby.id,
            guild_id=lobby.guild_id,
            leader_user_id=lobby.leader_user_id,
            robbery_id=lobby.robbery_id,
            approach=lobby.approach,
            outcome=outcome.outcome.value,
            gross_take=outcome.gross_take,
            secured_take=outcome.secured_take,
            final_take=outcome.final_take,
            heat_delta=outcome.heat_gain,
            rep_delta=outcome.rep_gain,
            rewards_json={"splits": outcome.splits, "bonus_rewards": outcome.bonus_rewards, "role_xp": outcome.role_xp},
            timeline_json={"timeline": list((lobby.state_json or {}).get("timeline", []))},
        )
        session.add(history)
        state = dict(lobby.state_json or {})
        state["results_applied"] = True
        lobby.state_json = state
        await finalize_lobby(session, lobby=lobby)
        return outcome

    async def _send_panel(self, interaction: discord.Interaction, *, embed: discord.Embed, ephemeral: bool = True):
        view = HeistHubView(self, interaction.user.id)
        validate_view_component_rows(view, context="heist hub panel")
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, view=view, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=ephemeral)

    async def _show_hub(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                profile = await get_or_create_profile(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                cooldowns = await get_cooldowns(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
        await self._send_panel(
            interaction,
            embed=build_hub_embed(profile=profile, cooldowns=cooldowns, guild_name=interaction.guild.name),
        )

    async def _show_lobby(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                template = get_template(lobby.robbery_id)
                if lobby.stage == "finale":
                    state = dict(lobby.state_json or {})
                    embed = build_finale_embed(lobby=lobby, template=template, state=state)
                else:
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    embed = build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows)
        await self._send_panel(interaction, embed=embed)

    async def _send_error(self, interaction: discord.Interaction, message: str):
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    @app_commands.command(name="heist", description="Open the all-in-one heist hub.")
    async def heist_cmd(self, interaction: discord.Interaction):
        await self._show_hub(interaction)

    async def handle_refresh_hub(self, interaction: discord.Interaction, *, owner_id: int):
        await interaction.response.defer(ephemeral=True)
        await self._show_hub(interaction)

    async def handle_lobby_status(self, interaction: discord.Interaction, *, owner_id: int):
        await interaction.response.defer(ephemeral=True)
        try:
            await self._show_lobby(interaction)
        except Exception as e:
            await self._send_error(interaction, str(e))

    async def handle_create_target(self, interaction: discord.Interaction, *, owner_id: int, robbery_id: str):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        approach = BankApproach.SILENT
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await create_lobby(session, guild_id=interaction.guild.id, leader_user_id=interaction.user.id, robbery_id=robbery_id, approach=approach)
                    await auto_configure_crew(session, lobby=lobby)
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    template = get_template(robbery_id)
            await self._send_panel(interaction, embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows))
        except Exception as e:
            await self._send_error(interaction, str(e))

    async def handle_set_approach(self, interaction: discord.Interaction, *, owner_id: int, approach: str):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    if interaction.user.id != lobby.leader_user_id:
                        raise ValueError("Only the leader can change the approach.")
                    lobby.approach = BankApproach(approach).value
                    state = dict(lobby.state_json or {})
                    state["approach"] = lobby.approach
                    lobby.state_json = state
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    template = get_template(lobby.robbery_id)
            await self._send_panel(interaction, embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows))
        except Exception as e:
            await self._send_error(interaction, str(e))

    async def handle_join_lobby(self, interaction: discord.Interaction, *, owner_id: int, leader_id: int):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await join_lobby(session, guild_id=interaction.guild.id, user_id=interaction.user.id, leader_user_id=leader_id)
                    await auto_configure_crew(session, lobby=lobby)
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    template = get_template(lobby.robbery_id)
            await self._send_panel(interaction, embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows))
        except Exception as e:
            await self._send_error(interaction, str(e))

    async def handle_complete_prep(self, interaction: discord.Interaction, *, owner_id: int, prep_key: str):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    await complete_prep(session, lobby=lobby, prep_key=prep_key, user_id=interaction.user.id)
                    prep_rows, prep_effects = await prep_summary(session, lobby)
                    template = get_template(lobby.robbery_id)
            await self._send_panel(interaction, embed=build_prep_embed(template=template, prep_rows=prep_rows, prep_effects=prep_effects))
        except Exception as e:
            await self._send_error(interaction, str(e))

    async def handle_toggle_ready(self, interaction: discord.Interaction, *, owner_id: int):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    participants = await list_participants(session, lobby_id=lobby.id)
                    current = next((member for member in participants if int(member.user_id) == int(interaction.user.id)), None)
                    if current is None:
                        raise ValueError("You are not in this crew.")
                    lobby = await set_ready(session, guild_id=interaction.guild.id, user_id=interaction.user.id, ready=not bool(current.ready))
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    template = get_template(lobby.robbery_id)
            await self._send_panel(interaction, embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows))
        except Exception as e:
            await self._send_error(interaction, str(e))

    async def handle_auto_setup(self, interaction: discord.Interaction, *, owner_id: int):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    if int(lobby.leader_user_id) != int(interaction.user.id):
                        raise ValueError("Only the leader can auto-configure the crew.")
                    await auto_configure_crew(session, lobby=lobby)
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    template = get_template(lobby.robbery_id)
            await self._send_panel(interaction, embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows))
        except Exception as e:
            await self._send_error(interaction, str(e))

    async def handle_leave_lobby(self, interaction: discord.Interaction, *, owner_id: int):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                lobby, disbanded = await leave_lobby(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
        if lobby is None:
            await self._send_error(interaction, "You are not in a heist crew.")
            return
        await self._send_error(interaction, "Crew disbanded." if disbanded else "You left the crew.")

    async def handle_launch_finale(self, interaction: discord.Interaction, *, owner_id: int):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    await auto_configure_crew(session, lobby=lobby)
                    await start_finale(session, lobby=lobby, actor_user_id=interaction.user.id)
                    await run_entry(session, lobby)
                    result = await run_vault(session, lobby)
                    template = get_template(lobby.robbery_id)
                    state = dict(lobby.state_json or {})
            await self._send_panel(interaction, embed=build_finale_embed(lobby=lobby, template=template, state=state, phase_result=result))
        except Exception as e:
            await self._send_error(interaction, str(e))

    async def handle_finale_action(self, interaction: discord.Interaction, *, owner_id: int, action: str):
        if interaction.guild is None:
            await self._send_error(interaction, "Server only.")
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    if lobby.stage != "finale":
                        raise ValueError("No active finale is running.")
                    template = get_template(lobby.robbery_id)
                    if action == "push":
                        result = await run_loot_round(session, lobby, push=True)
                    elif action == "leave":
                        await run_loot_round(session, lobby, push=False)
                        result = await run_escape(session, lobby)
                    elif action == "escape":
                        result = await run_escape(session, lobby)
                    elif action == "override":
                        used = await use_override(session, lobby)
                        if not used:
                            raise ValueError("Leader override was already used.")
                        state = dict(lobby.state_json or {})
                        await self._send_panel(interaction, embed=build_finale_embed(lobby=lobby, template=template, state=state))
                        return
                    else:
                        raise ValueError("Unknown finale action.")
                    if lobby.current_phase == FinalePhase.RESULTS.value:
                        outcome = await self._apply_outcome(session, lobby=lobby)
                        state = dict(lobby.state_json or {})
                        await self._send_panel(interaction, embed=build_results_embed(template=template, outcome_payload=outcome, state=state))
                        return
                    state = dict(lobby.state_json or {})
            await self._send_panel(interaction, embed=build_finale_embed(lobby=lobby, template=template, state=state, phase_result=result))
        except Exception as e:
            await self._send_error(interaction, str(e))


async def setup(bot: commands.Bot):
    await bot.add_cog(BankRobberyCog(bot))
