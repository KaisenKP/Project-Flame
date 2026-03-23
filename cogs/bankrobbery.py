from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import BankRobberyHistoryRow, BankRobberyLobbyRow, BankRobberyParticipantRow
from services.db import sessions
from services.users import ensure_user_rows
from services.bankrobbery.catalog import BankApproach, CrewRole, FinalePhase, PREP_DEFS, TEMPLATES, get_template
from services.bankrobbery.finale import calculate_outcome, run_entry, run_escape, run_loot_round, run_vault, use_override
from services.bankrobbery.lobby import (
    assign_role,
    confirm_cuts,
    create_lobby,
    finalize_lobby,
    get_active_lobby_for_user,
    join_lobby,
    leave_lobby,
    list_participants,
    set_cuts,
    set_ready,
    start_finale,
)
from services.bankrobbery.prep import complete_prep, prep_summary
from services.bankrobbery.progression import get_cooldowns, get_or_create_profile
from services.bankrobbery.rewards import get_or_create_crowns_wallet, get_or_create_wallet, grant_lootbox
from services.bankrobbery.ui import (
    build_board_embed,
    build_finale_embed,
    build_hub_embed,
    build_lobby_embed,
    build_prep_embed,
    build_results_embed,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class BankRobberyCog(commands.Cog):
    bankrobbery = app_commands.Group(name="bankrobbery", description="Premium heist-style robbery mode.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    async def _resolve_lobby(self, session, guild_id: int, user_id: int) -> BankRobberyLobbyRow:
        lobby = await get_active_lobby_for_user(session, guild_id=guild_id, user_id=user_id)
        if lobby is None:
            raise ValueError("You are not in an active Bank Robbery lobby.")
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

    @bankrobbery.command(name="hub", description="Open the Bank Robbery hub.")
    async def hub_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                profile = await get_or_create_profile(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                cooldowns = await get_cooldowns(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
        await interaction.followup.send(embed=build_hub_embed(profile=profile, cooldowns=cooldowns, guild_name=interaction.guild.name), ephemeral=True)

    @bankrobbery.command(name="board", description="View the robbery board.")
    async def board_cmd(self, interaction: discord.Interaction):
        await interaction.response.send_message(embed=build_board_embed(), ephemeral=True)

    @bankrobbery.command(name="create", description="Create a robbery lobby.")
    @app_commands.describe(robbery_id="Target id", approach="Approach to use")
    @app_commands.choices(
        robbery_id=[app_commands.Choice(name=t.display_name, value=k) for k, t in TEMPLATES.items()],
        approach=[app_commands.Choice(name=a.value.title(), value=a.value) for a in BankApproach],
    )
    async def create_cmd(self, interaction: discord.Interaction, robbery_id: str, approach: str):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await create_lobby(session, guild_id=interaction.guild.id, leader_user_id=interaction.user.id, robbery_id=robbery_id, approach=BankApproach(approach))
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    template = get_template(robbery_id)
            await interaction.followup.send(embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(str(e), ephemeral=True)

    @bankrobbery.command(name="join", description="Join a leader's robbery lobby.")
    @app_commands.describe(leader="Leader whose lobby you want to join")
    async def join_cmd(self, interaction: discord.Interaction, leader: discord.Member):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await join_lobby(session, guild_id=interaction.guild.id, user_id=interaction.user.id, leader_user_id=leader.id)
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    template = get_template(lobby.robbery_id)
            await interaction.followup.send(embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(str(e), ephemeral=True)

    @bankrobbery.command(name="leave", description="Leave your current robbery lobby.")
    async def leave_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                lobby, disbanded = await leave_lobby(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
        if lobby is None:
            await interaction.followup.send("You are not in a robbery lobby.", ephemeral=True)
            return
        await interaction.followup.send("Lobby disbanded." if disbanded else "You left the robbery crew.", ephemeral=True)

    @bankrobbery.command(name="roles", description="Assign or review robbery roles.")
    @app_commands.describe(member="Crew member", role="Role to assign")
    @app_commands.choices(role=[app_commands.Choice(name=r.value.title(), value=r.value) for r in CrewRole if r != CrewRole.FLEX])
    async def roles_cmd(self, interaction: discord.Interaction, member: Optional[discord.Member] = None, role: Optional[str] = None):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    if member is not None and role is not None:
                        await assign_role(session, lobby=lobby, actor_user_id=interaction.user.id, target_user_id=member.id, role=CrewRole(role))
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    template = get_template(lobby.robbery_id)
            await interaction.followup.send(embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(str(e), ephemeral=True)

    @bankrobbery.command(name="cuts", description="Set, confirm, or review cut splits.")
    @app_commands.describe(split="Format: @user=40,@user=30,@user=30")
    async def cuts_cmd(self, interaction: discord.Interaction, split: Optional[str] = None):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    if split:
                        parts = [part.strip() for part in split.split(",") if part.strip()]
                        cuts: dict[int, int] = {}
                        for part in parts:
                            left, right = [s.strip() for s in part.split("=", 1)]
                            user_id = int(left.replace("<@", "").replace(">", "").replace("!", ""))
                            cuts[user_id] = int(right)
                        await set_cuts(session, lobby=lobby, actor_user_id=interaction.user.id, cuts=cuts)
                    else:
                        lobby = await confirm_cuts(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
                    template = get_template(lobby.robbery_id)
            await interaction.followup.send(embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(str(e), ephemeral=True)

    @bankrobbery.command(name="prep", description="Complete or review robbery prep jobs.")
    @app_commands.describe(prep_key="Prep key to complete")
    @app_commands.choices(prep_key=[app_commands.Choice(name=v.name, value=k) for k, v in PREP_DEFS.items()])
    async def prep_cmd(self, interaction: discord.Interaction, prep_key: Optional[str] = None):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    if prep_key:
                        await complete_prep(session, lobby=lobby, prep_key=prep_key, user_id=interaction.user.id)
                    prep_rows, prep_effects = await prep_summary(session, lobby)
                    template = get_template(lobby.robbery_id)
            await interaction.followup.send(embed=build_prep_embed(template=template, prep_rows=prep_rows, prep_effects=prep_effects), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(str(e), ephemeral=True)

    @bankrobbery.command(name="start", description="Launch the robbery finale.")
    async def start_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    await start_finale(session, lobby=lobby, actor_user_id=interaction.user.id)
                    result = await run_entry(session, lobby)
                    result2 = await run_vault(session, lobby)
                    template = get_template(lobby.robbery_id)
                    state = dict(lobby.state_json or {})
            await interaction.followup.send(embed=build_finale_embed(lobby=lobby, template=template, state=state, phase_result=result2), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(str(e), ephemeral=True)

    @bankrobbery.command(name="status", description="View your current robbery status or set ready state.")
    @app_commands.describe(ready="Optional ready toggle for your current lobby")
    async def status_cmd(self, interaction: discord.Interaction, ready: Optional[bool] = None):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await self._resolve_lobby(session, interaction.guild.id, interaction.user.id)
                    if ready is not None:
                        lobby = await set_ready(session, guild_id=interaction.guild.id, user_id=interaction.user.id, ready=ready)
                    template = get_template(lobby.robbery_id)
                    if lobby.stage == "finale":
                        state = dict(lobby.state_json or {})
                        await interaction.followup.send(embed=build_finale_embed(lobby=lobby, template=template, state=state), ephemeral=True)
                        return
                    participants = await list_participants(session, lobby_id=lobby.id)
                    prep_rows, _ = await prep_summary(session, lobby)
            await interaction.followup.send(embed=build_lobby_embed(lobby=lobby, template=template, participants=participants, prep_rows=prep_rows), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(str(e), ephemeral=True)

    @bankrobbery.command(name="loadout", description="Review your role XP loadout bonuses.")
    async def loadout_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                profile = await get_or_create_profile(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
        embed = discord.Embed(title="🛠️ Bank Robbery Loadout", color=discord.Color.blurple())
        embed.add_field(name="Leader XP", value=f"**{profile.leader_xp:,}** — stronger Override and cleaner team stability.", inline=False)
        embed.add_field(name="Hacker XP", value=f"**{profile.hacker_xp:,}** — faster vault access and security delay.", inline=False)
        embed.add_field(name="Driver XP", value=f"**{profile.driver_xp:,}** — less loot loss during escape.", inline=False)
        embed.add_field(name="Enforcer XP", value=f"**{profile.enforcer_xp:,}** — lower penalty severity during bad events.", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @bankrobbery.command(name="cooldowns", description="View your robbery cooldowns.")
    async def cooldowns_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                rows = await get_cooldowns(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
        if not rows:
            await interaction.followup.send("No active Bank Robbery cooldowns.", ephemeral=True)
            return
        embed = discord.Embed(title="⏳ Bank Robbery Cooldowns", color=discord.Color.orange())
        for row in rows:
            if row.ends_at <= utc_now():
                continue
            embed.add_field(name=get_template(row.robbery_id).display_name, value=f"Ready at **{row.ends_at.strftime('%Y-%m-%d %H:%M UTC')}**", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @bankrobbery.command(name="history", description="View your recent Bank Robbery history.")
    async def history_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                rows = list((await session.execute(select(BankRobberyHistoryRow).where(BankRobberyHistoryRow.guild_id == interaction.guild.id, BankRobberyHistoryRow.leader_user_id == interaction.user.id).order_by(BankRobberyHistoryRow.created_at.desc()).limit(5))).scalars())
        embed = discord.Embed(title="📜 Bank Robbery History", color=discord.Color.dark_teal())
        if not rows:
            embed.description = "No completed scores yet."
        else:
            for row in rows:
                embed.add_field(name=f"{get_template(row.robbery_id).display_name} • {row.outcome.replace('_', ' ').title()}", value=f"Approach: **{row.approach.title()}**\nFinal Take: **{row.final_take:,} Silver**\nHeat: **+{row.heat_delta}** • Rep: **+{row.rep_delta}**", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def _finale_step(self, interaction: discord.Interaction, action: str):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
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
                        result = await run_loot_round(session, lobby, push=False)
                        result = await run_escape(session, lobby)
                    elif action == "escape":
                        result = await run_escape(session, lobby)
                    elif action == "override":
                        used = await use_override(session, lobby)
                        if not used:
                            raise ValueError("Leader Override has already been used.")
                        state = dict(lobby.state_json or {})
                        await interaction.followup.send(embed=build_finale_embed(lobby=lobby, template=template, state=state), ephemeral=True)
                        return
                    else:
                        raise ValueError("Unknown finale action.")
                    if lobby.current_phase == FinalePhase.RESULTS.value:
                        outcome = await self._apply_outcome(session, lobby=lobby)
                        state = dict(lobby.state_json or {})
                        await interaction.followup.send(embed=build_results_embed(template=template, outcome_payload=outcome, state=state), ephemeral=True)
                        return
                    state = dict(lobby.state_json or {})
            await interaction.followup.send(embed=build_finale_embed(lobby=lobby, template=template, state=state, phase_result=result), ephemeral=True)
        except Exception as e:
            await interaction.followup.send(str(e), ephemeral=True)

    @bankrobbery.command(name="advance", description="Advance the finale by pushing loot, leaving, escaping, or using Override.")
    @app_commands.describe(action="push, leave, escape, or override")
    @app_commands.choices(action=[
        app_commands.Choice(name="Push One More Loot Round", value="push"),
        app_commands.Choice(name="Leave Now", value="leave"),
        app_commands.Choice(name="Force Escape", value="escape"),
        app_commands.Choice(name="Leader Override", value="override"),
    ])
    async def advance_cmd(self, interaction: discord.Interaction, action: str):
        await self._finale_step(interaction, action)


async def setup(bot: commands.Bot):
    await bot.add_cog(BankRobberyCog(bot))
