from __future__ import annotations

from datetime import timedelta

import discord
from discord import app_commands
from discord.ext import commands

from db.models import BankRobberyCooldownRow, BankRobberyLobbyRow
from services.bankrobbery.rewards import get_or_create_wallet
from services.db import sessions

from .balance import cooldown_seconds, payout_from_progress, split_even
from .catalog import TARGETS, TARGET_BY_KEY
from .embeds import active_run_embed, hub_embed, lobby_embed, onboarding_embed, results_embed, target_embed
from .matchmaking import auto_assign_roles, create_crew, join_crew, leave_crew, toggle_ready
from .onboarding import OnboardingView
from .repo import (
    clear_lobby_members,
    create_history,
    get_active_cooldowns,
    get_lobby_members,
    get_or_create_profile,
    get_or_create_user_state,
    get_user_lobby,
    list_open_lobbies,
)
from .runtime import advance_run, initialize_run_state
from .ui import ActiveRunView, HubView, LobbyView, ResultsView, TargetBrowserView
from .util import utc_now


class HeistCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    @app_commands.command(name="heist", description="Open the all-in-one heist screen.")
    async def heist(self, interaction: discord.Interaction):
        await self.route_heist(interaction)

    async def _respond(self, interaction: discord.Interaction, *, embed: discord.Embed, view: discord.ui.View | None = None, ephemeral: bool = False):
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, view=view, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=ephemeral)

    async def _err(self, interaction: discord.Interaction, message: str):
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    async def route_heist(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await self._err(interaction, "Use this in a server.")
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                user_state = await get_or_create_user_state(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                profile = await get_or_create_profile(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                lobby = await get_user_lobby(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                now = utc_now()
                cooldowns = await get_active_cooldowns(session, guild_id=interaction.guild.id, user_id=interaction.user.id, now=now)
                if not user_state.onboarding_completed:
                    await self._respond(interaction, embed=onboarding_embed(), view=OnboardingView(self), ephemeral=True)
                    return
                if lobby is not None and lobby.stage == "active":
                    lobby.state_json = advance_run(dict(lobby.state_json or {}))
                    run = (lobby.state_json or {}).get("run") or {}
                    if run.get("status") == "complete" and (lobby.state_json or {}).get("results") is None:
                        await self._finalize_results(session, lobby=lobby)
                        lobby.stage = "results"
                        lobby.status = "completed"
                    await session.flush()
                if lobby is not None and lobby.stage == "results":
                    await self._show_results(interaction, lobby=lobby)
                    return
                if lobby is not None and lobby.stage in {"lobby", "active"}:
                    await self._show_lobby_or_active(interaction, lobby=lobby)
                    return
                cd_text = ""
                if cooldowns:
                    cd_text = "\n".join(f"{c.robbery_id}: <t:{int(c.ends_at.timestamp())}:R>" for c in cooldowns[:3])
                await self._respond(
                    interaction,
                    embed=hub_embed(profile=profile, state_label="Not in a crew", next_move="Browse targets or join a forming crew.", cooldown_text=cd_text),
                    view=HubView(self),
                    ephemeral=True,
                )

    async def handle_onboarding_how(self, interaction: discord.Interaction):
        await interaction.response.send_message("How it works: join a crew (never solo), auto-roles are assigned, the run auto-resolves in 3-8 minutes, then you collect results.", ephemeral=True)

    async def handle_onboarding_complete(self, interaction: discord.Interaction):
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await get_or_create_user_state(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                row.onboarding_completed = True
                row.tutorial_seen_at = utc_now()
        await self.handle_open_targets(interaction)

    async def handle_onboarding_later(self, interaction: discord.Interaction):
        await interaction.response.send_message("No pressure. Re-run /heist when you're ready.", ephemeral=True)

    async def handle_open_targets(self, interaction: discord.Interaction):
        await self.handle_target_page(interaction, 0)

    async def handle_target_page(self, interaction: discord.Interaction, index: int):
        if interaction.guild is None:
            return
        i = index % len(TARGETS)
        async with self.sessionmaker() as session:
            async with session.begin():
                profile = await get_or_create_profile(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                wallet = await get_or_create_wallet(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                t = TARGETS[i]
                ok = int(profile.heist_rep) >= t.rep_req and int(wallet.silver) >= t.entry_cost
                reason = "Need more rep." if int(profile.heist_rep) < t.rep_req else "Need more Silver entry cost." if int(wallet.silver) < t.entry_cost else ""
        await self._respond(interaction, embed=target_embed(target=t, index=i, total=len(TARGETS), qualifies=ok, reason=reason), view=TargetBrowserView(self, i), ephemeral=True)

    async def handle_create_crew(self, interaction: discord.Interaction, index: int):
        if interaction.guild is None:
            return
        target = TARGETS[index % len(TARGETS)]
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    profile = await get_or_create_profile(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                    if int(profile.heist_rep) < target.rep_req:
                        raise ValueError(f"Need {target.rep_req:,} Rep.")
                    lobby = await create_crew(session, guild_id=interaction.guild.id, leader_id=interaction.user.id, target=target)
                    await auto_assign_roles(session, lobby=lobby)
            await self.route_heist(interaction)
        except Exception as e:
            await self._err(interaction, str(e))

    async def _show_lobby_or_active(self, interaction: discord.Interaction, *, lobby: BankRobberyLobbyRow):
        target = TARGET_BY_KEY.get(lobby.robbery_id)
        if target is None:
            await self._err(interaction, "Unknown target.")
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                live = await session.get(BankRobberyLobbyRow, lobby.id)
                members = await get_lobby_members(session, lobby_id=lobby.id)
                if live.stage == "active":
                    live.state_json = advance_run(dict(live.state_json or {}))
                    run = (live.state_json or {}).get("run") or {}
                    await self._respond(interaction, embed=active_run_embed(target=target, run=run, members=members), view=ActiveRunView(self), ephemeral=True)
                    return
                can_launch = len(members) >= target.min_crew and all(m.ready for m in members)
                await self._respond(
                    interaction,
                    embed=lobby_embed(target=target, members=members, leader_id=live.leader_user_id, can_launch=can_launch),
                    view=LobbyView(self, can_launch=can_launch, is_leader=int(live.leader_user_id) == int(interaction.user.id)),
                    ephemeral=True,
                )

    async def handle_join_public(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobbies = await list_open_lobbies(session, guild_id=interaction.guild.id)
                    lobby = next((l for l in lobbies if l.leader_user_id != interaction.user.id), None)
                    if lobby is None:
                        raise ValueError("No public forming crews right now.")
                    target = TARGET_BY_KEY[lobby.robbery_id]
                    await join_crew(session, guild_id=interaction.guild.id, user_id=interaction.user.id, lobby=lobby, target=target)
            await self.route_heist(interaction)
        except Exception as e:
            await self._err(interaction, str(e))

    async def handle_toggle_ready(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    await toggle_ready(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
            await self.route_heist(interaction)
        except Exception as e:
            await self._err(interaction, str(e))

    async def handle_leave(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                disbanded, _ = await leave_crew(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
        await self._err(interaction, "Crew disbanded." if disbanded else "You left the crew.")

    async def handle_launch(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await get_user_lobby(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                    if lobby is None:
                        raise ValueError("No active crew.")
                    if int(lobby.leader_user_id) != int(interaction.user.id):
                        raise ValueError("Only the leader can launch.")
                    target = TARGET_BY_KEY[lobby.robbery_id]
                    members = await get_lobby_members(session, lobby_id=lobby.id)
                    if len(members) < target.min_crew:
                        raise ValueError(f"Need at least {target.min_crew} crew members.")
                    if any(not m.ready for m in members):
                        raise ValueError("Everyone must ready up.")
                    roles = {int(m.user_id): m.role for m in members}
                    lobby.state_json = initialize_run_state(dict(lobby.state_json or {}), duration_sec=target.duration_sec, seed=int(lobby.rng_seed), roles=roles)
                    lobby.stage = "active"
                    lobby.status = "active"
            await self.route_heist(interaction)
        except Exception as e:
            await self._err(interaction, str(e))

    async def _finalize_results(self, session, *, lobby: BankRobberyLobbyRow):
        run = (lobby.state_json or {}).get("run") or {}
        target = TARGET_BY_KEY[lobby.robbery_id]
        members = await get_lobby_members(session, lobby_id=lobby.id)
        user_ids = [int(m.user_id) for m in members]
        gross = payout_from_progress(target, progress=int(run.get("progress", 0)), strikes=int(run.get("strikes", 0)), alarm=int(run.get("alarm", 0)))
        outcome = str(run.get("outcome", "busted"))
        if outcome == "busted":
            final_take = gross // 6
        elif outcome == "clean":
            final_take = int(gross * 1.1)
        elif outcome == "partial":
            final_take = int(gross * 0.6)
        else:
            final_take = gross
        splits = split_even(final_take, user_ids)
        rep_gain = {"clean": 42, "messy": 30, "partial": 18, "busted": 6}.get(outcome, 10)
        heat_gain = target.heat_add + max(0, int(run.get("alarm", 0)) // 18)
        history = run.get("history") or []
        state = dict(lobby.state_json or {})
        state["results"] = {
            "outcome": outcome,
            "gross": gross,
            "final_take": final_take,
            "splits": splits,
            "rep_gain": rep_gain,
            "heat_gain": heat_gain,
            "best": history[-1]["title"] if history else "",
            "worst": history[0]["title"] if history else "",
            "collected": False,
        }
        lobby.state_json = state

    async def _show_results(self, interaction: discord.Interaction, *, lobby: BankRobberyLobbyRow):
        target = TARGET_BY_KEY[lobby.robbery_id]
        r = (lobby.state_json or {}).get("results") or {}
        await self._respond(
            interaction,
            embed=results_embed(target=target, outcome=r.get("outcome", "busted"), gross=int(r.get("gross", 0)), final_take=int(r.get("final_take", 0)), splits={int(k): int(v) for k, v in (r.get("splits") or {}).items()}, rep_gain=int(r.get("rep_gain", 0)), heat_gain=int(r.get("heat_gain", 0)), best=str(r.get("best", "")), worst=str(r.get("worst", ""))),
            view=ResultsView(self),
            ephemeral=True,
        )

    async def handle_collect(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    lobby = await get_user_lobby(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                    if lobby is None or lobby.stage != "results":
                        raise ValueError("No payout waiting.")
                    results = dict((lobby.state_json or {}).get("results") or {})
                    if results.get("collected"):
                        raise ValueError("Payout already collected.")
                    members = await get_lobby_members(session, lobby_id=lobby.id)
                    for m in members:
                        uid = int(m.user_id)
                        payout = int((results.get("splits") or {}).get(str(uid), (results.get("splits") or {}).get(uid, 0)))
                        wallet = await get_or_create_wallet(session, guild_id=interaction.guild.id, user_id=uid)
                        wallet.silver += payout
                        wallet.silver_earned += payout
                        profile = await get_or_create_profile(session, guild_id=interaction.guild.id, user_id=uid)
                        profile.heist_rep += int(results.get("rep_gain", 0))
                        profile.personal_heat += int(results.get("heat_gain", 0))
                        profile.lifetime_bankrobbery_earnings += payout
                        profile.highest_single_take = max(int(profile.highest_single_take), payout)
                        if results.get("outcome") == "clean":
                            profile.clean_successes += 1
                        elif results.get("outcome") == "messy":
                            profile.messy_successes += 1
                        elif results.get("outcome") == "partial":
                            profile.partial_successes += 1
                        else:
                            profile.full_failures += 1
                        cd = BankRobberyCooldownRow(
                            guild_id=interaction.guild.id,
                            user_id=uid,
                            robbery_id=lobby.robbery_id,
                            ends_at=utc_now() + timedelta(seconds=cooldown_seconds(TARGET_BY_KEY[lobby.robbery_id], outcome=str(results.get("outcome", "busted")))),
                        )
                        session.add(cd)
                    await create_history(
                        session,
                        lobby_id=lobby.id,
                        guild_id=lobby.guild_id,
                        leader_user_id=lobby.leader_user_id,
                        robbery_id=lobby.robbery_id,
                        approach="auto",
                        outcome=str(results.get("outcome", "busted")),
                        gross_take=int(results.get("gross", 0)),
                        secured_take=int(results.get("final_take", 0)),
                        final_take=int(results.get("final_take", 0)),
                        heat_delta=int(results.get("heat_gain", 0)),
                        rep_delta=int(results.get("rep_gain", 0)),
                        rewards_json={"splits": results.get("splits", {})},
                        timeline_json={"timeline": ((lobby.state_json or {}).get("run") or {}).get("history", [])},
                    )
                    results["collected"] = True
                    s = dict(lobby.state_json or {})
                    s["results"] = results
                    lobby.state_json = s
                    lobby.status = "archived"
                    await clear_lobby_members(session, lobby_id=lobby.id)
            await self._err(interaction, "Payout collected. Crew closed.")
        except Exception as e:
            await self._err(interaction, str(e))


async def setup(bot: commands.Bot):
    await bot.add_cog(HeistCog(bot))
