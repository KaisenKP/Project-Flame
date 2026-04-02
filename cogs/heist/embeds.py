from __future__ import annotations

import discord

from .catalog import HeistTarget
from .domain import crew_quality
from .util import fmt_int, pct


def onboarding_embed() -> discord.Embed:
    return discord.Embed(
        title="🕶️ Heists",
        description="Heists are team-based high-payout runs that auto-play through random chaos. Build a crew, launch, and check back for the madness.",
        color=discord.Color.blurple(),
    )


def hub_embed(*, profile, state_label: str, next_move: str, cooldown_text: str) -> discord.Embed:
    e = discord.Embed(title="💎 Heist Hub", color=discord.Color.teal())
    e.description = "Heists are co-op jackpot runs that auto-play through random situations. Build a crew, launch, and check back as the chaos unfolds."
    e.add_field(name="Your Status", value=state_label, inline=False)
    e.add_field(name="Your Next Move", value=next_move, inline=False)
    e.add_field(
        name="Profile",
        value=(
            f"Rep: **{fmt_int(profile.heist_rep)}**\n"
            f"Heat: **{fmt_int(profile.personal_heat)}**\n"
            f"Lifetime Heist Earnings: **{fmt_int(profile.lifetime_bankrobbery_earnings)}**\n"
            f"Heists Cleared: **{int(profile.clean_successes)+int(profile.messy_successes)}**\n"
            f"Biggest Score: **{fmt_int(profile.highest_single_take)}**"
        ),
        inline=False,
    )
    if cooldown_text:
        e.add_field(name="Cooldown", value=cooldown_text, inline=False)
    return e


def target_embed(*, target: HeistTarget, index: int, total: int, qualifies: bool, reason: str = "") -> discord.Embed:
    e = discord.Embed(title=f"🎯 {target.name}", color=discord.Color.orange())
    e.description = target.identity
    e.add_field(name="Difficulty", value=target.difficulty)
    e.add_field(name="Crew", value=f"Min {target.min_crew} • Rec {target.rec_crew}")
    e.add_field(name="Duration", value=f"~{target.duration_sec//60}m")
    e.add_field(name="Entry", value=f"{fmt_int(target.entry_cost)} Silver")
    e.add_field(name="Payout", value=f"{fmt_int(target.payout_min)} - {fmt_int(target.payout_max)}")
    e.add_field(name="Risk", value=target.risk)
    e.add_field(name="Unlock", value=f"Rep {fmt_int(target.rep_req)}")
    e.add_field(name="Status", value="✅ You qualify" if qualifies else f"🔒 {reason}", inline=False)
    e.set_footer(text=f"Target {index+1}/{total}")
    return e


def lobby_embed(*, target: HeistTarget, members: list, leader_id: int, can_launch: bool) -> discord.Embed:
    roles = [m.role for m in members]
    quality = crew_quality(roles)
    lines = [f"<@{m.user_id}> → **{m.role.title()}** • {'✅ Ready' if m.ready else '⏳ Not ready'}" for m in members]
    e = discord.Embed(title=f"👥 Crew Lobby • {target.name}", description=target.identity, color=discord.Color.gold())
    e.add_field(name="Job", value=f"{target.difficulty} • Risk {target.risk} • Est {target.duration_sec//60}m", inline=False)
    e.add_field(name="Crew", value="\n".join(lines) if lines else "No crew", inline=False)
    e.add_field(name="Coverage", value=f"{quality.title()} • {len(members)}/{target.rec_crew} players")
    e.add_field(name="Launch", value="✅ Ready to launch" if can_launch else f"Need {target.min_crew}+ all ready")
    if int(leader_id):
        e.set_footer(text=f"Leader: {leader_id}")
    return e


def active_run_embed(*, target: HeistTarget, run: dict, members: list) -> discord.Embed:
    last = (run.get("history") or [{}])[-1]
    e = discord.Embed(title=f"🚨 Active Heist • {target.name}", color=discord.Color.red())
    e.add_field(name="Progress", value=pct(int(run.get("progress", 0)), 100))
    e.add_field(name="Alarm", value=pct(int(run.get("alarm", 0)), 100))
    e.add_field(name="Strikes", value=f"{int(run.get('strikes', 0))}/3")
    e.add_field(name="Condition", value=str(run.get("condition", "clean")).title(), inline=False)
    e.add_field(name="Crew", value="\n".join(f"<@{m.user_id}> → {m.role.title()}" for m in members), inline=False)
    e.add_field(name="Latest", value=f"**{last.get('title','Warmup')}**\n{last.get('body','Run is spinning up.')}\nΔP {last.get('d_progress',0):+} • ΔA {last.get('d_alarm',0):+}", inline=False)
    return e


def results_embed(*, target: HeistTarget, outcome: str, gross: int, final_take: int, splits: dict[int, int], rep_gain: int, heat_gain: int, best: str, worst: str) -> discord.Embed:
    e = discord.Embed(title=f"🏁 Heist Results • {target.name}", description=f"Outcome: **{outcome.title()}**", color=discord.Color.green() if final_take > 0 else discord.Color.dark_red())
    e.add_field(name="Payout", value=f"Gross: **{fmt_int(gross)}**\nFinal: **{fmt_int(final_take)}**")
    e.add_field(name="Crew Splits", value="\n".join(f"<@{u}> • {fmt_int(v)}" for u, v in splits.items()) or "No payout", inline=False)
    e.add_field(name="Rep / Heat", value=f"+{rep_gain} Rep • +{heat_gain} Heat", inline=False)
    e.add_field(name="Biggest Save", value=best or "Nobody panicked. Miracles happened.")
    e.add_field(name="Worst Mistake", value=worst or "Greed with a side of bad timing.")
    return e
