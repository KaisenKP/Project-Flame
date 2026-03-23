from __future__ import annotations

import discord

from .catalog import PREP_DEFS, TEMPLATES, BankApproach, FinaleOutcome, get_template


def fmt_int(n: int) -> str:
    return f"{int(n):,}"


def fmt_seconds(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    mins, _ = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {mins}m"
    return f"{mins}m"


def build_hub_embed(*, profile, cooldowns: list, guild_name: str) -> discord.Embed:
    embed = discord.Embed(
        title="🏦 Bank Robbery Hub",
        description="Premium multiplayer scores with prep, greed pressure, heat, and major Silver swings.",
        color=discord.Color.dark_teal(),
    )
    embed.add_field(name="Your Profile", value=f"Heist Rep: **{fmt_int(profile.heist_rep)}**\nHeat: **{fmt_int(profile.personal_heat)}**\nLifetime Take: **{fmt_int(profile.lifetime_bankrobbery_earnings)} Silver**", inline=False)
    cd_map = {row.robbery_id: row for row in cooldowns}
    for template in TEMPLATES.values():
        lock = "✅ Ready"
        if template.robbery_id in cd_map:
            lock = f"⏳ {cd_map[template.robbery_id].ends_at.strftime('%Y-%m-%d %H:%M UTC')}"
        embed.add_field(
            name=f"{template.display_name} • {template.tier.value.title()}",
            value=(
                f"Crew **{template.crew_min}-{template.crew_max}** • Entry **{fmt_int(template.entry_cost)} Silver**\n"
                f"Payout **{fmt_int(template.payout_min)}-{fmt_int(template.payout_max)}**\n"
                f"Rep **{fmt_int(template.recommended_rep)}** • Heat **+{template.heat_gain}** • Cooldown **{fmt_seconds(template.cooldown_seconds)}**\n"
                f"Approaches: **{', '.join(a.value.title() for a in template.available_approaches)}**\n"
                f"Status: {lock}"
            ),
            inline=False,
        )
    embed.set_footer(text=f"{guild_name} • Use /bankrobbery board or /bankrobbery create")
    return embed


def build_board_embed() -> discord.Embed:
    embed = discord.Embed(title="🎯 Robbery Board", color=discord.Color.blurple())
    for template in TEMPLATES.values():
        embed.add_field(
            name=template.display_name,
            value=(
                f"Tier: **{template.tier.value.title()}**\nCrew: **{template.crew_min}-{template.crew_max}**\n"
                f"Entry Cost: **{fmt_int(template.entry_cost)} Silver**\nPayout: **{fmt_int(template.payout_min)}-{fmt_int(template.payout_max)} Silver**\n"
                f"Rep: **{fmt_int(template.recommended_rep)}** • Heat: **+{template.heat_gain}** • Cooldown: **{fmt_seconds(template.cooldown_seconds)}**"
            ),
            inline=False,
        )
    return embed


def build_lobby_embed(*, lobby, template, participants, prep_rows) -> discord.Embed:
    embed = discord.Embed(title=f"🚨 Crew Lobby • {template.display_name}", description=template.description, color=discord.Color.orange())
    embed.add_field(name="Target", value=f"Approach: **{lobby.approach.title()}**\nStage: **{lobby.stage.title()}**\nEntry: **{fmt_int(template.entry_cost)} Silver**", inline=False)
    crew_lines = []
    for member in participants:
        crew_lines.append(f"<@{member.user_id}> • **{member.role.title()}** • Cut **{member.cut_percent}%** • {'✅' if member.ready else '❌'} Ready")
    embed.add_field(name="Crew", value="\n".join(crew_lines) if crew_lines else "No crew yet.", inline=False)
    prep_lines = []
    for row in prep_rows:
        definition = PREP_DEFS[row.prep_key]
        who = f" by <@{row.completed_by_user_id}>" if row.completed_by_user_id else ""
        prep_lines.append(f"{'✅' if row.completed else '⬜'} **{definition.name}** — {definition.bonus_text}{who}")
    embed.add_field(name="Prep Checklist", value="\n".join(prep_lines), inline=False)
    embed.set_footer(text="Leader controls approach, roles, cuts, and launch.")
    return embed


def build_prep_embed(*, template, prep_rows, prep_effects: dict[str, int]) -> discord.Embed:
    embed = discord.Embed(title=f"🧰 Prep Board • {template.display_name}", color=discord.Color.dark_gold())
    lines = []
    for row in prep_rows:
        definition = PREP_DEFS[row.prep_key]
        owner = f" by <@{row.completed_by_user_id}>" if row.completed_by_user_id else ""
        lines.append(f"{'✅' if row.completed else '⬜'} **{definition.name}** — {definition.description}\n↳ {definition.bonus_text}{owner}")
    embed.add_field(name="Prep Jobs", value="\n".join(lines), inline=False)
    active = [f"**{k}**: {v:+,}" for k, v in prep_effects.items()]
    embed.add_field(name="Finale Modifiers", value="\n".join(active) if active else "No prep bonuses active.", inline=False)
    return embed


def build_finale_embed(*, lobby, template, state: dict, phase_result=None) -> discord.Embed:
    embed = discord.Embed(title=f"💥 Finale • {template.display_name}", color=discord.Color.red())
    embed.add_field(name="Run State", value=f"Phase: **{lobby.current_phase.title()}**\nAlert: **{state.get('alert', 0)} / 100**\nSecured Loot: **{fmt_int(state.get('secured_cash', 0))} Silver**\nLoot Rounds: **{state.get('loot_round', 0)}**", inline=False)
    embed.add_field(name="Active Modifiers", value="\n".join(state.get("active_modifiers", []) or ["No active temporary modifiers."]), inline=False)
    if phase_result is not None:
        embed.add_field(name=phase_result.title, value=f"{phase_result.description}\nEvent: **{phase_result.event_name or 'None'}**", inline=False)
    timeline = state.get("timeline", [])[-4:]
    embed.add_field(name="Crew Feed", value="\n".join(f"• {item.get('text', '')}" for item in timeline) if timeline else "Run just started.", inline=False)
    return embed


def build_results_embed(*, template, outcome_payload, state: dict) -> discord.Embed:
    names = {
        FinaleOutcome.CLEAN_SUCCESS: "Clean Success",
        FinaleOutcome.MESSY_SUCCESS: "Messy Success",
        FinaleOutcome.PARTIAL_SUCCESS: "Partial Success",
        FinaleOutcome.FAILED_ESCAPE: "Failed Escape",
        FinaleOutcome.FULL_FAILURE: "Full Failure",
    }
    embed = discord.Embed(title=f"🏁 Results • {template.display_name}", description=f"Outcome: **{names[outcome_payload.outcome]}**", color=discord.Color.green() if outcome_payload.final_take > 0 else discord.Color.dark_red())
    embed.add_field(name="Take", value=f"Gross: **{fmt_int(outcome_payload.gross_take)}**\nSecured: **{fmt_int(outcome_payload.secured_take)}**\nFinal: **{fmt_int(outcome_payload.final_take)} Silver**", inline=False)
    splits = "\n".join(f"<@{uid}> • **{fmt_int(amount)} Silver**" for uid, amount in outcome_payload.splits.items())
    embed.add_field(name="Final Split", value=splits or "No payout.", inline=False)
    bonuses = [f"{k.replace('_', ' ').title()}: **{v}**" for k, v in outcome_payload.bonus_rewards.items()]
    embed.add_field(name="Bonuses + Penalties", value=("\n".join(bonuses) if bonuses else "No rare bonus rewards this run.") + f"\nRep Gain: **+{outcome_payload.rep_gain}**\nHeat Gain: **+{outcome_payload.heat_gain}**", inline=False)
    xp_lines = []
    for uid, mapping in outcome_payload.role_xp.items():
        for role, xp in mapping.items():
            xp_lines.append(f"<@{uid}> • **{role.title()} XP +{xp}**")
    embed.add_field(name="Role XP", value="\n".join(xp_lines), inline=False)
    return embed


class LobbyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=600)


class FinaleView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=600)
