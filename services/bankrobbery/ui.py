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
        title="💎 Heist Hub",
        description=(
            "Plan the run, manage your crew, and push the finale from one control panel.\n"
            "Use the buttons below to create, join, prep, launch, and cash out."
        ),
        color=discord.Color.from_rgb(24, 164, 166),
    )
    embed.add_field(
        name="👤 Operator Profile",
        value=(
            f"**Rep** · {fmt_int(profile.heist_rep)}\n"
            f"**Heat** · {fmt_int(profile.personal_heat)}\n"
            f"**Lifetime Take** · {fmt_int(profile.lifetime_bankrobbery_earnings)} Silver"
        ),
        inline=False,
    )
    cd_map = {row.robbery_id: row for row in cooldowns}
    target_cards: list[str] = []
    for template in TEMPLATES.values():
        lock = "✅ Ready"
        if template.robbery_id in cd_map:
            lock = f"⏳ {cd_map[template.robbery_id].ends_at.strftime('%Y-%m-%d %H:%M UTC')}"
        target_cards.append(
            f"**{template.display_name}** · {template.tier.value.title()}\n"
            f"↳ Crew {template.crew_min}-{template.crew_max} • Entry {fmt_int(template.entry_cost)}\n"
            f"↳ Payout {fmt_int(template.payout_min)}-{fmt_int(template.payout_max)} • Heat +{template.heat_gain}\n"
            f"↳ Status {lock}"
        )
    embed.add_field(name="🎯 Active Targets", value="\n\n".join(target_cards), inline=False)
    embed.add_field(
        name="🕹️ Quick Flow",
        value=(
            "1. **Create** or **Join** a crew.\n"
            "2. Tap **Auto Setup** to assign roles and fair cuts.\n"
            "3. Clear prep with the prep selector.\n"
            "4. Hit **Launch Finale** and run it with the action buttons."
        ),
        inline=False,
    )
    embed.set_footer(text=f"{guild_name} • Centralized heist controls")
    return embed


def build_board_embed() -> discord.Embed:
    embed = discord.Embed(
        title="🧭 Heist Target Board",
        description="Every available score, tuned for a cleaner at-a-glance planning board.",
        color=discord.Color.blurple(),
    )
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
    embed = discord.Embed(
        title=f"🚨 Crew Lobby • {template.display_name}",
        description=template.description,
        color=discord.Color.from_rgb(255, 170, 64),
    )
    embed.add_field(
        name="🎯 Plan",
        value=(
            f"**Approach** · {lobby.approach.title()}\n"
            f"**Stage** · {lobby.stage.title()}\n"
            f"**Entry** · {fmt_int(template.entry_cost)} Silver"
        ),
        inline=False,
    )
    crew_lines = []
    for member in participants:
        crew_lines.append(
            f"<@{member.user_id}> · **{member.role.title()}** · "
            f"Cut **{member.cut_percent}%** · {'✅ Ready' if member.ready else '❌ Waiting'}"
        )
    embed.add_field(name="👥 Crew", value="\n".join(crew_lines) if crew_lines else "No crew yet.", inline=False)
    prep_lines = []
    for row in prep_rows:
        definition = PREP_DEFS[row.prep_key]
        who = f" by <@{row.completed_by_user_id}>" if row.completed_by_user_id else ""
        prep_lines.append(f"{'✅' if row.completed else '⬜'} **{definition.name}** — {definition.bonus_text}{who}")
    embed.add_field(name="🧰 Prep Checklist", value="\n".join(prep_lines), inline=False)
    embed.set_footer(text="Run everything from the hub buttons below.")
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
    embed.add_field(
        name="📡 Run State",
        value=(
            f"**Phase** · {lobby.current_phase.title()}\n"
            f"**Alert** · {state.get('alert', 0)} / 100\n"
            f"**Secured Loot** · {fmt_int(state.get('secured_cash', 0))} Silver\n"
            f"**Loot Rounds** · {state.get('loot_round', 0)}"
        ),
        inline=False,
    )
    embed.add_field(name="⚙️ Active Modifiers", value="\n".join(state.get("active_modifiers", []) or ["No active temporary modifiers."]), inline=False)
    if phase_result is not None:
        embed.add_field(name=phase_result.title, value=f"{phase_result.description}\nEvent: **{phase_result.event_name or 'None'}**", inline=False)
    timeline = state.get("timeline", [])[-4:]
    embed.add_field(name="📰 Crew Feed", value="\n".join(f"• {item.get('text', '')}" for item in timeline) if timeline else "Run just started.", inline=False)
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


class HeistTargetSelect(discord.ui.Select):
    def __init__(self, cog, owner_id: int):
        self.cog = cog
        self.owner_id = owner_id
        options = [
            discord.SelectOption(
                label=template.display_name[:100],
                value=template.robbery_id,
                description=f"{template.tier.value.title()} • Crew {template.crew_min}-{template.crew_max}",
                emoji="🎯",
            )
            for template in TEMPLATES.values()
        ]
        super().__init__(placeholder="Choose a target to create a crew", min_values=1, max_values=1, options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        await self.cog.handle_create_target(interaction, owner_id=self.owner_id, robbery_id=self.values[0])


class HeistApproachSelect(discord.ui.Select):
    def __init__(self, cog, owner_id: int):
        self.cog = cog
        self.owner_id = owner_id
        options = [
            discord.SelectOption(label=approach.value.title(), value=approach.value, emoji="🛠️")
            for approach in BankApproach
        ]
        super().__init__(placeholder="Choose your approach", min_values=1, max_values=1, options=options, row=1)

    async def callback(self, interaction: discord.Interaction):
        await self.cog.handle_set_approach(interaction, owner_id=self.owner_id, approach=self.values[0])


class HeistPrepSelect(discord.ui.Select):
    def __init__(self, cog, owner_id: int):
        self.cog = cog
        self.owner_id = owner_id
        options = [
            discord.SelectOption(label=definition.name[:100], value=key, description=definition.bonus_text[:100], emoji="🧰")
            for key, definition in PREP_DEFS.items()
        ]
        super().__init__(placeholder="Mark a prep objective complete", min_values=1, max_values=1, options=options, row=2)

    async def callback(self, interaction: discord.Interaction):
        await self.cog.handle_complete_prep(interaction, owner_id=self.owner_id, prep_key=self.values[0])


class HeistJoinLeaderSelect(discord.ui.UserSelect):
    def __init__(self, cog, owner_id: int):
        self.cog = cog
        self.owner_id = owner_id
        super().__init__(placeholder="Choose a leader to join", min_values=1, max_values=1, row=3)

    async def callback(self, interaction: discord.Interaction):
        selected = self.values[0]
        await self.cog.handle_join_lobby(interaction, owner_id=self.owner_id, leader_id=selected.id)


class HeistHubView(discord.ui.View):
    def __init__(self, cog, owner_id: int):
        super().__init__(timeout=600)
        self.cog = cog
        self.owner_id = owner_id
        self.add_item(HeistTargetSelect(cog, owner_id))
        self.add_item(HeistApproachSelect(cog, owner_id))
        self.add_item(HeistPrepSelect(cog, owner_id))
        self.add_item(HeistJoinLeaderSelect(cog, owner_id))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("This heist hub belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Refresh Hub", style=discord.ButtonStyle.secondary, emoji="🔄", row=4)
    async def refresh_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_refresh_hub(interaction, owner_id=self.owner_id)

    @discord.ui.button(label="Lobby Status", style=discord.ButtonStyle.secondary, emoji="📋", row=4)
    async def status_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_lobby_status(interaction, owner_id=self.owner_id)

    @discord.ui.button(label="Auto Setup", style=discord.ButtonStyle.primary, emoji="⚙️", row=4)
    async def auto_setup_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_auto_setup(interaction, owner_id=self.owner_id)

    @discord.ui.button(label="Ready Toggle", style=discord.ButtonStyle.success, emoji="✅", row=4)
    async def ready_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_toggle_ready(interaction, owner_id=self.owner_id)

    @discord.ui.button(label="Leave Crew", style=discord.ButtonStyle.danger, emoji="🚪", row=4)
    async def leave_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_leave_lobby(interaction, owner_id=self.owner_id)

    @discord.ui.button(label="Launch Finale", style=discord.ButtonStyle.success, emoji="🚀", row=5)
    async def launch_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_launch_finale(interaction, owner_id=self.owner_id)

    @discord.ui.button(label="Push Loot", style=discord.ButtonStyle.primary, emoji="💰", row=5)
    async def push_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_finale_action(interaction, owner_id=self.owner_id, action="push")

    @discord.ui.button(label="Leave Now", style=discord.ButtonStyle.secondary, emoji="📦", row=5)
    async def leave_now_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_finale_action(interaction, owner_id=self.owner_id, action="leave")

    @discord.ui.button(label="Escape", style=discord.ButtonStyle.danger, emoji="🏃", row=5)
    async def escape_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_finale_action(interaction, owner_id=self.owner_id, action="escape")

    @discord.ui.button(label="Override", style=discord.ButtonStyle.primary, emoji="🧠", row=5)
    async def override_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_finale_action(interaction, owner_id=self.owner_id, action="override")
