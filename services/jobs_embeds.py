from __future__ import annotations

from typing import Optional

import discord

from services.job_hub import (
    SlotSnapshot,
    income_range_for,
    perk_defs_for,
    prestige_preview,
    slot_label,
    stamina_cost_preview,
    tool_bonus_snapshot,
    tool_defs_for,
    unlocked_perks,
)
from services.jobs_core import JOB_DEFS, JOB_SWITCH_COST, JobCategory, category_fail_bp, fmt_int, unlock_level_for
from services.jobs_endgame import presentation_for

VIP_WORK_COOLDOWN_SECONDS = 10
EMBED_FIELD_VALUE_LIMIT = 1024


def _join_lines_with_limit(lines: list[str], *, max_len: int = EMBED_FIELD_VALUE_LIMIT) -> str:
    if not lines:
        return "—"
    accepted: list[str] = []
    used = 0
    hidden = 0
    for line in lines:
        piece = line if not accepted else f"\n{line}"
        if used + len(piece) <= max_len:
            accepted.append(line)
            used += len(piece)
            continue
        hidden += 1
    if hidden:
        suffix = f"\n… and {hidden} more."
        if used + len(suffix) <= max_len:
            accepted.append(f"… and {hidden} more.")
        elif accepted:
            last = accepted[-1]
            trim_to = max(1, len(last) - (len(suffix) - (max_len - used)))
            accepted[-1] = last[:trim_to].rstrip() + "…"
    return "\n".join(accepted)[:max_len]


def work_color(category: JobCategory) -> discord.Color:
    if category == JobCategory.EASY:
        return discord.Color.green()
    if category == JobCategory.STABLE:
        return discord.Color.blurple()
    if category == JobCategory.HARD:
        return discord.Color.red()
    return discord.Color.blurple()


def make_job_hub_embed(*, user: discord.abc.User, vip: bool, slot_snap: SlotSnapshot, section: str) -> discord.Embed:
    color = discord.Color.gold() if vip else discord.Color.blurple()
    title = f"Job Hub • {slot_label(slot_snap.slot_index)}"
    if slot_snap.is_active:
        title += " • Active"

    if not slot_snap.is_unlocked:
        embed = discord.Embed(title=title, description="This slot is locked. VIP unlocks the third slot instantly.", color=discord.Color.orange())
        embed.set_footer(text="Use the slot buttons to browse your other job loadouts.")
        return embed

    if not slot_snap.job_key:
        embed = discord.Embed(title=title, description="No job assigned yet. Use **Switch Job** to claim a role for this slot.", color=color)
        embed.add_field(name="What this slot stores", value="• Job assignment\n• Level & prestige\n• Tool loadout\n• Perk unlocks", inline=False)
        embed.set_footer(text="Job Hub keeps each slot fully independent.")
        return embed

    d = JOB_DEFS[slot_snap.job_key]
    presentation = presentation_for(slot_snap.job_key)
    progress = slot_snap.progress
    assert progress is not None
    income_bp, xp_bp, stamina_bp, tool_name = tool_bonus_snapshot(slot_snap.job_key, slot_snap.selected_tool_key, slot_snap.tool_levels)
    lo, hi = income_range_for(slot_snap.job_key, progress.level, progress.prestige, income_bp)
    stamina_cost = stamina_cost_preview(slot_snap.job_key, progress.level, progress.prestige, stamina_bp)
    unlocked, locked = unlocked_perks(slot_snap.job_key, progress.level)
    xp_pct = int((progress.xp / max(progress.xp_needed, 1)) * 100)
    prog_bar = "▰" * min(12, round(12 * progress.xp / max(progress.xp_needed, 1))) + "▱" * (12 - min(12, round(12 * progress.xp / max(progress.xp_needed, 1))))

    embed = discord.Embed(
        title=title,
        description=f"**{d.name}**\nLevel **{progress.level}/{progress.level_cap}** • Prestige **{progress.prestige}**\n`{prog_bar}` **{xp_pct}%** ({fmt_int(progress.xp)}/{fmt_int(progress.xp_needed)} XP)",
        color=color,
    )
    embed.set_author(name=str(user), icon_url=getattr(getattr(user, 'display_avatar', None), 'url', None))

    if section == "overview":
        buffs = [f"• Tool: **{tool_name or 'None'}**"]
        if income_bp:
            buffs.append(f"• Income: **+{income_bp / 100:.0f}%**")
        if xp_bp:
            buffs.append(f"• Job XP: **+{xp_bp / 100:.0f}%**")
        if stamina_bp:
            buffs.append(f"• Stamina efficiency: **+{stamina_bp / 100:.0f}%**")
        if len(buffs) == 1:
            buffs.append("• No active buffs yet")
        overview_lines = [
            f"• Current job: **{d.name}**",
            f"• Category: **{d.category.value.title()}**",
            f"• Active perks: **{len(unlocked)}**",
            f"• Selected tool: **{tool_name or 'Starter'}**",
        ]
        if presentation is not None:
            overview_lines.append(f"• Fantasy: {presentation.fantasy}")
            overview_lines.append(f"• Payout style: {presentation.payout_style}")
            overview_lines.append(f"• Risk: **{presentation.risk_level}**")
            if presentation.can_trigger_danger:
                overview_lines.append("• Danger Encounters: **Yes**")
        embed.add_field(name="Overview", value=_join_lines_with_limit(overview_lines), inline=False)
        embed.add_field(name="Economy Fit", value=f"• Income range: **{fmt_int(lo)} - {fmt_int(hi)} Silver**\n• Stamina cost: **{fmt_int(stamina_cost)}**\n• Switch cost: **{fmt_int(JOB_SWITCH_COST[d.category])} Silver**", inline=True)
        embed.add_field(name="Buffs & Multipliers", value=_join_lines_with_limit(buffs), inline=True)
        if presentation is not None:
            embed.add_field(name="Capstone Identity", value=f"• {presentation.perk_summary}\n• {presentation.danger_summary}", inline=False)
    elif section == "tools":
        lines = []
        for tool in tool_defs_for(slot_snap.job_key):
            lvl = slot_snap.tool_levels.get(tool.key, 0)
            marker = "✅" if slot_snap.selected_tool_key == tool.key else "•"
            effects = []
            if tool.income_bonus_bp:
                effects.append(f"+{tool.income_bonus_bp/100:.0f}% income/lv")
            if tool.xp_bonus_bp:
                effects.append(f"+{tool.xp_bonus_bp/100:.0f}% xp/lv")
            if tool.stamina_discount_bp:
                effects.append(f"-{tool.stamina_discount_bp/100:.0f}% stamina/lv")
            effects_txt = ", ".join(effects) if effects else "starter benefits"
            lines.append(f"{marker} **{tool.name}** • Lv {lvl}\nCost: **{fmt_int(tool.cost * (lvl + 1))}** • {effects_txt}\n{tool.description}")
        embed.add_field(name="Tools & Upgrades", value=_join_lines_with_limit(lines), inline=False)
    elif section == "perks":
        unlocked_lines = [f"✅ **{perk.name}** — {perk.description}" for perk in unlocked] or ["No perks unlocked yet."]
        locked_lines = [f"🔒 **{perk.name}** at **Lv {perk.level_required}** — {perk.description}" for perk in locked] or ["All perks unlocked."]
        embed.add_field(name="Unlocked", value=_join_lines_with_limit(unlocked_lines), inline=False)
        embed.add_field(name="Locked", value=_join_lines_with_limit(locked_lines), inline=False)
        if presentation is not None:
            embed.add_field(name="Job Identity", value=f"• {presentation.perk_summary}\n• {presentation.danger_summary}", inline=False)
    elif section == "prestige":
        current_mult, next_mult, cost = prestige_preview(slot_snap.job_key, progress)
        embed.add_field(name="Prestige Preview", value=f"• Current earnings multiplier: **x{current_mult}**\n• Next earnings multiplier: **x{next_mult}**\n• Required level: **{progress.level_cap}**\n• Cost: **{fmt_int(cost)} Silver**", inline=False)
        embed.add_field(name="Effects", value="Prestiging resets the slot level to 1, keeps the slot's job identity, and increases future earnings scaling.", inline=False)
    elif section == "switch":
        lines = []
        for job in sorted(JOB_DEFS.values(), key=lambda item: item.name.lower()):
            unlock = unlock_level_for(job.key, job.category)
            vip_lock = " • VIP" if job.vip_only else ""
            selected = "✅ " if job.key == slot_snap.job_key else ""
            lines.append(f"{selected}**{job.name}** — unlock **Lv {unlock}** • switch **{fmt_int(JOB_SWITCH_COST[job.category])}**{vip_lock}")
        embed.add_field(name="Available Jobs", value=_join_lines_with_limit(lines[:25]), inline=False)

    embed.set_footer(text="Buttons: slots • overview • switch • tools • perks • prestige")
    return embed


# legacy helpers retained for compatibility

def make_panel_embed(*, user: discord.abc.User, vip: bool, page: str, equipped: Optional[str], equipped_keys: Optional[list[str]] = None) -> discord.Embed:
    description = "The legacy jobs panel has been replaced by Job Hub. Use the buttons below to manage slots, tools, perks, and prestige."
    if equipped_keys:
        description += "\n\nCurrent loadout: " + ", ".join(JOB_DEFS.get(key, None).name if JOB_DEFS.get(key) else key for key in equipped_keys)
    return discord.Embed(title="Job Hub", description=description, color=discord.Color.gold() if vip else discord.Color.blurple())


def make_job_info_embed(*, vip: bool, job_key: str, equipped: Optional[str]) -> discord.Embed:
    key = (job_key or "").strip().lower()
    d = JOB_DEFS.get(key)
    if d is None:
        return discord.Embed(title="Unknown job", description=f"I don’t recognize `{key}`.", color=discord.Color.red())
    fail_bp = category_fail_bp(d.category, d.fail_chance_bp)
    unlock = unlock_level_for(d.key, d.category)
    presentation = presentation_for(d.key)
    extra = ""
    if presentation is not None:
        extra = f"\n{presentation.perk_summary}\n{presentation.danger_summary}"
    return discord.Embed(title=d.name, description=f"Unlock **Lv {unlock}** • Switch **{fmt_int(JOB_SWITCH_COST[d.category])}** • Fail **{fail_bp/100:.2f}%**{extra}", color=discord.Color.gold() if vip else discord.Color.blurple())


def make_rules_embed(*, vip: bool) -> discord.Embed:
    return discord.Embed(title="Job Hub Guide", description="Use slots to manage independent job loadouts. Only the active slot is used for `/work`.", color=discord.Color.gold() if vip else discord.Color.blurple())
