# services/jobs_embeds.py
from __future__ import annotations

from typing import Optional

import discord

from services.jobs_core import (
    JOB_DEFS,
    JOB_SWITCH_COST,
    JOB_UNLOCK_LEVEL,
    JobCategory,
    category_fail_bp,
    fmt_int,
)


def work_color(category: JobCategory) -> discord.Color:
    if category == JobCategory.EASY:
        return discord.Color.green()
    if category == JobCategory.STABLE:
        return discord.Color.blurple()
    if category == JobCategory.HARD:
        return discord.Color.red()
    return discord.Color.blurple()


def _equipped_line(equipped: Optional[str], equipped_keys: Optional[list[str]] = None) -> str:
    if equipped_keys:
        names = []
        for key in equipped_keys:
            d = JOB_DEFS.get(key)
            names.append(d.name if d else key)
        lines = ["✅ Equipped Jobs:"]
        lines.extend(f"{idx+1}. **{name}**" for idx, name in enumerate(names))
        return "\n".join(lines)
    if not equipped:
        return "No job equipped yet."
    d = JOB_DEFS.get(equipped)
    if d:
        return f"✅ Equipped: **{d.name}** (`{d.key}`)"
    return f"✅ Equipped: `{equipped}`"


def job_list_lines(*, vip: bool, want_vip: bool, equipped: Optional[str]) -> list[str]:
    lines: list[str] = []

    defs = sorted(JOB_DEFS.values(), key=lambda d: (d.vip_only, d.name.lower()))
    for d in defs:
        if bool(d.vip_only) != bool(want_vip):
            continue

        marker = "✅ " if equipped == d.key else ""
        unlock = JOB_UNLOCK_LEVEL[d.category]
        cost = JOB_SWITCH_COST[d.category]

        if d.vip_only and not vip:
            lines.append(f"{marker}🔒 **{d.name}** (`{d.key}`) • VIP")
        else:
            lines.append(
                f"{marker}• **{d.name}** (`{d.key}`) • {d.category.value} • unlock **{unlock}** • switch **{fmt_int(cost)}**"
            )

    return lines


def make_panel_embed(*, user: discord.abc.User, vip: bool, page: str, equipped: Optional[str], equipped_keys: Optional[list[str]] = None) -> discord.Embed:
    color = discord.Color.gold() if vip else discord.Color.blurple()

    desc_lines = [
        _equipped_line(equipped, equipped_keys),
        "",
        "Pick up to 3 jobs in the dropdown to build your loadout.",
        "Equip with the **Equip Selected** button.",
        "Each **/work** uses the first slot, then rotates to the next slot.",
        "",
        "**Unlocks + Switch Costs**",
        f"• Hard: unlock lvl {JOB_UNLOCK_LEVEL[JobCategory.HARD]}, switch {fmt_int(JOB_SWITCH_COST[JobCategory.HARD])}",
        f"• Stable: unlock lvl {JOB_UNLOCK_LEVEL[JobCategory.STABLE]}, switch {fmt_int(JOB_SWITCH_COST[JobCategory.STABLE])}",
        f"• Easy: unlock lvl {JOB_UNLOCK_LEVEL[JobCategory.EASY]}, switch {fmt_int(JOB_SWITCH_COST[JobCategory.EASY])}",
        "",
        "**Progression**",
        "• Each job has its own levels, titles, and prestige.",
        "• Your job title upgrades when you prestige.",
        "• **/work** awards user XP + job XP.",
    ]

    embed = discord.Embed(
        title="Jobs Panel",
        description="\n".join(desc_lines),
        color=color,
    )

    if page == "vip":
        lines = job_list_lines(vip=vip, want_vip=True, equipped=equipped)
        embed.add_field(
            name="VIP Jobs",
            value="\n".join(lines) if lines else "No VIP jobs configured.",
            inline=False,
        )
    else:
        lines = job_list_lines(vip=vip, want_vip=False, equipped=equipped)
        embed.add_field(
            name="Standard Jobs",
            value="\n".join(lines) if lines else "No standard jobs configured.",
            inline=False,
        )

    embed.set_footer(text="Use: /job • /work • /job_admin • /work_image_admin")
    return embed


def make_job_info_embed(*, vip: bool, job_key: str, equipped: Optional[str]) -> discord.Embed:
    key = (job_key or "").strip().lower()
    d = JOB_DEFS.get(key)

    if d is None:
        return discord.Embed(
            title="Unknown job",
            description=f"I don’t recognize `{key}`.",
            color=discord.Color.red(),
        )

    if d.vip_only and not vip:
        return discord.Embed(
            title=f"🔒 {d.name}",
            description="This job is VIP-locked.",
            color=discord.Color.red(),
        )

    fail_bp = category_fail_bp(d.category, d.fail_chance_bp)
    fail_txt = "0.00%" if d.category == JobCategory.EASY else f"{fail_bp / 100:.2f}%"
    bonus_txt = (
        f"{d.bonus_chance_bp / 100:.2f}% for x{d.bonus_multiplier:.1f}"
        if d.bonus_chance_bp > 0 and d.bonus_multiplier > 1.0
        else "None"
    )

    unlock = JOB_UNLOCK_LEVEL[d.category]
    switch_cost = JOB_SWITCH_COST[d.category]

    action_lines: list[str] = []
    for a in d.actions[:6]:
        lo = fmt_int(a.min_silver)
        hi = fmt_int(a.max_silver)
        tag = " (failure)" if (a.can_fail and int(a.min_silver) == 0 and int(a.max_silver) == 0) else ""
        action_lines.append(f"• `{a.key}`: {lo} to {hi}{tag}")

    title = d.name
    if equipped == d.key:
        title = f"✅ {title}"

    embed = discord.Embed(
        title=title,
        description=f"Key: `{d.key}`",
        color=discord.Color.gold() if vip else discord.Color.blurple(),
    )
    embed.add_field(name="Category", value=f"**{d.category.value}**", inline=True)
    embed.add_field(name="Unlock", value=f"**Level {unlock}**", inline=True)
    embed.add_field(name="Switch Cost", value=f"**{fmt_int(switch_cost)} Silver**", inline=True)
    embed.add_field(name="Cooldown", value=f"**{fmt_int(d.cooldown_seconds)}s**", inline=True)
    embed.add_field(name="Stamina Cost", value=f"**{fmt_int(d.stamina_cost)}**", inline=True)
    embed.add_field(name="User XP", value=f"**+{fmt_int(d.user_xp_gain)}**", inline=True)
    embed.add_field(name="Job XP", value=f"**+{fmt_int(d.job_xp_gain)}**", inline=True)
    embed.add_field(name="Fail Chance", value=f"**{fail_txt}**", inline=True)
    embed.add_field(name="Bonus", value=f"**{bonus_txt}**", inline=True)
    embed.add_field(
        name="Outcomes",
        value="\n".join(action_lines) if action_lines else "None",
        inline=False,
    )
    embed.set_footer(text="Use the Equip button to confirm switching.")
    return embed


def make_rules_embed(*, vip: bool) -> discord.Embed:
    lines = [
        "**How Jobs Work**",
        "• Pick a job from the dropdown",
        "• Click **Equip Selected**",
        "• Switching costs Silver after your first equip",
        "• Pick up to 3 jobs and equip the loadout",
        "• `/work` cycles slots: 1 → 2 → 3 → 1",
        "",
        "**Unlocks**",
        f"• Easy: level {JOB_UNLOCK_LEVEL[JobCategory.EASY]} ({fmt_int(JOB_SWITCH_COST[JobCategory.EASY])} to switch)",
        f"• Stable: level {JOB_UNLOCK_LEVEL[JobCategory.STABLE]} ({fmt_int(JOB_SWITCH_COST[JobCategory.STABLE])} to switch)",
        f"• Hard: level {JOB_UNLOCK_LEVEL[JobCategory.HARD]} ({fmt_int(JOB_SWITCH_COST[JobCategory.HARD])} to switch)",
        "",
        "**Job Progression**",
        "• Every job tracks its own Level, Title, Prestige",
        "• `/work` awards Job XP and upgrades your job title over time",
    ]
    return discord.Embed(
        title="Jobs Guide",
        description="\n".join(lines),
        color=discord.Color.gold() if vip else discord.Color.blurple(),
    )
