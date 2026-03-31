# cogs/jobs/work.py
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Dict, Optional, Sequence, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import delete, select

from db.models import WalletRow, ActiveEffectRow
from services.db import sessions
from services.stamina import StaminaService
from services.users import ensure_user_rows
from services.vip import is_vip_member
from services.xp_award import award_xp
from services.work_xp import apply_work_xp_multipliers, is_weekend
from services.work_drops import roll_and_grant_work_drops, WorkDropResult
from services.tool_procs import WorkToolProcResolver, resolve_tool_path

from services.jobs_balance import job_xp_for_work, payout_for_work, prestige_cost, stamina_cost_for_work, user_xp_for_work
from services.achievements import (
    check_and_grant_achievements,
    increment_counter,
    queue_achievement_announcements,
)

from services.jobs_core import (
    JobAction,
    JobCategory,
    JobDef,
    JobEffects,
    JobTier,
    NoopInventoryAdapter,
    apply_bp,
    bar,
    category_fail_bp,
    clamp_int,
    compute_effects_from_upgrades_and_items,
    fmt_int,
    get_equipped_key,
    get_equipped_keys,
    get_job_snapshot,
    get_level,
    get_or_create_job_row,
    job_row_image_get,
    rotate_equipped_jobs,
    roll_bp,
    sub_bp,
    tier_for_category,
)
from services.job_hub import (
    award_slot_job_xp,
    buy_or_upgrade_tool,
    event_defs_for,
    get_active_slot,
    get_slot_snapshot,
    prestige_slot,
    tool_bonus_snapshot,
    unlocked_perks,
    xp_needed,
)
from .jobs import get_job_def
from services.job_progression import level_cap_for, title_for
from services.job_upgrades import apply_income_upgrade, get_upgrade_level, upgrade_once
from services.jobs_endgame import (
    DangerEncounterView,
    build_danger_embed,
    build_danger_result_embed,
    pick_danger_encounter,
    pick_normal_interaction,
    resolve_danger_choice,
    resolve_normal_interaction,
    should_trigger_danger,
    should_trigger_normal,
)
from services.jobs_views import open_job_hub

# -------------------------
# Cooldowns
# -------------------------
_COOLDOWNS: Dict[Tuple[int, int, str], float] = {}
_WORK_COMBO_STREAK: Dict[Tuple[int, int], int] = {}


# -------------------------
# Local helpers
# -------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _work_color(category: JobCategory) -> discord.Color:
    if category == JobCategory.EASY:
        return discord.Color.green()
    if category == JobCategory.STABLE:
        return discord.Color.blurple()
    if category == JobCategory.HARD:
        return discord.Color.red()
    return discord.Color.blurple()


def _split_actions(d: JobDef) -> tuple[list[JobAction], list[JobAction]]:
    success: list[JobAction] = []
    failures: list[JobAction] = []

    for a in d.actions:
        is_zero = int(a.min_silver) == 0 and int(a.max_silver) == 0
        if a.can_fail and is_zero:
            failures.append(a)
        else:
            success.append(a)

    if not success:
        success = list(d.actions)
    if not failures:
        failures = list(d.actions)

    return success, failures


def _pick_weighted(actions: Sequence[JobAction]) -> JobAction:
    if not actions:
        raise ValueError("No actions to pick from")

    total = 0
    for a in actions:
        total += max(int(a.weight), 0)

    if total <= 0:
        return actions[0]

    r = random.randint(1, total)
    acc = 0
    for a in actions:
        w = max(int(a.weight), 0)
        acc += w
        if r <= acc:
            return a

    return actions[-1]


def _pct(n: int, d: int) -> int:
    d = max(int(d), 1)
    n = max(int(n), 0)
    return int(min(100, (n * 100) // d))


def _effective_work_cooldown_seconds(*, base_seconds: float, work_level: int, vip: bool) -> float:
    # 1% cooldown reduction per work level, capped at 50% reduction.
    reduction_bp = clamp_int(max(int(work_level), 1) - 1, 0, 50) * 100
    scale = max(0.5, 1.0 - (reduction_bp / 10_000.0))
    effective_seconds = max(1.0, float(base_seconds) * scale)
    if vip:
        effective_seconds = min(effective_seconds, 10.0)
    return effective_seconds


# -------------------------
# Item Effects (minimal in-cog resolver)
# -------------------------
@dataclass(frozen=True)
class _ItemMods:
    payout_bonus_bp: int = 0
    fail_reduction_bp: int = 0
    stamina_discount_bp: int = 0
    stamina_cost_flat_delta: int = 0
    job_xp_bonus_bp: int = 0
    double_payout_chance_bp: int = 0
    extra_roll_bp: int = 0
    rare_find_bp: int = 0
    protection_bp: int = 0
    greed_payout_bp: int = 0
    greed_fail_bp: int = 0
    burst_chance_bp: int = 0
    burst_payout_bp: int = 0
    combo_payout_step_bp: int = 0
    combo_max_stacks: int = 0
    lootbox_drop_bp: int = 0
    item_drop_bp: int = 0
    next_work_payout_bp: int = 0
    job_xp_progress_bp: int = 0
    job_level_gain: int = 0


async def _cleanup_expired_item_effects(session, *, guild_id: int, user_id: int) -> None:
    now = _utc_now()
    await session.execute(
        delete(ActiveEffectRow).where(
            ActiveEffectRow.guild_id == guild_id,
            ActiveEffectRow.user_id == user_id,
            ActiveEffectRow.expires_at.is_not(None),
            ActiveEffectRow.expires_at <= now,
        )
    )


def _int_payload(payload: object, key: str) -> int:
    if not isinstance(payload, dict):
        return 0
    v = payload.get(key, 0)
    try:
        return int(v)
    except Exception:
        return 0


async def _get_item_mods(session, *, guild_id: int, user_id: int) -> _ItemMods:
    await _cleanup_expired_item_effects(session, guild_id=guild_id, user_id=user_id)

    rows = await session.execute(
        select(ActiveEffectRow).where(
            ActiveEffectRow.guild_id == guild_id,
            ActiveEffectRow.user_id == user_id,
        )
    )

    payout_bonus_bp = 0
    fail_reduction_bp = 0
    stamina_discount_bp = 0
    stamina_cost_flat_delta = 0
    job_xp_bonus_bp = 0
    double_payout_chance_bp = 0
    extra_roll_bp = 0
    rare_find_bp = 0
    protection_bp = 0
    greed_payout_bp = 0
    greed_fail_bp = 0
    burst_chance_bp = 0
    burst_payout_bp = 0
    combo_payout_step_bp = 0
    combo_max_stacks = 0
    lootbox_drop_bp = 0
    item_drop_bp = 0
    next_work_payout_bp = 0
    job_xp_progress_bp = 0
    job_level_gain = 0

    for r in rows.scalars():
        payload = r.payload_json
        payout_bonus_bp += _int_payload(payload, "payout_bonus_bp")
        fail_reduction_bp += _int_payload(payload, "fail_reduction_bp")
        stamina_discount_bp += _int_payload(payload, "stamina_discount_bp")
        stamina_cost_flat_delta += _int_payload(payload, "stamina_cost_flat_delta")
        job_xp_bonus_bp += _int_payload(payload, "job_xp_bonus_bp")
        double_payout_chance_bp += _int_payload(payload, "double_payout_chance_bp")
        extra_roll_bp += _int_payload(payload, "extra_roll_bp")
        rare_find_bp += _int_payload(payload, "rare_find_bp")
        protection_bp += _int_payload(payload, "protection_bp")
        greed_payout_bp += _int_payload(payload, "greed_payout_bp")
        greed_fail_bp += _int_payload(payload, "greed_fail_bp")
        burst_chance_bp += _int_payload(payload, "burst_chance_bp")
        burst_payout_bp += _int_payload(payload, "burst_payout_bp")
        combo_payout_step_bp += _int_payload(payload, "combo_payout_step_bp")
        combo_max_stacks += _int_payload(payload, "combo_max_stacks")
        lootbox_drop_bp += _int_payload(payload, "lootbox_drop_bp")
        item_drop_bp += _int_payload(payload, "item_drop_bp")
        next_work_payout_bp += _int_payload(payload, "next_work_payout_bp")
        next_work_payout_bp += _int_payload(payload, "next_work_silver_mult_bp")
        job_xp_progress_bp += _int_payload(payload, "job_xp_progress_bp")
        job_level_gain += _int_payload(payload, "job_level_gain")

    payout_bonus_bp = clamp_int(payout_bonus_bp, -10_000, 50_000)
    fail_reduction_bp = clamp_int(fail_reduction_bp, 0, 10_000)
    stamina_discount_bp = clamp_int(stamina_discount_bp, 0, 10_000)
    stamina_cost_flat_delta = clamp_int(stamina_cost_flat_delta, -10, 10)
    job_xp_bonus_bp = clamp_int(job_xp_bonus_bp, -10_000, 50_000)
    double_payout_chance_bp = clamp_int(double_payout_chance_bp, 0, 10_000)
    extra_roll_bp = clamp_int(extra_roll_bp, 0, 10_000)
    rare_find_bp = clamp_int(rare_find_bp, 0, 10_000)
    protection_bp = clamp_int(protection_bp, 0, 10_000)
    greed_payout_bp = clamp_int(greed_payout_bp, 0, 50_000)
    greed_fail_bp = clamp_int(greed_fail_bp, 0, 10_000)
    burst_chance_bp = clamp_int(burst_chance_bp, 0, 10_000)
    burst_payout_bp = clamp_int(burst_payout_bp, 0, 50_000)
    combo_payout_step_bp = clamp_int(combo_payout_step_bp, 0, 20_000)
    combo_max_stacks = clamp_int(combo_max_stacks, 0, 20)
    lootbox_drop_bp = clamp_int(lootbox_drop_bp, 0, 9_000)
    item_drop_bp = clamp_int(item_drop_bp, 0, 9_000)
    next_work_payout_bp = clamp_int(next_work_payout_bp, 0, 40_000)
    job_xp_progress_bp = clamp_int(job_xp_progress_bp, 0, 10_000)
    job_level_gain = clamp_int(job_level_gain, 0, 5)

    return _ItemMods(
        payout_bonus_bp=payout_bonus_bp,
        fail_reduction_bp=fail_reduction_bp,
        stamina_discount_bp=stamina_discount_bp,
        stamina_cost_flat_delta=stamina_cost_flat_delta,
        job_xp_bonus_bp=job_xp_bonus_bp,
        double_payout_chance_bp=double_payout_chance_bp,
        extra_roll_bp=extra_roll_bp,
        rare_find_bp=rare_find_bp,
        protection_bp=protection_bp,
        greed_payout_bp=greed_payout_bp,
        greed_fail_bp=greed_fail_bp,
        burst_chance_bp=burst_chance_bp,
        burst_payout_bp=burst_payout_bp,
        combo_payout_step_bp=combo_payout_step_bp,
        combo_max_stacks=combo_max_stacks,
        lootbox_drop_bp=lootbox_drop_bp,
        item_drop_bp=item_drop_bp,
        next_work_payout_bp=next_work_payout_bp,
        job_xp_progress_bp=job_xp_progress_bp,
        job_level_gain=job_level_gain,
    )


async def _consume_charge_from_group(
    session,
    *,
    guild_id: int,
    user_id: int,
    group_key: str,
    amount: int = 1,
) -> None:
    if amount <= 0:
        return

    rows = await session.execute(
        select(ActiveEffectRow)
        .where(
            ActiveEffectRow.guild_id == guild_id,
            ActiveEffectRow.user_id == user_id,
            ActiveEffectRow.group_key == group_key,
            ActiveEffectRow.charges_remaining.is_not(None),
            ActiveEffectRow.charges_remaining > 0,
        )
        .order_by(ActiveEffectRow.created_at.asc(), ActiveEffectRow.id.asc())
        .with_for_update()
    )

    left = amount
    for r in rows.scalars():
        if left <= 0:
            break
        cur = int(r.charges_remaining or 0)
        take = min(cur, left)
        cur -= take
        left -= take

        if cur <= 0:
            await session.delete(r)
        else:
            r.charges_remaining = cur


@dataclass(frozen=True)
class _WorkEffectsAdapter:
    effects: JobEffects

    async def get_job_effects(
        self,
        session,
        *,
        guild_id: int,
        user_id: int,
        job_id: int,
        job_key: str,
        prestige: int,
        level: int,
    ) -> JobEffects:
        _ = session, guild_id, user_id, job_id, job_key, prestige, level
        return self.effects


@dataclass(frozen=True)
class _WorkResultPayload:
    action_text: str
    failed: bool
    payout: int
    base_pay: int
    item_bonus: int
    tool_bonus: int
    proc_bonus: int
    event_bonus: int
    other_bonus: int
    stamina_base_cost: int
    stamina_after_discount: int
    stamina_flat_delta: int
    stamina_final_cost: int
    tool_stamina_saved: bool
    tool_stamina_save_chance_bp: int
    user_xp: int
    user_xp_base: int
    user_xp_bonus: int
    job_xp: int
    job_xp_base: int
    job_xp_bonus: int
    job_title: str
    job_level: int
    job_prestige: int
    job_xp_into: int
    job_xp_need: int
    did_double: bool
    upgrade_level: int
    upgrade_bonus_pct: int
    work_image_url: Optional[str]
    leveled_up: bool
    prestiged: bool
    next_job_name: Optional[str]
    weekend_bonus_active: bool
    drop_result: Optional[WorkDropResult]


def _build_work_embed(
    *,
    user: discord.abc.User,
    d: JobDef,
    payload: _WorkResultPayload,
    effects: JobEffects,
    item_mods: _ItemMods,
    expanded: bool = False,
) -> discord.Embed:
    color = _work_color(d.category)

    outcome_emoji = "❌" if payload.failed else "✅"
    outcome_word = "FAILED" if payload.failed else "SUCCESS"

    stamina_line = f"• ⚡ **-{fmt_int(payload.stamina_final_cost)}** Stamina"
    if payload.tool_stamina_saved:
        stamina_line = "• ⚡ **0 Stamina** (tool save proc)"
    header_line = (
        f"{outcome_emoji} **{outcome_word}**  "
        f"• 💰 **{fmt_int(payload.payout)}** Silver  "
        f"{stamina_line}"
    )

    if payload.did_double:
        header_line += "  • 🪙 **2x payout!**"

    prog_bar = bar(payload.job_xp_into, payload.job_xp_need)
    prog_pct = _pct(payload.job_xp_into, payload.job_xp_need)

    prog_block = (
        f"**P{fmt_int(payload.job_prestige)}** • **{payload.job_title}**\n"
        f"Level **{fmt_int(payload.job_level)}**  "
        f"[{prog_bar}]  **{prog_pct}%**"
    )

    gain_lines = [
        f"🧠 User XP: **+{fmt_int(payload.user_xp)}**",
        f"🧰 Job XP: **+{fmt_int(payload.job_xp)}**",
        f"⚙️ Upgrade: **Lv {fmt_int(payload.upgrade_level)}** (**+{fmt_int(payload.upgrade_bonus_pct)}% income**)"
    ]
    if payload.weekend_bonus_active:
        gain_lines.append("🔥 Weekend Bonus: **2x XP Active**")
    gain_block = "\n".join(gain_lines)

    notes: list[str] = []
    if payload.prestiged:
        notes.append("✨ Prestiged, new title unlocked")
        notes.append(f"💸 Prestige Cost: **{fmt_int(prestige_cost(payload.job_prestige - 1))} Silver**")
    elif payload.leveled_up:
        notes.append("⬆️ Leveled up")

    eff_lines: list[str] = []
    if int(effects.payout_bonus_bp) != 0:
        eff_lines.append(f"💰 Payout: **+{effects.payout_bonus_bp / 100:.2f}%**")
    if int(effects.fail_reduction_bp) != 0:
        eff_lines.append(f"🛡️ Fail reduction: **{effects.fail_reduction_bp / 100:.2f}%**")
    if int(effects.stamina_discount_bp) != 0:
        eff_lines.append(f"⚡ Stamina discount: **{effects.stamina_discount_bp / 100:.2f}%**")
    if int(effects.job_xp_bonus_bp) != 0:
        eff_lines.append(f"🧰 Job XP: **+{effects.job_xp_bonus_bp / 100:.2f}%**")
    if int(getattr(effects, "extra_roll_bp", 0)) != 0:
        eff_lines.append(f"🎲 Extra roll chance: **{int(getattr(effects, 'extra_roll_bp', 0)) / 100:.2f}%**")
    if int(getattr(effects, "rare_find_bp", 0)) != 0:
        eff_lines.append(f"✨ Rare find chance: **{int(getattr(effects, 'rare_find_bp', 0)) / 100:.2f}%**")

    if int(item_mods.stamina_cost_flat_delta) != 0:
        eff_lines.append(f"⚡ Stamina cost: **{item_mods.stamina_cost_flat_delta:+d}** (flat)")
    if int(payload.tool_stamina_save_chance_bp) > 0:
        eff_lines.append(f"🧰 Tool stamina save: **{payload.tool_stamina_save_chance_bp / 100:.2f}%** chance")
    if int(item_mods.double_payout_chance_bp) != 0:
        eff_lines.append(f"🪙 2x payout chance: **{item_mods.double_payout_chance_bp / 100:.2f}%**")
    if int(item_mods.protection_bp) != 0:
        eff_lines.append(f"🛡️ Fail protection: **{item_mods.protection_bp / 100:.2f}%**")
    if int(item_mods.combo_payout_step_bp) != 0:
        eff_lines.append(f"🔥 Combo step: **+{item_mods.combo_payout_step_bp / 100:.2f}%**")

    avatar_url = getattr(getattr(user, "display_avatar", None), "url", None)

    embed = discord.Embed(
        title=f"{d.name} Work Result",
        description=f"{payload.action_text}\n\n{header_line}",
        color=color,
    )

    embed.set_author(name=str(user), icon_url=avatar_url)

    embed.add_field(name="Job Progress", value=prog_block, inline=False)
    embed.add_field(name="Gains", value=gain_block, inline=True)

    if notes:
        embed.add_field(name="Milestone", value="\n".join(notes), inline=True)

    if payload.drop_result is not None and (payload.drop_result.lootbox_rarity or payload.drop_result.item_key):
        rarity = (payload.drop_result.lootbox_rarity or "").lower()
        drop_lines: list[str] = []
        if rarity:
            flair = "🌌 JACKPOT!" if rarity == "mythical" else "🎁 Drop!"
            drop_lines.append(f"{flair} Lootbox: **{rarity.upper()}**")
        if payload.drop_result.item_key:
            drop_lines.append(f"🧩 Item: **{payload.drop_result.item_key}**")
        if expanded:
            drop_lines.append("Source: Work drop roll succeeded after progression/tier/proc modifiers.")
        embed.add_field(name="Drops", value="\n".join(drop_lines), inline=False)

    if eff_lines:
        embed.add_field(name="Bonuses Active", value="\n".join(eff_lines), inline=False)

    if expanded:
        income_lines: list[str] = [f"• Base Pay: **{fmt_int(payload.base_pay)}**"]
        if payload.item_bonus:
            income_lines.append(f"• Item Bonus: **+{fmt_int(payload.item_bonus)}**")
        if payload.tool_bonus:
            income_lines.append(f"• Tool Bonus: **+{fmt_int(payload.tool_bonus)}**")
        if payload.proc_bonus:
            income_lines.append(f"• Proc Bonus: **+{fmt_int(payload.proc_bonus)}**")
        if payload.event_bonus:
            income_lines.append(f"• Event Bonus: **+{fmt_int(payload.event_bonus)}**")
        if payload.other_bonus:
            income_lines.append(f"• Other Bonus: **+{fmt_int(payload.other_bonus)}**")
        income_lines.append(f"• Final Payout: **{fmt_int(payload.payout)}**")
        embed.add_field(name="Income Breakdown", value="\n".join(income_lines), inline=False)

        xp_lines = [
            f"• User XP: **{fmt_int(payload.user_xp_base)}** base + **{fmt_int(payload.user_xp_bonus)}** bonus = **{fmt_int(payload.user_xp)}**",
            f"• Job XP: **{fmt_int(payload.job_xp_base)}** base + **{fmt_int(payload.job_xp_bonus)}** bonus = **{fmt_int(payload.job_xp)}**",
        ]
        embed.add_field(name="XP Breakdown", value="\n".join(xp_lines), inline=False)

        stamina_lines = [
            f"• Base Cost: **{fmt_int(payload.stamina_base_cost)}**",
            f"• Discounted Cost: **{fmt_int(payload.stamina_after_discount)}**",
        ]
        if payload.stamina_flat_delta:
            stamina_lines.append(f"• Flat Delta: **{payload.stamina_flat_delta:+d}**")
        if payload.tool_stamina_saved:
            stamina_lines.append("• Tool Save Proc: **-1 full spend**")
        stamina_lines.append(f"• Final Cost: **{fmt_int(payload.stamina_final_cost)}**")
        embed.add_field(name="Stamina Breakdown", value="\n".join(stamina_lines), inline=False)

    if payload.work_image_url:
        embed.set_image(url=payload.work_image_url)

    if payload.next_job_name:
        embed.set_footer(text=f"Next shift: {payload.next_job_name} • Use /job to edit your 3 job slots")
    else:
        embed.set_footer(text="Use /job to edit your 3 job slots")
    return embed


class WorkCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.stamina = StaminaService()
        self._work_guard_map_lock = asyncio.Lock()
        self._work_user_locks: dict[tuple[int, int], asyncio.Lock] = {}

    async def _work_lock_for(self, guild_id: int, user_id: int) -> asyncio.Lock:
        key = (int(guild_id), int(user_id))
        async with self._work_guard_map_lock:
            lock = self._work_user_locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._work_user_locks[key] = lock
            return lock

    @app_commands.command(name="work", description="Work your equipped job.")
    async def work_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        guild_id = interaction.guild.id
        user_id = interaction.user.id
        work_lock = await self._work_lock_for(guild_id, user_id)
        if work_lock.locked():
            await interaction.response.send_message(
                "Slow down, your last work is still running.",
                ephemeral=True,
            )
            return

        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]
        now = time.time()
        lock_acquired = False

        try:
            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)

            await work_lock.acquire()
            lock_acquired = True

            embed: Optional[discord.Embed] = None
            result_payload: Optional[_WorkResultPayload] = None
            result_effects: Optional[JobEffects] = None
            result_item_mods: Optional[_ItemMods] = None
            key: Optional[str] = None
            used_cooldown_seconds: Optional[float] = None
            next_job_name: Optional[str] = None

            async with self.sessionmaker() as session:
                async with session.begin():
                    await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                    active_slot = await get_active_slot(session, guild_id=guild_id, user_id=user_id, vip=vip)
                    key = active_slot.job_key
                    if not key:
                        await interaction.followup.send(
                            "You don’t have a job assigned to your active Job Hub slot yet.\nUse **/job** to configure it.",
                            ephemeral=True,
                        )
                        return

                    d = get_job_def(key)
                    if d is None:
                        await interaction.followup.send(
                            "Your first job slot no longer exists. Use **/job** to set your slots again.",
                            ephemeral=True,
                        )
                        return

                    if d.vip_only and not vip:
                        await interaction.followup.send(
                            "Your current slot is VIP-locked and you’re not VIP. Pick another with **/job**.",
                            ephemeral=True,
                        )
                        return

                    job_row = await get_or_create_job_row(session, job_key=key)
                    if not bool(getattr(job_row, "enabled", True)):
                        await interaction.followup.send(f"Job `{key}` is disabled in DB.", ephemeral=True)
                        return

                    user_level = await get_level(session, guild_id=guild_id, user_id=user_id)

                    from services.jobs_core import unlock_level_for
                    need_unlock = unlock_level_for(d.key, d.category)

                    if (not vip) and user_level < need_unlock:
                        await interaction.followup.send(f"🔒 **{d.name}** unlocks at **Level {need_unlock}**.", ephemeral=True)
                        return

                    if not d.actions:
                        await interaction.followup.send("This job has no actions configured.", ephemeral=True)
                        return

                    slot_snap = await get_slot_snapshot(session, guild_id=guild_id, user_id=user_id, vip=vip, slot_index=int(active_slot.slot_index))
                    snap_before = slot_snap.progress
                    if snap_before is None:
                        await interaction.followup.send("This slot has no progression state yet. Reopen `/job` and try again.", ephemeral=True)
                        return
                    effective_cd = _effective_work_cooldown_seconds(
                        base_seconds=float(d.cooldown_seconds),
                        work_level=int(snap_before.level),
                        vip=vip,
                    )
                    cd_key = (guild_id, user_id, key)
                    ready_at = _COOLDOWNS.get(cd_key, 0.0)
                    if ready_at > now:
                        left = int(max(ready_at - now, 0))
                        await interaction.followup.send(f"Cooldown. Try again in **{fmt_int(left)}s**.", ephemeral=True)
                        return

                    income_tool_bp, xp_tool_bp, tool_stamina_save_chance_bp, selected_tool_name, selected_tool_index = tool_bonus_snapshot(
                        key,
                        slot_snap.selected_tool_key,
                        slot_snap.tool_levels,
                    )
                    tool_stamina_save_chance_bp = clamp_int(int(tool_stamina_save_chance_bp), 0, 9_500)
                    tool_path = resolve_tool_path(tool_index=selected_tool_index)
                    job_effects = JobEffects(
                        payout_bonus_bp=income_tool_bp,
                        fail_reduction_bp=0,
                        stamina_discount_bp=0,
                        job_xp_bonus_bp=xp_tool_bp,
                        user_xp_bonus_bp=0,
                    ).clamp()

                    item_mods = await _get_item_mods(session, guild_id=guild_id, user_id=user_id)

                    merged_effects = replace(
                        job_effects,
                        payout_bonus_bp=clamp_int(int(job_effects.payout_bonus_bp) + int(item_mods.payout_bonus_bp), -10_000, 50_000),
                        fail_reduction_bp=clamp_int(int(job_effects.fail_reduction_bp) + int(item_mods.fail_reduction_bp), 0, 10_000),
                        stamina_discount_bp=clamp_int(int(job_effects.stamina_discount_bp) + int(item_mods.stamina_discount_bp), 0, 10_000),
                        job_xp_bonus_bp=clamp_int(int(job_effects.job_xp_bonus_bp) + int(item_mods.job_xp_bonus_bp), -10_000, 50_000),
                        extra_roll_bp=clamp_int(int(job_effects.extra_roll_bp) + int(item_mods.extra_roll_bp), 0, 10_000),
                        rare_find_bp=clamp_int(int(job_effects.rare_find_bp) + int(item_mods.rare_find_bp), 0, 10_000),
                    )
                    merged_effects = merged_effects.clamp()

                    job_level_now = max(int(snap_before.level), 1)
                    job_prestige_now = max(int(snap_before.prestige), 0)

                    stamina_cost_base = stamina_cost_for_work(
                        job_level=job_level_now,
                        prestige=job_prestige_now,
                        category=d.category,
                    )
                    stamina_cost = sub_bp(stamina_cost_base, int(merged_effects.stamina_discount_bp))
                    stamina_cost = int(stamina_cost) + int(item_mods.stamina_cost_flat_delta)
                    stamina_cost = clamp_int(int(stamina_cost), 1, 10)
                    proc_messages: list[str] = []
                    effective_stamina_cost = int(stamina_cost)
                    tool_stamina_saved = False

                    success_actions, fail_actions = _split_actions(d)

                    fail_bp = category_fail_bp(d.category, d.fail_chance_bp)
                    fail_bp = max(int(fail_bp) - int(merged_effects.fail_reduction_bp), 0)
                    fail_bp = min(10_000, fail_bp + int(item_mods.greed_fail_bp))
                    fail_bp = clamp_int(fail_bp, 0, 10_000)

                    failed = roll_bp(fail_bp) if fail_bp > 0 else False
                    if failed and int(item_mods.protection_bp) > 0 and roll_bp(int(item_mods.protection_bp)):
                        failed = False

                    no_stamina_outcome = WorkToolProcResolver.roll_no_stamina(
                        tool_path=tool_path,
                        no_stamina_chance_bp=int(tool_stamina_save_chance_bp),
                    )
                    if no_stamina_outcome.skipped_stamina:
                        effective_stamina_cost = 0
                        tool_stamina_saved = True
                        proc_messages.extend(no_stamina_outcome.messages)

                    ok, stam_snap = await self.stamina.try_spend(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                        cost=effective_stamina_cost,
                        is_vip=vip,
                    )
                    if not ok:
                        await interaction.followup.send(
                            f"Not enough stamina. You have **{fmt_int(stam_snap.current)}/{fmt_int(stam_snap.max)}**.",
                            ephemeral=True,
                        )
                        return

                    failure_negation_outcome = WorkToolProcResolver.roll_failure_negation(
                        tool_path=tool_path,
                        failed=failed,
                    )
                    if failure_negation_outcome.failure_negated:
                        failed = False
                        proc_messages.extend(failure_negation_outcome.messages)

                    wallet = await session.scalar(
                        select(WalletRow).where(
                            WalletRow.guild_id == guild_id,
                            WalletRow.user_id == user_id,
                        )
                    )
                    if wallet is None:
                        wallet = WalletRow(guild_id=guild_id, user_id=user_id, silver=0, diamonds=0)
                        session.add(wallet)
                        await session.flush()

                    extra_roll = roll_bp(int(getattr(merged_effects, "extra_roll_bp", 0))) if int(getattr(merged_effects, "extra_roll_bp", 0)) > 0 else False

                    payout = 0
                    base_pay = 0
                    item_bonus_total = 0
                    tool_bonus_total = 0
                    proc_bonus_total = 0
                    event_bonus_total = 0
                    other_bonus_total = 0
                    action_text = ""
                    did_double = False
                    drop_result = WorkDropResult()
                    upgrade_level = 0
                    upgrade_bonus_pct = 0
                    base_user_xp = 0
                    adjusted_user_xp = 0
                    base_job_xp = 0
                    adjusted_job_xp = 0

                    if failed:
                        action = _pick_weighted(fail_actions)
                        action_text = action.text
                        payout = 0
                    else:
                        action = _pick_weighted(success_actions)
                        action_text = action.text

                        lo = int(action.min_silver)
                        hi = int(action.max_silver)
                        if hi < lo:
                            lo, hi = hi, lo

                        def _roll_payout_once() -> tuple[int, int, int, int]:
                            raw = random.randint(max(lo, 0), max(hi, 0))
                            raw_start = raw

                            if int(getattr(merged_effects, "rare_find_bp", 0)) > 0 and roll_bp(int(getattr(merged_effects, "rare_find_bp", 0))):
                                bump = max(int(round(raw * 0.25)), 1)
                                raw += bump
                            rare_delta = raw - raw_start

                            if d.bonus_chance_bp > 0 and d.bonus_multiplier > 1.0 and roll_bp(d.bonus_chance_bp):
                                raw = int(round(raw * float(d.bonus_multiplier)))
                            proc_delta = raw - raw_start - rare_delta

                            scaled = payout_for_work(
                                base_payout=raw,
                                job_key=key,
                                job_level=job_level_now,
                                prestige=job_prestige_now,
                            )
                            scale_delta = scaled - raw
                            final = apply_bp(scaled, int(merged_effects.payout_bonus_bp))
                            effect_delta = final - scaled
                            return max(int(final), 0), int(scale_delta), int(effect_delta), int(rare_delta + proc_delta)

                        p1, scale_delta_1, effect_delta_1, proc_delta_1 = _roll_payout_once()
                        if extra_roll:
                            p2, scale_delta_2, effect_delta_2, proc_delta_2 = _roll_payout_once()
                            if p2 > p1:
                                payout = p2
                                base_pay = payout - scale_delta_2 - effect_delta_2 - proc_delta_2
                                tool_bonus_total += effect_delta_2
                                proc_bonus_total += proc_delta_2
                            else:
                                payout = p1
                                base_pay = payout - scale_delta_1 - effect_delta_1 - proc_delta_1
                                tool_bonus_total += effect_delta_1
                                proc_bonus_total += proc_delta_1
                        else:
                            payout = p1
                            base_pay = payout - scale_delta_1 - effect_delta_1 - proc_delta_1
                            tool_bonus_total += effect_delta_1
                            proc_bonus_total += proc_delta_1
                        other_bonus_total += max(0, payout - base_pay - tool_bonus_total - proc_bonus_total)
                        if int(item_mods.greed_payout_bp) > 0:
                            before = payout
                            payout = apply_bp(payout, int(item_mods.greed_payout_bp))
                            item_bonus_total += max(0, payout - before)
                        if int(item_mods.burst_chance_bp) > 0 and int(item_mods.burst_payout_bp) > 0 and roll_bp(int(item_mods.burst_chance_bp)):
                            before = payout
                            payout = apply_bp(payout, int(item_mods.burst_payout_bp))
                            proc_bonus_total += max(0, payout - before)
                            action_text += "\n💥 **Burst proc!** Jackpot multiplier activated."
                        if int(item_mods.next_work_payout_bp) > 0:
                            before = payout
                            payout = apply_bp(payout, int(item_mods.next_work_payout_bp))
                            item_bonus_total += max(0, payout - before)
                            await _consume_charge_from_group(
                                session,
                                guild_id=guild_id,
                                user_id=user_id,
                                group_key="next_work_bonus",
                                amount=1,
                            )
                            await _consume_charge_from_group(
                                session,
                                guild_id=guild_id,
                                user_id=user_id,
                                group_key="next_work_multiplier",
                                amount=1,
                            )

                        upgrade_level = int(slot_snap.tool_levels.get(slot_snap.selected_tool_key or "", 0))
                        upgrade_bonus_pct = income_tool_bp // 100

                        perk_unlocked, _ = unlocked_perks(key, job_level_now)
                        for event in event_defs_for(key):
                            chance_bp = int(event.chance_bp)
                            chance_bp += sum(int(perk.event_weight_bonus_bp) for perk in perk_unlocked)
                            if roll_bp(chance_bp):
                                before = payout
                                payout = max((payout * (10_000 + int(event.payout_multiplier_bp))) // 10_000, 0) + int(event.bonus_silver_flat)
                                event_bonus_total += max(0, payout - before)
                                stamina_cost = clamp_int(stamina_cost + int(event.stamina_delta), 1, 10)
                                action_text += f"\n🎲 **{event.name}:** {event.description}"
                                if event.fail_override is False:
                                    failed = False
                                break

                        base_user_xp = user_xp_for_work(user_level=user_level, category=d.category)
                        adjusted_user_xp = apply_bp(base_user_xp, int(merged_effects.user_xp_bonus_bp))
                        user_xp_gain = apply_work_xp_multipliers(int(adjusted_user_xp))
                        await award_xp(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            amount=int(user_xp_gain),
                            apply_weekend_multiplier=False,
                        )
                        await increment_counter(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            counter_key="jobs_completed",
                            amount=1,
                        )

                        base_job_xp = job_xp_for_work(
                            job_level=job_level_now,
                            prestige=job_prestige_now,
                            category=d.category,
                        )
                        adjusted_job_xp = apply_bp(base_job_xp, int(merged_effects.job_xp_bonus_bp))
                        delta_job_xp = apply_work_xp_multipliers(int(adjusted_job_xp))

                        tool_proc_outcome = await WorkToolProcResolver.apply_success_effects(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            tool_path=tool_path,
                            payout=int(payout),
                            job_xp=int(delta_job_xp),
                        )
                        if int(tool_proc_outcome.xp_burst_bonus) > 0:
                            delta_job_xp += int(tool_proc_outcome.xp_burst_bonus)
                            proc_bonus_total += 0
                        if int(tool_proc_outcome.critical_silver_bonus) > 0:
                            before = payout
                            payout += int(tool_proc_outcome.critical_silver_bonus)
                            tool_bonus_total += max(0, payout - before)
                        if int(tool_proc_outcome.critical_job_xp_bonus) > 0:
                            delta_job_xp += int(tool_proc_outcome.critical_job_xp_bonus)
                        if int(tool_proc_outcome.double_action_payout) > 0:
                            before = payout
                            payout += int(tool_proc_outcome.double_action_payout)
                            tool_bonus_total += max(0, payout - before)
                        if int(tool_proc_outcome.double_action_job_xp) > 0:
                            delta_job_xp += int(tool_proc_outcome.double_action_job_xp)
                        proc_messages.extend(tool_proc_outcome.messages)

                        danger_triggered = (not failed) and should_trigger_danger(key)
                        force_special_pipeline = bool(tool_proc_outcome.rare_event_triggered)
                        if (not failed) and (not danger_triggered) and (force_special_pipeline or should_trigger_normal(key)):
                            normal_interaction = pick_normal_interaction(key)
                            if normal_interaction is not None:
                                before = payout
                                normal_resolution = resolve_normal_interaction(
                                    interaction=normal_interaction,
                                    payout=payout,
                                    job_xp=delta_job_xp,
                                )
                                payout = int(normal_resolution.payout)
                                delta_job_xp = int(normal_resolution.job_xp)
                                proc_bonus_total += max(0, payout - before)
                                action_text += (
                                    f"\n✨ **{normal_interaction.title}:** {normal_interaction.description}"
                                    f"\n↳ {normal_resolution.outcome.text}"
                                )

                        if danger_triggered:
                            encounter = pick_danger_encounter(key)
                            if encounter is not None:
                                _COOLDOWNS[(guild_id, user_id, key)] = now + float(effective_cd)

                                async def _resolve_danger(*, interaction: Optional[discord.Interaction], choice_key: str, timed_out: bool, view: DangerEncounterView) -> None:
                                    async with self.sessionmaker() as final_session:
                                        async with final_session.begin():
                                            final_wallet = await final_session.scalar(
                                                select(WalletRow).where(
                                                    WalletRow.guild_id == guild_id,
                                                    WalletRow.user_id == user_id,
                                                )
                                            )
                                            if final_wallet is None:
                                                final_wallet = WalletRow(guild_id=guild_id, user_id=user_id, silver=0, diamonds=0)
                                                final_session.add(final_wallet)
                                                await final_session.flush()

                                            resolution = resolve_danger_choice(encounter=encounter, choice_key=choice_key, payout=payout, job_xp=delta_job_xp)
                                            if timed_out:
                                                resolution = replace(resolution, timed_out=True)

                                            final_wallet.silver += int(resolution.payout)
                                            if hasattr(final_wallet, "silver_earned"):
                                                final_wallet.silver_earned += int(max(resolution.payout, 0))

                                            final_progress, final_leveled = await award_slot_job_xp(
                                                final_session,
                                                guild_id=guild_id,
                                                user_id=user_id,
                                                slot_index=int(active_slot.slot_index),
                                                job_key=key,
                                                amount=int(resolution.job_xp),
                                            )

                                            result_embed = build_danger_result_embed(
                                                user=interaction.user if interaction is not None else interaction_user,
                                                d=d,
                                                resolution=resolution,
                                                stamina_cost=effective_stamina_cost,
                                                user_xp=int(user_xp_gain),
                                                job_xp=int(resolution.job_xp),
                                                progress_after=final_progress,
                                                next_job_name=next_job_name,
                                                xp_needed_value=int(xp_needed(key, int(final_progress.level), int(final_progress.prestige))),
                                            )
                                            if final_leveled:
                                                result_embed.add_field(name="Milestone", value="⬆️ Leveled up from the danger encounter payout.", inline=False)

                                    if interaction is not None:
                                        await interaction.edit_original_response(embed=result_embed, view=view)
                                    elif view._message is not None:
                                        await view._message.edit(embed=result_embed, view=view)

                                interaction_user = interaction.user
                                danger_embed = build_danger_embed(user=interaction.user, d=d, encounter=encounter, payout=payout)
                                view = DangerEncounterView(
                                    owner_id=user_id,
                                    timeout_seconds=45.0,
                                    encounter=encounter,
                                    resolver=_resolve_danger,
                                )
                                sent_msg = await interaction.followup.send(embed=danger_embed, view=view, wait=True)
                                view.bind_message(sent_msg)
                                asyncio.create_task(
                                    self._check_and_announce_achievements(guild_id=guild_id, user_id=user_id),
                                    name="jobs.work.achievements",
                                )
                                return

                        if int(item_mods.double_payout_chance_bp) > 0 and roll_bp(int(item_mods.double_payout_chance_bp)):
                            did_double = True
                            before = payout
                            payout *= 2
                            item_bonus_total += max(0, payout - before)
                            await _consume_charge_from_group(
                                session,
                                guild_id=guild_id,
                                user_id=user_id,
                                group_key="double_payout",
                                amount=1,
                            )
                        combo_key = (guild_id, user_id)
                        if failed:
                            _WORK_COMBO_STREAK[combo_key] = 0
                        else:
                            combo_now = _WORK_COMBO_STREAK.get(combo_key, 0) + 1
                            combo_cap = int(item_mods.combo_max_stacks or 0)
                            if combo_cap > 0:
                                combo_now = min(combo_now, combo_cap)
                            _WORK_COMBO_STREAK[combo_key] = combo_now
                            if int(item_mods.combo_payout_step_bp) > 0 and combo_now > 1:
                                combo_bp = int(item_mods.combo_payout_step_bp) * (combo_now - 1)
                                before = payout
                                payout = apply_bp(payout, combo_bp)
                                item_bonus_total += max(0, payout - before)
                                action_text += f"\n🔥 **Combo x{combo_now}**: +{combo_bp / 100:.2f}% payout"

                        wallet.silver += int(payout)
                        if hasattr(wallet, "silver_earned"):
                            wallet.silver_earned += int(max(payout, 0))
                    if failed:
                        base_user_xp = user_xp_for_work(user_level=user_level, category=d.category)
                        adjusted_user_xp = apply_bp(base_user_xp, int(merged_effects.user_xp_bonus_bp))
                        user_xp_gain = apply_work_xp_multipliers(int(adjusted_user_xp))
                        await award_xp(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            amount=int(user_xp_gain),
                            apply_weekend_multiplier=False,
                        )
                        await increment_counter(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            counter_key="jobs_completed",
                            amount=1,
                        )

                        base_job_xp = job_xp_for_work(
                            job_level=job_level_now,
                            prestige=job_prestige_now,
                            category=d.category,
                        )
                        adjusted_job_xp = apply_bp(base_job_xp, int(merged_effects.job_xp_bonus_bp))
                        delta_job_xp = apply_work_xp_multipliers(int(adjusted_job_xp))
                    progress_after, leveled_up = await award_slot_job_xp(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                        slot_index=int(active_slot.slot_index),
                        job_key=key,
                        amount=int(delta_job_xp),
                    )

                    if int(item_mods.job_xp_progress_bp) > 0:
                        pct_xp = max((int(xp_needed(key, int(progress_after.level), int(progress_after.prestige))) * int(item_mods.job_xp_progress_bp)) // 10_000, 1)
                        progress_after, _ = await award_slot_job_xp(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            slot_index=int(active_slot.slot_index),
                            job_key=key,
                            amount=int(pct_xp),
                        )
                        await _consume_charge_from_group(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            group_key="job_level_gain",
                            amount=1,
                        )

                    if int(item_mods.job_level_gain) > 0:
                        level_xp = 0
                        tmp_level = int(progress_after.level)
                        tmp_prestige = int(progress_after.prestige)
                        tmp_into = int(progress_after.xp)
                        for _ in range(int(item_mods.job_level_gain)):
                            need = int(xp_needed(key, tmp_level, tmp_prestige))
                            level_xp += max(need - tmp_into, 1)
                            tmp_level += 1
                            tmp_into = 0
                        progress_after, _ = await award_slot_job_xp(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            slot_index=int(active_slot.slot_index),
                            job_key=key,
                            amount=int(level_xp),
                        )
                        await _consume_charge_from_group(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            group_key="job_level_gain",
                            amount=1,
                        )

                    if not failed:
                        drop_result = await roll_and_grant_work_drops(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            job_tier=d.category.value,
                            user_level=user_level,
                            prestige=job_prestige_now,
                            rare_find_bp=int(getattr(merged_effects, "rare_find_bp", 0)),
                            extra_roll_bp=int(getattr(merged_effects, "extra_roll_bp", 0)),
                            lootbox_drop_bp=int(item_mods.lootbox_drop_bp),
                            item_drop_bp=int(item_mods.item_drop_bp),
                        )

                    if proc_messages:
                        action_text += "\n" + "\n".join(proc_messages)

                    work_image_url = job_row_image_get(job_row)
                    next_job_name = d.name

                    user_xp_bonus = int(user_xp_gain) - int(base_user_xp)
                    job_xp_bonus = int(delta_job_xp) - int(base_job_xp)
                    final_component_sum = base_pay + item_bonus_total + tool_bonus_total + proc_bonus_total + event_bonus_total + other_bonus_total
                    other_bonus_total += int(payout) - int(final_component_sum)
                    result_payload = _WorkResultPayload(
                        action_text=action_text,
                        failed=failed,
                        payout=int(payout),
                        base_pay=max(int(base_pay), 0),
                        item_bonus=int(item_bonus_total),
                        tool_bonus=int(tool_bonus_total),
                        proc_bonus=int(proc_bonus_total),
                        event_bonus=int(event_bonus_total),
                        other_bonus=int(other_bonus_total),
                        stamina_base_cost=int(stamina_cost_base),
                        stamina_after_discount=int(sub_bp(stamina_cost_base, int(merged_effects.stamina_discount_bp))),
                        stamina_flat_delta=int(item_mods.stamina_cost_flat_delta),
                        stamina_final_cost=int(effective_stamina_cost),
                        tool_stamina_saved=tool_stamina_saved,
                        tool_stamina_save_chance_bp=int(tool_stamina_save_chance_bp),
                        user_xp=int(user_xp_gain),
                        user_xp_base=int(base_user_xp),
                        user_xp_bonus=int(user_xp_bonus),
                        job_xp=int(delta_job_xp),
                        job_xp_base=int(base_job_xp),
                        job_xp_bonus=int(job_xp_bonus),
                        job_title=title_for(key, int(progress_after.prestige)),
                        job_level=int(progress_after.level),
                        job_prestige=int(progress_after.prestige),
                        job_xp_into=int(progress_after.xp),
                        job_xp_need=int(xp_needed(key, int(progress_after.level), int(progress_after.prestige))),
                        did_double=did_double,
                        upgrade_level=upgrade_level,
                        upgrade_bonus_pct=upgrade_bonus_pct,
                        work_image_url=work_image_url,
                        leveled_up=bool(leveled_up),
                        prestiged=False,
                        next_job_name=next_job_name,
                        weekend_bonus_active=is_weekend(),
                        drop_result=drop_result,
                    )

                    embed = _build_work_embed(
                        user=interaction.user,
                        d=d,
                        payload=result_payload,
                        effects=merged_effects,
                        item_mods=item_mods,
                    )
                    result_effects = merged_effects
                    result_item_mods = item_mods
                    used_cooldown_seconds = effective_cd

            if key is not None and used_cooldown_seconds is not None:
                _COOLDOWNS[(guild_id, user_id, key)] = now + float(used_cooldown_seconds)

            if embed is not None and result_payload is not None and result_effects is not None and result_item_mods is not None:
                cooldown_ready_at = now + float(used_cooldown_seconds or 0.0)
                prestige_ready = bool(progress_after.level >= level_cap_for(progress_after.prestige))
                current_prestige = int(progress_after.prestige)
                view = _WorkUpgradeView(
                    sessionmaker=self.sessionmaker,
                    guild_id=guild_id,
                    user_id=user_id,
                    cooldown_ready_at=cooldown_ready_at,
                    active_slot_index=int(active_slot.slot_index),
                    job_key=key,
                    job_name=d.name,
                    prestige_ready=prestige_ready,
                    prestige_cost_value=prestige_cost(current_prestige),
                    current_earnings_multiplier=current_prestige + 1,
                    next_earnings_multiplier=current_prestige + 2,
                    job_def=d,
                    result_payload=result_payload,
                    result_effects=result_effects,
                    result_item_mods=result_item_mods,
                    requesting_user=interaction.user,
                )
                sent_msg = await interaction.followup.send(embed=embed, view=view, wait=True)
                view.bind_message(sent_msg)
                asyncio.create_task(
                    self._check_and_announce_achievements(guild_id=guild_id, user_id=user_id),
                    name="jobs.work.achievements",
                )
            else:
                await interaction.followup.send("Something went wrong generating the work result.", ephemeral=True)
        finally:
            if lock_acquired:
                work_lock.release()

    async def _check_and_announce_achievements(self, *, guild_id: int, user_id: int) -> None:
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    unlocks = await check_and_grant_achievements(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                    )
            if unlocks:
                queue_achievement_announcements(
                    bot=self.bot,
                    guild_id=guild_id,
                    user_id=user_id,
                    unlocks=unlocks,
                )
        except Exception:
            return


class _WorkUpgradeView(discord.ui.View):
    def __init__(
        self,
        *,
        sessionmaker,
        guild_id: int,
        user_id: int,
        cooldown_ready_at: float,
        active_slot_index: int,
        job_key: str,
        job_name: str,
        prestige_ready: bool,
        prestige_cost_value: int,
        current_earnings_multiplier: int,
        next_earnings_multiplier: int,
        job_def: JobDef,
        result_payload: _WorkResultPayload,
        result_effects: JobEffects,
        result_item_mods: _ItemMods,
        requesting_user: discord.abc.User,
    ):
        super().__init__(timeout=300)
        self.sessionmaker = sessionmaker
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.cooldown_ready_at = float(cooldown_ready_at)
        self.active_slot_index = int(active_slot_index)
        self.job_key = job_key
        self.job_name = job_name
        self.prestige_ready = bool(prestige_ready)
        self.prestige_cost_value = int(prestige_cost_value)
        self.current_earnings_multiplier = int(current_earnings_multiplier)
        self.next_earnings_multiplier = int(next_earnings_multiplier)
        self.job_def = job_def
        self.result_payload = result_payload
        self.result_effects = result_effects
        self.result_item_mods = result_item_mods
        self.requesting_user = requesting_user
        self.expanded = False
        self._message: Optional[discord.Message] = None
        self.work_again.disabled = (time.time() < self.cooldown_ready_at)
        if self.prestige_ready:
            self.add_item(_PrestigeReadyButton())

    def bind_message(self, message: discord.Message) -> None:
        self._message = message
        if self.work_again.disabled:
            asyncio.create_task(self._enable_work_when_ready())

    async def _enable_work_when_ready(self) -> None:
        wait_s = max(self.cooldown_ready_at - time.time(), 0.0)
        if wait_s > 0:
            await asyncio.sleep(wait_s)
        self.work_again.disabled = False
        if self._message is not None:
            try:
                await self._message.edit(view=self)
            except Exception:
                pass

    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True
        if self._message is not None:
            try:
                await self._message.edit(view=self)
            except Exception:
                pass

    def _sync_expand_button(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.custom_id == "work:expand_toggle":
                child.label = "Collapse" if self.expanded else "Expand"
                child.emoji = "🔽" if self.expanded else "🔎"
                break

    @discord.ui.button(label="Expand", style=discord.ButtonStyle.secondary, emoji="🔎", custom_id="work:expand_toggle")
    async def toggle_expand(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("This button only works in the original server.", ephemeral=True)
            return
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Only the user who ran /work can use this button.", ephemeral=True)
            return
        self.expanded = not self.expanded
        self._sync_expand_button()
        embed = _build_work_embed(
            user=self.requesting_user,
            d=self.job_def,
            payload=self.result_payload,
            effects=self.result_effects,
            item_mods=self.result_item_mods,
            expanded=self.expanded,
        )
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Upgrade Tool", style=discord.ButtonStyle.primary, emoji="⚙️")
    async def upgrade_tool(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("This button only works in the original server.", ephemeral=True)
            return

        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Only the user who ran /work can use this button.", ephemeral=True)
            return

        await interaction.response.send_message(
            "Confirm upgrade for your currently equipped job?",
            ephemeral=True,
            view=_UpgradeConfirmView(
                sessionmaker=self.sessionmaker,
                guild_id=self.guild_id,
                user_id=self.user_id,
                source_view=self,
            ),
        )

    @discord.ui.button(label="Job Hub", style=discord.ButtonStyle.secondary, emoji="🧰")
    async def open_job_hub_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("This button only works in the original server.", ephemeral=True)
            return

        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Only the user who ran /work can use this button.", ephemeral=True)
            return

        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]
        await open_job_hub(
            interaction=interaction,
            sessionmaker=self.sessionmaker,
            guild_id=self.guild_id,
            user_id=self.user_id,
            vip=vip,
            notice="Opened your Job Hub.",
        )

    @discord.ui.button(label="Work Again", style=discord.ButtonStyle.success, emoji="🔁")
    async def work_again(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("This button only works in the original server.", ephemeral=True)
            return

        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Only the user who ran /work can use this button.", ephemeral=True)
            return

        if button.disabled:
            left = max(int(self.cooldown_ready_at - time.time()), 0)
            await interaction.response.send_message(
                f"Cooldown. Try again in **{fmt_int(left)}s**.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)

        original_label = button.label
        button.disabled = True
        button.label = "Processing..."
        try:
            if interaction.message is not None:
                await interaction.message.edit(view=self)
        except Exception:
            button.disabled = False
            button.label = original_label

        cog = interaction.client.get_cog("WorkCog") if interaction.client else None
        cmd = getattr(cog, "work_cmd", None) if cog is not None else None
        if cog is None or cmd is None or not hasattr(cmd, "callback"):
            button.disabled = False
            button.label = original_label
            if interaction.message is not None:
                try:
                    await interaction.message.edit(view=self)
                except Exception:
                    pass
            await interaction.followup.send("Work command is currently unavailable.", ephemeral=True)
            return

        await cmd.callback(cog, interaction)




class _PrestigeReadyButton(discord.ui.Button["_WorkUpgradeView"]):
    def __init__(self):
        super().__init__(label="Prestige Ready", style=discord.ButtonStyle.primary, emoji="🟨")

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("This prestige button is no longer active.", ephemeral=True)
            return

        if interaction.guild is None or interaction.guild.id != view.guild_id:
            await interaction.response.send_message("This button only works in the original server.", ephemeral=True)
            return

        if interaction.user.id != view.user_id:
            await interaction.response.send_message("Only the user who ran /work can use this button.", ephemeral=True)
            return

        benefit_lines = [
            f"• Cost: **{fmt_int(view.prestige_cost_value)} Silver**",
            f"• Earnings multiplier: **x{view.current_earnings_multiplier} → x{view.next_earnings_multiplier}**",
            f"• Slot level resets: **Level 1**",
            f"• Keeps your job: **{view.job_name}**",
        ]
        await interaction.response.send_message(
            "Prestige confirmation for your active /work slot:\n" + "\n".join(benefit_lines),
            ephemeral=True,
            view=_PrestigeConfirmView(source_view=view),
        )



class _PrestigeConfirmView(discord.ui.View):
    def __init__(self, *, source_view: _WorkUpgradeView):
        super().__init__(timeout=60)
        self.source_view = source_view

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.source_view.user_id:
            await interaction.response.send_message("Only the user who ran /work can use this button.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Prestige", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_prestige(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.source_view.guild_id:
            await interaction.response.send_message("This button only works in the original server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]

        async with self.source_view.sessionmaker() as session:
            async with session.begin():
                snap = await get_slot_snapshot(
                    session,
                    guild_id=self.source_view.guild_id,
                    user_id=self.source_view.user_id,
                    vip=vip,
                    slot_index=self.source_view.active_slot_index,
                )
                if not snap.job_key or snap.job_key != self.source_view.job_key:
                    await interaction.followup.send("Your active /work slot changed jobs. Run /work again before prestiging.", ephemeral=True)
                    return

                if not snap.progress or snap.progress.level < snap.progress.level_cap:
                    await interaction.followup.send("This slot is no longer ready to prestige.", ephemeral=True)
                    return

                ok, message, _ = await prestige_slot(
                    session,
                    guild_id=self.source_view.guild_id,
                    user_id=self.source_view.user_id,
                    slot_index=self.source_view.active_slot_index,
                    job_key=self.source_view.job_key,
                    vip=vip,
                )
                if not ok:
                    await session.rollback()
                    await interaction.followup.send(message, ephemeral=True)
                    return

        self.source_view.prestige_ready = False
        for child in self.source_view.children:
            if isinstance(child, _PrestigeReadyButton):
                child.disabled = True
        if self.source_view._message is not None:
            try:
                await self.source_view._message.edit(view=self.source_view)
            except Exception:
                pass

        for child in self.children:
            child.disabled = True
        await interaction.edit_original_response(content=f"✅ {message}", view=self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="✖️")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Prestige cancelled.", view=self)

class _UpgradeConfirmView(discord.ui.View):
    def __init__(self, *, sessionmaker, guild_id: int, user_id: int, source_view: _WorkUpgradeView):
        super().__init__(timeout=30)
        self.sessionmaker = sessionmaker
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.source_view = source_view

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Only the user who ran /work can use this button.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]

        async with self.sessionmaker() as session:
            async with session.begin():
                active_slot = await get_active_slot(session, guild_id=self.guild_id, user_id=self.user_id, vip=vip)
                slot_snap = await get_slot_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.user_id,
                    vip=vip,
                    slot_index=int(active_slot.slot_index),
                )
                if not slot_snap.job_key or not slot_snap.selected_tool_key:
                    await interaction.response.send_message(
                        "Assign a job and select a tool in **/job** before upgrading.",
                        ephemeral=True,
                    )
                    return

                ok, message = await buy_or_upgrade_tool(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.user_id,
                    slot_index=int(active_slot.slot_index),
                    job_key=slot_snap.job_key,
                    tool_key=slot_snap.selected_tool_key,
                )

        if not ok:
            await interaction.response.send_message(message, ephemeral=True)
            return

        self.confirm.disabled = True
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.label == "Cancel":
                child.disabled = True
        await interaction.response.edit_message(content=f"✅ {message}", view=self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="✖️")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Upgrade cancelled.", view=self)
