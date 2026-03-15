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

from services.jobs_balance import job_xp_for_work, stamina_cost_for_work, user_xp_for_work
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
    get_job_snapshot,
    get_level,
    get_or_create_job_row,
    job_row_image_get,
    roll_bp,
    sub_bp,
    tier_for_category,
    award_job_xp,
)
from .jobs import get_job_def
from services.job_upgrades import apply_income_upgrade, get_upgrade_level, upgrade_once

# -------------------------
# Cooldowns
# -------------------------
_COOLDOWNS: Dict[Tuple[int, int, str], float] = {}


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


def _effective_work_cooldown_seconds(*, base_seconds: float, work_level: int) -> float:
    # 1% cooldown reduction per work level, capped at 50% reduction.
    reduction_bp = clamp_int(max(int(work_level), 1) - 1, 0, 50) * 100
    scale = max(0.5, 1.0 - (reduction_bp / 10_000.0))
    return max(1.0, float(base_seconds) * scale)


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
    user_xp_bonus_bp: int = 0
    double_payout_chance_bp: int = 0


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
    user_xp_bonus_bp = 0
    double_payout_chance_bp = 0

    for r in rows.scalars():
        payload = r.payload_json
        payout_bonus_bp += _int_payload(payload, "payout_bonus_bp")
        fail_reduction_bp += _int_payload(payload, "fail_reduction_bp")
        stamina_discount_bp += _int_payload(payload, "stamina_discount_bp")
        stamina_cost_flat_delta += _int_payload(payload, "stamina_cost_flat_delta")
        job_xp_bonus_bp += _int_payload(payload, "job_xp_bonus_bp")
        user_xp_bonus_bp += _int_payload(payload, "user_xp_bonus_bp")
        double_payout_chance_bp += _int_payload(payload, "double_payout_chance_bp")

    payout_bonus_bp = clamp_int(payout_bonus_bp, -10_000, 50_000)
    fail_reduction_bp = clamp_int(fail_reduction_bp, 0, 10_000)
    stamina_discount_bp = clamp_int(stamina_discount_bp, 0, 10_000)
    stamina_cost_flat_delta = clamp_int(stamina_cost_flat_delta, -10, 10)
    job_xp_bonus_bp = clamp_int(job_xp_bonus_bp, -10_000, 50_000)
    user_xp_bonus_bp = clamp_int(user_xp_bonus_bp, -10_000, 50_000)
    double_payout_chance_bp = clamp_int(double_payout_chance_bp, 0, 10_000)

    return _ItemMods(
        payout_bonus_bp=payout_bonus_bp,
        fail_reduction_bp=fail_reduction_bp,
        stamina_discount_bp=stamina_discount_bp,
        stamina_cost_flat_delta=stamina_cost_flat_delta,
        job_xp_bonus_bp=job_xp_bonus_bp,
        user_xp_bonus_bp=user_xp_bonus_bp,
        double_payout_chance_bp=double_payout_chance_bp,
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


def _build_work_embed(
    *,
    user: discord.abc.User,
    d: JobDef,
    action_text: str,
    failed: bool,
    payout: int,
    stamina_cost: int,
    user_xp: int,
    job_xp: int,
    job_title: str,
    job_level: int,
    job_prestige: int,
    job_xp_into: int,
    job_xp_need: int,
    effects: JobEffects,
    item_mods: _ItemMods,
    did_double: bool,
    upgrade_level: int,
    upgrade_bonus_pct: int,
    work_image_url: Optional[str],
    leveled_up: bool,
    prestiged: bool,
) -> discord.Embed:
    color = _work_color(d.category)

    outcome_emoji = "❌" if failed else "✅"
    outcome_word = "FAILED" if failed else "SUCCESS"

    header_line = (
        f"{outcome_emoji} **{outcome_word}**  "
        f"• 💰 **{fmt_int(payout)}** Silver  "
        f"• ⚡ **-{fmt_int(stamina_cost)}** Stamina"
    )

    if did_double:
        header_line += "  • 🪙 **2x payout!**"

    prog_bar = bar(job_xp_into, job_xp_need)
    prog_pct = _pct(job_xp_into, job_xp_need)

    prog_block = (
        f"**P{fmt_int(job_prestige)}** • **{job_title}**\n"
        f"Level **{fmt_int(job_level)}**  "
        f"[{prog_bar}]  **{prog_pct}%**"
    )

    gain_block = (
        f"🧠 User XP: **+{fmt_int(user_xp)}**\n"
        f"🧰 Job XP: **+{fmt_int(job_xp)}**\n"
        f"⚙️ Upgrade: **Lv {fmt_int(upgrade_level)}** (**+{fmt_int(upgrade_bonus_pct)}% income**)"
    )

    notes: list[str] = []
    if prestiged:
        notes.append("✨ Prestiged, new title unlocked")
    elif leveled_up:
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
    if int(effects.user_xp_bonus_bp) != 0:
        eff_lines.append(f"🧠 User XP: **+{effects.user_xp_bonus_bp / 100:.2f}%**")
    if int(getattr(effects, "extra_roll_bp", 0)) != 0:
        eff_lines.append(f"🎲 Extra roll chance: **{int(getattr(effects, 'extra_roll_bp', 0)) / 100:.2f}%**")
    if int(getattr(effects, "rare_find_bp", 0)) != 0:
        eff_lines.append(f"✨ Rare find chance: **{int(getattr(effects, 'rare_find_bp', 0)) / 100:.2f}%**")

    if int(item_mods.stamina_cost_flat_delta) != 0:
        eff_lines.append(f"⚡ Stamina cost: **{item_mods.stamina_cost_flat_delta:+d}** (flat)")
    if int(item_mods.double_payout_chance_bp) != 0:
        eff_lines.append(f"🪙 2x payout chance: **{item_mods.double_payout_chance_bp / 100:.2f}%**")

    avatar_url = getattr(getattr(user, "display_avatar", None), "url", None)

    embed = discord.Embed(
        title=f"{d.name} Work Result",
        description=f"{action_text}\n\n{header_line}",
        color=color,
    )

    embed.set_author(name=str(user), icon_url=avatar_url)

    embed.add_field(name="Job Progress", value=prog_block, inline=False)
    embed.add_field(name="Gains", value=gain_block, inline=True)

    if notes:
        embed.add_field(name="Milestone", value="\n".join(notes), inline=True)

    if eff_lines:
        embed.add_field(name="Bonuses Active", value="\n".join(eff_lines), inline=False)

    if work_image_url:
        embed.set_image(url=work_image_url)

    embed.set_footer(text="Use /job to switch jobs")
    return embed


class WorkCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.stamina = StaminaService()

    @app_commands.command(name="work", description="Work your equipped job.")
    async def work_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        guild_id = interaction.guild.id
        user_id = interaction.user.id
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]
        now = time.time()

        await interaction.response.defer(thinking=True)

        embed: Optional[discord.Embed] = None
        key: Optional[str] = None
        used_cooldown_seconds: Optional[float] = None
        unlocked_achievements = []

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                key = await get_equipped_key(session, guild_id=guild_id, user_id=user_id)
                if not key:
                    await interaction.followup.send(
                        "You don’t have a job equipped yet.\nUse **/job** to open the panel.",
                        ephemeral=True,
                    )
                    return

                d = get_job_def(key)
                if d is None:
                    await interaction.followup.send(
                        "Your equipped job no longer exists. Use **/job** to pick a new one.",
                        ephemeral=True,
                    )
                    return

                if d.vip_only and not vip:
                    await interaction.followup.send(
                        "Your equipped job is VIP-locked and you’re not VIP. Pick another with **/job**.",
                        ephemeral=True,
                    )
                    return

                job_row = await get_or_create_job_row(session, job_key=key)
                if not bool(getattr(job_row, "enabled", True)):
                    await interaction.followup.send(f"Job `{key}` is disabled in DB.", ephemeral=True)
                    return

                user_level = await get_level(session, guild_id=guild_id, user_id=user_id)

                need_unlock = 1
                if d.category == JobCategory.STABLE:
                    need_unlock = 40
                elif d.category == JobCategory.HARD:
                    need_unlock = 60

                if (not vip) and user_level < need_unlock:
                    await interaction.followup.send(f"🔒 **{d.name}** unlocks at **Level {need_unlock}**.", ephemeral=True)
                    return

                if not d.actions:
                    await interaction.followup.send("This job has no actions configured.", ephemeral=True)
                    return

                tier = tier_for_category(d.category)

                snap_before = await get_job_snapshot(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    job_id=int(getattr(job_row, "id")),
                    job_key=key,
                    tier=tier,
                    extras=None,
                )
                effective_cd = _effective_work_cooldown_seconds(
                    base_seconds=float(d.cooldown_seconds),
                    work_level=int(snap_before.level),
                )
                cd_key = (guild_id, user_id, key)
                ready_at = _COOLDOWNS.get(cd_key, 0.0)
                if ready_at > now:
                    left = int(max(ready_at - now, 0))
                    await interaction.followup.send(f"Cooldown. Try again in **{fmt_int(left)}s**.", ephemeral=True)
                    return

                job_effects = await compute_effects_from_upgrades_and_items(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    job_id=int(getattr(job_row, "id")),
                    prestige=int(snap_before.prestige),
                    level=int(snap_before.level),
                    inv=NoopInventoryAdapter(),
                )
                job_effects = job_effects.clamp()

                item_mods = await _get_item_mods(session, guild_id=guild_id, user_id=user_id)

                merged_effects = replace(
                    job_effects,
                    payout_bonus_bp=clamp_int(int(job_effects.payout_bonus_bp) + int(item_mods.payout_bonus_bp), -10_000, 50_000),
                    fail_reduction_bp=clamp_int(int(job_effects.fail_reduction_bp) + int(item_mods.fail_reduction_bp), 0, 10_000),
                    stamina_discount_bp=clamp_int(int(job_effects.stamina_discount_bp) + int(item_mods.stamina_discount_bp), 0, 10_000),
                    job_xp_bonus_bp=clamp_int(int(job_effects.job_xp_bonus_bp) + int(item_mods.job_xp_bonus_bp), -10_000, 50_000),
                    user_xp_bonus_bp=clamp_int(int(job_effects.user_xp_bonus_bp) + int(item_mods.user_xp_bonus_bp), -10_000, 50_000),
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

                ok, stam_snap = await self.stamina.try_spend(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    cost=stamina_cost,
                    is_vip=vip,
                )
                if not ok:
                    await interaction.followup.send(
                        f"Not enough stamina. You have **{fmt_int(stam_snap.current)}/{fmt_int(stam_snap.max)}**.",
                        ephemeral=True,
                    )
                    return

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

                success_actions, fail_actions = _split_actions(d)

                fail_bp = category_fail_bp(d.category, d.fail_chance_bp)
                fail_bp = max(int(fail_bp) - int(merged_effects.fail_reduction_bp), 0)
                fail_bp = clamp_int(fail_bp, 0, 10_000)

                failed = roll_bp(fail_bp) if fail_bp > 0 else False
                extra_roll = roll_bp(int(getattr(merged_effects, "extra_roll_bp", 0))) if int(getattr(merged_effects, "extra_roll_bp", 0)) > 0 else False

                payout = 0
                action_text = ""
                did_double = False
                upgrade_level = 0
                upgrade_bonus_pct = 0

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

                    def _roll_payout_once() -> int:
                        raw = random.randint(max(lo, 0), max(hi, 0))

                        if int(getattr(merged_effects, "rare_find_bp", 0)) > 0 and roll_bp(int(getattr(merged_effects, "rare_find_bp", 0))):
                            bump = max(int(round(raw * 0.25)), 1)
                            raw += bump

                        if d.bonus_chance_bp > 0 and d.bonus_multiplier > 1.0 and roll_bp(d.bonus_chance_bp):
                            raw = int(round(raw * float(d.bonus_multiplier)))

                        raw = apply_bp(raw, int(merged_effects.payout_bonus_bp))
                        return max(int(raw), 0)

                    p1 = _roll_payout_once()
                    payout = max(p1, _roll_payout_once()) if extra_roll else p1

                    upgrade_level = await get_upgrade_level(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                        job_id=int(getattr(job_row, "id")),
                    )
                    payout = apply_income_upgrade(payout, upgrade_level)
                    upgrade_bonus_pct = max(int(upgrade_level) * 25, 0)

                    if int(item_mods.double_payout_chance_bp) > 0 and roll_bp(int(item_mods.double_payout_chance_bp)):
                        did_double = True
                        payout *= 2
                        await _consume_charge_from_group(
                            session,
                            guild_id=guild_id,
                            user_id=user_id,
                            group_key="double_payout",
                            amount=1,
                        )

                    wallet.silver += int(payout)
                    if hasattr(wallet, "silver_earned"):
                        wallet.silver_earned += int(max(payout, 0))

                base_user_xp = user_xp_for_work(user_level=user_level, category=d.category)
                user_xp_gain = apply_bp(base_user_xp, int(merged_effects.user_xp_bonus_bp))
                await award_xp(session, guild_id=guild_id, user_id=user_id, amount=int(user_xp_gain))
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
                award_res = await award_job_xp(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    job_id=int(getattr(job_row, "id")),
                    job_key=key,
                    tier=tier,
                    base_xp=int(base_job_xp),
                    extras=_WorkEffectsAdapter(effects=merged_effects),
                )

                snap_after = award_res.snapshot
                delta = award_res.delta

                work_image_url = job_row_image_get(job_row)

                embed = _build_work_embed(
                    user=interaction.user,
                    d=d,
                    action_text=action_text,
                    failed=failed,
                    payout=payout,
                    stamina_cost=stamina_cost,
                    user_xp=int(user_xp_gain),
                    job_xp=int(delta.xp_gained),
                    job_title=str(snap_after.title),
                    job_level=int(snap_after.level),
                    job_prestige=int(snap_after.prestige),
                    job_xp_into=int(snap_after.xp_into_level),
                    job_xp_need=int(snap_after.xp_needed),
                    effects=merged_effects,
                    item_mods=item_mods,
                    did_double=did_double,
                    upgrade_level=upgrade_level,
                    upgrade_bonus_pct=upgrade_bonus_pct,
                    work_image_url=work_image_url,
                    leveled_up=bool(delta.leveled_up),
                    prestiged=bool(delta.prestiged),
                )
                unlocked_achievements = await check_and_grant_achievements(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )
                used_cooldown_seconds = effective_cd

        if key is not None and used_cooldown_seconds is not None:
            _COOLDOWNS[(guild_id, user_id, key)] = now + float(used_cooldown_seconds)

        if embed is not None:
            cooldown_ready_at = now + float(used_cooldown_seconds or 0.0)
            view = _WorkUpgradeView(
                sessionmaker=self.sessionmaker,
                guild_id=guild_id,
                user_id=user_id,
                cooldown_ready_at=cooldown_ready_at,
            )
            sent_msg = await interaction.followup.send(embed=embed, view=view, wait=True)
            view.bind_message(sent_msg)
            if unlocked_achievements:
                queue_achievement_announcements(
                    bot=self.bot,
                    guild_id=guild_id,
                    user_id=user_id,
                    unlocks=unlocked_achievements,
                )
        else:
            await interaction.followup.send("Something went wrong generating the work result.", ephemeral=True)


class _WorkUpgradeView(discord.ui.View):
    def __init__(self, *, sessionmaker, guild_id: int, user_id: int, cooldown_ready_at: float):
        super().__init__(timeout=300)
        self.sessionmaker = sessionmaker
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.cooldown_ready_at = float(cooldown_ready_at)
        self._message: Optional[discord.Message] = None
        self.work_again.disabled = (time.time() < self.cooldown_ready_at)

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
        self.upgrade_tool.disabled = True
        self.work_again.disabled = True
        if self._message is not None:
            try:
                await self._message.edit(view=self)
            except Exception:
                pass

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

        cog = interaction.client.get_cog("WorkCog") if interaction.client else None
        cmd = getattr(cog, "work_cmd", None) if cog is not None else None
        if cog is None or cmd is None or not hasattr(cmd, "callback"):
            await interaction.response.send_message("Work command is currently unavailable.", ephemeral=True)
            return

        await cmd.callback(cog, interaction)


class _UpgradeConfirmView(discord.ui.View):
    def __init__(self, *, sessionmaker, guild_id: int, user_id: int, source_view: _WorkUpgradeView):
        super().__init__(timeout=30)
        self.sessionmaker = sessionmaker
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.source_view = source_view

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("This button only works in the original server.", ephemeral=True)
            return

        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Only the user who ran /work can use this button.", ephemeral=True)
            return

        self.source_view.upgrade_tool.disabled = True
        if self.source_view._message is not None:
            try:
                await self.source_view._message.edit(view=self.source_view)
            except Exception:
                pass

        await interaction.response.defer(ephemeral=True, thinking=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.user_id)
                key = await get_equipped_key(session, guild_id=self.guild_id, user_id=self.user_id)
                if not key:
                    await interaction.followup.send("You don’t have a job equipped. Use **/job** first.", ephemeral=True)
                    return

                d = get_job_def(key)
                if d is None:
                    await interaction.followup.send(
                        "Your equipped job no longer exists. Re-equip with **/job**.",
                        ephemeral=True,
                    )
                    return

                if d.vip_only and not is_vip_member(interaction.user):
                    await interaction.followup.send(
                        "Your equipped job is VIP-locked and you’re not VIP. Pick another with **/job**.",
                        ephemeral=True,
                    )
                    return

                job_row = await get_or_create_job_row(session, job_key=key, name=d.name)
                if not bool(getattr(job_row, "enabled", True)):
                    await interaction.followup.send(f"Job `{key}` is disabled in DB.", ephemeral=True)
                    return

                ok, result_text, snap = await upgrade_once(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.user_id,
                    job_row=job_row,
                    job_def=d,
                )

        if not ok:
            self.source_view.upgrade_tool.disabled = False
            if self.source_view._message is not None:
                try:
                    await self.source_view._message.edit(view=self.source_view)
                except Exception:
                    pass
            await interaction.followup.send(f"❌ {result_text}", ephemeral=True)
            return

        await interaction.followup.send(
            "✅ "
            f"{result_text}\n"
            f"Next upgrade cost: **{fmt_int(snap.next_cost)}** silver.",
            ephemeral=True,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Upgrade cancelled.", ephemeral=True)
