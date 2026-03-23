from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from sqlalchemy import select

from db.models import JobRow, UserJobHubProgressRow, UserJobHubSlotRow, UserJobHubToolRow, UserJobSlotRow, WalletRow
from services.job_progression import level_cap_for, total_xp_from_state, xp_needed_for_level
from services.jobs_balance import payout_for_work, prestige_cost, stamina_cost_for_work
from services.jobs_core import JOB_DEFS, JOB_SWITCH_COST, fmt_int, tier_for_category
from services.jobs_endgame import event_defs_for_endgame

MAX_JOB_HUB_SLOTS = 3
DEFAULT_UNLOCKED_SLOTS = 2
VIP_UNLOCKED_SLOTS = 3
JOB_HUB_SWITCH_COOLDOWN_SECONDS = 30

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ToolDefinition:
    key: str
    name: str
    cost: int
    income_bonus_bp: int = 0
    xp_bonus_bp: int = 0
    stamina_discount_bp: int = 0
    description: str = ""


@dataclass(frozen=True)
class PerkDefinition:
    key: str
    level_required: int
    name: str
    description: str
    event_weight_bonus_bp: int = 0


@dataclass(frozen=True)
class RandomEventDefinition:
    key: str
    name: str
    description: str
    chance_bp: int
    payout_multiplier_bp: int = 0
    bonus_silver_flat: int = 0
    stamina_delta: int = 0
    fail_override: bool | None = None


@dataclass(frozen=True)
class SlotProgress:
    level: int
    prestige: int
    xp: int
    total_xp: int
    xp_needed: int
    level_cap: int


@dataclass(frozen=True)
class SlotSnapshot:
    slot_index: int
    is_active: bool
    is_unlocked: bool
    job_key: str | None
    progress: SlotProgress | None
    tool_levels: dict[str, int]
    selected_tool_key: str | None


TOOL_CATALOG: dict[str, tuple[ToolDefinition, ...]] = {
    "default": (
        ToolDefinition("kit_basic", "Basic Kit", 0, description="Standard work gear."),
        ToolDefinition("kit_refined", "Refined Kit", 750, income_bonus_bp=1200, xp_bonus_bp=600, description="Better output and cleaner reps."),
        ToolDefinition("kit_elite", "Elite Kit", 2500, income_bonus_bp=2500, xp_bonus_bp=1200, stamina_discount_bp=800, description="Premium gear for efficient work."),
    ),
    "bounty_hunter": (
        ToolDefinition("tracker_pad", "Tracker Pad", 0, description="Baseline intel package."),
        ToolDefinition("target_scope", "Target Scope", 1400, income_bonus_bp=1500, xp_bonus_bp=700, description="Boosts contract precision and pay."),
        ToolDefinition("hunter_rig", "Hunter Rig", 4200, income_bonus_bp=3000, xp_bonus_bp=1500, stamina_discount_bp=1000, description="High-end pursuit rig for elite captures."),
    ),
}

PERK_CATALOG: dict[str, tuple[PerkDefinition, ...]] = {
    "default": (
        PerkDefinition("steady_hands", 5, "Steady Hands", "Unlocks cleaner shifts with better consistency."),
        PerkDefinition("momentum", 10, "Momentum", "Unlocks momentum bursts that can increase rewards during /work."),
        PerkDefinition("mastercraft", 20, "Mastercraft", "Unlocks rare premium contracts for the slot."),
    ),
    "bounty_hunter": (
        PerkDefinition("street_whispers", 5, "Street Whispers", "Unlocks random encounters in /work.", event_weight_bonus_bp=600),
        PerkDefinition("high_value_mark", 10, "High-Value Mark", "Adds chances for doubled contract payouts.", event_weight_bonus_bp=1000),
        PerkDefinition("risky_capture", 20, "Risky Capture", "Unlocks high risk / high reward bounty scenarios.", event_weight_bonus_bp=1600),
    ),
}

EVENT_CATALOG: dict[str, tuple[RandomEventDefinition, ...]] = {
    "default": (
        RandomEventDefinition("lucky_tip", "Lucky Tip", "A client tips extra for excellent work.", 650, bonus_silver_flat=20),
        RandomEventDefinition("rush_order", "Rush Order", "A rush order pushes your payout higher.", 450, payout_multiplier_bp=2000),
    ),
    "bounty_hunter": (
        RandomEventDefinition("bonus_mission", "Bonus Mission", "A bonus mission appears mid-hunt.", 800, payout_multiplier_bp=2500),
        RandomEventDefinition("double_contract", "Double Contract", "Two targets share the same trail.", 500, payout_multiplier_bp=10000),
        RandomEventDefinition("dangerous_showdown", "Dangerous Showdown", "A dangerous showdown can either tank or spike your run.", 300, payout_multiplier_bp=5000, stamina_delta=1, fail_override=False),
    ),
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def unlocked_slot_count(*, vip: bool) -> int:
    return VIP_UNLOCKED_SLOTS if vip else DEFAULT_UNLOCKED_SLOTS


def slot_label(slot_index: int) -> str:
    return f"Slot {slot_index + 1}"


def tool_defs_for(job_key: str | None) -> tuple[ToolDefinition, ...]:
    if not job_key:
        return TOOL_CATALOG["default"]
    return TOOL_CATALOG.get(job_key, TOOL_CATALOG["default"])


def perk_defs_for(job_key: str | None) -> tuple[PerkDefinition, ...]:
    if not job_key:
        return PERK_CATALOG["default"]
    return PERK_CATALOG.get(job_key, PERK_CATALOG["default"])


def event_defs_for(job_key: str | None) -> tuple[RandomEventDefinition, ...]:
    if not job_key:
        return EVENT_CATALOG["default"]
    endgame_defs = event_defs_for_endgame(job_key)
    if endgame_defs:
        return endgame_defs
    return EVENT_CATALOG.get(job_key, EVENT_CATALOG["default"])


def xp_needed(job_key: str, level: int, prestige: int) -> int:
    job = JOB_DEFS[job_key]
    return xp_needed_for_level(tier=tier_for_category(job.category), prestige=prestige, level=level)


async def ensure_job_hub_slots(session, *, guild_id: int, user_id: int, vip: bool) -> list[UserJobHubSlotRow]:
    rows = list((await session.execute(
        select(UserJobHubSlotRow)
        .where(UserJobHubSlotRow.guild_id == guild_id, UserJobHubSlotRow.user_id == user_id)
        .order_by(UserJobHubSlotRow.slot_index.asc())
    )).scalars())

    if not rows:
        legacy = list((await session.execute(
            select(UserJobSlotRow)
            .where(UserJobSlotRow.guild_id == guild_id, UserJobSlotRow.user_id == user_id)
            .order_by(UserJobSlotRow.slot_index.asc())
        )).scalars())
        legacy_keys: list[str | None] = [None] * MAX_JOB_HUB_SLOTS
        for slot in legacy:
            if 0 <= int(slot.slot_index) < MAX_JOB_HUB_SLOTS:
                job = await session.get(JobRow, int(slot.job_id))
                legacy_keys[int(slot.slot_index)] = getattr(job, "key", None)

        for idx in range(MAX_JOB_HUB_SLOTS):
            row = UserJobHubSlotRow(
                guild_id=guild_id,
                user_id=user_id,
                slot_index=idx,
                is_unlocked=idx < unlocked_slot_count(vip=vip),
                is_active=idx == 0,
                job_key=legacy_keys[idx],
                selected_tool_key=None,
                last_switched_at=None,
            )
            session.add(row)
        await session.flush()
        rows = list((await session.execute(
            select(UserJobHubSlotRow)
            .where(UserJobHubSlotRow.guild_id == guild_id, UserJobHubSlotRow.user_id == user_id)
            .order_by(UserJobHubSlotRow.slot_index.asc())
        )).scalars())

    unlocks = unlocked_slot_count(vip=vip)
    for idx, row in enumerate(rows):
        row.is_unlocked = idx < unlocks
    if not any(bool(r.is_active) for r in rows):
        rows[0].is_active = True
    return rows


async def get_active_slot(session, *, guild_id: int, user_id: int, vip: bool) -> UserJobHubSlotRow:
    rows = await ensure_job_hub_slots(session, guild_id=guild_id, user_id=user_id, vip=vip)
    for row in rows:
        if row.is_active:
            return row
    rows[0].is_active = True
    return rows[0]


async def set_active_slot(session, *, guild_id: int, user_id: int, vip: bool, slot_index: int) -> UserJobHubSlotRow:
    rows = await ensure_job_hub_slots(session, guild_id=guild_id, user_id=user_id, vip=vip)
    target = rows[slot_index]
    if not target.is_unlocked:
        raise ValueError("slot_locked")
    for row in rows:
        row.is_active = row.slot_index == slot_index
    return target


async def get_or_create_progress(session, *, guild_id: int, user_id: int, slot_index: int, job_key: str) -> UserJobHubProgressRow:
    row = (await session.execute(
        select(UserJobHubProgressRow).where(
            UserJobHubProgressRow.guild_id == guild_id,
            UserJobHubProgressRow.user_id == user_id,
            UserJobHubProgressRow.slot_index == slot_index,
            UserJobHubProgressRow.job_key == job_key,
        )
    )).scalar_one_or_none()
    if row is None:
        row = UserJobHubProgressRow(guild_id=guild_id, user_id=user_id, slot_index=slot_index, job_key=job_key)
        session.add(row)
        await session.flush()
    return row


async def get_slot_snapshot(session, *, guild_id: int, user_id: int, vip: bool, slot_index: int) -> SlotSnapshot:
    rows = await ensure_job_hub_slots(session, guild_id=guild_id, user_id=user_id, vip=vip)
    slot = rows[slot_index]
    progress = None
    if slot.job_key:
        prog = await get_or_create_progress(session, guild_id=guild_id, user_id=user_id, slot_index=slot_index, job_key=slot.job_key)
        progress = SlotProgress(level=prog.level, prestige=prog.prestige, xp=prog.xp, total_xp=prog.total_xp, xp_needed=xp_needed(slot.job_key, prog.level, prog.prestige), level_cap=level_cap_for(prog.prestige))
    tool_rows = list((await session.execute(
        select(UserJobHubToolRow).where(
            UserJobHubToolRow.guild_id == guild_id,
            UserJobHubToolRow.user_id == user_id,
            UserJobHubToolRow.slot_index == slot_index,
            UserJobHubToolRow.job_key == slot.job_key,
        )
    )).scalars()) if slot.job_key else []
    return SlotSnapshot(slot_index=slot_index, is_active=bool(slot.is_active), is_unlocked=bool(slot.is_unlocked), job_key=slot.job_key, progress=progress, tool_levels={row.tool_key: row.level for row in tool_rows}, selected_tool_key=slot.selected_tool_key)


async def assign_job_to_slot(session, *, guild_id: int, user_id: int, vip: bool, slot_index: int, job_key: str) -> tuple[bool, int]:
    rows = await ensure_job_hub_slots(session, guild_id=guild_id, user_id=user_id, vip=vip)
    slot = rows[slot_index]
    if not slot.is_unlocked:
        raise ValueError("slot_locked")
    current = slot.job_key
    if current == job_key:
        return False, 0
    d = JOB_DEFS[job_key]
    cost = 0 if current is None else JOB_SWITCH_COST[d.category]
    slot.job_key = job_key
    slot.selected_tool_key = tool_defs_for(job_key)[0].key
    slot.last_switched_at = utc_now()
    await get_or_create_progress(session, guild_id=guild_id, user_id=user_id, slot_index=slot_index, job_key=job_key)
    return True, cost


async def get_wallet(session, *, guild_id: int, user_id: int) -> WalletRow:
    wallet = (await session.execute(select(WalletRow).where(WalletRow.guild_id == guild_id, WalletRow.user_id == user_id))).scalar_one_or_none()
    if wallet is None:
        wallet = WalletRow(guild_id=guild_id, user_id=user_id, silver=0, diamonds=0)
        session.add(wallet)
        await session.flush()
    return wallet


async def get_or_create_tool_row(session, *, guild_id: int, user_id: int, slot_index: int, job_key: str, tool_key: str) -> UserJobHubToolRow:
    row = (await session.execute(select(UserJobHubToolRow).where(
        UserJobHubToolRow.guild_id == guild_id,
        UserJobHubToolRow.user_id == user_id,
        UserJobHubToolRow.slot_index == slot_index,
        UserJobHubToolRow.job_key == job_key,
        UserJobHubToolRow.tool_key == tool_key,
    ))).scalar_one_or_none()
    if row is None:
        row = UserJobHubToolRow(guild_id=guild_id, user_id=user_id, slot_index=slot_index, job_key=job_key, tool_key=tool_key, level=0)
        session.add(row)
        await session.flush()
    return row


async def buy_or_upgrade_tool(session, *, guild_id: int, user_id: int, slot_index: int, job_key: str, tool_key: str) -> tuple[bool, str]:
    defs = {tool.key: tool for tool in tool_defs_for(job_key)}
    tool = defs[tool_key]
    row = await get_or_create_tool_row(session, guild_id=guild_id, user_id=user_id, slot_index=slot_index, job_key=job_key, tool_key=tool_key)
    next_level = row.level + 1
    cost = tool.cost * next_level
    wallet = await get_wallet(session, guild_id=guild_id, user_id=user_id)
    if int(wallet.silver) < cost:
        return False, f"Need **{fmt_int(cost)}** Silver to upgrade **{tool.name}** to Lv {next_level}."
    wallet.silver -= cost
    wallet.silver_spent += cost
    row.level = next_level
    rows = await ensure_job_hub_slots(session, guild_id=guild_id, user_id=user_id, vip=True)
    rows[slot_index].selected_tool_key = tool_key
    return True, f"Upgraded **{tool.name}** to **Lv {next_level}** for **{fmt_int(cost)}** Silver."


async def set_selected_tool(session, *, guild_id: int, user_id: int, vip: bool, slot_index: int, tool_key: str) -> None:
    rows = await ensure_job_hub_slots(session, guild_id=guild_id, user_id=user_id, vip=vip)
    rows[slot_index].selected_tool_key = tool_key


def tool_bonus_snapshot(job_key: str, selected_tool_key: str | None, tool_levels: dict[str, int]) -> tuple[int, int, int, str | None]:
    defs = {tool.key: tool for tool in tool_defs_for(job_key)}
    if not selected_tool_key or selected_tool_key not in defs:
        return 0, 0, 0, None
    tool = defs[selected_tool_key]
    level = max(int(tool_levels.get(selected_tool_key, 0)), 0)
    return tool.income_bonus_bp * level, tool.xp_bonus_bp * level, tool.stamina_discount_bp * level, tool.name


def unlocked_perks(job_key: str, level: int) -> tuple[list[PerkDefinition], list[PerkDefinition]]:
    unlocked: list[PerkDefinition] = []
    locked: list[PerkDefinition] = []
    for perk in perk_defs_for(job_key):
        (unlocked if level >= perk.level_required else locked).append(perk)
    return unlocked, locked


def income_range_for(job_key: str, level: int, prestige: int, income_bonus_bp: int) -> tuple[int, int]:
    d = JOB_DEFS[job_key]
    lows = []
    highs = []
    for action in d.actions:
        if action.can_fail and action.min_silver == 0 and action.max_silver == 0:
            continue
        lows.append(int(action.min_silver))
        highs.append(int(action.max_silver))
    lo = payout_for_work(base_payout=min(lows or [0]), job_key=job_key, job_level=level, prestige=prestige)
    hi = payout_for_work(base_payout=max(highs or [0]), job_key=job_key, job_level=level, prestige=prestige)
    lo = max((lo * (10_000 + income_bonus_bp)) // 10_000, 0)
    hi = max((hi * (10_000 + income_bonus_bp)) // 10_000, 0)
    return lo, hi


def stamina_cost_preview(job_key: str, level: int, prestige: int, stamina_discount_bp: int) -> int:
    d = JOB_DEFS[job_key]
    base = stamina_cost_for_work(job_level=level, prestige=prestige, category=d.category)
    return max((base * (10_000 - stamina_discount_bp)) // 10_000, 1)


def prestige_preview(job_key: str, progress: SlotProgress) -> tuple[int, int, int]:
    current_mult = max(progress.prestige, 0) + 1
    next_mult = current_mult + 1
    cost = prestige_cost(progress.prestige)
    return current_mult, next_mult, cost


async def prestige_slot(
    session,
    *,
    guild_id: int,
    user_id: int,
    slot_index: int,
    job_key: str,
    vip: bool,
) -> tuple[bool, str, SlotSnapshot | None]:
    progress = await get_or_create_progress(session, guild_id=guild_id, user_id=user_id, slot_index=slot_index, job_key=job_key)
    cap = level_cap_for(progress.prestige)
    if progress.level < cap:
        return False, f"Reach **Lv {cap}** before prestiging this slot.", None
    cost = prestige_cost(progress.prestige)
    wallet = await get_wallet(session, guild_id=guild_id, user_id=user_id)
    if int(wallet.silver) < cost:
        return False, f"Need **{fmt_int(cost)}** Silver to prestige this slot.", None

    old_prestige = int(progress.prestige)
    old_level = int(progress.level)
    log.debug(
        "Starting job hub prestige: guild_id=%s user_id=%s slot_index=%s job_key=%s old_prestige=%s old_level=%s cost=%s",
        guild_id,
        user_id,
        slot_index,
        job_key,
        old_prestige,
        old_level,
        cost,
    )

    wallet.silver -= cost
    wallet.silver_spent += cost
    progress.prestige += 1
    progress.level = 1
    progress.xp = 0
    progress.total_xp = total_xp_from_state(
        tier=tier_for_category(JOB_DEFS[job_key].category),
        job_key=job_key,
        prestige=progress.prestige,
        level=progress.level,
        xp_into=progress.xp,
    )
    await session.flush()
    log.debug(
        "Prestige DB flush complete: guild_id=%s user_id=%s slot_index=%s job_key=%s new_prestige=%s level=%s xp=%s total_xp=%s",
        guild_id,
        user_id,
        slot_index,
        job_key,
        int(progress.prestige),
        int(progress.level),
        int(progress.xp),
        int(progress.total_xp),
    )
    snapshot = await get_slot_snapshot(session, guild_id=guild_id, user_id=user_id, vip=vip, slot_index=slot_index)
    return True, f"🔥 You prestiged **{JOB_DEFS[job_key].name}**! You're now **Prestige {progress.prestige}**.", snapshot


async def set_slot_progress(
    session,
    *,
    guild_id: int,
    user_id: int,
    slot_index: int,
    job_key: str,
    prestige: int,
    xp: int,
) -> UserJobHubProgressRow:
    progress = await get_or_create_progress(
        session,
        guild_id=guild_id,
        user_id=user_id,
        slot_index=slot_index,
        job_key=job_key,
    )
    progress.prestige = max(int(prestige), 0)
    progress.level = max(int(progress.level), 1)
    cap = level_cap_for(progress.prestige)
    progress.level = min(progress.level, cap)
    need = xp_needed(job_key, progress.level, progress.prestige)
    progress.xp = max(min(int(xp), need), 0)
    progress.total_xp = total_xp_from_state(
        tier=tier_for_category(JOB_DEFS[job_key].category),
        job_key=job_key,
        prestige=progress.prestige,
        level=progress.level,
        xp_into=progress.xp,
    )
    return progress


async def award_slot_job_xp(session, *, guild_id: int, user_id: int, slot_index: int, job_key: str, amount: int) -> tuple[UserJobHubProgressRow, bool]:
    progress = await get_or_create_progress(session, guild_id=guild_id, user_id=user_id, slot_index=slot_index, job_key=job_key)
    leveled_up = False
    progress.total_xp += amount
    progress.xp += amount
    cap = level_cap_for(progress.prestige)
    while progress.level < cap:
        need = xp_needed(job_key, progress.level, progress.prestige)
        if progress.xp < need:
            break
        progress.xp -= need
        progress.level += 1
        leveled_up = True
    if progress.level >= cap:
        progress.level = cap
        need = xp_needed(job_key, progress.level, progress.prestige)
        progress.xp = min(progress.xp, need)
    return progress, leveled_up
