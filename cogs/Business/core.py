# cogs/Business/core.py
from __future__ import annotations

"""
Business Core

What this file does:
- Defines the static business catalog used by the /business system
- Defines the data contracts consumed by cog.py
- Builds hub/detail snapshots for business UI
- Handles buying businesses
- Handles starting business runs
- Calculates baseline business values like:
    - upgrade cost
    - display hourly profit
    - runtime
    - slot counts
- Reads and writes the new business DB models

What this file does NOT do:
- It does not run a background loop
- It does not tick active business sessions every hour
- It does not auto-deposit silver while sessions run
- It does not finalize worker/manager hiring systems yet
- It does not own Discord UI classes

What this file requires:
- db.models must expose:
    WalletRow
    BusinessOwnershipRow
    BusinessRunRow
    BusinessWorkerAssignmentRow
    BusinessManagerAssignmentRow

- runtime.py is optional for now.
  This file is currently self-contained for:
    - buy flow
    - run start flow
    - snapshot building

How this file is intended to be used:
- cog.py imports the dataclasses and functions below
- runtime.py will later import some helpers from this file
- the database models here are guild-scoped and user-scoped,
  matching the rest of your economy architecture
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError

from db.models import (
    BusinessManagerAssignmentRow,
    BusinessOwnershipRow,
    BusinessRunRow,
    BusinessWorkerAssignmentRow,
    WalletRow,
)

# =========================================================
# DATACLASSES CONSUMED BY cog.py
# =========================================================


@dataclass(slots=True)
class BusinessDef:
    key: str
    name: str
    emoji: str
    description: str
    cost_silver: int
    base_hourly_income: int
    base_upgrade_cost: int
    flavor: str = ""
    image_url: Optional[str] = None
    banner_url: Optional[str] = None


@dataclass(slots=True)
class BusinessCard:
    key: str
    name: str
    emoji: str
    owned: bool
    running: bool
    level: int
    prestige: int
    hourly_profit: int
    runtime_remaining_hours: int
    worker_slots_used: int
    worker_slots_total: int
    manager_slots_used: int
    manager_slots_total: int
    purchase_cost: int = 0
    image_url: Optional[str] = None


@dataclass(slots=True)
class BusinessHubSnapshot:
    silver_balance: int
    owned_count: int
    total_count: int
    total_hourly_income_active: int
    active_count: int
    cards: List[BusinessCard]


@dataclass(slots=True)
class BusinessManageSnapshot:
    key: str
    name: str
    emoji: str
    description: str
    flavor: str
    owned: bool
    running: bool
    level: int
    prestige: int
    hourly_profit: int
    base_hourly_income: int
    upgrade_cost: Optional[int]
    runtime_remaining_hours: int
    total_runtime_hours: int
    worker_slots_used: int
    worker_slots_total: int
    manager_slots_used: int
    manager_slots_total: int
    image_url: Optional[str] = None
    banner_url: Optional[str] = None
    notes: Optional[List[str]] = None


@dataclass(slots=True)
class BusinessActionResult:
    ok: bool
    message: str
    snapshot: Optional[BusinessHubSnapshot] = None
    manage_snapshot: Optional[BusinessManageSnapshot] = None


@dataclass(slots=True)
class WorkerAssignmentSlotSnapshot:
    slot_index: int
    assignment_id: Optional[int]
    worker_name: Optional[str]
    worker_type: Optional[str]
    rarity: Optional[str]
    flat_profit_bonus: int
    percent_profit_bonus_bp: int
    is_active: bool


@dataclass(slots=True)
class ManagerAssignmentSlotSnapshot:
    slot_index: int
    assignment_id: Optional[int]
    manager_name: Optional[str]
    rarity: Optional[str]
    runtime_bonus_hours: int
    profit_bonus_bp: int
    auto_restart_charges: int
    is_active: bool


# =========================================================
# STATIC BUSINESS CATALOG
# =========================================================

# You can add image_url / banner_url values later.
# Keeping them None for now is fine. cog.py already handles that cleanly.

_BUSINESS_DEFS: tuple[BusinessDef, ...] = (
    BusinessDef(
        key="restaurant",
        name="Restaurant",
        emoji="🍽️",
        description="A balanced starter business with clean, dependable income.",
        cost_silver=100_000,
        base_hourly_income=1_000,
        base_upgrade_cost=25_000,
        flavor="Your first proper business. Classy, simple, and steady.",
    ),
    BusinessDef(
        key="farm",
        name="Farm",
        emoji="🌾",
        description="Stable income with a calm, low-drama vibe.",
        cost_silver=250_000,
        base_hourly_income=1_800,
        base_upgrade_cost=60_000,
        flavor="Quiet money. Dirt, sweat, and a suspicious number of chickens.",
    ),
    BusinessDef(
        key="nightclub",
        name="Nightclub",
        emoji="🪩",
        description="Swingy profits and louder nights.",
        cost_silver=600_000,
        base_hourly_income=3_500,
        base_upgrade_cost=150_000,
        flavor="Half the money comes from vibes. The other half from poor decisions.",
    ),
    BusinessDef(
        key="factory",
        name="Factory",
        emoji="🏭",
        description="A heavy-output business built around production.",
        cost_silver=1_200_000,
        base_hourly_income=6_500,
        base_upgrade_cost=300_000,
        flavor="Loud, efficient, and probably violating something somewhere.",
    ),
    BusinessDef(
        key="casino",
        name="Casino",
        emoji="🎰",
        description="Big money, big risk, big gremlin energy.",
        cost_silver=2_500_000,
        base_hourly_income=10_000,
        base_upgrade_cost=625_000,
        flavor="A machine that legally weaponizes temptation.",
    ),
    BusinessDef(
        key="tech_company",
        name="Tech Company",
        emoji="💻",
        description="An upgrade-focused scaling business.",
        cost_silver=5_000_000,
        base_hourly_income=18_000,
        base_upgrade_cost=1_250_000,
        flavor="Buzzwords, dashboards, and someone definitely saying synergy unironically.",
    ),
    BusinessDef(
        key="shipping_company",
        name="Shipping Company",
        emoji="🚢",
        description="Long-haul profits built for extended operation.",
        cost_silver=9_000_000,
        base_hourly_income=30_000,
        base_upgrade_cost=2_250_000,
        flavor="Massive cargo, massive delays, massive invoices.",
    ),
    BusinessDef(
        key="hotel",
        name="Hotel",
        emoji="🏨",
        description="A staff-heavy business with lots of moving parts.",
        cost_silver=15_000_000,
        base_hourly_income=45_000,
        base_upgrade_cost=3_750_000,
        flavor="Customer service with a smile and a silent internal scream.",
    ),
    BusinessDef(
        key="movie_studio",
        name="Movie Studio",
        emoji="🎬",
        description="A flashy business driven by hype and momentum.",
        cost_silver=25_000_000,
        base_hourly_income=70_000,
        base_upgrade_cost=6_250_000,
        flavor="Drama in front of the camera and ten times more behind it.",
    ),
    BusinessDef(
        key="space_mining",
        name="Space Mining Company",
        emoji="🛰️",
        description="An absurd late-game income machine.",
        cost_silver=50_000_000,
        base_hourly_income=120_000,
        base_upgrade_cost=12_500_000,
        flavor="Mining rocks in space because Earth was apparently too easy.",
    ),
)

_BUSINESS_DEF_MAP: Dict[str, BusinessDef] = {b.key: b for b in _BUSINESS_DEFS}


# =========================================================
# CONSTANTS / TUNING
# =========================================================

BASE_RUNTIME_HOURS_DEFAULT = 4
BASE_RUNTIME_HOURS_SHIPPING = 8
MAX_RUNTIME_HOURS = 48

BASE_WORKER_SLOTS = 2
BASE_MANAGER_SLOTS = 1
HOTEL_STARTING_WORKER_SLOTS = 4

BASE_WORKER_HIRE_COST = 10_000
BASE_MANAGER_HIRE_COST = 35_000

RUN_STATUS_RUNNING = "running"
RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_CANCELLED = "cancelled"


# =========================================================
# GENERIC HELPERS
# =========================================================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(dt: datetime) -> datetime:
    """
    Normalize datetimes to UTC-aware values.

    Some DB backends (notably sqlite) can return naive datetimes even when
    timezone=True is set on the column, which breaks direct comparisons
    against timezone-aware "now" values.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _def_for_key(business_key: str) -> Optional[BusinessDef]:
    return _BUSINESS_DEF_MAP.get(str(business_key).strip())


def _clamp_int(n: int, lo: int, hi: int) -> int:
    if n < lo:
        return lo
    if n > hi:
        return hi
    return n


def _hours_remaining(ends_at: Optional[datetime], *, now: Optional[datetime] = None) -> int:
    if ends_at is None:
        return 0
    if now is None:
        now = _utc_now()
    seconds = int((_as_utc(ends_at) - _as_utc(now)).total_seconds())
    if seconds <= 0:
        return 0

    # ceil to next full hour for nicer UI
    hours = seconds // 3600
    if seconds % 3600:
        hours += 1
    return max(hours, 0)


def _upgrade_percent_bp_for_level(level: int) -> int:
    """
    Tiered upgrade scaling:
    - levels 1-10: +25% each
    - levels 11-20: +10% each
    - levels 21+: +5% each
    """
    lvl = max(int(level), 0)
    bp = 0

    first = min(lvl, 10)
    bp += first * 2500

    if lvl > 10:
        second = min(lvl - 10, 10)
        bp += second * 1000

    if lvl > 20:
        third = lvl - 20
        bp += third * 500

    return bp


def _apply_bp(value: int, basis_points: int) -> int:
    return max(int(round(int(value) * (10_000 + int(basis_points)) / 10_000)), 0)


def _base_runtime_hours_for_key(business_key: str) -> int:
    if business_key == "shipping_company":
        return BASE_RUNTIME_HOURS_SHIPPING
    return BASE_RUNTIME_HOURS_DEFAULT


def _worker_slots_for_business_key_and_level(business_key: str, level: int) -> int:
    base = HOTEL_STARTING_WORKER_SLOTS if business_key == "hotel" else BASE_WORKER_SLOTS
    extra = max(int(level), 0) // 2
    return base + extra


def _manager_slots_for_level(level: int) -> int:
    return BASE_MANAGER_SLOTS + (max(int(level), 0) // 5)


def _effective_base_income(defn: BusinessDef, *, level: int, prestige: int) -> int:
    value = int(defn.base_hourly_income)
    value = _apply_bp(value, _upgrade_percent_bp_for_level(level))
    value = _apply_bp(value, _prestige_bonus_bp(prestige))
    return max(value, 0)


def _prestige_bonus_bp(prestige: int) -> int:
    # I: +20%, II: +35%, III: +50%, IV: +65%, V: +80%
    p = max(int(prestige), 0)
    if p <= 0:
        return 0
    table = {
        1: 2000,
        2: 3500,
        3: 5000,
        4: 6500,
        5: 8000,
    }
    if p in table:
        return table[p]
    # after 5, keep scaling gently
    return 8000 + ((p - 5) * 1000)


def _upgrade_cost(defn: BusinessDef, level: int) -> int:
    current_level = max(int(level), 0)
    # cost to buy next level
    return int(defn.base_upgrade_cost) * (2 ** current_level)


def _manager_runtime_bonus_hours_from_rarity(rarity: str) -> int:
    key = str(rarity).strip().lower()
    if key == "common":
        return 4
    if key == "rare":
        return 8
    if key == "epic":
        return 12
    if key == "legendary":
        return 24
    if key == "mythical":
        return 24
    return 0


def _manager_profit_bonus_bp_from_rarity(rarity: str) -> int:
    key = str(rarity).strip().lower()
    if key == "mythical":
        return 10_000
    return 0


def _normalize_rarity(rarity: str) -> str:
    key = str(rarity).strip().lower()
    allowed = {"common", "rare", "epic", "legendary", "mythical"}
    return key if key in allowed else "common"


def _normalize_worker_type(worker_type: str) -> str:
    key = str(worker_type).strip().lower()
    allowed = {"fast", "efficient", "kind"}
    return key if key in allowed else "efficient"


def _worker_hire_cost(*, rarity: str, flat_profit_bonus: int, percent_profit_bonus_bp: int) -> int:
    rarity_multi = {
        "common": 1.00,
        "rare": 1.35,
        "epic": 1.85,
        "legendary": 2.60,
        "mythical": 4.00,
    }
    r = _normalize_rarity(rarity)
    flat = max(int(flat_profit_bonus), 0)
    bp = max(int(percent_profit_bonus_bp), 0)
    base = int(round(BASE_WORKER_HIRE_COST * rarity_multi[r]))
    return max(base + (flat * 5) + (bp * 20), BASE_WORKER_HIRE_COST)


def _manager_hire_cost(*, rarity: str, runtime_bonus_hours: int, profit_bonus_bp: int, auto_restart_charges: int) -> int:
    rarity_multi = {
        "common": 1.00,
        "rare": 1.40,
        "epic": 2.00,
        "legendary": 3.00,
        "mythical": 5.00,
    }
    r = _normalize_rarity(rarity)
    runtime = max(int(runtime_bonus_hours), 0)
    bp = max(int(profit_bonus_bp), 0)
    auto = max(int(auto_restart_charges), 0)
    base = int(round(BASE_MANAGER_HIRE_COST * rarity_multi[r]))
    return max(base + (runtime * 2_000) + (bp * 35) + (auto * 4_000), BASE_MANAGER_HIRE_COST)


def _normalize_slot_index(slot_index: int) -> int:
    return max(int(slot_index), 0)


# =========================================================
# DB HELPERS
# =========================================================

async def _get_wallet(session, *, guild_id: int, user_id: int) -> WalletRow:
    wallet = await session.scalar(
        select(WalletRow).where(
            WalletRow.guild_id == int(guild_id),
            WalletRow.user_id == int(user_id),
        )
    )
    if wallet is None:
        wallet = WalletRow(
            guild_id=int(guild_id),
            user_id=int(user_id),
            silver=0,
            diamonds=0,
        )
        session.add(wallet)
        await session.flush()
    return wallet


async def _get_ownership_row(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> Optional[BusinessOwnershipRow]:
    return await session.scalar(
        select(BusinessOwnershipRow).where(
            BusinessOwnershipRow.guild_id == int(guild_id),
            BusinessOwnershipRow.user_id == int(user_id),
            BusinessOwnershipRow.business_key == str(business_key),
        )
    )


async def _get_owned_rows_for_user(
    session,
    *,
    guild_id: int,
    user_id: int,
) -> Sequence[BusinessOwnershipRow]:
    rows = await session.scalars(
        select(BusinessOwnershipRow)
        .where(
            BusinessOwnershipRow.guild_id == int(guild_id),
            BusinessOwnershipRow.user_id == int(user_id),
        )
        .order_by(BusinessOwnershipRow.id.asc())
    )
    return list(rows)


async def _get_running_run_map_for_user(
    session,
    *,
    guild_id: int,
    user_id: int,
) -> Dict[str, BusinessRunRow]:
    """
    Returns the latest currently running row per business_key if it still has time left.
    """
    now = _utc_now()

    rows = await session.scalars(
        select(BusinessRunRow)
        .where(
            BusinessRunRow.guild_id == int(guild_id),
            BusinessRunRow.user_id == int(user_id),
            BusinessRunRow.status == RUN_STATUS_RUNNING,
        )
        .order_by(BusinessRunRow.started_at.desc(), BusinessRunRow.id.desc())
    )

    found: Dict[str, BusinessRunRow] = {}
    for row in rows:
        if row.business_key in found:
            continue
        if _as_utc(row.ends_at) <= now:
            continue
        found[row.business_key] = row
    return found


async def _get_running_run_for_business(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> Optional[BusinessRunRow]:
    now = _utc_now()
    row = await session.scalar(
        select(BusinessRunRow)
        .where(
            BusinessRunRow.guild_id == int(guild_id),
            BusinessRunRow.user_id == int(user_id),
            BusinessRunRow.business_key == str(business_key),
            BusinessRunRow.status == RUN_STATUS_RUNNING,
        )
        .order_by(BusinessRunRow.started_at.desc(), BusinessRunRow.id.desc())
    )
    if row is None:
        return None
    if _as_utc(row.ends_at) <= now:
        return None
    return row


async def _count_active_workers_for_ownership(
    session,
    *,
    ownership_id: int,
) -> int:
    value = await session.scalar(
        select(func.count(BusinessWorkerAssignmentRow.id)).where(
            BusinessWorkerAssignmentRow.ownership_id == int(ownership_id),
            BusinessWorkerAssignmentRow.is_active.is_(True),
        )
    )
    return int(value or 0)


async def _count_active_managers_for_ownership(
    session,
    *,
    ownership_id: int,
) -> int:
    value = await session.scalar(
        select(func.count(BusinessManagerAssignmentRow.id)).where(
            BusinessManagerAssignmentRow.ownership_id == int(ownership_id),
            BusinessManagerAssignmentRow.is_active.is_(True),
        )
    )
    return int(value or 0)


async def _sum_active_manager_runtime_bonus_for_ownership(
    session,
    *,
    ownership_id: int,
) -> int:
    rows = await session.scalars(
        select(BusinessManagerAssignmentRow).where(
            BusinessManagerAssignmentRow.ownership_id == int(ownership_id),
            BusinessManagerAssignmentRow.is_active.is_(True),
        )
    )
    total = 0
    for row in rows:
        bonus = int(row.runtime_bonus_hours or 0)
        if bonus <= 0:
            bonus = _manager_runtime_bonus_hours_from_rarity(str(row.rarity))
        total += max(bonus, 0)
    return total


async def _sum_active_manager_profit_bonus_bp_for_ownership(
    session,
    *,
    ownership_id: int,
) -> int:
    """
    Only one mythical passive should matter later, but for now this keeps it simple:
    sum explicit bonuses, with rarity fallback if blank.
    """
    rows = await session.scalars(
        select(BusinessManagerAssignmentRow).where(
            BusinessManagerAssignmentRow.ownership_id == int(ownership_id),
            BusinessManagerAssignmentRow.is_active.is_(True),
        )
    )
    total = 0
    for row in rows:
        bp = int(row.profit_bonus_bp or 0)
        if bp == 0:
            bp = _manager_profit_bonus_bp_from_rarity(str(row.rarity))
        total += max(bp, 0)
    return total


async def _sum_active_worker_flat_bonus_for_ownership(
    session,
    *,
    ownership_id: int,
) -> int:
    value = await session.scalar(
        select(func.coalesce(func.sum(BusinessWorkerAssignmentRow.flat_profit_bonus), 0)).where(
            BusinessWorkerAssignmentRow.ownership_id == int(ownership_id),
            BusinessWorkerAssignmentRow.is_active.is_(True),
        )
    )
    return int(value or 0)


async def _sum_active_worker_percent_bonus_bp_for_ownership(
    session,
    *,
    ownership_id: int,
) -> int:
    value = await session.scalar(
        select(func.coalesce(func.sum(BusinessWorkerAssignmentRow.percent_profit_bonus_bp), 0)).where(
            BusinessWorkerAssignmentRow.ownership_id == int(ownership_id),
            BusinessWorkerAssignmentRow.is_active.is_(True),
        )
    )
    return int(value or 0)


# =========================================================
# PUBLIC CATALOG API
# =========================================================

async def fetch_business_defs(session) -> Sequence[BusinessDef]:
    _ = session
    return list(_BUSINESS_DEFS)


# =========================================================
# SNAPSHOT CALCULATION
# =========================================================

async def _calc_display_hourly_profit_for_owned_business(
    session,
    *,
    ownership: BusinessOwnershipRow,
    defn: BusinessDef,
) -> int:
    """
    This is the display profit for UI right now.

    Current logic:
    - base income
    - upgrades
    - prestige
    - worker flat bonuses
    - worker percentage bonuses
    - manager profit bonus

    We are not doing deep worker synergies yet.
    That comes later when the staffing layer gets spicy.
    """
    level = int(ownership.level or 0)
    prestige = int(ownership.prestige or 0)

    base_after_scaling = _effective_base_income(defn, level=level, prestige=prestige)

    flat_bonus = await _sum_active_worker_flat_bonus_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    percent_bonus_bp = await _sum_active_worker_percent_bonus_bp_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    manager_bonus_bp = await _sum_active_manager_profit_bonus_bp_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )

    value = base_after_scaling + flat_bonus
    value = _apply_bp(value, percent_bonus_bp)
    value = _apply_bp(value, manager_bonus_bp)
    return max(value, 0)


async def _calc_total_runtime_hours_for_owned_business(
    session,
    *,
    ownership: BusinessOwnershipRow,
    defn: BusinessDef,
) -> int:
    base = _base_runtime_hours_for_key(defn.key)
    bonus = await _sum_active_manager_runtime_bonus_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    total = base + bonus
    return _clamp_int(total, 1, MAX_RUNTIME_HOURS)


async def _build_business_card_for_user(
    session,
    *,
    guild_id: int,
    user_id: int,
    defn: BusinessDef,
    owned_map: Dict[str, BusinessOwnershipRow],
    running_map: Dict[str, BusinessRunRow],
) -> BusinessCard:
    owned_row = owned_map.get(defn.key)

    if owned_row is None:
        return BusinessCard(
            key=defn.key,
            name=defn.name,
            emoji=defn.emoji,
            owned=False,
            running=False,
            level=0,
            prestige=0,
            hourly_profit=int(defn.base_hourly_income),
            runtime_remaining_hours=0,
            worker_slots_used=0,
            worker_slots_total=_worker_slots_for_business_key_and_level(defn.key, 0),
            manager_slots_used=0,
            manager_slots_total=_manager_slots_for_level(0),
            purchase_cost=int(defn.cost_silver),
            image_url=defn.image_url,
        )

    level = int(owned_row.level or 0)
    prestige = int(owned_row.prestige or 0)

    running_row = running_map.get(defn.key)
    running = running_row is not None
    runtime_remaining_hours = _hours_remaining(running_row.ends_at) if running_row is not None else 0

    worker_used = await _count_active_workers_for_ownership(
        session,
        ownership_id=int(owned_row.id),
    )
    manager_used = await _count_active_managers_for_ownership(
        session,
        ownership_id=int(owned_row.id),
    )

    hourly_profit = await _calc_display_hourly_profit_for_owned_business(
        session,
        ownership=owned_row,
        defn=defn,
    )

    return BusinessCard(
        key=defn.key,
        name=defn.name,
        emoji=defn.emoji,
        owned=True,
        running=running,
        level=level,
        prestige=prestige,
        hourly_profit=hourly_profit,
        runtime_remaining_hours=runtime_remaining_hours,
        worker_slots_used=worker_used,
        worker_slots_total=_worker_slots_for_business_key_and_level(defn.key, level),
        manager_slots_used=manager_used,
        manager_slots_total=_manager_slots_for_level(level),
        purchase_cost=int(defn.cost_silver),
        image_url=defn.image_url,
    )


# =========================================================
# PUBLIC SNAPSHOT API
# =========================================================

async def get_business_hub_snapshot(
    session,
    *,
    guild_id: int,
    user_id: int,
) -> BusinessHubSnapshot:
    defs = await fetch_business_defs(session)
    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)

    owned_rows = await _get_owned_rows_for_user(session, guild_id=guild_id, user_id=user_id)
    owned_map = {row.business_key: row for row in owned_rows}

    running_map = await _get_running_run_map_for_user(session, guild_id=guild_id, user_id=user_id)

    cards: list[BusinessCard] = []
    for defn in defs:
        card = await _build_business_card_for_user(
            session,
            guild_id=guild_id,
            user_id=user_id,
            defn=defn,
            owned_map=owned_map,
            running_map=running_map,
        )
        cards.append(card)

    owned_count = sum(1 for c in cards if c.owned)
    active_count = sum(1 for c in cards if c.running)
    total_hourly_income_active = sum(int(c.hourly_profit) for c in cards if c.running)

    return BusinessHubSnapshot(
        silver_balance=int(wallet.silver or 0),
        owned_count=owned_count,
        total_count=len(cards),
        total_hourly_income_active=total_hourly_income_active,
        active_count=active_count,
        cards=cards,
    )


async def get_business_manage_snapshot(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> Optional[BusinessManageSnapshot]:
    defn = _def_for_key(business_key)
    if defn is None:
        return None

    ownership = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    running_row = await _get_running_run_for_business(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )

    if ownership is None:
        level = 0
        prestige = 0
        worker_used = 0
        manager_used = 0
        runtime_total = _base_runtime_hours_for_key(defn.key)
        runtime_remaining = 0
        hourly_profit = int(defn.base_hourly_income)
        notes = [
            "You do not own this business yet.",
            "Buy it from the Business Hub to unlock upgrades and staffing later.",
        ]
        return BusinessManageSnapshot(
            key=defn.key,
            name=defn.name,
            emoji=defn.emoji,
            description=defn.description,
            flavor=defn.flavor,
            owned=False,
            running=False,
            level=level,
            prestige=prestige,
            hourly_profit=hourly_profit,
            base_hourly_income=int(defn.base_hourly_income),
            upgrade_cost=int(_upgrade_cost(defn, level)),
            runtime_remaining_hours=runtime_remaining,
            total_runtime_hours=runtime_total,
            worker_slots_used=worker_used,
            worker_slots_total=_worker_slots_for_business_key_and_level(defn.key, level),
            manager_slots_used=manager_used,
            manager_slots_total=_manager_slots_for_level(level),
            image_url=defn.image_url,
            banner_url=defn.banner_url,
            notes=notes,
        )

    level = int(ownership.level or 0)
    prestige = int(ownership.prestige or 0)

    worker_used = await _count_active_workers_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    manager_used = await _count_active_managers_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )

    hourly_profit = await _calc_display_hourly_profit_for_owned_business(
        session,
        ownership=ownership,
        defn=defn,
    )
    runtime_total = await _calc_total_runtime_hours_for_owned_business(
        session,
        ownership=ownership,
        defn=defn,
    )

    running = running_row is not None
    runtime_remaining = _hours_remaining(running_row.ends_at) if running_row is not None else 0

    notes: list[str] = []
    notes.append(f"Base runtime: {int(_base_runtime_hours_for_key(defn.key))}h")
    if manager_used > 0:
        notes.append("Manager bonuses are included in total runtime.")
    if worker_used > 0:
        notes.append("Assigned workers are included in displayed hourly profit.")
    if running_row is not None:
        notes.append("This business currently has an active run.")

    return BusinessManageSnapshot(
        key=defn.key,
        name=defn.name,
        emoji=defn.emoji,
        description=defn.description,
        flavor=defn.flavor,
        owned=True,
        running=running,
        level=level,
        prestige=prestige,
        hourly_profit=hourly_profit,
        base_hourly_income=int(defn.base_hourly_income),
        upgrade_cost=int(_upgrade_cost(defn, level)),
        runtime_remaining_hours=runtime_remaining,
        total_runtime_hours=runtime_total,
        worker_slots_used=worker_used,
        worker_slots_total=_worker_slots_for_business_key_and_level(defn.key, level),
        manager_slots_used=manager_used,
        manager_slots_total=_manager_slots_for_level(level),
        image_url=defn.image_url,
        banner_url=defn.banner_url,
        notes=notes,
    )


# =========================================================
# ACTIONS
# =========================================================

async def buy_business(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(
            ok=False,
            message="That business does not exist.",
        )

    existing = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if existing is not None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(
            session,
            guild_id=guild_id,
            user_id=user_id,
            business_key=business_key,
        )
        return BusinessActionResult(
            ok=False,
            message=f"You already own **{defn.name}**.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)

    cost = int(defn.cost_silver)
    if int(wallet.silver or 0) < cost:
        short = cost - int(wallet.silver or 0)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(
            ok=False,
            message=(
                f"You need **{cost:,} Silver** to buy **{defn.name}**.\n"
                f"You are short by **{short:,} Silver**."
            ),
            snapshot=hub,
        )

    row = BusinessOwnershipRow(
        guild_id=int(guild_id),
        user_id=int(user_id),
        business_key=str(defn.key),
        level=0,
        prestige=0,
        total_earned=0,
        total_spent=cost,
    )
    conflict = False
    try:
        async with session.begin_nested():
            session.add(row)
            await session.flush()
    except IntegrityError:
        conflict = True

    if conflict:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(
            session,
            guild_id=guild_id,
            user_id=user_id,
            business_key=business_key,
        )
        return BusinessActionResult(
            ok=False,
            message=f"You already own **{defn.name}**.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    wallet.silver -= cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent += cost

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )

    return BusinessActionResult(
        ok=True,
        message=(
            f"You bought **{defn.emoji} {defn.name}** for **{cost:,} Silver**.\n"
            f"Your new business is ready to run."
        ),
        snapshot=hub,
        manage_snapshot=manage,
    )


async def start_business_run(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(
            ok=False,
            message="That business does not exist.",
        )

    ownership = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(
            ok=False,
            message=f"You do not own **{defn.name}** yet.",
            snapshot=hub,
        )

    current_run = await _get_running_run_for_business(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if current_run is not None:
        remaining = _hours_remaining(current_run.ends_at)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(
            session,
            guild_id=guild_id,
            user_id=user_id,
            business_key=business_key,
        )
        return BusinessActionResult(
            ok=False,
            message=(
                f"**{defn.name}** is already running.\n"
                f"Time remaining: **{remaining}h**."
            ),
            snapshot=hub,
            manage_snapshot=manage,
        )

    total_runtime_hours = await _calc_total_runtime_hours_for_owned_business(
        session,
        ownership=ownership,
        defn=defn,
    )
    hourly_profit = await _calc_display_hourly_profit_for_owned_business(
        session,
        ownership=ownership,
        defn=defn,
    )

    now = _utc_now()
    ends_at = now + timedelta(hours=total_runtime_hours)

    run = BusinessRunRow(
        ownership_id=int(ownership.id),
        guild_id=int(guild_id),
        user_id=int(user_id),
        business_key=str(business_key),
        status=RUN_STATUS_RUNNING,
        started_at=now,
        ends_at=ends_at,
        last_payout_at=now,
        completed_at=None,
        runtime_hours_snapshot=int(total_runtime_hours),
        hourly_profit_snapshot=int(hourly_profit),
        auto_restart_remaining=0,
        snapshot_json={
            "business_key": defn.key,
            "business_name": defn.name,
            "hourly_profit_snapshot": int(hourly_profit),
            "runtime_hours_snapshot": int(total_runtime_hours),
            "ownership_level": int(ownership.level or 0),
            "ownership_prestige": int(ownership.prestige or 0),
            "started_at_iso": now.isoformat(),
            "ends_at_iso": ends_at.isoformat(),
        },
        report_json=None,
        silver_paid_total=0,
        hours_paid_total=0,
    )
    session.add(run)
    await session.flush()

    # Defensive guard: collapse accidental duplicate active runs created by rapid interactions.
    dupes = await session.scalars(
        select(BusinessRunRow)
        .where(
            BusinessRunRow.guild_id == int(guild_id),
            BusinessRunRow.user_id == int(user_id),
            BusinessRunRow.business_key == str(business_key),
            BusinessRunRow.status == RUN_STATUS_RUNNING,
        )
        .order_by(BusinessRunRow.started_at.asc(), BusinessRunRow.id.asc())
    )
    dupes_list = list(dupes)
    if len(dupes_list) > 1:
        keeper = dupes_list[0]
        for dupe in dupes_list[1:]:
            if int(dupe.id) == int(keeper.id):
                continue
            dupe.status = RUN_STATUS_CANCELLED
            dupe.completed_at = now
            dupe.report_json = {
                "run_id": int(dupe.id),
                "status": RUN_STATUS_CANCELLED,
                "reason": "Duplicate run cancelled automatically.",
                "completed_at_iso": now.isoformat(),
            }

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )

    return BusinessActionResult(
        ok=True,
        message=(
            f"Started **{defn.emoji} {defn.name}**.\n"
            f"Runtime: **{total_runtime_hours}h**\n"
            f"Projected hourly profit: **{hourly_profit:,}/hr**"
        ),
        snapshot=hub,
        manage_snapshot=manage,
    )


async def stop_business_run(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    current_run = await _get_running_run_for_business(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if current_run is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(
            session,
            guild_id=guild_id,
            user_id=user_id,
            business_key=business_key,
        )
        return BusinessActionResult(
            ok=False,
            message=f"**{defn.name}** is not currently running.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    now = _utc_now()
    hours_elapsed = max(0, int((now - _as_utc(current_run.started_at)).total_seconds() // 3600))
    runtime_hours = max(1, int(current_run.runtime_hours_snapshot or 0))
    paid_hours = max(0, int(current_run.hours_paid_total or 0))
    unclaimed_hours = max(0, min(hours_elapsed, runtime_hours) - paid_hours)
    estimated_earned = unclaimed_hours * int(current_run.hourly_profit_snapshot or 0)

    current_run.status = RUN_STATUS_CANCELLED
    current_run.completed_at = now
    current_run.report_json = {
        "run_id": int(current_run.id),
        "status": RUN_STATUS_CANCELLED,
        "reason": "Stopped manually by user.",
        "completed_at_iso": now.isoformat(),
        "estimated_unclaimed_hours": int(unclaimed_hours),
        "estimated_unclaimed_silver": int(estimated_earned),
    }

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )

    return BusinessActionResult(
        ok=True,
        message=(
            f"Stopped **{defn.emoji} {defn.name}**.\n"
            f"Estimated unclaimed progress: **{unclaimed_hours}h**\n"
            f"Estimated unclaimed earnings: **{estimated_earned:,} Silver**"
        ),
        snapshot=hub,
        manage_snapshot=manage,
    )


async def upgrade_business(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(
            ok=False,
            message="That business does not exist.",
        )

    ownership = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(
            ok=False,
            message=f"You do not own **{defn.name}** yet.",
            snapshot=hub,
        )

    old_level = int(ownership.level or 0)
    old_hourly = await _calc_display_hourly_profit_for_owned_business(
        session,
        ownership=ownership,
        defn=defn,
    )
    old_runtime_hours = await _calc_total_runtime_hours_for_owned_business(
        session,
        ownership=ownership,
        defn=defn,
    )

    cost = int(_upgrade_cost(defn, old_level))
    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)
    balance = int(wallet.silver or 0)
    if balance < cost:
        short = cost - balance
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(
            session,
            guild_id=guild_id,
            user_id=user_id,
            business_key=business_key,
        )
        return BusinessActionResult(
            ok=False,
            message=(
                f"You need **{cost:,} Silver** to upgrade **{defn.name}**.\n"
                f"You are short by **{short:,} Silver**."
            ),
            snapshot=hub,
            manage_snapshot=manage,
        )

    wallet.silver -= cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent += cost

    ownership.level = old_level + 1
    if hasattr(ownership, "total_spent"):
        ownership.total_spent = int(ownership.total_spent or 0) + cost

    await session.flush()

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )

    new_level = int(ownership.level or 0)
    new_hourly = old_hourly
    new_runtime_hours = old_runtime_hours
    if manage is not None:
        new_hourly = int(manage.hourly_profit)
        new_runtime_hours = int(manage.total_runtime_hours)

    return BusinessActionResult(
        ok=True,
        message=(
            f"Upgraded **{defn.emoji} {defn.name}** from **Level {old_level}** to **Level {new_level}** "
            f"for **{cost:,} Silver**.\n"
            f"Hourly profit: **{old_hourly:,}/hr** → **{new_hourly:,}/hr**\n"
            f"Run projection: **{old_runtime_hours:,}h / {old_hourly * old_runtime_hours:,} Silver**"
            f" → **{new_runtime_hours:,}h / {new_hourly * new_runtime_hours:,} Silver**"
        ),
        snapshot=hub,
        manage_snapshot=manage,
    )


async def get_worker_assignment_slots(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> List[WorkerAssignmentSlotSnapshot]:
    ownership = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if ownership is None:
        return []

    total_slots = _worker_slots_for_business_key_and_level(
        str(business_key),
        int(ownership.level or 0),
    )
    rows = await session.scalars(
        select(BusinessWorkerAssignmentRow)
        .where(
            BusinessWorkerAssignmentRow.ownership_id == int(ownership.id),
            BusinessWorkerAssignmentRow.business_key == str(business_key),
            BusinessWorkerAssignmentRow.guild_id == int(guild_id),
            BusinessWorkerAssignmentRow.user_id == int(user_id),
            BusinessWorkerAssignmentRow.is_active.is_(True),
        )
        .order_by(BusinessWorkerAssignmentRow.slot_index.asc(), BusinessWorkerAssignmentRow.id.asc())
    )
    by_slot = {int(r.slot_index): r for r in rows}
    out: List[WorkerAssignmentSlotSnapshot] = []
    for slot_index in range(1, total_slots + 1):
        row = by_slot.get(slot_index)
        out.append(
            WorkerAssignmentSlotSnapshot(
                slot_index=slot_index,
                assignment_id=int(row.id) if row is not None else None,
                worker_name=str(row.worker_name) if row is not None else None,
                worker_type=str(row.worker_type) if row is not None else None,
                rarity=str(row.rarity) if row is not None else None,
                flat_profit_bonus=int(row.flat_profit_bonus or 0) if row is not None else 0,
                percent_profit_bonus_bp=int(row.percent_profit_bonus_bp or 0) if row is not None else 0,
                is_active=bool(row is not None and row.is_active),
            )
        )
    return out


async def get_manager_assignment_slots(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> List[ManagerAssignmentSlotSnapshot]:
    ownership = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if ownership is None:
        return []

    total_slots = _manager_slots_for_level(int(ownership.level or 0))
    rows = await session.scalars(
        select(BusinessManagerAssignmentRow)
        .where(
            BusinessManagerAssignmentRow.ownership_id == int(ownership.id),
            BusinessManagerAssignmentRow.business_key == str(business_key),
            BusinessManagerAssignmentRow.guild_id == int(guild_id),
            BusinessManagerAssignmentRow.user_id == int(user_id),
            BusinessManagerAssignmentRow.is_active.is_(True),
        )
        .order_by(BusinessManagerAssignmentRow.slot_index.asc(), BusinessManagerAssignmentRow.id.asc())
    )
    by_slot = {int(r.slot_index): r for r in rows}
    out: List[ManagerAssignmentSlotSnapshot] = []
    for slot_index in range(1, total_slots + 1):
        row = by_slot.get(slot_index)
        out.append(
            ManagerAssignmentSlotSnapshot(
                slot_index=slot_index,
                assignment_id=int(row.id) if row is not None else None,
                manager_name=str(row.manager_name) if row is not None else None,
                rarity=str(row.rarity) if row is not None else None,
                runtime_bonus_hours=int(row.runtime_bonus_hours or 0) if row is not None else 0,
                profit_bonus_bp=int(row.profit_bonus_bp or 0) if row is not None else 0,
                auto_restart_charges=int(row.auto_restart_charges or 0) if row is not None else 0,
                is_active=bool(row is not None and row.is_active),
            )
        )
    return out


async def hire_worker(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
    worker_name: str,
    worker_type: str,
    rarity: str,
    flat_profit_bonus: int,
    percent_profit_bonus_bp: int,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    slots = await get_worker_assignment_slots(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    free_slot = next((s.slot_index for s in slots if not s.is_active), None)
    if free_slot is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message="All worker slots are full. Upgrade the business to unlock more slots.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    norm_type = _normalize_worker_type(worker_type)
    norm_rarity = _normalize_rarity(rarity)
    flat_bonus = _clamp_int(int(flat_profit_bonus), 0, 1_000_000)
    bp_bonus = _clamp_int(int(percent_profit_bonus_bp), 0, 250_000)
    hire_cost = _worker_hire_cost(rarity=norm_rarity, flat_profit_bonus=flat_bonus, percent_profit_bonus_bp=bp_bonus)

    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)
    if int(wallet.silver or 0) < hire_cost:
        short = hire_cost - int(wallet.silver or 0)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message=f"You need **{hire_cost:,} Silver** to hire this worker. Short by **{short:,} Silver**.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    row = BusinessWorkerAssignmentRow(
        ownership_id=int(ownership.id),
        guild_id=int(guild_id),
        user_id=int(user_id),
        business_key=str(business_key),
        slot_index=int(free_slot),
        worker_name=(str(worker_name).strip() or "Worker")[:64],
        worker_type=norm_type,
        rarity=norm_rarity,
        flat_profit_bonus=flat_bonus,
        percent_profit_bonus_bp=bp_bonus,
        special_json={},
        is_active=True,
    )
    conflict = False
    try:
        async with session.begin_nested():
            session.add(row)
            await session.flush()
    except IntegrityError:
        conflict = True

    if conflict:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message="That worker slot was taken by another action. Please try again.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    wallet.silver -= hire_cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent += hire_cost

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    return BusinessActionResult(
        ok=True,
        message=f"Hired **{row.worker_name}** ({norm_rarity}) into slot **#{free_slot}** for **{hire_cost:,} Silver**.",
        snapshot=hub,
        manage_snapshot=manage,
    )


async def remove_worker(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
    slot_index: int,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    normalized_slot = _normalize_slot_index(slot_index)
    max_slot = _worker_slots_for_business_key_and_level(str(business_key), int(ownership.level or 0))
    if normalized_slot <= 0 or normalized_slot > max_slot:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message=f"Worker slot must be between **1** and **{max_slot}**.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    row = await session.scalar(
        select(BusinessWorkerAssignmentRow).where(
            BusinessWorkerAssignmentRow.ownership_id == int(ownership.id),
            BusinessWorkerAssignmentRow.slot_index == int(normalized_slot),
            BusinessWorkerAssignmentRow.is_active.is_(True),
        )
    )
    if row is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message=f"No active worker found in slot **#{normalized_slot}**.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    row.is_active = False
    await session.flush()

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    return BusinessActionResult(
        ok=True,
        message=f"Removed worker **{row.worker_name}** from slot **#{normalized_slot}**.",
        snapshot=hub,
        manage_snapshot=manage,
    )


async def hire_manager(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
    manager_name: str,
    rarity: str,
    runtime_bonus_hours: int,
    profit_bonus_bp: int,
    auto_restart_charges: int,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    slots = await get_manager_assignment_slots(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    free_slot = next((s.slot_index for s in slots if not s.is_active), None)
    if free_slot is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message="All manager slots are full. Upgrade the business to unlock more slots.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    norm_rarity = _normalize_rarity(rarity)
    runtime_bonus = _clamp_int(int(runtime_bonus_hours), 0, 48)
    profit_bp = _clamp_int(int(profit_bonus_bp), 0, 250_000)
    auto_restart = _clamp_int(int(auto_restart_charges), 0, 100)
    hire_cost = _manager_hire_cost(
        rarity=norm_rarity,
        runtime_bonus_hours=runtime_bonus,
        profit_bonus_bp=profit_bp,
        auto_restart_charges=auto_restart,
    )

    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)
    if int(wallet.silver or 0) < hire_cost:
        short = hire_cost - int(wallet.silver or 0)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message=f"You need **{hire_cost:,} Silver** to hire this manager. Short by **{short:,} Silver**.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    row = BusinessManagerAssignmentRow(
        ownership_id=int(ownership.id),
        guild_id=int(guild_id),
        user_id=int(user_id),
        business_key=str(business_key),
        slot_index=int(free_slot),
        manager_name=(str(manager_name).strip() or "Manager")[:64],
        rarity=norm_rarity,
        runtime_bonus_hours=runtime_bonus,
        auto_restart_charges=auto_restart,
        profit_bonus_bp=profit_bp,
        special_json={},
        is_active=True,
    )
    conflict = False
    try:
        async with session.begin_nested():
            session.add(row)
            await session.flush()
    except IntegrityError:
        conflict = True

    if conflict:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message="That manager slot was taken by another action. Please try again.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    wallet.silver -= hire_cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent += hire_cost

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    return BusinessActionResult(
        ok=True,
        message=f"Hired manager **{row.manager_name}** ({norm_rarity}) into slot **#{free_slot}** for **{hire_cost:,} Silver**.",
        snapshot=hub,
        manage_snapshot=manage,
    )


async def remove_manager(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
    slot_index: int,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key=business_key,
    )
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    normalized_slot = _normalize_slot_index(slot_index)
    max_slot = _manager_slots_for_level(int(ownership.level or 0))
    if normalized_slot <= 0 or normalized_slot > max_slot:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message=f"Manager slot must be between **1** and **{max_slot}**.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    row = await session.scalar(
        select(BusinessManagerAssignmentRow).where(
            BusinessManagerAssignmentRow.ownership_id == int(ownership.id),
            BusinessManagerAssignmentRow.slot_index == int(normalized_slot),
            BusinessManagerAssignmentRow.is_active.is_(True),
        )
    )
    if row is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(
            ok=False,
            message=f"No active manager found in slot **#{normalized_slot}**.",
            snapshot=hub,
            manage_snapshot=manage,
        )

    row.is_active = False
    await session.flush()

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    return BusinessActionResult(
        ok=True,
        message=f"Removed manager **{row.manager_name}** from slot **#{normalized_slot}**.",
        snapshot=hub,
        manage_snapshot=manage,
    )


# =========================================================
# OPTIONAL FUTURE HELPERS FOR runtime.py
# =========================================================

async def get_active_runs_for_processing(
    session,
    *,
    guild_id: Optional[int] = None,
) -> Sequence[BusinessRunRow]:
    stmt: Select[tuple[BusinessRunRow]] = (
        select(BusinessRunRow)
        .where(BusinessRunRow.status == RUN_STATUS_RUNNING)
        .order_by(BusinessRunRow.ends_at.asc(), BusinessRunRow.id.asc())
    )
    if guild_id is not None:
        stmt = stmt.where(BusinessRunRow.guild_id == int(guild_id))

    rows = await session.scalars(stmt)
    return list(rows)


async def get_business_def_by_key(
    session,
    *,
    business_key: str,
) -> Optional[BusinessDef]:
    _ = session
    return _def_for_key(business_key)
