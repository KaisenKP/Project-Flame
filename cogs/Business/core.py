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

RUN_STATUS_RUNNING = "running"
RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_CANCELLED = "cancelled"


# =========================================================
# GENERIC HELPERS
# =========================================================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
    seconds = int((ends_at - now).total_seconds())
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
        if row.ends_at <= now:
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
    if row.ends_at <= now:
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

    wallet.silver -= cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent += cost

    row = BusinessOwnershipRow(
        guild_id=int(guild_id),
        user_id=int(user_id),
        business_key=str(defn.key),
        level=0,
        prestige=0,
        total_earned=0,
        total_spent=cost,
    )
    session.add(row)
    await session.flush()

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