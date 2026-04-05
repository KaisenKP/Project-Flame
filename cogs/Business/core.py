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
from decimal import Decimal, ROUND_HALF_UP
import logging
import random
from typing import Dict, List, Optional, Sequence

from .systems import (
    RUN_MODE_STANDARD,
    calc_synergy_bonus_bp,
    diminishing_worker_bonus_bp,
    format_duration_minutes,
    get_business_trait,
    get_run_mode_for_level,
    manager_downtime_reduction_bp,
    manager_instant_reward_bonus_bp,
    manager_negative_reduction_bp,
    manager_positive_bonus_bp,
    manager_role_label,
    summarize_active_events,
    worker_role_label,
    build_run_event_plan,
)

from .prestige import (
    MAX_BUSINESS_PRESTIGE,
    PrestigeConfig,
    at_level_cap,
    bulk_option_for,
    clamp_prestige,
    LEVELS_PER_PRESTIGE,
    max_stored_level_for_prestige,
    total_visible_level_for,
    max_visible_level_for_prestige,
    prestige_cost,
    prestige_multiplier,
    prestige_multiplier_display,
    visible_level_for,
)

from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError

from db.models import (
    BusinessManagerAssignmentRow,
    BusinessOwnershipRow,
    BusinessRunRow,
    BusinessWorkerAssignmentRow,
    WalletRow,
)

log = logging.getLogger(__name__)

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
    prestige_base_cost: int
    prestige_growth_rate: str
    prestige_revenue_hours: int = 72
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
    visible_level: int
    total_visible_level: int
    max_level: int
    prestige: int
    hourly_profit: int
    runtime_remaining_hours: int
    worker_slots_used: int
    worker_slots_total: int
    manager_slots_used: int
    manager_slots_total: int
    projected_payout: int = 0
    worker_bonus_bp: int = 0
    manager_summary: str = "None"
    active_event_summary: str = "No active events"
    active_event_lines: Optional[List[str]] = None
    run_mode: str = "Standard"
    synergy_summary: str = "No synergy"
    trait_summary: str = "Balanced"
    risk_badge: str = "Medium"
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
    visible_level: int
    total_visible_level: int
    max_level: int
    prestige: int
    hourly_profit: int
    base_hourly_income: int
    upgrade_cost: Optional[int]
    prestige_cost: Optional[int]
    can_prestige: bool
    prestige_multiplier: str
    bulk_upgrade_1_unlocked: bool
    bulk_upgrade_5_unlocked: bool
    bulk_upgrade_10_unlocked: bool
    runtime_remaining_hours: int
    total_runtime_hours: int
    worker_slots_used: int
    worker_slots_total: int
    manager_slots_used: int
    manager_slots_total: int
    projected_payout: int = 0
    worker_bonus_bp: int = 0
    worker_summary: str = "No workers assigned"
    manager_summary: str = "No managers assigned"
    active_event_summary: str = "No active events"
    active_event_lines: Optional[List[str]] = None
    synergy_bonus_bp: int = 0
    synergy_summary: str = "No synergy"
    run_mode: str = "Standard"
    run_mode_key: str = "standard"
    trait_summary: str = "Balanced"
    stability_label: str = "Stable"
    next_unlock: Optional[str] = None
    image_url: Optional[str] = None
    banner_url: Optional[str] = None
    notes: Optional[List[str]] = None


@dataclass(slots=True)
class BusinessActionResult:
    ok: bool
    message: str
    snapshot: Optional[BusinessHubSnapshot] = None
    manage_snapshot: Optional[BusinessManageSnapshot] = None
    hired_worker: Optional["HiredWorkerSnapshot"] = None
    hired_manager: Optional["HiredManagerSnapshot"] = None
    worker_candidate: Optional["WorkerCandidateSnapshot"] = None
    manager_candidate: Optional["ManagerCandidateSnapshot"] = None


@dataclass(slots=True)
class HiredWorkerSnapshot:
    slot_index: int
    worker_name: str
    worker_type: str
    rarity: str
    flat_profit_bonus: int
    percent_profit_bonus_bp: int
    hire_cost: int


@dataclass(slots=True)
class WorkerCandidateSnapshot:
    worker_name: str
    worker_type: str
    rarity: str
    flat_profit_bonus: int
    percent_profit_bonus_bp: int
    reroll_cost: int


@dataclass(slots=True)
class HiredManagerSnapshot:
    slot_index: int
    manager_name: str
    rarity: str
    runtime_bonus_hours: int
    profit_bonus_bp: int
    auto_restart_charges: int
    hire_cost: int


@dataclass(slots=True)
class ManagerCandidateSnapshot:
    manager_name: str
    rarity: str
    runtime_bonus_hours: int
    profit_bonus_bp: int
    auto_restart_charges: int
    reroll_cost: int


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
        prestige_base_cost=100_000,
        prestige_growth_rate="2.5",
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
        prestige_base_cost=180_000,
        prestige_growth_rate="2.42",
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
        prestige_base_cost=320_000,
        prestige_growth_rate="2.38",
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
        prestige_base_cost=550_000,
        prestige_growth_rate="2.34",
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
        prestige_base_cost=900_000,
        prestige_growth_rate="2.31",
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
        prestige_base_cost=1_500_000,
        prestige_growth_rate="2.28",
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
        prestige_base_cost=2_400_000,
        prestige_growth_rate="2.24",
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
        prestige_base_cost=3_800_000,
        prestige_growth_rate="2.21",
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
        prestige_base_cost=6_000_000,
        prestige_growth_rate="2.18",
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
        prestige_base_cost=9_500_000,
        prestige_growth_rate="2.15",
        flavor="Mining rocks in space because Earth was apparently too easy.",
    ),
    BusinessDef(
        key="liquor_store",
        name="Liquor Store",
        emoji="🥃",
        description="Limited stock, rush nights, and rare bottle drops.",
        cost_silver=100_000_000,
        base_hourly_income=260_000,
        base_upgrade_cost=25_000_000,
        prestige_base_cost=14_000_000,
        prestige_growth_rate="2.10",
        flavor="Premium shelves, midnight lines, and a very loud cash drawer.",
    ),
    BusinessDef(
        key="underground_market",
        name="Underground Market",
        emoji="🕶️",
        description="Safe flips or risky hits with hot-item spikes.",
        cost_silver=250_000_000,
        base_hourly_income=420_000,
        base_upgrade_cost=40_000_000,
        prestige_base_cost=20_000_000,
        prestige_growth_rate="2.08",
        flavor="Rare goods move fast when the right whispers spread.",
    ),
    BusinessDef(
        key="cartel",
        name="The Cartel",
        emoji="💼",
        description="Build control, hold pressure, and run the map.",
        cost_silver=500_000_000,
        base_hourly_income=700_000,
        base_upgrade_cost=75_000_000,
        prestige_base_cost=30_000_000,
        prestige_growth_rate="2.05",
        flavor="Power under the table. Money above it.",
    ),
    BusinessDef(
        key="shadow_government",
        name="The Shadow Government",
        emoji="🕴️",
        description="Hidden power that can boost your whole network.",
        cost_silver=1_000_000_000,
        base_hourly_income=1_200_000,
        base_upgrade_cost=125_000_000,
        prestige_base_cost=50_000_000,
        prestige_growth_rate="2.02",
        flavor="You don't chase markets anymore. You tilt them.",
    ),
)

_BUSINESS_DEF_MAP: Dict[str, BusinessDef] = {b.key: b for b in _BUSINESS_DEFS}


# =========================================================
# CONSTANTS / TUNING
# =========================================================

BASE_RUNTIME_HOURS_DEFAULT = 4
BASE_RUNTIME_HOURS_SHIPPING = 8
MAX_RUNTIME_HOURS = 48
# Live rebalance hotfix: keep staff buff active but reduce the previously
# overpowered multiplier to the intended x3 target for both managers/employees.
STAFF_POWER_BUFF_MULTIPLIER = 3
STAFF_BONUS_SOFT_CAP_BP = 250_000
FINAL_PROFIT_SOFT_CAP_START = 25_000_000
FINAL_PROFIT_SOFT_CAP_SLOPE_BP = 2500

BASE_WORKER_SLOTS = 2
BASE_MANAGER_SLOTS = 1
HOTEL_STARTING_WORKER_SLOTS = 4

BASE_WORKER_HIRE_COST = 10_000
BASE_MANAGER_HIRE_COST = 35_000
WORKER_CANDIDATE_REROLL_COST = 500
MANAGER_CANDIDATE_REROLL_COST = 1_000

RUN_STATUS_RUNNING = "running"
RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_CANCELLED = "cancelled"
PREMIUM_BUSINESS_KEYS = {"liquor_store", "underground_market", "cartel", "shadow_government"}

PREMIUM_START_ACTIONS: dict[str, dict[str, dict[str, object]]] = {
    "liquor_store": {
        RUN_MODE_STANDARD: {"label": "Restock", "stock_mode": "balanced", "hype_boost": 0, "profit_bp": 300, "start_stock": 100},
        RUN_MODE_SAFE: {"label": "Cheap Stock", "stock_mode": "cheap", "hype_boost": -200, "profit_bp": -600, "start_stock": 120},
        RUN_MODE_AGGRESSIVE: {"label": "Premium Stock", "stock_mode": "premium", "hype_boost": 1200, "profit_bp": 1800, "start_stock": 90},
    },
    "underground_market": {
        RUN_MODE_STANDARD: {"label": "Lock Deal", "risk": "mixed", "hot_push": 600, "profit_bp": 700},
        RUN_MODE_SAFE: {"label": "Play Safe", "risk": "safe", "hot_push": -300, "profit_bp": -900},
        RUN_MODE_AGGRESSIVE: {"label": "Take Risk", "risk": "risky", "hot_push": 1400, "profit_bp": 2200},
    },
    "cartel": {
        RUN_MODE_STANDARD: {"label": "Collect Pressure", "control_delta": 0, "pressure_start": 30, "profit_bp": 800},
        RUN_MODE_SAFE: {"label": "Lock Down", "control_delta": 12, "pressure_start": 20, "profit_bp": 500},
        RUN_MODE_AGGRESSIVE: {"label": "Expand", "control_delta": -8, "pressure_start": 55, "profit_bp": 2000},
    },
    "shadow_government": {
        RUN_MODE_STANDARD: {"label": "Build Power", "focus": "power", "network_boost_bp": 1500, "profit_bp": 400},
        RUN_MODE_SAFE: {"label": "Call Favors", "focus": "network", "network_boost_bp": 2400, "profit_bp": 900},
        RUN_MODE_AGGRESSIVE: {"label": "Cash Out", "focus": "cashout", "network_boost_bp": 800, "profit_bp": 3200},
    },
}


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
    - levels 1-10: +35% each
    - levels 11-20: +15% each
    - levels 21+: +8% each

    This keeps businesses feeling like a primary long-term income source.
    """
    lvl = max(int(level), 0)
    bp = 0

    first = min(lvl, 10)
    bp += first * 3500

    if lvl > 10:
        second = min(lvl - 10, 10)
        bp += second * 1500

    if lvl > 20:
        third = lvl - 20
        bp += third * 800

    return bp


def _apply_bp(value: int, basis_points: int) -> int:
    return max(int(round(int(value) * (10_000 + int(basis_points)) / 10_000)), 0)


def _buff_staff_flat_bonus(flat_bonus: int) -> int:
    return max(int(flat_bonus or 0), 0) * STAFF_POWER_BUFF_MULTIPLIER


def _buff_staff_bonus_bp(basis_points: int) -> int:
    return max(int(basis_points or 0), 0) * STAFF_POWER_BUFF_MULTIPLIER


def _apply_staff_bonus_soft_cap_bp(total_staff_bonus_bp: int) -> int:
    """
    Keep staff buffs explosive while adding non-destructive diminishing returns
    once combined worker+manager power gets extreme.
    """
    total = max(int(total_staff_bonus_bp or 0), 0)
    if total <= STAFF_BONUS_SOFT_CAP_BP:
        return total
    overflow = total - STAFF_BONUS_SOFT_CAP_BP
    softened_overflow = int(round(overflow * 0.45))
    return STAFF_BONUS_SOFT_CAP_BP + softened_overflow


def _apply_final_profit_soft_cap(value: int) -> int:
    """
    Final non-destructive economy protection layer.
    Keeps ownership data intact while softening only extreme output.
    """
    normalized = max(int(value or 0), 0)
    if normalized <= FINAL_PROFIT_SOFT_CAP_START:
        return normalized
    overflow = normalized - FINAL_PROFIT_SOFT_CAP_START
    softened_overflow = int(round(overflow * (FINAL_PROFIT_SOFT_CAP_SLOPE_BP / 10_000)))
    return FINAL_PROFIT_SOFT_CAP_START + softened_overflow


def compute_employee_contribution(*, base_profit: int, worker_flat_bonus: int, worker_percent_bonus_bp: int) -> int:
    value = max(int(base_profit or 0), 0) + _buff_staff_flat_bonus(worker_flat_bonus)
    effective_worker_bp = _buff_staff_bonus_bp(worker_percent_bonus_bp)
    value = _apply_bp(value, effective_worker_bp)
    return max(value, 0)


def compute_manager_bonus(*, current_profit: int, manager_bonus_bp: int, worker_bonus_bp: int) -> int:
    boosted_manager_bp = _buff_staff_bonus_bp(manager_bonus_bp)
    boosted_worker_bp = _buff_staff_bonus_bp(worker_bonus_bp)
    combined_staff_bp = _apply_staff_bonus_soft_cap_bp(boosted_worker_bp + boosted_manager_bp)
    manager_effective_bp = max(combined_staff_bp - boosted_worker_bp, 0)
    return _apply_bp(max(int(current_profit or 0), 0), manager_effective_bp)


def compute_business_income(
    *,
    base_profit: int,
    worker_flat_bonus: int,
    worker_percent_bonus_bp: int,
    manager_bonus_bp: int,
    prestige_bonus_bp: int,
    synergy_bonus_bp: int,
    temporary_bonus_bp: int,
) -> int:
    value = compute_employee_contribution(
        base_profit=base_profit,
        worker_flat_bonus=worker_flat_bonus,
        worker_percent_bonus_bp=worker_percent_bonus_bp,
    )
    value = compute_manager_bonus(
        current_profit=value,
        manager_bonus_bp=manager_bonus_bp,
        worker_bonus_bp=worker_percent_bonus_bp,
    )
    value = _apply_bp(value, int(prestige_bonus_bp or 0))
    value = _apply_bp(value, int(synergy_bonus_bp or 0))
    value = _apply_bp(value, int(temporary_bonus_bp or 0))
    return _apply_final_profit_soft_cap(value)


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


def _normalize_business_progress(*, level: int, prestige: int) -> tuple[int, int]:
    """
    Compatibility shim for pre-prestige ownership rows.

    Older rows could carry their total upgrade count in `level` with `prestige=0`.
    Fold any overflow above the current prestige cap into prestige tiers so the new
    prestige flow can still reason about the row without crashing or soft-locking it.
    """
    normalized_prestige = clamp_prestige(int(prestige))
    normalized_level = max(int(level), 0)
    if normalized_level <= max_stored_level_for_prestige(normalized_prestige):
        return normalized_level, normalized_prestige

    overflow, normalized_level = divmod(normalized_level, LEVELS_PER_PRESTIGE)
    normalized_prestige = clamp_prestige(normalized_prestige + overflow)
    normalized_level = min(normalized_level, max_stored_level_for_prestige(normalized_prestige))
    return normalized_level, normalized_prestige


def _minimum_level_for_worker_count(business_key: str, worker_count: int) -> int:
    base_slots = HOTEL_STARTING_WORKER_SLOTS if business_key == "hotel" else BASE_WORKER_SLOTS
    required_workers = max(int(worker_count), 0)
    if required_workers <= base_slots:
        return 0
    return (required_workers - base_slots) * 2


async def _resolve_effective_business_progress(session, *, ownership: BusinessOwnershipRow) -> tuple[int, int]:
    normalized_level, normalized_prestige = _normalize_business_progress(
        level=int(ownership.level or 0),
        prestige=int(ownership.prestige or 0),
    )
    active_worker_count = await _count_active_workers_for_ownership(session, ownership_id=int(ownership.id))
    inferred_level = _minimum_level_for_worker_count(str(ownership.business_key), active_worker_count)
    if inferred_level <= normalized_level:
        return normalized_level, normalized_prestige

    repaired_level, repaired_prestige = _normalize_business_progress(
        level=inferred_level,
        prestige=normalized_prestige,
    )
    ownership.level = repaired_level
    ownership.prestige = repaired_prestige
    await session.flush()
    return repaired_level, repaired_prestige


def _effective_base_income(defn: BusinessDef, *, level: int, prestige: int) -> int:
    level, prestige = _normalize_business_progress(level=level, prestige=prestige)
    value = int(defn.base_hourly_income)
    value = _apply_bp(value, _upgrade_percent_bp_for_level(level))
    value = Decimal(value) * prestige_multiplier(prestige)
    return max(int(value.to_integral_value(rounding=ROUND_HALF_UP)), 0)


def _prestige_config_for(defn: BusinessDef) -> PrestigeConfig:
    return PrestigeConfig(
        base_cost=int(defn.prestige_base_cost),
        growth_rate=str(defn.prestige_growth_rate),
    )


def _prestige_cost(defn: BusinessDef, prestige: int) -> int:
    config = _prestige_config_for(defn)
    return prestige_cost(config=config, current_prestige=prestige)


def _upgrade_cost(defn: BusinessDef, level: int) -> int:
    current_level = max(int(level), 0)
    # Keep upgrade ROI around ~12 hours of additional income.
    # This keeps every next upgrade in roughly the same payback window.
    cur_income = _effective_base_income(defn, level=current_level, prestige=0)
    next_income = _effective_base_income(defn, level=current_level + 1, prestige=0)
    delta_income = max(int(next_income) - int(cur_income), 1)
    return max(int(round(delta_income * 12)), 1)


@dataclass(frozen=True, slots=True)
class StaffCatalogEntry:
    key: str
    staff_kind: str
    display_name: str
    rarity: str
    business_key: Optional[str]
    worker_type: Optional[str] = None
    flat_profit_bonus: int = 0
    percent_profit_bonus_bp: int = 0
    runtime_bonus_hours: int = 0
    profit_bonus_bp: int = 0
    auto_restart_charges: int = 0


_WORKER_PREFIX_POOL: dict[str, tuple[str, ...]] = {
    "common": ("Rookie", "Steady", "Local", "Budget"),
    "uncommon": ("Skilled", "Trusted", "Prime", "Swift"),
    "rare": ("Skilled", "Trusted", "Prime", "Swift"),
    "epic": ("Elite", "Veteran", "Master", "Ace"),
    "mythic": ("Celestial", "Eternal", "Godspeed", "Arcane"),
}
_WORKER_ROLE_POOL: dict[str, tuple[str, ...]] = {
    "fast": ("Runner", "Courier", "Sprinter", "Dash"),
    "efficient": ("Planner", "Optimizer", "Operator", "Engineer"),
    "kind": ("Host", "Caretaker", "Concierge", "Greeter"),
}
_MANAGER_PREFIX_POOL: dict[str, tuple[str, ...]] = {
    "common": ("Lead", "Floor", "Shift"),
    "rare": ("Senior", "Strategic", "Prime"),
    "epic": ("Executive", "Tactical", "Director"),
    "legendary": ("Legend", "Oracle", "Chief"),
    "mythical": ("Sovereign", "Ascendant", "Epoch"),
}
_WORKER_RARITY_FLAT_RANGE: dict[str, tuple[int, int]] = {
    "common": (50, 120),
    "uncommon": (120, 220),
    "rare": (120, 260),
    "epic": (260, 420),
    "mythic": (750, 1200),
}
_WORKER_RARITY_BP_RANGE: dict[str, tuple[int, int]] = {
    "common": (10, 60),
    "uncommon": (45, 100),
    "rare": (60, 140),
    "epic": (140, 260),
    "mythic": (420, 650),
}


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
    if key == "mythic":
        key = "mythical"
    allowed = {"common", "rare", "epic", "legendary", "mythical"}
    return key if key in allowed else "common"


def _normalize_worker_rarity(rarity: str) -> str:
    key = str(rarity).strip().lower()
    if key == "legendary":
        key = "epic"
    if key == "mythical":
        key = "mythic"
    allowed = {"common", "uncommon", "rare", "epic", "mythic"}
    return key if key in allowed else "common"


def _normalize_worker_type(worker_type: str) -> str:
    key = str(worker_type).strip().lower()
    allowed = {"fast", "efficient", "kind"}
    return key if key in allowed else "efficient"


def _worker_display_rarity(rarity: str) -> str:
    return "mythical" if str(rarity).strip().lower() == "mythic" else str(rarity).strip().lower()


def _worker_midpoint_stats(*, worker_type: str, rarity: str) -> tuple[int, int]:
    rarity_key = _normalize_worker_rarity(rarity)
    flat_low, flat_high = _WORKER_RARITY_FLAT_RANGE[rarity_key]
    bp_low, bp_high = _WORKER_RARITY_BP_RANGE[rarity_key]
    flat_bonus = int(round((flat_low + flat_high) / 2))
    bp_bonus = int(round((bp_low + bp_high) / 2))
    type_key = _normalize_worker_type(worker_type)
    if type_key == "fast":
        bp_bonus = int(round(bp_bonus * 1.2))
    elif type_key == "efficient":
        flat_bonus = int(round(flat_bonus * 1.15))
    elif type_key == "kind":
        flat_bonus = int(round(flat_bonus * 1.05))
        bp_bonus = int(round(bp_bonus * 1.05))
    return _clamp_int(flat_bonus, 0, 1_000_000), _clamp_int(bp_bonus, 0, 250_000)


def get_staff_grant_catalog(*, staff_kind: str, business_key: Optional[str] = None, rarity_filter: Optional[set[str]] = None) -> list[StaffCatalogEntry]:
    kind = str(staff_kind or "").strip().lower()
    entries: list[StaffCatalogEntry] = []
    seen: set[str] = set()
    normalized_rarity_filter = {str(r).strip().lower() for r in (rarity_filter or set()) if str(r).strip()}
    if kind == "worker":
        for rarity, prefixes in _WORKER_PREFIX_POOL.items():
            display_rarity = _worker_display_rarity(rarity)
            if normalized_rarity_filter and display_rarity not in normalized_rarity_filter:
                continue
            for worker_type, roles in _WORKER_ROLE_POOL.items():
                flat_bonus, bp_bonus = _worker_midpoint_stats(worker_type=worker_type, rarity=rarity)
                for prefix in prefixes:
                    for role in roles:
                        display_name = f"{prefix} {role}"
                        key = f"worker:{display_rarity}:{worker_type}:{prefix.lower()}_{role.lower()}"
                        if key in seen:
                            log.warning("business staff catalog duplicate worker key: %s", key)
                            continue
                        seen.add(key)
                        entries.append(
                            StaffCatalogEntry(
                                key=key,
                                staff_kind="worker",
                                display_name=display_name,
                                rarity=display_rarity,
                                business_key=business_key,
                                worker_type=worker_type,
                                flat_profit_bonus=flat_bonus,
                                percent_profit_bonus_bp=bp_bonus,
                            )
                        )
    elif kind == "manager":
        for rarity, prefixes in _MANAGER_PREFIX_POOL.items():
            rarity_key = _normalize_rarity(rarity)
            if normalized_rarity_filter and rarity_key not in normalized_rarity_filter:
                continue
            for prefix in prefixes:
                display_name = f"{prefix} Manager"
                key = f"manager:{rarity_key}:{prefix.lower()}"
                if key in seen:
                    log.warning("business staff catalog duplicate manager key: %s", key)
                    continue
                seen.add(key)
                entries.append(
                    StaffCatalogEntry(
                        key=key,
                        staff_kind="manager",
                        display_name=display_name,
                        rarity=rarity_key,
                        business_key=business_key,
                        runtime_bonus_hours=_manager_runtime_bonus_hours_from_rarity(rarity_key),
                        profit_bonus_bp=_manager_profit_bonus_bp_from_rarity(rarity_key),
                        auto_restart_charges=1 if rarity_key in {"epic", "legendary", "mythical"} else 0,
                    )
                )
    else:
        log.warning("get_staff_grant_catalog called with invalid staff_kind=%s", staff_kind)
        return []

    for entry in entries:
        if not entry.display_name:
            log.warning("business staff catalog entry missing display name: %s", entry.key)
    entries.sort(key=lambda e: (e.rarity, e.display_name))
    return entries


def _roll_weighted_rarity() -> str:
    roll = random.random()
    if roll < 0.60:
        return "common"
    if roll < 0.85:
        return "rare"
    if roll < 0.95:
        return "epic"
    if roll < 0.99:
        return "legendary"
    return "mythical"


def _generate_worker_name(*, worker_type: str, rarity: str) -> str:
    rarity_key = _normalize_worker_rarity(rarity)
    type_key = _normalize_worker_type(worker_type)
    prefix = random.choice(_WORKER_PREFIX_POOL[rarity_key])
    role = random.choice(_WORKER_ROLE_POOL[type_key])
    badge = random.randint(10, 99)
    return f"{prefix} {role} {badge}"


def _generate_worker_roll() -> dict[str, int | str]:
    worker_type = random.choice(("fast", "efficient", "kind"))
    rarity_roll = random.random()
    if rarity_roll < 0.60:
        rarity = "common"
    elif rarity_roll < 0.85:
        rarity = "uncommon"
    elif rarity_roll < 0.95:
        rarity = "rare"
    elif rarity_roll < 0.99:
        rarity = "epic"
    else:
        rarity = "mythic"
    flat_low, flat_high = _WORKER_RARITY_FLAT_RANGE[rarity]
    bp_low, bp_high = _WORKER_RARITY_BP_RANGE[rarity]
    flat_bonus = random.randint(flat_low, flat_high)
    bp_bonus = random.randint(bp_low, bp_high)
    if worker_type == "fast":
        bp_bonus = int(round(bp_bonus * 1.2))
    elif worker_type == "efficient":
        flat_bonus = int(round(flat_bonus * 1.15))
    elif worker_type == "kind":
        flat_bonus = int(round(flat_bonus * 1.05))
        bp_bonus = int(round(bp_bonus * 1.05))
    return {
        "worker_name": _generate_worker_name(worker_type=worker_type, rarity=rarity),
        "worker_type": worker_type,
        "rarity": rarity,
        "flat_profit_bonus": _clamp_int(flat_bonus, 0, 1_000_000),
        "percent_profit_bonus_bp": _clamp_int(bp_bonus, 0, 250_000),
    }


def _generate_manager_name(*, rarity: str) -> str:
    rarity_key = _normalize_rarity(rarity)
    prefix = random.choice(_MANAGER_PREFIX_POOL[rarity_key])
    return f"{prefix} Manager {random.randint(10, 99)}"


def _generate_manager_roll() -> dict[str, int | str]:
    rarity = _roll_weighted_rarity()
    runtime_range = {
        "common": (0, 2),
        "rare": (2, 6),
        "epic": (6, 10),
        "legendary": (10, 16),
        "mythical": (16, 24),
    }
    bp_range = {
        "common": (0, 50),
        "rare": (50, 130),
        "epic": (130, 260),
        "legendary": (260, 500),
        "mythical": (500, 800),
    }
    auto_range = {
        "common": (0, 0),
        "rare": (0, 1),
        "epic": (1, 2),
        "legendary": (2, 3),
        "mythical": (3, 5),
    }
    runtime_low, runtime_high = runtime_range[rarity]
    bp_low, bp_high = bp_range[rarity]
    auto_low, auto_high = auto_range[rarity]
    return {
        "manager_name": _generate_manager_name(rarity=rarity),
        "rarity": rarity,
        "runtime_bonus_hours": random.randint(runtime_low, runtime_high),
        "profit_bonus_bp": random.randint(bp_low, bp_high),
        "auto_restart_charges": random.randint(auto_low, auto_high),
    }


def _worker_hire_cost(*, rarity: str, flat_profit_bonus: int, percent_profit_bonus_bp: int) -> int:
    rarity_multi = {
        "common": 1.00,
        "uncommon": 1.20,
        "rare": 1.35,
        "epic": 1.85,
        "mythic": 4.00,
    }
    r = _normalize_worker_rarity(rarity)
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


async def _sum_active_manager_auto_restart_charges_for_ownership(
    session,
    *,
    ownership_id: int,
) -> int:
    value = await session.scalar(
        select(func.coalesce(func.sum(BusinessManagerAssignmentRow.auto_restart_charges), 0)).where(
            BusinessManagerAssignmentRow.ownership_id == int(ownership_id),
            BusinessManagerAssignmentRow.is_active.is_(True),
        )
    )
    return int(value or 0)


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


async def _get_active_worker_rows_for_ownership(session, *, ownership_id: int) -> list[BusinessWorkerAssignmentRow]:
    rows = await session.scalars(
        select(BusinessWorkerAssignmentRow).where(
            BusinessWorkerAssignmentRow.ownership_id == int(ownership_id),
            BusinessWorkerAssignmentRow.is_active.is_(True),
        ).order_by(BusinessWorkerAssignmentRow.slot_index.asc())
    )
    return list(rows)


async def _get_active_manager_rows_for_ownership(session, *, ownership_id: int) -> list[BusinessManagerAssignmentRow]:
    rows = await session.scalars(
        select(BusinessManagerAssignmentRow).where(
            BusinessManagerAssignmentRow.ownership_id == int(ownership_id),
            BusinessManagerAssignmentRow.is_active.is_(True),
        ).order_by(BusinessManagerAssignmentRow.slot_index.asc())
    )
    return list(rows)


def _format_percent_bp(bp: int) -> str:
    value = int(bp) / 100
    if float(value).is_integer():
        return f"{int(value)}%"
    return f"{value:.1f}%"


async def _worker_bonus_snapshot(session, *, ownership: BusinessOwnershipRow) -> tuple[int, str]:
    rows = await _get_active_worker_rows_for_ownership(session, ownership_id=int(ownership.id))
    if not rows:
        return 0, "No workers assigned"
    total_bp = _buff_staff_bonus_bp(diminishing_worker_bonus_bp(sum(int(row.percent_profit_bonus_bp or 0) for row in rows)))
    parts = []
    for row in rows[:3]:
        boosted_row_bp = _buff_staff_bonus_bp(int(row.percent_profit_bonus_bp or 0))
        parts.append(f"{worker_role_label(str(row.worker_type), str(ownership.business_key))} +{_format_percent_bp(boosted_row_bp)}")
    suffix = f" | +{len(rows)-3} more" if len(rows) > 3 else ""
    return total_bp, ", ".join(parts) + suffix


async def _manager_summary_snapshot(session, *, ownership: BusinessOwnershipRow) -> tuple[str, int, int, int]:
    rows = await _get_active_manager_rows_for_ownership(session, ownership_id=int(ownership.id))
    if not rows:
        return "No managers assigned", 0, 0, 0
    positive_bp = manager_positive_bonus_bp(rows)
    negative_bp = manager_negative_reduction_bp(rows)
    downtime_bp = manager_downtime_reduction_bp(rows)
    labels = []
    for row in rows[:2]:
        labels.append(f"{manager_role_label(str(ownership.business_key), int(row.slot_index))} Δ-{_format_percent_bp(manager_downtime_reduction_bp([row]))}")
    return ", ".join(labels), positive_bp, negative_bp, downtime_bp


async def _get_run_mode_key_for_ownership(session, *, ownership: BusinessOwnershipRow) -> str:
    running = await _get_running_run_for_business(session, guild_id=int(ownership.guild_id), user_id=int(ownership.user_id), business_key=str(ownership.business_key))
    if running is not None:
        return str((running.snapshot_json or {}).get("run_mode", RUN_MODE_STANDARD))
    return str((await _get_active_manager_rows_for_ownership(session, ownership_id=int(ownership.id)) and RUN_MODE_STANDARD) or RUN_MODE_STANDARD)


def _is_premium_business_key(business_key: str) -> bool:
    return str(business_key).strip().lower() in PREMIUM_BUSINESS_KEYS


async def _active_shadow_network_bonus_bp(session, *, guild_id: int, user_id: int, target_business_key: str) -> int:
    if str(target_business_key) == "shadow_government":
        return 0
    shadow_run = await _get_running_run_for_business(
        session,
        guild_id=guild_id,
        user_id=user_id,
        business_key="shadow_government",
    )
    if shadow_run is None:
        return 0
    premium = dict((shadow_run.snapshot_json or {}).get("premium_run", {}))
    if premium.get("business_key") != "shadow_government":
        return 0
    return max(int(premium.get("network_boost_bp", 0) or 0), 0)


async def _recent_shadow_power_bank(session, *, guild_id: int, user_id: int) -> int:
    rows = await session.scalars(
        select(BusinessRunRow)
        .where(
            BusinessRunRow.guild_id == int(guild_id),
            BusinessRunRow.user_id == int(user_id),
            BusinessRunRow.business_key == "shadow_government",
            BusinessRunRow.status == RUN_STATUS_COMPLETED,
        )
        .order_by(BusinessRunRow.completed_at.desc(), BusinessRunRow.id.desc())
        .limit(5)
    )
    for row in rows:
        tracking = dict((row.report_json or {}).get("summary_tracking", {}))
        premium = dict(tracking.get("premium", {}))
        bank = int(premium.get("power_bank", 0) or 0)
        if bank > 0:
            return min(bank, 120)
    return 0


async def _recent_cartel_control_streak(session, *, guild_id: int, user_id: int) -> int:
    rows = await session.scalars(
        select(BusinessRunRow)
        .where(
            BusinessRunRow.guild_id == int(guild_id),
            BusinessRunRow.user_id == int(user_id),
            BusinessRunRow.business_key == "cartel",
            BusinessRunRow.status == RUN_STATUS_COMPLETED,
        )
        .order_by(BusinessRunRow.completed_at.desc(), BusinessRunRow.id.desc())
        .limit(6)
    )
    streak = 0
    for row in rows:
        tracking = dict((row.report_json or {}).get("summary_tracking", {}))
        premium = dict(tracking.get("premium", {}))
        if bool(premium.get("control_kept", False)):
            streak += 1
        else:
            break
    return min(streak, 5)


async def _compute_run_state_summary(session, *, ownership: BusinessOwnershipRow, defn: BusinessDef, running_row: Optional[BusinessRunRow]) -> dict:
    level, _ = await _resolve_effective_business_progress(session, ownership=ownership)
    worker_rows = await _get_active_worker_rows_for_ownership(session, ownership_id=int(ownership.id))
    manager_rows = await _get_active_manager_rows_for_ownership(session, ownership_id=int(ownership.id))
    worker_bp, worker_summary = await _worker_bonus_snapshot(session, ownership=ownership)
    manager_summary, _, _, _ = await _manager_summary_snapshot(session, ownership=ownership)
    run_mode_key = RUN_MODE_STANDARD
    active_event_summary = "No active events"
    active_event_lines: list[str] = []
    if running_row is not None:
        run_mode_key = str((running_row.snapshot_json or {}).get("run_mode", RUN_MODE_STANDARD))
        active_bp, active_lines = summarize_active_events(list((running_row.snapshot_json or {}).get("event_plan", [])), now=_utc_now())
        if active_lines:
            active_event_summary = active_lines[0]
            active_event_lines = active_lines
    run_mode = get_run_mode_for_level(level, run_mode_key)
    owned_rows = await _get_owned_rows_for_user(session, guild_id=int(ownership.guild_id), user_id=int(ownership.user_id))
    running_map = await _get_running_run_map_for_user(session, guild_id=int(ownership.guild_id), user_id=int(ownership.user_id))
    synergy_bp, synergy_labels = calc_synergy_bonus_bp(str(ownership.business_key), running_map.keys(), [row.business_key for row in owned_rows])
    shadow_bonus_bp = await _active_shadow_network_bonus_bp(
        session,
        guild_id=int(ownership.guild_id),
        user_id=int(ownership.user_id),
        target_business_key=str(ownership.business_key),
    )
    if shadow_bonus_bp > 0:
        synergy_bp += shadow_bonus_bp
        synergy_labels = list(synergy_labels) + [f"Shadow network +{shadow_bonus_bp/100:.0f}%"]
    trait = get_business_trait(defn.key)
    return {
        "worker_bp": worker_bp,
        "worker_summary": worker_summary,
        "manager_summary": manager_summary,
        "active_event_summary": active_event_summary,
        "active_event_lines": active_event_lines,
        "synergy_bp": synergy_bp,
        "synergy_summary": synergy_labels[0] if synergy_labels else "No synergy active",
        "run_mode_key": run_mode.key,
        "run_mode_label": run_mode.label,
        "trait_summary": trait.positive_bias,
        "stability_label": f"Stability {trait.stability}/100",
    }


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
    level, prestige = await _resolve_effective_business_progress(session, ownership=ownership)

    trait = get_business_trait(defn.key)
    running_row = await _get_running_run_for_business(
        session,
        guild_id=int(ownership.guild_id),
        user_id=int(ownership.user_id),
        business_key=str(defn.key),
    )
    state = await _compute_run_state_summary(session, ownership=ownership, defn=defn, running_row=running_row)

    base_after_scaling = int(defn.base_hourly_income)
    base_after_scaling = _apply_bp(base_after_scaling, _upgrade_percent_bp_for_level(level))
    base_after_scaling = _apply_bp(base_after_scaling, trait.base_profit_multiplier_bp - 10_000)

    flat_bonus = await _sum_active_worker_flat_bonus_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    raw_percent_bonus_bp = await _sum_active_worker_percent_bonus_bp_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    state_worker_bp_raw = int(state["worker_bp"]) // STAFF_POWER_BUFF_MULTIPLIER if STAFF_POWER_BUFF_MULTIPLIER > 0 else int(state["worker_bp"])
    percent_bonus_bp = max(
        diminishing_worker_bonus_bp(raw_percent_bonus_bp),
        max(state_worker_bp_raw, 0),
    )
    manager_bonus_bp = await _sum_active_manager_profit_bonus_bp_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    temporary_bonus_bp = 0
    if running_row is not None:
        active_event_bp, _ = summarize_active_events(list((running_row.snapshot_json or {}).get("event_plan", [])), now=_utc_now())
        temporary_bonus_bp += int(active_event_bp)
    prestige_bonus_bp = int(prestige_multiplier(prestige) * 10_000) - 10_000
    value = compute_business_income(
        base_profit=base_after_scaling,
        worker_flat_bonus=flat_bonus,
        worker_percent_bonus_bp=percent_bonus_bp,
        manager_bonus_bp=manager_bonus_bp,
        prestige_bonus_bp=prestige_bonus_bp,
        synergy_bonus_bp=int(state["synergy_bp"]),
        temporary_bonus_bp=temporary_bonus_bp,
    )
    return max(int(value), 0)


async def _calc_total_runtime_hours_for_owned_business(
    session,
    *,
    ownership: BusinessOwnershipRow,
    defn: BusinessDef,
) -> int:
    trait = get_business_trait(defn.key)
    base = _base_runtime_hours_for_key(defn.key)
    bonus = await _sum_active_manager_runtime_bonus_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    total = base + bonus
    total = int(round(total * (trait.max_run_duration_modifier_bp / 10_000)))
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
            visible_level=visible_level_for(0),
            total_visible_level=total_visible_level_for(stored_level=0, prestige=0),
            max_level=max_visible_level_for_prestige(0),
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

    level, prestige = await _resolve_effective_business_progress(session, ownership=owned_row)

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
    state = await _compute_run_state_summary(session, ownership=owned_row, defn=defn, running_row=running_row)

    hourly_profit = await _calc_display_hourly_profit_for_owned_business(
        session,
        ownership=owned_row,
        defn=defn,
    )
    runtime_total = await _calc_total_runtime_hours_for_owned_business(session, ownership=owned_row, defn=defn)

    return BusinessCard(
        key=defn.key,
        name=defn.name,
        emoji=defn.emoji,
        owned=True,
        running=running,
        level=level,
        visible_level=visible_level_for(level),
        total_visible_level=total_visible_level_for(stored_level=level, prestige=prestige),
        max_level=max_visible_level_for_prestige(prestige),
        prestige=prestige,
        hourly_profit=hourly_profit,
        runtime_remaining_hours=runtime_remaining_hours,
        worker_slots_used=worker_used,
        worker_slots_total=_worker_slots_for_business_key_and_level(defn.key, level),
        manager_slots_used=manager_used,
        manager_slots_total=_manager_slots_for_level(level),
        projected_payout=int(hourly_profit * runtime_total),
        worker_bonus_bp=int(state["worker_bp"]),
        manager_summary=str(state["manager_summary"]),
        active_event_summary=str(state["active_event_summary"]),
        active_event_lines=list(state["active_event_lines"]),
        run_mode=str(state["run_mode_label"]),
        synergy_summary=str(state["synergy_summary"]),
        trait_summary=str(state["trait_summary"]),
        risk_badge=str(state["stability_label"]),
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

    ownership = await _get_ownership_row(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    running_row = await _get_running_run_for_business(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    trait = get_business_trait(business_key)

    if ownership is None:
        level = 0
        prestige = 0
        runtime_total = _base_runtime_hours_for_key(defn.key)
        return BusinessManageSnapshot(
            key=defn.key, name=defn.name, emoji=defn.emoji, description=defn.description, flavor=defn.flavor,
            owned=False, running=False, level=level, visible_level=visible_level_for(level),
            total_visible_level=total_visible_level_for(stored_level=level, prestige=prestige),
            max_level=max_visible_level_for_prestige(prestige), prestige=prestige, hourly_profit=int(defn.base_hourly_income),
            base_hourly_income=int(defn.base_hourly_income), upgrade_cost=int(_upgrade_cost(defn, level)),
            prestige_cost=int(_prestige_cost(defn, prestige)), can_prestige=False,
            prestige_multiplier=prestige_multiplier_display(prestige), bulk_upgrade_1_unlocked=True,
            bulk_upgrade_5_unlocked=bulk_option_for(prestige, 5).unlocked, bulk_upgrade_10_unlocked=bulk_option_for(prestige, 10).unlocked,
            runtime_remaining_hours=0, total_runtime_hours=runtime_total, worker_slots_used=0,
            worker_slots_total=_worker_slots_for_business_key_and_level(defn.key, level), manager_slots_used=0,
            manager_slots_total=_manager_slots_for_level(level), projected_payout=int(defn.base_hourly_income * runtime_total),
            worker_bonus_bp=0, worker_summary="No workers assigned", manager_summary="No managers assigned",
            active_event_summary="No active events", active_event_lines=[], synergy_bonus_bp=0, synergy_summary="No synergy active",
            run_mode="Standard", run_mode_key=RUN_MODE_STANDARD, trait_summary=trait.positive_bias,
            stability_label=f"Stability {trait.stability}/100", next_unlock="Own the business to unlock staffing, modes, and synergies.",
            image_url=defn.image_url, banner_url=defn.banner_url,
            notes=["You do not own this business yet.", "Buy it from the Business Hub to unlock upgrades and staffing later."]
        )

    level, prestige = await _resolve_effective_business_progress(session, ownership=ownership)
    worker_used = await _count_active_workers_for_ownership(session, ownership_id=int(ownership.id))
    manager_used = await _count_active_managers_for_ownership(session, ownership_id=int(ownership.id))
    runtime_total = await _calc_total_runtime_hours_for_owned_business(session, ownership=ownership, defn=defn)
    hourly_profit = await _calc_display_hourly_profit_for_owned_business(session, ownership=ownership, defn=defn)
    runtime_remaining = _hours_remaining(running_row.ends_at) if running_row is not None else 0
    state = await _compute_run_state_summary(session, ownership=ownership, defn=defn, running_row=running_row)

    next_unlock = None
    if level < 25:
        next_unlock = "Level 25 unlocks stronger event scaling and deeper staffing value."
    elif level < 50:
        next_unlock = "Level 50 unlocks Aggressive mode for this business."
    elif level < 75:
        next_unlock = "Level 75 slightly improves rare event quality and payout variance."
    else:
        next_unlock = "Keep prestiging to expand level cap and improve event ceilings."

    notes = [
        f"Identity: {trait.positive_bias} • {trait.risk_label}",
        f"Workers +{int(state['worker_bp'])/100:.0f}% | Manager: {state['manager_summary']}",
        f"Manager Power Buff Active: x{STAFF_POWER_BUFF_MULTIPLIER}",
        f"Employee Efficiency Buff Active: x{STAFF_POWER_BUFF_MULTIPLIER}",
        f"Final Profit After Staff Bonuses: {hourly_profit:,}/hr",
        f"Mode: {state['run_mode_label']} | Synergy: {state['synergy_summary']}",
    ]
    if _is_premium_business_key(defn.key):
        notes.append("Premium Run: Make your setup choices in the first 2 minutes, then let it ride.")
        notes.append("This business has exclusive run mechanics and a premium end summary.")
    if running_row is not None:
        notes.append(f"Event: {state['active_event_summary']}")

    return BusinessManageSnapshot(
        key=defn.key, name=defn.name, emoji=defn.emoji, description=defn.description, flavor=defn.flavor,
        owned=True, running=running_row is not None, level=level, visible_level=visible_level_for(level),
        total_visible_level=total_visible_level_for(stored_level=level, prestige=prestige),
        max_level=max_visible_level_for_prestige(prestige), prestige=prestige, hourly_profit=hourly_profit,
        base_hourly_income=int(defn.base_hourly_income),
        upgrade_cost=None if at_level_cap(stored_level=level, prestige=prestige) else int(_upgrade_cost(defn, level)),
        prestige_cost=int(_prestige_cost(defn, prestige)) if at_level_cap(stored_level=level, prestige=prestige) and prestige < MAX_BUSINESS_PRESTIGE else None,
        can_prestige=at_level_cap(stored_level=level, prestige=prestige) and prestige < MAX_BUSINESS_PRESTIGE,
        prestige_multiplier=prestige_multiplier_display(prestige), bulk_upgrade_1_unlocked=True,
        bulk_upgrade_5_unlocked=bulk_option_for(prestige, 5).unlocked, bulk_upgrade_10_unlocked=bulk_option_for(prestige, 10).unlocked,
        runtime_remaining_hours=runtime_remaining, total_runtime_hours=runtime_total, worker_slots_used=worker_used,
        worker_slots_total=_worker_slots_for_business_key_and_level(defn.key, level), manager_slots_used=manager_used,
        manager_slots_total=_manager_slots_for_level(level), projected_payout=int(hourly_profit * runtime_total),
        worker_bonus_bp=int(state['worker_bp']), worker_summary=str(state['worker_summary']), manager_summary=str(state['manager_summary']),
        active_event_summary=str(state['active_event_summary']), active_event_lines=list(state['active_event_lines']),
        synergy_bonus_bp=int(state['synergy_bp']), synergy_summary=str(state['synergy_summary']),
        run_mode=str(state['run_mode_label']), run_mode_key=str(state['run_mode_key']), trait_summary=str(state['trait_summary']),
        stability_label=str(state['stability_label']), next_unlock=next_unlock, image_url=defn.image_url, banner_url=defn.banner_url, notes=notes
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
    run_mode_key: str = RUN_MODE_STANDARD,
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

    level, prestige = await _resolve_effective_business_progress(session, ownership=ownership)
    run_mode = get_run_mode_for_level(level, run_mode_key)
    premium_action = None
    premium_state: dict[str, object] = {}
    if _is_premium_business_key(defn.key):
        mode_map = PREMIUM_START_ACTIONS.get(defn.key, {})
        premium_action = dict(mode_map.get(run_mode_key) or mode_map.get(RUN_MODE_STANDARD) or {})
        run_mode = get_run_mode_for_level(level, RUN_MODE_STANDARD)

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
    hourly_profit = _apply_bp(hourly_profit, run_mode.profit_bp)
    if premium_action:
        hourly_profit = _apply_bp(hourly_profit, int(premium_action.get("profit_bp", 0) or 0))
        if defn.key == "shadow_government":
            bank = await _recent_shadow_power_bank(session, guild_id=guild_id, user_id=user_id)
            bank_bonus_bp = min(bank * 20, 1800)
            hourly_profit = _apply_bp(hourly_profit, bank_bonus_bp)
            premium_state["power_bank"] = bank
            premium_state["power_bank_bonus_bp"] = bank_bonus_bp
        if defn.key == "cartel":
            streak = await _recent_cartel_control_streak(session, guild_id=guild_id, user_id=user_id)
            streak_bonus_bp = streak * 400
            hourly_profit = _apply_bp(hourly_profit, streak_bonus_bp)
            premium_state["control_streak"] = streak
            premium_state["control_streak_bonus_bp"] = streak_bonus_bp
    # Snapshot a presentational-only contribution breakdown for end-of-run summaries.
    trait = get_business_trait(defn.key)
    base_after_scaling = int(defn.base_hourly_income)
    base_after_scaling = _apply_bp(base_after_scaling, _upgrade_percent_bp_for_level(level))
    base_after_scaling = _apply_bp(base_after_scaling, trait.base_profit_multiplier_bp - 10_000)
    prestige_bonus_bp = int(prestige_multiplier(prestige) * 10_000) - 10_000
    flat_bonus = await _sum_active_worker_flat_bonus_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    raw_percent_bonus_bp = await _sum_active_worker_percent_bonus_bp_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    manager_bonus_bp = await _sum_active_manager_profit_bonus_bp_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    baseline_hourly_component = compute_business_income(
        base_profit=base_after_scaling,
        worker_flat_bonus=0,
        worker_percent_bonus_bp=0,
        manager_bonus_bp=0,
        prestige_bonus_bp=prestige_bonus_bp,
        synergy_bonus_bp=0,
        temporary_bonus_bp=0,
    )
    worker_hourly_component = max(
        compute_business_income(
            base_profit=base_after_scaling,
            worker_flat_bonus=flat_bonus,
            worker_percent_bonus_bp=diminishing_worker_bonus_bp(raw_percent_bonus_bp),
            manager_bonus_bp=0,
            prestige_bonus_bp=prestige_bonus_bp,
            synergy_bonus_bp=0,
            temporary_bonus_bp=0,
        )
        - baseline_hourly_component,
        0,
    )
    manager_hourly_component = max(
        compute_business_income(
            base_profit=base_after_scaling,
            worker_flat_bonus=flat_bonus,
            worker_percent_bonus_bp=diminishing_worker_bonus_bp(raw_percent_bonus_bp),
            manager_bonus_bp=manager_bonus_bp,
            prestige_bonus_bp=prestige_bonus_bp,
            synergy_bonus_bp=0,
            temporary_bonus_bp=0,
        )
        - (baseline_hourly_component + worker_hourly_component),
        0,
    )

    now = _utc_now()
    ends_at = now + timedelta(hours=total_runtime_hours)

    auto_restart_remaining = await _sum_active_manager_auto_restart_charges_for_ownership(
        session,
        ownership_id=int(ownership.id),
    )
    manager_rows = await _get_active_manager_rows_for_ownership(session, ownership_id=int(ownership.id))
    worker_rows = await _get_active_worker_rows_for_ownership(session, ownership_id=int(ownership.id))
    event_plan = build_run_event_plan(
        run_id=random.randint(1, 2_147_483_647),
        business_key=defn.key,
        level=level,
        worker_count=len(worker_rows),
        worker_rows=worker_rows,
        manager_rows=manager_rows,
        started_at=now,
        ends_at=ends_at,
        run_mode_key=run_mode.key,
    )
    if premium_action:
        premium_state.update(
            {
                "business_key": defn.key,
                "setup_locked_after_seconds": 120,
                "start_action": str(premium_action.get("label", "Start")),
                "run_mode_key": str(run_mode_key),
                "network_boost_bp": int(premium_action.get("network_boost_bp", 0) or 0),
            }
        )
        if defn.key == "liquor_store":
            premium_state.update(
                {
                    "stock_mode": str(premium_action.get("stock_mode", "balanced")),
                    "stock": int(premium_action.get("start_stock", 100) or 100),
                    "hype_boost": int(premium_action.get("hype_boost", 0) or 0),
                }
            )
        elif defn.key == "underground_market":
            premium_state.update(
                {
                    "risk": str(premium_action.get("risk", "mixed")),
                    "hot_push": int(premium_action.get("hot_push", 0) or 0),
                    "locked_deal": random.choice(("Rare Merch", "Hidden Supply", "Night Flip", "Backdoor Drop")),
                }
            )
        elif defn.key == "cartel":
            premium_state.update(
                {
                    "control": _clamp_int(70 + int(premium_action.get("control_delta", 0) or 0), 35, 100),
                    "pressure": _clamp_int(int(premium_action.get("pressure_start", 25) or 25), 0, 100),
                }
            )
        elif defn.key == "shadow_government":
            premium_state.update({"focus": str(premium_action.get("focus", "power"))})

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
        auto_restart_remaining=int(auto_restart_remaining),
        snapshot_json={
            "business_key": defn.key,
            "business_name": defn.name,
            "hourly_profit_snapshot": int(hourly_profit),
            "runtime_hours_snapshot": int(total_runtime_hours),
            "ownership_level": int(level),
            "ownership_prestige": int(prestige),
            "auto_restart_remaining": int(auto_restart_remaining),
            "started_at_iso": now.isoformat(),
            "ends_at_iso": ends_at.isoformat(),
            "run_mode": run_mode.key,
            "run_mode_label": str(premium_action.get("label")) if premium_action else run_mode.label,
            "summary_components": {
                "base_hourly_income": int(baseline_hourly_component),
                "worker_hourly_bonus": int(worker_hourly_component),
                "manager_hourly_bonus": int(manager_hourly_component),
            },
            "event_plan": event_plan,
            "premium_run": premium_state if premium_action else None,
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
            f"Hourly profit: **{hourly_profit:,}/hr**\n"
            f"Projected run profit: **{(hourly_profit * total_runtime_hours):,} per run**"
            + (f"\nSetup: **{premium_action.get('label')}** (locks in after 2 minutes)." if premium_action else "")
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
    quantity: int = 1,
    include_snapshots: bool = True,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    requested_quantity = max(int(quantity), 1)
    old_level, old_prestige = await _resolve_effective_business_progress(session, ownership=ownership)
    old_visible_level = visible_level_for(old_level)

    if at_level_cap(stored_level=old_level, prestige=old_prestige):
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(
            ok=False,
            message=(
                f"**{defn.name}** is capped at **Level {max_visible_level_for_prestige(old_prestige)}** for Prestige **{old_prestige}**.\n"
                "Prestige the business to unlock the next 10 levels."
            ),
            snapshot=hub,
            manage_snapshot=manage,
        )

    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)
    balance = int(wallet.silver or 0)
    unlock = bulk_option_for(old_prestige, requested_quantity)
    if requested_quantity > 1 and not unlock.unlocked:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        needed_prestige = 3 if requested_quantity == 5 else 10
        return BusinessActionResult(ok=False, message=f"Upgrade x{requested_quantity} unlocks at Prestige **{needed_prestige}**.", snapshot=hub, manage_snapshot=manage)

    cap_level = max_stored_level_for_prestige(old_prestige)
    affordable_cost = 0
    actual_upgrades = 0
    for lvl in range(old_level, min(old_level + requested_quantity, cap_level + 1)):
        if lvl > cap_level - 1:
            break
        cost = int(_upgrade_cost(defn, lvl))
        if balance < affordable_cost + cost:
            break
        affordable_cost += cost
        actual_upgrades += 1

    if actual_upgrades <= 0:
        next_cost = int(_upgrade_cost(defn, old_level))
        short = max(next_cost - balance, 0)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message=f"You need **{next_cost:,} Silver** to upgrade **{defn.name}**.\nYou are short by **{short:,} Silver**.", snapshot=hub, manage_snapshot=manage)

    old_hourly = await _calc_display_hourly_profit_for_owned_business(session, ownership=ownership, defn=defn)
    old_runtime_hours = await _calc_total_runtime_hours_for_owned_business(session, ownership=ownership, defn=defn)

    wallet.silver -= affordable_cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent += affordable_cost
    ownership.level = old_level + actual_upgrades
    if hasattr(ownership, "total_spent"):
        ownership.total_spent = int(ownership.total_spent or 0) + affordable_cost

    await session.flush()
    hub = None
    manage = None
    if include_snapshots:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)

    new_level, new_prestige = await _resolve_effective_business_progress(session, ownership=ownership)
    new_visible_level = visible_level_for(new_level)
    if manage is None:
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    new_hourly = int(manage.hourly_profit) if manage is not None else old_hourly
    new_runtime_hours = int(manage.total_runtime_hours) if manage is not None else old_runtime_hours
    requested_text = f"x{requested_quantity}" if requested_quantity > 1 else "x1"
    landed_at_cap = at_level_cap(stored_level=new_level, prestige=new_prestige)
    cap_suffix = "\nLevel cap reached. Prestige to keep scaling." if landed_at_cap else ""

    return BusinessActionResult(
        ok=True,
        message=(
            f"Upgraded **{defn.emoji} {defn.name}** {requested_text} from **Level {old_visible_level}** to **Level {new_visible_level}** "
            f"for **{affordable_cost:,} Silver**.\n"
            f"Hourly profit: **{old_hourly:,}/hr** → **{new_hourly:,}/hr**\n"
            f"Run projection: **{old_runtime_hours:,}h / {old_hourly * old_runtime_hours:,} Silver**"
            f" → **{new_runtime_hours:,}h / {new_hourly * new_runtime_hours:,} Silver**"
            f"{cap_suffix}"
        ),
        snapshot=hub,
        manage_snapshot=manage,
    )


async def prestige_business(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    current_level, current_prestige = await _resolve_effective_business_progress(session, ownership=ownership)
    current_visible_level = visible_level_for(current_level)
    current_max_level = max_visible_level_for_prestige(current_prestige)
    if not at_level_cap(stored_level=current_level, prestige=current_prestige):
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message=f"Reach **Level {current_max_level}** before prestiging **{defn.name}**.", snapshot=hub, manage_snapshot=manage)
    if current_prestige >= MAX_BUSINESS_PRESTIGE:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message=f"**{defn.name}** is already at the max Prestige **{MAX_BUSINESS_PRESTIGE}**.", snapshot=hub, manage_snapshot=manage)

    cost = int(_prestige_cost(defn, current_prestige))
    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)
    balance = int(wallet.silver or 0)
    if balance < cost:
        short = cost - balance
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message=f"You need **{cost:,} Silver** to prestige **{defn.name}**.\nYou are short by **{short:,} Silver**.", snapshot=hub, manage_snapshot=manage)

    old_hourly = await _calc_display_hourly_profit_for_owned_business(session, ownership=ownership, defn=defn)
    old_runtime_hours = await _calc_total_runtime_hours_for_owned_business(session, ownership=ownership, defn=defn)
    wallet.silver -= cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent += cost
    ownership.level = 0
    ownership.prestige = current_prestige + 1
    if hasattr(ownership, "total_spent"):
        ownership.total_spent = int(ownership.total_spent or 0) + cost

    await session.flush()
    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    new_prestige = clamp_prestige(int(ownership.prestige or 0))
    new_hourly = int(manage.hourly_profit) if manage is not None else old_hourly
    new_runtime_hours = int(manage.total_runtime_hours) if manage is not None else old_runtime_hours
    return BusinessActionResult(
        ok=True,
        message=(
            f"Prestiged **{defn.emoji} {defn.name}** from **Prestige {current_prestige}** to **Prestige {new_prestige}** for **{cost:,} Silver**.\n"
            f"Visible level reset: **Level {current_visible_level}** → **Level 1**\n"
            f"Output multiplier: **x{prestige_multiplier_display(current_prestige)}** → **x{prestige_multiplier_display(new_prestige)}**\n"
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

    level, _ = await _resolve_effective_business_progress(session, ownership=ownership)
    total_slots = _worker_slots_for_business_key_and_level(
        str(business_key),
        level,
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

    level, _ = await _resolve_effective_business_progress(session, ownership=ownership)
    total_slots = _manager_slots_for_level(level)
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

    roll = _generate_worker_roll()
    worker_name = str(roll["worker_name"])
    norm_type = str(roll["worker_type"])
    norm_rarity = str(roll["rarity"])
    flat_bonus = int(roll["flat_profit_bonus"])
    bp_bonus = int(roll["percent_profit_bonus_bp"])
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

    row = await session.scalar(
        select(BusinessWorkerAssignmentRow).where(
            BusinessWorkerAssignmentRow.ownership_id == int(ownership.id),
            BusinessWorkerAssignmentRow.slot_index == int(free_slot),
        )
    )
    if row is None:
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
    else:
        row.guild_id = int(guild_id)
        row.user_id = int(user_id)
        row.business_key = str(business_key)
        row.worker_name = (str(worker_name).strip() or "Worker")[:64]
        row.worker_type = norm_type
        row.rarity = norm_rarity
        row.flat_profit_bonus = flat_bonus
        row.percent_profit_bonus_bp = bp_bonus
        row.special_json = {}
        row.is_active = True
        await session.flush()

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
        hired_worker=HiredWorkerSnapshot(
            slot_index=int(free_slot),
            worker_name=str(row.worker_name),
            worker_type=str(row.worker_type),
            rarity=str(row.rarity),
            flat_profit_bonus=int(row.flat_profit_bonus or 0),
            percent_profit_bonus_bp=int(row.percent_profit_bonus_bp or 0),
            hire_cost=int(hire_cost),
        ),
    )



async def hire_worker_manual(
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
    charge_silver: bool = True,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    slots = await get_worker_assignment_slots(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    free_slot = next((s.slot_index for s in slots if not s.is_active), None)
    if free_slot is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message="All worker slots are full. Upgrade the business to unlock more slots.", snapshot=hub, manage_snapshot=manage)

    norm_type = _normalize_worker_type(worker_type)
    norm_rarity = _normalize_worker_rarity(rarity)
    flat_bonus = _clamp_int(int(flat_profit_bonus), 0, 1_000_000)
    bp_bonus = _clamp_int(int(percent_profit_bonus_bp), 0, 250_000)
    hire_cost = _worker_hire_cost(rarity=norm_rarity, flat_profit_bonus=flat_bonus, percent_profit_bonus_bp=bp_bonus) if charge_silver else 0

    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)
    if charge_silver and int(wallet.silver or 0) < hire_cost:
        short = hire_cost - int(wallet.silver or 0)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message=f"You need **{hire_cost:,} Silver** to hire this worker. Short by **{short:,} Silver**.", snapshot=hub, manage_snapshot=manage)

    row = await session.scalar(
        select(BusinessWorkerAssignmentRow).where(
            BusinessWorkerAssignmentRow.ownership_id == int(ownership.id),
            BusinessWorkerAssignmentRow.slot_index == int(free_slot),
        )
    )
    if row is None:
        row = BusinessWorkerAssignmentRow(
            ownership_id=int(ownership.id), guild_id=int(guild_id), user_id=int(user_id), business_key=str(business_key),
            slot_index=int(free_slot), worker_name=(str(worker_name).strip() or "Worker")[:64], worker_type=norm_type,
            rarity=norm_rarity, flat_profit_bonus=flat_bonus, percent_profit_bonus_bp=bp_bonus, special_json={}, is_active=True,
        )
        try:
            async with session.begin_nested():
                session.add(row)
                await session.flush()
        except IntegrityError:
            hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
            manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
            return BusinessActionResult(ok=False, message="That worker slot was taken by another action. Please try again.", snapshot=hub, manage_snapshot=manage)
    else:
        row.guild_id = int(guild_id)
        row.user_id = int(user_id)
        row.business_key = str(business_key)
        row.worker_name = (str(worker_name).strip() or "Worker")[:64]
        row.worker_type = norm_type
        row.rarity = norm_rarity
        row.flat_profit_bonus = flat_bonus
        row.percent_profit_bonus_bp = bp_bonus
        row.special_json = {}
        row.is_active = True
        await session.flush()

    if charge_silver:
        wallet.silver -= hire_cost
        if hasattr(wallet, "silver_spent"):
            wallet.silver_spent += hire_cost

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    return BusinessActionResult(
        ok=True,
        message=f"[Admin] Hired **{row.worker_name}** ({norm_rarity}) into slot **#{free_slot}** for **{hire_cost:,} Silver**.",
        snapshot=hub,
        manage_snapshot=manage,
        hired_worker=HiredWorkerSnapshot(
            slot_index=int(free_slot), worker_name=str(row.worker_name), worker_type=str(row.worker_type), rarity=str(row.rarity),
            flat_profit_bonus=int(row.flat_profit_bonus or 0), percent_profit_bonus_bp=int(row.percent_profit_bonus_bp or 0), hire_cost=int(hire_cost),
        ),
    )


async def roll_worker_candidate(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
    reroll_cost: int = WORKER_CANDIDATE_REROLL_COST,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    slots = await get_worker_assignment_slots(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    free_slot = next((s.slot_index for s in slots if not s.is_active), None)
    if free_slot is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message="All worker slots are full. Upgrade the business to unlock more slots.", snapshot=hub, manage_snapshot=manage)

    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)
    safe_cost = max(int(reroll_cost), 0)
    if int(wallet.silver or 0) < safe_cost:
        short = safe_cost - int(wallet.silver or 0)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message=f"You need **{safe_cost:,} Silver** to reroll. Short by **{short:,} Silver**.", snapshot=hub, manage_snapshot=manage)

    wallet.silver -= safe_cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent += safe_cost

    roll = _generate_worker_roll()
    candidate = WorkerCandidateSnapshot(
        worker_name=str(roll["worker_name"]),
        worker_type=str(roll["worker_type"]),
        rarity=str(roll["rarity"]),
        flat_profit_bonus=int(roll["flat_profit_bonus"]),
        percent_profit_bonus_bp=int(roll["percent_profit_bonus_bp"]),
        reroll_cost=int(safe_cost),
    )
    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    return BusinessActionResult(
        ok=True,
        message=f"Generated candidate **{candidate.worker_name}** for **{safe_cost:,} Silver**.",
        snapshot=hub,
        manage_snapshot=manage,
        worker_candidate=candidate,
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
    level, _ = await _resolve_effective_business_progress(session, ownership=ownership)
    max_slot = _worker_slots_for_business_key_and_level(str(business_key), level)
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

    roll = _generate_manager_roll()
    manager_name = str(roll["manager_name"])
    norm_rarity = str(roll["rarity"])
    runtime_bonus = _clamp_int(int(roll["runtime_bonus_hours"]), 0, 48)
    profit_bp = _clamp_int(int(roll["profit_bonus_bp"]), 0, 250_000)
    auto_restart = _clamp_int(int(roll["auto_restart_charges"]), 0, 100)
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

    row = await session.scalar(
        select(BusinessManagerAssignmentRow).where(
            BusinessManagerAssignmentRow.ownership_id == int(ownership.id),
            BusinessManagerAssignmentRow.slot_index == int(free_slot),
        )
    )
    if row is None:
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
    else:
        row.guild_id = int(guild_id)
        row.user_id = int(user_id)
        row.business_key = str(business_key)
        row.manager_name = (str(manager_name).strip() or "Manager")[:64]
        row.rarity = norm_rarity
        row.runtime_bonus_hours = runtime_bonus
        row.auto_restart_charges = auto_restart
        row.profit_bonus_bp = profit_bp
        row.special_json = {}
        row.is_active = True
        await session.flush()

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
        hired_manager=HiredManagerSnapshot(
            slot_index=int(free_slot),
            manager_name=str(row.manager_name),
            rarity=str(row.rarity),
            runtime_bonus_hours=int(row.runtime_bonus_hours or 0),
            profit_bonus_bp=int(row.profit_bonus_bp or 0),
            auto_restart_charges=int(row.auto_restart_charges or 0),
            hire_cost=int(hire_cost),
        ),
    )



async def roll_manager_candidate(
    session,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
    reroll_cost: int = MANAGER_CANDIDATE_REROLL_COST,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    slots = await get_manager_assignment_slots(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    free_slot = next((s.slot_index for s in slots if not s.is_active), None)
    if free_slot is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message="All manager slots are full. Upgrade the business to unlock more slots.", snapshot=hub, manage_snapshot=manage)

    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)
    safe_cost = max(int(reroll_cost), 0)
    if int(wallet.silver or 0) < safe_cost:
        short = safe_cost - int(wallet.silver or 0)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message=f"You need **{safe_cost:,} Silver** to reroll. Short by **{short:,} Silver**.", snapshot=hub, manage_snapshot=manage)

    wallet.silver -= safe_cost
    if hasattr(wallet, "silver_spent"):
        wallet.silver_spent += safe_cost

    roll = _generate_manager_roll()
    candidate = ManagerCandidateSnapshot(
        manager_name=str(roll["manager_name"]),
        rarity=str(roll["rarity"]),
        runtime_bonus_hours=int(roll["runtime_bonus_hours"]),
        profit_bonus_bp=int(roll["profit_bonus_bp"]),
        auto_restart_charges=int(roll["auto_restart_charges"]),
        reroll_cost=int(safe_cost),
    )
    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    return BusinessActionResult(
        ok=True,
        message=f"Generated manager candidate **{candidate.manager_name}** for **{safe_cost:,} Silver**.",
        snapshot=hub,
        manage_snapshot=manage,
        manager_candidate=candidate,
    )


async def hire_manager_manual(
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
    charge_silver: bool = True,
) -> BusinessActionResult:
    defn = _def_for_key(business_key)
    if defn is None:
        return BusinessActionResult(ok=False, message="That business does not exist.")

    ownership = await _get_ownership_row(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    if ownership is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        return BusinessActionResult(ok=False, message=f"You do not own **{defn.name}** yet.", snapshot=hub)

    slots = await get_manager_assignment_slots(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    free_slot = next((s.slot_index for s in slots if not s.is_active), None)
    if free_slot is None:
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message="All manager slots are full. Upgrade the business to unlock more slots.", snapshot=hub, manage_snapshot=manage)

    norm_rarity = _normalize_rarity(rarity)
    runtime_bonus = _clamp_int(int(runtime_bonus_hours), 0, 48)
    profit_bp = _clamp_int(int(profit_bonus_bp), 0, 250_000)
    auto_restart = _clamp_int(int(auto_restart_charges), 0, 100)
    hire_cost = _manager_hire_cost(rarity=norm_rarity, runtime_bonus_hours=runtime_bonus, profit_bonus_bp=profit_bp, auto_restart_charges=auto_restart)

    wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)
    if charge_silver and int(wallet.silver or 0) < hire_cost:
        short = hire_cost - int(wallet.silver or 0)
        hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
        manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
        return BusinessActionResult(ok=False, message=f"You need **{hire_cost:,} Silver** to hire this manager. Short by **{short:,} Silver**.", snapshot=hub, manage_snapshot=manage)

    row = await session.scalar(
        select(BusinessManagerAssignmentRow).where(
            BusinessManagerAssignmentRow.ownership_id == int(ownership.id),
            BusinessManagerAssignmentRow.slot_index == int(free_slot),
        )
    )
    if row is None:
        row = BusinessManagerAssignmentRow(
            ownership_id=int(ownership.id), guild_id=int(guild_id), user_id=int(user_id), business_key=str(business_key), slot_index=int(free_slot),
            manager_name=(str(manager_name).strip() or "Manager")[:64], rarity=norm_rarity, runtime_bonus_hours=runtime_bonus,
            auto_restart_charges=auto_restart, profit_bonus_bp=profit_bp, special_json={}, is_active=True,
        )
        try:
            async with session.begin_nested():
                session.add(row)
                await session.flush()
        except IntegrityError:
            hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
            manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
            return BusinessActionResult(ok=False, message="That manager slot was taken by another action. Please try again.", snapshot=hub, manage_snapshot=manage)
    else:
        row.guild_id = int(guild_id)
        row.user_id = int(user_id)
        row.business_key = str(business_key)
        row.manager_name = (str(manager_name).strip() or "Manager")[:64]
        row.rarity = norm_rarity
        row.runtime_bonus_hours = runtime_bonus
        row.auto_restart_charges = auto_restart
        row.profit_bonus_bp = profit_bp
        row.special_json = {}
        row.is_active = True
        await session.flush()

    if charge_silver:
        wallet.silver -= hire_cost
        if hasattr(wallet, "silver_spent"):
            wallet.silver_spent += hire_cost

    hub = await get_business_hub_snapshot(session, guild_id=guild_id, user_id=user_id)
    manage = await get_business_manage_snapshot(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
    return BusinessActionResult(
        ok=True,
        message=f"[Admin] Hired manager **{row.manager_name}** ({norm_rarity}) into slot **#{free_slot}** for **{hire_cost:,} Silver**.",
        snapshot=hub,
        manage_snapshot=manage,
        hired_manager=HiredManagerSnapshot(
            slot_index=int(free_slot), manager_name=str(row.manager_name), rarity=str(row.rarity), runtime_bonus_hours=int(row.runtime_bonus_hours or 0),
            profit_bonus_bp=int(row.profit_bonus_bp or 0), auto_restart_charges=int(row.auto_restart_charges or 0), hire_cost=int(hire_cost),
        ),
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
    level, _ = await _resolve_effective_business_progress(session, ownership=ownership)
    max_slot = _manager_slots_for_level(level)
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
