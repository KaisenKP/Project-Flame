# cogs/Business/cog.py
from __future__ import annotations

"""
Business Cog

What this file does:
- Registers the /business slash command
- Builds and renders business-related embeds
- Handles Discord buttons and select menus
- Locks interactions so only the command user can use the panel
- Calls into .core for all business data and actions
- Refreshes views after buy / run / inspect actions

What this file does NOT do:
- It does not calculate business formulas
- It does not define the real business economy
- It does not handle hourly ticking / auto-payout logic
- It does not manage database row design directly
- It does not own worker or manager logic

What this file requires from other files in this package:

1) cogs/Business/core.py
This file must expose these names:

    BusinessActionResult
    BusinessCard
    BusinessDef
    BusinessHubSnapshot
    BusinessManageSnapshot
    buy_business
    fetch_business_defs
    get_business_hub_snapshot
    get_business_manage_snapshot
    start_business_run

2) cogs/Business/runtime.py
This file is not imported directly by this cog right now.
The expectation is that core.py may call runtime.py internally when needed.

3) services.db
Must provide:
- sessions()

4) services.users
Must provide:
- ensure_user_rows(...)
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from typing import Dict, List, Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select, text

from db.models import AdminAuditLogRow, BusinessAutoHireSessionRow, BusinessManagerAssignmentRow, BusinessOwnershipRow, BusinessRunRow, BusinessWorkerAssignmentRow, BusinessWorkerMigrationStateRow, WalletRow
from services.db import sessions
from services.achievements import check_and_grant_achievements, queue_achievement_announcements
from services.users import ensure_user_rows
from services.vip import is_vip_member
from services.vip_hiring_service import VipHiringService
from services.vip_hiring_recovery import reconcile_incomplete_jobs
import os
from .runtime import BusinessRuntimeEngine, CompletedRunNotice

log = logging.getLogger(__name__)

AUTO_HIRE_MAX_REROLLS = 250
AUTO_HIRE_ALLOWED_RARITIES = {"common", "uncommon", "rare", "epic", "mythic"}
AUTO_HIRE_PROGRESS_UPDATE_INTERVAL_SECONDS = 1.0
AUTO_HIRE_PROGRESS_ROLL_CADENCE = 3
AUTO_HIRE_ROLL_DELAY_SECONDS = 0.45
VIP_REROLL_AMOUNT_OPTIONS = (1, 5, 10, 25, 50, 100)
VIP_REROLL_TARGET_OPTIONS = (1, 2, 3, 5, 10)
VIP_REROLL_TIMEOUT_SECONDS = None

_BUSINESS_ADMIN_ROLE_IDS = {int(part) for part in ((os.getenv("BUSINESS_ADMIN_ROLE_IDS") or os.getenv("BUSINESS_ADMIN_ROLE_ID") or "").replace(",", " ").split()) if part.strip().isdigit()}
RARITY_ORDER = ("common", "uncommon", "rare", "epic", "legendary", "mythical")
_PANEL_PAGE_SIZE = 5
_ASSIGNMENTS_PAGE_SIZE = 10
_ACCESS_DENIED = "Access Denied - You do not have permission to use this dashboard."

_BUSINESS_RUNTIME_STATE_PATH = Path("data/business_runtime_state.json")
_BUSINESS_REVENUE_ANNOUNCEMENT_CHANNEL_ID = 1460859446480867339

# =========================================================
# CORE CONTRACT IMPORTS
# =========================================================

try:
    from .core import (
        BusinessActionResult,
        BusinessCard,
        BusinessDef,
        BusinessHubSnapshot,
        BusinessManageSnapshot,
        HiredManagerSnapshot,
        HiredWorkerSnapshot,
        WorkerCandidateSnapshot,
        ManagerCandidateSnapshot,
        ManagerAssignmentSlotSnapshot,
        WorkerAssignmentSlotSnapshot,
        StaffCatalogEntry,
        buy_business,
        fetch_business_defs,
        get_business_hub_snapshot,
        get_business_manage_snapshot,
        get_manager_assignment_slots,
        get_worker_assignment_slots,
        hire_manager,
        hire_manager_manual,
        hire_worker,
        hire_worker_manual,
        roll_worker_candidate,
        roll_manager_candidate,
        WORKER_CANDIDATE_REROLL_COST,
        MANAGER_CANDIDATE_REROLL_COST,
        remove_manager,
        remove_worker,
        start_business_run,
        stop_business_run,
        upgrade_business,
        prestige_business,
        get_business_def_by_key,
        get_staff_grant_catalog,
        migrate_worker_system_for_all_users,
        preview_worker_migration_for_ownership,
        restore_archived_workers_for_business,
        start_all_business_runs,
        stop_all_business_runs,
        WORKER_MIGRATION_VERSION,
    )
except Exception:
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
        affordable_upgrades_now: int = 0
        upgrade_guard_text: Optional[str] = None
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

    def get_staff_grant_catalog(*, staff_kind: str, business_key: Optional[str] = None, rarity_filter: Optional[set[str]] = None) -> list[StaffCatalogEntry]:
        _ = (staff_kind, business_key, rarity_filter)
        return []

    async def start_all_business_runs(session, *, guild_id: int, user_id: int):
        _ = (session, guild_id, user_id)
        raise RuntimeError("Bulk business start is unavailable because core imports failed.")

    async def stop_all_business_runs(session, *, guild_id: int, user_id: int):
        _ = (session, guild_id, user_id)
        raise RuntimeError("Bulk business stop is unavailable because core imports failed.")

    WORKER_MIGRATION_VERSION = 0

    async def migrate_worker_system_for_all_users(session) -> dict[str, int]:
        _ = session
        return {"migrated": 0, "skipped": 0, "failed": 0}

    async def preview_worker_migration_for_ownership(session, *, ownership_id: int) -> dict:
        _ = (session, ownership_id)
        return {"ok": False, "error": "Preview is unavailable because core imports failed."}

    async def restore_archived_workers_for_business(session, *, guild_id: int, user_id: int, business_key: str, migration_version: int = 0) -> tuple[bool, str]:
        _ = (session, guild_id, user_id, business_key, migration_version)
        return False, "Restore is unavailable because core imports failed."


    async def fetch_business_defs(session) -> Sequence[BusinessDef]:
        _ = session
        return [
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
        ]

    async def get_business_hub_snapshot(session, *, guild_id: int, user_id: int) -> BusinessHubSnapshot:
        _ = session, guild_id, user_id
        defs = await fetch_business_defs(session)
        cards = [
            BusinessCard(
                key=d.key,
                name=d.name,
                emoji=d.emoji,
                owned=False,
                running=False,
                level=0,
                visible_level=1,
                total_visible_level=1,
                max_level=10,
                prestige=0,
                hourly_profit=d.base_hourly_income,
                runtime_remaining_hours=0,
                worker_slots_used=0,
                worker_slots_total=0,
                manager_slots_used=0,
                manager_slots_total=0,
                purchase_cost=d.cost_silver,
                image_url=d.image_url,
            )
            for d in defs
        ]
        return BusinessHubSnapshot(
            silver_balance=0,
            owned_count=0,
            total_count=len(cards),
            total_hourly_income_active=0,
            active_count=0,
            cards=cards,
        )

    async def get_business_manage_snapshot(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> Optional[BusinessManageSnapshot]:
        _ = guild_id, user_id
        defs = await fetch_business_defs(session)
        d = next((x for x in defs if x.key == business_key), None)
        if d is None:
            return None
        return BusinessManageSnapshot(
            key=d.key,
            name=d.name,
            emoji=d.emoji,
            description=d.description,
            flavor=d.flavor,
            owned=False,
            running=False,
            level=0,
            visible_level=1,
            total_visible_level=1,
            max_level=10,
            prestige=0,
            hourly_profit=d.base_hourly_income,
            base_hourly_income=d.base_hourly_income,
            upgrade_cost=d.base_upgrade_cost,
            runtime_remaining_hours=0,
            total_runtime_hours=4,
            worker_slots_used=0,
            worker_slots_total=0,
            manager_slots_used=0,
            manager_slots_total=0,
            image_url=d.image_url,
            banner_url=d.banner_url,
            notes=[
                "core.py is not wired yet.",
                "This is fallback preview data from cog.py.",
            ],
        )

    async def buy_business(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build buy_business(...) in cogs/Business/core.py.",
        )

    async def start_business_run(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build start_business_run(...) in cogs/Business/core.py.",
        )

    async def stop_business_run(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build stop_business_run(...) in cogs/Business/core.py.",
        )

    async def upgrade_business(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
        quantity: int | str = 1,
        include_snapshots: bool = True,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key, quantity, include_snapshots
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build upgrade_business(...) in cogs/Business/core.py.",
        )


    async def prestige_business(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> BusinessActionResult:
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build prestige_business(...) in cogs/Business/core.py.",
        )


try:
    WORKER_CANDIDATE_REROLL_COST
except NameError:
    WORKER_CANDIDATE_REROLL_COST = 500

try:
    MANAGER_CANDIDATE_REROLL_COST
except NameError:
    MANAGER_CANDIDATE_REROLL_COST = 1_000

try:
    roll_worker_candidate
except NameError:
    async def roll_worker_candidate(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
        reroll_cost: int = WORKER_CANDIDATE_REROLL_COST,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key, reroll_cost
        return BusinessActionResult(ok=False, message="Worker candidate services are not wired yet.")

try:
    roll_manager_candidate
except NameError:
    async def roll_manager_candidate(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
        reroll_cost: int = MANAGER_CANDIDATE_REROLL_COST,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key, reroll_cost
        return BusinessActionResult(ok=False, message="Manager candidate services are not wired yet.")

# =========================================================
# CONSTANTS
# =========================================================

VIEW_TIMEOUT = 180
EMBED_COLOR = discord.Color.from_rgb(88, 101, 242)
SUCCESS_COLOR = discord.Color.green()
ERROR_COLOR = discord.Color.red()
PREMIUM_ACTION_BUTTONS = {
    "liquor_store": ("Restock", "Cheap Stock", "Premium Stock"),
    "underground_market": ("Lock Deal", "Play Safe", "Take Risk"),
    "cartel": ("Collect Pressure", "Lock Down", "Expand"),
    "shadow_government": ("Build Power", "Call Favors", "Cash Out"),
}


# =========================================================
# FORMATTERS
# =========================================================

def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _fmt_compact(n: int) -> str:
    try:
        value = float(int(n))
    except Exception:
        return str(n)
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{value/1_000_000_000:.1f}B".rstrip("0").rstrip(".")
    if abs_value >= 1_000_000:
        return f"{value/1_000_000:.1f}M".rstrip("0").rstrip(".")
    if abs_value >= 1_000:
        return f"{value/1_000:.1f}k".rstrip("0").rstrip(".")
    return f"{int(value)}"


def _status_badge(running: bool, owned: bool) -> str:
    if not owned:
        return "🔒 Locked"
    if running:
        return "🟢 Running"
    return "⚪ Stopped"


def _slot_text(used: int, total: int) -> str:
    return f"{_fmt_int(used)}/{_fmt_int(total)}"


def _estimated_cycle_hours_for_card(card: BusinessCard) -> int:
    if getattr(card, "running", False):
        return max(int(getattr(card, "runtime_remaining_hours", 0) or 0), 1)
    if getattr(card, "key", "") == "shipping_company":
        return 8
    return 4


def _safe_str(v: object, fallback: str = "Unknown") -> str:
    try:
        s = str(v).strip()
        return s or fallback
    except Exception:
        return fallback


def _parse_int(value: str, default: int = 0) -> int:
    text = str(value).strip()
    if not text:
        return int(default)
    sign = -1 if text.startswith("-") else 1
    digits = text[1:] if text.startswith("-") else text
    if not digits.isdigit():
        return int(default)
    return sign * int(digits)


def _clamp_int(value: object, minimum: int, maximum: int) -> int:
    low = int(minimum)
    high = int(maximum)
    if high < low:
        low, high = high, low
    try:
        parsed = int(value)
    except Exception:
        parsed = low
    return max(low, min(parsed, high))


@dataclass(slots=True)
class AutoHireProgress:
    rerolls_used: int = 0
    hires: int = 0
    best_hit: str = "None yet"
    latest_hit: str = "None yet"
    spent_silver: int = 0
    initial_open_slots: int = 0
    slots_full: bool = False
    last_error: str = ""


def _trim(s: str, limit: int) -> str:
    if len(s) <= limit:
        return s
    if limit <= 3:
        return s[:limit]
    return s[: limit - 3] + "..."


def _chunk_field_lines(lines: Sequence[str], *, max_chars: int = 1024) -> list[str]:
    """Split line items into embed-safe field chunks."""
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for line in lines:
        text = str(line)
        if len(text) > max_chars:
            text = _trim(text, max_chars)

        sep_len = 2 if current else 0  # separator for "\n\n"
        if current and (current_len + sep_len + len(text)) > max_chars:
            chunks.append("\n\n".join(current))
            current = [text]
            current_len = len(text)
            continue

        if current:
            current_len += sep_len
        current.append(text)
        current_len += len(text)

    if current:
        chunks.append("\n\n".join(current))
    return chunks


def _showcase_image_from_cards(cards: Sequence[BusinessCard]) -> Optional[str]:
    running_owned = [c for c in cards if c.owned and c.running and c.image_url]
    if running_owned:
        return running_owned[0].image_url

    owned = [c for c in cards if c.owned and c.image_url]
    if owned:
        return owned[0].image_url

    locked = [c for c in cards if (not c.owned) and c.image_url]
    if locked:
        return locked[0].image_url

    return None


def _showcase_image_from_defs(defs: Sequence[BusinessDef]) -> Optional[str]:
    for d in defs:
        if d.image_url:
            return d.image_url
    return None


def _hub_color_for_business_key(business_key: Optional[str]) -> discord.Color:
    palette = {
        "restaurant": discord.Color.from_rgb(220, 96, 52),
        "farm": discord.Color.from_rgb(92, 163, 74),
        "nightclub": discord.Color.from_rgb(108, 92, 231),
        "liquor_store": discord.Color.from_rgb(193, 127, 68),
        "underground_market": discord.Color.from_rgb(126, 87, 194),
        "cartel": discord.Color.from_rgb(198, 40, 40),
        "shadow_government": discord.Color.from_rgb(69, 90, 100),
    }
    return palette.get(str(business_key or "").lower(), EMBED_COLOR)


def _status_chip_for_card(card: BusinessCard) -> str:
    if not card.owned:
        return "🔒 Locked"
    if card.running:
        return "🟢 Running"
    return "⬆ Ready"


def _status_chip_for_snapshot(snap: BusinessManageSnapshot) -> str:
    if not snap.owned:
        return "🔒 Locked"
    if snap.running:
        return "🟢 Running"
    return "⚪ Stopped"


def _format_short_percent_from_bp(bp: int) -> str:
    value = int(bp) / 100
    if float(value).is_integer():
        return f"{int(value)}%"
    return f"{value:.1f}%"


def _format_hours_short(hours: int) -> str:
    total = max(int(hours or 0), 0)
    if total <= 0:
        return "0h"
    days, rem = divmod(total, 24)
    if days and rem:
        return f"{days}d {rem}h"
    if days:
        return f"{days}d"
    return f"{rem}h"


def _compact_business_hint(card: BusinessCard) -> Optional[str]:
    if card.running and getattr(card, "active_event_summary", None):
        return f"⚡ {_trim(str(getattr(card, 'active_event_summary')), 28)}"
    worker_bp = int(getattr(card, "worker_bonus_bp", 0) or 0)
    if worker_bp > 0:
        return f"+{_format_short_percent_from_bp(worker_bp)} workers"
    synergy_bp = int(getattr(card, "synergy_bonus_bp", 0) or 0)
    if synergy_bp > 0:
        return f"+{_format_short_percent_from_bp(synergy_bp)} synergy"
    if card.running:
        return f"{_format_hours_short(card.runtime_remaining_hours)} left"
    return "Ready to run"


def _format_manager_summary(raw: object) -> Optional[str]:
    text = _safe_str(raw, "").replace("Manager", "").strip(" |,-")
    event_bp = int(getattr(raw, "event_bonus_bp", 0) or 0) if not isinstance(raw, str) else 0
    if text and event_bp > 0:
        return f"{_trim(text, 18)} | Events +{_format_short_percent_from_bp(event_bp)}"
    if text:
        return _trim(text, 36)
    return None


def _normalize_rarity_key(raw: object) -> str:
    key = _safe_str(raw, "").strip().lower()
    if key == "mythic":
        return "mythical"
    return key


def _rarity_order_index(raw: object) -> int:
    key = _normalize_rarity_key(raw)
    try:
        return RARITY_ORDER.index(key)
    except ValueError:
        return -1


def _build_rarity_filter_options(*, target_kind: str) -> dict[str, set[str]]:
    if target_kind == "worker":
        return {
            "any": {"common", "uncommon", "rare", "epic", "mythic"},
            "rare_only": {"rare"},
            "epic_only": {"epic"},
            "mythical_only": {"mythic"},
            "rare_plus": {"rare", "epic", "mythic"},
            "epic_plus": {"epic", "mythic"},
        }
    return {
        "any": {"common", "rare", "epic", "legendary", "mythical"},
        "rare_only": {"rare"},
        "epic_only": {"epic"},
        "mythical_only": {"mythical"},
        "rare_plus": {"rare", "epic", "legendary", "mythical"},
        "epic_plus": {"epic", "legendary", "mythical"},
    }


def _display_rarity_filter(filter_key: str) -> str:
    mapping = {
        "any": "Any rarity",
        "rare_only": "Rare only",
        "epic_only": "Epic only",
        "mythical_only": "Mythical only",
        "rare_plus": "Rare+",
        "epic_plus": "Epic+",
    }
    return mapping.get(filter_key, "Any rarity")


def _worker_matches_kind(candidate: WorkerCandidateSnapshot, kind_key: str) -> bool:
    if kind_key == "any":
        return True
    return _safe_str(getattr(candidate, "worker_type", "efficient"), "efficient").lower() == kind_key


def _manager_matches_kind(candidate: ManagerCandidateSnapshot, kind_key: str) -> bool:
    runtime = int(getattr(candidate, "runtime_bonus_hours", 0) or 0)
    profit = int(getattr(candidate, "profit_bonus_bp", 0) or 0)
    auto = int(getattr(candidate, "auto_restart_charges", 0) or 0)
    if kind_key == "any":
        return True
    if kind_key == "runtime":
        return runtime >= profit / 100 and runtime >= auto * 3
    if kind_key == "profit":
        return profit >= runtime * 100 and profit >= auto * 200
    if kind_key == "automation":
        return auto > 0 and auto * 200 >= profit and auto * 3 >= runtime
    if kind_key == "balanced":
        return runtime > 0 and profit > 0 and auto > 0
    return True


def _manager_kind_pool_possible(kind_key: str, rarity_keys: set[str]) -> bool:
    if kind_key != "automation":
        return True
    return any(r in {"rare", "epic", "legendary", "mythical"} for r in rarity_keys)


def _kind_label(target_kind: str, kind_key: str) -> str:
    if target_kind == "worker":
        return {
            "any": "Any worker type",
            "fast": "Fast workers",
            "efficient": "Efficient workers",
            "kind": "Kind workers",
        }.get(kind_key, "Any worker type")
    return {
        "any": "Any manager profile",
        "runtime": "Runtime-focused",
        "profit": "Profit-focused",
        "automation": "Automation-focused",
        "balanced": "Balanced profile",
    }.get(kind_key, "Any manager profile")




def _worker_rarity_meta(rarity: object) -> tuple[str, str, discord.Color, int]:
    key = _safe_str(rarity, "common").strip().lower()
    table = {
        "common": ("•", "Common", discord.Color.from_rgb(125, 133, 145), 0),
        "uncommon": ("◈", "Uncommon", discord.Color.from_rgb(78, 186, 114), 1),
        "rare": ("◆", "Rare", discord.Color.from_rgb(78, 141, 255), 2),
        "epic": ("⬢", "Epic", discord.Color.from_rgb(163, 92, 255), 3),
        "legendary": ("✹", "Legendary", discord.Color.from_rgb(255, 170, 64), 4),
        "mythic": ("✦", "Mythic", discord.Color.from_rgb(255, 84, 164), 5),
        "mythical": ("✦", "Mythical", discord.Color.from_rgb(255, 84, 164), 5),
    }
    return table.get(key, table["common"])


def _worker_rarity_badge(rarity: object) -> str:
    marker, label, _color, _rank = _worker_rarity_meta(rarity)
    return f"{marker} {label}"


def _worker_candidate_color(candidate: object | None = None) -> discord.Color:
    if candidate is not None:
        return _worker_rarity_meta(getattr(candidate, "rarity", None))[2]
    return discord.Color.from_rgb(88, 170, 122)


def _worker_embed_color(detail: BusinessManageSnapshot, slots: Sequence[WorkerAssignmentSlotSnapshot], candidate: object | None = None) -> discord.Color:
    if candidate is not None:
        return _worker_rarity_meta(getattr(candidate, "rarity", None))[2]
    active_rarities = [getattr(slot, "rarity", None) for slot in slots if bool(getattr(slot, "is_active", False))]
    if active_rarities:
        top = max(active_rarities, key=lambda value: _worker_rarity_meta(value)[3])
        top_color = _worker_rarity_meta(top)[2]
        if _worker_rarity_meta(top)[3] >= 2:
            return top_color
    return _hub_color_for_business_key(getattr(detail, "key", None))


def _worker_role_best_for(worker_type: object) -> str:
    kind = _safe_str(worker_type, "efficient").lower()
    mapping = {
        "fast": "Fast runs",
        "efficient": "Income growth",
        "kind": "Event boosts",
    }
    return mapping.get(kind, "Balanced growth")


def _worker_bonus_parts(candidate: object) -> list[str]:
    parts: list[str] = []
    flat_bonus = int(getattr(candidate, "flat_profit_bonus", 0) or 0)
    percent_bp = int(getattr(candidate, "percent_profit_bonus_bp", 0) or 0)
    worker_type = _safe_str(getattr(candidate, "worker_type", None), "efficient").lower()
    if flat_bonus > 0:
        parts.append(f"Income +{_fmt_int(flat_bonus)}")
    if percent_bp > 0:
        label = {"fast": "Speed", "kind": "Event Boost"}.get(worker_type, "Output")
        parts.append(f"{label} +{_format_short_percent_from_bp(percent_bp)}")
    return parts or ["No bonus"]


def _worker_summary_line(candidate: object) -> str:
    return " • ".join(_worker_bonus_parts(candidate))


def _worker_special_line(candidate: object) -> str | None:
    worker_type = _safe_str(getattr(candidate, "worker_type", None), "efficient").lower()
    mapping = {
        "fast": "Best for shorter work cycles",
        "efficient": "Best for steady income",
        "kind": "Best for event-heavy runs",
    }
    return mapping.get(worker_type)


def _worker_odds_lines() -> tuple[str, None]:
    odds = (("common", 0.58), ("uncommon", 0.24), ("rare", 0.12), ("epic", 0.05), ("mythical", 0.01))
    base = " • ".join(f"{_worker_rarity_badge(name)} {int(chance * 100)}%" for name, chance in odds)
    return f"Base Odds: {base}", None


def _worker_candidate_score(candidate: object | None) -> int:
    if candidate is None:
        return -1
    rarity_rank = _worker_rarity_meta(getattr(candidate, "rarity", None))[3]
    flat_bonus = int(getattr(candidate, "flat_profit_bonus", 0) or 0)
    percent_bp = int(getattr(candidate, "percent_profit_bonus_bp", 0) or 0)
    return rarity_rank * 100000 + flat_bonus * 10 + percent_bp


def _manager_candidate_score(candidate: object | None) -> int:
    if candidate is None:
        return -1
    rarity_rank = _manager_rarity_meta(getattr(candidate, "rarity", None))[3]
    runtime = int(getattr(candidate, "runtime_bonus_hours", 0) or 0)
    power = int(getattr(candidate, "profit_bonus_bp", 0) or 0)
    auto_run = int(getattr(candidate, "auto_restart_charges", 0) or 0)
    return rarity_rank * 100000 + runtime * 1000 + power * 10 + auto_run * 250


def _worker_compare_tags(candidate: object, current_candidate: object | None = None, slots: Sequence[WorkerAssignmentSlotSnapshot] | None = None) -> list[str]:
    tags: list[str] = []
    rarity_key = _safe_str(getattr(candidate, "rarity", None), "common").lower()
    rarity_rank = _worker_rarity_meta(rarity_key)[3]
    if rarity_rank >= 3:
        tags.append("Rare Pull")
    if rarity_key in {"mythic", "mythical"}:
        tags.append("Mythical Pull")
    score = _worker_candidate_score(candidate)
    if current_candidate is not None:
        current_score = _worker_candidate_score(current_candidate)
        if score > current_score:
            tags.append("Upgrade")
        elif score == current_score:
            tags.append("Sidegrade")
        else:
            tags.append("Weaker than Current")
    active = [slot for slot in (slots or []) if bool(getattr(slot, "is_active", False))]
    if active:
        best_owned = max(active, key=_worker_candidate_score)
        best_score = _worker_candidate_score(best_owned)
        if score > best_score:
            tags.append("New Best")
        else:
            best_type = _safe_str(getattr(best_owned, "worker_type", None), "efficient").lower()
            if _safe_str(getattr(candidate, "worker_type", None), "efficient").lower() == best_type and score > (best_score * 8 // 10):
                tags.append("Close to Best")
    flat_bonus = int(getattr(candidate, "flat_profit_bonus", 0) or 0)
    percent_bp = int(getattr(candidate, "percent_profit_bonus_bp", 0) or 0)
    if slots and active:
        if flat_bonus >= max(int(getattr(slot, "flat_profit_bonus", 0) or 0) for slot in active):
            tags.append("Best Flat")
        if percent_bp >= max(int(getattr(slot, "percent_profit_bonus_bp", 0) or 0) for slot in active):
            tags.append("Best Profit")
    if not tags and rarity_rank >= 2:
        tags.append("Solid Pull")
    return tags[:3]


def _manager_compare_tags(candidate: object, current_candidate: object | None = None, slots: Sequence[ManagerAssignmentSlotSnapshot] | None = None) -> list[str]:
    tags: list[str] = []
    rarity_key = _safe_str(getattr(candidate, "rarity", None), "common").lower()
    rarity_rank = _manager_rarity_meta(rarity_key)[3]
    if rarity_rank >= 2:
        tags.append("Rare Pull")
    if rarity_key == "mythical":
        tags.append("Mythical Pull")
    score = _manager_candidate_score(candidate)
    if current_candidate is not None:
        current_score = _manager_candidate_score(current_candidate)
        if score > current_score:
            tags.append("Better than Current")
        elif score == current_score:
            tags.append("Sidegrade")
        else:
            tags.append("Weaker than Current")
    active = [slot for slot in (slots or []) if bool(getattr(slot, "is_active", False))]
    if active:
        best_owned = max(active, key=_manager_candidate_score)
        best_score = _manager_candidate_score(best_owned)
        if score > best_score:
            tags.append("New Best")
    runtime = int(getattr(candidate, "runtime_bonus_hours", 0) or 0)
    power = int(getattr(candidate, "profit_bonus_bp", 0) or 0)
    if active:
        if runtime >= max(int(getattr(slot, "runtime_bonus_hours", 0) or 0) for slot in active):
            tags.append("Longest Runtime")
        if power >= max(int(getattr(slot, "profit_bonus_bp", 0) or 0) for slot in active):
            tags.append("Best Power")
    if not tags and rarity_rank >= 1:
        tags.append("Strong Pull")
    return tags[:3]


def _progress_bar(current: int, total: int, *, width: int = 8) -> str:
    total = max(int(total), 1)
    current = min(max(int(current), 0), total)
    filled = round((current / total) * width)
    return "█" * filled + "░" * (width - filled)

def _manager_rarity_meta(rarity: object) -> tuple[str, str, discord.Color, int]:
    key = _safe_str(rarity, "common").strip().lower()
    table = {
        "common": ("•", "Common", discord.Color.from_rgb(120, 130, 144), 0),
        "rare": ("◆", "Rare", discord.Color.from_rgb(78, 141, 255), 1),
        "epic": ("⬢", "Epic", discord.Color.from_rgb(163, 92, 255), 2),
        "legendary": ("✹", "Legendary", discord.Color.from_rgb(255, 170, 64), 3),
        "mythical": ("✦", "Mythical", discord.Color.from_rgb(255, 84, 164), 4),
    }
    return table.get(key, table["common"])


def _manager_rarity_badge(rarity: object) -> str:
    marker, label, _color, _rank = _manager_rarity_meta(rarity)
    return f"{marker} {label}"


def _manager_embed_color(detail: BusinessManageSnapshot, slots: Sequence[ManagerAssignmentSlotSnapshot], candidate: Optional[ManagerCandidateSnapshot] = None) -> discord.Color:
    if candidate is not None:
        return _manager_rarity_meta(getattr(candidate, "rarity", None))[2]
    active_rarities = [getattr(slot, "rarity", None) for slot in slots if bool(getattr(slot, "is_active", False))]
    if active_rarities:
        top = max(active_rarities, key=lambda value: _manager_rarity_meta(value)[3])
        top_color = _manager_rarity_meta(top)[2]
        if _manager_rarity_meta(top)[3] >= 2:
            return top_color
    return _hub_color_for_business_key(getattr(detail, "key", None))


def _manager_odds_lines() -> tuple[str, Optional[str]]:
    odds = (("common", 0.60), ("rare", 0.25), ("epic", 0.10), ("legendary", 0.04), ("mythical", 0.01))
    base = " • ".join(f"{_manager_rarity_badge(name)} {int(chance * 100)}%" for name, chance in odds)
    return f"Base Odds: {base}", None


def _roman_auto_run(level: int) -> str:
    numerals = {0: "0", 1: "I", 2: "II", 3: "III", 4: "IV", 5: "V"}
    value = max(int(level or 0), 0)
    return numerals.get(value, str(value))


def _format_manager_special_effects(slot: ManagerAssignmentSlotSnapshot) -> list[str]:
    effects: list[str] = []
    power_bp = int(getattr(slot, "profit_bonus_bp", 0) or 0)
    if power_bp > 0:
        effects.append(f"Profit Multiplier +{_format_short_percent_from_bp(power_bp)}")
    return effects


def _manager_highlight_map(slots: Sequence[ManagerAssignmentSlotSnapshot]) -> dict[int, str]:
    active = [slot for slot in slots if bool(getattr(slot, "is_active", False))]
    if not active:
        return {}
    highlights: dict[int, str] = {}
    highest_rarity = max(active, key=lambda slot: _manager_rarity_meta(getattr(slot, "rarity", None))[3])
    highest_power = max(active, key=lambda slot: int(getattr(slot, "profit_bonus_bp", 0) or 0))
    longest_runtime = max(active, key=lambda slot: int(getattr(slot, "runtime_bonus_hours", 0) or 0))
    best_auto = max(active, key=lambda slot: int(getattr(slot, "auto_restart_charges", 0) or 0))
    highlights[int(getattr(highest_rarity, "slot_index", 0) or 0)] = "Highest Rarity"
    if int(getattr(highest_power, "slot_index", 0) or 0) not in highlights:
        highlights[int(getattr(highest_power, "slot_index", 0) or 0)] = "Best Power"
    if int(getattr(longest_runtime, "slot_index", 0) or 0) not in highlights:
        highlights[int(getattr(longest_runtime, "slot_index", 0) or 0)] = "Longest Runtime"
    if int(getattr(best_auto, "slot_index", 0) or 0) not in highlights and int(getattr(best_auto, "auto_restart_charges", 0) or 0) > 0:
        highlights[int(getattr(best_auto, "slot_index", 0) or 0)] = "Best Auto Run"
    return highlights


def _build_manager_summary_lines(slots: Sequence[ManagerAssignmentSlotSnapshot]) -> tuple[str, Optional[str], str]:
    active = [slot for slot in slots if bool(getattr(slot, "is_active", False))]
    runtime_total = sum(int(getattr(slot, "runtime_bonus_hours", 0) or 0) for slot in active)
    power_total = sum(int(getattr(slot, "profit_bonus_bp", 0) or 0) for slot in active)
    auto_total = sum(int(getattr(slot, "auto_restart_charges", 0) or 0) for slot in active)
    highest = max((_manager_rarity_meta(getattr(slot, "rarity", None)) for slot in active), default=("•", "Common", EMBED_COLOR, 0), key=lambda item: item[3])
    summary = f"{_fmt_int(len(active))} Active • Runtime +{_fmt_int(runtime_total)}h • Power +{_fmt_int(power_total)} • Best {highest[1]}"
    special = []
    if auto_total > 0:
        special.append(f"Auto Run {_roman_auto_run(auto_total) if auto_total <= 5 else auto_total}")
    if power_total > 0:
        special.append(f"Profit Multiplier +{_format_short_percent_from_bp(power_total)}")
    return summary, (" • ".join(special[:2]) if special else None), f"{_fmt_int(len(active))}/{_fmt_int(len(slots))} Slots Filled"


def _format_spotlight_line(label: str, value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    return f"**{label}:** {text}"


# =========================================================
# EMBED BUILDERS
# =========================================================

def _base_embed(*, title: str, description: str = "", color: discord.Color = EMBED_COLOR) -> discord.Embed:
    e = discord.Embed(title=title, description=description or "", color=color)
    e.set_footer(text="Business System • Licka Store Economy")
    return e


def _author_icon_url(user: Optional[discord.abc.User]) -> Optional[str]:
    return getattr(getattr(user, "display_avatar", None), "url", None)


def _interaction_message_id(interaction: discord.Interaction) -> Optional[int]:
    msg = getattr(interaction, "message", None)
    mid = getattr(msg, "id", None)
    return int(mid) if mid is not None else None


async def _resolve_panel_message_id(interaction: discord.Interaction) -> Optional[int]:
    """Best-effort resolver for the panel message ID across interaction types."""
    panel_message_id = _interaction_message_id(interaction)
    if panel_message_id is not None:
        return panel_message_id

    try:
        original = await interaction.original_response()
    except (discord.NotFound, discord.HTTPException, AttributeError):
        return None

    oid = getattr(original, "id", None)
    return int(oid) if oid is not None else None


async def _safe_defer(interaction: discord.Interaction, *, thinking: bool = False) -> None:
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(thinking=thinking)
    except discord.HTTPException:
        log.debug("Business interaction defer failed", exc_info=True)


async def _safe_edit_panel(
    interaction: discord.Interaction,
    *,
    embed: Optional[discord.Embed] = None,
    embeds: Optional[Sequence[discord.Embed]] = None,
    view: Optional[discord.ui.View] = None,
    message_id: Optional[int] = None,
) -> bool:
    panel_message_id = message_id
    if panel_message_id is None:
        msg = getattr(interaction, "message", None)
        panel_message_id = getattr(msg, "id", None)

    if panel_message_id is None:
        await interaction.followup.send(
            "This business panel expired. Please run `/business` again.",
            ephemeral=True,
        )
        return False

    try:
        kwargs = {"message_id": int(panel_message_id), "view": view}
        if embeds:
            kwargs["embeds"] = list(embeds)
        else:
            kwargs["embed"] = embed
        await interaction.followup.edit_message(**kwargs)
        return True
    except (discord.NotFound, discord.HTTPException, AttributeError):
        log.exception(
            "Failed to edit business panel message | guild_id=%s user_id=%s message_id=%s",
            getattr(getattr(interaction, "guild", None), "id", None),
            getattr(getattr(interaction, "user", None), "id", None),
            panel_message_id,
        )
        await interaction.followup.send(
            "I couldn't update that panel. Please run `/business` again.",
            ephemeral=True,
        )
        return False


def _build_hub_embed(
    *,
    user: discord.abc.User,
    snap: BusinessHubSnapshot,
    selected_business_key: Optional[str] = None,
) -> discord.Embed:
    owned_cards = [c for c in snap.cards if c.owned]
    selected_card = next((c for c in owned_cards if c.key == selected_business_key), None) or (owned_cards[0] if owned_cards else None)
    total_prestige = sum(max(int(getattr(card, "prestige", 0) or 0), 0) for card in owned_cards)

    summary_bits = [
        f"💰 Silver {_fmt_compact(snap.silver_balance)}",
        f"📈 Active {_fmt_compact(snap.total_hourly_income_active)}/hr",
        f"🏢 Owned {_fmt_int(snap.owned_count)}",
    ]
    if owned_cards:
        summary_bits.append(f"Prestige {_fmt_int(total_prestige)}")

    e = _base_embed(
        title="Empire Hub",
        description=" • ".join(summary_bits),
        color=_hub_color_for_business_key(selected_card.key if selected_card else None),
    )
    e.set_author(
        name=_safe_str(user),
        icon_url=_author_icon_url(user),
    )

    showcase = selected_card.image_url if selected_card and selected_card.image_url else _showcase_image_from_cards(owned_cards)
    if showcase:
        e.set_thumbnail(url=showcase)

    if not owned_cards:
        e.add_field(
            name="Roster",
            value="No businesses yet.\nUse **Buy** to purchase your first business.",
            inline=False,
        )
        return e

    rows: list[str] = []
    active_event_rows: list[str] = []
    idle_hourly = 0
    projected_cycle_total = 0
    for c in owned_cards[:10]:
        marker = "▶" if selected_card and c.key == selected_card.key else "•"
        time_remaining = _format_hours_short(c.runtime_remaining_hours) if c.running else "Ready"
        status_label = "Running" if c.running else "Idle"
        if not c.running:
            idle_hourly += int(getattr(c, "hourly_profit", 0) or 0)
        projected_cycle_total += int(getattr(c, "projected_payout", int(c.hourly_profit) * _estimated_cycle_hours_for_card(c)))
        rows.append(
            f"{marker} {c.emoji} **{c.name}**\n"
            f"💰 `{_fmt_compact(c.hourly_profit)}/hr` • ⏱ `{time_remaining}` • `{status_label}`"
        )
        for event_line in list(getattr(c, "active_event_lines", []) or [])[:1]:
            clean = _safe_str(event_line, "")
            if not clean:
                continue
            rarity = "Unknown"
            event_name = clean
            effect = "Income modifier active"
            if "•" in clean:
                parts = [p.strip() for p in clean.split("•") if p.strip()]
                if len(parts) >= 2:
                    rarity = parts[0]
                    event_name = parts[1]
                if len(parts) >= 3:
                    effect = parts[2]
            active_event_rows.append(
                f"{c.emoji} **{c.name}**\n"
                f"Event: `{event_name}` • Rarity: `{rarity}`\n"
                f"Effect: `{effect}` • Time: `{time_remaining}`"
            )
    e.add_field(name="Roster", value="\n\n".join(rows), inline=False)

    if active_event_rows:
        e.add_field(name="🔥 ACTIVE EVENTS", value="\n\n".join(active_event_rows[:5]), inline=False)
    run_ratio = _progress_bar(sum(1 for card in owned_cards if card.running), max(len(owned_cards), 1), width=10)
    e.add_field(
        name="Portfolio Health",
        value=(
            f"Run Coverage: `{run_ratio}`\n"
            f"Idle Opportunity: `{_fmt_compact(idle_hourly)}/hr`\n"
            f"Projected Cycle Value: `{_fmt_compact(projected_cycle_total)}`"
        ),
        inline=False,
    )

    if selected_card:
        projected_total = int(getattr(selected_card, "projected_payout", int(selected_card.hourly_profit) * _estimated_cycle_hours_for_card(selected_card)))
        e.add_field(
            name=f"Spotlight • {selected_card.name}",
            value=(
                f"{selected_card.emoji} **{selected_card.name}**\n"
                f"💰 `{_fmt_compact(selected_card.hourly_profit)}/hr` • "
                f"⏱ `{_format_hours_short(selected_card.runtime_remaining_hours) if selected_card.running else 'Ready'}` • "
                f"`{'Running' if selected_card.running else 'Idle'}`\n"
                f"💰 Est. Total: `{_fmt_compact(projected_total)}`"
            ),
            inline=False,
        )
    hint = "Start your idle businesses."
    idle_count = sum(1 for card in owned_cards if not card.running)
    if active_event_rows:
        hint = "Check active events."
    elif idle_count == 0 and owned_cards:
        hint = "Upgrade your highest income business."
    e.add_field(name="Priority Hint", value=f"💡 {hint}", inline=False)
    return e


def _build_buy_menu_embed(
    *,
    user: discord.abc.User,
    defs: Sequence[BusinessDef],
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    available = [d for d in defs if not any(c.key == d.key and c.owned for c in snap.cards)]

    desc = (
        f"💰 **Your Silver:** `{_fmt_int(snap.silver_balance)}`\n"
        "Pick a business to buy and start earning silver."
    )
    e = _base_embed(title="🛒 Buy Business", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=_author_icon_url(user),
    )

    showcase = _showcase_image_from_defs(available)
    if showcase:
        e.set_thumbnail(url=showcase)

    if not available:
        e.add_field(
            name="All Businesses Owned",
            value="You already own every available business.",
            inline=False,
        )
        return e

    lines: list[str] = []
    for d in available[:25]:
        lines.append(
            f"{d.emoji} **{d.name}**\n"
            f"└ Cost: `{_fmt_int(d.cost_silver)}` Silver\n"
            f"└ Income: `{_fmt_int(d.base_hourly_income)}/hr`"
        )

    e.add_field(name="Available Businesses", value="\n\n".join(lines), inline=False)
    return e


def _build_worker_migration_embed(*, summary: dict) -> discord.Embed:
    old_workers = int(summary.get("old_workers", 0) or 0)
    new_workers = int(summary.get("new_workers", 0) or 0)
    slot_before = int(summary.get("slot_before", 0) or 0)
    slot_after = int(summary.get("slot_after", 0) or 0)
    power_delta = float(summary.get("estimated_power_delta_pct", 0.0) or 0.0)
    merge_lines = list(summary.get("merge_lines", []) or [])
    embed = _base_embed(
        title="✨ Worker System Upgraded",
        description=(
            "Your roster was consolidated into a new worker generation with fewer, much stronger workers.\n"
            "Sorry for the sudden change. We rebuilt the worker system to make it cleaner, stronger, and less grindy going forward."
        ),
        color=SUCCESS_COLOR,
    )
    embed.add_field(
        name="Before vs After",
        value=(
            f"Old workers: **{_fmt_int(old_workers)}**\n"
            f"New workers: **{_fmt_int(new_workers)}**\n"
            f"Worker slots: **{_fmt_int(slot_before)} → {_fmt_int(slot_after)}**\n"
            f"Estimated roster power impact: **{power_delta:+.1f}%**"
        ),
        inline=False,
    )
    if merge_lines:
        embed.add_field(
            name="Merged Summary",
            value="\n".join(f"• {line}" for line in merge_lines[:8]),
            inline=False,
        )
    embed.add_field(
        name="What changed",
        value=(
            "• Prestige now grants **+1 worker slot**.\n"
            "• Prestige now increases high-rarity worker and manager odds.\n"
            "• This migration was one-time and permanent."
        ),
        inline=False,
    )
    embed.set_footer(text="You were upgraded — your roster is now cleaner and stronger.")
    return embed


def _build_worker_migration_dry_run_embed(*, report: dict) -> discord.Embed:
    if not bool(report.get("ok", False)):
        return discord.Embed(
            title="Worker Migration Dry-Run",
            description=str(report.get("error", "Unable to build dry-run report.")),
            color=ERROR_COLOR,
        )
    e = discord.Embed(
        title="Worker Migration Dry-Run",
        description=(
            f"Ownership **#{_fmt_int(int(report.get('ownership_id', 0)))}** • "
            f"`{_safe_str(report.get('business_key', 'unknown')).replace('_', ' ')}`"
        ),
        color=INFO_COLOR,
    )
    e.add_field(
        name="Counts",
        value=(
            f"Old workers: **{_fmt_int(int(report.get('old_worker_count', 0) or 0))}**\n"
            f"Projected new workers: **{_fmt_int(int(report.get('projected_new_worker_count', 0) or 0))}**\n"
            f"Current slots: **{_fmt_int(int(report.get('current_worker_slots', 0) or 0))}**\n"
            f"Required slot floor: **{_fmt_int(int(report.get('projected_required_slot_floor', 0) or 0))}**"
        ),
        inline=False,
    )
    e.add_field(
        name="Power Snapshot (Estimated)",
        value=(
            f"Old flat/bp: **{_fmt_int(int(report.get('old_total_flat_bonus', 0) or 0))} / {_fmt_int(int(report.get('old_total_percent_bp', 0) or 0))}**\n"
            f"Projected flat/bp: **{_fmt_int(int(report.get('projected_new_total_flat_bonus', 0) or 0))} / {_fmt_int(int(report.get('projected_new_total_percent_bp', 0) or 0))}**\n"
            f"Estimated delta: **{float(report.get('estimated_power_delta_pct', 0.0) or 0.0):+.2f}%**"
        ),
        inline=False,
    )
    lines = list(report.get("merge_lines", []) or [])
    if lines:
        e.add_field(name="Merge Preview", value="\n".join(f"• {line}" for line in lines[:10]), inline=False)
    e.set_footer(text="Dry-run only: no changes were written.")
    return e


def _build_run_menu_embed(
    *,
    user: discord.abc.User,
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    owned = [c for c in snap.cards if c.owned]

    desc = "Pick one business. Keep focus on income, event, time, and status."
    e = _base_embed(title="▶️ Run Business", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=_author_icon_url(user),
    )

    showcase = _showcase_image_from_cards(owned)
    if showcase:
        e.set_thumbnail(url=showcase)

    if not owned:
        e.add_field(
            name="Nothing To Run",
            value="You do not own any businesses yet.",
            inline=False,
        )
        return e

    lines: list[str] = []
    for c in owned[:25]:
        runtime_txt = f"{_fmt_int(c.runtime_remaining_hours)}h" if c.running else "Ready"
        lines.append(
            f"{c.emoji} **{c.name}**\n"
            f"└ 💰 `{_fmt_compact(c.hourly_profit)}/hr` • 📊 `{_status_badge(c.running, c.owned)}`\n"
            f"└ ⏱ `{runtime_txt}`"
        )

    e.add_field(name="Owned Businesses", value="\n\n".join(lines), inline=False)
    return e


def _build_manage_menu_embed(
    *,
    user: discord.abc.User,
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    owned = [c for c in snap.cards if c.owned]

    desc = "Select a business to act quickly: Run, Upgrade, or Hire."
    e = _base_embed(title="🛠️ Manage Businesses", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=_author_icon_url(user),
    )

    showcase = _showcase_image_from_cards(owned)
    if showcase:
        e.set_thumbnail(url=showcase)

    if not owned:
        e.add_field(
            name="Nothing To Manage",
            value="You do not own any businesses yet.",
            inline=False,
        )
        return e

    lines: list[str] = []
    for c in owned[:25]:
        time_txt = f"{_fmt_int(c.runtime_remaining_hours)}h" if c.running else "Ready"
        lines.append(
            f"{c.emoji} **{c.name}**\n"
            f"└ 💰 `{_fmt_compact(c.hourly_profit)}/hr` • 📊 `{_status_chip_for_card(c)}`\n"
            f"└ ⏱ `{time_txt}`\n"
            f"└ 👷 `{_slot_text(c.worker_slots_used, c.worker_slots_total)}` • 🧠 `{_slot_text(c.manager_slots_used, c.manager_slots_total)}`"
        )

    e.add_field(name="Your Businesses", value="\n\n".join(lines), inline=False)
    return e


def _build_business_detail_embed(
    *,
    user: discord.abc.User,
    snap: BusinessManageSnapshot,
    show_details: bool = False,
) -> discord.Embed:
    status = "Running" if snap.running else "Idle"
    remaining = f"{_fmt_int(snap.runtime_remaining_hours)}h" if snap.running else "Ready"
    projected_total = int(getattr(snap, "projected_payout", int(snap.hourly_profit) * int(snap.total_runtime_hours)))
    event_summary = _trim(getattr(snap, "active_event_summary", "No active events"), 80)
    has_active_event = event_summary.lower() != "no active events"
    worker_bp = int(getattr(snap, "worker_bonus_bp", 0) or 0)
    worker_est = max(int(int(snap.base_hourly_income) * worker_bp / 10_000), 0)
    total_delta = max(int(snap.hourly_profit) - int(snap.base_hourly_income), 0)
    manager_est = max(total_delta - worker_est, 0)
    event_est = 0
    if has_active_event and manager_est > 0:
        event_est = max(int(manager_est * 0.35), 0)
        manager_est = max(manager_est - event_est, 0)
    mode_label = _safe_str(getattr(snap, "run_mode", None), "Standard")
    affordable_now = int(getattr(snap, "affordable_upgrades_now", 0) or 0)
    guard_text = _safe_str(getattr(snap, "upgrade_guard_text", None), "")
    if getattr(snap, "upgrade_cost", None) is not None:
        progression_upgrade_line = (
            f"Next Upgrade: `{_fmt_compact(int(snap.upgrade_cost))} Silver` • "
            f"Affordable now: `{_fmt_int(affordable_now)} levels`"
        )
    elif guard_text:
        progression_upgrade_line = f"Next Upgrade: `{guard_text}`"
    else:
        progression_upgrade_line = "Next Upgrade: `Unavailable`"

    e = _base_embed(
        title=f"{snap.emoji} {snap.name}",
        description="Clean view of what matters now.",
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))

    e.add_field(
        name="HEADER",
        value=(
            f"Status: `{status}`\n"
            f"Income/Hour: `{_fmt_compact(snap.hourly_profit)}`\n"
            f"Time Remaining: `{remaining}`\n"
            f"💰 Est. Total: `{_fmt_compact(projected_total)}`"
        ),
        inline=False,
    )
    e.add_field(
        name="MODE",
        value=(
            f"Run Mode: `{mode_label}`\n"
            f"Cycle Runtime: `{_fmt_int(snap.total_runtime_hours)}h`"
        ),
        inline=True,
    )
    e.add_field(
        name="PROGRESSION",
        value=(
            f"Level: `{_fmt_int(snap.visible_level)}/{_fmt_int(snap.max_level)}`\n"
            f"Main Multiplier: `x{snap.prestige_multiplier}`\n"
            f"{progression_upgrade_line}"
        ),
        inline=True,
    )
    if has_active_event:
        event_lines = list(getattr(snap, "active_event_lines", []) or [])
        line = event_lines[0] if event_lines else event_summary
        e.add_field(
            name="EVENT",
            value=(
                f"🔥 `{_trim(line, 110)}`\n"
                f"Time Remaining: `{remaining}`"
            ),
            inline=False,
        )

    e.add_field(
        name="INCOME BREAKDOWN",
        value=(
            f"Base: `{_fmt_compact(snap.base_hourly_income)}/hr`\n"
            f"Workers: `+{_format_short_percent_from_bp(worker_bp)}` (`+{_fmt_compact(worker_est)}/hr`)\n"
            f"Managers: `+{_fmt_compact(manager_est)}/hr`\n"
            f"Events: `+{_fmt_compact(event_est)}/hr`\n"
            f"Synergy: `+{_format_short_percent_from_bp(int(getattr(snap, 'synergy_bonus_bp', 0) or 0))}`"
        ),
        inline=False,
    )
    e.add_field(
        name="TEAM",
        value=(
            f"Workers: `{_slot_text(snap.worker_slots_used, snap.worker_slots_total)}` • {_trim(getattr(snap, 'worker_summary', 'No workers assigned'), 60)}\n"
            f"Manager: `{_slot_text(snap.manager_slots_used, snap.manager_slots_total)}` • {_trim(getattr(snap, 'manager_summary', 'No managers assigned'), 60)}"
        ),
        inline=False,
    )

    next_action = "Press **Run** to start earning."
    if snap.running:
        next_action = "Keep running for payout, or press **Cash Out** now."
    elif snap.worker_slots_used < snap.worker_slots_total:
        next_action = "Hire workers to improve income/hour."
    elif snap.manager_slots_used < snap.manager_slots_total:
        next_action = "Hire a manager to improve control and runtime."
    elif getattr(snap, "upgrade_cost", None) is not None:
        next_action = "Upgrade now to improve your base output."
    e.add_field(name="Actions", value=f"👉 {next_action}", inline=False)

    if show_details:
        progression_lines = [
            f"Workers: `{_slot_text(snap.worker_slots_used, snap.worker_slots_total)}` • {_trim(getattr(snap, 'worker_summary', 'No workers assigned'), 80)}",
            f"Managers: `{_slot_text(snap.manager_slots_used, snap.manager_slots_total)}` • {_trim(getattr(snap, 'manager_summary', 'No managers assigned'), 80)}",
            f"Synergy: `+{_format_short_percent_from_bp(int(getattr(snap, 'synergy_bonus_bp', 0) or 0))}` • {_trim(getattr(snap, 'synergy_summary', 'No synergy'), 60)}",
            f"Bulk x1/x5/x10: `{'Y' if snap.bulk_upgrade_1_unlocked else 'N'}` / `{'Y' if snap.bulk_upgrade_5_unlocked else 'N'}` / `{'Y' if snap.bulk_upgrade_10_unlocked else 'N'}`",
            f"Legacy Level: `{int(snap.total_visible_level)}`",
        ]
        if getattr(snap, "next_unlock", None):
            progression_lines.append(f"Next Unlock: `{_trim(str(getattr(snap, 'next_unlock')), 80)}`")
        e.add_field(name="View Details", value="\n".join(f"• {line}" for line in progression_lines), inline=False)

    if snap.banner_url:
        e.set_image(url=snap.banner_url)
    elif snap.image_url:
        e.set_thumbnail(url=snap.image_url)

    return e


def _worker_highlight_map(slots: Sequence[WorkerAssignmentSlotSnapshot]) -> dict[int, str]:
    active = [slot for slot in slots if bool(getattr(slot, "is_active", False))]
    if not active:
        return {}
    highlights: dict[int, str] = {}
    highest_rarity = max(active, key=lambda slot: _worker_rarity_meta(getattr(slot, "rarity", None))[3])
    best_income = max(active, key=lambda slot: int(getattr(slot, "flat_profit_bonus", 0) or 0))
    best_output = max(active, key=lambda slot: int(getattr(slot, "percent_profit_bonus_bp", 0) or 0))
    fastest = max(active, key=lambda slot: (1 if _safe_str(getattr(slot, "worker_type", None), "").lower() == "fast" else 0, int(getattr(slot, "percent_profit_bonus_bp", 0) or 0)))
    event_best = max(active, key=lambda slot: (1 if _safe_str(getattr(slot, "worker_type", None), "").lower() == "kind" else 0, int(getattr(slot, "percent_profit_bonus_bp", 0) or 0)))
    highlights[int(getattr(highest_rarity, "slot_index", 0) or 0)] = "Highest Rarity"
    if int(getattr(best_income, "slot_index", 0) or 0) not in highlights and int(getattr(best_income, "flat_profit_bonus", 0) or 0) > 0:
        highlights[int(getattr(best_income, "slot_index", 0) or 0)] = "Best Income"
    if int(getattr(best_output, "slot_index", 0) or 0) not in highlights and int(getattr(best_output, "percent_profit_bonus_bp", 0) or 0) > 0:
        highlights[int(getattr(best_output, "slot_index", 0) or 0)] = "Best Output"
    if _safe_str(getattr(fastest, "worker_type", None), "").lower() == "fast" and int(getattr(fastest, "slot_index", 0) or 0) not in highlights:
        highlights[int(getattr(fastest, "slot_index", 0) or 0)] = "Fastest"
    if _safe_str(getattr(event_best, "worker_type", None), "").lower() == "kind" and int(getattr(event_best, "slot_index", 0) or 0) not in highlights:
        highlights[int(getattr(event_best, "slot_index", 0) or 0)] = "Event Specialist"
    return highlights


def _build_worker_summary_lines(slots: Sequence[WorkerAssignmentSlotSnapshot]) -> tuple[str, Optional[str], str]:
    active = [slot for slot in slots if bool(getattr(slot, "is_active", False))]
    income_total = sum(int(getattr(slot, "flat_profit_bonus", 0) or 0) for slot in active)
    output_total = sum(int(getattr(slot, "percent_profit_bonus_bp", 0) or 0) for slot in active)
    speed_total = sum(int(getattr(slot, "percent_profit_bonus_bp", 0) or 0) for slot in active if _safe_str(getattr(slot, "worker_type", None), "").lower() == "fast")
    event_total = sum(int(getattr(slot, "percent_profit_bonus_bp", 0) or 0) for slot in active if _safe_str(getattr(slot, "worker_type", None), "").lower() == "kind")
    highest = max((_worker_rarity_meta(getattr(slot, "rarity", None)) for slot in active), default=("•", "Common", EMBED_COLOR, 0), key=lambda item: item[3])
    summary_parts = [f"{_fmt_int(len(active))} Active"]
    if income_total > 0:
        summary_parts.append(f"Income +{_fmt_int(income_total)}")
    if speed_total > 0:
        summary_parts.append(f"Speed +{_format_short_percent_from_bp(speed_total)}")
    elif output_total > 0:
        summary_parts.append(f"Output +{_format_short_percent_from_bp(output_total)}")
    summary_parts.append(f"Best {highest[1]}")
    special_parts: list[str] = []
    if output_total > 0 and speed_total > 0:
        special_parts.append(f"Total Output +{_format_short_percent_from_bp(output_total)}")
    if event_total > 0:
        special_parts.append(f"Event Boost +{_format_short_percent_from_bp(event_total)}")
    fill = f"{_fmt_int(len(active))}/{_fmt_int(len(slots))} Workers Assigned"
    return " • ".join(summary_parts), (" • ".join(special_parts[:2]) if special_parts else None), fill


def _build_worker_assignments_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    slots: Sequence[WorkerAssignmentSlotSnapshot],
    page: int = 0,
) -> discord.Embed:
    all_slots = list(slots or ())
    total_pages = max(1, (len(all_slots) + _ASSIGNMENTS_PAGE_SIZE - 1) // _ASSIGNMENTS_PAGE_SIZE)
    current_page = min(max(int(page), 0), total_pages - 1)
    start = current_page * _ASSIGNMENTS_PAGE_SIZE
    visible_slots = all_slots[start:start + _ASSIGNMENTS_PAGE_SIZE]
    summary_line, special_line, slot_fill = _build_worker_summary_lines(all_slots)
    odds_line, adjusted_odds_line = _worker_odds_lines()
    color = _worker_embed_color(detail, all_slots)
    title = f"Worker Roster • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}"
    description = f"{slot_fill}\nHire, organize, and manage your workforce. (Base 3 slots +1 per prestige)"
    e = _base_embed(title=title, description=description, color=color)
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))

    workforce_summary = [summary_line]
    if special_line:
        workforce_summary.append(special_line)
    e.add_field(name="Workforce Summary", value="\n".join(workforce_summary), inline=False)

    recruit_lines = [odds_line, f"Hire Cost: {_fmt_int(WORKER_CANDIDATE_REROLL_COST)} Silver • Reroll Cost: {_fmt_int(WORKER_CANDIDATE_REROLL_COST)} Silver"]
    if adjusted_odds_line:
        recruit_lines.insert(1, adjusted_odds_line)
    e.add_field(name="Recruit / Odds Panel", value="\n".join(recruit_lines), inline=False)

    if total_pages > 1:
        e.add_field(name="Worker Roster", value=f"Page **{_fmt_int(current_page + 1)}** of **{_fmt_int(total_pages)}**", inline=False)

    highlights = _worker_highlight_map(all_slots)
    lines: list[str] = []
    for slot in visible_slots:
        slot_index = int(getattr(slot, 'slot_index', 0) or 0)
        is_active = bool(getattr(slot, 'is_active', False))
        if is_active:
            rarity_badge = _worker_rarity_badge(getattr(slot, 'rarity', None))
            tag = highlights.get(slot_index)
            line_one = f"**#{_fmt_int(slot_index)} {_safe_str(getattr(slot, 'worker_name', None), 'Worker')}** {rarity_badge}"
            if tag:
                line_one += f" • {tag}"
            line_two = _worker_summary_line(slot)
            special = _worker_special_line(slot)
            line_three = "🟢 Active"
            if special:
                line_three += f" • {special}"
            lines.append(f"{line_one}\n{line_two}\n{line_three}")
        else:
            lines.append(
                f"**#{_fmt_int(slot_index)} Empty Slot**\n"
                "No worker assigned\n"
                "➕ Hire a worker to improve this business"
            )

    empty_text = "No worker slots unlocked." if not getattr(detail, 'worker_slots_total', 0) else "No workers assigned yet."
    if not lines:
        e.add_field(name="Worker Roster", value=empty_text, inline=False)
    else:
        chunks = _chunk_field_lines(lines)
        for idx, chunk in enumerate(chunks, start=1):
            field_name = "Worker Roster" if len(chunks) == 1 else f"Worker Roster ({idx}/{len(chunks)})"
            e.add_field(name=field_name, value=chunk, inline=False)

    e.set_footer(text="Hire to grow your workforce • Reroll to refresh candidates • Remove to free a slot")
    return e


def _build_manager_assignments_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    slots: Sequence[ManagerAssignmentSlotSnapshot],
    page: int = 0,
) -> discord.Embed:
    all_slots = list(slots or ())
    total_pages = max(1, (len(all_slots) + _ASSIGNMENTS_PAGE_SIZE - 1) // _ASSIGNMENTS_PAGE_SIZE)
    current_page = min(max(int(page), 0), total_pages - 1)
    start = current_page * _ASSIGNMENTS_PAGE_SIZE
    visible_slots = all_slots[start:start + _ASSIGNMENTS_PAGE_SIZE]
    summary_line, special_line, slot_fill = _build_manager_summary_lines(all_slots)
    odds_line, adjusted_odds_line = _manager_odds_lines()
    color = _manager_embed_color(detail, all_slots)
    title = f"Manager Roster • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}"
    description = f"{slot_fill}\nHire, reroll, and manage staff for this business. (Base 3 slots +1 per prestige)"
    e = _base_embed(title=title, description=description, color=color)
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))

    roster_summary = [summary_line]
    if special_line:
        roster_summary.append(special_line)
    e.add_field(name="Roster Summary", value="\n".join(roster_summary), inline=False)

    recruit_lines = [odds_line, f"Hire Cost: { _fmt_int(MANAGER_CANDIDATE_REROLL_COST) } Silver • Reroll Cost: { _fmt_int(MANAGER_CANDIDATE_REROLL_COST) } Silver"]
    if adjusted_odds_line:
        recruit_lines.insert(1, adjusted_odds_line)
    e.add_field(name="Recruit / Odds Panel", value="\n".join(recruit_lines), inline=False)

    if total_pages > 1:
        e.add_field(name="Manager Roster", value=f"Page **{_fmt_int(current_page + 1)}** of **{_fmt_int(total_pages)}**", inline=False)

    highlights = _manager_highlight_map(all_slots)
    lines: list[str] = []
    for slot in visible_slots:
        slot_index = int(getattr(slot, 'slot_index', 0) or 0)
        is_active = bool(getattr(slot, 'is_active', False))
        if is_active:
            tag = highlights.get(slot_index)
            line_one = f"**#{_fmt_int(slot_index)} {_safe_str(getattr(slot, 'manager_name', None), 'Manager')}** {_manager_rarity_badge(getattr(slot, 'rarity', None))}"
            if tag:
                line_one += f" • {tag}"
            line_two = (
                f"+{_fmt_int(getattr(slot, 'runtime_bonus_hours', 0))}h Runtime • "
                f"+{_fmt_int(getattr(slot, 'profit_bonus_bp', 0))} Power • "
                f"Auto Run {_roman_auto_run(getattr(slot, 'auto_restart_charges', 0))}"
            )
            special_effects = _format_manager_special_effects(slot)
            status_line = "🟢 Active"
            if special_effects:
                status_line += f" • {special_effects[0]}"
            lines.append(f"{line_one}\n{line_two}\n{status_line}")
        else:
            lines.append(
                f"**#{_fmt_int(slot_index)} Empty Slot**\n"
                "No manager assigned\n"
                "➕ Hire a manager to unlock new bonuses"
            )

    empty_text = "No manager slots unlocked." if not getattr(detail, 'manager_slots_total', 0) else "No managers assigned yet."
    if not lines:
        e.add_field(name="Manager Roster", value=empty_text, inline=False)
    else:
        chunks = _chunk_field_lines(lines)
        for idx, chunk in enumerate(chunks, start=1):
            field_name = "Manager Roster" if len(chunks) == 1 else f"Manager Roster ({idx}/{len(chunks)})"
            e.add_field(name=field_name, value=chunk, inline=False)

    e.set_footer(text="Hire to recruit new staff • Reroll to refresh the offer • Remove to free a slot")
    return e


def _build_result_embed(*, title: str, message: str, ok: bool) -> discord.Embed:
    return _base_embed(
        title=("✅ " if ok else "❌ ") + title,
        description=message,
        color=SUCCESS_COLOR if ok else ERROR_COLOR,
    )


def _summary_reaction_line(summary: dict) -> str:
    total = int(summary.get("silver_paid_total", 0))
    hours = max(int(summary.get("hours_paid_total", 0)), 1)
    avg = total / hours
    event_pos = int(summary.get("event_income_positive", 0))
    event_neg = int(summary.get("event_income_negative", 0))
    worker = int(summary.get("worker_contribution", 0))
    manager = int(summary.get("manager_contribution", 0))
    if total <= max(avg * 1.1, 2_000):
        return "yeah this run was cursed"
    if total <= max(avg * 1.4, 6_000):
        return "profit fighting for its life"
    if event_pos >= max(total * 0.3, 1) and event_pos > (event_neg * 2):
        return "ain’t no way you hit that 😭"
    if worker > manager and worker >= max(total * 0.25, 1):
        return "your workers carried HARD"
    if hours >= 10 and total >= 50_000:
        return "you made all that doing nothing 😭"
    if total >= 200_000:
        return "bro just printed money 😭"
    return "okay yeah this was clean"


def _build_run_summary_embed(*, summary: dict) -> discord.Embed:
    total = int(summary.get("silver_paid_total", 0))
    hours = max(int(summary.get("hours_paid_total", 0)), 0)
    avg = int(round(total / max(hours, 1)))
    best_event = summary.get("best_event") or {"name": "None", "delta": 0}
    worst_event = summary.get("worst_event") or {"name": "None", "delta": 0}
    peak_hour = int(summary.get("highest_single_hour_payout", 0))
    e = _base_embed(title="📊 Business Run Summary", color=discord.Color.gold())
    e.add_field(
        name="Main",
        value=(
            f"💰 Total Earned: **{_fmt_int(total)}**\n"
            f"⏱ Duration: **{_fmt_int(hours)} hours**\n"
            f"📈 Avg/hour: **{_fmt_int(avg)}**"
        ),
        inline=False,
    )
    e.add_field(
        name="🔥 Highlights",
        value=(
            f"- Best Event: **{best_event.get('name', 'None')}** (+{_fmt_int(int(best_event.get('delta', 0)))})\n"
            f"- Worst Event: **{worst_event.get('name', 'None')}** (-{_fmt_int(abs(int(worst_event.get('delta', 0))))})\n"
            f"- Peak Hour: **+{_fmt_int(peak_hour)}**"
        ),
        inline=False,
    )
    e.add_field(
        name="👷 Contributions",
        value=(
            f"- Base Income: **{_fmt_int(int(summary.get('base_contribution', 0)))}**\n"
            f"- Workers: **+{_fmt_int(int(summary.get('worker_contribution', 0)))}**\n"
            f"- Managers: **+{_fmt_int(int(summary.get('manager_contribution', 0)))}**\n"
            f"- Events: **+{_fmt_int(int(summary.get('event_income_positive', 0)))} / -{_fmt_int(int(summary.get('event_income_negative', 0)))}**"
        ),
        inline=False,
    )
    e.add_field(
        name="🎲 Events",
        value=(
            f"- Total: **{_fmt_int(int(summary.get('event_count', 0)))}**\n"
            f"- Positive: **{_fmt_int(int(summary.get('positive_events', 0)))}**\n"
            f"- Negative: **{_fmt_int(int(summary.get('negative_events', 0)))}**\n"
            f"- Highest Rarity Triggered: **{str(summary.get('highest_rarity', 'None')).title()}**"
        ),
        inline=False,
    )
    premium = dict(summary.get("premium", {}))
    if premium:
        e.add_field(
            name="👑 Premium Moments",
            value=(
                f"- Start Choice: **{premium.get('start_action', 'Standard')}**\n"
                f"- Special Moment: **{premium.get('last_moment', 'No special moment')}**\n"
                f"- Premium Impact: **{_fmt_int(int(premium.get('premium_income_delta', 0) or 0))}**\n"
                f"- Control/Power End: **{_fmt_int(int(premium.get('control_end', 0) or 0))} / {_fmt_int(int(premium.get('power_end', 0) or 0))}**"
            ),
            inline=False,
        )
    e.add_field(name="🧠", value=_summary_reaction_line(summary), inline=False)
    return e


def _build_run_detail_embed(*, summary: dict) -> discord.Embed:
    e = _base_embed(title="🧾 Detailed Run Breakdown", color=discord.Color.blurple())
    hourly = list(summary.get("hourly_breakdown", []))
    event_lines = list(summary.get("event_lines", []))
    hourly_lines = [
        f"H{int(item.get('hour_index', 0))}: {_fmt_int(int(item.get('total_payout', 0)))} ({int(item.get('event_delta', 0)):+,})"
        for item in hourly[-12:]
    ] or ["No hourly ticks recorded."]
    e.add_field(name="Per-hour Breakdown", value="\n".join(hourly_lines), inline=False)
    e.add_field(name="Event Log", value="\n".join(event_lines[:12]) if event_lines else "No events triggered.", inline=False)
    impactful = summary.get("most_impactful_factor", "base")
    e.add_field(name="Most Impactful Factor", value=f"**{str(impactful).title()}**", inline=False)
    premium = dict(summary.get("premium", {}))
    if premium:
        e.add_field(
            name="Premium Track",
            value=(
                f"Start: **{premium.get('start_action', 'Standard')}**\n"
                f"Last swing: **{premium.get('last_moment', 'No special moment')}**\n"
                f"Stock Left: **{_fmt_int(int(premium.get('stock_left', 0) or 0))}** • "
                f"Power Bank: **{_fmt_int(int(premium.get('power_bank', 0) or 0))}**"
            ),
            inline=False,
        )
    return e


class RunSummaryView(discord.ui.View):
    def __init__(self, *, owner_id: int, summary: dict):
        super().__init__(timeout=300)
        self.owner_id = int(owner_id)
        self.summary = summary

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_id:
            await interaction.response.send_message("This summary belongs to another player.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="View Detailed Breakdown", style=discord.ButtonStyle.secondary, emoji="🧾")
    async def view_detail(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_message(embed=_build_run_detail_embed(summary=self.summary), ephemeral=True)


def _build_worker_hire_result_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    hired: HiredWorkerSnapshot,
) -> discord.Embed:
    e = _base_embed(
        title=f"✅ Worker Hired • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}",
        description="Your new worker is now part of the roster.",
        color=_worker_embed_color(detail, [], hired),
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))
    e.add_field(
        name="New Worker",
        value=(
            f"**{_safe_str(hired.worker_name, 'Worker')}** {_worker_rarity_badge(hired.rarity)}\n"
            f"{_worker_summary_line(hired)}\n"
            f"🟢 Active • {_worker_special_line(hired) or _worker_role_best_for(hired.worker_type)}"
        ),
        inline=False,
    )
    e.add_field(
        name="Assignment",
        value=f"Slot **#{_fmt_int(hired.slot_index)}** • Cost **{_fmt_int(hired.hire_cost)} Silver**",
        inline=False,
    )
    e.set_footer(text="Your workforce just got stronger.")
    return e


def _build_worker_candidate_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    candidate: WorkerCandidateSnapshot,
    current_candidate: WorkerCandidateSnapshot | None = None,
    slots: Sequence[WorkerAssignmentSlotSnapshot] | None = None,
    stage_label: str = "New Candidate Found",
    status_line: str | None = None,
) -> discord.Embed:
    tags = _worker_compare_tags(candidate, current_candidate=current_candidate, slots=slots)
    rarity_badge = _worker_rarity_badge(candidate.rarity)
    odds_line, adjusted_odds_line = _worker_odds_lines()
    e = _base_embed(
        title=f"{stage_label} • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}",
        description=status_line or "A new recruit is ready to join your workforce.",
        color=_worker_embed_color(detail, list(slots or []), candidate),
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))
    recruit_lines = [odds_line]
    if adjusted_odds_line:
        recruit_lines.append(adjusted_odds_line)
    recruit_lines.append(f"Reroll Cost: **{_fmt_int(getattr(candidate, 'reroll_cost', WORKER_CANDIDATE_REROLL_COST))} Silver**")
    recruit_lines.append(f"Hire Cost: **{_fmt_int(getattr(candidate, 'reroll_cost', WORKER_CANDIDATE_REROLL_COST))} Silver**")
    e.add_field(name="Recruit / Odds Panel", value="\n".join(recruit_lines), inline=False)
    candidate_lines = [
        f"**{_safe_str(candidate.worker_name, 'Worker')}** {rarity_badge}",
        _worker_summary_line(candidate),
        f"Best For: {_worker_role_best_for(getattr(candidate, 'worker_type', None))}",
    ]
    special = _worker_special_line(candidate)
    if special:
        candidate_lines.append(special)
    e.add_field(name="Candidate Preview", value="\n".join(candidate_lines), inline=False)
    if tags:
        e.add_field(name="Highlights", value=" • ".join(tags), inline=False)
    e.set_footer(text="Hire Worker assigns this recruit right away after the roll.")
    return e


def _build_manager_candidate_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    candidate: ManagerCandidateSnapshot,
    current_candidate: ManagerCandidateSnapshot | None = None,
    slots: Sequence[ManagerAssignmentSlotSnapshot] | None = None,
    stage_label: str = "New Candidate Found",
    status_line: str | None = None,
) -> discord.Embed:
    odds_line, adjusted_odds_line = _manager_odds_lines()
    color = _manager_embed_color(detail, list(slots or []), candidate)
    rarity_badge = _manager_rarity_badge(candidate.rarity)
    power = int(getattr(candidate, 'profit_bonus_bp', 0) or 0)
    runtime = int(getattr(candidate, 'runtime_bonus_hours', 0) or 0)
    auto_run = int(getattr(candidate, 'auto_restart_charges', 0) or 0)
    best_for = 'Power spikes and long runs' if power >= 300 or runtime >= 10 else ('Balanced automation' if auto_run > 0 else 'Reliable early growth')
    tags = _manager_compare_tags(candidate, current_candidate=current_candidate, slots=slots)
    e = _base_embed(
        title=f"{stage_label} • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}",
        description=status_line or "Premium recruitment board for your next manager.",
        color=color,
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))
    recruit_lines = [odds_line]
    if adjusted_odds_line:
        recruit_lines.append(adjusted_odds_line)
    recruit_lines.append(f"Reroll Cost: **{_fmt_int(getattr(candidate, 'reroll_cost', MANAGER_CANDIDATE_REROLL_COST))} Silver**")
    recruit_lines.append(f"Hire Cost: **{_fmt_int(getattr(candidate, 'reroll_cost', MANAGER_CANDIDATE_REROLL_COST))} Silver**")
    e.add_field(name="Recruit Panel", value="\n".join(recruit_lines), inline=False)
    e.add_field(
        name="Candidate",
        value=(
            f"**{_safe_str(candidate.manager_name, 'Manager')}** {rarity_badge}\n"
            f"+{_fmt_int(runtime)}h Runtime • +{_fmt_int(power)} Power • Auto Run {_roman_auto_run(auto_run)}\n"
            f"Best For: {best_for}"
        ),
        inline=False,
    )
    if tags:
        e.add_field(name="Highlights", value=" • ".join(tags), inline=False)
    e.set_footer(text="Hire Manager assigns this candidate for free after the roll.")
    return e


def _build_manager_hire_result_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    hired: HiredManagerSnapshot,
) -> discord.Embed:
    e = _base_embed(
        title=f"✅ Manager Hired • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}",
        description="Your roster just gained a new collectible manager.",
        color=_manager_embed_color(detail, [], hired),
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))
    e.add_field(
        name="New Manager",
        value=(
            f"**{_safe_str(hired.manager_name, 'Manager')}** {_manager_rarity_badge(hired.rarity)}\n"
            f"+{_fmt_int(hired.runtime_bonus_hours)}h Runtime • +{_fmt_int(hired.profit_bonus_bp)} Power • Auto Run {_roman_auto_run(hired.auto_restart_charges)}"
        ),
        inline=False,
    )
    e.add_field(
        name="Assignment",
        value=f"Slot **#{_fmt_int(hired.slot_index)}** • Cost **{_fmt_int(hired.hire_cost)} Silver**",
        inline=False,
    )
    e.set_footer(text="Open the roster to compare rarities, bonuses, and active staff at a glance.")
    return e




# =========================================================
# VIEW BASE
# =========================================================

class BusinessBaseView(discord.ui.View):
    def __init__(self, *, cog: "BusinessCog", owner_id: int, guild_id: int):
        super().__init__(timeout=VIEW_TIMEOUT)
        self.cog = cog
        self.owner_id = int(owner_id)
        self.guild_id = int(guild_id)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_id:
            await interaction.response.send_message(
                "This business panel belongs to someone else.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self) -> None:
        for item in self.children:
            try:
                item.disabled = True
            except Exception:
                pass


# =========================================================
# SELECT MENUS
# =========================================================

class BuyBusinessSelect(discord.ui.Select):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        defs: Sequence[BusinessDef],
        snap: BusinessHubSnapshot,
    ):
        self.cog = cog
        self.owner_id = owner_id
        self.guild_id = guild_id

        owned_keys = {c.key for c in snap.cards if c.owned}
        available = [d for d in defs if d.key not in owned_keys]

        options: list[discord.SelectOption] = []
        for d in available[:25]:
            options.append(
                discord.SelectOption(
                    label=_trim(d.name, 100),
                    value=d.key,
                    description=_trim(f"Cost {_fmt_int(d.cost_silver)} • {_fmt_int(d.base_hourly_income)}/hr", 100),
                    emoji=d.emoji,
                )
            )

        if not options:
            options.append(
                discord.SelectOption(
                    label="No businesses available",
                    value="__none__",
                    description="You already own all businesses.",
                    emoji="✅",
                )
            )

        super().__init__(
            placeholder="Choose a business to buy...",
            min_values=1,
            max_values=1,
            options=options,
            disabled=(len(available) == 0),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not self.values:
            await interaction.response.send_message("No business selected.", ephemeral=True)
            return

        picked = self.values[0]
        if picked == "__none__":
            await interaction.response.send_message("There is nothing left to buy.", ephemeral=True)
            return

        await _safe_defer(interaction)
        unlocked_achievements = []

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)

                result = await buy_business(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=picked,
                )

                defs = await fetch_business_defs(session)
                hub = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )
                if result.ok:
                    unlocked_achievements = await check_and_grant_achievements(
                        session,
                        guild_id=self.guild_id,
                        user_id=self.owner_id,
                    )

        embed = _build_result_embed(
            title="Business Purchase",
            message=result.message,
            ok=result.ok,
        )
        view = BuyBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            defs=defs,
            hub_snapshot=hub,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)
        if unlocked_achievements:
            queue_achievement_announcements(
                bot=self.cog.bot,
                guild_id=self.guild_id,
                user_id=self.owner_id,
                unlocks=unlocked_achievements,
            )


class RunBusinessSelect(discord.ui.Select):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        snap: BusinessHubSnapshot,
    ):
        self.cog = cog
        self.owner_id = owner_id
        self.guild_id = guild_id

        owned = [c for c in snap.cards if c.owned]

        options: list[discord.SelectOption] = []
        for c in owned[:25]:
            desc = f"{_status_badge(c.running, c.owned)} • Profit {_fmt_int(c.hourly_profit)}/hr"
            options.append(
                discord.SelectOption(
                    label=_trim(c.name, 100),
                    value=c.key,
                    description=_trim(desc, 100),
                    emoji=c.emoji,
                )
            )

        if not options:
            options.append(
                discord.SelectOption(
                    label="No owned businesses",
                    value="__none__",
                    description="Buy a business first.",
                    emoji="🔒",
                )
            )

        super().__init__(
            placeholder="Choose a business to run...",
            min_values=1,
            max_values=1,
            options=options,
            disabled=(len(owned) == 0),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not self.values:
            await interaction.response.send_message("No business selected.", ephemeral=True)
            return

        picked = self.values[0]
        if picked == "__none__":
            await interaction.response.send_message("You do not own a business yet.", ephemeral=True)
            return

        await _safe_defer(interaction)

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)

                result = await start_business_run(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=picked,
                )
                hub = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_result_embed(
            title="Run Business",
            message=result.message,
            ok=result.ok,
        )
        view = RunBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=hub,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)


class ManageBusinessSelect(discord.ui.Select):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        snap: BusinessHubSnapshot,
    ):
        self.cog = cog
        self.owner_id = owner_id
        self.guild_id = guild_id

        owned = [c for c in snap.cards if c.owned]

        options: list[discord.SelectOption] = []
        for c in owned[:25]:
            options.append(
                discord.SelectOption(
                    label=_trim(c.name, 100),
                    value=c.key,
                    description=_trim(f"Lvl {_fmt_int(c.level)} • {_fmt_int(c.hourly_profit)}/hr • {_status_badge(c.running, c.owned)}", 100),
                    emoji=c.emoji,
                )
            )

        if not options:
            options.append(
                discord.SelectOption(
                    label="No owned businesses",
                    value="__none__",
                    description="Buy a business first.",
                    emoji="🔒",
                )
            )

        super().__init__(
            placeholder="Choose a business to manage...",
            min_values=1,
            max_values=1,
            options=options,
            disabled=(len(owned) == 0),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not self.values:
            await interaction.response.send_message("No business selected.", ephemeral=True)
            return

        picked = self.values[0]
        if picked == "__none__":
            await interaction.response.send_message("You do not own a business yet.", ephemeral=True)
            return

        await _safe_defer(interaction)

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)

                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=picked,
                )

        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return

        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=False)
        view = BusinessDetailView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            business_key=picked,
            owned=detail.owned,
            upgrade_enabled=detail.owned,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)


# =========================================================
# VIEWS
# =========================================================

class HubBusinessSelect(discord.ui.Select):
    def __init__(self, *, view: "BusinessHubView"):
        self.hub_view = view
        owned = [c for c in view.hub_snapshot.cards if c.owned]
        options: list[discord.SelectOption] = []
        for c in owned[:25]:
            options.append(
                discord.SelectOption(
                    label=_trim(c.name, 100),
                    value=c.key,
                    description=_trim(f"P{_fmt_int(c.prestige)} • Lv{_fmt_int(c.visible_level)} • {_fmt_int(c.hourly_profit)}/hr", 100),
                    emoji=c.emoji,
                    default=(c.key == view.selected_business_key),
                )
            )
        super().__init__(
            placeholder="Select spotlight business...",
            min_values=1,
            max_values=1,
            options=options or [discord.SelectOption(label="No owned businesses", value="__none__")],
            disabled=(len(owned) == 0),
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        picked = self.values[0]
        if picked == "__none__":
            await interaction.response.send_message("You do not own a business yet.", ephemeral=True)
            return
        await _safe_defer(interaction)
        self.hub_view.selected_business_key = picked
        async with self.hub_view.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(session, guild_id=self.hub_view.guild_id, user_id=self.hub_view.owner_id)
        embed = _build_hub_embed(user=interaction.user, snap=snap, selected_business_key=picked)
        view = BusinessHubView(
            cog=self.hub_view.cog,
            owner_id=self.hub_view.owner_id,
            guild_id=self.hub_view.guild_id,
            hub_snapshot=snap,
            selected_business_key=picked,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)


class BusinessHubView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        hub_snapshot: BusinessHubSnapshot,
        selected_business_key: Optional[str] = None,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.hub_snapshot = hub_snapshot
        owned_cards = [c for c in hub_snapshot.cards if c.owned]
        self.selected_business_key = selected_business_key or (owned_cards[0].key if owned_cards else None)
        self.add_item(HubBusinessSelect(view=self))
        self._configure_buttons()

    def _configure_buttons(self) -> None:
        owns_all = self.hub_snapshot.total_count > 0 and self.hub_snapshot.owned_count >= self.hub_snapshot.total_count
        has_selected = bool(self.selected_business_key)
        owned_cards = [c for c in self.hub_snapshot.cards if c.owned]
        selected_card = next((c for c in self.hub_snapshot.cards if c.key == self.selected_business_key), None)
        at_cap = bool(selected_card is not None and selected_card.visible_level >= selected_card.max_level)
        any_running = any(c.running for c in owned_cards)
        any_stopped = any(not c.running for c in owned_cards)
        self.buy_button.disabled = owns_all
        self.manage_button.disabled = not has_selected
        self.run_button.disabled = (not has_selected) or bool(selected_card and selected_card.running)
        self.stop_button.disabled = (not has_selected) or not bool(selected_card and selected_card.running)
        self.start_all_button.disabled = not any_stopped
        self.stop_all_button.disabled = not any_running
        self.workers_button.disabled = not has_selected
        self.managers_button.disabled = not has_selected
        self.upgrade_button.disabled = (not has_selected) or at_cap

    async def _load_selected_detail(self):
        if not self.selected_business_key:
            return None
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                return await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.selected_business_key,
                )

    def _set_bulk_action_loading_state(self, *, loading: bool) -> None:
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = loading

    def _build_bulk_action_loading_embed(
        self,
        *,
        user: discord.abc.User,
        action_label: str,
        progress_text: str,
    ) -> discord.Embed:
        embed = _build_hub_embed(
            user=user,
            snap=self.hub_snapshot,
            selected_business_key=self.selected_business_key,
        )
        embed.add_field(
            name=f"{action_label} in progress",
            value=(
                f"⏳ **{progress_text}**\n"
                "▰▱▱ Preparing jobs...\n"
                "Please wait while your empire updates."
            ),
            inline=False,
        )
        return embed

    async def _show_bulk_action_loading(
        self,
        interaction: discord.Interaction,
        *,
        action_label: str,
        progress_text: str,
    ) -> None:
        self._set_bulk_action_loading_state(loading=True)
        loading_embed = self._build_bulk_action_loading_embed(
            user=interaction.user,
            action_label=action_label,
            progress_text=progress_text,
        )
        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=loading_embed, view=self)
            else:
                await _safe_edit_panel(interaction, embed=loading_embed, view=self)
        except (discord.NotFound, discord.HTTPException, AttributeError):
            log.debug("Failed to render business bulk-action loading state", exc_info=True)

    @discord.ui.button(label="Manage", style=discord.ButtonStyle.secondary, emoji="🛠️", row=1)
    async def manage_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._load_selected_detail()
        if detail is None:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=detail.key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Run", style=discord.ButtonStyle.success, emoji="▶️", row=1)
    async def run_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        if not self.selected_business_key:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await start_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=self.show_details)
        embed.add_field(name="Run Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.selected_business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Cash Out", style=discord.ButtonStyle.danger, emoji="⏹️", row=1)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        if not self.selected_business_key:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await stop_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Cash Out Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.selected_business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Start All", style=discord.ButtonStyle.success, emoji="▶️", row=3)
    async def start_all_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await self._show_bulk_action_loading(
            interaction,
            action_label="Start All",
            progress_text="Starting businesses…",
        )
        started = 0
        already_running = 0
        failed = 0
        attempted = 0
        top_errors: list[str] = []
        error_message: Optional[str] = None
        try:
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    batch = await start_all_business_runs(session, guild_id=self.guild_id, user_id=self.owner_id)
                    started = int(batch.succeeded)
                    already_running = int(batch.already_in_state)
                    failed = int(batch.failed)
                    attempted = int(batch.attempted)
                    top_errors = list(batch.errors)
                    snap = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)
        except Exception:
            log.exception(
                "Start All bulk action failed | guild_id=%s user_id=%s",
                self.guild_id,
                self.owner_id,
            )
            error_message = "I couldn't start every business right now. No data was lost—please try again."
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    snap = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)

        coverage = _progress_bar(started + already_running, max(attempted, 1), width=10)
        summary = (
            f"Processed: **{attempted}**\n"
            f"Coverage: `{coverage}`\n"
            f"Started: **{started}**\n"
            f"Already running: **{already_running}**\n"
            f"Failed: **{failed}**"
        )
        embed = _build_hub_embed(user=interaction.user, snap=snap, selected_business_key=self.selected_business_key)
        embed.add_field(name="Start All Businesses", value=summary, inline=False)
        if top_errors:
            embed.add_field(name="Top Errors", value="\n".join(f"• {line}" for line in top_errors[:3]), inline=False)
        if error_message:
            embed.add_field(name="⚠️ Bulk Action Error", value=error_message, inline=False)
        view = BusinessHubView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
            selected_business_key=self.selected_business_key,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Stop All", style=discord.ButtonStyle.danger, emoji="⛔", row=3)
    async def stop_all_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await self._show_bulk_action_loading(
            interaction,
            action_label="Cash Out All",
            progress_text="Stopping businesses…",
        )
        stopped = 0
        already_stopped = 0
        failed = 0
        attempted = 0
        top_errors: list[str] = []
        error_message: Optional[str] = None
        try:
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    batch = await stop_all_business_runs(session, guild_id=self.guild_id, user_id=self.owner_id)
                    stopped = int(batch.succeeded)
                    already_stopped = int(batch.already_in_state)
                    failed = int(batch.failed)
                    attempted = int(batch.attempted)
                    top_errors = list(batch.errors)
                    snap = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)
        except Exception:
            log.exception(
                "Stop All bulk action failed | guild_id=%s user_id=%s",
                self.guild_id,
                self.owner_id,
            )
            error_message = "I couldn't stop every business right now. Your controls were safely restored."
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    snap = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)

        coverage = _progress_bar(stopped + already_stopped, max(attempted, 1), width=10)
        summary = (
            f"Processed: **{attempted}**\n"
            f"Coverage: `{coverage}`\n"
            f"Stopped: **{stopped}**\n"
            f"Already stopped: **{already_stopped}**\n"
            f"Failed: **{failed}**"
        )
        embed = _build_hub_embed(user=interaction.user, snap=snap, selected_business_key=self.selected_business_key)
        embed.add_field(name="Cash Out All Businesses", value=summary, inline=False)
        if top_errors:
            embed.add_field(name="Top Errors", value="\n".join(f"• {line}" for line in top_errors[:3]), inline=False)
        if error_message:
            embed.add_field(name="⚠️ Bulk Action Error", value=error_message, inline=False)
        view = BusinessHubView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
            selected_business_key=self.selected_business_key,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Workers", style=discord.ButtonStyle.secondary, emoji="👷", row=2)
    async def workers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._load_selected_detail()
        if detail is None:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                slots = await get_worker_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=detail.key)
        embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        panel_message_id = await _resolve_panel_message_id(interaction)
        if panel_message_id is None:
            await interaction.followup.send("This business panel expired. Please run `/business` again.", ephemeral=True)
            return
        view = WorkerAssignmentsView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=detail.key, panel_message_id=panel_message_id, requester=interaction.user)
        view._sync_pagination_buttons(total_slots=len(slots))
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Manager", style=discord.ButtonStyle.secondary, emoji="👤", row=2)
    async def managers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._load_selected_detail()
        if detail is None:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                slots = await get_manager_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=detail.key)
        embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        panel_message_id = await _resolve_panel_message_id(interaction)
        if panel_message_id is None:
            await interaction.followup.send("This business panel expired. Please run `/business` again.", ephemeral=True)
            return
        view = ManagerAssignmentsView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=detail.key, panel_message_id=panel_message_id, requester=interaction.user)
        view._sync_pagination_buttons(total_slots=len(slots))
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Upgrade", style=discord.ButtonStyle.primary, emoji="⬆️", row=2)
    async def upgrade_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        if not self.selected_business_key:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await upgrade_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key, include_snapshots=False)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Upgrade Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.selected_business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Buy", style=discord.ButtonStyle.success, emoji="🛒", row=4)
    async def buy_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                defs = await fetch_business_defs(session)
                snap = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)
        embed = _build_buy_menu_embed(user=interaction.user, defs=defs, snap=snap)
        view = BuyBusinessView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, defs=defs, hub_snapshot=snap)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=4)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)
        embed = _build_hub_embed(user=interaction.user, snap=snap, selected_business_key=self.selected_business_key)
        view = BusinessHubView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, hub_snapshot=snap, selected_business_key=self.selected_business_key)
        await _safe_edit_panel(interaction, embed=embed, view=view)


class BuyBusinessView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        defs: Sequence[BusinessDef],
        hub_snapshot: BusinessHubSnapshot,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.defs = list(defs)
        self.hub_snapshot = hub_snapshot
        self.add_item(
            BuyBusinessSelect(
                cog=cog,
                owner_id=owner_id,
                guild_id=guild_id,
                defs=self.defs,
                snap=hub_snapshot,
            )
        )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                defs = await fetch_business_defs(session)
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_buy_menu_embed(user=interaction.user, defs=defs, snap=snap)
        view = BuyBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            defs=defs,
            hub_snapshot=snap,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)


class RunBusinessView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        hub_snapshot: BusinessHubSnapshot,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.hub_snapshot = hub_snapshot
        self.add_item(
            RunBusinessSelect(
                cog=cog,
                owner_id=owner_id,
                guild_id=guild_id,
                snap=hub_snapshot,
            )
        )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_run_menu_embed(user=interaction.user, snap=snap)
        view = RunBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)


class ManageBusinessView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        hub_snapshot: BusinessHubSnapshot,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.hub_snapshot = hub_snapshot
        self.add_item(
            ManageBusinessSelect(
                cog=cog,
                owner_id=owner_id,
                guild_id=guild_id,
                snap=hub_snapshot,
            )
        )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_manage_menu_embed(user=interaction.user, snap=snap)
        view = ManageBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)


class BusinessDetailView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        business_key: str,
        owned: Optional[bool] = None,
        upgrade_enabled: Optional[bool] = None,
        detail: Optional[BusinessManageSnapshot] = None,
        show_details: bool = False,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key
        self.show_details = bool(show_details)
        self.status_button.label = "Hide Details" if self.show_details else "View Details"
        is_enabled = bool(upgrade_enabled) if upgrade_enabled is not None else bool(owned)
        self.upgrade_button.disabled = (not is_enabled) or bool(getattr(detail, "upgrade_cost", None) is None)
        self.upgrade_max_button.disabled = (not is_enabled) or bool(getattr(detail, "upgrade_cost", None) is None)
        self.upgrade_5_button.disabled = (not is_enabled) or (not bool(getattr(detail, "bulk_upgrade_5_unlocked", False))) or bool(getattr(detail, "upgrade_cost", None) is None)
        self.upgrade_10_button.disabled = (not is_enabled) or (not bool(getattr(detail, "bulk_upgrade_10_unlocked", False))) or bool(getattr(detail, "upgrade_cost", None) is None)
        self.prestige_button.disabled = (not is_enabled) or (not bool(getattr(detail, "can_prestige", False)))
        if detail is not None and not bool(getattr(detail, "can_prestige", False)):
            self.remove_item(self.prestige_button)
        self.workers_button.disabled = not is_enabled
        self.managers_button.disabled = not is_enabled
        self.run_button.disabled = (not is_enabled) or bool(getattr(detail, 'running', False))
        self.stop_button.disabled = (not is_enabled) or not bool(getattr(detail, 'running', False))
        self.run_safe_button.disabled = self.run_button.disabled
        self.run_aggressive_button.disabled = self.run_button.disabled or int(getattr(detail, 'level', 0) or 0) < 50
        labels = PREMIUM_ACTION_BUTTONS.get(str(business_key))
        if labels:
            self.run_button.label = labels[0]
            self.run_safe_button.label = labels[1]
            self.run_aggressive_button.label = labels[2]
            self.run_aggressive_button.disabled = self.run_button.disabled

    async def _reload_detail(self):
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                return await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )

    @discord.ui.button(label="Run Business", style=discord.ButtonStyle.success, emoji="▶️", row=0)
    async def run_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await start_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Run Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail, show_details=self.show_details)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Run Safe", style=discord.ButtonStyle.secondary, emoji="🛡️", row=0)
    async def run_safe_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await start_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key, run_mode_key="safe")
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=self.show_details)
        embed.add_field(name="Run Safe", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail, show_details=self.show_details)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Run Aggressive", style=discord.ButtonStyle.primary, emoji="🔥", row=0)
    async def run_aggressive_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await start_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key, run_mode_key="aggressive")
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=self.show_details)
        embed.add_field(name="Run Aggressive", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail, show_details=self.show_details)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Stop Business", style=discord.ButtonStyle.danger, emoji="⏹️", row=0)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await stop_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=self.show_details)
        embed.add_field(name="Stop Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail, show_details=self.show_details)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="View Details", style=discord.ButtonStyle.secondary, emoji="🧾", row=0)
    async def status_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._reload_detail()
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        next_show_details = not self.show_details
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=next_show_details)
        embed.add_field(
            name="View Details",
            value="Expanded details shown." if next_show_details else "Showing clean summary view.",
            inline=False,
        )
        view = BusinessDetailView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            business_key=self.business_key,
            owned=detail.owned,
            detail=detail,
            show_details=next_show_details,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Upgrade Business", style=discord.ButtonStyle.primary, emoji="⬆️", row=1)
    async def upgrade_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await upgrade_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key, include_snapshots=False)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=self.show_details)
        embed.add_field(name="Upgrade Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail, show_details=self.show_details)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Upgrade Max", style=discord.ButtonStyle.primary, emoji="⏫", row=1)
    async def upgrade_max_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await upgrade_business(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                    quantity="max",
                    include_snapshots=True,
                )
                detail = result.manage_snapshot or await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )
                if result.snapshot is None:
                    await get_business_hub_snapshot(
                        session,
                        guild_id=self.guild_id,
                        user_id=self.owner_id,
                    )
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=self.show_details)
        embed.add_field(name="Upgrade Max", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail, show_details=self.show_details)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Upgrade x5", style=discord.ButtonStyle.primary, emoji="5️⃣", row=1)
    async def upgrade_5_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await upgrade_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key, quantity=5, include_snapshots=False)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=self.show_details)
        embed.add_field(name="Upgrade x5", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail, show_details=self.show_details)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Upgrade x10", style=discord.ButtonStyle.primary, emoji="🔟", row=1)
    async def upgrade_10_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await upgrade_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key, quantity=10, include_snapshots=False)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=self.show_details)
        embed.add_field(name="Upgrade x10", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail, show_details=self.show_details)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Prestige Business", style=discord.ButtonStyle.success, emoji="🌟", row=2)
    async def prestige_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await prestige_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail, show_details=self.show_details)
        embed.add_field(name="Prestige Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail, show_details=self.show_details)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="View Workers", style=discord.ButtonStyle.secondary, emoji="👷", row=3, disabled=True)
    async def workers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        panel_message_id = await _resolve_panel_message_id(interaction)
        if panel_message_id is None:
            await interaction.followup.send("This business panel expired. Please run `/business` again.", ephemeral=True)
            return
        view = WorkerAssignmentsView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, panel_message_id=panel_message_id, requester=interaction.user)
        view._sync_pagination_buttons(total_slots=len(slots))
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="View Managers", style=discord.ButtonStyle.secondary, emoji="🧑‍💼", row=3, disabled=True)
    async def managers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        panel_message_id = await _resolve_panel_message_id(interaction)
        if panel_message_id is None:
            await interaction.followup.send("This business panel expired. Please run `/business` again.", ephemeral=True)
            return
        view = ManagerAssignmentsView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, panel_message_id=panel_message_id, requester=interaction.user)
        view._sync_pagination_buttons(total_slots=len(slots))
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Back to Business Hub", style=discord.ButtonStyle.secondary, emoji="⬅️", row=4)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                hub = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)
        embed = _build_hub_embed(user=interaction.user, snap=hub, selected_business_key=self.business_key)
        view = BusinessHubView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, hub_snapshot=hub, selected_business_key=self.business_key)
        await _safe_edit_panel(interaction, embed=embed, view=view)



class RemoveStaffModal(discord.ui.Modal):
    def __init__(self, view: "WorkerAssignmentsView | ManagerAssignmentsView", *, staff_kind: str):
        self.parent_view = view
        self.staff_kind = "manager" if str(staff_kind).strip().lower() == "manager" else "worker"
        title = "Fire Manager" if self.staff_kind == "manager" else "Fire Worker"
        super().__init__(title=title)
        self.slot_index = discord.ui.TextInput(
            label=f"Reply with {self.staff_kind} slot #",
            placeholder="1",
            max_length=4,
        )
        self.add_item(self.slot_index)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        requested_slot = _parse_int(str(self.slot_index.value), 0)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=self.parent_view.guild_id,
                    user_id=self.parent_view.owner_id,
                    business_key=self.parent_view.business_key,
                )
                if self.staff_kind == "manager":
                    slots = await get_manager_assignment_slots(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                    )
                else:
                    slots = await get_worker_assignment_slots(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                    )
        if detail is None:
            await interaction.response.send_message("That business could not be found.", ephemeral=True)
            return
        selected_slot = next((slot for slot in slots if int(getattr(slot, "slot_index", 0)) == int(requested_slot)), None)
        if not selected_slot or not bool(getattr(selected_slot, "is_active", False)):
            await _safe_defer(interaction)
            embed = (
                _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.parent_view.page)
                if self.staff_kind == "manager"
                else _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.parent_view.page)
            )
            embed.add_field(
                name="Action",
                value=f"❌ No active {self.staff_kind} found in slot **#{_fmt_int(requested_slot)}**.",
                inline=False,
            )
            await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
            return

        display_name = _safe_str(
            getattr(selected_slot, "manager_name", None) if self.staff_kind == "manager" else getattr(selected_slot, "worker_name", None),
            "Manager" if self.staff_kind == "manager" else "Worker",
        )
        rarity = _safe_str(getattr(selected_slot, "rarity", None), "common")
        embed = discord.Embed(
            title=f"Confirm {self.staff_kind.title()} Removal",
            description=(
                f"You are firing {self.staff_kind} **{display_name}** from slot **#{_fmt_int(requested_slot)}**.\n"
                "Are you sure you want to continue?"
            ),
            color=discord.Color.orange(),
        )
        if self.staff_kind == "manager":
            runtime_bonus = _fmt_int(getattr(selected_slot, "runtime_bonus_hours", 0))
            profit_bonus = _fmt_int(getattr(selected_slot, "profit_bonus_bp", 0))
            embed.add_field(name="Manager", value=f"{display_name} ({rarity})", inline=True)
            embed.add_field(name="Bonuses", value=f"+{runtime_bonus}h runtime • +{profit_bonus} bp", inline=True)
        else:
            worker_type = _safe_str(getattr(selected_slot, "worker_type", None), "efficient")
            embed.add_field(name="Worker", value=f"{display_name} ({rarity})", inline=True)
            embed.add_field(name="Type", value=worker_type, inline=True)
        await interaction.response.send_message(
            embed=embed,
            ephemeral=True,
            view=ConfirmStaffRemovalView(
                parent_view=self.parent_view,
                slot_index=int(requested_slot),
                staff_kind=self.staff_kind,
            ),
        )


class ConfirmStaffRemovalView(discord.ui.View):
    def __init__(self, *, parent_view: "WorkerAssignmentsView | ManagerAssignmentsView", slot_index: int, staff_kind: str):
        super().__init__(timeout=60)
        self.parent_view = parent_view
        self.slot_index = int(slot_index)
        self.staff_kind = "manager" if str(staff_kind).strip().lower() == "manager" else "worker"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This confirmation belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Fire", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                if self.staff_kind == "manager":
                    result = await remove_manager(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        slot_index=self.slot_index,
                    )
                    slots = await get_manager_assignment_slots(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                    )
                else:
                    result = await remove_worker(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        slot_index=self.slot_index,
                    )
                    slots = await get_worker_assignment_slots(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                    )
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=self.parent_view.guild_id,
                    user_id=self.parent_view.owner_id,
                    business_key=self.parent_view.business_key,
                )
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        self.parent_view._sync_pagination_buttons(total_slots=len(slots))
        embed = (
            _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.parent_view.page)
            if self.staff_kind == "manager"
            else _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.parent_view.page)
        )
        embed.add_field(name="Action", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
        await interaction.edit_original_response(content=f"{self.staff_kind.title()} fired." if result.ok else "No changes made.", embed=None, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.edit_message(content=f"{self.staff_kind.title()} removal cancelled.", embed=None, view=None)


class VIPRerollSetupView(discord.ui.View):
    def __init__(self, *, parent_view: "WorkerAssignmentsView | ManagerAssignmentsView", target_kind: str):
        super().__init__(timeout=VIP_REROLL_TIMEOUT_SECONDS)
        self.parent_view = parent_view
        self.target_kind = target_kind
        self.processing = False
        self.rarity_filter_key = "any"
        self.kind_key = "any"
        self.amount_key = "1"
        self.target_hires_key = "max"
        self._message: Optional[discord.Message] = None
        self._build_controls()

    def _build_controls(self) -> None:
        rarity_select = discord.ui.Select(placeholder="Rarity filter", min_values=1, max_values=1, row=0, options=[discord.SelectOption(label="Any rarity", value="any"), discord.SelectOption(label="Rare only", value="rare_only"), discord.SelectOption(label="Epic only", value="epic_only"), discord.SelectOption(label="Mythical only", value="mythical_only"), discord.SelectOption(label="Rare+", value="rare_plus"), discord.SelectOption(label="Epic+", value="epic_plus")])
        rarity_select.callback = self._on_rarity_change
        self.add_item(rarity_select)
        if self.target_kind == "worker":
            kind_options = [discord.SelectOption(label="Any worker type", value="any"), discord.SelectOption(label="Fast", value="fast"), discord.SelectOption(label="Efficient", value="efficient"), discord.SelectOption(label="Kind", value="kind")]
            kind_placeholder = "Worker kind"
        else:
            kind_options = [discord.SelectOption(label="Any manager profile", value="any"), discord.SelectOption(label="Runtime focused", value="runtime"), discord.SelectOption(label="Profit focused", value="profit"), discord.SelectOption(label="Automation focused", value="automation"), discord.SelectOption(label="Balanced", value="balanced")]
            kind_placeholder = "Manager kind"
        kind_select = discord.ui.Select(placeholder=kind_placeholder, min_values=1, max_values=1, row=1, options=kind_options)
        kind_select.callback = self._on_kind_change
        self.add_item(kind_select)
        amount_options = [discord.SelectOption(label=str(v), value=str(v)) for v in VIP_REROLL_AMOUNT_OPTIONS]
        amount_options.append(discord.SelectOption(label="Max available", value="max"))
        amount_select = discord.ui.Select(placeholder="Reroll amount", min_values=1, max_values=1, row=2, options=amount_options)
        amount_select.callback = self._on_amount_change
        self.add_item(amount_select)
        target_options = [discord.SelectOption(label="Fill all open slots", value="max")]
        target_options.extend(discord.SelectOption(label=f"{v} hire{'s' if v != 1 else ''}", value=str(v)) for v in VIP_REROLL_TARGET_OPTIONS)
        target_select = discord.ui.Select(placeholder="Stop after hires", min_values=1, max_values=1, row=3, options=target_options)
        target_select.callback = self._on_target_hires_change
        self.add_item(target_select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This VIP reroll panel belongs to someone else.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True
        if self._message is not None:
            try:
                await self._message.edit(content="VIP reroll setup timed out. Open it again from Auto-Hire (VIP).", view=self)
            except Exception:
                pass

    async def _resolve_wallet_and_rerolls(self) -> tuple[int, int, int, int]:
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                wallet = await session.scalar(select(WalletRow).where(WalletRow.guild_id == int(self.parent_view.guild_id), WalletRow.user_id == int(self.parent_view.owner_id)))
                manage = await get_business_manage_snapshot(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
        silver = int(getattr(wallet, "silver", 0) or 0) if wallet is not None else 0
        unit_cost = WORKER_CANDIDATE_REROLL_COST if self.target_kind == "worker" else MANAGER_CANDIDATE_REROLL_COST
        if self.target_kind == "worker":
            slots_total = int(getattr(manage, "worker_slots_total", 0) or 0) if manage is not None else 0
            slots_used = int(getattr(manage, "worker_slots_used", 0) or 0) if manage is not None else 0
        else:
            slots_total = int(getattr(manage, "manager_slots_total", 0) or 0) if manage is not None else 0
            slots_used = int(getattr(manage, "manager_slots_used", 0) or 0) if manage is not None else 0
        open_slots = max(slots_total - slots_used, 0)
        return silver, unit_cost, max(silver // max(unit_cost, 1), 0), open_slots

    def _selected_rarity_keys(self) -> set[str]:
        filters = _build_rarity_filter_options(target_kind=self.target_kind)
        return set(filters.get(self.rarity_filter_key, filters["any"]))

    async def _build_summary_embed(self) -> discord.Embed:
        silver, unit_cost, owned, open_slots = await self._resolve_wallet_and_rerolls()
        selected = self._selected_reroll_amount(owned=owned)
        target_hires = self._selected_hire_goal(open_slots=open_slots)
        remaining = max(owned - selected, 0)
        title_target = "Worker" if self.target_kind == "worker" else "Manager"
        embed = discord.Embed(title=f"VIP {title_target} Reroll Setup", description="Tune your rerolls, then confirm. Nothing is spent until you press **Confirm**.", color=discord.Color.gold())
        embed.add_field(name="Target type", value=title_target, inline=True)
        embed.add_field(name="Business", value=f"`{self.parent_view.business_key}`", inline=True)
        embed.add_field(name="Kind", value=_kind_label(self.target_kind, self.kind_key), inline=True)
        embed.add_field(name="Rarity filter", value=_display_rarity_filter(self.rarity_filter_key), inline=True)
        embed.add_field(name="Rerolls owned", value=f"**{_fmt_int(owned)}**", inline=True)
        embed.add_field(name="Rerolls selected", value=f"**{_fmt_int(selected)}**", inline=True)
        embed.add_field(name="Open slots", value=f"**{_fmt_int(open_slots)}**", inline=True)
        embed.add_field(name="Stop after hires", value=f"**{_fmt_int(target_hires)}**", inline=True)
        embed.add_field(name="Rerolls remaining", value=f"**{_fmt_int(remaining)}**", inline=True)
        embed.add_field(name="Cost per reroll", value=f"**{_fmt_int(unit_cost)} Silver**", inline=True)
        embed.add_field(name="Expected remaining Silver", value=f"**{_fmt_int(max(silver - (selected * unit_cost), 0))}**", inline=True)
        embed.add_field(name="Filtered pool", value=f"`{', '.join(sorted(self._selected_rarity_keys()))}`", inline=False)
        embed.set_footer(text=f"Per run cap: {_fmt_int(AUTO_HIRE_MAX_REROLLS)} rerolls")
        return embed

    def _selected_reroll_amount(self, *, owned: int) -> int:
        if self.amount_key == "max":
            return min(max(int(owned), 0), AUTO_HIRE_MAX_REROLLS)
        selected = _clamp_int(_parse_int(self.amount_key, 1), 1, AUTO_HIRE_MAX_REROLLS)
        return min(selected, max(int(owned), 0))

    @staticmethod
    def _is_slots_full_message(message: str) -> bool:
        normalized = _safe_str(message, "").strip().lower()
        return "slots are full" in normalized

    async def _refresh_message(self, interaction: discord.Interaction) -> None:
        embed = await self._build_summary_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    async def _on_rarity_change(self, interaction: discord.Interaction) -> None:
        self.rarity_filter_key = interaction.data.get("values", ["any"])[0]  # type: ignore[union-attr]
        await self._refresh_message(interaction)

    async def _on_kind_change(self, interaction: discord.Interaction) -> None:
        self.kind_key = interaction.data.get("values", ["any"])[0]  # type: ignore[union-attr]
        await self._refresh_message(interaction)

    async def _on_amount_change(self, interaction: discord.Interaction) -> None:
        self.amount_key = interaction.data.get("values", ["1"])[0]  # type: ignore[union-attr]
        await self._refresh_message(interaction)

    async def _on_target_hires_change(self, interaction: discord.Interaction) -> None:
        self.target_hires_key = interaction.data.get("values", ["max"])[0]  # type: ignore[union-attr]
        await self._refresh_message(interaction)

    def _selected_hire_goal(self, *, open_slots: int) -> int:
        open_slots = max(int(open_slots), 0)
        if open_slots <= 0:
            return 0
        if self.target_hires_key == "max":
            return open_slots
        selected = _clamp_int(_parse_int(self.target_hires_key, 1), 1, open_slots)
        return min(selected, open_slots)

    def _candidate_matches_filters(self, candidate: WorkerCandidateSnapshot | ManagerCandidateSnapshot) -> bool:
        rarity_ok = _normalize_rarity_key(getattr(candidate, "rarity", "")) in {_normalize_rarity_key(r) for r in self._selected_rarity_keys()}
        if not rarity_ok:
            return False
        if self.target_kind == "worker":
            return _worker_matches_kind(candidate, self.kind_key)  # type: ignore[arg-type]
        return _manager_matches_kind(candidate, self.kind_key)  # type: ignore[arg-type]

    async def _process_rerolls(self, interaction: discord.Interaction, *, amount: int, hire_goal: int) -> tuple[int, int, str, str, str, str]:
        hires, rerolls_used = 0, 0
        best_hit, latest_hit = "None yet", "None yet"
        best_score = -1
        stopped_note = ""
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                for idx in range(amount):
                    if self.target_kind == "worker":
                        roll_result = await roll_worker_candidate(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key, reroll_cost=WORKER_CANDIDATE_REROLL_COST)
                        candidate = roll_result.worker_candidate
                    else:
                        roll_result = await roll_manager_candidate(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key, reroll_cost=MANAGER_CANDIDATE_REROLL_COST)
                        candidate = roll_result.manager_candidate
                    if (not roll_result.ok) or candidate is None:
                        if self._is_slots_full_message(roll_result.message):
                            stopped_note = "Stopped early because all assignment slots are full."
                            break
                        return hires, rerolls_used, best_hit, latest_hit, stopped_note, roll_result.message
                    rerolls_used += 1
                    rarity_key = _normalize_rarity_key(getattr(candidate, "rarity", ""))
                    hit_name = _safe_str(getattr(candidate, "worker_name", getattr(candidate, "manager_name", None)), "Candidate")
                    hit_line = f"{hit_name} {(_worker_rarity_badge(rarity_key) if self.target_kind == 'worker' else _manager_rarity_badge(rarity_key))}"
                    score = _worker_candidate_score(candidate) if self.target_kind == "worker" else _manager_candidate_score(candidate)  # type: ignore[arg-type]
                    if score > best_score:
                        best_hit, best_score = hit_line, score
                    if not self._candidate_matches_filters(candidate):
                        if idx == 0 or (idx + 1) % 5 == 0:
                            progress = discord.Embed(title="VIP Reroll In Progress", description="Filtering and evaluating candidates...", color=discord.Color.gold())
                            progress.add_field(name="Progress", value=f"Rolls: **{_fmt_int(rerolls_used)}/{_fmt_int(amount)}**", inline=False)
                            progress.add_field(name="Best hit", value=best_hit, inline=False)
                            await interaction.edit_original_response(embed=progress, view=None)
                        continue
                    if self.target_kind == "worker":
                        hire_result = await hire_worker_manual(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key, worker_name=str(getattr(candidate, "worker_name", "Worker")), worker_type=str(getattr(candidate, "worker_type", "efficient")), rarity=str(getattr(candidate, "rarity", "common")), flat_profit_bonus=int(getattr(candidate, "flat_profit_bonus", 0) or 0), percent_profit_bonus_bp=int(getattr(candidate, "percent_profit_bonus_bp", 0) or 0), charge_silver=False)
                    else:
                        hire_result = await hire_manager_manual(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key, manager_name=str(getattr(candidate, "manager_name", "Manager")), rarity=str(getattr(candidate, "rarity", "common")), runtime_bonus_hours=int(getattr(candidate, "runtime_bonus_hours", 0) or 0), profit_bonus_bp=int(getattr(candidate, "profit_bonus_bp", 0) or 0), auto_restart_charges=int(getattr(candidate, "auto_restart_charges", 0) or 0), charge_silver=False)
                    if not hire_result.ok:
                        if self._is_slots_full_message(hire_result.message):
                            stopped_note = "Stopped early because all assignment slots are full."
                            break
                        return hires, rerolls_used, best_hit, latest_hit, stopped_note, hire_result.message
                    hires += 1
                    latest_hit = hit_line
                    progress = discord.Embed(title="VIP Reroll In Progress", description="Applying successful hires...", color=discord.Color.gold())
                    progress.add_field(name="Progress", value=f"Hires: **{_fmt_int(hires)}**/{_fmt_int(max(hire_goal, 1))}\nRolls: **{_fmt_int(rerolls_used)}/{_fmt_int(amount)}**", inline=False)
                    progress.add_field(name="Best hit", value=best_hit, inline=False)
                    await interaction.edit_original_response(embed=progress, view=None)
                    if hire_goal > 0 and hires >= hire_goal:
                        stopped_note = f"Stopped after reaching your hire goal ({_fmt_int(hire_goal)})."
                        break
        return hires, rerolls_used, best_hit, latest_hit, stopped_note, ""

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success, emoji="✅", row=4)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if self.processing:
            await interaction.response.send_message("This reroll is already running.", ephemeral=True)
            return
        self.processing = True
        await interaction.response.defer(ephemeral=True)
        try:
            _, _, owned, open_slots = await self._resolve_wallet_and_rerolls()
            amount = self._selected_reroll_amount(owned=owned)
            hire_goal = self._selected_hire_goal(open_slots=open_slots)
            if open_slots <= 0:
                await interaction.edit_original_response(content="All assignment slots are already full for this business.", embed=None, view=self)
                return
            if amount <= 0 or owned <= 0:
                await interaction.edit_original_response(content="You do not currently own any rerolls for this action.", embed=None, view=self)
                return
            if amount > owned:
                await interaction.edit_original_response(content=f"You only own **{_fmt_int(owned)}** rerolls right now.", embed=None, view=self)
                return
            if self.target_kind == "manager" and not _manager_kind_pool_possible(self.kind_key, self._selected_rarity_keys()):
                await interaction.edit_original_response(content="This kind + rarity combination has an empty result pool.", embed=None, view=self)
                return
            hires, rerolls_used, best_hit, latest_hit, stopped_note, err = await self._process_rerolls(interaction, amount=amount, hire_goal=hire_goal)
            self.parent_view.current_candidate = None
            summary = discord.Embed(title="VIP Reroll Complete", description="Your rerolls have finished.", color=SUCCESS_COLOR if not err else ERROR_COLOR)
            summary.add_field(name="Hires", value=f"**{_fmt_int(hires)}**", inline=True)
            summary.add_field(name="Rolls used", value=f"**{_fmt_int(rerolls_used)}**/**{_fmt_int(amount)}**", inline=True)
            summary.add_field(name="Best pull", value=best_hit, inline=True)
            if latest_hit != "None yet":
                summary.add_field(name="Latest matched pull", value=latest_hit, inline=False)
            if stopped_note:
                summary.add_field(name="Auto-stop", value=stopped_note, inline=False)
            if err:
                summary.add_field(name="Stopped early", value=err, inline=False)
            await interaction.edit_original_response(embed=summary, view=None)
            await self.parent_view._show_recruitment_board(interaction, action_message=f"✅ VIP reroll complete: **{_fmt_int(rerolls_used)}** rolls used, **{_fmt_int(hires)}** hires.")
        finally:
            self.processing = False

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=4)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="VIP reroll cancelled.", view=self)


class AutoHireWorkersModal(discord.ui.Modal, title="Auto-Hire Workers"):
    def __init__(self, view: "WorkerAssignmentsView"):
        super().__init__()
        self.parent_view = view
        self.rarity_filter = discord.ui.TextInput(label="Allowed rarities", placeholder="rare, epic, mythic (or all)", default="all", max_length=64)
        self.reroll_count = discord.ui.TextInput(label="Max rerolls budget", placeholder="15", default="15", max_length=4)
        self.add_item(self.rarity_filter)
        self.add_item(self.reroll_count)

    def _parse_allowed_rarities(self) -> set[str]:
        raw = str(self.rarity_filter.value or "all").strip().lower()
        if raw in {"", "all", "any", "*"}:
            return set(AUTO_HIRE_ALLOWED_RARITIES)
        allowed = {part.strip() for part in raw.replace("|", ",").split(",") if part.strip()}
        return {r for r in allowed if r in AUTO_HIRE_ALLOWED_RARITIES}

    async def on_submit(self, interaction: discord.Interaction) -> None:
        rerolls = _clamp_int(_parse_int(str(self.reroll_count.value), 0), 1, AUTO_HIRE_MAX_REROLLS)
        allowed_rarities = self._parse_allowed_rarities()
        if not allowed_rarities:
            await interaction.response.send_message("Please enter valid rarity filters: common, uncommon, rare, epic, mythic.", ephemeral=True)
            return

        total_cost = rerolls * WORKER_CANDIDATE_REROLL_COST
        embed = discord.Embed(
            title="Confirm Worker Auto-Hire",
            description=f"Auto-Hire can spend up to **{_fmt_int(total_cost)} Silver** for **{_fmt_int(rerolls)} rerolls**.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Allowed rarities", value=f"`{', '.join(sorted(allowed_rarities))}`", inline=False)
        embed.add_field(name="Flow", value="Auto-Hire keeps rolling and hires matches until slots are full or budget is spent.", inline=False)
        await interaction.response.send_message(
            embed=embed,
            view=ConfirmWorkerAutoHireView(parent_view=self.parent_view, rerolls=rerolls, allowed_rarities=allowed_rarities),
            ephemeral=True,
        )


class AutoHireManagersModal(discord.ui.Modal, title="Auto-Hire Managers"):
    def __init__(self, view: "ManagerAssignmentsView"):
        super().__init__()
        self.parent_view = view
        self.rarity_filter = discord.ui.TextInput(label="Allowed rarities", placeholder="rare, epic, mythic (or all)", default="all", max_length=64)
        self.reroll_count = discord.ui.TextInput(label="Max rerolls budget", placeholder="15", default="15", max_length=4)
        self.add_item(self.rarity_filter)
        self.add_item(self.reroll_count)

    def _parse_allowed_rarities(self) -> set[str]:
        raw = str(self.rarity_filter.value or "all").strip().lower()
        if raw in {"", "all", "any", "*"}:
            return set(AUTO_HIRE_ALLOWED_RARITIES)
        allowed = {part.strip() for part in raw.replace("|", ",").split(",") if part.strip()}
        return {r for r in allowed if r in AUTO_HIRE_ALLOWED_RARITIES}

    async def on_submit(self, interaction: discord.Interaction) -> None:
        rerolls = _clamp_int(_parse_int(str(self.reroll_count.value), 0), 1, AUTO_HIRE_MAX_REROLLS)
        allowed_rarities = self._parse_allowed_rarities()
        if not allowed_rarities:
            await interaction.response.send_message("Please enter valid rarity filters: common, uncommon, rare, epic, mythic.", ephemeral=True)
            return

        total_cost = rerolls * MANAGER_CANDIDATE_REROLL_COST
        embed = discord.Embed(
            title="Confirm Manager Auto-Hire",
            description=f"Auto-Hire can spend up to **{_fmt_int(total_cost)} Silver** for **{_fmt_int(rerolls)} rerolls**.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Allowed rarities", value=f"`{', '.join(sorted(allowed_rarities))}`", inline=False)
        embed.add_field(name="Flow", value="Auto-Hire keeps rolling and hires matches until slots are full or budget is spent.", inline=False)
        await interaction.response.send_message(
            embed=embed,
            view=ConfirmManagerAutoHireView(parent_view=self.parent_view, rerolls=rerolls, allowed_rarities=allowed_rarities),
            ephemeral=True,
        )


class ConfirmWorkerAutoHireView(discord.ui.View):
    def __init__(self, *, parent_view: "WorkerAssignmentsView", rerolls: int, allowed_rarities: set[str]):
        super().__init__(timeout=120)
        self.parent_view = parent_view
        self.rerolls = int(rerolls)
        self.allowed_rarities = set(allowed_rarities)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This confirmation belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Auto-Hire", style=discord.ButtonStyle.success, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer(ephemeral=True)
        service = self.parent_view.cog.vip_hiring_service
        job, err = await service.start_job(
            guild_id=self.parent_view.guild_id,
            user_id=self.parent_view.owner_id,
            started_by_user_id=int(interaction.user.id),
            business_key=self.parent_view.business_key,
            mode="worker",
            requested_count=self.rerolls,
            allowed_rarities=set(self.allowed_rarities),
        )
        if job is None:
            await interaction.edit_original_response(content=f"❌ {err}", embed=None, view=None)
            return
        await service.attach_progress_message(job_id=int(job.id), channel_id=int(interaction.channel_id))
        await interaction.edit_original_response(content=f"✅ Started worker job `{job.job_id}`. Progress is posted in-channel.", embed=None, view=None)
        await service.run_job(job_id=int(job.id))
        await self.parent_view._show_recruitment_board(interaction, action_message=f"✅ Worker hiring job `{job.job_id}` finished.")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_message("Auto-Hire cancelled.", ephemeral=True)


class ConfirmManagerAutoHireView(discord.ui.View):
    def __init__(self, *, parent_view: "ManagerAssignmentsView", rerolls: int, allowed_rarities: set[str]):
        super().__init__(timeout=120)
        self.parent_view = parent_view
        self.rerolls = int(rerolls)
        self.allowed_rarities = set(allowed_rarities)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This confirmation belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Auto-Hire", style=discord.ButtonStyle.success, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer(ephemeral=True)
        service = self.parent_view.cog.vip_hiring_service
        job, err = await service.start_job(
            guild_id=self.parent_view.guild_id,
            user_id=self.parent_view.owner_id,
            started_by_user_id=int(interaction.user.id),
            business_key=self.parent_view.business_key,
            mode="manager",
            requested_count=self.rerolls,
            allowed_rarities=set(self.allowed_rarities),
        )
        if job is None:
            await interaction.edit_original_response(content=f"❌ {err}", embed=None, view=None)
            return
        await service.attach_progress_message(job_id=int(job.id), channel_id=int(interaction.channel_id))
        await interaction.edit_original_response(content=f"✅ Started manager job `{job.job_id}`. Progress is posted in-channel.", embed=None, view=None)
        await service.run_job(job_id=int(job.id))
        await self.parent_view._show_recruitment_board(interaction, action_message=f"✅ Manager hiring job `{job.job_id}` finished.")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_message("Auto-Hire cancelled.", ephemeral=True)


class WorkerAssignmentsView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        business_key: str,
        panel_message_id: int,
        requester: Optional[discord.abc.User] = None,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key
        self.panel_message_id = int(panel_message_id)
        self.current_candidate: Optional[WorkerCandidateSnapshot] = None
        self.page = 0
        member: Optional[discord.Member] = requester if isinstance(requester, discord.Member) else None
        if member is None:
            guild = self.cog.bot.get_guild(self.guild_id)
            if guild is not None:
                member = guild.get_member(self.owner_id)
        self.is_vip = is_vip_member(member)
        self.auto_hire_button.disabled = not self.is_vip
        self.is_processing = False
        self._sync_pagination_buttons(total_slots=0)

    def _sync_pagination_buttons(self, *, total_slots: int) -> None:
        total_pages = max(1, (max(int(total_slots), 0) + _ASSIGNMENTS_PAGE_SIZE - 1) // _ASSIGNMENTS_PAGE_SIZE)
        if self.page >= total_pages:
            self.page = total_pages - 1
        self.prev_page_button.disabled = self.page <= 0
        self.next_page_button.disabled = self.page >= (total_pages - 1)

    async def _send_auto_hire_reply(self, interaction: discord.Interaction) -> None:
        setup_view = VIPRerollSetupView(parent_view=self, target_kind="worker")
        embed = await setup_view._build_summary_embed()
        await interaction.response.send_message(embed=embed, view=setup_view, ephemeral=True)
        setup_view._message = await interaction.original_response()

    async def _refresh_assignments_embed(self, interaction: discord.Interaction) -> Optional[tuple[BusinessManageSnapshot, Sequence[WorkerAssignmentSlotSnapshot], discord.Embed]]:
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return None
        self._sync_pagination_buttons(total_slots=len(slots))
        return detail, slots, _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.page)

    async def _show_recruitment_board(self, interaction: discord.Interaction, action_message: Optional[str] = None) -> None:
        payload = await self._refresh_assignments_embed(interaction)
        if payload is None:
            return
        detail, slots, assignments_embed = payload
        if self.current_candidate is None:
            assignments_embed.add_field(
                name="Recruit Station",
                value=f"Press **Hire Worker** to start a reveal for **{_fmt_int(WORKER_CANDIDATE_REROLL_COST)} Silver**.",
                inline=False,
            )
            if action_message:
                assignments_embed.add_field(name="Action", value=action_message, inline=False)
            await _safe_edit_panel(interaction, embed=assignments_embed, view=self, message_id=self.panel_message_id)
            return

        candidate_embed = _build_worker_candidate_embed(user=interaction.user, detail=detail, candidate=self.current_candidate, slots=slots)
        if action_message:
            candidate_embed.add_field(name="Action", value=action_message, inline=False)
        await _safe_edit_panel(interaction, embeds=[candidate_embed, assignments_embed], view=self, message_id=self.panel_message_id)

    @discord.ui.button(label="Hire Worker", style=discord.ButtonStyle.success, emoji="➕", row=0)
    async def hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if self.is_processing:
            await interaction.response.send_message("A recruit action is already in progress. Please wait a moment.", ephemeral=True)
            return
        self.is_processing = True
        await _safe_defer(interaction)
        try:
            if self.current_candidate is None:
                payload = await self._refresh_assignments_embed(interaction)
                if payload is None:
                    return
                detail, slots, assignments_embed = payload
                rolling = _build_worker_candidate_embed(
                    user=interaction.user,
                    detail=detail,
                    candidate=WorkerCandidateSnapshot(worker_name="Searching...", worker_type="efficient", rarity="common", flat_profit_bonus=0, percent_profit_bonus_bp=0, reroll_cost=WORKER_CANDIDATE_REROLL_COST),
                    slots=slots,
                    stage_label="Re-rolling Candidate...",
                    status_line="Searching for a better hire...",
                )
                await _safe_edit_panel(interaction, embeds=[rolling, assignments_embed], view=self, message_id=self.panel_message_id)
                await asyncio.sleep(0.4)
                async with self.cog.sessionmaker() as session:
                    async with session.begin():
                        result = await roll_worker_candidate(
                            session,
                            guild_id=self.guild_id,
                            user_id=self.owner_id,
                            business_key=self.business_key,
                            reroll_cost=WORKER_CANDIDATE_REROLL_COST,
                        )
                if not result.ok or result.worker_candidate is None:
                    await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
                    return
                self.current_candidate = result.worker_candidate
                await self._show_recruitment_board(interaction, action_message="✨ New candidate found. Hire now for free.")
                return

            candidate = self.current_candidate
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    result = await hire_worker_manual(
                        session,
                        guild_id=self.guild_id,
                        user_id=self.owner_id,
                        business_key=self.business_key,
                        worker_name=str(getattr(candidate, "worker_name", "Worker")),
                        worker_type=str(getattr(candidate, "worker_type", "efficient")),
                        rarity=str(getattr(candidate, "rarity", "common")),
                        flat_profit_bonus=int(getattr(candidate, "flat_profit_bonus", 0) or 0),
                        percent_profit_bonus_bp=int(getattr(candidate, "percent_profit_bonus_bp", 0) or 0),
                        charge_silver=False,
                    )
                    detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                    slots = await get_worker_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
            if detail is None:
                await interaction.followup.send("That business could not be found.", ephemeral=True)
                return

            assignments_embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.page)
            if result.ok and result.hired_worker is not None:
                self.current_candidate = None
                result_embed = _build_worker_hire_result_embed(user=interaction.user, detail=detail, hired=result.hired_worker)
                await _safe_edit_panel(interaction, embeds=[result_embed, assignments_embed], view=self, message_id=self.panel_message_id)
                return
            await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
        finally:
            self.is_processing = False

    @discord.ui.button(label="Reroll Worker", style=discord.ButtonStyle.primary, emoji="🎲", row=0)
    async def reroll_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if self.is_processing:
            await interaction.response.send_message("A recruit action is already in progress. Please wait a moment.", ephemeral=True)
            return
        self.is_processing = True
        await _safe_defer(interaction)
        try:
            previous_candidate = self.current_candidate
            payload = await self._refresh_assignments_embed(interaction)
            if payload is None:
                return
            detail, slots, assignments_embed = payload
            stages = ("Re-rolling candidate...", "Scanning candidates...", "Rare...", "Revealing final candidate...")
            for idx, line in enumerate(stages):
                rolling = _build_worker_candidate_embed(
                    user=interaction.user,
                    detail=detail,
                    candidate=WorkerCandidateSnapshot(worker_name="Recruit Scan", worker_type="efficient", rarity=("common" if idx < 2 else "rare"), flat_profit_bonus=0, percent_profit_bonus_bp=0, reroll_cost=WORKER_CANDIDATE_REROLL_COST),
                    slots=slots,
                    current_candidate=previous_candidate,
                    stage_label="Recruit Spin",
                    status_line=line,
                )
                await _safe_edit_panel(interaction, embeds=[rolling, assignments_embed], view=self, message_id=self.panel_message_id)
                await asyncio.sleep(0.18)
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    result = await roll_worker_candidate(
                        session,
                        guild_id=self.guild_id,
                        user_id=self.owner_id,
                        business_key=self.business_key,
                        reroll_cost=WORKER_CANDIDATE_REROLL_COST,
                    )
            if not result.ok or result.worker_candidate is None:
                await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
                return
            self.current_candidate = result.worker_candidate
            await self._show_recruitment_board(interaction, action_message="✨ Recruit reveal complete.")
        finally:
            self.is_processing = False

    @discord.ui.button(label="Auto-Hire (VIP)", style=discord.ButtonStyle.secondary, emoji="⭐", row=1)
    async def auto_hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if not self.is_vip:
            await interaction.response.send_message("Auto-Hire is a VIP feature.", ephemeral=True)
            return
        await self._send_auto_hire_reply(interaction)

    @discord.ui.button(label="Fire Worker", style=discord.ButtonStyle.danger, emoji="➖", row=1)
    async def fire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_modal(RemoveStaffModal(self, staff_kind="worker"))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        self.current_candidate = None
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view, message_id=self.panel_message_id)

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, emoji="◀️", row=2, disabled=True)
    async def prev_page_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        self.page = max(self.page - 1, 0)
        await _safe_defer(interaction)
        await self._show_recruitment_board(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="▶️", row=2, disabled=True)
    async def next_page_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        self.page += 1
        await _safe_defer(interaction)
        await self._show_recruitment_board(interaction)


class ManagerAssignmentsView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        business_key: str,
        panel_message_id: int,
        requester: Optional[discord.abc.User] = None,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key
        self.panel_message_id = int(panel_message_id)
        self.current_candidate: Optional[ManagerCandidateSnapshot] = None
        self.page = 0
        member: Optional[discord.Member] = requester if isinstance(requester, discord.Member) else None
        if member is None:
            guild = self.cog.bot.get_guild(self.guild_id)
            if guild is not None:
                member = guild.get_member(self.owner_id)
        self.is_vip = is_vip_member(member)
        self.auto_hire_button.disabled = not self.is_vip
        self.is_processing = False
        self._sync_pagination_buttons(total_slots=0)

    def _sync_pagination_buttons(self, *, total_slots: int) -> None:
        total_pages = max(1, (max(int(total_slots), 0) + _ASSIGNMENTS_PAGE_SIZE - 1) // _ASSIGNMENTS_PAGE_SIZE)
        if self.page >= total_pages:
            self.page = total_pages - 1
        self.prev_page_button.disabled = self.page <= 0
        self.next_page_button.disabled = self.page >= (total_pages - 1)

    async def _send_auto_hire_reply(self, interaction: discord.Interaction) -> None:
        setup_view = VIPRerollSetupView(parent_view=self, target_kind="manager")
        embed = await setup_view._build_summary_embed()
        await interaction.response.send_message(embed=embed, view=setup_view, ephemeral=True)
        setup_view._message = await interaction.original_response()

    async def _refresh_assignments_embed(self, interaction: discord.Interaction) -> Optional[tuple[BusinessManageSnapshot, Sequence[ManagerAssignmentSlotSnapshot], discord.Embed]]:
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return None
        self._sync_pagination_buttons(total_slots=len(slots))
        return detail, slots, _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.page)

    async def _show_recruitment_board(self, interaction: discord.Interaction, action_message: Optional[str] = None) -> None:
        payload = await self._refresh_assignments_embed(interaction)
        if payload is None:
            return
        detail, slots, assignments_embed = payload
        if self.current_candidate is None:
            assignments_embed.add_field(
                name="Recruit Station",
                value=f"Press **Hire Manager** to start a reveal for **{_fmt_int(MANAGER_CANDIDATE_REROLL_COST)} Silver**.",
                inline=False,
            )
            if action_message:
                assignments_embed.add_field(name="Action", value=action_message, inline=False)
            await _safe_edit_panel(interaction, embed=assignments_embed, view=self, message_id=self.panel_message_id)
            return

        candidate_embed = _build_manager_candidate_embed(user=interaction.user, detail=detail, candidate=self.current_candidate, slots=slots)
        if action_message:
            candidate_embed.add_field(name="Action", value=action_message, inline=False)
        await _safe_edit_panel(interaction, embeds=[candidate_embed, assignments_embed], view=self, message_id=self.panel_message_id)

    @discord.ui.button(label="Hire Manager", style=discord.ButtonStyle.success, emoji="➕", row=0)
    async def hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if self.is_processing:
            await interaction.response.send_message("A recruit action is already in progress. Please wait a moment.", ephemeral=True)
            return
        self.is_processing = True
        await _safe_defer(interaction)
        try:
            if self.current_candidate is None:
                payload = await self._refresh_assignments_embed(interaction)
                if payload is None:
                    return
                detail, slots, assignments_embed = payload
                rolling = _build_manager_candidate_embed(
                    user=interaction.user,
                    detail=detail,
                    candidate=ManagerCandidateSnapshot(manager_name="Searching...", rarity="common", runtime_bonus_hours=0, profit_bonus_bp=0, auto_restart_charges=0, reroll_cost=MANAGER_CANDIDATE_REROLL_COST),
                    slots=slots,
                    stage_label="Re-rolling Candidate...",
                    status_line="Searching for a better hire...",
                )
                await _safe_edit_panel(interaction, embeds=[rolling, assignments_embed], view=self, message_id=self.panel_message_id)
                await asyncio.sleep(0.4)
                async with self.cog.sessionmaker() as session:
                    async with session.begin():
                        result = await roll_manager_candidate(
                            session,
                            guild_id=self.guild_id,
                            user_id=self.owner_id,
                            business_key=self.business_key,
                            reroll_cost=MANAGER_CANDIDATE_REROLL_COST,
                        )
                if not result.ok or result.manager_candidate is None:
                    await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
                    return
                self.current_candidate = result.manager_candidate
                await self._show_recruitment_board(interaction, action_message="✨ New candidate found. Hire now for free.")
                return

            candidate = self.current_candidate
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    result = await hire_manager_manual(
                        session,
                        guild_id=self.guild_id,
                        user_id=self.owner_id,
                        business_key=self.business_key,
                        manager_name=str(getattr(candidate, "manager_name", "Manager")),
                        rarity=str(getattr(candidate, "rarity", "common")),
                        runtime_bonus_hours=int(getattr(candidate, "runtime_bonus_hours", 0) or 0),
                        profit_bonus_bp=int(getattr(candidate, "profit_bonus_bp", 0) or 0),
                        auto_restart_charges=int(getattr(candidate, "auto_restart_charges", 0) or 0),
                        charge_silver=False,
                    )
                    detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                    slots = await get_manager_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
            if detail is None:
                await interaction.followup.send("That business could not be found.", ephemeral=True)
                return

            assignments_embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.page)
            if result.ok and result.hired_manager is not None:
                self.current_candidate = None
                result_embed = _build_manager_hire_result_embed(user=interaction.user, detail=detail, hired=result.hired_manager)
                await _safe_edit_panel(interaction, embeds=[result_embed, assignments_embed], view=self, message_id=self.panel_message_id)
                return
            await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
        finally:
            self.is_processing = False

    @discord.ui.button(label="Reroll Manager", style=discord.ButtonStyle.primary, emoji="🎲", row=0)
    async def reroll_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if self.is_processing:
            await interaction.response.send_message("A recruit action is already in progress. Please wait a moment.", ephemeral=True)
            return
        self.is_processing = True
        await _safe_defer(interaction)
        try:
            previous_candidate = self.current_candidate
            payload = await self._refresh_assignments_embed(interaction)
            if payload is None:
                return
            detail, slots, assignments_embed = payload
            stages = (("common", "Re-rolling candidate..."), ("rare", "Scanning candidates..."), ("epic", "Epic..."), ("mythical", "Revealing final candidate..."))
            for rarity, line in stages:
                rolling = _build_manager_candidate_embed(
                    user=interaction.user,
                    detail=detail,
                    candidate=ManagerCandidateSnapshot(manager_name="Recruit Scan", rarity=rarity, runtime_bonus_hours=0, profit_bonus_bp=0, auto_restart_charges=0, reroll_cost=MANAGER_CANDIDATE_REROLL_COST),
                    slots=slots,
                    current_candidate=previous_candidate,
                    stage_label="Recruit Spin",
                    status_line=line,
                )
                await _safe_edit_panel(interaction, embeds=[rolling, assignments_embed], view=self, message_id=self.panel_message_id)
                await asyncio.sleep(0.18)
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    result = await roll_manager_candidate(
                        session,
                        guild_id=self.guild_id,
                        user_id=self.owner_id,
                        business_key=self.business_key,
                        reroll_cost=MANAGER_CANDIDATE_REROLL_COST,
                    )
            if not result.ok or result.manager_candidate is None:
                await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
                return
            self.current_candidate = result.manager_candidate
            await self._show_recruitment_board(interaction, action_message="✨ Recruit reveal complete.")
        finally:
            self.is_processing = False

    @discord.ui.button(label="Auto-Hire (VIP)", style=discord.ButtonStyle.secondary, emoji="⭐", row=1)
    async def auto_hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if not self.is_vip:
            await interaction.response.send_message("Auto-Hire is a VIP feature.", ephemeral=True)
            return
        await self._send_auto_hire_reply(interaction)

    @discord.ui.button(label="Fire Manager", style=discord.ButtonStyle.danger, emoji="➖", row=1)
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_modal(RemoveStaffModal(self, staff_kind="manager"))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        self.current_candidate = None
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view, message_id=self.panel_message_id)

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, emoji="◀️", row=2, disabled=True)
    async def prev_page_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        self.page = max(self.page - 1, 0)
        await _safe_defer(interaction)
        await self._show_recruitment_board(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="▶️", row=2, disabled=True)
    async def next_page_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        self.page += 1
        await _safe_defer(interaction)
        await self._show_recruitment_board(interaction)


# =========================================================
# COG
# =========================================================

class BusinessCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.vip_hiring_service = VipHiringService(sessionmaker=self.sessionmaker, bot=bot)
        self.runtime_engine = BusinessRuntimeEngine(on_run_completed=self._notify_business_run_completed)
        self._active_auto_hire_tasks: dict[str, asyncio.Task[None]] = {}

    async def _ensure_business_ownership_worker_columns(self) -> None:
        target_columns: dict[str, str] = {
            "worker_slot_legacy_floor": "ALTER TABLE business_ownership ADD COLUMN worker_slot_legacy_floor INT NOT NULL DEFAULT 0",
            "worker_system_generation": "ALTER TABLE business_ownership ADD COLUMN worker_system_generation INT NOT NULL DEFAULT 1",
            "worker_migration_version": "ALTER TABLE business_ownership ADD COLUMN worker_migration_version INT NOT NULL DEFAULT 0",
            "worker_migration_summary_json": "ALTER TABLE business_ownership ADD COLUMN worker_migration_summary_json JSON NULL",
            "worker_migration_summary_seen": "ALTER TABLE business_ownership ADD COLUMN worker_migration_summary_seen TINYINT(1) NOT NULL DEFAULT 0",
        }

        async with self.sessionmaker() as session:
            async with session.begin():
                present_rows = await session.execute(
                    text(
                        """
                        SELECT COLUMN_NAME
                        FROM information_schema.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'business_ownership'
                        """
                    )
                )
                present = {str(row[0]).strip().lower() for row in present_rows if row and row[0]}
                for column_name, ddl in target_columns.items():
                    if column_name in present:
                        continue
                    await session.execute(text(ddl))
                    log.warning("Patched missing column on business_ownership: %s", column_name)

    def _auto_hire_task_key(self, *, guild_id: int, user_id: int, business_key: str, staff_kind: str) -> str:
        return f"{int(guild_id)}:{int(user_id)}:{str(business_key)}:{str(staff_kind)}"

    async def upsert_auto_hire_session(
        self,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
        staff_kind: str,
        rerolls: int,
        allowed_rarities: set[str],
    ) -> None:
        normalized_kind = str(staff_kind).strip().lower()
        payload = {"allowed_rarities": sorted({str(r).strip().lower() for r in allowed_rarities if str(r).strip()})}
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(
                    select(BusinessAutoHireSessionRow).where(
                        BusinessAutoHireSessionRow.guild_id == int(guild_id),
                        BusinessAutoHireSessionRow.user_id == int(user_id),
                        BusinessAutoHireSessionRow.business_key == str(business_key),
                        BusinessAutoHireSessionRow.staff_kind == normalized_kind,
                    )
                )
                if row is None:
                    row = BusinessAutoHireSessionRow(
                        guild_id=int(guild_id),
                        user_id=int(user_id),
                        business_key=str(business_key),
                        staff_kind=normalized_kind,
                    )
                    session.add(row)
                row.remaining_rerolls = max(int(rerolls), 0)
                row.allowed_rarities_json = payload
                row.active = bool(int(rerolls) > 0)
                row.last_error = None

    async def _resume_active_auto_hire_sessions(self) -> None:
        async with self.sessionmaker() as session:
            rows = list(
                (
                    await session.scalars(
                        select(BusinessAutoHireSessionRow).where(BusinessAutoHireSessionRow.active.is_(True))
                    )
                ).all()
            )
        for row in rows:
            self._launch_auto_hire_task(
                guild_id=int(row.guild_id),
                user_id=int(row.user_id),
                business_key=str(row.business_key),
                staff_kind=str(row.staff_kind),
            )

    def _launch_auto_hire_task(self, *, guild_id: int, user_id: int, business_key: str, staff_kind: str) -> None:
        key = self._auto_hire_task_key(guild_id=guild_id, user_id=user_id, business_key=business_key, staff_kind=staff_kind)
        existing = self._active_auto_hire_tasks.get(key)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(
            self._run_auto_hire_session(
                guild_id=guild_id,
                user_id=user_id,
                business_key=business_key,
                staff_kind=staff_kind,
            ),
            name=f"business_auto_hire:{key}",
        )
        self._active_auto_hire_tasks[key] = task

    async def _run_auto_hire_session(self, *, guild_id: int, user_id: int, business_key: str, staff_kind: str) -> None:
        key = self._auto_hire_task_key(guild_id=guild_id, user_id=user_id, business_key=business_key, staff_kind=staff_kind)
        try:
            while True:
                async with self.sessionmaker() as session:
                    async with session.begin():
                        state = await session.scalar(
                            select(BusinessAutoHireSessionRow).where(
                                BusinessAutoHireSessionRow.guild_id == int(guild_id),
                                BusinessAutoHireSessionRow.user_id == int(user_id),
                                BusinessAutoHireSessionRow.business_key == str(business_key),
                                BusinessAutoHireSessionRow.staff_kind == str(staff_kind),
                            )
                        )
                        if state is None or not bool(state.active):
                            return
                        if int(state.remaining_rerolls or 0) <= 0:
                            state.active = False
                            return

                        allowed = {
                            str(r).strip().lower()
                            for r in (dict(state.allowed_rarities_json or {}).get("allowed_rarities") or [])
                            if str(r).strip()
                        } or set(AUTO_HIRE_ALLOWED_RARITIES)

                        if str(staff_kind) == "worker":
                            slots = await get_worker_assignment_slots(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
                            if not any(not bool(getattr(slot, "is_active", False)) for slot in slots):
                                state.active = False
                                state.last_error = "All worker slots are full."
                                return
                            roll_result = await roll_worker_candidate(session, guild_id=guild_id, user_id=user_id, business_key=business_key, reroll_cost=WORKER_CANDIDATE_REROLL_COST)
                            if not roll_result.ok or roll_result.worker_candidate is None:
                                state.active = False
                                state.last_error = str(roll_result.message or "Worker auto-hire stopped.")
                                return
                            state.remaining_rerolls = max(int(state.remaining_rerolls or 0) - 1, 0)
                            candidate = roll_result.worker_candidate
                            rarity = str(getattr(candidate, "rarity", "common")).strip().lower()
                            if rarity in allowed:
                                hire_result = await hire_worker_manual(
                                    session,
                                    guild_id=guild_id,
                                    user_id=user_id,
                                    business_key=business_key,
                                    worker_name=str(getattr(candidate, "worker_name", "Worker")),
                                    worker_type=str(getattr(candidate, "worker_type", "efficient")),
                                    rarity=str(getattr(candidate, "rarity", "common")),
                                    flat_profit_bonus=int(getattr(candidate, "flat_profit_bonus", 0) or 0),
                                    percent_profit_bonus_bp=int(getattr(candidate, "percent_profit_bonus_bp", 0) or 0),
                                    charge_silver=False,
                                )
                                if not hire_result.ok:
                                    state.active = False
                                    state.last_error = str(hire_result.message or "Worker hire failed.")
                                    return
                        else:
                            slots = await get_manager_assignment_slots(session, guild_id=guild_id, user_id=user_id, business_key=business_key)
                            if not any(not bool(getattr(slot, "is_active", False)) for slot in slots):
                                state.active = False
                                state.last_error = "All manager slots are full."
                                return
                            roll_result = await roll_manager_candidate(session, guild_id=guild_id, user_id=user_id, business_key=business_key, reroll_cost=MANAGER_CANDIDATE_REROLL_COST)
                            if not roll_result.ok or roll_result.manager_candidate is None:
                                state.active = False
                                state.last_error = str(roll_result.message or "Manager auto-hire stopped.")
                                return
                            state.remaining_rerolls = max(int(state.remaining_rerolls or 0) - 1, 0)
                            candidate = roll_result.manager_candidate
                            rarity = str(getattr(candidate, "rarity", "common")).strip().lower()
                            if rarity in allowed:
                                hire_result = await hire_manager_manual(
                                    session,
                                    guild_id=guild_id,
                                    user_id=user_id,
                                    business_key=business_key,
                                    manager_name=str(getattr(candidate, "manager_name", "Manager")),
                                    rarity=str(getattr(candidate, "rarity", "common")),
                                    runtime_bonus_hours=int(getattr(candidate, "runtime_bonus_hours", 0) or 0),
                                    profit_bonus_bp=int(getattr(candidate, "profit_bonus_bp", 0) or 0),
                                    auto_restart_charges=int(getattr(candidate, "auto_restart_charges", 0) or 0),
                                    charge_silver=False,
                                )
                                if not hire_result.ok:
                                    state.active = False
                                    state.last_error = str(hire_result.message or "Manager hire failed.")
                                    return

                        if int(state.remaining_rerolls or 0) <= 0:
                            state.active = False
                await asyncio.sleep(AUTO_HIRE_ROLL_DELAY_SECONDS)
        finally:
            self._active_auto_hire_tasks.pop(key, None)

    def _load_runtime_state(self) -> dict:
        default = {
            "refund_migration_ran": False,
            "notification_prefs": {},
            "pending_summaries": {},
        }
        try:
            if not _BUSINESS_RUNTIME_STATE_PATH.exists():
                return default
            raw = json.loads(_BUSINESS_RUNTIME_STATE_PATH.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return default
            default.update(raw)
            return default
        except Exception:
            return default

    def _save_runtime_state(self, state: dict) -> None:
        _BUSINESS_RUNTIME_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _BUSINESS_RUNTIME_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

    def _state_key(self, *, guild_id: int, user_id: int) -> str:
        return f"{int(guild_id)}:{int(user_id)}"

    def _notifications_enabled_for(self, *, guild_id: int, user_id: int) -> bool:
        state = self._load_runtime_state()
        prefs = dict(state.get("notification_prefs", {}))
        return bool(prefs.get(self._state_key(guild_id=guild_id, user_id=user_id), True))

    def _set_notifications_enabled_for(self, *, guild_id: int, user_id: int, enabled: bool) -> None:
        state = self._load_runtime_state()
        prefs = dict(state.get("notification_prefs", {}))
        prefs[self._state_key(guild_id=guild_id, user_id=user_id)] = bool(enabled)
        state["notification_prefs"] = prefs
        self._save_runtime_state(state)

    def _push_pending_summary(self, *, guild_id: int, user_id: int, summary: dict) -> None:
        state = self._load_runtime_state()
        pending = dict(state.get("pending_summaries", {}))
        key = self._state_key(guild_id=guild_id, user_id=user_id)
        items = list(pending.get(key, []))
        items.append(summary)
        pending[key] = items[-20:]
        state["pending_summaries"] = pending
        self._save_runtime_state(state)

    def _pop_pending_summaries(self, *, guild_id: int, user_id: int) -> list[dict]:
        state = self._load_runtime_state()
        pending = dict(state.get("pending_summaries", {}))
        key = self._state_key(guild_id=guild_id, user_id=user_id)
        items = list(pending.pop(key, []))
        state["pending_summaries"] = pending
        self._save_runtime_state(state)
        return items

    def _new_upgrade_cost(self, *, base_hourly_income: int, level: int) -> int:
        lvl = max(int(level), 0)
        first = min(lvl, 10)
        bp = first * 3500
        if lvl > 10:
            bp += min(lvl - 10, 10) * 1500
        if lvl > 20:
            bp += (lvl - 20) * 800
        cur = int(round(int(base_hourly_income) * (10_000 + bp) / 10_000))

        nxt_lvl = lvl + 1
        first_n = min(nxt_lvl, 10)
        bp_n = first_n * 3500
        if nxt_lvl > 10:
            bp_n += min(nxt_lvl - 10, 10) * 1500
        if nxt_lvl > 20:
            bp_n += (nxt_lvl - 20) * 800
        nxt = int(round(int(base_hourly_income) * (10_000 + bp_n) / 10_000))
        delta = max(nxt - cur, 1)
        return max(int(round(delta * 12)), 1)

    async def _run_one_time_upgrade_refund(self) -> None:
        state = self._load_runtime_state()
        refund_migration_action = "business_upgrade_refund_migration_v1"

        if bool(state.get("refund_migration_ran", False)):
            return

        async with self.sessionmaker() as session:
            existing_marker = await session.scalar(
                select(AdminAuditLogRow).where(
                    AdminAuditLogRow.action == refund_migration_action,
                    AdminAuditLogRow.table_name == "business_ownership",
                )
            )
            if existing_marker is not None:
                state["refund_migration_ran"] = True
                self._save_runtime_state(state)
                log.info("Business refund migration already marked in database; skipping rerun.")
                return

        refunded_total = 0
        refunded_rows = 0

        async with self.sessionmaker() as session:
            async with session.begin():
                defs = await fetch_business_defs(session)
                def_map = {str(d.key): d for d in defs}
                ownership_rows = list((await session.scalars(select(BusinessOwnershipRow))).all())

                for row in ownership_rows:
                    defn = def_map.get(str(row.business_key))
                    if defn is None:
                        continue
                    level = max(int(row.level or 0), 0)
                    if level <= 0:
                        continue
                    legacy_spent = 0
                    new_spent = 0
                    for i in range(level):
                        legacy_spent += int(defn.base_upgrade_cost) * (2 ** i)
                        new_spent += self._new_upgrade_cost(base_hourly_income=int(defn.base_hourly_income), level=i)
                    refund = max(int(legacy_spent) - int(new_spent), 0)
                    if refund <= 0:
                        continue

                    wallet = await session.scalar(
                        select(WalletRow).where(
                            WalletRow.guild_id == int(row.guild_id),
                            WalletRow.user_id == int(row.user_id),
                        )
                    )
                    if wallet is None:
                        wallet = WalletRow(
                            guild_id=int(row.guild_id),
                            user_id=int(row.user_id),
                            silver=0,
                            diamonds=0,
                        )
                        session.add(wallet)
                        await session.flush()

                    wallet.silver = int(wallet.silver or 0) + refund
                    wallet.silver_earned = int(wallet.silver_earned or 0) + refund
                    row.total_spent = max(int(row.total_spent or 0) - refund, 0)
                    refunded_total += refund
                    refunded_rows += 1

                session.add(
                    AdminAuditLogRow(
                        guild_id=0,
                        actor_user_id=0,
                        target_user_id=None,
                        action=refund_migration_action,
                        table_name="business_ownership",
                        pk_json=json.dumps({"migration": refund_migration_action}),
                        before_json=None,
                        after_json=json.dumps(
                            {
                                "refunded_rows": refunded_rows,
                                "refunded_total": refunded_total,
                            }
                        ),
                        reason="One-time business upgrade refund migration completed.",
                    )
                )

        state["refund_migration_ran"] = True
        self._save_runtime_state(state)
        log.info("Business refund migration done | rows=%s refunded_total=%s", refunded_rows, refunded_total)

    async def _notify_business_run_completed(self, notice: CompletedRunNotice) -> None:
        business_name = notice.business_key.replace("_", " ").title()
        async with self.sessionmaker() as session:
            async with session.begin():
                defs = await fetch_business_defs(session)
        def_map = {str(d.key): d for d in defs}
        matched = def_map.get(str(notice.business_key))
        if matched is not None:
            business_name = f"{matched.emoji} {matched.name}"

        summary_data = dict(notice.summary or {})
        hourly_breakdown = list(summary_data.get("hourly_breakdown", []))
        best_event = None
        worst_event = None
        event_lines: list[str] = []
        for evt in notice.event_outcomes:
            delta = int(evt.silver_delta)
            line = f"• {evt.title} ({evt.rarity.title()}) {delta:+,}"
            event_lines.append(line)
            if best_event is None or delta > int(best_event["delta"]):
                best_event = {"name": str(evt.title), "delta": int(delta)}
            if worst_event is None or delta < int(worst_event["delta"]):
                worst_event = {"name": str(evt.title), "delta": int(delta)}
        base_total = int(summary_data.get("base_contribution", 0))
        worker_total = int(summary_data.get("worker_contribution", 0))
        manager_total = int(summary_data.get("manager_contribution", 0))
        net_events_total = int(summary_data.get("event_income_positive", 0)) - int(summary_data.get("event_income_negative", 0))
        factor_pairs = [("base", base_total), ("workers", worker_total), ("managers", manager_total), ("events", abs(net_events_total))]
        most_impactful_factor = max(factor_pairs, key=lambda item: item[1])[0] if factor_pairs else "base"
        summary = {
            "business_name": business_name,
            "hours_paid_total": int(notice.hours_paid_total),
            "silver_paid_total": int(notice.silver_paid_total),
            "event_count": int(summary_data.get("events_triggered_total", len(notice.event_outcomes))),
            "net_event_delta": int(net_events_total),
            "event_income_positive": int(summary_data.get("event_income_positive", 0)),
            "event_income_negative": int(summary_data.get("event_income_negative", 0)),
            "worker_contribution": worker_total,
            "manager_contribution": manager_total,
            "base_contribution": base_total,
            "positive_events": int(summary_data.get("positive_events", 0)),
            "negative_events": int(summary_data.get("negative_events", 0)),
            "highest_single_hour_payout": int(summary_data.get("highest_single_hour_payout", 0)),
            "lowest_single_hour_payout": int(summary_data.get("lowest_single_hour_payout", 0)),
            "highest_rarity": str(summary_data.get("highest_rarity", "none")),
            "best_event": best_event or {"name": "None", "delta": 0},
            "worst_event": worst_event or {"name": "None", "delta": 0},
            "hourly_breakdown": hourly_breakdown,
            "event_lines": event_lines,
            "most_impactful_factor": most_impactful_factor,
            "premium": dict(summary_data.get("premium", {})),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        embed = _build_run_summary_embed(summary=summary)
        content = f"<@{notice.user_id}> your business run finished: **{business_name}**"
        view = RunSummaryView(owner_id=int(notice.user_id), summary=summary)

        notify_enabled = self._notifications_enabled_for(guild_id=int(notice.guild_id), user_id=int(notice.user_id))
        if not notify_enabled:
            self._push_pending_summary(guild_id=int(notice.guild_id), user_id=int(notice.user_id), summary=summary)
            return

        guild = self.bot.get_guild(int(notice.guild_id))
        if guild is None:
            return

        channels: list[discord.abc.Messageable] = []

        preferred = guild.get_channel(_BUSINESS_REVENUE_ANNOUNCEMENT_CHANNEL_ID)
        if isinstance(preferred, discord.abc.Messageable):
            channels.append(preferred)

        if guild.system_channel is not None and guild.system_channel not in channels:
            channels.append(guild.system_channel)
        for channel in guild.text_channels:
            if channel in channels:
                continue
            me = guild.me
            if me is None:
                continue
            perms = channel.permissions_for(me)
            if perms.send_messages:
                channels.append(channel)
            if len(channels) >= 4:
                break

        for channel in channels:
            try:
                await channel.send(content=content, embed=embed, view=view)
                return
            except Exception:
                continue

        user = self.bot.get_user(int(notice.user_id))
        if user is None:
            try:
                user = await self.bot.fetch_user(int(notice.user_id))
            except Exception:
                user = None
        if user is not None:
            try:
                await user.send(embed=embed, view=view)
                return
            except Exception:
                self._push_pending_summary(guild_id=int(notice.guild_id), user_id=int(notice.user_id), summary=summary)
                return
        self._push_pending_summary(guild_id=int(notice.guild_id), user_id=int(notice.user_id), summary=summary)

    async def _ensure_business_prestige_merge(self) -> None:
        state = self._load_runtime_state()
        merge_state = dict(state.get("business_prestige_system", {}))
        if bool(merge_state.get("merged", False)):
            return
        merge_state["merged"] = True
        state["business_prestige_system"] = merge_state
        self._save_runtime_state(state)
        log.info("Business prestige system merge flag written.")

    async def cog_load(self) -> None:
        await self._ensure_business_ownership_worker_columns()
        await self._ensure_business_prestige_merge()
        await self._run_one_time_upgrade_refund()
        async with self.sessionmaker() as session:
            async with session.begin():
                migration_result = await migrate_worker_system_for_all_users(session)
        log.info(
            "worker_system_migration_v%s done | migrated=%s skipped=%s failed=%s",
            WORKER_MIGRATION_VERSION,
            migration_result.get("migrated"),
            migration_result.get("skipped"),
            migration_result.get("failed"),
        )
        await reconcile_incomplete_jobs(service=self.vip_hiring_service)
        await self._resume_active_auto_hire_sessions()
        log.info(
            "Business runtime start requested | cog=%s running=%s",
            self.__class__.__name__,
            self.runtime_engine.running,
        )
        await self.runtime_engine.start_loop()
        log.info(
            "Business runtime started | cog=%s running=%s",
            self.__class__.__name__,
            self.runtime_engine.running,
        )

    async def cog_unload(self) -> None:
        for task in list(self._active_auto_hire_tasks.values()):
            task.cancel()
        self._active_auto_hire_tasks.clear()
        log.info(
            "Business runtime stop requested | cog=%s running=%s",
            self.__class__.__name__,
            self.runtime_engine.running,
        )
        await self.runtime_engine.stop_loop()
        log.info(
            "Business runtime stopped | cog=%s running=%s",
            self.__class__.__name__,
            self.runtime_engine.running,
        )

    async def _build_hub_for_user(
        self,
        *,
        guild_id: int,
        user_id: int,
    ) -> BusinessHubSnapshot:
        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )
        return snap

    async def _consume_worker_migration_summary(self, *, guild_id: int, user_id: int) -> Optional[dict]:
        if WORKER_MIGRATION_VERSION <= 0:
            return None
        async with self.sessionmaker() as session:
            async with session.begin():
                rows = list(
                    (
                        await session.scalars(
                            select(BusinessOwnershipRow).where(
                                BusinessOwnershipRow.guild_id == int(guild_id),
                                BusinessOwnershipRow.user_id == int(user_id),
                                BusinessOwnershipRow.worker_migration_version >= int(WORKER_MIGRATION_VERSION),
                                BusinessOwnershipRow.worker_migration_summary_seen.is_(False),
                            )
                        )
                    ).all()
                )
                if not rows:
                    return None
                old_workers = 0
                new_workers = 0
                slot_before = 0
                slot_after = 0
                power_delta_values: list[float] = []
                merge_lines: list[str] = []
                for row in rows:
                    data = dict(getattr(row, "worker_migration_summary_json", {}) or {})
                    old_workers += int(data.get("old_worker_count", 0) or 0)
                    new_workers += int(data.get("new_worker_count", 0) or 0)
                    slot_before += int(data.get("old_worker_count", 0) or 0)
                    slot_after += max(int(getattr(row, "worker_slot_legacy_floor", 0) or 0), int(getattr(row, "prestige", 0) or 0) + 3)
                    try:
                        power_delta_values.append(float(data.get("estimated_power_delta_pct", 0.0) or 0.0))
                    except Exception:
                        power_delta_values.append(0.0)
                    merge_lines.extend([str(line) for line in list(data.get("merge_summary_lines", []) or [])])
                    row.worker_migration_summary_seen = True
                estimated_power_delta_pct = (sum(power_delta_values) / len(power_delta_values)) if power_delta_values else 0.0
                return {
                    "old_workers": old_workers,
                    "new_workers": new_workers,
                    "slot_before": slot_before,
                    "slot_after": slot_after,
                    "estimated_power_delta_pct": estimated_power_delta_pct,
                    "merge_lines": merge_lines,
                }

    @app_commands.command(name="business", description="Open your business management hub.")
    async def business_cmd(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        user_id = int(interaction.user.id)

        await _safe_defer(interaction, thinking=True)

        try:
            snap = await self._build_hub_for_user(
                guild_id=guild_id,
                user_id=user_id,
            )
        except Exception as e:
            embed = _build_result_embed(
                title="Business Hub",
                message=f"Failed to load the business hub.\n```py\n{type(e).__name__}: {e}\n```",
                ok=False,
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        embed = _build_hub_embed(user=interaction.user, snap=snap)
        pending = self._pop_pending_summaries(guild_id=guild_id, user_id=user_id)
        if pending:
            lines = []
            for item in pending[-5:]:
                business_name = str(item.get("business_name", "Business"))
                silver = int(item.get("silver_paid_total", 0))
                hours = int(item.get("hours_paid_total", 0))
                net = int(item.get("net_event_delta", 0))
                sign = "+" if net >= 0 else ""
                peak = int(item.get("highest_single_hour_payout", 0))
                lines.append(f"• **{business_name}** — {silver:,} Silver over {hours}h | events {sign}{net:,} | peak {peak:,}/h")
            embed.add_field(
                name="📋 Offline Business Summary",
                value="\n".join(lines),
                inline=False,
            )
        view = BusinessHubView(
            cog=self,
            owner_id=user_id,
            guild_id=guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.send(embed=embed, view=view)
        migration_summary = await self._consume_worker_migration_summary(guild_id=guild_id, user_id=user_id)
        if migration_summary:
            await interaction.followup.send(embed=_build_worker_migration_embed(summary=migration_summary), ephemeral=True)
        if pending:
            latest = dict(pending[-1])
            await interaction.followup.send(
                content="You had completed runs while away. Latest full summary:",
                embed=_build_run_summary_embed(summary=latest),
                view=RunSummaryView(owner_id=user_id, summary=latest),
                ephemeral=True,
            )

    @app_commands.command(name="business_notifications", description="Toggle business completion pings for your account.")
    async def business_notifications_cmd(self, interaction: discord.Interaction, enabled: bool) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        guild_id = int(interaction.guild.id)
        user_id = int(interaction.user.id)
        self._set_notifications_enabled_for(guild_id=guild_id, user_id=user_id, enabled=bool(enabled))
        msg = "Business completion notifications are now **ON**." if enabled else "Business completion notifications are now **OFF**. You'll see summaries next time you run `/business`."
        await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.command(name="admin_restore_worker_archive", description="Admin: restore archived workers for a migrated business.")
    async def admin_restore_worker_archive_cmd(self, interaction: discord.Interaction, user: discord.Member, business_key: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not await self._business_admin_authorized(interaction):
            await interaction.response.send_message(_ACCESS_DENIED, ephemeral=True)
            return
        await _safe_defer(interaction, thinking=True, ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                ok, message = await restore_archived_workers_for_business(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=int(user.id),
                    business_key=str(business_key).strip().lower(),
                    migration_version=WORKER_MIGRATION_VERSION,
                )
        color = SUCCESS_COLOR if ok else ERROR_COLOR
        await interaction.followup.send(embed=discord.Embed(title="Worker Archive Restore", description=message, color=color), ephemeral=True)

    @app_commands.command(name="admin_worker_migration_dryrun", description="Admin: preview worker migration for one business ownership.")
    async def admin_worker_migration_dryrun_cmd(self, interaction: discord.Interaction, user: discord.Member, business_key: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not await self._business_admin_authorized(interaction):
            await interaction.response.send_message(_ACCESS_DENIED, ephemeral=True)
            return
        await _safe_defer(interaction, thinking=True, ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                ownership = await session.scalar(
                    select(BusinessOwnershipRow).where(
                        BusinessOwnershipRow.guild_id == int(interaction.guild.id),
                        BusinessOwnershipRow.user_id == int(user.id),
                        BusinessOwnershipRow.business_key == str(business_key).strip().lower(),
                    )
                )
                if ownership is None:
                    await interaction.followup.send(
                        embed=discord.Embed(
                            title="Worker Migration Dry-Run",
                            description="Ownership not found for that business.",
                            color=ERROR_COLOR,
                        ),
                        ephemeral=True,
                    )
                    return
                report = await preview_worker_migration_for_ownership(session, ownership_id=int(ownership.id))
        await interaction.followup.send(embed=_build_worker_migration_dry_run_embed(report=report), ephemeral=True)

    @app_commands.command(name="admin_worker_migration_report", description="Admin: show recent worker migration state rows.")
    async def admin_worker_migration_report_cmd(self, interaction: discord.Interaction, target_user_id: Optional[str] = None, limit: app_commands.Range[int, 1, 25] = 10) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not await self._business_admin_authorized(interaction):
            await interaction.response.send_message(_ACCESS_DENIED, ephemeral=True)
            return
        await _safe_defer(interaction, thinking=True, ephemeral=True)
        parsed_user_id = 0
        try:
            parsed_user_id = int(str(target_user_id or "0").strip())
        except Exception:
            parsed_user_id = 0
        async with self.sessionmaker() as session:
            async with session.begin():
                stmt = select(BusinessWorkerMigrationStateRow).where(
                    BusinessWorkerMigrationStateRow.migration_version == int(WORKER_MIGRATION_VERSION)
                ).order_by(BusinessWorkerMigrationStateRow.updated_at.desc())
                if parsed_user_id > 0:
                    stmt = stmt.where(BusinessWorkerMigrationStateRow.user_id == int(parsed_user_id))
                rows = list((await session.scalars(stmt.limit(int(limit)))).all())
        if not rows:
            await interaction.followup.send(
                embed=discord.Embed(title="Worker Migration Report", description="No migration rows found for the selected filter.", color=INFO_COLOR),
                ephemeral=True,
            )
            return
        lines: list[str] = []
        for row in rows:
            lines.append(
                f"• ownership #{int(row.ownership_id)} • <@{int(row.user_id)}> • `{str(row.business_key)}`\n"
                f"  status `{str(row.status)}` • old `{int(row.old_worker_count)}` → new `{int(row.new_worker_count)}`"
            )
        embed = discord.Embed(
            title=f"Worker Migration Report (v{WORKER_MIGRATION_VERSION})",
            description="\n".join(lines[:10]),
            color=INFO_COLOR,
        )
        embed.set_footer(text=f"Showing {min(len(rows), 10)} of {len(rows)} rows")
        await interaction.followup.send(embed=embed, ephemeral=True)



    async def _business_admin_authorized(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            return False
        member = interaction.user if isinstance(interaction.user, discord.Member) else interaction.guild.get_member(interaction.user.id)
        if member is not None and member.guild_permissions.administrator:
            return True
        if member is not None and _BUSINESS_ADMIN_ROLE_IDS and any(int(role.id) in _BUSINESS_ADMIN_ROLE_IDS for role in member.roles):
            return True
        owner_ids = getattr(self.bot, "owner_ids", set()) or set()
        return int(interaction.user.id) in owner_ids or int(interaction.user.id) == int(interaction.guild.owner_id)

    async def _log_business_admin_action(self, session, *, guild_id: int, actor_user_id: int, target_user_id: int, action: str, table_name: str, pk_json: dict, before: Optional[dict], after: Optional[dict], reason: str) -> None:
        row = AdminAuditLogRow(
            guild_id=int(guild_id),
            actor_user_id=int(actor_user_id),
            target_user_id=int(target_user_id),
            action=str(action)[:32],
            table_name=str(table_name)[:64],
            pk_json=json.dumps(pk_json, default=str),
            before_json=json.dumps(before, default=str) if before is not None else None,
            after_json=json.dumps(after, default=str) if after is not None else None,
            reason=str(reason)[:200],
        )
        session.add(row)
        log.info("business_admin action=%s guild_id=%s actor=%s target=%s reason=%s", action, guild_id, actor_user_id, target_user_id, reason)

    async def _fetch_target_businesses(self, session, *, guild_id: int, user_id: int) -> list[BusinessOwnershipRow]:
        rows = await session.scalars(select(BusinessOwnershipRow).where(BusinessOwnershipRow.guild_id == int(guild_id), BusinessOwnershipRow.user_id == int(user_id)).order_by(BusinessOwnershipRow.created_at.asc(), BusinessOwnershipRow.business_key.asc()))
        return list(rows.all())

    async def _build_business_admin_payload(self, *, guild_id: int, session: BusinessAdminSession) -> dict:
        async with self.sessionmaker() as db_session:
            ownerships = await self._fetch_target_businesses(db_session, guild_id=guild_id, user_id=session.target_user_id)
            if session.target_business_key is None and ownerships:
                session.target_business_key = ownerships[0].business_key
            ownership = next((row for row in ownerships if row.business_key == session.target_business_key), None)
            embed = discord.Embed(title="Business Admin Dashboard", color=discord.Color.orange(), timestamp=datetime.now(timezone.utc))
            embed.add_field(name="Target User", value=f"<@{session.target_user_id}>\n`{session.target_user_id}`", inline=False)
            if ownership is None:
                embed.description = "This user does not currently own a business. Use **Edit Core Stats** after initializing a business to repair data if needed."
            else:
                detail = await get_business_manage_snapshot(db_session, guild_id=guild_id, user_id=session.target_user_id, business_key=ownership.business_key)
                worker_slots = await get_worker_assignment_slots(db_session, guild_id=guild_id, user_id=session.target_user_id, business_key=ownership.business_key)
                manager_slots = await get_manager_assignment_slots(db_session, guild_id=guild_id, user_id=session.target_user_id, business_key=ownership.business_key)
                run = await db_session.scalar(select(BusinessRunRow).where(BusinessRunRow.guild_id == int(guild_id), BusinessRunRow.user_id == int(session.target_user_id), BusinessRunRow.business_key == ownership.business_key, BusinessRunRow.status == "running").order_by(BusinessRunRow.created_at.desc()))
                rarity_counts = {}
                for slot in [*worker_slots, *manager_slots]:
                    if slot.is_active and slot.rarity:
                        rarity_counts[slot.rarity.title()] = rarity_counts.get(slot.rarity.title(), 0) + 1
                embed.description = f"Panel: **{session.panel.title()}**\nBusiness: **{detail.name}** (`{detail.key}`)"
                embed.add_field(name="Progress", value=f"Level **{detail.visible_level}**\nPrestige **{detail.prestige}**", inline=True)
                embed.add_field(name="Staffing", value=f"Managers **{detail.manager_slots_used}/{detail.manager_slots_total}**\nEmployees **{detail.worker_slots_used}/{detail.worker_slots_total}**", inline=True)
                embed.add_field(name="Dates", value=f"Created {_fmt_dt(ownership.created_at)}\nUpdated {_fmt_dt(ownership.updated_at)}", inline=True)
                if session.panel == "overview":
                    embed.add_field(name="Overview", value=f"Owner: <@{ownership.user_id}>\nIncome Mod: x{detail.prestige_multiplier}\nHourly Profit: {detail.hourly_profit:,}\nRunning: {'Yes' if detail.running else 'No'}", inline=False)
                    embed.add_field(name="Rarity Breakdown", value="\n".join(f"• {k}: {v}" for k, v in sorted(rarity_counts.items())) or "No active staff.", inline=False)
                elif session.panel == "managers":
                    lines=[]
                    active=[slot for slot in manager_slots]
                    start=session.page*_PANEL_PAGE_SIZE
                    for slot in active[start:start+_PANEL_PAGE_SIZE]:
                        if slot.is_active:
                            lines.append(f"Slot {slot.slot_index}: **{slot.manager_name}** ({slot.rarity}) • +{slot.runtime_bonus_hours}h • {_bp_to_percent(slot.profit_bonus_bp)} • restart {slot.auto_restart_charges}")
                        else:
                            lines.append(f"Slot {slot.slot_index}: *(empty)*")
                    embed.add_field(name="Manager List", value="\n".join(lines) or "No manager slots.", inline=False)
                elif session.panel == "employees":
                    lines=[]
                    active=[slot for slot in worker_slots]
                    start=session.page*_PANEL_PAGE_SIZE
                    for slot in active[start:start+_PANEL_PAGE_SIZE]:
                        if slot.is_active:
                            lines.append(f"Slot {slot.slot_index}: **{slot.worker_name}** ({slot.rarity}/{slot.worker_type}) • +{slot.flat_profit_bonus:,} • {_bp_to_percent(slot.percent_profit_bonus_bp)}")
                        else:
                            lines.append(f"Slot {slot.slot_index}: *(empty)*")
                    embed.add_field(name="Employee List", value="\n".join(lines) or "No employee slots.", inline=False)
                elif session.panel == "level":
                    embed.add_field(name="Level Controls", value=f"Current visible level: **{detail.visible_level}**\nUse the buttons below for ±1/5/10 or set an exact stored level via modal.", inline=False)
                elif session.panel == "prestige":
                    embed.add_field(name="Prestige Controls", value=f"Current prestige: **{detail.prestige}**\nUse the buttons below for ±1 or exact set.", inline=False)
                elif session.panel == "core":
                    embed.add_field(name="Core Stats", value=f"Stored Level: `{ownership.level}`\nPrestige: `{ownership.prestige}`\nTotal Earned: `{ownership.total_earned}`\nTotal Spent: `{ownership.total_spent}`\nActive Run: `{run.id if run else 'none'}`", inline=False)
                elif session.panel == "special":
                    target_kind = "worker" if session.special_staff_type == "employee" else "manager"
                    amount_label = "Max available" if session.special_roll_amount_key == "max" else _fmt_int(_clamp_int(_parse_int(session.special_roll_amount_key, 10), 1, AUTO_HIRE_MAX_REROLLS))
                    embed.add_field(
                        name="Grant Special Staff",
                        value=(
                            f"Type: **{session.special_staff_type.title()}**\n"
                            f"Rarity Filter: **{_display_rarity_filter(session.special_rarity_filter_key)}**\n"
                            f"Kind Filter: **{_kind_label(target_kind, session.special_kind_key)}**\n"
                            f"Max Rolls: **{amount_label}**"
                        ),
                        inline=False,
                    )
            view = BusinessAdminDashboardView(cog=self, guild_id=guild_id, session=session, ownerships=ownerships)
            self._configure_business_admin_view(view, ownership is not None)
            return {"embed": embed, "view": view}

    def _configure_business_admin_view(self, view: BusinessAdminDashboardView, has_business: bool) -> None:
        view.btn_overview.disabled = not has_business
        view.btn_managers.disabled = not has_business
        view.btn_employees.disabled = not has_business
        view.btn_level.disabled = not has_business
        view.btn_prestige.disabled = not has_business
        view.btn_special.disabled = not has_business
        panel = view.session.panel
        primary = {"overview": "Fix Data", "managers": "Add/Edit", "employees": "Add/Edit", "level": "+1 / Set", "prestige": "+1 / Set", "core": "Edit Core", "special": "Grant"}.get(panel, "Action")
        secondary = {"overview": "Initialize", "managers": "Remove/Replace", "employees": "Remove/Replace", "level": "-1 / +5", "prestige": "-1", "core": "Normalize", "special": "Cycle Type"}.get(panel, "Secondary")
        view.btn_action.label = primary
        view.btn_secondary.label = secondary
        view.btn_action.disabled = not has_business and panel not in {"overview", "core"}
        view.btn_secondary.disabled = False

    async def _business_admin_adjust_level(self, interaction: discord.Interaction, admin_session: BusinessAdminSession, delta: int) -> None:
        view = BusinessAdminDashboardView(cog=self, guild_id=int(interaction.guild_id), session=admin_session, ownerships=[])
        session_obj = admin_session
        async with self.sessionmaker() as session:
            async with session.begin():
                ownership = await session.scalar(select(BusinessOwnershipRow).where(BusinessOwnershipRow.guild_id == int(interaction.guild_id), BusinessOwnershipRow.user_id == int(session_obj.target_user_id), BusinessOwnershipRow.business_key == str(session_obj.target_business_key)))
                if ownership is None:
                    await interaction.response.send_message("Target user has no selected business.", ephemeral=True)
                    return
                before = {"level": int(ownership.level), "prestige": int(ownership.prestige)}
                ownership.level = max(0, int(ownership.level) + int(delta))
                await self._log_business_admin_action(session, guild_id=interaction.guild_id, actor_user_id=interaction.user.id, target_user_id=session_obj.target_user_id, action="update", table_name="business_ownership", pk_json={"id": ownership.id}, before=before, after={"level": ownership.level, "prestige": ownership.prestige}, reason=f"Level delta {delta}")
        payload = await self._build_business_admin_payload(guild_id=int(interaction.guild_id), session=admin_session)
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=payload["embed"], view=payload["view"])
        else:
            await interaction.response.edit_message(embed=payload["embed"], view=payload["view"])


    async def _handle_business_admin_primary_action(self, interaction: discord.Interaction, admin_session: BusinessAdminSession, view: BusinessAdminDashboardView) -> None:
        panel = admin_session.panel
        if panel == "level":
            await self._business_admin_adjust_level(interaction, admin_session, 1)
            return
        if panel == "prestige":
            await self._business_admin_adjust_prestige(interaction, admin_session, 1)
            return
        if panel == "special":
            setup_view = AdminGrantStaffSetupView(parent_view=view)
            await interaction.response.edit_message(embed=await setup_view._summary_embed(), view=setup_view)
            return
        if panel == "core":
            await interaction.response.send_modal(AdminValueModal(title="Edit Core Stats", fields=[("level","Stored level", "0", True), ("prestige","Prestige", "0", True), ("earned","Total earned", "0", True), ("spent","Total spent", "0", True)], on_submit_cb=lambda i,v: self._business_admin_save_core_modal(i, admin_session, v)))
            return
        await interaction.response.send_message("Switch to a specific panel to perform that action.", ephemeral=True)

    async def _handle_business_admin_secondary_action(self, interaction: discord.Interaction, admin_session: BusinessAdminSession, view: BusinessAdminDashboardView) -> None:
        panel = admin_session.panel
        if panel == "level":
            await self._business_admin_adjust_level(interaction, admin_session, -1)
            return
        if panel == "prestige":
            await self._business_admin_adjust_prestige(interaction, admin_session, -1)
            return
        if panel == "special":
            admin_session.special_staff_type = "employee" if admin_session.special_staff_type == "manager" else "manager"
            admin_session.special_staff_template = None
            admin_session.special_kind_key = "any"
            await view.refresh(interaction, notice=f"Grant type changed to {admin_session.special_staff_type}.")
            return
        await interaction.response.send_message("Secondary action is not available on this panel yet.", ephemeral=True)

    async def _business_admin_adjust_prestige(self, interaction: discord.Interaction, admin_session: BusinessAdminSession, delta: int) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                ownership = await session.scalar(select(BusinessOwnershipRow).where(BusinessOwnershipRow.guild_id == int(interaction.guild_id), BusinessOwnershipRow.user_id == int(admin_session.target_user_id), BusinessOwnershipRow.business_key == str(admin_session.target_business_key)))
                if ownership is None:
                    await interaction.response.send_message("Target user has no selected business.", ephemeral=True)
                    return
                before = {"level": int(ownership.level), "prestige": int(ownership.prestige)}
                ownership.prestige = max(0, int(ownership.prestige) + int(delta))
                await self._log_business_admin_action(session, guild_id=interaction.guild_id, actor_user_id=interaction.user.id, target_user_id=admin_session.target_user_id, action="update", table_name="business_ownership", pk_json={"id": ownership.id}, before=before, after={"level": ownership.level, "prestige": ownership.prestige}, reason=f"Prestige delta {delta}")
        payload = await self._build_business_admin_payload(guild_id=int(interaction.guild_id), session=admin_session)
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=payload["embed"], view=payload["view"])
        else:
            await interaction.response.edit_message(embed=payload["embed"], view=payload["view"])

    async def _business_admin_save_core_modal(self, interaction: discord.Interaction, admin_session: BusinessAdminSession, values: dict[str, str]) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                ownership = await session.scalar(select(BusinessOwnershipRow).where(BusinessOwnershipRow.guild_id == int(interaction.guild_id), BusinessOwnershipRow.user_id == int(admin_session.target_user_id), BusinessOwnershipRow.business_key == str(admin_session.target_business_key)))
                if ownership is None:
                    await interaction.response.send_message("Target user has no selected business.", ephemeral=True)
                    return
                before = {"level": int(ownership.level), "prestige": int(ownership.prestige), "total_earned": int(ownership.total_earned), "total_spent": int(ownership.total_spent)}
                ownership.level = max(0, int(values["level"]))
                ownership.prestige = max(0, int(values["prestige"]))
                ownership.total_earned = max(0, int(values["earned"]))
                ownership.total_spent = max(0, int(values["spent"]))
                await self._log_business_admin_action(session, guild_id=interaction.guild_id, actor_user_id=interaction.user.id, target_user_id=admin_session.target_user_id, action="update", table_name="business_ownership", pk_json={"id": ownership.id}, before=before, after={"level": ownership.level, "prestige": ownership.prestige, "total_earned": ownership.total_earned, "total_spent": ownership.total_spent}, reason="Core stats edit")
        payload = await self._build_business_admin_payload(guild_id=int(interaction.guild_id), session=admin_session)
        await interaction.response.edit_message(embed=payload["embed"], view=payload["view"])

    async def _business_admin_grant_special(self, interaction: discord.Interaction, admin_session: BusinessAdminSession) -> None:
        target_kind = "worker" if admin_session.special_staff_type == "employee" else "manager"
        rarity_filters = _build_rarity_filter_options(target_kind=target_kind)
        allowed_rarities = {_normalize_rarity_key(r) for r in rarity_filters.get(admin_session.special_rarity_filter_key, rarity_filters["any"])}
        if target_kind == "manager" and not _manager_kind_pool_possible(admin_session.special_kind_key, allowed_rarities):
            await interaction.response.send_message("This kind + rarity combination has an empty result pool.", ephemeral=True)
            return

        max_rolls = AUTO_HIRE_MAX_REROLLS if admin_session.special_roll_amount_key == "max" else _clamp_int(_parse_int(admin_session.special_roll_amount_key, 10), 1, AUTO_HIRE_MAX_REROLLS)
        matched_candidate: WorkerCandidateSnapshot | ManagerCandidateSnapshot | None = None
        best_candidate: WorkerCandidateSnapshot | ManagerCandidateSnapshot | None = None
        rolls_used = 0

        async with self.sessionmaker() as session:
            async with session.begin():
                for _ in range(max_rolls):
                    if target_kind == "manager":
                        roll_result = await roll_manager_candidate(
                            session,
                            guild_id=int(interaction.guild_id),
                            user_id=int(admin_session.target_user_id),
                            business_key=str(admin_session.target_business_key),
                            reroll_cost=0,
                        )
                        candidate = roll_result.manager_candidate
                    else:
                        roll_result = await roll_worker_candidate(
                            session,
                            guild_id=int(interaction.guild_id),
                            user_id=int(admin_session.target_user_id),
                            business_key=str(admin_session.target_business_key),
                            reroll_cost=0,
                        )
                        candidate = roll_result.worker_candidate
                    if (not roll_result.ok) or candidate is None:
                        result = roll_result
                        break
                    rolls_used += 1
                    if best_candidate is None:
                        best_candidate = candidate
                    else:
                        score_now = _manager_candidate_score(candidate) if target_kind == "manager" else _worker_candidate_score(candidate)  # type: ignore[arg-type]
                        score_best = _manager_candidate_score(best_candidate) if target_kind == "manager" else _worker_candidate_score(best_candidate)  # type: ignore[arg-type]
                        if score_now > score_best:
                            best_candidate = candidate
                    rarity_ok = _normalize_rarity_key(getattr(candidate, "rarity", "")) in allowed_rarities
                    kind_ok = _manager_matches_kind(candidate, admin_session.special_kind_key) if target_kind == "manager" else _worker_matches_kind(candidate, admin_session.special_kind_key)  # type: ignore[arg-type]
                    if rarity_ok and kind_ok:
                        matched_candidate = candidate
                        break
                else:
                    result = BusinessActionResult(ok=False, message="No candidate matched the selected filters in the allowed roll budget.")

                if matched_candidate is not None:
                    if target_kind == "manager":
                        result = await hire_manager_manual(
                            session,
                            guild_id=int(interaction.guild_id),
                            user_id=int(admin_session.target_user_id),
                            business_key=str(admin_session.target_business_key),
                            manager_name=str(getattr(matched_candidate, "manager_name", "Manager")),
                            rarity=str(getattr(matched_candidate, "rarity", "common")),
                            runtime_bonus_hours=int(getattr(matched_candidate, "runtime_bonus_hours", 0) or 0),
                            profit_bonus_bp=int(getattr(matched_candidate, "profit_bonus_bp", 0) or 0),
                            auto_restart_charges=int(getattr(matched_candidate, "auto_restart_charges", 0) or 0),
                            charge_silver=False,
                        )
                    else:
                        result = await hire_worker_manual(
                            session,
                            guild_id=int(interaction.guild_id),
                            user_id=int(admin_session.target_user_id),
                            business_key=str(admin_session.target_business_key),
                            worker_name=str(getattr(matched_candidate, "worker_name", "Worker")),
                            worker_type=str(getattr(matched_candidate, "worker_type", "efficient")),
                            rarity=str(getattr(matched_candidate, "rarity", "common")),
                            flat_profit_bonus=int(getattr(matched_candidate, "flat_profit_bonus", 0) or 0),
                            percent_profit_bonus_bp=int(getattr(matched_candidate, "percent_profit_bonus_bp", 0) or 0),
                            charge_silver=False,
                        )
                best_name = _safe_str(
                    getattr(best_candidate, "worker_name", getattr(best_candidate, "manager_name", None)) if best_candidate is not None else None,
                    "None",
                )
                best_rarity = _safe_str(getattr(best_candidate, "rarity", None), "n/a") if best_candidate is not None else "n/a"
                await self._log_business_admin_action(
                    session,
                    guild_id=interaction.guild_id,
                    actor_user_id=interaction.user.id,
                    target_user_id=admin_session.target_user_id,
                    action="insert",
                    table_name=f"business_{admin_session.special_staff_type}_assignments",
                    pk_json={"business_key": admin_session.target_business_key},
                    before=None,
                    after={
                        "ok": result.ok,
                        "message": result.message,
                        "rolls_used": rolls_used,
                        "rarity_filter": admin_session.special_rarity_filter_key,
                        "kind_filter": admin_session.special_kind_key,
                        "best_candidate": best_name,
                        "best_rarity": best_rarity,
                    },
                    reason=f"Granted special {admin_session.special_staff_type} via filtered roll",
                )
        payload = await self._build_business_admin_payload(guild_id=int(interaction.guild_id), session=admin_session)
        embed = payload["embed"]
        embed.description = f"{result.message}\nRolls used: **{_fmt_int(rolls_used)}**\n\n{embed.description or ''}".strip()
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=payload["view"])
        else:
            await interaction.response.edit_message(embed=embed, view=payload["view"])

    @app_commands.command(name="businessadmin", description="Open the admin-only business management dashboard for a target user.")
    async def business_admin_dashboard_cmd(self, interaction: discord.Interaction, user: discord.Member) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not await self._business_admin_authorized(interaction):
            await interaction.response.send_message(_ACCESS_DENIED, ephemeral=True)
            return
        admin_session = BusinessAdminSession(admin_id=int(interaction.user.id), target_user_id=int(user.id))
        await interaction.response.defer(ephemeral=True, thinking=True)
        payload = await self._build_business_admin_payload(guild_id=int(interaction.guild.id), session=admin_session)
        await interaction.followup.send(embed=payload["embed"], view=payload["view"], ephemeral=True)

    async def _fetch_business_grant_choices(self, session, *, guild_id: int, user_id: int) -> list[tuple[str, str]]:
        ownerships = await session.scalars(
            select(BusinessOwnershipRow)
            .where(BusinessOwnershipRow.guild_id == int(guild_id), BusinessOwnershipRow.user_id == int(user_id))
            .order_by(BusinessOwnershipRow.updated_at.desc(), BusinessOwnershipRow.id.desc())
        )
        rows = list(ownerships.all())
        return [(str(row.business_key), str(row.business_key).replace("_", " ").title()) for row in rows]

    async def _count_matching_staff(self, session, *, guild_id: int, user_id: int, business_key: str, grant_type: str, unit_name: str, rarity: str) -> int:
        rarity_key = _normalize_rarity_key(rarity)
        if grant_type == "worker":
            rows = await session.scalars(
                select(BusinessWorkerAssignmentRow).where(
                    BusinessWorkerAssignmentRow.guild_id == int(guild_id),
                    BusinessWorkerAssignmentRow.user_id == int(user_id),
                    BusinessWorkerAssignmentRow.business_key == str(business_key),
                    BusinessWorkerAssignmentRow.is_active == True,  # noqa: E712
                    BusinessWorkerAssignmentRow.worker_name == str(unit_name),
                )
            )
            return sum(1 for row in rows.all() if _normalize_rarity_key(getattr(row, "rarity", "")) == rarity_key)
        rows = await session.scalars(
            select(BusinessManagerAssignmentRow).where(
                BusinessManagerAssignmentRow.guild_id == int(guild_id),
                BusinessManagerAssignmentRow.user_id == int(user_id),
                BusinessManagerAssignmentRow.business_key == str(business_key),
                BusinessManagerAssignmentRow.is_active == True,  # noqa: E712
                BusinessManagerAssignmentRow.manager_name == str(unit_name),
            )
        )
        return sum(1 for row in rows.all() if _normalize_rarity_key(getattr(row, "rarity", "")) == rarity_key)

    def _resolve_staff_catalog(self, *, grant_type: str, business_key: Optional[str], rarity: Optional[str]) -> list[StaffCatalogEntry]:
        rarity_filter = {str(rarity).strip().lower()} if rarity else None
        entries = get_staff_grant_catalog(
            staff_kind=str(grant_type),
            business_key=business_key,
            rarity_filter=rarity_filter,
        )
        if not entries:
            log.warning(
                "admin_businessgrant catalog returned no entries | kind=%s business=%s rarity=%s",
                grant_type,
                business_key,
                rarity,
            )
        return entries

    def _find_staff_catalog_entry(self, *, state: "BusinessAdminGrantState") -> Optional[StaffCatalogEntry]:
        if not state.unit_key:
            return None
        entries = self._resolve_staff_catalog(
            grant_type=state.grant_type,
            business_key=state.business_key,
            rarity=state.rarity,
        )
        for entry in entries:
            if entry.key == state.unit_key:
                return entry
        log.warning(
            "admin_businessgrant selected entry missing from live catalog | key=%s type=%s business=%s rarity=%s",
            state.unit_key,
            state.grant_type,
            state.business_key,
            state.rarity,
        )
        return None

    async def _execute_business_admin_grant(self, *, guild_id: int, actor_user_id: int, state: "BusinessAdminGrantState") -> tuple[bool, str, dict]:
        grant_type = state.grant_type
        if state.target_user_id is None:
            return False, "Pick a target user first.", {}
        if not state.business_key:
            return False, "Pick a business first.", {}
        selected_entry = self._find_staff_catalog_entry(state=state)
        if selected_entry is None:
            return False, "Pick a worker/manager first.", {}
        quantity = max(1, int(state.quantity))
        rarity = _normalize_rarity(selected_entry.rarity)
        unit_name = str(selected_entry.display_name)
        details: dict = {}
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    await ensure_user_rows(session, guild_id=int(guild_id), user_id=int(state.target_user_id))
                    ownership = await session.scalar(
                        select(BusinessOwnershipRow).where(
                            BusinessOwnershipRow.guild_id == int(guild_id),
                            BusinessOwnershipRow.user_id == int(state.target_user_id),
                            BusinessOwnershipRow.business_key == str(state.business_key),
                        )
                    )
                    if ownership is None:
                        await self._log_business_admin_action(
                            session,
                            guild_id=guild_id,
                            actor_user_id=actor_user_id,
                            target_user_id=int(state.target_user_id),
                            action="grant_failed",
                            table_name=f"business_{grant_type}_assignments",
                            pk_json={"business_key": state.business_key},
                            before=None,
                            after={"ok": False, "reason": "missing_business", "requested_quantity": quantity},
                            reason="Interactive admin business grant",
                        )
                        return False, "Selected business no longer exists for that user.", {}
                    before_count = await self._count_matching_staff(
                        session,
                        guild_id=guild_id,
                        user_id=int(state.target_user_id),
                        business_key=str(state.business_key),
                        grant_type=grant_type,
                        unit_name=unit_name,
                        rarity=rarity,
                    )
                    if grant_type == "worker":
                        slots = await get_worker_assignment_slots(
                            session,
                            guild_id=int(guild_id),
                            user_id=int(state.target_user_id),
                            business_key=str(state.business_key),
                        )
                    else:
                        slots = await get_manager_assignment_slots(
                            session,
                            guild_id=int(guild_id),
                            user_id=int(state.target_user_id),
                            business_key=str(state.business_key),
                        )
                    empty_slots = sum(1 for slot in slots if not bool(getattr(slot, "is_active", False)))
                    to_grant = min(quantity, empty_slots)
                    if to_grant <= 0:
                        return False, "No empty staff slots are available on this business.", {"empty_slots": 0}
                    for _ in range(to_grant):
                        if grant_type == "worker":
                            result = await hire_worker_manual(
                                session,
                                guild_id=int(guild_id),
                                user_id=int(state.target_user_id),
                                business_key=str(state.business_key),
                                worker_name=unit_name,
                                worker_type=str(selected_entry.worker_type or "efficient"),
                                rarity=rarity,
                                flat_profit_bonus=int(selected_entry.flat_profit_bonus or 0),
                                percent_profit_bonus_bp=int(selected_entry.percent_profit_bonus_bp or 0),
                                charge_silver=False,
                            )
                        else:
                            result = await hire_manager_manual(
                                session,
                                guild_id=int(guild_id),
                                user_id=int(state.target_user_id),
                                business_key=str(state.business_key),
                                manager_name=unit_name,
                                rarity=rarity,
                                runtime_bonus_hours=int(selected_entry.runtime_bonus_hours or 0),
                                profit_bonus_bp=int(selected_entry.profit_bonus_bp or 0),
                                auto_restart_charges=int(selected_entry.auto_restart_charges or 0),
                                charge_silver=False,
                            )
                        if not result.ok:
                            raise RuntimeError(str(result.message))
                    after_count = await self._count_matching_staff(
                        session,
                        guild_id=guild_id,
                        user_id=int(state.target_user_id),
                        business_key=str(state.business_key),
                        grant_type=grant_type,
                        unit_name=unit_name,
                        rarity=rarity,
                    )
                    details = {
                        "ok": True,
                        "requested_quantity": quantity,
                        "granted_quantity": to_grant,
                        "empty_slots_before": empty_slots,
                        "owned_before": before_count,
                        "owned_after": after_count,
                        "unit_name": unit_name,
                        "unit_key": selected_entry.key,
                        "rarity": rarity,
                        "grant_type": grant_type,
                    }
                    await self._log_business_admin_action(
                        session,
                        guild_id=guild_id,
                        actor_user_id=actor_user_id,
                        target_user_id=int(state.target_user_id),
                        action="grant",
                        table_name=f"business_{grant_type}_assignments",
                        pk_json={"business_key": state.business_key},
                        before={"owned_count": before_count},
                        after=details,
                        reason="Interactive admin business grant",
                    )
        except Exception as exc:
            log.exception(
                "admin_businessgrant failed | guild_id=%s actor=%s target=%s type=%s business=%s",
                guild_id,
                actor_user_id,
                state.target_user_id,
                grant_type,
                state.business_key,
            )
            log.warning(
                "admin_businessgrant audit | status=failure guild_id=%s admin=%s target=%s entry=%s type=%s business=%s qty=%s ts=%s",
                guild_id,
                actor_user_id,
                state.target_user_id,
                state.unit_key,
                grant_type,
                state.business_key,
                quantity,
                datetime.now(timezone.utc).isoformat(),
            )
            return False, f"Grant failed: {exc}", {}
        granted = int(details.get("granted_quantity", 0) or 0)
        requested = int(details.get("requested_quantity", quantity) or quantity)
        suffix = f" (requested {requested}, granted {granted} due to open slots)." if granted != requested else "."
        log.info(
            "admin_businessgrant audit | status=success guild_id=%s admin=%s target=%s entry=%s name=%s type=%s business=%s rarity=%s qty=%s granted=%s ts=%s",
            guild_id,
            actor_user_id,
            state.target_user_id,
            details.get("unit_key"),
            unit_name,
            grant_type,
            state.business_key,
            rarity,
            requested,
            granted,
            datetime.now(timezone.utc).isoformat(),
        )
        return True, f"Granted **{granted}x {unit_name}** ({rarity.title()}) to <@{int(state.target_user_id)}>{suffix}", details

    @app_commands.command(name="admin_businessgrant", description="Admin: interactive worker/manager grant panel.")
    @app_commands.checks.has_permissions(administrator=True)
    async def admin_business_grant_cmd(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not await self._business_admin_authorized(interaction):
            await interaction.response.send_message(_ACCESS_DENIED, ephemeral=True)
            return
        state = BusinessAdminGrantState(admin_user_id=int(interaction.user.id))
        view = BusinessAdminGrantView(cog=self, guild_id=int(interaction.guild.id), state=state)
        await view._refresh_businesses()
        view._build_components()
        embed = await view.build_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    @app_commands.command(name="business_admin_hire_worker", description="[Admin] Give a worker to a player. Business key is optional.")
    @app_commands.describe(
        target_user="Player receiving the worker (defaults to you).",
        business_key="Business key to receive the worker (optional: auto-picks player's most recent business).",
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def business_admin_hire_worker_cmd(
        self,
        interaction: discord.Interaction,
        worker_name: str,
        worker_type: str,
        rarity: str,
        flat_profit_bonus: int,
        percent_profit_bonus_bp: int,
        target_user: Optional[discord.Member] = None,
        business_key: Optional[str] = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True, thinking=True)
        owner_id = int(target_user.id) if target_user is not None else int(interaction.user.id)
        async with self.sessionmaker() as session:
            async with session.begin():
                resolved_business_key = _safe_str(business_key, "").strip().lower()
                if not resolved_business_key:
                    latest_ownership = await session.scalar(
                        select(BusinessOwnershipRow)
                        .where(
                            BusinessOwnershipRow.guild_id == int(interaction.guild.id),
                            BusinessOwnershipRow.user_id == owner_id,
                        )
                        .order_by(BusinessOwnershipRow.updated_at.desc(), BusinessOwnershipRow.id.desc())
                    )
                    if latest_ownership is None:
                        await interaction.followup.send(
                            "That player has no businesses yet. Provide a business key or have them buy one first.",
                            ephemeral=True,
                        )
                        return
                    resolved_business_key = str(latest_ownership.business_key)
                result = await hire_worker_manual(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=owner_id,
                    business_key=resolved_business_key,
                    worker_name=worker_name,
                    worker_type=worker_type,
                    rarity=rarity,
                    flat_profit_bonus=flat_profit_bonus,
                    percent_profit_bonus_bp=percent_profit_bonus_bp,
                )
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=owner_id,
                    business_key=resolved_business_key,
                )
        if detail is None:
            await interaction.followup.send("That business could not be found for this player.", ephemeral=True)
            return
        if result.ok and result.hired_worker is not None:
            owner_member = target_user or interaction.user
            embed = _build_worker_hire_result_embed(user=owner_member, detail=detail, hired=result.hired_worker)
            embed.add_field(name="Mode", value="Admin Manual Grant", inline=False)
            embed.add_field(name="Target", value=f"{owner_member.mention} • `{resolved_business_key}`", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        await interaction.followup.send(
            embed=_build_result_embed(title="Admin Worker Grant", message=result.message, ok=False),
            ephemeral=True,
        )

    @app_commands.command(name="business_admin_hire_manager", description="[Admin] Give a manager to a player. Business key is optional.")
    @app_commands.describe(
        target_user="Player receiving the manager (defaults to you).",
        business_key="Business key to receive the manager (optional: auto-picks player's most recent business).",
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def business_admin_hire_manager_cmd(
        self,
        interaction: discord.Interaction,
        manager_name: str,
        rarity: str,
        runtime_bonus_hours: int,
        profit_bonus_bp: int,
        auto_restart_charges: int,
        target_user: Optional[discord.Member] = None,
        business_key: Optional[str] = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True, thinking=True)
        owner_id = int(target_user.id) if target_user is not None else int(interaction.user.id)
        async with self.sessionmaker() as session:
            async with session.begin():
                resolved_business_key = _safe_str(business_key, "").strip().lower()
                if not resolved_business_key:
                    latest_ownership = await session.scalar(
                        select(BusinessOwnershipRow)
                        .where(
                            BusinessOwnershipRow.guild_id == int(interaction.guild.id),
                            BusinessOwnershipRow.user_id == owner_id,
                        )
                        .order_by(BusinessOwnershipRow.updated_at.desc(), BusinessOwnershipRow.id.desc())
                    )
                    if latest_ownership is None:
                        await interaction.followup.send(
                            "That player has no businesses yet. Provide a business key or have them buy one first.",
                            ephemeral=True,
                        )
                        return
                    resolved_business_key = str(latest_ownership.business_key)
                result = await hire_manager_manual(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=owner_id,
                    business_key=resolved_business_key,
                    manager_name=manager_name,
                    rarity=rarity,
                    runtime_bonus_hours=runtime_bonus_hours,
                    profit_bonus_bp=profit_bonus_bp,
                    auto_restart_charges=auto_restart_charges,
                )
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=owner_id,
                    business_key=resolved_business_key,
                )
        if detail is None:
            await interaction.followup.send("That business could not be found for this player.", ephemeral=True)
            return
        if result.ok and result.hired_manager is not None:
            owner_member = target_user or interaction.user
            embed = _build_manager_hire_result_embed(user=owner_member, detail=detail, hired=result.hired_manager)
            embed.add_field(name="Mode", value="Admin Manual Grant", inline=False)
            embed.add_field(name="Target", value=f"{owner_member.mention} • `{resolved_business_key}`", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        await interaction.followup.send(
            embed=_build_result_embed(title="Admin Manager Grant", message=result.message, ok=False),
            ephemeral=True,
        )
from dataclasses import asdict


def _fmt_dt(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return discord.utils.format_dt(dt, style="F")


def _bp_to_percent(bp: int) -> str:
    return f"{bp / 100:.2f}%"


def _normalize_rarity(value: str) -> str:
    text = str(value or "common").strip().lower()
    return text if text in RARITY_ORDER else "common"


class BusinessAdminSession:
    def __init__(self, *, admin_id: int, target_user_id: int, target_business_key: Optional[str] = None):
        self.admin_id = int(admin_id)
        self.target_user_id = int(target_user_id)
        self.target_business_key = target_business_key
        self.panel = "overview"
        self.page = 0
        self.selected_slot: Optional[int] = None
        self.special_staff_type = "manager"
        self.special_staff_rarity = "mythical"
        self.special_staff_template: Optional[str] = None
        self.special_rarity_filter_key = "any"
        self.special_kind_key = "any"
        self.special_roll_amount_key = "10"


@dataclass
class BusinessAdminGrantState:
    admin_user_id: int
    target_user_id: Optional[int] = None
    grant_type: str = "worker"
    business_key: Optional[str] = None
    rarity: str = "any"
    unit_key: Optional[str] = None
    quantity: int = 1
    business_page: int = 0
    unit_page: int = 0
    business_filter_key: str = "__selected__"
    processing: bool = False
    last_success_message: Optional[str] = None
    last_result_details: Optional[dict] = None


class BusinessAdminGrantView(discord.ui.View):
    def __init__(self, *, cog: "BusinessCog", guild_id: int, state: BusinessAdminGrantState):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = int(guild_id)
        self.state = state
        self._businesses: list[tuple[str, str]] = []
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        allowed = await self.cog._business_admin_authorized(interaction)
        if (not allowed) or int(interaction.user.id) != int(self.state.admin_user_id):
            msg = _ACCESS_DENIED if not allowed else "This admin grant panel belongs to another admin."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
            return False
        return True

    def _unit_catalog(self) -> list[StaffCatalogEntry]:
        business_key = self.state.business_key if self.state.business_filter_key == "__selected__" else None
        rarity = None if self.state.rarity == "any" else self.state.rarity
        return self.cog._resolve_staff_catalog(
            grant_type=self.state.grant_type,
            business_key=business_key,
            rarity=rarity,
        )

    def _selected_unit(self) -> Optional[StaffCatalogEntry]:
        if not self.state.unit_key:
            return None
        for entry in self._unit_catalog():
            if entry.key == self.state.unit_key:
                return entry
        return None

    def _rarity_options(self) -> list[str]:
        if self.state.grant_type == "worker":
            return ["any", "common", "uncommon", "rare", "epic", "mythical"]
        return ["any", "common", "rare", "epic", "legendary", "mythical"]

    async def _refresh_businesses(self) -> None:
        if self.state.target_user_id is None:
            self._businesses = []
            self.state.business_key = None
            return
        async with self.cog.sessionmaker() as session:
            self._businesses = await self.cog._fetch_business_grant_choices(
                session,
                guild_id=self.guild_id,
                user_id=int(self.state.target_user_id),
            )
        if self.state.business_key not in {key for key, _ in self._businesses}:
            self.state.business_key = self._businesses[0][0] if self._businesses else None

    async def build_embed(self, *, error: Optional[str] = None) -> discord.Embed:
        await self._refresh_businesses()
        embed = discord.Embed(
            title="🛠️ Business Grant Control Panel",
            description="Fast admin grant flow for workers/managers. Pick values, review, then confirm.",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        target = f"<@{self.state.target_user_id}> (`{self.state.target_user_id}`)" if self.state.target_user_id else "`Not selected`"
        selected_business = f"`{self.state.business_key}`" if self.state.business_key else "`Not selected`"
        selected_entry = self._selected_unit()
        selected_unit = selected_entry.display_name if selected_entry else "Not selected"
        unit_catalog = self._unit_catalog()
        unit_page_size = 25
        unit_pages = max(1, (len(unit_catalog) + unit_page_size - 1) // unit_page_size)
        self.state.unit_page = max(0, min(self.state.unit_page, unit_pages - 1))
        embed.add_field(
            name="Selection",
            value=(
                f"**Target**: {target}\n"
                f"**Type**: `{self.state.grant_type.title()}`\n"
                f"**Business**: {selected_business}\n"
                f"**Catalog Filter**: `{'Selected business' if self.state.business_filter_key == '__selected__' else 'All businesses'}`\n"
                f"**Rarity**: `{'Any' if self.state.rarity == 'any' else _normalize_rarity(self.state.rarity).title()}`\n"
                f"**Unit**: `{selected_unit}`\n"
                f"**Page**: `{self.state.unit_page + 1}/{unit_pages}`\n"
                f"**Quantity**: `{max(1, int(self.state.quantity))}`"
            ),
            inline=False,
        )
        if self.state.target_user_id and self.state.business_key and selected_entry:
            async with self.cog.sessionmaker() as session:
                owned = await self.cog._count_matching_staff(
                    session,
                    guild_id=self.guild_id,
                    user_id=int(self.state.target_user_id),
                    business_key=str(self.state.business_key),
                    grant_type=self.state.grant_type,
                    unit_name=str(selected_entry.display_name),
                    rarity=_normalize_rarity(selected_entry.rarity),
                )
            embed.add_field(name="Current Ownership", value=f"Currently owns **{owned}** matching units.", inline=False)
        if unit_catalog:
            page_start = self.state.unit_page * unit_page_size
            page_entries = unit_catalog[page_start : page_start + unit_page_size]
            preview_lines: list[str] = []
            for entry in page_entries[:6]:
                if entry.staff_kind == "worker":
                    preview_lines.append(
                        f"• **{entry.display_name}** ({entry.rarity.title()} • {entry.worker_type}) • +{entry.flat_profit_bonus:,} • {_bp_to_percent(entry.percent_profit_bonus_bp)}"
                    )
                else:
                    preview_lines.append(
                        f"• **{entry.display_name}** ({entry.rarity.title()}) • +{entry.runtime_bonus_hours}h • {_bp_to_percent(entry.profit_bonus_bp)} • AR {entry.auto_restart_charges}"
                    )
            embed.add_field(name="Catalog Preview", value="\n".join(preview_lines), inline=False)
        if self.state.last_success_message:
            details = self.state.last_result_details or {}
            embed.add_field(
                name="Latest Result",
                value=(
                    f"{self.state.last_success_message}\n"
                    f"Owned before: `{details.get('owned_before', 0)}` • after: `{details.get('owned_after', 0)}`"
                ),
                inline=False,
            )
        if error:
            embed.add_field(name="⚠️ Validation", value=error, inline=False)
        embed.set_footer(text="Step flow: User → Type → Business → Rarity → Unit → Quantity → Confirm")
        return embed

    def _build_components(self) -> None:
        self.clear_items()
        user_select = discord.ui.UserSelect(placeholder="1) Choose target user", min_values=1, max_values=1, row=0)

        async def user_select_cb(interaction: discord.Interaction) -> None:
            user = user_select.values[0]
            self.state.target_user_id = int(user.id)
            self.state.business_page = 0
            self.state.last_success_message = None
            self.state.last_result_details = None
            await self._refresh_and_render(interaction)

        user_select.callback = user_select_cb
        self.add_item(user_select)

        type_select = discord.ui.Select(
            placeholder="2) Choose grant type",
            min_values=1,
            max_values=1,
            row=1,
            options=[
                discord.SelectOption(label="Worker", value="worker", default=self.state.grant_type == "worker"),
                discord.SelectOption(label="Manager", value="manager", default=self.state.grant_type == "manager"),
            ],
        )

        async def type_select_cb(interaction: discord.Interaction) -> None:
            self.state.grant_type = type_select.values[0]
            self.state.unit_key = None
            self.state.rarity = "any"
            self.state.unit_page = 0
            await self._refresh_and_render(interaction)

        type_select.callback = type_select_cb
        self.add_item(type_select)

        page_size = 25
        start = max(0, int(self.state.business_page)) * page_size
        business_slice = self._businesses[start : start + page_size]
        business_options = [
            discord.SelectOption(label=label[:100], value=key, default=(key == self.state.business_key))
            for key, label in business_slice
        ]
        if start > 0:
            business_options.insert(0, discord.SelectOption(label="⬅ Previous page", value="__page_prev__"))
        if (start + page_size) < len(self._businesses):
            business_options.append(discord.SelectOption(label="Next page ➡", value="__page_next__"))
        if not business_options:
            business_options = [discord.SelectOption(label="No businesses found for selected user", value="__none__")]
        business_select = discord.ui.Select(placeholder="3) Choose business", min_values=1, max_values=1, row=2, options=business_options)
        business_select.disabled = not bool(business_slice)

        async def business_select_cb(interaction: discord.Interaction) -> None:
            value = business_select.values[0]
            if value == "__page_next__":
                self.state.business_page += 1
            elif value == "__page_prev__":
                self.state.business_page = max(0, self.state.business_page - 1)
            elif value != "__none__":
                self.state.business_key = value
            await self._refresh_and_render(interaction)

        business_select.callback = business_select_cb
        self.add_item(business_select)

        unit_catalog = self._unit_catalog()
        unit_page_size = 25
        total_pages = max(1, (len(unit_catalog) + unit_page_size - 1) // unit_page_size)
        self.state.unit_page = max(0, min(self.state.unit_page, total_pages - 1))
        unit_start = self.state.unit_page * unit_page_size
        unit_slice = unit_catalog[unit_start : unit_start + unit_page_size]
        unit_options: list[discord.SelectOption] = []
        for entry in unit_slice:
            desc = (
                f"{entry.rarity.title()} • {entry.worker_type} • +{entry.flat_profit_bonus} • {_bp_to_percent(entry.percent_profit_bonus_bp)}"
                if entry.staff_kind == "worker"
                else f"{entry.rarity.title()} • +{entry.runtime_bonus_hours}h • {_bp_to_percent(entry.profit_bonus_bp)}"
            )
            unit_options.append(
                discord.SelectOption(
                    label=entry.display_name[:100],
                    value=entry.key,
                    description=desc[:100],
                    default=(entry.key == self.state.unit_key),
                )
            )
        if not unit_options:
            unit_options = [discord.SelectOption(label="No matching catalog entries", value="__none__")]
        unit_select = discord.ui.Select(
            placeholder=f"4) Choose {'worker' if self.state.grant_type == 'worker' else 'manager'} (Page {self.state.unit_page + 1}/{total_pages})",
            min_values=1,
            max_values=1,
            row=3,
            options=unit_options,
        )
        unit_select.disabled = not bool(unit_slice)

        async def unit_select_cb(interaction: discord.Interaction) -> None:
            value = unit_select.values[0]
            if value != "__none__":
                self.state.unit_key = value
            await self._refresh_and_render(interaction)

        unit_select.callback = unit_select_cb
        self.add_item(unit_select)

        btn_rarity = discord.ui.Button(label=f"Rarity: {_normalize_rarity(self.state.rarity).title()}", style=discord.ButtonStyle.secondary, row=4)

        async def btn_rarity_cb(interaction: discord.Interaction) -> None:
            options = self._rarity_options()
            current = _normalize_rarity(self.state.rarity)
            idx = options.index(current) if current in options else 0
            self.state.rarity = options[(idx + 1) % len(options)]
            await self._refresh_and_render(interaction)

        btn_rarity.callback = btn_rarity_cb
        self.add_item(btn_rarity)

        btn_prev_units = discord.ui.Button(label="⬅ Prev", style=discord.ButtonStyle.secondary, row=4, disabled=self.state.unit_page <= 0)

        async def btn_prev_units_cb(interaction: discord.Interaction) -> None:
            self.state.unit_page = max(0, self.state.unit_page - 1)
            await self._refresh_and_render(interaction)

        btn_prev_units.callback = btn_prev_units_cb
        self.add_item(btn_prev_units)

        btn_next_units = discord.ui.Button(label="Next ➡", style=discord.ButtonStyle.secondary, row=4, disabled=self.state.unit_page >= (total_pages - 1))

        async def btn_next_units_cb(interaction: discord.Interaction) -> None:
            self.state.unit_page = min(total_pages - 1, self.state.unit_page + 1)
            await self._refresh_and_render(interaction)

        btn_next_units.callback = btn_next_units_cb
        self.add_item(btn_next_units)

        btn_confirm = discord.ui.Button(label="Confirm", style=discord.ButtonStyle.success, emoji="✅", row=4)

        async def btn_confirm_cb(interaction: discord.Interaction) -> None:
            if self.state.processing:
                await interaction.response.send_message("Grant is already processing. Please wait.", ephemeral=True)
                return
            error = self._validate()
            if error:
                await self._refresh_and_render(interaction, error=error)
                return
            self.state.processing = True
            for item in self.children:
                item.disabled = True
            if interaction.response.is_done():
                await interaction.edit_original_response(view=self)
            else:
                await interaction.response.edit_message(view=self)
            ok, message, details = await self.cog._execute_business_admin_grant(
                guild_id=self.guild_id,
                actor_user_id=int(interaction.user.id),
                state=self.state,
            )
            self.state.processing = False
            self.state.last_success_message = message if ok else None
            self.state.last_result_details = details if ok else None
            await self._refresh_and_render(interaction, error=None if ok else message)

        btn_confirm.callback = btn_confirm_cb
        self.add_item(btn_confirm)

        btn_cancel = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.danger, row=4)

        async def btn_cancel_cb(interaction: discord.Interaction) -> None:
            for item in self.children:
                item.disabled = True
            if interaction.response.is_done():
                await interaction.edit_original_response(view=self)
            else:
                await interaction.response.edit_message(view=self)

        btn_cancel.callback = btn_cancel_cb
        self.add_item(btn_cancel)

    async def _refresh_and_render(self, interaction: discord.Interaction, *, error: Optional[str] = None) -> None:
        await self._refresh_businesses()
        self._build_components()
        embed = await self.build_embed(error=error)
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    def _validate(self) -> Optional[str]:
        if self.state.target_user_id is None:
            return "Select a target user."
        if not self.state.business_key:
            return "Select a business."
        if self.state.grant_type not in {"worker", "manager"}:
            return "Invalid grant type selected."
        entry = self._selected_unit()
        if entry is None:
            return "Select a valid unit from the dropdown."
        if max(1, int(self.state.quantity)) <= 0:
            return "Quantity must be at least 1."
        return None


class BusinessAdminBaseView(discord.ui.View):
    def __init__(self, *, cog: "BusinessCog", guild_id: int, session: BusinessAdminSession, timeout: float = 600):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = int(guild_id)
        self.session = session

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        allowed = await self.cog._business_admin_authorized(interaction)
        if not allowed or int(interaction.user.id) != self.session.admin_id:
            msg = _ACCESS_DENIED if not allowed else "This admin dashboard belongs to another admin session."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True


class BusinessAdminTargetSelect(discord.ui.UserSelect):
    def __init__(self, view: "BusinessAdminDashboardView"):
        super().__init__(placeholder="Switch target user…", min_values=1, max_values=1, row=0)
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        user = self.values[0]
        self.parent_view.session.target_user_id = int(user.id)
        self.parent_view.session.target_business_key = None
        self.parent_view.session.page = 0
        await self.parent_view.refresh(interaction, notice=f"Target changed to {user.mention}.")


class BusinessAdminBusinessSelect(discord.ui.Select):
    def __init__(self, view: "BusinessAdminDashboardView", ownerships: list[BusinessOwnershipRow]):
        options = [discord.SelectOption(label=row.business_key.replace('_', ' ').title(), value=row.business_key, default=(row.business_key == view.session.target_business_key)) for row in ownerships[:25]]
        super().__init__(placeholder="Choose business…", options=options or [discord.SelectOption(label="No business found", value="__none__")], row=0)
        self.parent_view = view
        self.disabled = not ownerships

    async def callback(self, interaction: discord.Interaction) -> None:
        value = self.values[0]
        if value == "__none__":
            await interaction.response.send_message("No business available for that user.", ephemeral=True)
            return
        self.parent_view.session.target_business_key = value
        self.parent_view.session.page = 0
        await self.parent_view.refresh(interaction)


class AdminValueModal(discord.ui.Modal):
    def __init__(self, *, title: str, fields: list[tuple[str, str, str, bool]], on_submit_cb):
        super().__init__(title=title)
        self._on_submit_cb = on_submit_cb
        self.inputs = {}
        for custom_id, label, default, required in fields:
            inp = discord.ui.TextInput(label=label, default=default, required=required)
            self.inputs[custom_id] = inp
            self.add_item(inp)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        values = {key: str(inp.value).strip() for key, inp in self.inputs.items()}
        await self._on_submit_cb(interaction, values)


class AdminGrantStaffSetupView(BusinessAdminBaseView):
    def __init__(self, *, parent_view: "BusinessAdminDashboardView"):
        super().__init__(cog=parent_view.cog, guild_id=parent_view.guild_id, session=parent_view.session, timeout=240)
        self.parent_view = parent_view
        self._build_controls()

    def _build_controls(self) -> None:
        target_kind = "worker" if self.session.special_staff_type == "employee" else "manager"
        rarity_select = discord.ui.Select(
            placeholder="Rarity filter",
            min_values=1,
            max_values=1,
            row=0,
            options=[
                discord.SelectOption(label="Any rarity", value="any", default=self.session.special_rarity_filter_key == "any"),
                discord.SelectOption(label="Rare only", value="rare_only", default=self.session.special_rarity_filter_key == "rare_only"),
                discord.SelectOption(label="Epic only", value="epic_only", default=self.session.special_rarity_filter_key == "epic_only"),
                discord.SelectOption(label="Mythical only", value="mythical_only", default=self.session.special_rarity_filter_key == "mythical_only"),
                discord.SelectOption(label="Rare+", value="rare_plus", default=self.session.special_rarity_filter_key == "rare_plus"),
                discord.SelectOption(label="Epic+", value="epic_plus", default=self.session.special_rarity_filter_key == "epic_plus"),
            ],
        )
        rarity_select.callback = self._on_rarity_change
        self.add_item(rarity_select)

        if target_kind == "worker":
            kind_options = [
                discord.SelectOption(label="Any worker type", value="any", default=self.session.special_kind_key == "any"),
                discord.SelectOption(label="Fast", value="fast", default=self.session.special_kind_key == "fast"),
                discord.SelectOption(label="Efficient", value="efficient", default=self.session.special_kind_key == "efficient"),
                discord.SelectOption(label="Kind", value="kind", default=self.session.special_kind_key == "kind"),
            ]
            kind_placeholder = "Worker kind"
        else:
            kind_options = [
                discord.SelectOption(label="Any manager profile", value="any", default=self.session.special_kind_key == "any"),
                discord.SelectOption(label="Runtime focused", value="runtime", default=self.session.special_kind_key == "runtime"),
                discord.SelectOption(label="Profit focused", value="profit", default=self.session.special_kind_key == "profit"),
                discord.SelectOption(label="Automation focused", value="automation", default=self.session.special_kind_key == "automation"),
                discord.SelectOption(label="Balanced", value="balanced", default=self.session.special_kind_key == "balanced"),
            ]
            kind_placeholder = "Manager kind"
        kind_select = discord.ui.Select(placeholder=kind_placeholder, min_values=1, max_values=1, row=1, options=kind_options)
        kind_select.callback = self._on_kind_change
        self.add_item(kind_select)

        amount_options = [discord.SelectOption(label=str(v), value=str(v), default=self.session.special_roll_amount_key == str(v)) for v in VIP_REROLL_AMOUNT_OPTIONS]
        amount_options.append(discord.SelectOption(label="Max available", value="max", default=self.session.special_roll_amount_key == "max"))
        amount_select = discord.ui.Select(placeholder="Reroll amount", min_values=1, max_values=1, row=2, options=amount_options)
        amount_select.callback = self._on_amount_change
        self.add_item(amount_select)

    async def _summary_embed(self) -> discord.Embed:
        target_kind = "worker" if self.session.special_staff_type == "employee" else "manager"
        rarity_pool = _build_rarity_filter_options(target_kind=target_kind).get(self.session.special_rarity_filter_key, set())
        amount_label = "Max available" if self.session.special_roll_amount_key == "max" else _fmt_int(_clamp_int(_parse_int(self.session.special_roll_amount_key, 10), 1, AUTO_HIRE_MAX_REROLLS))
        embed = discord.Embed(
            title="Admin Staff Grant Setup",
            description="Same filtering controls as VIP reroll setup. Confirm to grant one matching staff member to the selected user.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Grant type", value=self.session.special_staff_type.title(), inline=True)
        embed.add_field(name="Rarity filter", value=_display_rarity_filter(self.session.special_rarity_filter_key), inline=True)
        embed.add_field(name="Kind filter", value=_kind_label(target_kind, self.session.special_kind_key), inline=True)
        embed.add_field(name="Roll budget", value=f"**{amount_label}**", inline=True)
        embed.add_field(name="Filtered pool", value=f"`{', '.join(sorted(rarity_pool))}`", inline=True)
        embed.set_footer(text="No Silver is charged while staff uses this panel.")
        return embed

    async def _on_rarity_change(self, interaction: discord.Interaction) -> None:
        self.session.special_rarity_filter_key = interaction.data.get("values", ["any"])[0]  # type: ignore[union-attr]
        await interaction.response.edit_message(embed=await self._summary_embed(), view=self)

    async def _on_kind_change(self, interaction: discord.Interaction) -> None:
        self.session.special_kind_key = interaction.data.get("values", ["any"])[0]  # type: ignore[union-attr]
        await interaction.response.edit_message(embed=await self._summary_embed(), view=self)

    async def _on_amount_change(self, interaction: discord.Interaction) -> None:
        self.session.special_roll_amount_key = interaction.data.get("values", ["10"])[0]  # type: ignore[union-attr]
        await interaction.response.edit_message(embed=await self._summary_embed(), view=self)

    @discord.ui.button(label="Confirm Grant", style=discord.ButtonStyle.success, emoji="✅", row=3)
    async def confirm_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog._business_admin_grant_special(interaction, self.session)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=3)
    async def cancel_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.parent_view.refresh(interaction, notice="Grant setup cancelled.")


class BusinessAdminDashboardView(BusinessAdminBaseView):
    def __init__(self, *, cog: "BusinessCog", guild_id: int, session: BusinessAdminSession, ownerships: list[BusinessOwnershipRow]):
        super().__init__(cog=cog, guild_id=guild_id, session=session)
        self.ownerships = ownerships
        self.add_item(BusinessAdminTargetSelect(self))
        self.add_item(BusinessAdminBusinessSelect(self, ownerships))

    async def refresh(self, interaction: discord.Interaction, *, notice: Optional[str] = None) -> None:
        payload = await self.cog._build_business_admin_payload(guild_id=self.guild_id, session=self.session)
        embed = payload["embed"]
        view = payload["view"]
        if notice:
            embed.description = f"{notice}\n\n{embed.description or ''}".strip()
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=view)
        else:
            await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="View Overview", style=discord.ButtonStyle.primary, row=1)
    async def btn_overview(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.session.panel = "overview"
        self.session.page = 0
        await self.refresh(interaction)

    @discord.ui.button(label="Manage Managers", style=discord.ButtonStyle.secondary, row=1)
    async def btn_managers(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.session.panel = "managers"
        self.session.page = 0
        await self.refresh(interaction)

    @discord.ui.button(label="Manage Employees", style=discord.ButtonStyle.secondary, row=1)
    async def btn_employees(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.session.panel = "employees"
        self.session.page = 0
        await self.refresh(interaction)

    @discord.ui.button(label="Edit Business Level", style=discord.ButtonStyle.secondary, row=2)
    async def btn_level(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.session.panel = "level"
        await self.refresh(interaction)

    @discord.ui.button(label="Edit Prestige", style=discord.ButtonStyle.secondary, row=2)
    async def btn_prestige(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.session.panel = "prestige"
        await self.refresh(interaction)

    @discord.ui.button(label="Edit Core Stats", style=discord.ButtonStyle.secondary, row=2)
    async def btn_core(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.session.panel = "core"
        await self.refresh(interaction)

    @discord.ui.button(label="Grant Special Staff", style=discord.ButtonStyle.success, row=3)
    async def btn_special(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.session.panel = "special"
        await self.refresh(interaction)

    @discord.ui.button(label="Action", style=discord.ButtonStyle.success, row=4)
    async def btn_action(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog._handle_business_admin_primary_action(interaction, self.session, self)

    @discord.ui.button(label="Secondary", style=discord.ButtonStyle.secondary, row=4)
    async def btn_secondary(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog._handle_business_admin_secondary_action(interaction, self.session, self)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=3)
    async def btn_refresh(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.refresh(interaction)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger, row=3)
    async def btn_close(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        for item in self.children:
            item.disabled = True
        if interaction.response.is_done():
            await interaction.edit_original_response(view=self)
        else:
            await interaction.response.edit_message(view=self)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(BusinessCog(bot))
