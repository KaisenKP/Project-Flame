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
from typing import List, Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import AdminAuditLogRow, BusinessManagerAssignmentRow, BusinessOwnershipRow, BusinessRunRow, BusinessWorkerAssignmentRow, WalletRow
from services.db import sessions
from services.achievements import check_and_grant_achievements, queue_achievement_announcements
from services.users import ensure_user_rows
from services.vip import is_vip_member
import os
from .runtime import BusinessRuntimeEngine, CompletedRunNotice

log = logging.getLogger(__name__)

AUTO_HIRE_MAX_REROLLS = 250
AUTO_HIRE_ALLOWED_RARITIES = {"common", "uncommon", "rare", "epic", "mythic"}
AUTO_HIRE_PROGRESS_UPDATE_INTERVAL_SECONDS = 1.0

_BUSINESS_ADMIN_ROLE_IDS = {int(part) for part in ((os.getenv("BUSINESS_ADMIN_ROLE_IDS") or os.getenv("BUSINESS_ADMIN_ROLE_ID") or "").replace(",", " ").split()) if part.strip().isdigit()}
RARITY_ORDER = ("common", "uncommon", "rare", "epic", "legendary", "mythical")
_MANAGER_TEMPLATES = {"Operations Lead": {"runtime_bonus_hours": 4, "profit_bonus_bp": 300, "auto_restart_charges": 1}, "Revenue Director": {"runtime_bonus_hours": 2, "profit_bonus_bp": 650, "auto_restart_charges": 0}, "Automation Chief": {"runtime_bonus_hours": 6, "profit_bonus_bp": 250, "auto_restart_charges": 2}, "Mythical Overseer": {"runtime_bonus_hours": 12, "profit_bonus_bp": 1200, "auto_restart_charges": 5}}
_WORKER_TEMPLATES = {"Analyst": {"worker_type": "efficient", "flat_profit_bonus": 250, "percent_profit_bonus_bp": 125}, "Closer": {"worker_type": "fast", "flat_profit_bonus": 400, "percent_profit_bonus_bp": 90}, "Specialist": {"worker_type": "kind", "flat_profit_bonus": 150, "percent_profit_bonus_bp": 220}, "Mythical Operator": {"worker_type": "efficient", "flat_profit_bonus": 1500, "percent_profit_bonus_bp": 900}}
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
        quantity: int = 1,
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


# =========================================================
# FORMATTERS
# =========================================================

def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


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
        effects.append(f"Productivity +{_format_short_percent_from_bp(power_bp)}")
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
        special.append(f"Productivity +{_format_short_percent_from_bp(power_total)}")
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
    e.set_footer(text="Business System • Chatbox Economy")
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
        f"Silver {_fmt_int(snap.silver_balance)}",
        f"Active {_fmt_int(snap.total_hourly_income_active)}/hr",
        f"Owned {_fmt_int(snap.owned_count)}",
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
    for c in owned_cards[:10]:
        marker = "▶" if selected_card and c.key == selected_card.key else "•"
        top_line = (
            f"{marker} {c.emoji} {c.name} • P{_fmt_int(c.prestige)} • "
            f"Lv{_fmt_int(c.visible_level)} • {_status_chip_for_card(c)}"
        )
        details = [f"{_fmt_int(c.hourly_profit)}/hr"]
        if c.running:
            details.append(f"{_format_hours_short(c.runtime_remaining_hours)} left")
        else:
            details.append("Ready to run")
        hint = _compact_business_hint(c)
        if hint:
            details.append(hint)
        rows.append(
            f"{top_line}\n" + " • ".join(_trim(part, 32) for part in details[:3])
        )
    e.add_field(name="Roster", value="\n\n".join(rows), inline=False)

    if selected_card:
        projected_total = int(getattr(selected_card, "projected_payout", int(selected_card.hourly_profit) * _estimated_cycle_hours_for_card(selected_card)))
        manager_summary = _format_manager_summary(getattr(selected_card, "manager_summary", None))
        worker_bp = int(getattr(selected_card, "worker_bonus_bp", 0) or 0)
        worker_summary = f"+{_format_short_percent_from_bp(worker_bp)} income" if worker_bp > 0 else None
        event_summary = _safe_str(getattr(selected_card, "active_event_summary", ""), "")
        synergy_summary = _safe_str(getattr(selected_card, "synergy_summary", ""), "")
        run_mode = _safe_str(getattr(selected_card, "run_mode", ""), "")
        next_hint = "Tap Stop to cash out or Manage for deeper control." if selected_card.running else "Tap Run to start the next payout cycle."

        spotlight_lines = [
            f"{selected_card.emoji} **{selected_card.name}** • P{_fmt_int(selected_card.prestige)} • Lv{_fmt_int(selected_card.visible_level)}",
            f"**Status:** {_status_chip_for_card(selected_card)}",
        ]
        if selected_card.running:
            spotlight_lines.append(f"**Projected Take:** {_fmt_int(projected_total)}")
        else:
            spotlight_lines.append(f"**Next Run Estimate:** {_fmt_int(projected_total)}")
        spotlight_lines.extend(
            line for line in [
                _format_spotlight_line("Workers", worker_summary),
                _format_spotlight_line("Manager", manager_summary),
                _format_spotlight_line("Event", _trim(event_summary, 60) if event_summary else None),
                _format_spotlight_line("Synergy", _trim(synergy_summary, 60) if synergy_summary else None),
                _format_spotlight_line("Run Mode", _trim(run_mode, 24) if run_mode and run_mode.lower() != "standard" else None),
                _format_spotlight_line("Next", next_hint),
            ]
            if line is not None
        )
        e.add_field(name=f"Spotlight • {selected_card.name}", value="\n".join(spotlight_lines), inline=False)
        e.add_field(
            name="Actions",
            value=(
                f"Selected: {selected_card.emoji} **{selected_card.name}**\n"
                "Buttons below act on the selected business only."
            ),
            inline=False,
        )
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


def _build_run_menu_embed(
    *,
    user: discord.abc.User,
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    owned = [c for c in snap.cards if c.owned]

    desc = "Pick one business to start now and collect hourly profit."
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
        runtime_txt = f"{_fmt_int(c.runtime_remaining_hours)}h left" if c.running else "Ready to start"
        approx_runtime = _estimated_cycle_hours_for_card(c)
        cycle_profit = int(c.hourly_profit) * approx_runtime
        lines.append(
            f"{c.emoji} **{c.name}**\n"
            f"└ {_status_badge(c.running, c.owned)}\n"
            f"└ Income: `{_fmt_int(c.hourly_profit)}/hr`\n"
            f"└ Run Profit: `{_fmt_int(cycle_profit)} per run`\n"
            f"└ ⏱️ {runtime_txt}"
        )

    e.add_field(name="Owned Businesses", value="\n\n".join(lines), inline=False)
    return e


def _build_manage_menu_embed(
    *,
    user: discord.abc.User,
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    owned = [c for c in snap.cards if c.owned]

    desc = "Select a business to view upgrades, staffing, and details."
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
        cycle_profit = int(c.hourly_profit) * _estimated_cycle_hours_for_card(c)
        lines.append(
            f"{c.emoji} **{c.name}**\n"
            f"└ Level `{_fmt_int(c.visible_level)}/{_fmt_int(c.max_level)}` • Prestige `{_fmt_int(c.prestige)}`\n"
            f"└ Total Progress `{_fmt_int(c.total_visible_level)}`\n"
            f"└ Profit `{_fmt_int(c.hourly_profit)}/hr` • Run `{_fmt_int(cycle_profit)}`\n"
            f"└ Workers `{_slot_text(c.worker_slots_used, c.worker_slots_total)}` • Managers `{_slot_text(c.manager_slots_used, c.manager_slots_total)}`"
        )

    e.add_field(name="Your Businesses", value="\n\n".join(lines), inline=False)
    return e


def _build_business_detail_embed(
    *,
    user: discord.abc.User,
    snap: BusinessManageSnapshot,
) -> discord.Embed:
    status = "🟢 Running" if snap.running else "🔴 Stopped"
    remaining = f"{_fmt_int(snap.runtime_remaining_hours)}h" if snap.running else "—"
    projected_total = int(getattr(snap, "projected_payout", int(snap.hourly_profit) * int(snap.total_runtime_hours)))

    e = _base_embed(
        title=f"📊 {snap.emoji} {snap.name}",
        description=(
            f"`Status` {status} • `Level` `{_fmt_int(snap.visible_level)}/{_fmt_int(snap.max_level)}` "
            f"• `Prestige` `{_fmt_int(snap.prestige)}` • `Mode` `{getattr(snap, 'run_mode', 'Standard')}` • `Total Progress` `{_fmt_int(snap.total_visible_level)}`"
        ),
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))

    e.add_field(
        name="Run Status",
        value=(
            f"Runtime Remaining: `{remaining}`\n"
            f"Cycle Runtime: `{_fmt_int(snap.total_runtime_hours)}h`\n"
            f"Projected Cycle Profit: `{_fmt_int(projected_total)} Silver`\n"
            f"Active Event: `{_trim(getattr(snap, 'active_event_summary', 'No active events'), 60)}`"
        ),
        inline=True,
    )
    e.add_field(
        name="Income",
        value=(
            f"Current Profit: `{_fmt_int(snap.hourly_profit)}/hr`\n"
            f"Run Profit: `{_fmt_int(projected_total)} per run`\n"
            f"Base Profit: `{_fmt_int(snap.base_hourly_income)}/hr`\n"
            f"Workers: `+{_fmt_int(getattr(snap, 'worker_bonus_bp', 0)/100)}%`\n"
            f"Synergy: `+{_fmt_int(getattr(snap, 'synergy_bonus_bp', 0)/100)}%`\n"
            f"Output Multiplier: `x{snap.prestige_multiplier}`"
        ),
        inline=True,
    )
    e.add_field(
        name="Staff",
        value=(
            f"Workers: `{_slot_text(snap.worker_slots_used, snap.worker_slots_total)}` • {_trim(getattr(snap, 'worker_summary', 'No workers assigned'), 80)}\n"
            f"Managers: `{_slot_text(snap.manager_slots_used, snap.manager_slots_total)}` • {_trim(getattr(snap, 'manager_summary', 'No managers assigned'), 80)}"
        ),
        inline=False,
    )

    progression_lines = [
        f"Bulk x1: {'Unlocked' if snap.bulk_upgrade_1_unlocked else 'Locked'}",
        f"Bulk x5: {'Unlocked' if snap.bulk_upgrade_5_unlocked else 'Locked'}",
        f"Bulk x10: {'Unlocked' if snap.bulk_upgrade_10_unlocked else 'Locked'}",
        f"Legacy equivalent level: {int(snap.total_visible_level)}",
    ]
    if snap.can_prestige:
        progression_lines.append("Prestige available now.")
    else:
        progression_lines.append(f"Prestige unlocks at Level {snap.max_level}.")
    if getattr(snap, 'next_unlock', None):
        progression_lines.append(str(getattr(snap, 'next_unlock')))
    e.add_field(name="Progression", value="\n".join(f"• {x}" for x in progression_lines), inline=False)

    if snap.notes:
        e.add_field(name="Active Bonuses", value="\n".join(f"• {x}" for x in snap.notes[:6]), inline=False)

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
    description = f"{slot_fill}\nHire, organize, and manage your workforce."
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
    description = f"{slot_fill}\nHire, reroll, and manage staff for this business."
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

        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
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
        selected_card = next((c for c in self.hub_snapshot.cards if c.key == self.selected_business_key), None)
        at_cap = bool(selected_card is not None and selected_card.visible_level >= selected_card.max_level)
        self.buy_button.disabled = owns_all
        self.manage_button.disabled = not has_selected
        self.run_button.disabled = (not has_selected) or bool(selected_card and selected_card.running)
        self.stop_button.disabled = (not has_selected) or not bool(selected_card and selected_card.running)
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

    @discord.ui.button(label="Manage", style=discord.ButtonStyle.secondary, emoji="🛠️", row=1)
    async def manage_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._load_selected_detail()
        if detail is None:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
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
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Run Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.selected_business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Stop", style=discord.ButtonStyle.danger, emoji="⏹️", row=1)
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
        embed.add_field(name="Stop Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.selected_business_key, owned=detail.owned, detail=detail)
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

    @discord.ui.button(label="Manager", style=discord.ButtonStyle.secondary, emoji="🧑‍💼", row=2)
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

    @discord.ui.button(label="Buy", style=discord.ButtonStyle.success, emoji="🛒", row=3)
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

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=3)
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
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key
        is_enabled = bool(upgrade_enabled) if upgrade_enabled is not None else bool(owned)
        self.upgrade_button.disabled = (not is_enabled) or bool(getattr(detail, "upgrade_cost", None) is None)
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
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
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
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Run Safe", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
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
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Run Aggressive", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
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
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Stop Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Status", style=discord.ButtonStyle.secondary, emoji="📡", row=0)
    async def status_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._reload_detail()
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Status", value="Current runtime, bonuses, and staffing shown above.", inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
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
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Upgrade Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
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
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Upgrade x5", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
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
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Upgrade x10", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
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
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Prestige Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
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



class RemoveWorkerModal(discord.ui.Modal, title="Remove Worker"):
    def __init__(self, view: "WorkerAssignmentsView"):
        super().__init__()
        self.parent_view = view
        self.slot_index = discord.ui.TextInput(label="Reply with worker slot #", placeholder="1", max_length=4)
        self.add_item(self.slot_index)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        requested_slot = _parse_int(str(self.slot_index.value), 0)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
        if detail is None:
            await interaction.response.send_message("That business could not be found.", ephemeral=True)
            return
        selected_slot = next((slot for slot in slots if int(getattr(slot, "slot_index", 0)) == int(requested_slot)), None)
        if not selected_slot or not bool(getattr(selected_slot, "is_active", False)):
            await _safe_defer(interaction)
            embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.parent_view.page)
            embed.add_field(name="Action", value=f"❌ No active worker found in slot **#{_fmt_int(requested_slot)}**.", inline=False)
            await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
            return

        worker_name = _safe_str(getattr(selected_slot, "worker_name", None), "Worker")
        worker_rarity = _safe_str(getattr(selected_slot, "rarity", None), "common")
        worker_type = _safe_str(getattr(selected_slot, "worker_type", None), "efficient")

        embed = discord.Embed(
            title="Confirm Worker Removal",
            description=(
                f"You are removing worker **{worker_name}** from slot **#{_fmt_int(requested_slot)}**.\n"
                "Are you sure you want to continue?"
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Worker", value=f"{worker_name} ({worker_rarity})", inline=True)
        embed.add_field(name="Type", value=worker_type, inline=True)
        await interaction.response.send_message(
            embed=embed,
            ephemeral=True,
            view=ConfirmWorkerRemovalView(parent_view=self.parent_view, slot_index=int(requested_slot), worker_name=worker_name),
        )


class ConfirmWorkerRemovalView(discord.ui.View):
    def __init__(self, *, parent_view: "WorkerAssignmentsView", slot_index: int, worker_name: str):
        super().__init__(timeout=60)
        self.parent_view = parent_view
        self.slot_index = int(slot_index)
        self.worker_name = str(worker_name)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This confirmation belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Remove", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                result = await remove_worker(
                    session,
                    guild_id=self.parent_view.guild_id,
                    user_id=self.parent_view.owner_id,
                    business_key=self.parent_view.business_key,
                    slot_index=self.slot_index,
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        self.parent_view._sync_pagination_buttons(total_slots=len(slots))
        embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.parent_view.page)
        embed.add_field(name="Action", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
        await interaction.edit_original_response(content="Removal confirmed.", embed=None, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.edit_message(content="Worker removal cancelled.", embed=None, view=None)


class RemoveManagerModal(discord.ui.Modal, title="Remove Manager"):
    def __init__(self, view: "ManagerAssignmentsView"):
        super().__init__()
        self.parent_view = view
        self.slot_index = discord.ui.TextInput(label="Reply with manager slot #", placeholder="1", max_length=4)
        self.add_item(self.slot_index)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        requested_slot = _parse_int(str(self.slot_index.value), 0)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
        if detail is None:
            await interaction.response.send_message("That business could not be found.", ephemeral=True)
            return
        selected_slot = next((slot for slot in slots if int(getattr(slot, "slot_index", 0)) == int(requested_slot)), None)
        if not selected_slot or not bool(getattr(selected_slot, "is_active", False)):
            await _safe_defer(interaction)
            embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.parent_view.page)
            embed.add_field(name="Action", value=f"❌ No active manager found in slot **#{_fmt_int(requested_slot)}**.", inline=False)
            await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
            return

        manager_name = _safe_str(getattr(selected_slot, "manager_name", None), "Manager")
        manager_rarity = _safe_str(getattr(selected_slot, "rarity", None), "common")
        runtime_bonus = _fmt_int(getattr(selected_slot, "runtime_bonus_hours", 0))
        profit_bonus = _fmt_int(getattr(selected_slot, "profit_bonus_bp", 0))

        embed = discord.Embed(
            title="Confirm Manager Removal",
            description=(
                f"You are removing manager **{manager_name}** from slot **#{_fmt_int(requested_slot)}**.\n"
                "Are you sure you want to continue?"
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Manager", value=f"{manager_name} ({manager_rarity})", inline=True)
        embed.add_field(name="Bonuses", value=f"+{runtime_bonus}h runtime • +{profit_bonus} bp", inline=True)
        await interaction.response.send_message(
            embed=embed,
            ephemeral=True,
            view=ConfirmManagerRemovalView(parent_view=self.parent_view, slot_index=int(requested_slot), manager_name=manager_name),
        )


class ConfirmManagerRemovalView(discord.ui.View):
    def __init__(self, *, parent_view: "ManagerAssignmentsView", slot_index: int, manager_name: str):
        super().__init__(timeout=60)
        self.parent_view = parent_view
        self.slot_index = int(slot_index)
        self.manager_name = str(manager_name)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This confirmation belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Remove", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                result = await remove_manager(
                    session,
                    guild_id=self.parent_view.guild_id,
                    user_id=self.parent_view.owner_id,
                    business_key=self.parent_view.business_key,
                    slot_index=self.slot_index,
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        self.parent_view._sync_pagination_buttons(total_slots=len(slots))
        embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots, page=self.parent_view.page)
        embed.add_field(name="Action", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
        await interaction.edit_original_response(content="Removal confirmed.", embed=None, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.edit_message(content="Manager removal cancelled.", embed=None, view=None)


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
        progress = discord.Embed(
            title="VIP Auto-Hiring Started",
            description="Rolling until worker slots are filled...",
            color=discord.Color.gold(),
        )
        progress.add_field(name="Progress", value=f"Filled: **0/?**\nRolls: **0/{_fmt_int(self.rerolls)}**\nSpent: **0 Silver**", inline=False)
        progress.add_field(name="Best Hit", value="None yet", inline=False)
        await interaction.edit_original_response(embed=progress, view=None)
        hires, rerolls_used, slots_full = 0, 0, False
        last_error = ""
        best_hit = "None yet"
        best_score = -1
        latest_hit = "None yet"
        filled_total = 0
        last_progress_update = time.monotonic()

        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                slots_before = await get_worker_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                open_slots = sum(1 for slot in slots_before if not bool(getattr(slot, "is_active", False)))
                for _ in range(self.rerolls):
                    roll_result = await roll_worker_candidate(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        reroll_cost=WORKER_CANDIDATE_REROLL_COST,
                    )
                    if not roll_result.ok or roll_result.worker_candidate is None:
                        last_error = roll_result.message
                        break
                    rerolls_used += 1
                    c = roll_result.worker_candidate
                    tags = _worker_compare_tags(c)
                    hit_line = f"{_safe_str(getattr(c, 'worker_name', None), 'Worker')} {_worker_rarity_badge(getattr(c, 'rarity', None))}"
                    candidate_score = _worker_candidate_score(c)
                    if candidate_score > best_score:
                        best_score = candidate_score
                        best_hit = hit_line
                    if str(getattr(c, "rarity", "common")).strip().lower() not in self.allowed_rarities:
                        should_push_update = (
                            rerolls_used == 1
                            or rerolls_used % 10 == 0
                            or "Mythical Pull" in tags
                            or "Rare Pull" in tags
                            or (time.monotonic() - last_progress_update) >= AUTO_HIRE_PROGRESS_UPDATE_INTERVAL_SECONDS
                        )
                        if should_push_update:
                            progress = discord.Embed(
                                title="VIP Auto-Hiring In Progress",
                                description="Scanning workers and snapping up matching hires...",
                                color=discord.Color.gold(),
                            )
                            progress.add_field(name="Progress", value=f"Filled: **{_fmt_int(hires)}/{_fmt_int(open_slots or max(hires,1))}**\nRolls: **{_fmt_int(rerolls_used)}/{_fmt_int(self.rerolls)}**\nSpent: **{_fmt_int(rerolls_used * WORKER_CANDIDATE_REROLL_COST)} Silver**", inline=False)
                            progress.add_field(name="Best Hit", value=best_hit, inline=False)
                            progress.add_field(name="Latest Hit", value=hit_line, inline=False)
                            await interaction.edit_original_response(embed=progress, view=None)
                            last_progress_update = time.monotonic()
                        continue
                    hire_result = await hire_worker_manual(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        worker_name=str(getattr(c, "worker_name", "Worker")),
                        worker_type=str(getattr(c, "worker_type", "efficient")),
                        rarity=str(getattr(c, "rarity", "common")),
                        flat_profit_bonus=int(getattr(c, "flat_profit_bonus", 0) or 0),
                        percent_profit_bonus_bp=int(getattr(c, "percent_profit_bonus_bp", 0) or 0),
                        charge_silver=False,
                    )
                    if not hire_result.ok:
                        last_error = hire_result.message
                        slots_full = "slots are full" in str(hire_result.message).lower()
                        break
                    hires += 1
                    filled_total = open_slots
                    latest_hit = hit_line + (" • " + " • ".join(tags[:2]) if tags else "")
                    progress = discord.Embed(
                        title="VIP Auto-Hiring In Progress",
                        description="Rolling live results into your roster...",
                        color=discord.Color.gold(),
                    )
                    progress.add_field(name="Progress", value=f"Filled: **{_fmt_int(hires)}/{_fmt_int(open_slots or hires)}**\nRolls: **{_fmt_int(rerolls_used)}/{_fmt_int(self.rerolls)}**\nSpent: **{_fmt_int(rerolls_used * WORKER_CANDIDATE_REROLL_COST)} Silver**", inline=False)
                    progress.add_field(name="Best Hit", value=best_hit, inline=False)
                    progress.add_field(name="Latest Hit", value=latest_hit, inline=False)
                    await interaction.edit_original_response(embed=progress, view=None)
                    last_progress_update = time.monotonic()

        self.parent_view.current_candidate = None
        suffix = " Worker slots are full." if slots_full else (f" {last_error}" if last_error else "")
        await self.parent_view._show_recruitment_board(interaction, action_message=f"✅ Auto-Hire complete: hired **{_fmt_int(hires)}** workers in **{_fmt_int(rerolls_used)}** rerolls.{suffix}")
        done = discord.Embed(
            title="VIP Auto-Hiring Complete",
            description="Your worker rush is finished.",
            color=SUCCESS_COLOR,
        )
        done.add_field(name="Results", value=f"Filled **{_fmt_int(hires)}/{_fmt_int(filled_total or hires)}** slots\nRolls Used: **{_fmt_int(rerolls_used)}**\nSpent: **{_fmt_int(rerolls_used * WORKER_CANDIDATE_REROLL_COST)} Silver**", inline=False)
        done.add_field(name="Best Pull", value=best_hit, inline=False)
        if latest_hit != "None yet":
            done.add_field(name="Final Hit", value=latest_hit, inline=False)
        await interaction.edit_original_response(embed=done, view=None)

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
        progress = discord.Embed(
            title="VIP Auto-Hiring Started",
            description="Rolling until manager slots are filled...",
            color=discord.Color.gold(),
        )
        progress.add_field(name="Progress", value=f"Filled: **0/?**\nRolls: **0/{_fmt_int(self.rerolls)}**\nSpent: **0 Silver**", inline=False)
        progress.add_field(name="Best Hit", value="None yet", inline=False)
        await interaction.edit_original_response(embed=progress, view=None)
        hires, rerolls_used, slots_full = 0, 0, False
        last_error = ""
        best_hit = "None yet"
        best_score = -1
        latest_hit = "None yet"
        filled_total = 0
        last_progress_update = time.monotonic()

        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                slots_before = await get_manager_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                open_slots = sum(1 for slot in slots_before if not bool(getattr(slot, "is_active", False)))
                for _ in range(self.rerolls):
                    roll_result = await roll_manager_candidate(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        reroll_cost=MANAGER_CANDIDATE_REROLL_COST,
                    )
                    if not roll_result.ok or roll_result.manager_candidate is None:
                        last_error = roll_result.message
                        break
                    rerolls_used += 1
                    c = roll_result.manager_candidate
                    tags = _manager_compare_tags(c)
                    hit_line = f"{_safe_str(getattr(c, 'manager_name', None), 'Manager')} {_manager_rarity_badge(getattr(c, 'rarity', None))}"
                    candidate_score = _manager_candidate_score(c)
                    if candidate_score > best_score:
                        best_score = candidate_score
                        best_hit = hit_line
                    if str(getattr(c, "rarity", "common")).strip().lower() not in self.allowed_rarities:
                        should_push_update = (
                            rerolls_used == 1
                            or rerolls_used % 10 == 0
                            or "Mythical Pull" in tags
                            or "Rare Pull" in tags
                            or (time.monotonic() - last_progress_update) >= AUTO_HIRE_PROGRESS_UPDATE_INTERVAL_SECONDS
                        )
                        if should_push_update:
                            progress = discord.Embed(
                                title="VIP Auto-Hiring In Progress",
                                description="Rolling live manager reveals...",
                                color=discord.Color.gold(),
                            )
                            progress.add_field(name="Progress", value=f"Filled: **{_fmt_int(hires)}/{_fmt_int(open_slots or max(hires,1))}**\nRolls: **{_fmt_int(rerolls_used)}/{_fmt_int(self.rerolls)}**\nSpent: **{_fmt_int(rerolls_used * MANAGER_CANDIDATE_REROLL_COST)} Silver**", inline=False)
                            progress.add_field(name="Best Hit", value=best_hit, inline=False)
                            progress.add_field(name="Latest Hit", value=hit_line, inline=False)
                            await interaction.edit_original_response(embed=progress, view=None)
                            last_progress_update = time.monotonic()
                        continue
                    hire_result = await hire_manager_manual(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        manager_name=str(getattr(c, "manager_name", "Manager")),
                        rarity=str(getattr(c, "rarity", "common")),
                        runtime_bonus_hours=int(getattr(c, "runtime_bonus_hours", 0) or 0),
                        profit_bonus_bp=int(getattr(c, "profit_bonus_bp", 0) or 0),
                        auto_restart_charges=int(getattr(c, "auto_restart_charges", 0) or 0),
                        charge_silver=False,
                    )
                    if not hire_result.ok:
                        last_error = hire_result.message
                        slots_full = "slots are full" in str(hire_result.message).lower()
                        break
                    hires += 1
                    filled_total = open_slots
                    latest_hit = hit_line + (" • " + " • ".join(tags[:2]) if tags else "")
                    progress = discord.Embed(
                        title="VIP Auto-Hiring In Progress",
                        description="Live recruiting in progress...",
                        color=discord.Color.gold(),
                    )
                    progress.add_field(name="Progress", value=f"Filled: **{_fmt_int(hires)}/{_fmt_int(open_slots or hires)}**\nRolls: **{_fmt_int(rerolls_used)}/{_fmt_int(self.rerolls)}**\nSpent: **{_fmt_int(rerolls_used * MANAGER_CANDIDATE_REROLL_COST)} Silver**", inline=False)
                    progress.add_field(name="Best Hit", value=best_hit, inline=False)
                    progress.add_field(name="Latest Hit", value=latest_hit, inline=False)
                    await interaction.edit_original_response(embed=progress, view=None)
                    last_progress_update = time.monotonic()

        self.parent_view.current_candidate = None
        suffix = " Manager slots are full." if slots_full else (f" {last_error}" if last_error else "")
        await self.parent_view._show_recruitment_board(interaction, action_message=f"✅ Auto-Hire complete: hired **{_fmt_int(hires)}** managers in **{_fmt_int(rerolls_used)}** rerolls.{suffix}")
        done = discord.Embed(
            title="VIP Auto-Hiring Complete",
            description="Your manager session is complete.",
            color=SUCCESS_COLOR,
        )
        done.add_field(name="Results", value=f"Filled **{_fmt_int(hires)}/{_fmt_int(filled_total or hires)}** slots\nRolls Used: **{_fmt_int(rerolls_used)}**\nSpent: **{_fmt_int(rerolls_used * MANAGER_CANDIDATE_REROLL_COST)} Silver**", inline=False)
        done.add_field(name="Best Pull", value=best_hit, inline=False)
        if latest_hit != "None yet":
            done.add_field(name="Final Hit", value=latest_hit, inline=False)
        await interaction.edit_original_response(embed=done, view=None)

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
        rerolls = 15
        allowed_rarities = set(AUTO_HIRE_ALLOWED_RARITIES)
        total_cost = rerolls * WORKER_CANDIDATE_REROLL_COST
        embed = discord.Embed(
            title="Confirm Worker Auto-Hire",
            description=f"Auto-Hire can spend up to **{_fmt_int(total_cost)} Silver** for **{_fmt_int(rerolls)} rerolls**.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Allowed rarities", value=f"`{', '.join(sorted(allowed_rarities))}`", inline=False)
        embed.add_field(name="Flow", value="Auto-Hire keeps rolling and hires matches until slots are full or budget is spent.", inline=False)
        await interaction.response.send_message(
            "VIP Auto-Reroll started with default settings (all rarities, 15 rerolls).",
            embed=embed,
            view=ConfirmWorkerAutoHireView(parent_view=self, rerolls=rerolls, allowed_rarities=allowed_rarities),
            ephemeral=True,
        )

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
        await _safe_defer(interaction)

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

    @discord.ui.button(label="Reroll Worker", style=discord.ButtonStyle.primary, emoji="🎲", row=0)
    async def reroll_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
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

    @discord.ui.button(label="Auto-Hire (VIP)", style=discord.ButtonStyle.secondary, emoji="⭐", row=1)
    async def auto_hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if not self.is_vip:
            await interaction.response.send_message("Auto-Hire is a VIP feature.", ephemeral=True)
            return
        await self._send_auto_hire_reply(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="⬅️", row=1)
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
        rerolls = 15
        allowed_rarities = set(AUTO_HIRE_ALLOWED_RARITIES)
        total_cost = rerolls * MANAGER_CANDIDATE_REROLL_COST
        embed = discord.Embed(
            title="Confirm Manager Auto-Hire",
            description=f"Auto-Hire can spend up to **{_fmt_int(total_cost)} Silver** for **{_fmt_int(rerolls)} rerolls**.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Allowed rarities", value=f"`{', '.join(sorted(allowed_rarities))}`", inline=False)
        embed.add_field(name="Flow", value="Auto-Hire keeps rolling and hires matches until slots are full or budget is spent.", inline=False)
        await interaction.response.send_message(
            "VIP Auto-Reroll started with default settings (all rarities, 15 rerolls).",
            embed=embed,
            view=ConfirmManagerAutoHireView(parent_view=self, rerolls=rerolls, allowed_rarities=allowed_rarities),
            ephemeral=True,
        )

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
        await _safe_defer(interaction)

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

    @discord.ui.button(label="Reroll Manager", style=discord.ButtonStyle.primary, emoji="🎲", row=0)
    async def reroll_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
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

    @discord.ui.button(label="Auto-Hire (VIP)", style=discord.ButtonStyle.secondary, emoji="⭐", row=1)
    async def auto_hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if not self.is_vip:
            await interaction.response.send_message("Auto-Hire is a VIP feature.", ephemeral=True)
            return
        await self._send_auto_hire_reply(interaction)

    @discord.ui.button(label="Remove Manager", style=discord.ButtonStyle.danger, emoji="➖", row=1)
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_modal(RemoveManagerModal(self))

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
        self.runtime_engine = BusinessRuntimeEngine(on_run_completed=self._notify_business_run_completed)

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

        event_summary = "No special events this run."
        if notice.event_outcomes:
            net_delta = sum(int(evt.silver_delta) for evt in notice.event_outcomes)
            sign = "+" if net_delta >= 0 else ""
            event_summary = (
                f"Events: **{len(notice.event_outcomes)}** triggered "
                f"(net {sign}{net_delta:,} Silver vs baseline)."
            )

        summary = {
            "business_name": business_name,
            "hours_paid_total": int(notice.hours_paid_total),
            "silver_paid_total": int(notice.silver_paid_total),
            "event_count": len(notice.event_outcomes),
            "net_event_delta": int(sum(int(evt.silver_delta) for evt in notice.event_outcomes)),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        message = (
            f"<@{notice.user_id}> your business run finished: **{business_name}**\n"
            f"• Runtime paid: **{notice.hours_paid_total}h**\n"
            f"• Total earned: **{notice.silver_paid_total:,} Silver**\n"
            f"• {event_summary}"
        )

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
                await channel.send(message)
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
                await user.send(message)
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
        await self._ensure_business_prestige_merge()
        await self._run_one_time_upgrade_refund()
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
                lines.append(f"• **{business_name}** — {silver:,} Silver over {hours}h (events {sign}{net:,})")
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
                    embed.add_field(name="Grant Special Staff", value=f"Type: **{session.special_staff_type.title()}**\nRarity: **{session.special_staff_rarity.title()}**\nTemplate: **{session.special_staff_template or 'Not selected'}**", inline=False)
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
            await self._business_admin_grant_special(interaction, admin_session)
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
        template_name = admin_session.special_staff_template
        if not template_name:
            template_name = ("Mythical Overseer" if admin_session.special_staff_type == "manager" else "Mythical Operator")
            admin_session.special_staff_template = template_name
        async with self.sessionmaker() as session:
            async with session.begin():
                if admin_session.special_staff_type == "manager":
                    stats = dict(_MANAGER_TEMPLATES[template_name])
                    result = await hire_manager_manual(session, guild_id=int(interaction.guild_id), user_id=int(admin_session.target_user_id), business_key=str(admin_session.target_business_key), manager_name=template_name, rarity=admin_session.special_staff_rarity, runtime_bonus_hours=int(stats["runtime_bonus_hours"]), profit_bonus_bp=int(stats["profit_bonus_bp"]), auto_restart_charges=int(stats["auto_restart_charges"]), charge_silver=False)
                else:
                    stats = dict(_WORKER_TEMPLATES[template_name])
                    result = await hire_worker_manual(session, guild_id=int(interaction.guild_id), user_id=int(admin_session.target_user_id), business_key=str(admin_session.target_business_key), worker_name=template_name, worker_type=str(stats["worker_type"]), rarity=admin_session.special_staff_rarity, flat_profit_bonus=int(stats["flat_profit_bonus"]), percent_profit_bonus_bp=int(stats["percent_profit_bonus_bp"]), charge_silver=False)
                await self._log_business_admin_action(session, guild_id=interaction.guild_id, actor_user_id=interaction.user.id, target_user_id=admin_session.target_user_id, action="insert", table_name=f"business_{admin_session.special_staff_type}_assignments", pk_json={"business_key": admin_session.target_business_key}, before=None, after={"template": template_name, "rarity": admin_session.special_staff_rarity, "ok": result.ok, "message": result.message}, reason=f"Granted special {admin_session.special_staff_type}")
        payload = await self._build_business_admin_payload(guild_id=int(interaction.guild_id), session=admin_session)
        embed = payload["embed"]
        embed.description = f"{result.message}\n\n{embed.description or ''}".strip()
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
    @app_commands.command(name="business_admin_hire_worker", description="[Admin/Debug] Manually hire a worker with explicit stats.")
    @app_commands.checks.has_permissions(administrator=True)
    async def business_admin_hire_worker_cmd(
        self,
        interaction: discord.Interaction,
        business_key: str,
        worker_name: str,
        worker_type: str,
        rarity: str,
        flat_profit_bonus: int,
        percent_profit_bonus_bp: int,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True, thinking=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                result = await hire_worker_manual(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=int(interaction.user.id),
                    business_key=business_key,
                    worker_name=worker_name,
                    worker_type=worker_type,
                    rarity=rarity,
                    flat_profit_bonus=flat_profit_bonus,
                    percent_profit_bonus_bp=percent_profit_bonus_bp,
                )
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=int(interaction.user.id),
                    business_key=business_key,
                )
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        if result.ok and result.hired_worker is not None:
            embed = _build_worker_hire_result_embed(user=interaction.user, detail=detail, hired=result.hired_worker)
            embed.add_field(name="Mode", value="Admin/Debug Manual Hire", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        await interaction.followup.send(
            embed=_build_result_embed(title="Admin Worker Hire", message=result.message, ok=False),
            ephemeral=True,
        )

    @app_commands.command(name="business_admin_hire_manager", description="[Admin/Debug] Manually hire a manager with explicit stats.")
    @app_commands.checks.has_permissions(administrator=True)
    async def business_admin_hire_manager_cmd(
        self,
        interaction: discord.Interaction,
        business_key: str,
        manager_name: str,
        rarity: str,
        runtime_bonus_hours: int,
        profit_bonus_bp: int,
        auto_restart_charges: int,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True, thinking=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                result = await hire_manager_manual(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=int(interaction.user.id),
                    business_key=business_key,
                    manager_name=manager_name,
                    rarity=rarity,
                    runtime_bonus_hours=runtime_bonus_hours,
                    profit_bonus_bp=profit_bonus_bp,
                    auto_restart_charges=auto_restart_charges,
                )
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=int(interaction.user.id),
                    business_key=business_key,
                )
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        if result.ok and result.hired_manager is not None:
            embed = _build_manager_hire_result_embed(user=interaction.user, detail=detail, hired=result.hired_manager)
            embed.add_field(name="Mode", value="Admin/Debug Manual Hire", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        await interaction.followup.send(
            embed=_build_result_embed(title="Admin Manager Hire", message=result.message, ok=False),
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
